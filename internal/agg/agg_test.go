package agg

import (
	"context"
	"testing"
	"time"

	"github.com/lesomnus/mcap-exporter/internal/tail"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const sec = int64(time.Second)

func msg(topic string, ns int64) tail.Msg {
	return tail.Msg{Topic: topic, Schema: "S", Time: time.Unix(0, ns)}
}

// capture is a Sink that records all emitted data points and the last metric.
func capture(dst *[]metricdata.DataPoint[int64], last **metricdata.Metrics) Sink {
	return func(_ context.Context, ms []metricdata.Metrics) error {
		for i := range ms {
			*last = &ms[i]
			sum := ms[i].Data.(metricdata.Sum[int64])
			*dst = append(*dst, sum.DataPoints...)
		}
		return nil
	}
}

func TestFlush_BucketsSealOnWatermark(t *testing.T) {
	a := New(time.Second, 0, nil, nil, nil)
	// bucket [1s,2s): 3 messages; bucket [2s,3s): 2 messages.
	for _, ns := range []int64{1 * sec, 1*sec + sec/5, 2*sec - 1} {
		a.observe(msg("/t", ns))
	}
	for _, ns := range []int64{2 * sec, 2*sec + sec/2} {
		a.observe(msg("/t", ns))
	}

	var got []metricdata.DataPoint[int64]
	var lastMetric *metricdata.Metrics
	sink := capture(&got, &lastMetric)

	// watermark = 2.5s. With grace 0, [1,2) is sealed (wm >= 2s) but [2,3) is not.
	require.NoError(t, a.flush(context.Background(), sink, false))
	require.Len(t, got, 1)
	require.Equal(t, int64(3), got[0].Value)
	require.Equal(t, time.Unix(0, 1*sec), got[0].StartTime)
	require.Equal(t, time.Unix(0, 2*sec), got[0].Time)

	// metric shape: delta, monotonic, well-named.
	sum := lastMetric.Data.(metricdata.Sum[int64])
	require.Equal(t, MetricName, lastMetric.Name)
	require.Equal(t, metricdata.DeltaTemporality, sum.Temporality)
	require.True(t, sum.IsMonotonic)

	topic, ok := got[0].Attributes.Value("topic")
	require.True(t, ok)
	require.Equal(t, "/t", topic.AsString())

	// Final flush drains the still-open [2,3) bucket.
	got = nil
	require.NoError(t, a.flush(context.Background(), sink, true))
	require.Len(t, got, 1)
	require.Equal(t, int64(2), got[0].Value)
	require.Equal(t, time.Unix(0, 2*sec), got[0].StartTime)
	require.Equal(t, time.Unix(0, 3*sec), got[0].Time)
}

func TestFlush_GraceDelaysSealing(t *testing.T) {
	a := New(time.Second, 500*time.Millisecond, nil, nil, nil)
	a.observe(msg("/t", 1*sec)) // bucket [1,2)
	a.observe(msg("/t", 2*sec)) // watermark = 2s; [1,2) end+grace = 2.5s > 2s -> not sealed

	var got []metricdata.DataPoint[int64]
	var lm *metricdata.Metrics
	require.NoError(t, a.flush(context.Background(), capture(&got, &lm), false))
	require.Empty(t, got)

	a.observe(msg("/t", 2*sec+sec/2)) // watermark = 2.5s; now [1,2) is sealed
	require.NoError(t, a.flush(context.Background(), capture(&got, &lm), false))
	require.Len(t, got, 1)
	require.Equal(t, int64(1), got[0].Value)
}

func TestRecent_HoldsThenBoundsAtAnchor(t *testing.T) {
	recent := 2 * time.Second
	a := New(time.Second, 0, &recent, nil, nil)
	// Backlog: buckets [1,2)..[5,6). wmax climbs to 5s.
	for _, ns := range []int64{1 * sec, 2 * sec, 3 * sec, 4 * sec, 5 * sec} {
		a.observe(msg("/t", ns))
	}

	var got []metricdata.DataPoint[int64]
	var lm *metricdata.Metrics
	sink := capture(&got, &lm)

	// Before catch-up nothing is emitted (and old buckets are pruned).
	require.NoError(t, a.flush(context.Background(), sink, false))
	require.Empty(t, got)

	// At catch-up the floor locks to wmax(5s) - recent(2s) = 3s; only buckets
	// ending after 3s and sealed are emitted: [3,4) and [4,5). [5,6) is unsealed.
	a.anchor()
	require.NoError(t, a.flush(context.Background(), sink, false))
	starts := []time.Time{}
	for _, dp := range got {
		starts = append(starts, dp.StartTime)
	}
	require.ElementsMatch(t, []time.Time{time.Unix(0, 3*sec), time.Unix(0, 4*sec)}, starts)
}

func TestRecent_PruneBoundsMemory(t *testing.T) {
	recent := 2 * time.Second
	a := New(time.Second, 0, &recent, nil, nil)
	for _, ns := range []int64{1 * sec, 2 * sec, 3 * sec, 4 * sec, 5 * sec} {
		a.observe(msg("/t", ns))
	}
	// A warmup flush prunes buckets that can never be within the window
	// (end <= wmax - recent = 3s): [1,2) and [2,3) are dropped from memory.
	var got []metricdata.DataPoint[int64]
	var lm *metricdata.Metrics
	require.NoError(t, a.flush(context.Background(), capture(&got, &lm), false))
	_, has1 := a.topics["/t"].counts[1*sec]
	_, has2 := a.topics["/t"].counts[2*sec]
	_, has3 := a.topics["/t"].counts[3*sec]
	require.False(t, has1)
	require.False(t, has2)
	require.True(t, has3)
}

func TestRecent_FinalFlushAnchorsBeforeCatchUp(t *testing.T) {
	// Shutdown before catch-up must still emit (anchoring immediately).
	recent := time.Hour
	a := New(time.Second, 0, &recent, nil, nil)
	a.observe(msg("/t", 1*sec))
	a.observe(msg("/t", 2*sec))

	var got []metricdata.DataPoint[int64]
	var lm *metricdata.Metrics
	require.NoError(t, a.flush(context.Background(), capture(&got, &lm), true))
	require.NotEmpty(t, got)
}

func TestFlush_LastEmittedSurvivesRestart(t *testing.T) {
	// Simulate a restart that has already emitted up to bucket start 1s.
	a := New(time.Second, 0, nil, nil, map[string]int64{"/t": 1 * sec})
	a.observe(msg("/t", 1*sec)) // start 1s <= last 1s -> skipped
	a.observe(msg("/t", 2*sec)) // start 2s > last -> emitted
	a.observe(msg("/t", 3*sec)) // advance watermark

	var got []metricdata.DataPoint[int64]
	var lm *metricdata.Metrics
	require.NoError(t, a.flush(context.Background(), capture(&got, &lm), false))
	require.Len(t, got, 1)
	require.Equal(t, time.Unix(0, 2*sec), got[0].StartTime)
	require.Equal(t, int64(2*sec), a.lastEmitted["/t"])
}

func TestRun_FlushesOnContextCancel(t *testing.T) {
	a := New(time.Second, 0, nil, nil, nil)
	in := make(chan tail.Msg, 8)
	in <- msg("/t", 1*sec)
	in <- msg("/t", 1*sec+1)

	var got []metricdata.DataPoint[int64]
	var lm *metricdata.Metrics
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx, in, nil, time.Hour, capture(&got, &lm), nil) }()

	// Give Run a moment to drain, then cancel to trigger the final flush.
	time.Sleep(50 * time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	require.Len(t, got, 1)
	require.Equal(t, int64(2), got[0].Value)
}

func TestRun_RecentAnchorsOnCaughtUp(t *testing.T) {
	recent := 2 * time.Second
	a := New(time.Second, 0, &recent, nil, nil)
	in := make(chan tail.Msg, 16)
	caughtUp := make(chan struct{})
	for _, ns := range []int64{1 * sec, 2 * sec, 3 * sec, 4 * sec, 5 * sec} {
		in <- msg("/t", ns)
	}

	var got []metricdata.DataPoint[int64]
	var lm *metricdata.Metrics
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx, in, caughtUp, time.Hour, capture(&got, &lm), nil) }()

	time.Sleep(50 * time.Millisecond) // let the backlog drain (held, not emitted)
	close(caughtUp)                   // catch-up: floor locks to 5s - 2s = 3s
	time.Sleep(50 * time.Millisecond)
	cancel()
	require.NoError(t, <-done)

	starts := map[int64]bool{}
	for _, dp := range got {
		starts[dp.StartTime.Unix()] = true
	}
	require.True(t, starts[3])
	require.True(t, starts[4])
	require.False(t, starts[1])
	require.False(t, starts[2])
}

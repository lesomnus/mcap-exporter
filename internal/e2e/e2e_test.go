// Package e2e exercises the whole pipeline: tail an MCAP file, bucket its
// messages, and push delta data points through export.Sink — asserting the
// per-(topic,bucket) counts and that each point is stamped with the message
// time window, not wall-clock.
package e2e

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/mcap-exporter/internal/agg"
	"github.com/lesomnus/mcap-exporter/internal/export"
	"github.com/lesomnus/mcap-exporter/internal/mcaptest"
	"github.com/lesomnus/mcap-exporter/internal/tail"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type fakeExporter struct {
	mu  sync.Mutex
	rms []*metricdata.ResourceMetrics
}

func (f *fakeExporter) Temporality(metric.InstrumentKind) metricdata.Temporality {
	return metricdata.DeltaTemporality
}
func (f *fakeExporter) Aggregation(metric.InstrumentKind) metric.Aggregation {
	return metric.AggregationDefault{}
}
func (f *fakeExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// ResourceMetrics may be reused by the caller; copy what we assert on.
	cp := *rm
	f.rms = append(f.rms, &cp)
	return nil
}
func (f *fakeExporter) ForceFlush(context.Context) error { return nil }
func (f *fakeExporter) Shutdown(context.Context) error   { return nil }

const s = 1_000_000_000

func TestPipeline_TailToDeltaExport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bag.mcap")
	require.NoError(t, mcaptest.Write(path, 0, []mcaptest.Spec{
		{Topic: "/a", LogTime: 1.0 * s},
		{Topic: "/a", LogTime: 1.2 * s},
		{Topic: "/a", LogTime: 1.4 * s},
		{Topic: "/b", LogTime: 1.5 * s},
		{Topic: "/a", LogTime: 2.1 * s},
	}))

	fake := &fakeExporter{}
	sink := export.Sink(fake, export.Resource())
	aggr := agg.New(time.Second, 0, nil, nil, nil)

	out := make(chan tail.Msg, 64)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer close(out)
		// The finalized file has a footer, so TailMany returns once drained.
		require.NoError(t, tail.TailMany(ctx, []string{path}, nil, 10*time.Millisecond, out, nil))
	}()
	go func() {
		defer wg.Done()
		require.NoError(t, aggr.Run(ctx, out, nil, 20*time.Millisecond, sink, nil))
	}()
	wg.Wait()

	// Collect every emitted point keyed by topic + bucket start (seconds).
	type key struct {
		topic string
		start int64
	}
	got := map[key]int64{}
	fake.mu.Lock()
	for _, rm := range fake.rms {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name != agg.MetricName { // bytes/interval asserted elsewhere
					continue
				}
				sum := m.Data.(metricdata.Sum[int64])
				require.Equal(t, metricdata.DeltaTemporality, sum.Temporality)
				require.True(t, sum.IsMonotonic)
				for _, dp := range sum.DataPoints {
					// Window width is exactly the bucket, and Hz = Value/width.
					require.Equal(t, int64(s), dp.Time.UnixNano()-dp.StartTime.UnixNano())
					topic, _ := dp.Attributes.Value("topic")
					got[key{topic.AsString(), dp.StartTime.Unix()}] += dp.Value
				}
			}
		}
	}
	fake.mu.Unlock()

	require.Equal(t, map[key]int64{
		{"/a", 1}: 3,
		{"/a", 2}: 1,
		{"/b", 1}: 1,
	}, got)
}

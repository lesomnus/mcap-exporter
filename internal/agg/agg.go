// Package agg turns a stream of observed messages into delta-temporality
// counter data points, one per (topic, message-time bucket). Each point is
// stamped with the bucket's message-time window, so a downstream query computes
// Hz = count / bucket-width aligned to recording time rather than wall clock.
package agg

import (
	"context"
	"sort"
	"time"

	"github.com/lesomnus/mcap-exporter/internal/tail"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// MetricName is the single metric emitted; topic and schema are attributes.
const MetricName = "mcap.topic.message.count"

// Sink exports one batch of metrics, typically as a single OTLP request.
type Sink func(ctx context.Context, ms []metricdata.Metrics) error

type topicState struct {
	schema    string
	watermark int64           // max message time (unix nanos) observed
	counts    map[int64]int64 // bucket start (unix nanos) -> message count
}

// Aggregator buckets messages by message time and emits sealed buckets. It is
// not safe for concurrent use; drive it from a single goroutine via Run.
type Aggregator struct {
	bucket int64 // bucket width in nanos
	grace  int64 // extra slack (nanos) before a bucket is sealed

	recent    int64 // recent window in nanos; > 0 enables bounded export
	hasRecent bool
	anchored  bool  // once true, the emit floor is locked and emission proceeds
	minEmit   int64 // emit floor (unix nanos); buckets ending at/before are dropped
	wmax      int64 // global max message time (unix nanos) observed so far

	topics      map[string]*topicState
	lastEmitted map[string]int64 // topic -> last emitted bucket start (unix nanos)
	store       *Store           // checkpoint; may be nil
}

// New creates an Aggregator. When recent is non-nil, only buckets within that
// window of the latest observed message time are exported: the floor is locked
// to (latest message time − recent) at catch-up (see Run). When recent is nil,
// every bucket is exported. store and last carry the restart checkpoint.
func New(bucket, grace time.Duration, recent *time.Duration, store *Store, last map[string]int64) *Aggregator {
	if last == nil {
		last = map[string]int64{}
	}
	a := &Aggregator{
		bucket:      int64(bucket),
		grace:       int64(grace),
		anchored:    true, // no recent window => emit immediately
		topics:      map[string]*topicState{},
		lastEmitted: last,
		store:       store,
	}
	if recent != nil {
		a.recent = int64(*recent)
		a.hasRecent = true
		a.anchored = false // hold emission until the floor is anchored at catch-up
	}
	return a
}

// Run consumes messages from in and flushes sealed buckets every flush interval
// until in is closed or ctx is cancelled, then performs a final flush. When a
// recent window is configured, emission is held until caughtUp fires (the
// backlog has been read), at which point the floor is locked to the latest
// observed message time minus the window. A sink error is reported to onErr (if
// set) and does not stop the loop.
func (a *Aggregator) Run(ctx context.Context, in <-chan tail.Msg, caughtUp <-chan struct{}, flush time.Duration, sink Sink, onErr func(error)) error {
	ticker := time.NewTicker(flush)
	defer ticker.Stop()

	emit := func(final bool) {
		if err := a.flush(ctx, sink, final); err != nil && onErr != nil {
			onErr(err)
		}
	}
	cu := caughtUp
	for {
		select {
		case <-ctx.Done():
			emit(true)
			return nil
		case m, ok := <-in:
			if !ok {
				emit(true)
				return nil
			}
			a.observe(m)
		case <-cu:
			cu = nil // a closed channel is always ready; stop selecting it
			// Drain buffered backlog so the anchor reflects all data read so far.
			for drained := false; !drained; {
				select {
				case m, ok := <-in:
					if !ok {
						emit(true)
						return nil
					}
					a.observe(m)
				default:
					drained = true
				}
			}
			a.anchor()
			emit(false)
		case <-ticker.C:
			emit(false)
		}
	}
}

func (a *Aggregator) observe(m tail.Msg) {
	ts := m.Time.UnixNano()
	if ts > a.wmax {
		a.wmax = ts
	}
	st := a.topics[m.Topic]
	if st == nil {
		st = &topicState{counts: map[int64]int64{}}
		a.topics[m.Topic] = st
	}
	st.schema = m.Schema
	if ts > st.watermark {
		st.watermark = ts
	}
	b := ts - mod(ts, a.bucket)
	st.counts[b]++
}

// anchor locks the emit floor to (wmax − recent). Idempotent. Before anchoring,
// recent-mode buckets are retained (and pruned) but not emitted.
func (a *Aggregator) anchor() {
	if a.anchored {
		return
	}
	a.anchored = true
	if a.hasRecent {
		a.minEmit = a.wmax - a.recent
	}
}

// prune drops buckets that can never be emitted under the recent window so the
// pre-anchor backlog does not grow without bound.
func (a *Aggregator) prune() {
	floor := a.wmax - a.recent
	for _, st := range a.topics {
		for b := range st.counts {
			if b+a.bucket <= floor {
				delete(st.counts, b)
			}
		}
	}
}

// flush emits every sealed bucket (or, when final, every remaining bucket) as a
// delta data point and advances the per-topic checkpoint. A bucket is sealed
// once the topic's watermark has advanced past the bucket end plus grace.
func (a *Aggregator) flush(ctx context.Context, sink Sink, final bool) error {
	if a.hasRecent && !a.anchored {
		if !final {
			a.prune()
			return nil // still reading the backlog; hold emission
		}
		a.anchor() // shutting down before catch-up: lock the floor now and emit
	}

	dps := []metricdata.DataPoint[int64]{}
	for topic, st := range a.topics {
		sealed := make([]int64, 0, len(st.counts))
		for b := range st.counts {
			if final || st.watermark >= b+a.bucket+a.grace {
				sealed = append(sealed, b)
			}
		}
		sort.Slice(sealed, func(i, j int) bool { return sealed[i] < sealed[j] })

		last := a.lastEmitted[topic]
		for _, b := range sealed {
			count := st.counts[b]
			delete(st.counts, b)

			end := b + a.bucket
			if (a.minEmit > 0 && end <= a.minEmit) || b <= last {
				continue // before the emit floor, or already emitted in a prior run
			}
			dps = append(dps, metricdata.DataPoint[int64]{
				Attributes: attribute.NewSet(
					attribute.String("topic", topic),
					attribute.String("schema", st.schema),
				),
				StartTime: time.Unix(0, b),
				Time:      time.Unix(0, end),
				Value:     count,
			})
			last = b
		}
		a.lastEmitted[topic] = last
	}

	if len(dps) == 0 {
		return nil
	}

	err := sink(ctx, []metricdata.Metrics{{
		Name:        MetricName,
		Description: "Number of ROS2 messages observed on a topic within a message-time bucket.",
		Unit:        "1",
		Data: metricdata.Sum[int64]{
			Temporality: metricdata.DeltaTemporality,
			IsMonotonic: true,
			DataPoints:  dps,
		},
	}})
	if err != nil {
		return err
	}
	if a.store != nil {
		_ = a.store.Save(a.lastEmitted)
	}
	return nil
}

func mod(a, b int64) int64 {
	if b <= 0 {
		return 0
	}
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

package export

import (
	"context"
	"testing"
	"time"

	"github.com/lesomnus/mkot"
	"github.com/lesomnus/mkot/debug"
	"github.com/lesomnus/mkot/pretty"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type fakeExporter struct {
	last *metricdata.ResourceMetrics
}

func (f *fakeExporter) Temporality(metric.InstrumentKind) metricdata.Temporality {
	return metricdata.DeltaTemporality
}
func (f *fakeExporter) Aggregation(metric.InstrumentKind) metric.Aggregation {
	return metric.AggregationDefault{}
}
func (f *fakeExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	f.last = rm
	return nil
}
func (f *fakeExporter) ForceFlush(context.Context) error { return nil }
func (f *fakeExporter) Shutdown(context.Context) error   { return nil }

func TestBuild_ExtractsMetricExporter(t *testing.T) {
	cfg := &mkot.Config{Exporters: map[mkot.Id]mkot.ExporterConfig{
		"debug": debug.ExporterConfig{},
	}}
	exp, err := Build(context.Background(), cfg, "debug")
	require.NoError(t, err)
	require.NotNil(t, exp)
	_ = exp.Shutdown(context.Background())
}

func TestBuild_RejectsNonMetricExporter(t *testing.T) {
	cfg := &mkot.Config{Exporters: map[mkot.Id]mkot.ExporterConfig{
		"pretty": pretty.ExporterConfig{},
	}}
	_, err := Build(context.Background(), cfg, "pretty")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not support metric push")
}

func TestBuild_UnknownExporter(t *testing.T) {
	cfg := &mkot.Config{Exporters: map[mkot.Id]mkot.ExporterConfig{}}
	_, err := Build(context.Background(), cfg, "nope")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not declared")
}

func TestSink_TransmitsHistoricalTimestamp(t *testing.T) {
	fake := &fakeExporter{}
	sink := Sink(fake, Resource())

	start := time.Unix(1000, 0)
	end := time.Unix(1001, 0)
	ms := []metricdata.Metrics{{
		Name: "mcap.topic.message.count",
		Data: metricdata.Sum[int64]{
			Temporality: metricdata.DeltaTemporality,
			IsMonotonic: true,
			DataPoints: []metricdata.DataPoint[int64]{{
				StartTime: start,
				Time:      end,
				Value:     7,
			}},
		},
	}}
	require.NoError(t, sink(context.Background(), ms))

	require.NotNil(t, fake.last)
	require.Len(t, fake.last.ScopeMetrics, 1)
	require.Equal(t, scopeName, fake.last.ScopeMetrics[0].Scope.Name)
	dp := fake.last.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64]).DataPoints[0]
	require.Equal(t, start, dp.StartTime)
	require.Equal(t, end, dp.Time)
	require.Equal(t, int64(7), dp.Value)
}

func TestSink_EmptyIsNoop(t *testing.T) {
	fake := &fakeExporter{}
	require.NoError(t, Sink(fake, Resource())(context.Background(), nil))
	require.Nil(t, fake.last)
}

// Package export builds the metric exporter from the mkot configuration and
// pushes hand-built data points stamped with historical message times. It
// deliberately bypasses the mkot Resolver / MeterProvider: every instrument on
// that path is stamped by the SDK with collection-time "now", which is wrong
// for replaying recorded data. Instead it extracts the raw metric Exporter from
// the same YAML config and calls Exporter.Export directly.
package export

import (
	"context"
	"errors"
	"fmt"

	"github.com/lesomnus/mcap-exporter/cmd/version"
	"github.com/lesomnus/mkot"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

const scopeName = "github.com/lesomnus/mcap-exporter"

// Build extracts the raw metric Exporter declared as id under otel.exporters.
// The periodic-reader options returned alongside it (the live-sampling path)
// are discarded. The caller owns the returned exporter's lifecycle and must
// call Shutdown — it is not registered with the mkot Resolver.
func Build(ctx context.Context, cfg *mkot.Config, id string) (sdkmetric.Exporter, error) {
	if id == "" {
		return nil, errors.New("no exporter configured (set mcap.exporter to an otel.exporters id)")
	}
	ec, ok := cfg.Exporters[mkot.Id(id)]
	if !ok {
		return nil, fmt.Errorf("exporter %q is not declared under otel.exporters", id)
	}

	exp, _, err := ec.MetricExporter(ctx)
	if err != nil {
		if errors.Is(err, mkot.ErrUnimplemented) {
			return nil, fmt.Errorf("exporter %q does not support metric push; use an otlp or debug exporter", id)
		}
		return nil, fmt.Errorf("build metric exporter %q: %w", id, err)
	}
	return exp, nil
}

// Resource describes this service for the exported metrics.
func Resource() *resource.Resource {
	return resource.NewSchemaless(
		attribute.String("service.name", "mcap-exporter"),
		attribute.String("service.version", version.Get().Version),
	)
}

// Sink wraps each batch of metrics in a ResourceMetrics and pushes it through
// exp in a single Export call. The returned function satisfies agg.Sink.
func Sink(exp sdkmetric.Exporter, res *resource.Resource) func(ctx context.Context, ms []metricdata.Metrics) error {
	scope := instrumentation.Scope{Name: scopeName}
	return func(ctx context.Context, ms []metricdata.Metrics) error {
		if len(ms) == 0 {
			return nil
		}
		return exp.Export(ctx, &metricdata.ResourceMetrics{
			Resource: res,
			ScopeMetrics: []metricdata.ScopeMetrics{{
				Scope:   scope,
				Metrics: ms,
			}},
		})
	}
}

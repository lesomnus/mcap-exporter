package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lesomnus/mcap-exporter/internal/agg"
	"github.com/lesomnus/mcap-exporter/internal/export"
	"github.com/lesomnus/mcap-exporter/internal/tail"
	"github.com/lesomnus/otx/log"
	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/arg"
	"golang.org/x/sync/errgroup"
)

func NewCmdWatch() *xli.Command {
	return &xli.Command{
		Name:  "watch",
		Brief: "watch MCAP files and export ROS2 topic message-rate metrics",

		Args: arg.Args{
			&arg.RestStrings{
				Name:  "paths",
				Brief: "MCAP files or directories to watch (overrides config)",
			},
		},

		Handler: xli.OnRun(func(ctx context.Context, cmd *xli.Command, next xli.Next) error {
			c := use_config.Must(ctx)
			l := log.From(ctx)

			paths := c.Mcap.Paths
			if v, ok := arg.Get[[]string](cmd, "paths"); ok && len(v) > 0 {
				paths = v
			}
			if len(paths) == 0 {
				return fmt.Errorf("no paths to watch (pass paths as arguments or set mcap.paths)")
			}

			exp, err := export.Build(ctx, &c.Otel.Config, c.Mcap.Exporter)
			if err != nil {
				return fmt.Errorf("build exporter: %w", err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				defer cancel()
				if err := exp.Shutdown(ctx); err != nil {
					l.Warn("exporter shutdown", slog.String("error", err.Error()))
				}
			}()

			store, last, err := agg.LoadStore(c.Mcap.StatePath)
			if err != nil {
				return fmt.Errorf("load state %q: %w", c.Mcap.StatePath, err)
			}

			var recent *time.Duration
			if c.Mcap.Recent != nil {
				d := c.Mcap.Recent.D()
				recent = &d
			}

			aggr := agg.New(c.Mcap.Bucket.D(), c.Mcap.Grace.D(), recent, store, last)
			sink := export.Sink(exp, export.Resource())

			l.Info("watching",
				slog.Any("paths", paths),
				slog.String("exporter", c.Mcap.Exporter),
				slog.Duration("bucket", c.Mcap.Bucket.D()),
				slog.Duration("flush_interval", c.Mcap.FlushInterval.D()),
				slog.Any("recent", recent),
			)

			ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			out := make(chan tail.Msg, 1024)
			caughtUp := make(chan struct{})
			eg, ctx := errgroup.WithContext(ctx)
			eg.Go(func() error {
				defer close(out)
				return tail.TailMany(ctx, paths, c.Mcap.HeaderSet(), c.Mcap.PollInterval.D(), out, func() { close(caughtUp) })
			})
			eg.Go(func() error {
				return aggr.Run(ctx, out, caughtUp, c.Mcap.FlushInterval.D(), sink, func(err error) {
					l.Warn("export batch failed", slog.String("error", err.Error()))
				})
			})

			return eg.Wait()
		}),
	}
}

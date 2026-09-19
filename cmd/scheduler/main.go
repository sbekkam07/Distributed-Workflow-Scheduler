// Command scheduler performs leader-only workflow coordination.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/config"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/observability"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/scheduler"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load configuration", "error", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.OpenPool(ctx, cfg.DatabaseURL, cfg.DatabaseMaxConns)
	if err != nil {
		slog.Error("connect to PostgreSQL", "error", err)
		return
	}
	defer pool.Close()
	metrics := observability.New()
	metricsServer := observability.NewServer(cfg.SchedulerMetricsAddress, metrics.Handler())
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("scheduler metrics server failed", "error", err)
		}
	}()
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownContext); err != nil {
			slog.Error("scheduler metrics shutdown", "error", err)
		}
	}()

	elector := postgres.NewSchedulerElector(pool)
	s, err := scheduler.New(scheduler.ElectorFunc(func(ctx context.Context) (scheduler.Leadership, bool, error) {
		leadership, acquired, err := elector.TryAcquire(ctx)
		return leadership, acquired, err
	}), cfg.SchedulerPollInterval, slog.Default(), metrics)
	if err != nil {
		slog.Error("configure scheduler", "error", err)
		return
	}

	slog.Info("scheduler started", "poll_interval", cfg.SchedulerPollInterval, "metrics_addr", cfg.SchedulerMetricsAddress)
	if err := s.Run(ctx); err != nil {
		slog.Error("scheduler stopped with error", "error", err)
		return
	}
	slog.Info("scheduler stopped")
}

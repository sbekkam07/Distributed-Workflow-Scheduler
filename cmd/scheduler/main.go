// Command scheduler performs leader-only workflow coordination.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/config"
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

	elector := postgres.NewSchedulerElector(pool)
	s, err := scheduler.New(scheduler.ElectorFunc(func(ctx context.Context) (scheduler.Leadership, bool, error) {
		leadership, acquired, err := elector.TryAcquire(ctx)
		return leadership, acquired, err
	}), cfg.SchedulerPollInterval, slog.Default())
	if err != nil {
		slog.Error("configure scheduler", "error", err)
		return
	}

	slog.Info("scheduler started", "poll_interval", cfg.SchedulerPollInterval)
	if err := s.Run(ctx); err != nil {
		slog.Error("scheduler stopped with error", "error", err)
		return
	}
	slog.Info("scheduler stopped")
}

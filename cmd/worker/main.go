// Command worker will poll for and execute queued jobs.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/config"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/worker"
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

	w, err := worker.New(
		postgres.NewJobRepository(pool),
		worker.NewEchoExecutor(slog.Default()),
		cfg.WorkerPollInterval,
		slog.Default(),
	)
	if err != nil {
		slog.Error("configure worker", "error", err)
		return
	}

	slog.Info("worker started", "poll_interval", cfg.WorkerPollInterval)
	if err := w.Run(ctx); err != nil {
		slog.Error("worker stopped with error", "error", err)
		return
	}
	slog.Info("worker stopped")
}

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
	workerID := cfg.WorkerID
	if workerID == "" {
		workerID, err = worker.NewID()
		if err != nil {
			slog.Error("generate worker ID", "error", err)
			return
		}
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
		workerID,
		cfg.WorkerPollInterval,
		cfg.WorkerLeaseDuration,
		cfg.WorkerHeartbeatInterval,
		cfg.WorkerRetryBackoffBase,
		cfg.WorkerRetryBackoffMax,
		slog.Default(),
	)
	if err != nil {
		slog.Error("configure worker", "error", err)
		return
	}

	slog.Info(
		"worker started",
		"worker_id", workerID,
		"poll_interval", cfg.WorkerPollInterval,
		"lease_duration", cfg.WorkerLeaseDuration,
		"heartbeat_interval", cfg.WorkerHeartbeatInterval,
		"retry_backoff_base", cfg.WorkerRetryBackoffBase,
		"retry_backoff_max", cfg.WorkerRetryBackoffMax,
	)
	if err := w.Run(ctx); err != nil {
		slog.Error("worker stopped with error", "error", err)
		return
	}
	slog.Info("worker stopped")
}

package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
)

const shutdownFinalizationTimeout = 5 * time.Second

// Store is the persistence boundary used by a worker.
type Store interface {
	ClaimNext(context.Context) (jobs.Job, bool, error)
	MarkSucceeded(context.Context, string) error
	MarkFailed(context.Context, string, string) error
}

// Executor performs a claimed job outside the database claim operation.
type Executor interface {
	Execute(context.Context, jobs.Job) error
}

// Worker polls for and executes jobs one at a time.
type Worker struct {
	store        Store
	executor     Executor
	pollInterval time.Duration
	logger       *slog.Logger
}

// New constructs a single-worker Phase 1 polling loop.
func New(store Store, executor Executor, pollInterval time.Duration, logger *slog.Logger) (*Worker, error) {
	if store == nil {
		return nil, fmt.Errorf("worker store is required")
	}
	if executor == nil {
		return nil, fmt.Errorf("worker executor is required")
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("worker poll interval must be positive")
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Worker{
		store:        store,
		executor:     executor,
		pollInterval: pollInterval,
		logger:       logger,
	}, nil
}

// Run polls until ctx is canceled. Database errors are logged and retried after
// the polling interval so a temporary outage does not create a busy loop.
func (w *Worker) Run(ctx context.Context) error {
	for {
		claimed, err := w.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Error("worker iteration failed", "error", err)
		}
		if claimed {
			continue
		}

		timer := time.NewTimer(w.pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

// RunOnce claims and executes at most one job. A job execution error is an
// expected job outcome, so it is persisted as FAILED rather than returned as a
// worker-process error.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	job, claimed, err := w.store.ClaimNext(ctx)
	if err != nil || !claimed {
		return claimed, err
	}

	w.logger.Info("claimed job", "job_id", job.ID, "kind", job.Kind)
	executionErr := w.executor.Execute(ctx, job)
	finalizationContext, cancel := finalizationContext(ctx)
	defer cancel()

	if executionErr != nil {
		if err := w.store.MarkFailed(finalizationContext, job.ID, executionErr.Error()); err != nil {
			return true, fmt.Errorf("persist job failure: %w", err)
		}
		w.logger.Warn("job failed", "job_id", job.ID, "error", executionErr)
		return true, nil
	}

	if err := w.store.MarkSucceeded(finalizationContext, job.ID); err != nil {
		return true, fmt.Errorf("persist job success: %w", err)
	}
	w.logger.Info("job succeeded", "job_id", job.ID)
	return true, nil
}

func finalizationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.Background(), shutdownFinalizationTimeout)
}

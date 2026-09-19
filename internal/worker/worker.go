package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
)

const shutdownFinalizationTimeout = 5 * time.Second

// Store is the persistence boundary used by a worker.
type Store interface {
	RecoverExpired(context.Context) (int64, error)
	ClaimNext(context.Context, string, time.Duration) (jobs.Job, bool, error)
	RenewLease(context.Context, string, string, time.Duration) (bool, error)
	MarkSucceeded(context.Context, string, string) error
	RecordFailure(context.Context, string, string, string, bool, time.Duration) (jobs.Status, error)
}

// Executor performs a claimed job outside the database claim operation.
type Executor interface {
	Execute(context.Context, jobs.Job) error
}

// Observer receives process-local operational measurements. Implementations
// must not affect job execution when metrics collection is unavailable.
type Observer interface {
	JobClaimed(createdAt, claimedAt time.Time)
	JobFinished(outcome string, duration time.Duration)
	ActiveWorker(active bool)
}

// Worker polls for and executes jobs one at a time.
type Worker struct {
	store             Store
	executor          Executor
	pollInterval      time.Duration
	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	retryBackoffBase  time.Duration
	retryBackoffMax   time.Duration
	workerID          string
	logger            *slog.Logger
	observer          Observer
}

// New constructs a worker with a renewable lease. workerID must be unique among
// concurrently running workers so lease ownership is unambiguous.
func New(
	store Store,
	executor Executor,
	workerID string,
	pollInterval, leaseDuration, heartbeatInterval, retryBackoffBase, retryBackoffMax time.Duration,
	logger *slog.Logger,
	observers ...Observer,
) (*Worker, error) {
	if store == nil {
		return nil, fmt.Errorf("worker store is required")
	}
	if executor == nil {
		return nil, fmt.Errorf("worker executor is required")
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("worker poll interval must be positive")
	}
	if workerID == "" {
		return nil, fmt.Errorf("worker ID is required")
	}
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("worker lease duration must be positive")
	}
	if heartbeatInterval <= 0 || heartbeatInterval >= leaseDuration {
		return nil, fmt.Errorf("worker heartbeat interval must be positive and shorter than lease duration")
	}
	if retryBackoffBase <= 0 {
		return nil, fmt.Errorf("worker retry backoff base must be positive")
	}
	if retryBackoffMax < retryBackoffBase {
		return nil, fmt.Errorf("worker retry backoff max must be at least retry backoff base")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if len(observers) > 1 {
		return nil, fmt.Errorf("at most one worker observer is supported")
	}
	var observer Observer
	if len(observers) == 1 {
		observer = observers[0]
	}

	return &Worker{
		store:             store,
		executor:          executor,
		pollInterval:      pollInterval,
		leaseDuration:     leaseDuration,
		heartbeatInterval: heartbeatInterval,
		retryBackoffBase:  retryBackoffBase,
		retryBackoffMax:   retryBackoffMax,
		workerID:          workerID,
		logger:            logger,
		observer:          observer,
	}, nil
}

// Run polls until ctx is canceled. Database errors are logged and retried after
// the polling interval so a temporary outage does not create a busy loop.
func (w *Worker) Run(ctx context.Context) error {
	if w.observer != nil {
		w.observer.ActiveWorker(true)
		defer w.observer.ActiveWorker(false)
	}
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
	recovered, err := w.store.RecoverExpired(ctx)
	if err != nil {
		return false, fmt.Errorf("recover expired leases: %w", err)
	}
	if recovered > 0 {
		w.logger.Warn("recovered expired job leases", "count", recovered)
	}

	job, claimed, err := w.store.ClaimNext(ctx, w.workerID, w.leaseDuration)
	if err != nil || !claimed {
		return claimed, err
	}

	w.logger.Info("claimed job", "job_id", job.ID, "kind", job.Kind, "effect_key", job.EffectKey(), "lease_owner", w.workerID)
	claimedAt := time.Now()
	if w.observer != nil {
		w.observer.JobClaimed(job.CreatedAt, claimedAt)
	}
	executionErr := w.executeWithHeartbeats(ctx, job)
	executionDuration := time.Since(claimedAt)
	finalizationContext, cancel := finalizationContext(ctx)
	defer cancel()

	if executionErr != nil {
		retryable := isRetryable(executionErr)
		retryDelay := time.Duration(0)
		if retryable {
			retryDelay = exponentialBackoff(w.retryBackoffBase, w.retryBackoffMax, job.AttemptCount)
		}
		outcome, err := w.store.RecordFailure(finalizationContext, job.ID, w.workerID, executionErr.Error(), retryable, retryDelay)
		if err != nil {
			if errors.Is(err, jobs.ErrLeaseLost) {
				w.logger.Warn("could not persist job failure after lease loss", "job_id", job.ID)
				return true, nil
			}
			return true, fmt.Errorf("persist job failure: %w", err)
		}
		switch outcome {
		case jobs.StatusQueued:
			w.observeFinished("retried", executionDuration)
			w.logger.Warn("job retry scheduled", "job_id", job.ID, "attempt", job.AttemptCount, "backoff", retryDelay, "error", executionErr)
		case jobs.StatusDead:
			w.observeFinished("dead", executionDuration)
			w.logger.Error("job dead-lettered", "job_id", job.ID, "attempt", job.AttemptCount, "error", executionErr)
		default:
			w.observeFinished("failed", executionDuration)
			w.logger.Warn("job failed", "job_id", job.ID, "error", executionErr)
		}
		return true, nil
	}

	if err := w.store.MarkSucceeded(finalizationContext, job.ID, w.workerID); err != nil {
		if errors.Is(err, jobs.ErrLeaseLost) {
			w.logger.Warn("could not persist job success after lease loss", "job_id", job.ID)
			return true, nil
		}
		return true, fmt.Errorf("persist job success: %w", err)
	}
	w.observeFinished("succeeded", executionDuration)
	w.logger.Info("job succeeded", "job_id", job.ID)
	return true, nil
}

func (w *Worker) observeFinished(outcome string, duration time.Duration) {
	if w.observer != nil {
		w.observer.JobFinished(outcome, duration)
	}
}

func (w *Worker) executeWithHeartbeats(ctx context.Context, job jobs.Job) error {
	executionContext, cancelExecution := context.WithCancel(ctx)
	defer cancelExecution()

	heartbeatError := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.heartbeat(executionContext, job.ID, heartbeatError, cancelExecution)
	}()

	executionErr := w.executor.Execute(executionContext, job)
	cancelExecution()
	<-done

	select {
	case err := <-heartbeatError:
		return err
	default:
		return executionErr
	}
}

func (w *Worker) heartbeat(ctx context.Context, jobID string, heartbeatError chan<- error, cancelExecution context.CancelFunc) {
	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewed, err := w.store.RenewLease(ctx, jobID, w.workerID, w.leaseDuration)
			if err != nil {
				heartbeatError <- fmt.Errorf("renew job lease: %w", err)
				cancelExecution()
				return
			}
			if !renewed {
				heartbeatError <- jobs.ErrLeaseLost
				cancelExecution()
				return
			}
			w.logger.Debug("renewed job lease", "job_id", jobID, "lease_owner", w.workerID)
		}
	}
}

func finalizationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.Background(), shutdownFinalizationTimeout)
}

package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
)

type fakeStore struct {
	mu sync.Mutex

	job              jobs.Job
	claimed          bool
	claimErr         error
	recovered        int64
	recoverErr       error
	renewed          bool
	renewErr         error
	renewedSignal    chan struct{}
	succeededJobID   string
	succeededOwner   string
	failedJobID      string
	failedOwner      string
	failureMessage   string
	failureRetryable bool
	failureDelay     time.Duration
	failureOutcome   jobs.Status
	markSuccessErr   error
	markFailureErr   error
}

func (s *fakeStore) RecoverExpired(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recovered, s.recoverErr
}

func (s *fakeStore) ClaimNext(_ context.Context, _ string, _ time.Duration) (jobs.Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.job, s.claimed, s.claimErr
}

func (s *fakeStore) RenewLease(_ context.Context, _ string, _ string, _ time.Duration) (bool, error) {
	s.mu.Lock()
	renewed, err, signal := s.renewed, s.renewErr, s.renewedSignal
	s.mu.Unlock()
	if signal != nil {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
	return renewed, err
}

func (s *fakeStore) MarkSucceeded(_ context.Context, id, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.succeededJobID = id
	s.succeededOwner = owner
	return s.markSuccessErr
}

func (s *fakeStore) RecordFailure(_ context.Context, id, owner, message string, retryable bool, retryDelay time.Duration) (jobs.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedJobID = id
	s.failedOwner = owner
	s.failureMessage = message
	s.failureRetryable = retryable
	s.failureDelay = retryDelay
	if s.failureOutcome == "" {
		s.failureOutcome = jobs.StatusFailed
	}
	return s.failureOutcome, s.markFailureErr
}

type fakeExecutor struct {
	err      error
	executed jobs.Job
}

func (e *fakeExecutor) Execute(_ context.Context, job jobs.Job) error {
	e.executed = job
	return e.err
}

type blockingExecutor struct {
	started chan struct{}
}

func (e *blockingExecutor) Execute(ctx context.Context, _ jobs.Job) error {
	close(e.started)
	<-ctx.Done()
	return ctx.Err()
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestWorker(t *testing.T, store Store, executor Executor, heartbeatInterval time.Duration) *Worker {
	t.Helper()
	worker, err := New(store, executor, "worker-test", time.Second, time.Second, heartbeatInterval, time.Second, time.Minute, testLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return worker
}

func TestRunOnceSchedulesRetryableExecutionFailure(t *testing.T) {
	store := &fakeStore{
		job:            jobs.Job{ID: "job-retry", Kind: "echo", AttemptCount: 2},
		claimed:        true,
		failureOutcome: jobs.StatusQueued,
	}
	executor := &fakeExecutor{err: Retryable(errors.New("temporary outage"))}
	worker := newTestWorker(t, store, executor, 100*time.Millisecond)

	claimed, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !claimed || !store.failureRetryable || store.failureDelay != 2*time.Second || store.failureOutcome != jobs.StatusQueued {
		t.Errorf("RunOnce() retry = %+v, want retryable queued job with 2s backoff", store)
	}
}

func TestRunOnceMarksSuccessfulExecution(t *testing.T) {
	store := &fakeStore{job: jobs.Job{ID: "job-1", Kind: "echo"}, claimed: true}
	executor := &fakeExecutor{}
	worker := newTestWorker(t, store, executor, 100*time.Millisecond)

	claimed, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !claimed || executor.executed.ID != "job-1" || store.succeededJobID != "job-1" || store.succeededOwner != "worker-test" {
		t.Errorf("RunOnce() did not execute and mark job succeeded: %+v", store)
	}
	if store.failedJobID != "" {
		t.Errorf("failed job ID = %q, want empty", store.failedJobID)
	}
}

func TestExponentialBackoffCapsAtMaximum(t *testing.T) {
	if got := exponentialBackoff(time.Second, 10*time.Second, 1); got != time.Second {
		t.Errorf("attempt 1 backoff = %s, want 1s", got)
	}
	if got := exponentialBackoff(time.Second, 10*time.Second, 4); got != 8*time.Second {
		t.Errorf("attempt 4 backoff = %s, want 8s", got)
	}
	if got := exponentialBackoff(time.Second, 10*time.Second, 8); got != 10*time.Second {
		t.Errorf("attempt 8 backoff = %s, want 10s", got)
	}
}

func TestRunOnceMarksExecutionFailure(t *testing.T) {
	store := &fakeStore{job: jobs.Job{ID: "job-2", Kind: "echo"}, claimed: true}
	executor := &fakeExecutor{err: errors.New("echo failed")}
	worker := newTestWorker(t, store, executor, 100*time.Millisecond)

	claimed, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !claimed || store.failedJobID != "job-2" || store.failedOwner != "worker-test" || store.failureMessage != "echo failed" {
		t.Errorf("RunOnce() did not persist execution failure: %+v", store)
	}
}

func TestLeaseLossCancelsExecution(t *testing.T) {
	store := &fakeStore{
		job:           jobs.Job{ID: "job-3", Kind: "echo"},
		claimed:       true,
		renewed:       false,
		renewedSignal: make(chan struct{}, 1),
	}
	executor := &blockingExecutor{started: make(chan struct{})}
	worker := newTestWorker(t, store, executor, time.Millisecond)

	finished := make(chan error, 1)
	go func() {
		_, err := worker.RunOnce(context.Background())
		finished <- err
	}()

	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("executor did not start")
	}
	select {
	case <-store.renewedSignal:
	case <-time.After(time.Second):
		t.Fatal("worker did not send a heartbeat")
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("RunOnce() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after lease loss")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.failedJobID != "job-3" {
		t.Errorf("failed job ID = %q, want job-3", store.failedJobID)
	}
}

func TestRunReturnsOnCancellation(t *testing.T) {
	worker := newTestWorker(t, &fakeStore{}, &fakeExecutor{}, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := worker.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

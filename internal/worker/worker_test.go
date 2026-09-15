package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
)

type fakeStore struct {
	job            jobs.Job
	claimed        bool
	claimErr       error
	succeededJobID string
	failedJobID    string
	failureMessage string
	markSuccessErr error
	markFailureErr error
}

func (s *fakeStore) ClaimNext(context.Context) (jobs.Job, bool, error) {
	return s.job, s.claimed, s.claimErr
}

func (s *fakeStore) MarkSucceeded(_ context.Context, id string) error {
	s.succeededJobID = id
	return s.markSuccessErr
}

func (s *fakeStore) MarkFailed(_ context.Context, id, message string) error {
	s.failedJobID = id
	s.failureMessage = message
	return s.markFailureErr
}

type fakeExecutor struct {
	err      error
	executed jobs.Job
}

func (e *fakeExecutor) Execute(_ context.Context, job jobs.Job) error {
	e.executed = job
	return e.err
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunOnceMarksSuccessfulExecution(t *testing.T) {
	store := &fakeStore{job: jobs.Job{ID: "job-1", Kind: "echo"}, claimed: true}
	executor := &fakeExecutor{}
	worker, err := New(store, executor, time.Second, testLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	claimed, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if !claimed || executor.executed.ID != "job-1" || store.succeededJobID != "job-1" {
		t.Errorf("RunOnce() did not execute and mark job succeeded: %+v", store)
	}
	if store.failedJobID != "" {
		t.Errorf("failed job ID = %q, want empty", store.failedJobID)
	}
}

func TestRunOnceMarksExecutionFailure(t *testing.T) {
	store := &fakeStore{job: jobs.Job{ID: "job-2", Kind: "echo"}, claimed: true}
	executor := &fakeExecutor{err: errors.New("echo failed")}
	worker, err := New(store, executor, time.Second, testLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	claimed, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if !claimed || store.failedJobID != "job-2" || store.failureMessage != "echo failed" {
		t.Errorf("RunOnce() did not persist execution failure: %+v", store)
	}
}

func TestRunReturnsOnCancellation(t *testing.T) {
	worker, err := New(&fakeStore{}, &fakeExecutor{}, time.Hour, testLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := worker.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

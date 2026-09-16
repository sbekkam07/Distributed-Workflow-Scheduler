package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
)

// TestRetryBecomesEligibleThenDeadLetters proves that a retry waits in durable
// queue state rather than occupying a worker, then reaches DEAD at its attempt
// budget. It moves next_attempt_at directly instead of sleeping.
func TestRetryBecomesEligibleThenDeadLetters(t *testing.T) {
	if integrationDisabled(t) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	temporaryURL, cleanup := createTemporaryDatabase(t, ctx)
	defer cleanup()

	if err := postgres.MigrateUp(ctx, temporaryURL); err != nil {
		t.Fatalf("MigrateUp() error = %v", err)
	}
	pool, err := postgres.OpenPool(ctx, temporaryURL, 2)
	if err != nil {
		t.Fatalf("OpenPool() error = %v", err)
	}
	defer pool.Close()
	repository := postgres.NewJobRepository(pool)

	job, err := jobs.NewWithMaxAttempts("echo", json.RawMessage(`{"message":"retry me"}`), 2, time.Now())
	if err != nil {
		t.Fatalf("NewWithMaxAttempts() error = %v", err)
	}
	created, err := repository.Create(ctx, job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	first, claimed, err := repository.ClaimNext(ctx, "retry-worker", 10*time.Second)
	if err != nil || !claimed || first.AttemptCount != 1 {
		t.Fatalf("first ClaimNext() = %+v, %t, %v", first, claimed, err)
	}
	outcome, err := repository.RecordFailure(ctx, created.ID, "retry-worker", "temporary outage", true, time.Hour)
	if err != nil || outcome != jobs.StatusQueued {
		t.Fatalf("first RecordFailure() = %s, %v, want QUEUED", outcome, err)
	}
	queued, err := repository.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get() after retry = %v", err)
	}
	if queued.Status != jobs.StatusQueued || queued.NextAttemptAt == nil || queued.AttemptCount != 1 || queued.LeaseOwner != nil {
		t.Errorf("queued retry = %+v, want ineligible queued job with one attempt", queued)
	}
	if _, claimed, err := repository.ClaimNext(ctx, "other-worker", 10*time.Second); err != nil || claimed {
		t.Fatalf("ClaimNext() before retry eligibility = claimed %t, err %v", claimed, err)
	}

	if _, err := pool.Exec(ctx, `UPDATE jobs SET next_attempt_at = now() - INTERVAL '1 microsecond' WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("make retry eligible: %v", err)
	}
	second, claimed, err := repository.ClaimNext(ctx, "retry-worker", 10*time.Second)
	if err != nil || !claimed || second.ID != created.ID || second.AttemptCount != 2 {
		t.Fatalf("second ClaimNext() = %+v, %t, %v", second, claimed, err)
	}
	outcome, err = repository.RecordFailure(ctx, created.ID, "retry-worker", "temporary outage", true, time.Hour)
	if err != nil || outcome != jobs.StatusDead {
		t.Fatalf("second RecordFailure() = %s, %v, want DEAD", outcome, err)
	}
	dead, err := repository.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get() after dead letter = %v", err)
	}
	if dead.Status != jobs.StatusDead || dead.AttemptCount != dead.MaxAttempts || dead.NextAttemptAt != nil || dead.ErrorMessage == nil {
		t.Errorf("dead-lettered job = %+v, want terminal DEAD job", dead)
	}
}

func TestNonRetryableFailureIsFailedImmediately(t *testing.T) {
	if integrationDisabled(t) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	temporaryURL, cleanup := createTemporaryDatabase(t, ctx)
	defer cleanup()

	if err := postgres.MigrateUp(ctx, temporaryURL); err != nil {
		t.Fatalf("MigrateUp() error = %v", err)
	}
	pool, err := postgres.OpenPool(ctx, temporaryURL, 2)
	if err != nil {
		t.Fatalf("OpenPool() error = %v", err)
	}
	defer pool.Close()
	repository := postgres.NewJobRepository(pool)

	job, err := jobs.New("echo", json.RawMessage(`{"message":"fail me"}`), time.Now())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	created, err := repository.Create(ctx, job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, claimed, err := repository.ClaimNext(ctx, "worker", 10*time.Second); err != nil || !claimed {
		t.Fatalf("ClaimNext() = claimed %t, err %v", claimed, err)
	}
	outcome, err := repository.RecordFailure(ctx, created.ID, "worker", "invalid request", false, 0)
	if err != nil || outcome != jobs.StatusFailed {
		t.Fatalf("RecordFailure() = %s, %v, want FAILED", outcome, err)
	}
}

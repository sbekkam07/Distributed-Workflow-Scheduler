package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
)

// TestExpiredLeaseIsRecoveredWithoutSleeping simulates a worker crash by
// directly expiring its lease in an isolated database. This avoids flaky
// wall-clock sleeps while exercising PostgreSQL's recovery behavior.
func TestExpiredLeaseIsRecoveredWithoutSleeping(t *testing.T) {
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

	job, err := jobs.New("echo", json.RawMessage(`{"message":"recover me"}`), time.Now())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	created, err := repository.Create(ctx, job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	claimed, ok, err := repository.ClaimNext(ctx, "crashed-worker", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("ClaimNext() = %+v, %t, %v", claimed, ok, err)
	}
	if claimed.LeaseOwner == nil || *claimed.LeaseOwner != "crashed-worker" || claimed.LeaseExpiresAt == nil || claimed.LastHeartbeatAt == nil {
		t.Errorf("claim did not establish lease fields: %+v", claimed)
	}

	renewed, err := repository.RenewLease(ctx, created.ID, "crashed-worker", 10*time.Second)
	if err != nil || !renewed {
		t.Fatalf("RenewLease() = %t, %v", renewed, err)
	}
	if recovered, err := repository.RecoverExpired(ctx); err != nil || recovered != 0 {
		t.Fatalf("RecoverExpired() before expiry = %d, %v", recovered, err)
	}

	if _, err := pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = now() - INTERVAL '1 microsecond' WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("expire lease directly: %v", err)
	}
	recovered, err := repository.RecoverExpired(ctx)
	if err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() after expiry = %d, %v", recovered, err)
	}

	reclaimed, ok, err := repository.ClaimNext(ctx, "replacement-worker", 10*time.Second)
	if err != nil || !ok || reclaimed.ID != created.ID {
		t.Fatalf("replacement ClaimNext() = %+v, %t, %v", reclaimed, ok, err)
	}
	if reclaimed.LeaseOwner == nil || *reclaimed.LeaseOwner != "replacement-worker" {
		t.Errorf("reclaimed lease owner = %v, want replacement-worker", reclaimed.LeaseOwner)
	}
	if err := repository.MarkSucceeded(ctx, created.ID, "crashed-worker"); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Errorf("stale worker MarkSucceeded() error = %v, want %v", err, jobs.ErrLeaseLost)
	}
	if err := repository.MarkSucceeded(ctx, created.ID, "replacement-worker"); err != nil {
		t.Fatalf("replacement MarkSucceeded() error = %v", err)
	}
	completed, err := repository.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if completed.Status != jobs.StatusSucceeded || completed.LeaseOwner != nil || completed.LeaseExpiresAt != nil || completed.LastHeartbeatAt != nil {
		t.Errorf("completed job = %+v, want succeeded job with cleared lease", completed)
	}
}

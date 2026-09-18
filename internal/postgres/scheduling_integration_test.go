package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
)

// TestFutureJobIsNotClaimedEarly proves run_at is a database-enforced
// eligibility bound. It makes the row due directly instead of sleeping.
func TestFutureJobIsNotClaimedEarly(t *testing.T) {
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

	job, err := jobs.NewWithSchedule("echo", json.RawMessage(`{"message":"later"}`), 3, jobs.PriorityHigh, time.Now().Add(time.Hour), time.Now())
	if err != nil {
		t.Fatalf("NewWithSchedule() error = %v", err)
	}
	created, err := repository.Create(ctx, job)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, claimed, err := repository.ClaimNext(ctx, "schedule-worker", 10*time.Second); err != nil || claimed {
		t.Fatalf("ClaimNext() before run_at = claimed %t, err %v", claimed, err)
	}

	if _, err := pool.Exec(ctx, `UPDATE jobs SET run_at = now() - INTERVAL '1 microsecond' WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("make job due: %v", err)
	}
	claimed, ok, err := repository.ClaimNext(ctx, "schedule-worker", 10*time.Second)
	if err != nil || !ok || claimed.ID != created.ID {
		t.Fatalf("ClaimNext() after run_at = %+v, %t, %v", claimed, ok, err)
	}
}

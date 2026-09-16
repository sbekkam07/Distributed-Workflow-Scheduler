package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
)

// TestClaimNextUsesStrictPriorityOrdering proves that an eligible high-priority
// job is claimed before older normal and low work. It intentionally documents
// the Phase 6 starvation trade-off rather than promising fairness.
func TestClaimNextUsesStrictPriorityOrdering(t *testing.T) {
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

	created := make(map[jobs.Priority]jobs.Job, 3)
	for _, priority := range []jobs.Priority{jobs.PriorityLow, jobs.PriorityNormal, jobs.PriorityHigh} {
		job, err := jobs.NewWithOptions("echo", json.RawMessage(`{"message":"priority"}`), 3, priority, time.Now())
		if err != nil {
			t.Fatalf("NewWithOptions(%s): %v", priority, err)
		}
		persisted, err := repository.Create(ctx, job)
		if err != nil {
			t.Fatalf("Create(%s): %v", priority, err)
		}
		created[priority] = persisted
	}

	for _, want := range []jobs.Priority{jobs.PriorityHigh, jobs.PriorityNormal, jobs.PriorityLow} {
		claimed, ok, err := repository.ClaimNext(ctx, "priority-worker", 10*time.Second)
		if err != nil || !ok {
			t.Fatalf("ClaimNext() = %+v, %t, %v", claimed, ok, err)
		}
		if claimed.ID != created[want].ID || claimed.Priority != want {
			t.Fatalf("claimed job = %s/%s, want %s/%s", claimed.ID, claimed.Priority, created[want].ID, want)
		}
		if err := repository.MarkSucceeded(ctx, claimed.ID, "priority-worker"); err != nil {
			t.Fatalf("MarkSucceeded(%s): %v", want, err)
		}
	}
}

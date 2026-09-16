package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
)

func TestIdempotencyKeyCreatesOneJobAndRejectsConflict(t *testing.T) {
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

	job, err := jobs.New("echo", json.RawMessage(`{"message":"once"}`), time.Now())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	job, err = job.WithIdempotencyKey("client-operation-9")
	if err != nil {
		t.Fatalf("WithIdempotencyKey() error = %v", err)
	}
	type result struct {
		job     jobs.Job
		created bool
		err     error
	}
	const callers = 4
	start := make(chan struct{})
	results := make(chan result, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			persisted, created, err := repository.CreateOrGet(ctx, job)
			results <- result{job: persisted, created: created, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	ids := make(map[string]struct{}, callers)
	createdCount := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent CreateOrGet() error = %v", result.err)
		}
		ids[result.job.ID] = struct{}{}
		if result.created {
			createdCount++
		}
	}
	if len(ids) != 1 || createdCount != 1 {
		t.Fatalf("concurrent idempotent creates produced IDs %v and %d creators, want one of each", ids, createdCount)
	}

	conflict, err := jobs.New("echo", json.RawMessage(`{"message":"different"}`), time.Now())
	if err != nil {
		t.Fatalf("New conflict job: %v", err)
	}
	conflict, err = conflict.WithIdempotencyKey("client-operation-9")
	if err != nil {
		t.Fatalf("WithIdempotencyKey() error = %v", err)
	}
	if _, _, err := repository.CreateOrGet(ctx, conflict); !errors.Is(err, jobs.ErrIdempotencyConflict) {
		t.Errorf("conflicting CreateOrGet() error = %v, want %v", err, jobs.ErrIdempotencyConflict)
	}
}

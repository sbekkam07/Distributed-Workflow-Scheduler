package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
)

func TestSchedulerLeadershipIsExclusiveAndFailsOver(t *testing.T) {
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

	firstElector := postgres.NewSchedulerElector(pool)
	secondElector := postgres.NewSchedulerElector(pool)
	first, acquired, err := firstElector.TryAcquire(ctx)
	if err != nil || !acquired {
		t.Fatalf("first TryAcquire() = %v, %t, want leadership", err, acquired)
	}
	defer first.Release(context.Background())
	if blocked, err := first.ResolveBlocked(ctx); err != nil || blocked != 0 {
		t.Fatalf("leader ResolveBlocked() = %d, %v; want 0, nil", blocked, err)
	}

	if second, acquired, err := secondElector.TryAcquire(ctx); err != nil || acquired || second != nil {
		t.Fatalf("second TryAcquire() while first leads = %v, %t, %v; want nil, false, nil", err, acquired, second)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatalf("first Release() error = %v", err)
	}

	second, acquired, err := secondElector.TryAcquire(ctx)
	if err != nil || !acquired {
		t.Fatalf("second TryAcquire() after release = %v, %t, want leadership", err, acquired)
	}
	defer second.Release(context.Background())
}

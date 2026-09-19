package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
)

func TestDependenciesRequireSuccessAndPropagateFailure(t *testing.T) {
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

	newJob := func(message string) jobs.Job {
		now := time.Now()
		job, err := jobs.NewWithSchedule("echo", json.RawMessage(`{"message":"`+message+`"}`), 3, jobs.PriorityNormal, now.Add(-time.Hour), now)
		if err != nil {
			t.Fatalf("New(%s): %v", message, err)
		}
		return job
	}

	prerequisite, err := repository.Create(ctx, newJob("prerequisite"))
	if err != nil {
		t.Fatalf("Create prerequisite: %v", err)
	}
	dependent, err := repository.CreateWithDependencies(ctx, newJob("dependent"), []string{prerequisite.ID})
	if err != nil {
		t.Fatalf("CreateWithDependencies: %v", err)
	}
	if len(dependent.DependsOn) != 1 || dependent.DependsOn[0] != prerequisite.ID {
		t.Fatalf("dependent dependencies = %v, want %s", dependent.DependsOn, prerequisite.ID)
	}

	claimed, ok, err := repository.ClaimNext(ctx, "dag-worker", 10*time.Second)
	if err != nil || !ok || claimed.ID != prerequisite.ID {
		t.Fatalf("first ClaimNext() = %+v, %t, %v", claimed, ok, err)
	}
	if _, ok, err := repository.ClaimNext(ctx, "other-worker", 10*time.Second); err != nil || ok {
		t.Fatalf("dependent ClaimNext() before prerequisite success = %t, %v", ok, err)
	}
	if err := repository.MarkSucceeded(ctx, prerequisite.ID, "dag-worker"); err != nil {
		t.Fatalf("MarkSucceeded prerequisite: %v", err)
	}
	claimed, ok, err = repository.ClaimNext(ctx, "dag-worker", 10*time.Second)
	if err != nil || !ok || claimed.ID != dependent.ID {
		t.Fatalf("dependent ClaimNext() after prerequisite success = %+v, %t, %v", claimed, ok, err)
	}

	failedPrerequisite, err := repository.Create(ctx, newJob("will fail"))
	if err != nil {
		t.Fatalf("Create failed prerequisite: %v", err)
	}
	blockedChild, err := repository.CreateWithDependencies(ctx, newJob("blocked child"), []string{failedPrerequisite.ID})
	if err != nil {
		t.Fatalf("Create blocked child: %v", err)
	}
	claimed, ok, err = repository.ClaimNext(ctx, "dag-worker", 10*time.Second)
	if err != nil || !ok || claimed.ID != failedPrerequisite.ID {
		t.Fatalf("failed prerequisite ClaimNext() = %+v, %t, %v", claimed, ok, err)
	}
	outcome, err := repository.RecordFailure(ctx, failedPrerequisite.ID, "dag-worker", "invalid input", false, 0)
	if err != nil || outcome != jobs.StatusFailed {
		t.Fatalf("RecordFailure() = %s, %v", outcome, err)
	}
	if blocked, err := repository.ResolveBlocked(ctx); err != nil || blocked != 1 {
		t.Fatalf("ResolveBlocked() = %d, %v", blocked, err)
	}
	resolved, err := repository.Get(ctx, blockedChild.ID)
	if err != nil {
		t.Fatalf("Get blocked child: %v", err)
	}
	if resolved.Status != jobs.StatusBlocked || resolved.ErrorMessage == nil || resolved.NextAttemptAt != nil {
		t.Errorf("blocked child = %+v, want BLOCKED terminal job", resolved)
	}
}

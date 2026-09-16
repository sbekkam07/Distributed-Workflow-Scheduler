package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/worker"
)

const (
	concurrentWorkers = 4
	concurrentJobs    = 24
)

// TestConcurrentWorkersClaimEveryJobOnce exercises PostgreSQL's real row-lock
// semantics in an isolated temporary database. It is opt-in because it creates
// and drops a database; run it with RUN_POSTGRES_INTEGRATION=1 and DATABASE_URL.
func TestConcurrentWorkersClaimEveryJobOnce(t *testing.T) {
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
	pool, err := postgres.OpenPool(ctx, temporaryURL, concurrentWorkers+1)
	if err != nil {
		t.Fatalf("OpenPool() error = %v", err)
	}
	defer pool.Close()

	repository := postgres.NewJobRepository(pool)
	jobIDs := make(map[string]struct{}, concurrentJobs)
	for i := 0; i < concurrentJobs; i++ {
		job, err := jobs.New("echo", json.RawMessage(fmt.Sprintf(`{"message":"job-%d"}`, i)), time.Now())
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		created, err := repository.Create(ctx, job)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		jobIDs[created.ID] = struct{}{}
	}

	executor := &recordingExecutor{executions: make(map[string]int)}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	workers := make([]*worker.Worker, concurrentWorkers)
	for i := range workers {
		workers[i], err = worker.New(
			repository,
			executor,
			fmt.Sprintf("claim-worker-%d", i),
			time.Second,
			10*time.Second,
			3*time.Second,
			time.Second,
			time.Minute,
			logger,
		)
		if err != nil {
			t.Fatalf("worker.New() error = %v", err)
		}
	}

	start := make(chan struct{})
	errors := make(chan error, concurrentWorkers)
	var group sync.WaitGroup
	for _, concurrentWorker := range workers {
		group.Add(1)
		go func(w *worker.Worker) {
			defer group.Done()
			<-start
			for {
				claimed, err := w.RunOnce(ctx)
				if err != nil {
					errors <- err
					return
				}
				if !claimed {
					return
				}
			}
		}(concurrentWorker)
	}
	close(start)
	group.Wait()
	close(errors)
	for err := range errors {
		t.Errorf("worker RunOnce() error = %v", err)
	}

	executor.assertExactlyOnce(t, jobIDs)
	for id := range jobIDs {
		job, err := repository.Get(ctx, id)
		if err != nil {
			t.Errorf("Get(%q) error = %v", id, err)
			continue
		}
		if job.Status != jobs.StatusSucceeded {
			t.Errorf("job %q status = %s, want %s", id, job.Status, jobs.StatusSucceeded)
		}
	}
}

func integrationDisabled(t *testing.T) bool {
	t.Helper()
	if os.Getenv("RUN_POSTGRES_INTEGRATION") != "1" {
		t.Skip("set RUN_POSTGRES_INTEGRATION=1 to run PostgreSQL integration tests")
		return true
	}
	return false
}

type recordingExecutor struct {
	mu         sync.Mutex
	executions map[string]int
}

func (e *recordingExecutor) Execute(_ context.Context, job jobs.Job) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.executions[job.ID]++
	return nil
}

func (e *recordingExecutor) assertExactlyOnce(t *testing.T, expected map[string]struct{}) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.executions) != len(expected) {
		t.Errorf("executed %d distinct jobs, want %d", len(e.executions), len(expected))
	}
	for id := range expected {
		if got := e.executions[id]; got != 1 {
			t.Errorf("job %q executed %d times, want 1", id, got)
		}
	}
}

func createTemporaryDatabase(t *testing.T, ctx context.Context) (string, func()) {
	t.Helper()
	baseURL := os.Getenv("DATABASE_URL")
	if strings.TrimSpace(baseURL) == "" {
		t.Fatal("DATABASE_URL is required for PostgreSQL integration tests")
	}

	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	name := "scheduler_phase2_" + randomHex(t)

	adminPool, err := pgxpool.New(ctx, baseURL)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		adminPool.Close()
		t.Fatalf("create temporary database: %v", err)
	}

	parsedURL.Path = "/" + name
	temporaryURL := parsedURL.String()
	return temporaryURL, func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		defer adminPool.Close()
		if _, err := adminPool.Exec(cleanupContext, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("drop temporary database %q: %v", name, err)
		}
	}
}

func randomHex(t *testing.T) string {
	t.Helper()
	bytes := make([]byte, 6)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatalf("generate database suffix: %v", err)
	}
	return hex.EncodeToString(bytes)
}

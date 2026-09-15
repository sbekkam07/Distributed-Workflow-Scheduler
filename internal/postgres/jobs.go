package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
)

// JobRepository persists jobs in PostgreSQL.
type JobRepository struct {
	pool *pgxpool.Pool
}

// NewJobRepository constructs a repository backed by pool.
func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

// Create inserts a queued job and returns the database-generated fields.
func (r *JobRepository) Create(ctx context.Context, job jobs.Job) (jobs.Job, error) {
	const query = `
		INSERT INTO jobs (kind, payload)
		VALUES ($1, $2)
		RETURNING id, kind, payload, status, error_message, created_at, started_at, completed_at`

	created, err := scanJob(r.pool.QueryRow(ctx, query, job.Kind, job.Payload))
	if err != nil {
		return jobs.Job{}, fmt.Errorf("insert job: %w", err)
	}
	return created, nil
}

// Get returns a job by its PostgreSQL-generated UUID.
func (r *JobRepository) Get(ctx context.Context, id string) (jobs.Job, error) {
	const query = `
		SELECT id, kind, payload, status, error_message, created_at, started_at, completed_at
		FROM jobs
		WHERE id = $1`

	job, err := scanJob(r.pool.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

// ClaimNext transactionally changes one queued job to RUNNING and returns it.
//
// FOR UPDATE SKIP LOCKED lets concurrent workers move past a row another worker
// is claiming instead of blocking or reading it as available. The transaction
// ends before the job is executed, so database locks never span user work.
func (r *JobRepository) ClaimNext(ctx context.Context) (jobs.Job, bool, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("begin job claim: %w", err)
	}
	defer tx.Rollback(ctx)

	const selectQuery = `
		SELECT id
		FROM jobs
		WHERE status = 'QUEUED'
		ORDER BY created_at ASC, id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1`

	var id string
	if err := tx.QueryRow(ctx, selectQuery).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.Job{}, false, nil
		}
		return jobs.Job{}, false, fmt.Errorf("select queued job for claim: %w", err)
	}

	const updateQuery = `
		UPDATE jobs
		SET status = 'RUNNING', started_at = now()
		WHERE id = $1 AND status = 'QUEUED'
		RETURNING id, kind, payload, status, error_message, created_at, started_at, completed_at`

	job, err := scanJob(tx.QueryRow(ctx, updateQuery, id))
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("mark claimed job running: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, false, fmt.Errorf("commit job claim: %w", err)
	}

	return job, true, nil
}

// MarkSucceeded records successful execution. It only updates the worker's
// expected RUNNING state so a stale worker cannot overwrite another outcome.
func (r *JobRepository) MarkSucceeded(ctx context.Context, id string) error {
	const query = `
		UPDATE jobs
		SET status = 'SUCCEEDED', completed_at = now(), error_message = NULL
		WHERE id = $1 AND status = 'RUNNING'`

	result, err := r.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("mark job succeeded: %w", err)
	}
	if result.RowsAffected() != 1 {
		return jobs.ErrNotRunning
	}
	return nil
}

// MarkFailed records an execution failure. It only updates RUNNING jobs.
func (r *JobRepository) MarkFailed(ctx context.Context, id, message string) error {
	const query = `
		UPDATE jobs
		SET status = 'FAILED', completed_at = now(), error_message = $2
		WHERE id = $1 AND status = 'RUNNING'`

	result, err := r.pool.Exec(ctx, query, id, message)
	if err != nil {
		return fmt.Errorf("mark job failed: %w", err)
	}
	if result.RowsAffected() != 1 {
		return jobs.ErrNotRunning
	}
	return nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanJob(row rowScanner) (jobs.Job, error) {
	var (
		job         jobs.Job
		payload     []byte
		status      string
		errorText   *string
		startedAt   *time.Time
		completedAt *time.Time
	)

	if err := row.Scan(
		&job.ID,
		&job.Kind,
		&payload,
		&status,
		&errorText,
		&job.CreatedAt,
		&startedAt,
		&completedAt,
	); err != nil {
		return jobs.Job{}, err
	}

	job.Payload = json.RawMessage(payload)
	job.Status = jobs.Status(status)
	job.ErrorMessage = errorText
	job.StartedAt = startedAt
	job.CompletedAt = completedAt
	return job, nil
}

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
		INSERT INTO jobs (kind, payload, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, kind, payload, priority, run_at, status, error_message, created_at, started_at, completed_at,
			lease_owner, lease_expires_at, last_heartbeat_at, attempt_count, max_attempts, next_attempt_at, idempotency_key`

	created, err := scanJob(r.pool.QueryRow(ctx, query, job.Kind, job.Payload, job.Priority, job.RunAt, job.MaxAttempts, job.IdempotencyKey))
	if err != nil {
		return jobs.Job{}, fmt.Errorf("insert job: %w", err)
	}
	return created, nil
}

// CreateOrGet creates job once for its idempotency key. PostgreSQL's unique
// index serializes concurrent inserts; a reused key must describe identical
// work or it is a client conflict.
func (r *JobRepository) CreateOrGet(ctx context.Context, job jobs.Job) (jobs.Job, bool, error) {
	const insertQuery = `
		INSERT INTO jobs (kind, payload, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING id, kind, payload, priority, run_at, status, error_message, created_at, started_at, completed_at,
			lease_owner, lease_expires_at, last_heartbeat_at, attempt_count, max_attempts, next_attempt_at, idempotency_key`

	created, err := scanJob(r.pool.QueryRow(ctx, insertQuery, job.Kind, job.Payload, job.Priority, job.RunAt, job.MaxAttempts, job.IdempotencyKey))
	if err == nil {
		return created, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, false, fmt.Errorf("insert idempotent job: %w", err)
	}

	const existingQuery = `
		SELECT id, kind, payload, priority, run_at, status, error_message, created_at, started_at, completed_at,
			lease_owner, lease_expires_at, last_heartbeat_at, attempt_count, max_attempts, next_attempt_at, idempotency_key
		FROM jobs
		WHERE idempotency_key = $1
			AND kind = $2
			AND payload = $3::jsonb
			AND max_attempts = $4
			AND priority = $5`
	existing, err := scanJob(r.pool.QueryRow(ctx, existingQuery, job.IdempotencyKey, job.Kind, job.Payload, job.MaxAttempts, job.Priority))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, false, jobs.ErrIdempotencyConflict
	}
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("get idempotent job: %w", err)
	}
	return existing, false, nil
}

// Get returns a job by its PostgreSQL-generated UUID.
func (r *JobRepository) Get(ctx context.Context, id string) (jobs.Job, error) {
	const query = `
		SELECT id, kind, payload, priority, run_at, status, error_message, created_at, started_at, completed_at,
			lease_owner, lease_expires_at, last_heartbeat_at, attempt_count, max_attempts, next_attempt_at, idempotency_key
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
func (r *JobRepository) ClaimNext(ctx context.Context, owner string, leaseDuration time.Duration) (jobs.Job, bool, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("begin job claim: %w", err)
	}
	defer tx.Rollback(ctx)

	const selectQuery = `
		SELECT id
		FROM jobs
		WHERE status = 'QUEUED'
			AND GREATEST(run_at, next_attempt_at) <= now()
			AND attempt_count < max_attempts
		ORDER BY CASE priority WHEN 'HIGH' THEN 0 WHEN 'NORMAL' THEN 1 ELSE 2 END ASC,
			GREATEST(run_at, next_attempt_at) ASC, created_at ASC, id ASC
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
		SET status = 'RUNNING',
			started_at = now(),
			attempt_count = attempt_count + 1,
			next_attempt_at = NULL,
			lease_owner = $2,
			lease_expires_at = now() + ($3 * INTERVAL '1 microsecond'),
			last_heartbeat_at = now()
		WHERE id = $1 AND status = 'QUEUED'
		RETURNING id, kind, payload, priority, run_at, status, error_message, created_at, started_at, completed_at,
			lease_owner, lease_expires_at, last_heartbeat_at, attempt_count, max_attempts, next_attempt_at, idempotency_key`

	job, err := scanJob(tx.QueryRow(ctx, updateQuery, id, owner, leaseDuration.Microseconds()))
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("mark claimed job running: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, false, fmt.Errorf("commit job claim: %w", err)
	}

	return job, true, nil
}

// RecordFailure records a failed execution. Retryable failures remain queued
// until their backoff expires; the final retryable failure becomes DEAD. A
// non-retryable failure becomes FAILED immediately.
func (r *JobRepository) RecordFailure(ctx context.Context, id, owner, message string, retryable bool, retryDelay time.Duration) (jobs.Status, error) {
	const query = `
		UPDATE jobs
		SET status = CASE
				WHEN $4 AND attempt_count < max_attempts THEN 'QUEUED'
				WHEN $4 THEN 'DEAD'
				ELSE 'FAILED'
			END,
			started_at = CASE WHEN $4 AND attempt_count < max_attempts THEN NULL ELSE started_at END,
			completed_at = CASE WHEN $4 AND attempt_count < max_attempts THEN NULL ELSE now() END,
			error_message = CASE WHEN $4 AND attempt_count < max_attempts THEN NULL ELSE $3 END,
			next_attempt_at = CASE
				WHEN $4 AND attempt_count < max_attempts THEN now() + ($5 * INTERVAL '1 microsecond')
				ELSE NULL
			END,
			lease_owner = NULL,
			lease_expires_at = NULL,
			last_heartbeat_at = NULL
		WHERE id = $1
			AND status = 'RUNNING'
			AND lease_owner = $2
			AND lease_expires_at > now()
		RETURNING status`

	var status string
	err := r.pool.QueryRow(ctx, query, id, owner, message, retryable, retryDelay.Microseconds()).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", jobs.ErrLeaseLost
	}
	if err != nil {
		return "", fmt.Errorf("record job failure: %w", err)
	}
	return jobs.Status(status), nil
}

// MarkSucceeded records successful execution. It only updates the worker's
// expected RUNNING state so a stale worker cannot overwrite another outcome.
func (r *JobRepository) MarkSucceeded(ctx context.Context, id, owner string) error {
	const query = `
		UPDATE jobs
		SET status = 'SUCCEEDED',
			completed_at = now(),
			error_message = NULL,
			lease_owner = NULL,
			lease_expires_at = NULL,
			last_heartbeat_at = NULL
		WHERE id = $1
			AND status = 'RUNNING'
			AND lease_owner = $2
			AND lease_expires_at > now()`

	result, err := r.pool.Exec(ctx, query, id, owner)
	if err != nil {
		return fmt.Errorf("mark job succeeded: %w", err)
	}
	if result.RowsAffected() != 1 {
		return jobs.ErrLeaseLost
	}
	return nil
}

// RenewLease extends the lease only when owner still owns an unexpired running
// job. A false result means the worker must stop treating the job as its own.
func (r *JobRepository) RenewLease(ctx context.Context, id, owner string, leaseDuration time.Duration) (bool, error) {
	const query = `
		UPDATE jobs
		SET lease_expires_at = now() + ($3 * INTERVAL '1 microsecond'),
			last_heartbeat_at = now()
		WHERE id = $1
			AND status = 'RUNNING'
			AND lease_owner = $2
			AND lease_expires_at > now()`

	result, err := r.pool.Exec(ctx, query, id, owner, leaseDuration.Microseconds())
	if err != nil {
		return false, fmt.Errorf("renew job lease: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

// RecoverExpired returns jobs whose running lease has expired to the queue.
// A worker that later wakes up cannot write a terminal state because its owner
// and expiry checks will no longer match.
func (r *JobRepository) RecoverExpired(ctx context.Context) (int64, error) {
	const query = `
		UPDATE jobs
		SET status = CASE WHEN attempt_count < max_attempts THEN 'QUEUED' ELSE 'DEAD' END,
			started_at = CASE WHEN attempt_count < max_attempts THEN NULL ELSE started_at END,
			completed_at = CASE WHEN attempt_count < max_attempts THEN NULL ELSE now() END,
			error_message = CASE WHEN attempt_count < max_attempts THEN NULL ELSE 'worker lease expired after maximum attempts' END,
			next_attempt_at = CASE WHEN attempt_count < max_attempts THEN now() ELSE NULL END,
			lease_owner = NULL,
			lease_expires_at = NULL,
			last_heartbeat_at = NULL
		WHERE status = 'RUNNING' AND lease_expires_at <= now()`

	result, err := r.pool.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("recover expired job leases: %w", err)
	}
	return result.RowsAffected(), nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanJob(row rowScanner) (jobs.Job, error) {
	var (
		job             jobs.Job
		payload         []byte
		status          string
		errorText       *string
		startedAt       *time.Time
		completedAt     *time.Time
		leaseOwner      *string
		leaseExpiresAt  *time.Time
		lastHeartbeatAt *time.Time
	)

	if err := row.Scan(
		&job.ID,
		&job.Kind,
		&payload,
		&job.Priority,
		&job.RunAt,
		&status,
		&errorText,
		&job.CreatedAt,
		&startedAt,
		&completedAt,
		&leaseOwner,
		&leaseExpiresAt,
		&lastHeartbeatAt,
		&job.AttemptCount,
		&job.MaxAttempts,
		&job.NextAttemptAt,
		&job.IdempotencyKey,
	); err != nil {
		return jobs.Job{}, err
	}

	job.Payload = json.RawMessage(payload)
	job.Status = jobs.Status(status)
	job.ErrorMessage = errorText
	job.StartedAt = startedAt
	job.CompletedAt = completedAt
	job.LeaseOwner = leaseOwner
	job.LeaseExpiresAt = leaseExpiresAt
	job.LastHeartbeatAt = lastHeartbeatAt
	return job, nil
}

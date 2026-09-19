package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	return r.create(ctx, r.pool, job)
}

type rowQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (r *JobRepository) create(ctx context.Context, queryer rowQueryer, job jobs.Job) (jobs.Job, error) {
	const query = `
		INSERT INTO jobs (kind, payload, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, kind, payload, priority, run_at, status, error_message, created_at, started_at, completed_at,
			lease_owner, lease_expires_at, last_heartbeat_at, attempt_count, max_attempts, next_attempt_at, idempotency_key`

	created, err := scanJob(queryer.QueryRow(ctx, query, job.Kind, job.Payload, job.Priority, job.RunAt, job.MaxAttempts, job.IdempotencyKey))
	if err != nil {
		return jobs.Job{}, fmt.Errorf("insert job: %w", err)
	}
	return created, nil
}

// CreateWithDependencies inserts a job and all immutable prerequisite edges in
// one transaction, preventing a worker from claiming it between those writes.
func (r *JobRepository) CreateWithDependencies(ctx context.Context, job jobs.Job, dependencies []string) (jobs.Job, error) {
	if len(dependencies) == 0 {
		return r.Create(ctx, job)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return jobs.Job{}, fmt.Errorf("begin dependency create: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := verifyDependencies(ctx, tx, dependencies); err != nil {
		return jobs.Job{}, err
	}
	created, err := r.create(ctx, tx, job)
	if err != nil {
		return jobs.Job{}, err
	}
	if err := insertDependencies(ctx, tx, created.ID, dependencies); err != nil {
		return jobs.Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, fmt.Errorf("commit dependency create: %w", err)
	}
	created.DependsOn = append([]string(nil), dependencies...)
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

// CreateOrGetWithDependencies applies idempotency and edge creation in one
// transaction. A repeated key must retain the same prerequisite set.
func (r *JobRepository) CreateOrGetWithDependencies(ctx context.Context, job jobs.Job, dependencies []string) (jobs.Job, bool, error) {
	if len(dependencies) == 0 {
		return r.CreateOrGet(ctx, job)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("begin idempotent dependency create: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := verifyDependencies(ctx, tx, dependencies); err != nil {
		return jobs.Job{}, false, err
	}
	const insertQuery = `
		INSERT INTO jobs (kind, payload, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING id, kind, payload, priority, run_at, status, error_message, created_at, started_at, completed_at,
			lease_owner, lease_expires_at, last_heartbeat_at, attempt_count, max_attempts, next_attempt_at, idempotency_key`
	created, err := scanJob(tx.QueryRow(ctx, insertQuery, job.Kind, job.Payload, job.Priority, job.RunAt, job.MaxAttempts, job.IdempotencyKey))
	if err == nil {
		if err := insertDependencies(ctx, tx, created.ID, dependencies); err != nil {
			return jobs.Job{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return jobs.Job{}, false, fmt.Errorf("commit idempotent dependency create: %w", err)
		}
		created.DependsOn = append([]string(nil), dependencies...)
		return created, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, false, fmt.Errorf("insert idempotent dependent job: %w", err)
	}
	const existingQuery = `
		SELECT id, kind, payload, priority, run_at, status, error_message, created_at, started_at, completed_at,
			lease_owner, lease_expires_at, last_heartbeat_at, attempt_count, max_attempts, next_attempt_at, idempotency_key
		FROM jobs
		WHERE idempotency_key = $1 AND kind = $2 AND payload = $3::jsonb AND max_attempts = $4 AND priority = $5`
	existing, err := scanJob(tx.QueryRow(ctx, existingQuery, job.IdempotencyKey, job.Kind, job.Payload, job.MaxAttempts, job.Priority))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, false, jobs.ErrIdempotencyConflict
	}
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("get idempotent dependent job: %w", err)
	}
	existingDependencies, err := dependenciesFor(ctx, tx, existing.ID)
	if err != nil {
		return jobs.Job{}, false, err
	}
	if !sameDependencies(existingDependencies, dependencies) {
		return jobs.Job{}, false, jobs.ErrIdempotencyConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, false, fmt.Errorf("commit idempotent dependency lookup: %w", err)
	}
	existing.DependsOn = existingDependencies
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
	dependencies, err := dependenciesFor(ctx, r.pool, job.ID)
	if err != nil {
		return jobs.Job{}, err
	}
	job.DependsOn = dependencies
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
		FROM jobs AS job
		WHERE status = 'QUEUED'
			AND GREATEST(run_at, next_attempt_at) <= now()
			AND attempt_count < max_attempts
			AND NOT EXISTS (
				SELECT 1
				FROM job_dependencies AS dependency
				JOIN jobs AS prerequisite ON prerequisite.id = dependency.prerequisite_job_id
				WHERE dependency.job_id = job.id AND prerequisite.status <> 'SUCCEEDED'
			)
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

// ResolveBlocked applies the workflow failure policy: a queued job becomes
// BLOCKED when any prerequisite fails, dead-letters, or is itself blocked.
func (r *JobRepository) ResolveBlocked(ctx context.Context) (int64, error) {
	return resolveBlocked(ctx, r.pool)
}

type commandExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func resolveBlocked(ctx context.Context, executor commandExecer) (int64, error) {
	const query = `
		UPDATE jobs AS job
		SET status = 'BLOCKED',
			completed_at = now(),
			error_message = 'a prerequisite did not succeed',
			next_attempt_at = NULL,
			lease_owner = NULL,
			lease_expires_at = NULL,
			last_heartbeat_at = NULL
		WHERE job.status = 'QUEUED'
			AND EXISTS (
				SELECT 1
				FROM job_dependencies AS dependency
				JOIN jobs AS prerequisite ON prerequisite.id = dependency.prerequisite_job_id
				WHERE dependency.job_id = job.id
					AND prerequisite.status IN ('FAILED', 'DEAD', 'BLOCKED')
			)`
	result, err := executor.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("resolve blocked jobs: %w", err)
	}
	return result.RowsAffected(), nil
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

type dependencyQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func verifyDependencies(ctx context.Context, queryer dependencyQueryer, dependencies []string) error {
	var count int
	if err := queryer.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE id = ANY($1::uuid[])`, dependencies).Scan(&count); err != nil {
		return fmt.Errorf("verify job dependencies: %w", err)
	}
	if count != len(dependencies) {
		return jobs.ErrDependencyNotFound
	}
	return nil
}

func insertDependencies(ctx context.Context, tx pgx.Tx, jobID string, dependencies []string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO job_dependencies (job_id, prerequisite_job_id)
		SELECT $1, prerequisite_job_id
		FROM unnest($2::uuid[]) AS prerequisite_job_id`, jobID, dependencies)
	if err != nil {
		return fmt.Errorf("insert job dependencies: %w", err)
	}
	return nil
}

func dependenciesFor(ctx context.Context, queryer dependencyQueryer, jobID string) ([]string, error) {
	rows, err := queryer.Query(ctx, `
		SELECT prerequisite_job_id::text
		FROM job_dependencies
		WHERE job_id = $1
		ORDER BY prerequisite_job_id`, jobID)
	if err != nil {
		return nil, fmt.Errorf("get job dependencies: %w", err)
	}
	defer rows.Close()
	var dependencies []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan job dependency: %w", err)
		}
		dependencies = append(dependencies, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job dependencies: %w", err)
	}
	return dependencies, nil
}

func sameDependencies(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, id := range left {
		seen[id] = struct{}{}
	}
	for _, id := range right {
		if _, ok := seen[id]; !ok {
			return false
		}
	}
	return true
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

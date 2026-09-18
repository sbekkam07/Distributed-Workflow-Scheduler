-- Phase 7: run_at is a durable lower bound for job execution. Existing jobs
-- are immediately eligible, preserving their Phase 6 behavior.
ALTER TABLE jobs
    ADD COLUMN run_at TIMESTAMPTZ NOT NULL DEFAULT now();

DROP INDEX jobs_queued_priority_next_attempt_at_created_at_id_idx;

-- ClaimNext uses this same priority and combined availability ordering. A job
-- cannot run until both its original schedule and retry backoff are due.
CREATE INDEX jobs_queued_priority_available_at_created_at_id_idx
    ON jobs (
        (CASE priority WHEN 'HIGH' THEN 0 WHEN 'NORMAL' THEN 1 ELSE 2 END),
        (GREATEST(run_at, next_attempt_at)),
        created_at,
        id
    )
    WHERE status = 'QUEUED';

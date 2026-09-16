-- Phase 4: retries are durable queue state. A worker never sleeps while a
-- retry waits; it claims only QUEUED rows whose next_attempt_at is eligible.
ALTER TABLE jobs
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN next_attempt_at TIMESTAMPTZ DEFAULT now();

-- Existing pre-retry terminal and running jobs have already consumed one
-- execution. Existing queued jobs remain eligible immediately.
UPDATE jobs
SET attempt_count = CASE WHEN status = 'QUEUED' THEN 0 ELSE 1 END,
    next_attempt_at = CASE WHEN status = 'QUEUED' THEN now() ELSE NULL END;

ALTER TABLE jobs DROP CONSTRAINT jobs_lifecycle_is_consistent;

ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_status_check CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD'));

ALTER TABLE jobs
    ADD CONSTRAINT jobs_lifecycle_is_consistent CHECK (
        max_attempts > 0
        AND attempt_count >= 0
        AND (
            (status = 'QUEUED'
                AND started_at IS NULL
                AND completed_at IS NULL
                AND error_message IS NULL
                AND lease_owner IS NULL
                AND lease_expires_at IS NULL
                AND last_heartbeat_at IS NULL
                AND next_attempt_at IS NOT NULL
                AND attempt_count < max_attempts)
            OR
            (status = 'RUNNING'
                AND started_at IS NOT NULL
                AND completed_at IS NULL
                AND error_message IS NULL
                AND lease_owner IS NOT NULL
                AND lease_expires_at IS NOT NULL
                AND last_heartbeat_at IS NOT NULL
                AND next_attempt_at IS NULL
                AND attempt_count > 0
                AND attempt_count <= max_attempts)
            OR
            (status = 'SUCCEEDED'
                AND started_at IS NOT NULL
                AND completed_at IS NOT NULL
                AND error_message IS NULL
                AND lease_owner IS NULL
                AND lease_expires_at IS NULL
                AND last_heartbeat_at IS NULL
                AND next_attempt_at IS NULL
                AND attempt_count > 0
                AND attempt_count <= max_attempts)
            OR
            (status = 'FAILED'
                AND started_at IS NOT NULL
                AND completed_at IS NOT NULL
                AND error_message IS NOT NULL
                AND lease_owner IS NULL
                AND lease_expires_at IS NULL
                AND last_heartbeat_at IS NULL
                AND next_attempt_at IS NULL
                AND attempt_count > 0
                AND attempt_count <= max_attempts)
            OR
            (status = 'DEAD'
                AND started_at IS NOT NULL
                AND completed_at IS NOT NULL
                AND error_message IS NOT NULL
                AND lease_owner IS NULL
                AND lease_expires_at IS NULL
                AND last_heartbeat_at IS NULL
                AND next_attempt_at IS NULL
                AND attempt_count = max_attempts)
        )
    );

CREATE INDEX jobs_queued_next_attempt_at_created_at_id_idx
    ON jobs (next_attempt_at, created_at, id)
    WHERE status = 'QUEUED';

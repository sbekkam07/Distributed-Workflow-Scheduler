-- Map the Phase 8 terminal BLOCKED outcome to Phase 7's terminal FAILED shape.
UPDATE jobs
SET status = 'FAILED',
    started_at = completed_at
WHERE status = 'BLOCKED';

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
                AND started_at IS NULL AND completed_at IS NULL AND error_message IS NULL
                AND lease_owner IS NULL AND lease_expires_at IS NULL AND last_heartbeat_at IS NULL
                AND next_attempt_at IS NOT NULL AND attempt_count < max_attempts)
            OR
            (status = 'RUNNING'
                AND started_at IS NOT NULL AND completed_at IS NULL AND error_message IS NULL
                AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL AND last_heartbeat_at IS NOT NULL
                AND next_attempt_at IS NULL AND attempt_count > 0 AND attempt_count <= max_attempts)
            OR
            (status = 'SUCCEEDED'
                AND started_at IS NOT NULL AND completed_at IS NOT NULL AND error_message IS NULL
                AND lease_owner IS NULL AND lease_expires_at IS NULL AND last_heartbeat_at IS NULL
                AND next_attempt_at IS NULL AND attempt_count > 0 AND attempt_count <= max_attempts)
            OR
            (status IN ('FAILED', 'DEAD')
                AND started_at IS NOT NULL AND completed_at IS NOT NULL AND error_message IS NOT NULL
                AND lease_owner IS NULL AND lease_expires_at IS NULL AND last_heartbeat_at IS NULL
                AND next_attempt_at IS NULL AND attempt_count > 0 AND attempt_count <= max_attempts)
        )
    );

DROP TABLE job_dependencies;

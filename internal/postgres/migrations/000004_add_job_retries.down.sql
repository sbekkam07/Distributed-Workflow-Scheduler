DROP INDEX IF EXISTS jobs_queued_next_attempt_at_created_at_id_idx;

-- A down migration cannot retain Phase 4's DEAD state after the older schema
-- removes retry metadata, so preserve it as the earlier terminal FAILED state.
UPDATE jobs
SET status = 'FAILED'
WHERE status = 'DEAD';

ALTER TABLE jobs DROP CONSTRAINT jobs_lifecycle_is_consistent;

ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_status_check CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED'));

ALTER TABLE jobs
    ADD CONSTRAINT jobs_lifecycle_is_consistent CHECK (
        (status = 'QUEUED'
            AND started_at IS NULL
            AND completed_at IS NULL
            AND error_message IS NULL
            AND lease_owner IS NULL
            AND lease_expires_at IS NULL
            AND last_heartbeat_at IS NULL)
        OR
        (status = 'RUNNING'
            AND started_at IS NOT NULL
            AND completed_at IS NULL
            AND error_message IS NULL
            AND lease_owner IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND last_heartbeat_at IS NOT NULL)
        OR
        (status = 'SUCCEEDED'
            AND started_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND error_message IS NULL
            AND lease_owner IS NULL
            AND lease_expires_at IS NULL
            AND last_heartbeat_at IS NULL)
        OR
        (status = 'FAILED'
            AND started_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND error_message IS NOT NULL
            AND lease_owner IS NULL
            AND lease_expires_at IS NULL
            AND last_heartbeat_at IS NULL)
    );

ALTER TABLE jobs
    DROP COLUMN next_attempt_at,
    DROP COLUMN max_attempts,
    DROP COLUMN attempt_count;

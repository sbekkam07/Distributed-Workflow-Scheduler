DROP INDEX IF EXISTS jobs_running_lease_expiry_idx;

ALTER TABLE jobs DROP CONSTRAINT jobs_lifecycle_is_consistent;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_lifecycle_is_consistent CHECK (
        (status = 'QUEUED' AND started_at IS NULL AND completed_at IS NULL AND error_message IS NULL)
        OR
        (status = 'RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL AND error_message IS NULL)
        OR
        (status = 'SUCCEEDED' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND error_message IS NULL)
        OR
        (status = 'FAILED' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND error_message IS NOT NULL)
    );

ALTER TABLE jobs
    DROP COLUMN last_heartbeat_at,
    DROP COLUMN lease_expires_at,
    DROP COLUMN lease_owner;

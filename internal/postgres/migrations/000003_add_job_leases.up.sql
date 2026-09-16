-- Phase 3: a RUNNING job has a renewable owner lease. Stop old workers before
-- applying this migration; any legacy RUNNING jobs are returned to QUEUED so
-- they become recoverable under the new lease protocol.
ALTER TABLE jobs
    ADD COLUMN lease_owner TEXT,
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN last_heartbeat_at TIMESTAMPTZ;

UPDATE jobs
SET status = 'QUEUED',
    started_at = NULL
WHERE status = 'RUNNING';

ALTER TABLE jobs DROP CONSTRAINT jobs_lifecycle_is_consistent;

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

CREATE INDEX jobs_running_lease_expiry_idx
    ON jobs (lease_expires_at)
    WHERE status = 'RUNNING';

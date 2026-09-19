-- Phase 8: immutable DAG edges. Prerequisites must succeed before a dependent
-- job is eligible; failure propagation uses the reverse lookup index below.
CREATE TABLE job_dependencies (
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    prerequisite_job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    PRIMARY KEY (job_id, prerequisite_job_id),
    CONSTRAINT job_dependencies_not_self CHECK (job_id <> prerequisite_job_id)
);

CREATE INDEX job_dependencies_prerequisite_job_id_job_id_idx
    ON job_dependencies (prerequisite_job_id, job_id);

ALTER TABLE jobs DROP CONSTRAINT jobs_lifecycle_is_consistent;
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_status_check CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD', 'BLOCKED'));

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
            OR
            (status = 'BLOCKED'
                AND started_at IS NULL AND completed_at IS NOT NULL AND error_message IS NOT NULL
                AND lease_owner IS NULL AND lease_expires_at IS NULL AND last_heartbeat_at IS NULL
                AND next_attempt_at IS NULL AND attempt_count >= 0 AND attempt_count <= max_attempts)
        )
    );

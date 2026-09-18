DROP INDEX IF EXISTS jobs_queued_priority_available_at_created_at_id_idx;

ALTER TABLE jobs DROP COLUMN run_at;

CREATE INDEX jobs_queued_priority_next_attempt_at_created_at_id_idx
    ON jobs (
        (CASE priority WHEN 'HIGH' THEN 0 WHEN 'NORMAL' THEN 1 ELSE 2 END),
        next_attempt_at,
        created_at,
        id
    )
    WHERE status = 'QUEUED';

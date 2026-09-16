-- Phase 6: strict priority claim ordering. Existing work is normal priority.
ALTER TABLE jobs
    ADD COLUMN priority TEXT NOT NULL DEFAULT 'NORMAL',
    ADD CONSTRAINT jobs_priority_check CHECK (priority IN ('HIGH', 'NORMAL', 'LOW'));

-- This expression exactly matches ClaimNext's strict priority ordering, then
-- preserves retry eligibility and FIFO order within a priority class.
CREATE INDEX jobs_queued_priority_next_attempt_at_created_at_id_idx
    ON jobs (
        (CASE priority WHEN 'HIGH' THEN 0 WHEN 'NORMAL' THEN 1 ELSE 2 END),
        next_attempt_at,
        created_at,
        id
    )
    WHERE status = 'QUEUED';

-- Phase 2: supports the deterministic ordering used by concurrent workers when
-- selecting the next claimable queued job.
CREATE INDEX jobs_queued_created_at_id_idx
    ON jobs (created_at, id)
    WHERE status = 'QUEUED';

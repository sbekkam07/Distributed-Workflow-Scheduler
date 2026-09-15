-- Phase 1's durable queue. Later migrations add retries, leases, priorities,
-- scheduling, and workflow dependencies instead of changing this history.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind TEXT NOT NULL CHECK (length(btrim(kind)) > 0),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'QUEUED'
        CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT jobs_lifecycle_is_consistent CHECK (
        (status = 'QUEUED' AND started_at IS NULL AND completed_at IS NULL AND error_message IS NULL)
        OR
        (status = 'RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL AND error_message IS NULL)
        OR
        (status = 'SUCCEEDED' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND error_message IS NULL)
        OR
        (status = 'FAILED' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND error_message IS NOT NULL)
    )
);

-- Supports the worker's Phase 1 polling query and the concurrent claim query
-- introduced in Phase 2, without indexing completed jobs.
CREATE INDEX jobs_queued_created_at_idx ON jobs (created_at) WHERE status = 'QUEUED';

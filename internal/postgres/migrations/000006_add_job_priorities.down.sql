DROP INDEX IF EXISTS jobs_queued_priority_next_attempt_at_created_at_id_idx;

ALTER TABLE jobs DROP CONSTRAINT jobs_priority_check;

ALTER TABLE jobs DROP COLUMN priority;

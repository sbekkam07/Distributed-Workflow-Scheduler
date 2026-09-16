DROP INDEX IF EXISTS jobs_idempotency_key_unique_idx;

ALTER TABLE jobs DROP CONSTRAINT jobs_idempotency_key_length;

ALTER TABLE jobs DROP COLUMN idempotency_key;

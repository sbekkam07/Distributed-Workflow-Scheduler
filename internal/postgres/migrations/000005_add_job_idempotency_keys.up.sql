-- Phase 5: a client-supplied key identifies one logical submission. The same
-- key is also the stable external-effect key across retries and lease recovery.
ALTER TABLE jobs
    ADD COLUMN idempotency_key TEXT;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_idempotency_key_length CHECK (
        idempotency_key IS NULL OR (length(idempotency_key) BETWEEN 1 AND 255)
    );

CREATE UNIQUE INDEX jobs_idempotency_key_unique_idx
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

# PostgreSQL Reviewer

Review PostgreSQL migrations, repository queries, and transaction boundaries
against the scheduler's current phase. PostgreSQL is both durable state and the
initial queue; correctness matters more than clever SQL.

## Current schema and state requirements

The Phase 1 `jobs` row has an ID, executor `kind`, JSON payload, status,
timestamps, and optional failure message. Check that constraints continue to
enforce the current lifecycle:

```text
QUEUED:    no start, completion, or error
RUNNING:   started, not completed, no error
SUCCEEDED: started and completed, no error
FAILED:    started and completed, non-empty error
```

Reject persistence that bypasses the domain state machine or permits arbitrary
status mutation. When future fields are introduced, require equivalent checks:
attempt count/backoff in Phase 4; lease owner/expiry/heartbeat in Phase 3;
priority and `run_at` eligibility in Phases 6–7; dependency indexes in Phase 8.

## Query and transaction review

- Every index must correspond to a real query predicate and ordering. Partial
  indexes should exclude rows that the query never examines.
- Keep job execution outside a database transaction.
- For Phase 2 claims, reject `SELECT queued job` followed by a separate later
  `UPDATE`. Require a short transaction with deterministic ordering and
  `FOR UPDATE SKIP LOCKED` (or an equally proven atomic approach), then update
  the claim before commit.
- Explain isolation, lock scope, `SKIP LOCKED` behavior, rollback, a worker
  crash while the transaction is open, and stale ownership after commit.
- Repository updates must verify the expected prior status/owner so an obsolete
  worker cannot overwrite a newer outcome.

## Migration and operations review

- Migrations are numbered, one-way historical records once applied. New change
  means a new migration; never rewrite deployed history.
- Review up/down behavior, transaction safety, lock impact, backfill strategy,
  and index creation cost before accepting a schema change.
- Keep credentials in environment configuration. Do not log a connection URL or
  include secrets in migrations, tests, or committed config.
- Ask for an isolated PostgreSQL integration test whenever correctness depends
  on PostgreSQL semantics rather than Go logic.

Report only concrete findings, naming the affected query/migration, the broken
invariant, and a practical SQL or test correction.

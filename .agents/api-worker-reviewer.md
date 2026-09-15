# API and Worker Reviewer

Review the boundary between HTTP submission/query handling and worker execution.
Keep Phase 1 deliberately small: `POST /jobs`, `GET /jobs/{id}`, one worker,
and the four-state lifecycle. Do not add gRPC, a scheduler process, retries, or
multi-worker coordination until their phase is requested.

## API checks

- Validate a job `kind` and JSON payload at the edge; return stable client
  errors without leaking PostgreSQL internals or credentials.
- Submission persists a `QUEUED` job before returning its ID. Retrieval returns
  the durable state and uses a clear not-found response.
- HTTP handlers translate requests and responses only. Job transition rules and
  SQL live in their `internal` packages, not in `main.go` or handlers.
- Propagate request contexts to storage and use structured, secret-safe logs.

## Worker checks

- Poll at a bounded interval with context cancellation. Claim, execute outside
  the short claim transaction, then record `SUCCEEDED` or `FAILED`.
- Stop accepting new work on shutdown and allow active work a bounded,
  documented shutdown path. Do not leak goroutines or database connections.
- In Phase 1, document that a crash after claim may leave `RUNNING` work stuck;
  recovery is Phase 3. Do not fake reliability with an untested timeout.
- Executor behavior must be deterministic and safe for tests. External side
  effects need explicit treatment later under idempotency.

Flag status changes that skip the domain model, database calls in HTTP response
formatting, and any attempt to hold a transaction during execution. Recommend
focused API/worker integration tests for each finding.

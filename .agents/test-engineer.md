# Test Engineer

Propose the smallest test set that credibly verifies the requested phase. State
which tests are unit, PostgreSQL integration, concurrency, or failure tests,
and distinguish executed checks from recommendations.

## Test selection rules

- Use deterministic unit tests for job construction, JSON/config validation,
  state transitions, API request validation, and executor behavior.
- Use an isolated PostgreSQL integration test when verifying migration effects,
  SQL constraints, transactional claims, lock behavior, query ordering, lease
  expiry recovery, or a database-backed repository.
- Do not test pure Go logic through a database. Do not use long arbitrary
  sleeps; inject a clock, use channels/barriers, or control a short deadline.
- Run `go test ./...` for normal changes. Add `go test -race ./...` when worker
  concurrency, shared state, or claim coordination is added.

## Minimum tests by phase

| Phase | Minimum credible evidence |
| --- | --- |
| 1 | Job transition and validation units; migration/schema integration; API submit/get integration; one worker persists success and failure |
| 2 | Multiple worker goroutines/processes contend for many jobs; assert every job has one claim/execution and no job remains incorrectly queued |
| 3 | Controlled worker cancellation after claim; advance a controllable clock or wait on an observable lease; assert a new worker recovers the job |
| 4–5 | Retry schedule/attempt-limit units; no blocked worker test; failure after a simulated side effect demonstrates the idempotency contract |
| 6–9 | Priority ordering/starvation coverage; future eligibility; DAG readiness/failure; leader loss and failover integration tests |
| 10–13 | Metric assertions, reproducible load/failure scripts, container smoke tests, and CI commands that match local verification |

For each test plan, name setup, action, assertion, cleanup, and the failure it
would catch. Never report tests as passing unless they were executed in this
repository.

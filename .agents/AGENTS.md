# Distributed Workflow Scheduler: Project Guide

## Repository role and learning goal

This is the implementation repository for a Go distributed workflow scheduler.
Build it incrementally so the user can understand each reliability mechanism.
For a meaningful distributed-systems change, explain the failure mode, invariant,
mechanism, trade-offs, and tests before or while implementing it.

`../distributed-workflow-orchestrator` is a **read-only reference repository**.
It may be inspected for architecture, package organization, database/lease/retry
patterns, DAG design, tests, Docker, and load testing. Never modify it or copy
its code. If an idea is adapted, state the pattern and why this project uses it.

## Intended architecture

```text
Client -> API Server -> PostgreSQL <- Scheduler -> Workers (1..N)
                                      ^
                         Prometheus -> Grafana (later)
```

The API accepts and exposes jobs. Workers execute jobs. PostgreSQL is initially
the durable source of truth and queue. A scheduler is introduced only when a
future feature needs a single coordinator. Later, Docker Compose runs the local
environment; Kubernetes deploys the API, scheduler, workers, PostgreSQL, and
observability components. Do not build this target architecture all at once.

## Current verified status

Phases 1–6 are complete. The repository has Go process entry points, a
`Job` domain state machine, PostgreSQL connection configuration, an embedded
migration command, an initial `jobs` schema, a PostgreSQL job repository, and
`POST /jobs` and `GET /jobs/{id}` endpoints. Multiple workers safely claim jobs
using a short `FOR UPDATE SKIP LOCKED` transaction, then run the `echo` executor
and persist `SUCCEEDED` or `FAILED`. A claim records a worker owner, lease
expiry, and heartbeat; workers recover expired `RUNNING` jobs, and stale owners
cannot persist a terminal state after recovery. PostgreSQL integration tests
verify both concurrent claims and deterministic lease recovery. This is
at-least-once execution, not exactly-once delivery: a side effect can be
duplicated after a lease expires. Jobs have a bounded total attempt budget;
retryable failures receive durable exponential backoff through
`next_attempt_at`, while non-retryable failures are `FAILED` and exhausted
retryable jobs are `DEAD`. Workers do not sleep for backoff. A unique
client-supplied `Idempotency-Key` returns the same durable job on repeated
submission and is retained as the stable effect key across retries. External
executors must use that key with the target system's idempotency support; this
project still provides at-least-once, not global exactly-once, execution.
Eligible work is claimed in strict `HIGH`, `NORMAL`, then `LOW` priority order,
with an index matching the claim predicate and ordering. This can starve lower
priorities under sustained high-priority load; the project does not make a
fairness claim. Scheduling features are not yet implemented. Inspect the code
and tests; the roadmap is not proof that a feature exists.

## Phase boundaries

| Phase | Build now | Explicitly defer |
| --- | --- | --- |
| 1. Basic execution | PostgreSQL, migrations, `POST /jobs`, `GET /jobs/{id}`, one worker that polls, claims, executes, and records `QUEUED`, `RUNNING`, `SUCCEEDED`, or `FAILED` | Multi-worker claiming, leases, retries, priorities, schedules, DAGs, leadership, metrics, Kubernetes |
| 2. Concurrent claims | Transactional claims, likely `SELECT ... FOR UPDATE SKIP LOCKED`; prove many workers never claim one job twice | Leases and heartbeats |
| 3. Leases | Owner, expiry, heartbeat, and recovery of a `RUNNING` job after worker loss | Exactly-once claims |
| 4. Retries and DLQ | Attempts, retryability, exponential backoff, `next_attempt_at`, and a dead-letter outcome; workers do not sleep awaiting retries | Idempotency guarantees |
| 5. Idempotency | At-least-once delivery and protection against duplicated external side effects after a crash before success is persisted | Claims of exactly-once execution |
| 6. Priorities | `HIGH`, `NORMAL`, `LOW` claims with a documented starvation trade-off | Unbounded priority bypasses |
| 7. Scheduling | `run_at`, eligibility checks, later recurring schedules | Claiming future work early |
| 8. DAG workflows | Dependency storage, readiness on successful prerequisites, explicit failure policy, indexed readiness discovery | Full-table readiness scans |
| 9. Leadership | A simple database-appropriate coordination mechanism only if a scheduler operation requires one; explain failover and split brain | Raft or a custom consensus protocol |
| 10. Observability | Useful Prometheus metrics and Grafana dashboards: submitted/completed/failed/retried jobs, execution/queue latency, depth, active workers, failures | Metrics with no operational question |
| 11. Load and fault tests | Reproducible jobs/sec, p50/p95/p99, queue latency, recovery, 1/2/4/8-worker scaling, CPU/memory; worker/database/scheduler failure scenarios | Invented resume metrics |
| 12. Containers and Kubernetes | Compose for local services; later Deployments, StatefulSet, ConfigMaps, Secrets, probes, and worker scaling | Kubernetes before a working local app |
| 13. CI/CD | GitHub Actions formatting, linting, unit/integration/race tests, and Docker build | CI that claims unrun checks |

Phase 1 is complete only when a submitted job is processed by one worker, its
terminal state is queryable, tests pass, and the database setup is reproducible.

## Design rules

- Keep `cmd/` to process wiring. Keep domain, persistence, API, worker, and
  scheduler behavior in `internal/`; do not place business logic in `main.go`.
- Prefer small interfaces at real dependency boundaries. Do not introduce an
  abstraction merely because it might be useful later.
- Preserve the initial state machine exactly:

  ```text
  QUEUED -> RUNNING -> SUCCEEDED
                    -> FAILED
  ```

  Every new state or transition needs a reason, domain validation, persistence
  rules, and tests. `DEAD` is the Phase 4 dead-letter state; additional retry
  states belong to later phases.
- Workers claim work, execute outside the claim transaction, then persist a
  terminal outcome. Never hold a database transaction during execution.
- Use contexts, graceful shutdown, and structured logs. Never log a
  `DATABASE_URL`, credentials, or secrets.
- Migrations are ordered and immutable after use. Add a migration; do not edit
  an applied one. Index actual worker/API predicates and ordering.

## Testing and scope

- Use unit tests for pure domain behavior; use PostgreSQL integration tests for
  migrations, locks, transactions, and isolation; use `go test -race ./...`
  whenever concurrency is introduced.
- Avoid arbitrary sleeps and timing assumptions. State which checks were run,
  which passed, and which are only recommended.
- Do not add a custom message broker, database, consensus protocol, service
  mesh, multi-region design, Kubernetes operator, complex auth, or a large UI
  unless the user explicitly requests it. Optimize for correct depth, not
  feature count.

## Focused review briefs

- `distributed-systems-reviewer.md`: reliability, concurrency, delivery claims.
- `postgres-reviewer.md`: schema, locking, queries, and migrations.
- `api-worker-reviewer.md`: Phase 1 HTTP/worker boundaries and shutdown.
- `test-engineer.md`: smallest credible test plan and truthful verification.

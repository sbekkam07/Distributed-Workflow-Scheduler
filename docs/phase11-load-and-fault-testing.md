# Phase 11 load and fault testing

This phase supplies a repeatable harness and scenarios. It does **not** claim
that one run represents a benchmark for every machine, database configuration,
or workload.

## Load-test procedure

Start the API, scheduler, and the desired number of worker processes with the
same `DATABASE_URL`. Then run:

```bash
go run ./cmd/loadtest -jobs 1000 -concurrency 32 -timeout 5m > results-1-worker.json
```

The command submits `echo` jobs, waits for terminal results, and writes JSON
with:

- submitted and terminal counts plus a durable-outcome breakdown;
- end-to-end jobs per second for that exact run;
- p50/p95/p99 queue latency (created to claimed), execution latency (claimed
  to completed), and end-to-end latency (created to completed).

Repeat the same command with 1, 2, 4, and 8 worker processes. Give every
worker its own `WORKER_METRICS_ADDR`; record the matching `results-*.json`,
machine CPU/memory, PostgreSQL resource use, and the Prometheus/Grafana time
range. Do not compare runs with changed job count, concurrency, machine, or
database configuration without labeling that difference.

## Fault scenarios

| Scenario | Action | Expected durable observation |
| --- | --- | --- |
| Worker loss during execution | Submit a job, wait until `RUNNING`, then stop that worker. | After lease expiry, another worker reclaims the job. The side effect can repeat; this remains at-least-once. |
| Scheduler leader loss | Run two schedulers and stop the current leader. | PostgreSQL releases its session advisory lock; the remaining scheduler becomes leader and continues DAG failure reconciliation. |
| PostgreSQL interruption | Stop database connectivity briefly, then restore it. | API/worker/scheduler log an iteration error and retry; no successful claim transaction is half-committed. |
| Dependency failure | Submit a prerequisite and dependent, then make the prerequisite terminal `FAILED` or `DEAD`. | The scheduler marks the dependent `BLOCKED`; no worker executes it. |

The automated PostgreSQL tests cover transactional claims, lease recovery,
dependency failure propagation, and leadership failover. The manual scenarios
exercise real process and database boundaries that unit tests should not fake.

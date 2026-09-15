# Distributed Workflow Scheduler

A Go workflow scheduler built incrementally to explore reliable job execution.

## Current phase

Phase 1 complete: a single worker polls PostgreSQL, atomically claims one queued
job, runs the initial `echo` executor, and records its final state. Concurrent
worker coordination, leases, and retries remain later phases.

## Layout

```text
cmd/
  api/        API process entry point
  worker/     Worker process entry point
internal/
  config/     Process configuration
  httpapi/    HTTP transport and validation
  jobs/       Job domain rules and use cases
  postgres/   PostgreSQL connection, repositories, and migrations
  worker/     Polling and execution loop
```

`cmd` contains only process wiring. Business rules remain under `internal`, so
the API and worker can use the same rules without depending on one another.

## Job lifecycle

Jobs contain an executor `kind` and valid JSON `payload`. They move through one
strict lifecycle:

```text
QUEUED -> RUNNING -> SUCCEEDED
                  -> FAILED
```

`jobs` keeps this state machine explicit in Go, and the database migration
enforces matching timestamps and failure messages. The partial queued-job index
supports polling without indexing jobs that have already finished.

Migration files live in `internal/postgres/migrations` and are embedded in the
`migrate` executable. This ensures the executable carries exactly the schema it
will apply.

## Database setup

Set `DATABASE_URL` to a PostgreSQL connection URL. `DATABASE_MAX_CONNS` is
optional and defaults to `4` for each long-running service process.

```bash
export DATABASE_URL='postgres://scheduler:password@localhost:5432/scheduler?sslmode=disable'
go run ./cmd/migrate
go run ./cmd/api
```

The API listens on `:8080` by default; override it with `HTTP_ADDR`.

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"kind":"echo","payload":{"message":"hello"}}'

curl http://localhost:8080/jobs/<job-id>
```

In a separate terminal, using the same `DATABASE_URL`, start one worker:

```bash
go run ./cmd/worker
```

The current Phase 1 executor supports jobs with `"kind":"echo"`. It logs the
payload's `message` and then marks the job `SUCCEEDED`. An unsupported kind or
invalid executor payload is marked `FAILED`.

Run migrations as a separate deployment step, before starting API or worker
processes. Do not run them from every service instance: with future replicas,
that would turn a deployment concern into an unnecessary startup race.

## Project agents

`AGENTS.md` gives project-wide guidance. The focused briefs in `.agents/` can
be used to review distributed-systems design, PostgreSQL changes, and tests.

## Verify the skeleton

```bash
go test ./...
go run ./cmd/api
go run ./cmd/worker
```

## Next increment

Define the job model and create the initial PostgreSQL schema, then connect the
API process to a database-backed repository.

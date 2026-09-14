# Distributed Workflow Scheduler

A Go workflow scheduler built incrementally to explore reliable job execution.

## Current phase

Phase 1: basic job execution. The application structure is in place; HTTP and
PostgreSQL behavior will be added in the next increments.

## Layout

```text
cmd/
  api/        API process entry point
  worker/     Worker process entry point
internal/
  config/     Process configuration
  httpapi/    HTTP transport and validation
  jobs/       Job domain rules and use cases
  postgres/   PostgreSQL connection and repositories
  worker/     Polling and execution loop
migrations/   Ordered PostgreSQL schema migrations
```

`cmd` contains only process wiring. Business rules remain under `internal`, so
the API and worker can use the same rules without depending on one another.

## Verify the skeleton

```bash
go test ./...
go run ./cmd/api
go run ./cmd/worker
```

## Next increment

Define the job model and create the initial PostgreSQL schema, then connect the
API process to a database-backed repository.

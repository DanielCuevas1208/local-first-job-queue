# Local-first Durable Job Queue

A durable background job queue with SQLite storage. The queue supports leases, retries, idempotency keys, crash recovery, and an append-only event log.

## Architecture

The queue stores jobs in SQLite. A worker leases jobs for exclusive processing. If the worker crashes, expired leases are recovered on the next worker start. Each state change is recorded in an append-only event log.

Components:

- `internal/queue` -- Core queue API and SQLite storage.
- `internal/worker` -- Worker that leases and executes jobs.
- `internal/fixture` -- Sample data generator.
- `cmd/enqueue` -- CLI to add jobs.
- `cmd/inspect` -- CLI to view queue state and events.
- `cmd/work` -- CLI to start a worker.

## Quick start

Requires Go 1.23.

```
go build -o jobqueue .
./jobqueue enqueue -kind email -payload '{"to":"user@example.com"}'
./jobqueue work -kind email
./jobqueue inspect
```

The `inspect` command shows queue state, job list, and recent events.

### Sample data

```
go run ./internal/fixture/ -db queue.db
```

## Commands

### enqueue

Add a job to the queue.

```
jobqueue enqueue -kind <type> -payload <json> [-idempotency-key <key>] [-max-retries <n>] [-db <path>]
```

### work

Start a worker that processes jobs of a given kind.

```
jobqueue work -kind <type> [-concurrency <n>] [-db <path>]
```

The worker recovers orphaned leases on startup.

### inspect

View queue state, jobs, and event log.

```
jobqueue inspect [-json] [-db <path>]
```

## Features

### Leases

Leases prevent duplicate processing. A worker leases a job for a fixed duration. Only one worker can lease a given job at a time.

### Retries

When a job handler fails, the job is returned to the pending state with an incremented retry count. After exceeding MaxRetries, the job moves to failed.

### Idempotency keys

An idempotency key ensures that enqueuing the same logical job multiple times produces the same job ID.

### Crash recovery

On startup, the worker detects leases that have expired. Those jobs are returned to the pending state and retried.

### Event log

Every state change is appended to the events table. The log is readable with the inspect command.

## Tests

```
go test -v -count=1 -race ./...
```

## Benchmarks

```
go test -bench=. -benchmem ./internal/queue/
```

## Roadmap

- [ ] Job scheduling (delay execution until a specified time).
- [ ] Priority queues.
- [ ] Dead-letter queue for permanently failed jobs.
- [ ] Prometheus metrics.
- [ ] Web UI for queue inspection.
- [ ] Horizontal scaling with a shared SQLite file.

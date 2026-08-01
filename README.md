# Local-first Durable Job Queue

A durable background job queue with SQLite storage. It supports leases, retries,
idempotency keys, crash recovery, and an append-only event log.

## Architecture

The queue stores jobs in SQLite. A worker leases each job for exclusive
processing. If the worker crashes, the next worker recovers expired leases.
Each state change is recorded in the event log.

Components:

- `internal/queue` — Core queue API and SQLite storage.
- `internal/worker` — Worker that leases and executes jobs.
- `internal/fixture` — Sample data generator.
- `cmd/enqueue` — CLI to add jobs.
- `cmd/inspect` — CLI to view queue state and events.
- `cmd/work` — CLI to start a worker.

## Quick start

Requires Go 1.25.

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
jobqueue enqueue -kind <type> -payload <json> [-idempotency-key <key>] [-max-attempts <n>] [-run-at <RFC3339> | -run-after <duration>] [-db <path>]
```

Use `-run-at` to delay leasing until an absolute time. Use `-run-after` to delay leasing by a duration from enqueue. The two flags are mutually exclusive. A delayed job stays in the pending state until its run_at time passes.

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

### Scheduled jobs

A job can wait until a chosen time before a worker leases it. Set `WithRunAt` or `WithRunAfter` on enqueue. The lease query skips pending jobs whose run_at time is in the future. Once that time passes, the job becomes ready and leases in run_at order. The append-only log records a `scheduled` event for delayed jobs.

## Tests

```
go test -v -count=1 -race ./...
```

The test status on the main branch is set by CI. The workflow runs `go vet`, `go build`, `go test -race`, and the queue benchmarks on each push and pull request.

## Benchmarks

```
go test -bench=. -benchmem ./internal/queue/
```

## Sample output

```
$ go build -o jobqueue .
$ ./jobqueue enqueue -kind email -payload '{"to":"user@example.com"}' -run-after 2s
enqueued job 7bfb363d-... (email)
scheduled for 2026-07-28T19:01:03Z

$ ./jobqueue work -kind email
worker started kind="email" concurrency=1 lease=30s poll=1s
recovered 0 orphaned jobs
processed kind=email id=7bfb363d-... payload={"to":"user@example.com"}

$ ./jobqueue inspect
Queue state
-----------
  completed: 1

Recent events (3)
--------------
  [19:01:03] 7bfb363d scheduled 2026-07-28T19:01:03Z
  [19:01:05] 7bfb363d leased
  [19:01:05] 7bfb363d acknowledged

Jobs (1)
--------
  7bfb363d kind=email state=completed attempts=0/3
```

## Limitations

The queue uses one writer connection because SQLite serializes writes. A backlog
can grow when the writer cannot keep up. The store keeps all jobs and events
until an operator removes them. There is no priority field. Ready jobs of one
kind share one FIFO lane. The worker is one process and does not coordinate
across hosts.

## Roadmap

- [x] Job scheduling (delay execution until a specified time).
- [ ] Priority queues.
- [ ] Dead-letter queue for permanently failed jobs.
- [ ] Prometheus metrics.
- [ ] Web UI for queue inspection.
- [ ] Horizontal scaling with a shared SQLite file.

### Release notes

This release adds scheduled jobs. A new `run_at` column on the jobs table stores the earliest lease time. Existing databases gain the column on the next open through an idempotent migration. The lease query and sort use the column, so delayed jobs wait and then run in time order. The event log records a `scheduled` event for each delayed enqueue.

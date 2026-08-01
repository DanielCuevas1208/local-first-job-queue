# Local-first Durable Job Queue

A small durable background queue built with Go and SQLite.

The project shows leases, retries, idempotency, crash recovery, priority dispatch, and an append-only event log.

## Value

Use this project to study queue behavior without an external service.

Every lease, retry, recovery, and acknowledgement remains visible in SQLite.

The demo injects repeatable faults, so failure paths are easy to inspect.

## Architecture

The queue separates durable state from worker execution.

- `internal/queue` owns the public queue API and SQLite store.
- `internal/worker` leases jobs and runs handlers.
- `internal/fault` injects deterministic errors, panics, delays, and stalls.
- `internal/cli` renders commands, snapshots, history, and the demo.
- `internal/fixture` provides repeatable sample workloads.

A job starts as `pending`.

A worker claims it with a time-limited lease.

The worker acknowledges success or records failure.

An expired lease returns to the queue during recovery.

Each transition appends an event with a timestamp and optional metadata.

## Setup

Requires Go 1.25.

Run these commands from the repository root.

```text
go build -o jobqueue .
./jobqueue enqueue -kind email -payload '{"to":"user@example.com"}' -priority 20
./jobqueue work -kind email
./jobqueue inspect
```

Use `-priority` to place urgent work ahead of normal work.

Higher values run first.

The default priority is zero.

Use `-run-at` or `-run-after` to delay leasing.

Run the deterministic showcase with this command.

```text
go run . demo
```

Load sample jobs with this command.

```text
go run . seed -db queue.db
go run . inspect -db queue.db
```

## Commands

### `enqueue`

```text
jobqueue enqueue -kind <type> -payload <json> [-priority <n>] [-idempotency-key <key>] [-max-attempts <n>] [-run-at <RFC3339> | -run-after <duration>] [-db <path>]
```

`-priority` uses zero by default.

`-run-at` and `-run-after` cannot appear together.

### `work`

```text
jobqueue work -kind <type> [-concurrency <n>] [-lease <duration>] [-poll <duration>] [-db <path>]
```

The worker recovers expired leases when it starts.

### `inspect`

```text
jobqueue inspect [-json] [-db <path>]
```

The command prints state counts, recent events, and job details.

The JSON form supports scripts and other inspection tools.

### `history`

```text
jobqueue history <job-id> [-json] [-db <path>]
```

The command prints one job and its event timeline.

### `seed`

```text
jobqueue seed [-db <path>]
```

The command loads three idempotent jobs for each bundled workload.

### `demo`

```text
jobqueue demo [-db <path>] [-keep] [-run <duration>] [-kind <type>]
```

The demo combines priority, retries, panic recovery, crash recovery, and scheduling.

## Features

### Leases

A lease gives one worker temporary ownership of a job.

An expired lease becomes recoverable.

### Priority dispatch

Ready jobs with higher priority values lease first.

A future job cannot bypass its `run_at` time, even when its priority is higher.

Equal priorities use readiness time, creation time, and job ID as deterministic tie breakers.

### Retries

A failed handler returns the job to `pending` while attempts remain.

The job enters `failed` after the attempt budget is exhausted.

### Idempotency

An idempotency key makes repeated enqueue calls return one durable job.

The database enforces the uniqueness rule.

### Crash recovery

Startup recovery finds leases past their deadlines.

Recovery consumes an attempt and records a `recovered` event.

### Event log

Every state change appends one event row.

The `history` command shows one job's complete timeline.

### Scheduling

A scheduled job stores its earliest lease time in `run_at`.

SQLite stores queue timestamps with nanosecond precision.

Existing databases gain new columns through idempotent migrations.

## Sample output

Run `jobqueue demo` to see a complete local scenario.

```text
== Local-first Durable Job Queue: demo ==
enqueuing scenario jobs:
  first-try success      alpha                  priority= 0 <id>
  priority retry         beta                  priority=10 <id>
  orphaned by a crash    delta                 priority= 0 <id>

orphaned job delta was leased and then abandoned.
starting worker; it will recover orphans and process jobs.

queue drained before the run deadline.

Queue state
-----------
  completed: 5
  failed: 1

Recent events (29)
------------------
  [12:00:00] <id> acknowledged
  [12:00:00] <id> recovered attempt 1/3

Jobs (6)
--------
  <id> kind=demo priority=10 state=completed attempts=2/3
```

The demo uses generated job IDs and current timestamps.

The final counts depend on the scenario and run deadline.

## Verification

Run the full test suite with this command.

```text
go test -v -count=1 -race ./...
```

Run static checks with these commands.

```text
go vet ./...
go build ./...
```

Run queue benchmarks with this command.

```text
go test ./internal/queue -run '^$' -bench Benchmark -benchmem -count=1
```

Verification status: tests, vet, build, and benchmarks pass locally on Go 1.25.

Race tests run in CI on Ubuntu, where the required C compiler is available.

## Limitations

SQLite serializes writes through one store connection.

A sustained backlog can exceed the writer's capacity.

Jobs and events remain until an operator removes them.

A high-priority stream can delay lower-priority jobs.

The queue has no dead-letter workflow or priority aging.

The worker is one process and does not coordinate across hosts.

The project does not provide Prometheus metrics or a web interface.

## Roadmap

- [x] Durable leases, retries, idempotency, crash recovery, and event history.
- [x] Scheduled jobs with nanosecond-safe release times.
- [x] Priority-aware dispatch with deterministic ordering.
- [ ] Dead-letter queue for permanently failed jobs.
- [ ] Prometheus metrics.
- [ ] Web UI for queue inspection.
- [ ] Horizontal scaling with a shared SQLite file.

### Release notes

This release adds durable priority dispatch.

Jobs store an integer priority with a default of zero.

The lease query selects ready jobs by descending priority.

The migration adds `priority` to existing databases before creating its indexes.

The release also preserves sub-second schedule deadlines during SQLite writes.
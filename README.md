# Local-first Durable Job Queue

A small durable background queue built with Go and SQLite.

The project shows leases, retries, idempotency, crash recovery, priority dispatch, priority aging, a dead-letter queue, Prometheus metrics, and an append-only event log.

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
- `internal/metrics` renders queue state in the Prometheus text format.
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

Inspect and requeue a dead-lettered job with these commands.

```text
go run . history <id> -db queue.db
go run . requeue <id> -db queue.db
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
jobqueue work -kind <type> [-concurrency <n>] [-lease <duration>] [-poll <duration>] [-aging <duration>] [-metrics-addr <addr>] [-db <path>]
```

The worker recovers expired leases when it starts.

Priority aging is enabled by default with a 30-second interval.

A job gains one priority point per interval it waits.

Use `-aging 0` to disable aging.

Use `-metrics-addr` to serve Prometheus metrics beside the worker.

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

### `requeue`

```text
jobqueue requeue <job-id> [-db <path>] [-max-attempts <n>] [-payload <json>]
```

The command returns a dead-lettered job to the pending state.

The job resets its attempt count and keeps its data.

Use `-payload` to correct the job data before it runs again.

### `seed`

```text
jobqueue seed [-db <path>]
```

The command loads three idempotent jobs for each bundled workload.

### `metrics`

```text
jobqueue metrics [-once] [-addr <addr>] [-db <path>]
```

The command serves queue state in the Prometheus text format.

The default address is `:9090`.

Scrape the endpoint with a Prometheus server.

Use `-once` to print one snapshot and exit.

### `demo`

```text
jobqueue demo [-db <path>] [-keep] [-run <duration>] [-kind <type>]
```

The demo combines priority, retries, panic recovery, crash recovery, scheduling, priority aging, and dead-letter requeue.

## Features

### Leases

A lease gives one worker temporary ownership of a job.

An expired lease becomes recoverable.

### Priority dispatch

Ready jobs with higher priority values lease first.

A future job cannot bypass its `run_at` time, even when its priority is higher.

Equal priorities use readiness time, creation time, and job ID as deterministic tie breakers.

### Priority aging

A pending job gains one priority point per aging interval it waits.

The interval is a store setting; the default is 30 seconds.

The `work` command enables aging by default.

Use `-aging 0` to disable it.

An older low-priority job can overtake a fresher high-priority job.

The store measures the wait from the job's readiness time.

A scheduled job starts aging only when its `run_at` time passes.

Aging prevents a constant high-priority stream from starving other work.

The `demo` command shows a low-priority job winning after five intervals.

### Retries

A failed handler returns the job to `pending` while attempts remain.

The job enters the dead-letter queue after the attempt budget is exhausted.

### Idempotency

An idempotency key makes repeated enqueue calls return one durable job.

The database enforces the uniqueness rule.

### Dead-letter queue

A job that exhausts its attempts enters the `dead_letter` state.

The event log records one `dead_lettered` event per exhausted job.

Use the `requeue` command to return a dead-lettered job to `pending`.

The job keeps its data unless the command supplies a new payload.

A requeued job resets its attempt count and can fail again.

### Crash recovery

Startup recovery finds leases past their deadlines.

Recovery consumes an attempt and records a `recovered` event.

A recovered job with no attempts left enters the dead-letter queue.

### Event log

Every state change appends one event row.

The `history` command shows one job's complete timeline.

### Metrics

The exporter renders queue state in the Prometheus text format.

Each scrape computes a fresh snapshot from the SQLite store.

The exporter reports four metric families.

`jobqueue_jobs` counts jobs by state.

`jobqueue_jobs_by_kind` counts jobs by kind and state.

`jobqueue_events_total` counts events by type.

`jobqueue_oldest_pending_seconds` reports the oldest pending job's age.

Every known state and event type appears with an explicit zero.

The output order stays stable across scrapes.

Use the `metrics` command for one snapshot or a live endpoint.

Use `work -metrics-addr` to serve the same endpoint beside a worker.

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
  priority retry         beta                   priority=10 <id>
  exhausts attempts      gamma                  priority= 0 <id>
  panic then ok          epsilon                priority= 0 <id>
  orphaned by a crash    delta                  priority= 0 <id>
  delayed run            omega                  priority= 0 <id>

orphaned job delta was leased and then abandoned.
starting worker; it will recover orphans and process jobs.

queue drained before the run deadline.

Dead-letter queue
-----------------
  <id> kind=demo priority=0 state=dead_letter attempts=3/3

operator requeues the dead-lettered job with a corrected payload.
starting worker again; it will process the requeued job.

queue drained before the run deadline.

Priority aging
--------------
aging interval: 100ms; a job gains one priority point per interval it waits.
  aged  (low priority)   priority= 0 waited=5 intervals effective=5
  fresh (high priority)  priority= 1 waited=0 intervals effective=1

lease order: <id> (aged) then <id> (fresh)
the waiting job outranks the fresher higher-priority job.

Queue state
-----------
  completed: 6

Recent events (32)
-----------------
  [12:00:00] <id> requeued attempts reset to 0/3
  [12:00:00] <id> dead_lettered attempt 3/3 exhausted: disk full

Jobs (6)
--------
  <id> kind=demo priority=0 state=completed attempts=0/3

Metrics
-------
# HELP jobqueue_jobs Number of jobs in each state.
# TYPE jobqueue_jobs gauge
jobqueue_jobs{state="pending"} 0
jobqueue_jobs{state="leased"} 0
jobqueue_jobs{state="completed"} 6
jobqueue_jobs{state="dead_letter"} 0
jobqueue_jobs{state="failed"} 0
# HELP jobqueue_events_total Number of events per event type.
# TYPE jobqueue_events_total counter
jobqueue_events_total{type="enqueued"} 5
jobqueue_events_total{type="retried"} 5
jobqueue_events_total{type="dead_lettered"} 1
jobqueue_events_total{type="requeued"} 1
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

Verification status: tests, vet, build, and benchmarks pass locally and in CI.

Race tests run in CI on Ubuntu.

## Limitations

SQLite serializes writes through one store connection.

A sustained backlog can exceed the writer's capacity.

Jobs and events remain until an operator removes them.

A high-priority stream can delay lower-priority jobs until aging lifts them.

The worker is one process and does not coordinate across hosts.

The project does not provide a web interface.

## Roadmap

- [x] Durable leases, retries, idempotency, crash recovery, and event history.
- [x] Scheduled jobs with nanosecond-safe release times.
- [x] Priority-aware dispatch with deterministic ordering.
- [x] Priority aging to prevent starvation.
- [x] Dead-letter queue with requeue of permanently failed jobs.
- [x] Prometheus metrics for queue inspection.
- [ ] Web UI for queue inspection.
- [ ] Horizontal scaling with a shared SQLite file.

### Release notes

This release adds Prometheus metrics.

The new `metrics` command serves the exposition format over HTTP.

Use `-once` to print one snapshot instead.

The `work` command can serve the same endpoint beside a worker.

The demo prints the final metrics snapshot.

Each scrape reads the SQLite store and reports current state.

The previous release added priority aging to prevent starvation.

A pending job gains one priority point per aging interval it waits.

The default aging interval is 30 seconds.

The `work` command enables aging by default.

Use `-aging 0` to disable aging.

The store measures the wait from the job's readiness time.

A scheduled job starts aging only when its `run_at` time passes.

The library keeps aging opt-in, so callers keep their exact ordering.

The demo now shows a low-priority job overtaking a fresher one.

The previous release added a dead-letter queue for jobs that exhaust their attempts.

A job enters the `dead_letter` state after its attempt budget runs out.

The event log records a `dead_lettered` event for each exhausted job.

The new `requeue` command returns a dead-lettered job to `pending`.

The command can supply a new payload and a new attempt budget.

The demo now shows the full dead-letter workflow.

The previous release added durable priority dispatch.

Jobs store an integer priority with a default of zero.

The lease query selects ready jobs by descending priority.

The migration adds `priority` to existing databases before creating its indexes.

That release also preserved sub-second schedule deadlines during SQLite writes.

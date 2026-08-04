# Local-first Durable Job Queue

A small durable background queue built with Go and SQLite.

The project shows leases, retries, idempotency, and crash recovery.

It adds priority dispatch, priority aging, and a dead-letter queue.

It provides Prometheus metrics and an append-only event log.

It includes a browser dashboard and shared-file horizontal scaling.

The dashboard can require HTTP Basic credentials for shared deployments.

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
- `internal/web` serves a read-only browser dashboard and a small JSON API.

A job starts as `pending`.

A worker claims it with a time-limited lease.

The worker acknowledges success or records failure.

An expired lease returns to the queue during recovery.

Each transition appends an event with a timestamp and optional metadata.

A lease claim runs as one atomic write.

Several worker processes can share one SQLite file without double-leasing a job.

## Setup

Requires Go 1.25.

Run these commands from the repository root.

```text
go build -o jobqueue .
./jobqueue enqueue -kind email -payload '{"to":"user@example.com"}' -priority 20
./jobqueue work -kind email
./jobqueue inspect
./jobqueue web
```

Use `-priority` to place urgent work ahead of normal work.

Higher values run first.

The default priority is zero.

Use `-run-at` or `-run-after` to delay leasing.

Run the deterministic showcase with this command.

```text
go run . demo
```

The demo prints a command that opens the dashboard for its database.

Run a worker with a browser dashboard on the same database.

```text
./jobqueue work -kind email &
./jobqueue web
```

Open the printed address in a browser.

Run several workers on one database file.

```text
./jobqueue work -kind email &
./jobqueue work -kind email &
./jobqueue web
```

Each worker claims jobs atomically from the shared file.

No job runs twice, even when workers start together.

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
jobqueue work -kind <type> [-concurrency <n>] [-lease <duration>] [-poll <duration>] [-aging <duration>] [-metrics-addr <addr>] [-web-addr <addr>] [-web-user <name>] [-web-pass <password>] [-db <path>]
```

The worker recovers expired leases when it starts.

Priority aging is enabled by default with a 30-second interval.

A job gains one priority point per interval it waits.

Use `-aging 0` to disable aging.

Use `-metrics-addr` to serve Prometheus metrics beside the worker.

Use `-web-addr` to serve the web dashboard beside the worker.

Use `-web-user` and `-web-pass` to require credentials for that dashboard.

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

### `web`

```text
jobqueue web [-addr <addr>] [-db <path>] [-user <name>] [-pass <password>]
```

The command serves a read-only browser dashboard.

The default address is `:8080`.

The dashboard shows state counts, filters, and a live job table.

Each job page shows its payload and full event timeline.

The page refreshes itself every five seconds.

The server also exposes a JSON API under `/api`.

Use `-user` and `-pass` to require credentials for every page and API route.

The health endpoint stays open so load balancers can probe it.

Use `-addr 127.0.0.1:8080` to bind to one interface.

## Features

### Leases

A lease gives one worker temporary ownership of a job.

An expired lease becomes recoverable.

One atomic statement claims a lease, so shared-file workers stay safe.

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

### Horizontal scaling

Several worker processes can share one SQLite file.

Each worker opens its own store connection.

The lease claim runs as one atomic SQL statement.

Two workers cannot lease the same job, even when they claim together.

Concurrent recovery applies each orphan exactly once.

WAL mode lets one writer and many readers work at the same time.

Scale worker processes without changing the code.

SQLite serializes writers, so the shared file has a write ceiling.

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

### Web dashboard

The `web` command serves a read-only dashboard in a browser.

The dashboard reads the same SQLite store as every other command.

It shows one count card per state, plus a total event count.

State, kind, and search filters narrow the job table.

The search field matches job IDs, idempotency keys, and payloads.

Each job links to a detail page with its full event timeline.

The page refreshes every five seconds while the tab is open.

The server embeds all templates and static files in the binary.

The JSON API supports scripts and other inspection tools.

`GET /api/summary` reports state counts, kinds, and the event total.

`GET /api/jobs` lists matching jobs with pagination fields.

`GET /api/jobs/<id>` returns one job with its event timeline.

`GET /healthz` reports readiness for load balancers.

The dashboard never writes to the store.

It is safe to run beside a worker on the same database.

Use `work -web-addr` to serve the dashboard beside a worker.

### Authentication

Use `-user` and `-pass` to protect the dashboard with HTTP Basic Auth.

Every page and API route then requires the credentials.

The health endpoint stays open so load balancers can probe readiness.

A wrong or missing credential returns a 401 response.

The browser shows its own prompt for the username and password.

The comparison uses constant-time routines to resist timing probes.

The password travels in clear text unless you add TLS.

Put the dashboard behind a reverse proxy for HTTPS.

The dashboard stays read-only, so a leaked credential cannot change jobs.

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

Point a browser at the dashboard to inspect the same store.

```text
./jobqueue web -db queue.db
```

The JSON API returns the same view for scripts.

```text
curl -s http://localhost:8080/api/jobs?state=pending&limit=2
```

Protect the dashboard with credentials for a shared deployment.

```text
./jobqueue web -db queue.db -user admin -pass 'change-me'
```

Scripts pass the credentials in the Authorization header.

```text
curl -s -u admin:'change-me' http://localhost:8080/api/jobs?state=pending&limit=2
```

```json
{
  "jobs": [
    {
      "id": "3f9e1b2a-7d0c-4b6a-9e2f-1c8d4a7b5e01",
      "kind": "email",
      "payload": "{\"to\":\"user@example.com\"}",
      "state": "pending",
      "retry_count": 0,
      "max_attempts": 3,
      "priority": 20,
      "created_at": "2026-08-04T05:53:56.083712Z",
      "updated_at": "2026-08-04T05:53:56.083712Z"
    }
  ],
  "total": 1,
  "limit": 2,
  "offset": 0
}
```

The dashboard and the API read the live SQLite store.

The demo uses generated job IDs and current timestamps.

The final counts depend on the scenario and run deadline.

The demo ends with commands to inspect the results.

The final line opens the same database in a browser dashboard.

```text
view it in a browser with: jobqueue web -db "queue.db"
```

The dashboard shows the same state, events, and metrics in a page.

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

The web tests cover the dashboard, the JSON API, payload escaping, and access control.

The shared-file tests cover multi-process leases, recovery, and worker scaling.

## Limitations

SQLite serializes writes through one store connection.

A sustained backlog can exceed the writer's capacity.

Jobs and events remain until an operator removes them.

A high-priority stream can delay lower-priority jobs until aging lifts them.

Workers share one file, so scaling across hosts needs a shared filesystem.

The dashboard is read-only and does not manage jobs.

Basic Auth sends the password in clear text without TLS.

Add a reverse proxy with HTTPS for any shared deployment.

## Roadmap

- [x] Durable leases, retries, idempotency, crash recovery, and event history.
- [x] Scheduled jobs with nanosecond-safe release times.
- [x] Priority-aware dispatch with deterministic ordering.
- [x] Priority aging to prevent starvation.
- [x] Dead-letter queue with requeue of permanently failed jobs.
- [x] Prometheus metrics for queue inspection.
- [x] Browser dashboard for queue inspection.
- [x] Horizontal scaling with a shared SQLite file.
- [x] Authenticated dashboard access for shared deployments.
- [ ] Job retention policies and event log cleanup.

### Release notes

This release adds optional authentication for the web dashboard.

The `web` command accepts `-user` and `-pass` credentials.

Every page and JSON API route then requires HTTP Basic Auth.

The health endpoint stays open for load balancers.

The `work` command passes the same options to its dashboard.

Constant-time comparison resists timing attacks on the password.

New tests cover anonymous, valid, and wrong credentials.

The previous release added horizontal scaling with a shared SQLite file.

A lease claim now runs as one atomic SQL statement.

Several worker processes can share one database file without double-leasing a job.

Concurrent crash recovery applies each orphan exactly once.

The store enables WAL mode with a normal sync mode for shared access.

New tests race two store connections and two worker processes against one file.

The tests prove that every job is claimed and acknowledged exactly once.

The dashboard and the demo work unchanged beside any number of workers.

The previous release added a read-only browser dashboard.

The new `web` command serves the dashboard and a JSON API.

The dashboard shows state counts, filters, and a live job table.

Each job page shows its payload and full event timeline.

The server embeds its templates and static files in the binary.

The page refreshes every five seconds while the tab is open.

The dashboard reads the same SQLite store as every command.

The release also adds filtered store queries for the dashboard.

`SearchJobs` returns a filtered, paginated job list.

`CountJobs` reports the total across all pages.

`GetKinds` lists the distinct job kinds in a stable order.

This release upgrades the SQLite driver and GitHub Actions.

The driver moves from `modernc.org/sqlite` 1.54.0 to 1.55.0.

The workflow moves `actions/checkout` and `actions/setup-go` to v7.

The previous release added Prometheus metrics.

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

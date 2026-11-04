# Roadmap

This roadmap records completed releases, the current slice, and bounded follow-up work.

## Completed releases

### `core-durability` — complete

Outcome: Durable leases, retries, idempotency keys, crash recovery, and event history.

Verification: `go test ./internal/queue -run 'Test(EnqueueAndInspect|Lease|FailAndRetry|FullLifecycle)$'`

### `scheduling-priority` — complete

Outcome: Scheduled jobs, deterministic priority dispatch, and priority aging.

Verification: `go test ./internal/queue -run 'Test(Priority|Scheduled|FutureHighPriority)'`

### `dead-letter-recovery` — complete

Outcome: Dead-letter storage and operator requeue with payload correction.

Verification: `go test ./internal/queue -run 'TestDeadLetterEventsAndRequeue|TestRequeue'`

### `operational-surface` — complete

Outcome: Prometheus metrics, browser inspection, authenticated access, shared-file workers, and dashboard requeue.

Verification: `go test ./internal/metrics ./internal/web ./internal/queue -run 'Test'`

### `retention` — complete

Outcome: Manual and scheduled retention with event cleanup and activity reporting.

Verification: `go test ./internal/queue ./internal/worker ./internal/cli -run 'Test.*Retention|TestPrune'`

### `history-export` — complete

Outcome: Versioned JSON Lines export with stable ordering and kind or state filters.

Verification: `go test ./internal/cli -run 'TestExport'`

### `lease-fencing` — complete

Outcome: Lease renewal and fencing tokens protect long-running handlers from stale workers.

Verification: `go test ./internal/queue ./internal/worker -run 'Test.*Lease|Test.*Renew'`

## Current release

### `history-verification` — complete

User-visible outcome: Operators can verify an exported history archive without opening the live queue.

Bounded deliverable: Add the `verify` command and an in-memory archive verifier.

Acceptance criteria:

- [x] Read versioned JSON Lines records into an isolated verifier.
- [x] Report broken event references and invalid state transitions.
- [x] Continue after corrupt lines and report source line numbers.
- [x] Test valid and corrupt fixtures.
- [x] Prove live database state remains unchanged during verification.
- [x] Show verification evidence in the deterministic demo.

Verification commands:

```text
go test ./internal/archive ./internal/cli -run 'TestVerify'
go run . demo -run 3s
```

Evidence: The verifier tests pass, and the demo reports `live queue unchanged: true`.

## Planned follow-up

### `workflow-dependencies` — planned

User-visible outcome: A job waits for named predecessors before a worker can lease it.

Bounded deliverable: Add dependency edges, cycle rejection, predecessor release, and restart recovery.

Acceptance criteria:

- [ ] Release a dependent job only after every predecessor succeeds.
- [ ] Reject cycles during submission.
- [ ] Keep dependents blocked after a predecessor fails.
- [ ] Recover dependency state after a restart.

Verification command: `go test ./internal/queue ./internal/worker -run 'Test.*Depend'`

### `stream-checkpoints` — planned

User-visible outcome: Operators can replay one keyed event-time summary from a durable checkpoint.

Bounded deliverable: Add one keyed window aggregation with explicit late-event handling and checkpoint recovery.

Acceptance criteria:

- [ ] Compute one keyed time-window aggregation.
- [ ] Record the late-event policy in the result.
- [ ] Persist checkpoints beside the queue data.
- [ ] Replay correctly after a crash between processing and checkpoint writes.

Verification command: `go test ./internal/stream -run 'Test.*Checkpoint'`

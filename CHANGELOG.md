# Changelog

All notable changes to this project are listed here.

The format follows Keep a Changelog.
This project uses calendar versioning by release.

## Unreleased

### Added

- Retention with the `purge` command. It removes finished jobs and their events in one transaction.
- The `purge -dry-run` flag previews a removal without changing the store.
- The `purge -state` and `purge -before` flags name states and set an age cutoff.
- The demo shows a retention segment with a dry run and a real purge.
- Deterministic tests for purge selection, age bounds, and event removal.

## 2026-08-03

### Added

- Prometheus metrics for queue inspection.
- The `metrics` command serves the exposition format over HTTP.
- The `work` command can serve the same endpoint beside a worker.
- Priority aging prevents starvation of low-priority work.
- The demo shows a low-priority job overtaking a fresher high-priority job.

## 2026-08-01

### Added

- Dead-letter queue with the `requeue` command.
- Durable priority dispatch with deterministic ordering.
- Scheduled jobs with nanosecond-safe release times.

## 2026-07-28

### Added

- Durable leases, retries, idempotency, crash recovery, and event history.
- The `enqueue`, `work`, `inspect`, `history`, `seed`, and `demo` commands.
- A deterministic fault-injection harness for worker handlers.

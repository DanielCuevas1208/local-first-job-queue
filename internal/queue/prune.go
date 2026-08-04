package queue

import (
	"fmt"
	"time"
)

// PrunePolicy describes what one retention run may remove from the queue. A
// zero-value policy removes nothing. Operators combine the two limits to keep
// the store small: old terminal jobs leave with their events, and every job
// that stays keeps only its newest events.
type PrunePolicy struct {
	// MaxJobAge removes jobs in a terminal state whose last update is older
	// than the age. Pending, leased, and scheduled jobs never match, because
	// they may still make progress. A non-positive value disables the limit.
	MaxJobAge time.Duration
	// MaxEventsPerJob keeps only the newest events for each surviving job,
	// which caps the event log for long-lived jobs. A non-positive value
	// disables the limit.
	MaxEventsPerJob int
}

// PruneResult reports how many rows one retention run removed. Operators and
// the CLI use the counts to confirm that a policy applied.
type PruneResult struct {
	JobsRemoved   int `json:"jobs_removed"`
	EventsRemoved int `json:"events_removed"`
}

// Prune applies a retention policy in one transaction and records the run as a
// manual retention action. The age limit removes terminal jobs and their
// events, and the per-job event cap trims the log of the jobs that remain. The
// transaction keeps the two deletes consistent, so a job can never lose its
// events while it survives. A concurrent writer is excluded until the run
// commits, matching the single-writer lease model.
func (s *SQLiteStore) Prune(p PrunePolicy) (PruneResult, error) {
	return s.PruneWithSource(p, RetentionSourceManual)
}

// PruneWithSource applies a retention policy and records the run under the
// supplied source. Manual runs come from the prune command or a direct Prune
// call. Automatic runs come from a worker that applies the policy on a
// schedule. The recorded log lets metrics and the dashboard report retention
// activity without a separate collector.
func (s *SQLiteStore) PruneWithSource(p PrunePolicy, source RetentionSource) (PruneResult, error) {
	if p.MaxJobAge <= 0 && p.MaxEventsPerJob <= 0 {
		return PruneResult{}, nil
	}
	if source != RetentionSourceManual && source != RetentionSourceAuto {
		return PruneResult{}, fmt.Errorf("unknown retention source %q", source)
	}
	startedAt := time.Now().UTC()
	tx, err := s.db.Begin()
	if err != nil {
		return PruneResult{}, fmt.Errorf("begin prune: %w", err)
	}
	defer tx.Rollback()

	res := PruneResult{}
	if p.MaxJobAge > 0 {
		cutoff := startedAt.Add(-p.MaxJobAge).Format(sqliteTimeFormat)
		events, err := tx.Exec(
			`DELETE FROM events WHERE job_id IN (
				SELECT id FROM jobs
				WHERE state IN (?, ?, ?) AND updated_at < ?
			)`,
			string(StateCompleted), string(StateDeadLetter), string(StateFailed), cutoff)
		if err != nil {
			return PruneResult{}, fmt.Errorf("delete events of old jobs: %w", err)
		}
		n, err := events.RowsAffected()
		if err != nil {
			return PruneResult{}, fmt.Errorf("count deleted events: %w", err)
		}
		res.EventsRemoved += int(n)

		jobs, err := tx.Exec(
			`DELETE FROM jobs
			 WHERE state IN (?, ?, ?) AND updated_at < ?`,
			string(StateCompleted), string(StateDeadLetter), string(StateFailed), cutoff)
		if err != nil {
			return PruneResult{}, fmt.Errorf("delete old jobs: %w", err)
		}
		n, err = jobs.RowsAffected()
		if err != nil {
			return PruneResult{}, fmt.Errorf("count deleted jobs: %w", err)
		}
		res.JobsRemoved += int(n)
	}

	if p.MaxEventsPerJob > 0 {
		events, err := tx.Exec(
			`DELETE FROM events WHERE id IN (
				SELECT id FROM (
					SELECT id, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY id DESC) AS rn
					FROM events
				) WHERE rn > ?
			)`,
			p.MaxEventsPerJob)
		if err != nil {
			return PruneResult{}, fmt.Errorf("trim event log: %w", err)
		}
		n, err := events.RowsAffected()
		if err != nil {
			return PruneResult{}, fmt.Errorf("count trimmed events: %w", err)
		}
		res.EventsRemoved += int(n)
	}

	if _, err := tx.Exec(
		`INSERT INTO retention_runs (started_at, source, max_job_age, max_events_per_job, jobs_removed, events_removed)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		startedAt.Format(sqliteTimeFormat), string(source),
		p.MaxJobAge.String(), p.MaxEventsPerJob, res.JobsRemoved, res.EventsRemoved); err != nil {
		return PruneResult{}, fmt.Errorf("record retention run: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return PruneResult{}, fmt.Errorf("commit prune: %w", err)
	}
	return res, nil
}

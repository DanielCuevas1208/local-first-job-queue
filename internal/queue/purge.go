package queue

import (
	"fmt"
	"strings"
	"time"
)

// PurgeOption configures a purge.
type PurgeOption func(*purgeConfig)

type purgeConfig struct {
	states    []JobState
	statesSet bool
	before    *time.Time
}

// PurgeStates limits a purge to the given job states. When the option is
// absent, a purge targets the terminal states: completed, failed, and
// dead_letter. Pending and leased jobs are never removed unless a caller names
// those states explicitly. An empty call removes nothing.
func PurgeStates(states ...JobState) PurgeOption {
	return func(c *purgeConfig) {
		c.states = append(c.states, states...)
		c.statesSet = true
	}
}

// PurgeBefore removes only jobs whose last update is older than the given
// time. When the option is absent, a purge ignores job age. The comparison
// uses the job's updated_at field, which records when the job last changed
// state.
func PurgeBefore(t time.Time) PurgeOption {
	return func(c *purgeConfig) {
		v := t.UTC()
		c.before = &v
	}
}

// PurgeStats reports what one purge removed.
type PurgeStats struct {
	JobsRemoved   int `json:"jobs_removed"`
	EventsRemoved int `json:"events_removed"`
}

// resolvePurgeConfig turns the options into the effective state set and age
// filter. The default state set is terminal, so a plain purge never touches
// work that is still pending or leased. An explicit empty state set stays
// empty and therefore removes nothing.
func resolvePurgeConfig(opts []PurgeOption) (states []JobState, before *time.Time) {
	cfg := purgeConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	states = cfg.states
	if !cfg.statesSet {
		states = []JobState{StateCompleted, StateFailed, StateDeadLetter}
	}
	return states, cfg.before
}

// Purge removes finished jobs and their events from the store. Each removed
// job takes its event rows with it, so the append-only log stays consistent
// with the remaining jobs. The default target set is the terminal states. Use
// PurgeBefore to keep recent history and PurgeStates to name different states.
func (q *Queue) Purge(opts ...PurgeOption) (PurgeStats, error) {
	states, before := resolvePurgeConfig(opts)
	removedJobs, removedEvents, err := q.store.PurgeJobs(states, before)
	if err != nil {
		return PurgeStats{}, fmt.Errorf("purge: %w", err)
	}
	return PurgeStats{JobsRemoved: removedJobs, EventsRemoved: removedEvents}, nil
}

// PurgeCandidates reports what Purge would remove without changing the store.
// Operators use it to preview a retention policy before applying it.
func (q *Queue) PurgeCandidates(opts ...PurgeOption) (PurgeStats, error) {
	states, before := resolvePurgeConfig(opts)
	jobs, events, err := q.store.CountPurgeCandidates(states, before)
	if err != nil {
		return PurgeStats{}, fmt.Errorf("count purge: %w", err)
	}
	return PurgeStats{JobsRemoved: jobs, EventsRemoved: events}, nil
}

// PurgeJobs removes jobs in the given states, and the events of those jobs,
// in one transaction. When before is non-nil, only jobs whose updated_at is
// older than that time are removed. The method returns the number of jobs and
// events removed. A removed job's events are removed with it, so the
// append-only log stays consistent with the remaining jobs.
func (s *SQLiteStore) PurgeJobs(states []JobState, before *time.Time) (int, int, error) {
	if len(states) == 0 {
		return 0, 0, nil
	}
	where, args := purgeWhere(states, before)

	tx, err := s.db.Begin()
	if err != nil {
		return 0, 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// The event rows are deleted first. Foreign keys are not enforced, so the
	// order is what keeps the log consistent with the remaining jobs.
	eventsRes, err := tx.Exec(`DELETE FROM events WHERE job_id IN (SELECT id FROM jobs WHERE `+where+`)`, args...)
	if err != nil {
		return 0, 0, fmt.Errorf("purge events: %w", err)
	}
	jobsRes, err := tx.Exec(`DELETE FROM jobs WHERE `+where, args...)
	if err != nil {
		return 0, 0, fmt.Errorf("purge jobs: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit purge: %w", err)
	}

	removedJobs, err := jobsRes.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("jobs removed: %w", err)
	}
	removedEvents, err := eventsRes.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("events removed: %w", err)
	}
	return int(removedJobs), int(removedEvents), nil
}

// CountPurgeCandidates reports how many jobs a purge would remove, along with
// the number of their events. It reads the same rows that PurgeJobs deletes,
// so the preview matches a real run.
func (s *SQLiteStore) CountPurgeCandidates(states []JobState, before *time.Time) (int, int, error) {
	if len(states) == 0 {
		return 0, 0, nil
	}
	where, args := purgeWhere(states, before)

	var jobs int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE `+where, args...).Scan(&jobs); err != nil {
		return 0, 0, fmt.Errorf("count jobs: %w", err)
	}
	var events int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE job_id IN (SELECT id FROM jobs WHERE `+where+`)`, args...).Scan(&events); err != nil {
		return 0, 0, fmt.Errorf("count events: %w", err)
	}
	return jobs, events, nil
}

// purgeWhere builds the WHERE clause that selects the rows a purge touches.
// The state placeholders appear first and the optional age bound last, so one
// argument slice serves both the job and the event queries.
func purgeWhere(states []JobState, before *time.Time) (string, []any) {
	placeholders := strings.TrimRight(strings.Repeat("?,", len(states)), ",")
	where := `state IN (` + placeholders + `)`
	args := make([]any, 0, len(states)+1)
	for _, st := range states {
		args = append(args, string(st))
	}
	if before != nil {
		where += ` AND updated_at < ?`
		args = append(args, before.UTC().Format(sqliteTimeFormat))
	}
	return where, args
}

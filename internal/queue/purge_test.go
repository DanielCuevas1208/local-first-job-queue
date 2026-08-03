package queue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// seedPurgeLifecycle builds a store with one job in each state: completed,
// dead_letter, leased, and pending. Each job uses its own kind so the leases
// are unambiguous. The function returns the store and the pending job ID.
func seedPurgeLifecycle(t *testing.T) (*SQLiteStore, string) {
	t.Helper()
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	if _, err := q.Enqueue("done", `{}`); err != nil {
		t.Fatalf("enqueue done: %v", err)
	}
	if job, err := q.Lease(ctx, "done", time.Minute); err != nil || job == nil {
		t.Fatalf("lease done: job=%v err=%v", job, err)
	} else if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("ack done: %v", err)
	}

	if _, err := q.Enqueue("lost", `{}`, WithMaxAttempts(1)); err != nil {
		t.Fatalf("enqueue lost: %v", err)
	}
	if job, err := q.Lease(ctx, "lost", time.Minute); err != nil || job == nil {
		t.Fatalf("lease lost: job=%v err=%v", job, err)
	} else if err := q.Fail(job.ID, "boom"); err != nil {
		t.Fatalf("fail lost: %v", err)
	}

	if _, err := q.Enqueue("held", `{}`); err != nil {
		t.Fatalf("enqueue held: %v", err)
	}
	if _, err := q.Lease(ctx, "held", time.Minute); err != nil {
		t.Fatalf("lease held: %v", err)
	}

	pending, err := q.Enqueue("waiting", `{}`)
	if err != nil {
		t.Fatalf("enqueue waiting: %v", err)
	}

	stats, err := s.GetQueueStats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	want := map[JobState]int{
		StateCompleted:  1,
		StateDeadLetter: 1,
		StateLeased:     1,
		StatePending:    1,
	}
	for state, count := range want {
		if stats[state] != count {
			t.Fatalf("seed state %s: got %d, want %d", state, stats[state], count)
		}
	}

	return s, pending.ID
}

// TestPurgeDefaultsToTerminalStates verifies that a purge without options
// removes completed and dead-lettered jobs while leaving pending and leased
// work untouched.
func TestPurgeDefaultsToTerminalStates(t *testing.T) {
	s, pendingID := seedPurgeLifecycle(t)
	q := NewQueue(s)

	stats, err := q.Purge()
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if stats.JobsRemoved != 2 {
		t.Errorf("expected 2 jobs removed, got %d", stats.JobsRemoved)
	}
	if stats.EventsRemoved != 6 {
		t.Errorf("expected 6 events removed (3 per terminal job), got %d", stats.EventsRemoved)
	}

	jobs, err := s.GetAllJobs()
	if err != nil {
		t.Fatalf("get jobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 remaining jobs, got %d", len(jobs))
	}
	states := map[JobState]bool{}
	for _, j := range jobs {
		states[j.State] = true
		if j.ID == pendingID && j.State != StatePending {
			t.Errorf("pending job must survive the purge, got state %s", j.State)
		}
	}
	if !states[StatePending] || !states[StateLeased] {
		t.Errorf("expected pending and leased jobs to survive, got %v", states)
	}
}

// TestPurgeStatesSelectsExactStates verifies that naming states limits the
// purge to exactly those states.
func TestPurgeStatesSelectsExactStates(t *testing.T) {
	s, _ := seedPurgeLifecycle(t)
	q := NewQueue(s)

	stats, err := q.Purge(PurgeStates(StateDeadLetter))
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if stats.JobsRemoved != 1 {
		t.Errorf("expected 1 dead-letter job removed, got %d", stats.JobsRemoved)
	}
	if stats.EventsRemoved != 3 {
		t.Errorf("expected 3 events removed, got %d", stats.EventsRemoved)
	}

	snap, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if snap.Stats[StateDeadLetter] != 0 {
		t.Errorf("expected no dead-letter jobs, got %d", snap.Stats[StateDeadLetter])
	}
	if snap.Stats[StateCompleted] != 1 {
		t.Errorf("expected completed job to survive, got %d", snap.Stats[StateCompleted])
	}
}

// TestPurgeRemovesEventsWithJobs verifies that the events of purged jobs are
// deleted while the events of surviving jobs remain readable.
func TestPurgeRemovesEventsWithJobs(t *testing.T) {
	s, pendingID := seedPurgeLifecycle(t)
	q := NewQueue(s)

	if _, err := q.Purge(PurgeStates(StateCompleted)); err != nil {
		t.Fatalf("purge: %v", err)
	}

	survivors, err := s.GetAllJobs()
	if err != nil {
		t.Fatalf("get jobs: %v", err)
	}
	for _, j := range survivors {
		if j.State == StateCompleted {
			t.Errorf("completed job %s must be gone", j.ID)
		}
		events, err := s.GetJobEvents(j.ID)
		if err != nil {
			t.Fatalf("events for %s: %v", j.ID, err)
		}
		if len(events) == 0 {
			t.Errorf("surviving job %s lost its events", j.ID)
		}
	}

	// The pending job kept its full timeline: enqueued.
	pending, err := s.GetJob(pendingID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	events, err := s.GetJobEvents(pending.ID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) != 1 || events[0].EventType != EventEnqueued {
		t.Errorf("expected the pending job to keep its enqueued event, got %+v", events)
	}
}

// TestPurgeBeforeKeepsRecentJobs verifies the age filter. A job whose last
// update is older than the cutoff is removed; a freshly updated job survives.
func TestPurgeBeforeKeepsRecentJobs(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	old, err := q.Enqueue("old", `{}`)
	if err != nil {
		t.Fatalf("enqueue old: %v", err)
	}
	if job, err := q.Lease(ctx, "old", time.Minute); err != nil || job == nil {
		t.Fatalf("lease old: job=%v err=%v", job, err)
	} else if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("ack old: %v", err)
	}
	// Backdate the completed job so it is older than the cutoff.
	if _, err := s.db.Exec(
		`UPDATE jobs SET updated_at = ? WHERE id = ?`,
		time.Now().UTC().Add(-48*time.Hour).Format(sqliteTimeFormat), old.ID); err != nil {
		t.Fatalf("backdate old: %v", err)
	}

	fresh, err := q.Enqueue("fresh", `{}`)
	if err != nil {
		t.Fatalf("enqueue fresh: %v", err)
	}
	if job, err := q.Lease(ctx, "fresh", time.Minute); err != nil || job == nil {
		t.Fatalf("lease fresh: job=%v err=%v", job, err)
	} else if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("ack fresh: %v", err)
	}

	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	stats, err := q.Purge(PurgeBefore(cutoff))
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if stats.JobsRemoved != 1 {
		t.Errorf("expected 1 old job removed, got %d", stats.JobsRemoved)
	}
	if _, err := s.GetJob(old.ID); err == nil {
		t.Errorf("old job must be gone")
	}
	if _, err := s.GetJob(fresh.ID); err != nil {
		t.Errorf("fresh job must survive: %v", err)
	}
}

// TestPurgeCandidatesMatchesPurge verifies that the preview counts equal the
// counts a real purge removes.
func TestPurgeCandidatesMatchesPurge(t *testing.T) {
	s, _ := seedPurgeLifecycle(t)
	q := NewQueue(s)

	candidates, err := q.PurgeCandidates()
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if candidates.JobsRemoved != 2 || candidates.EventsRemoved != 6 {
		t.Fatalf("unexpected candidates %+v", candidates)
	}

	stats, err := q.Purge()
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if stats != candidates {
		t.Errorf("purge %+v does not match candidates %+v", stats, candidates)
	}
}

// TestPurgeCandidatesDoesNotMutate verifies that a preview leaves the store
// unchanged.
func TestPurgeCandidatesDoesNotMutate(t *testing.T) {
	s, _ := seedPurgeLifecycle(t)
	q := NewQueue(s)

	before, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if _, err := q.PurgeCandidates(); err != nil {
		t.Fatalf("candidates: %v", err)
	}
	after, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(before.Jobs) != len(after.Jobs) {
		t.Errorf("preview changed job count: %d -> %d", len(before.Jobs), len(after.Jobs))
	}
	if len(before.Events) != len(after.Events) {
		t.Errorf("preview changed event count: %d -> %d", len(before.Events), len(after.Events))
	}
}

// TestPurgeIsIdempotent verifies that a second purge removes nothing.
func TestPurgeIsIdempotent(t *testing.T) {
	s, _ := seedPurgeLifecycle(t)
	q := NewQueue(s)

	if _, err := q.Purge(); err != nil {
		t.Fatalf("first purge: %v", err)
	}
	second, err := q.Purge()
	if err != nil {
		t.Fatalf("second purge: %v", err)
	}
	if second.JobsRemoved != 0 || second.EventsRemoved != 0 {
		t.Errorf("expected an empty second purge, got %+v", second)
	}
}

// TestPurgeRejectsNoStates verifies that an explicit empty state set removes
// nothing and reports no error.
func TestPurgeRejectsNoStates(t *testing.T) {
	s, _ := seedPurgeLifecycle(t)
	q := NewQueue(s)

	stats, err := q.Purge(PurgeStates())
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if stats.JobsRemoved != 0 || stats.EventsRemoved != 0 {
		t.Errorf("expected no removal for an empty state set, got %+v", stats)
	}
	snap, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(snap.Jobs) != 4 {
		t.Errorf("expected all 4 jobs to survive, got %d", len(snap.Jobs))
	}
}

// TestPurgePendingIsExplicit verifies that naming pending works, so an operator
// can clear a backlog on purpose.
func TestPurgePendingIsExplicit(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	if _, err := q.Enqueue("stuck", `{}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := q.Enqueue("stuck", `{}`, WithPriority(5)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	stats, err := q.Purge(PurgeStates(StatePending))
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if stats.JobsRemoved != 2 {
		t.Errorf("expected 2 pending jobs removed, got %d", stats.JobsRemoved)
	}
	snap, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(snap.Jobs) != 0 {
		t.Errorf("expected an empty queue, got %d jobs", len(snap.Jobs))
	}
}

// TestPurgeBeforeBoundaryUsesStrictInequality verifies that a job updated at
// the cutoff survives because the filter is strictly older.
func TestPurgeBeforeBoundaryUsesStrictInequality(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	job, err := q.Enqueue("edge", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if leased, err := q.Lease(ctx, "edge", time.Minute); err != nil || leased == nil {
		t.Fatalf("lease: job=%v err=%v", leased, err)
	} else if err := q.Acknowledge(leased.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// The cutoff equals the exact stored update time. The strict < comparison
	// must keep the job at the boundary.
	stored, err := s.GetJob(job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	stats, err := q.Purge(PurgeStates(StateCompleted), PurgeBefore(stored.UpdatedAt.UTC()))
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if stats.JobsRemoved != 0 {
		t.Errorf("expected the boundary job to survive, removed %d", stats.JobsRemoved)
	}
	if _, err := s.GetJob(job.ID); err != nil {
		t.Errorf("job must survive an equal cutoff: %v", err)
	}
}

func TestPurgeJobsUsesStrictInequality(t *testing.T) {
	// This test pins the SQL comparison so the behavior cannot drift silently.
	s := newTestStore(t)
	q := NewQueue(s)

	if _, err := q.Enqueue("x", `{}`, WithPriority(1)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	stats, err := q.Purge(PurgeStates(StatePending), PurgeBefore(time.Now().UTC().Add(-time.Hour)))
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if stats.JobsRemoved != 0 {
		t.Errorf("expected 0 removed for a past cutoff, got %d", stats.JobsRemoved)
	}
}

func ExampleQueue_Purge() {
	s, err := NewSQLiteStore("file:example_purge?mode=memory&cache=shared")
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	defer s.Close()
	q := NewQueue(s)

	if _, err := q.Enqueue("email", `{}`); err != nil {
		fmt.Println("enqueue:", err)
		return
	}
	stats, err := q.Purge()
	if err != nil {
		fmt.Println("purge:", err)
		return
	}
	fmt.Printf("removed %d jobs and %d events\n", stats.JobsRemoved, stats.EventsRemoved)
	// Output:
	// removed 0 jobs and 0 events
}

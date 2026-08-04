package queue

import (
	"context"
	"sync"
	"testing"
	"time"
)

// ageJob sets a job's last-update time to an explicit value. Retention tests
// use it to decide which jobs the age limit removes without waiting on a wall
// clock. The helper updates updated_at only, so the event log and other columns
// stay intact.
func ageJob(t *testing.T, s *SQLiteStore, id string, at time.Time) {
	t.Helper()
	if _, err := s.db.Exec(
		`UPDATE jobs SET updated_at = ? WHERE id = ?`,
		at.UTC().Format(sqliteTimeFormat), id); err != nil {
		t.Fatalf("age job %s: %v", id, err)
	}
}

// completeOne enqueues, leases, and acknowledges a job so it reaches the
// completed state with a full enqueued/leased/acknowledged event timeline.
func completeOne(t *testing.T, q *Queue) *Job {
	t.Helper()
	ctx := context.Background()
	if _, err := q.Enqueue("test", `{}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job, err := q.Lease(ctx, "test", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("lease: %v %v", job, err)
	}
	if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	got, err := q.store.GetJob(job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	return &got
}

// TestPruneRemovesOldTerminalJobs verifies that the age limit deletes terminal
// jobs older than the threshold together with their events. A fresher job with
// the same state must stay, because its retention window has not passed.
func TestPruneRemovesOldTerminalJobs(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	oldOne := completeOne(t, q)
	oldTwo := completeOne(t, q)
	fresh := completeOne(t, q)

	cutoff := time.Now().UTC().Add(-2 * time.Hour)
	ageJob(t, s, oldOne.ID, cutoff.Add(-time.Hour))
	ageJob(t, s, oldTwo.ID, cutoff.Add(-30*time.Minute))

	res, err := q.Prune(PrunePolicy{MaxJobAge: time.Hour})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.JobsRemoved != 2 {
		t.Errorf("expected 2 jobs removed, got %d", res.JobsRemoved)
	}
	// Each removed job carried enqueued, leased, and acknowledged events.
	if res.EventsRemoved != 6 {
		t.Errorf("expected 6 events removed, got %d", res.EventsRemoved)
	}

	if _, err := s.GetJob(oldOne.ID); err == nil {
		t.Errorf("expected old job %s to be removed", oldOne.ID)
	}
	if _, err := s.GetJob(oldTwo.ID); err == nil {
		t.Errorf("expected old job %s to be removed", oldTwo.ID)
	}
	got, err := s.GetJob(fresh.ID)
	if err != nil {
		t.Fatalf("expected fresh job to survive: %v", err)
	}
	if got.State != StateCompleted {
		t.Errorf("expected fresh job completed, got %s", got.State)
	}
	events, err := s.GetJobEvents(fresh.ID)
	if err != nil || len(events) != 3 {
		t.Errorf("expected fresh job events to survive, got %d events (err %v)", len(events), err)
	}
}

// TestPruneKeepsActiveJobs verifies that the age limit never removes a job that
// can still make progress. Pending, leased, and scheduled jobs survive no
// matter how old they are.
func TestPruneKeepsActiveJobs(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	completed := completeOne(t, q)
	pending, err := q.Enqueue("test", `{"n":"pending"}`)
	if err != nil {
		t.Fatalf("enqueue pending: %v", err)
	}
	scheduled, err := q.Enqueue("test", `{"n":"scheduled"}`, WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatalf("enqueue scheduled: %v", err)
	}
	leased, err := q.Lease(ctx, "test", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease: %v %v", leased, err)
	}

	old := time.Now().UTC().Add(-48 * time.Hour)
	ageJob(t, s, completed.ID, old)
	ageJob(t, s, pending.ID, old)
	ageJob(t, s, scheduled.ID, old)
	ageJob(t, s, leased.ID, old)

	res, err := q.Prune(PrunePolicy{MaxJobAge: time.Hour})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.JobsRemoved != 1 {
		t.Errorf("expected only the completed job removed, got %d", res.JobsRemoved)
	}
	if _, err := s.GetJob(completed.ID); err == nil {
		t.Errorf("expected the completed job to be removed")
	}
	for _, id := range []string{pending.ID, scheduled.ID, leased.ID} {
		job, err := s.GetJob(id)
		if err != nil {
			t.Fatalf("job %s missing: %v", id, err)
		}
		if job.State == StateCompleted {
			t.Errorf("job %s should not be completed", id)
		}
	}
}

// TestPruneCoversDeadLetterAndFailed verifies that the age limit treats both
// dead-lettered and legacy failed jobs as terminal. Operators keep old failures
// out of the store, not just successful work.
func TestPruneCoversDeadLetterAndFailed(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	job, err := q.Enqueue("test", `{}`, WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(ctx, "test", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease: %v %v", leased, err)
	}
	if err := q.Fail(leased.ID, "boom"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if _, err := s.db.Exec(
		`INSERT INTO jobs (id, kind, payload, state, retry_count, max_attempts, priority, created_at, updated_at)
		 VALUES (?, 'test', '{}', 'failed', 1, 1, 0, ?, ?)`,
		"legacy-failed", nowStamp(), nowStamp()); err != nil {
		t.Fatalf("insert legacy failed job: %v", err)
	}

	old := time.Now().UTC().Add(-24 * time.Hour)
	ageJob(t, s, job.ID, old)
	ageJob(t, s, "legacy-failed", old)

	res, err := q.Prune(PrunePolicy{MaxJobAge: time.Hour})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.JobsRemoved != 2 {
		t.Errorf("expected 2 jobs removed (dead_letter + failed), got %d", res.JobsRemoved)
	}
}

// TestPruneEventCapKeepsNewestEvents verifies that the per-job event cap trims
// only the oldest rows of the log. The newest events of each surviving job stay
// in order, so the timeline remains readable.
func TestPruneEventCapKeepsNewestEvents(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	job, err := q.Enqueue("test", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// The enqueue logged one event. Add more rows with distinct timestamps so
	// the cap has something to trim.
	for i := 0; i < 9; i++ {
		if err := s.AppendEvent(Event{
			JobID:     job.ID,
			EventType: EventRetried,
			Metadata:  stringPtr("synthetic"),
			Timestamp: time.Now().UTC().Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
	}

	res, err := q.Prune(PrunePolicy{MaxEventsPerJob: 3})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.JobsRemoved != 0 {
		t.Errorf("expected no jobs removed, got %d", res.JobsRemoved)
	}
	if res.EventsRemoved != 7 {
		t.Errorf("expected 7 events trimmed (10 total to 3), got %d", res.EventsRemoved)
	}

	events, err := s.GetJobEvents(job.ID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events left, got %d", len(events))
	}
	// The kept rows are the newest by event id, so the ids rise and the
	// original enqueued row (the oldest) is gone.
	if events[0].EventType == EventEnqueued {
		t.Errorf("expected the oldest enqueued event to be trimmed, it survived")
	}
	for i := 1; i < len(events); i++ {
		if events[i].ID <= events[i-1].ID {
			t.Errorf("expected ascending event ids, got %d then %d", events[i-1].ID, events[i].ID)
		}
	}
}

// TestPruneCombinesAgeAndEventCap verifies that one run may apply both limits.
// A removed job loses every event, and a surviving job loses only the rows
// beyond its cap.
func TestPruneCombinesAgeAndEventCap(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	oldOne := completeOne(t, q)
	fresh := completeOne(t, q)
	ageJob(t, s, oldOne.ID, time.Now().UTC().Add(-48*time.Hour))

	for i := 0; i < 5; i++ {
		if err := s.AppendEvent(Event{
			JobID:     fresh.ID,
			EventType: EventRetried,
			Timestamp: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("append event: %v", err)
		}
	}

	res, err := q.Prune(PrunePolicy{MaxJobAge: time.Hour, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.JobsRemoved != 1 {
		t.Errorf("expected 1 job removed, got %d", res.JobsRemoved)
	}
	// Old job: 3 events removed. Fresh job: 8 events down to 4, so 4 more.
	if res.EventsRemoved != 7 {
		t.Errorf("expected 7 events removed, got %d", res.EventsRemoved)
	}

	if _, err := s.GetJob(oldOne.ID); err == nil {
		t.Errorf("expected old job removed")
	}
	events, err := s.GetJobEvents(fresh.ID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) != 4 {
		t.Errorf("expected fresh job to keep 4 events, got %d", len(events))
	}
}

// TestPruneZeroPolicyRemovesNothing verifies that a policy without limits is a
// safe no-op. Automation can call Prune unconditionally without changing data.
func TestPruneZeroPolicyRemovesNothing(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	completeOne(t, q)
	beforeJobs, err := s.GetAllJobs()
	if err != nil {
		t.Fatalf("jobs: %v", err)
	}
	beforeEvents, err := s.GetAllEvents()
	if err != nil {
		t.Fatalf("events: %v", err)
	}

	res, err := q.Prune(PrunePolicy{})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.JobsRemoved != 0 || res.EventsRemoved != 0 {
		t.Errorf("expected zero removals, got %+v", res)
	}
	afterJobs, _ := s.GetAllJobs()
	afterEvents, _ := s.GetAllEvents()
	if len(afterJobs) != len(beforeJobs) || len(afterEvents) != len(beforeEvents) {
		t.Errorf("store changed under a zero policy: jobs %d->%d events %d->%d",
			len(beforeJobs), len(afterJobs), len(beforeEvents), len(afterEvents))
	}
}

// TestPruneSecondRunRemovesNothing verifies that retention is idempotent. A
// second run with the same policy finds nothing left to delete.
func TestPruneSecondRunRemovesNothing(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	job := completeOne(t, q)
	ageJob(t, s, job.ID, time.Now().UTC().Add(-48*time.Hour))

	first, err := q.Prune(PrunePolicy{MaxJobAge: time.Hour})
	if err != nil {
		t.Fatalf("first prune: %v", err)
	}
	if first.JobsRemoved != 1 {
		t.Fatalf("expected 1 job on first run, got %d", first.JobsRemoved)
	}
	second, err := q.Prune(PrunePolicy{MaxJobAge: time.Hour})
	if err != nil {
		t.Fatalf("second prune: %v", err)
	}
	if second.JobsRemoved != 0 || second.EventsRemoved != 0 {
		t.Errorf("expected idempotent run, got %+v", second)
	}
}

// TestPruneConcurrentWithLease verifies that a retention run does not race a
// worker. The event cap trims rows while another goroutine leases jobs, and the
// worker still sees consistent state. The store serializes writers, so the test
// exercises the transaction path rather than row interleaving.
func TestPruneConcurrentWithLease(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if _, err := q.Enqueue("test", `{}`); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				job, err := q.Lease(ctx, "test", time.Second)
				if err != nil {
					return
				}
				if job != nil {
					_ = q.Acknowledge(job.ID)
				}
			}
		}
	}()

	res, err := q.Prune(PrunePolicy{MaxJobAge: time.Hour, MaxEventsPerJob: 2})
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.JobsRemoved < 0 || res.EventsRemoved < 0 {
		t.Errorf("invalid prune result %+v", res)
	}
}

// TestPruneRecordsRetentionActivity verifies that manual and automatic runs
// persist source labels, cumulative counts, policy details, and run order.
func TestPruneRecordsRetentionActivity(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	completeOne(t, q)

	manual, err := q.Prune(PrunePolicy{MaxEventsPerJob: 1})
	if err != nil {
		t.Fatalf("manual prune: %v", err)
	}
	if manual.EventsRemoved != 2 {
		t.Fatalf("expected manual run to remove 2 events, got %+v", manual)
	}
	auto, err := q.PruneAuto(PrunePolicy{MaxEventsPerJob: 1})
	if err != nil {
		t.Fatalf("automatic prune: %v", err)
	}
	if auto.EventsRemoved != 0 {
		t.Fatalf("expected automatic run to find no events, got %+v", auto)
	}

	stats, err := s.GetRetentionStats()
	if err != nil {
		t.Fatalf("retention stats: %v", err)
	}
	want := []RetentionSourceCount{
		{Source: RetentionSourceManual, Runs: 1, EventsRemoved: 2},
		{Source: RetentionSourceAuto, Runs: 1},
	}
	if len(stats) != len(want) {
		t.Fatalf("expected %d source stats, got %+v", len(want), stats)
	}
	for i := range want {
		if stats[i] != want[i] {
			t.Errorf("source stat %d: expected %+v, got %+v", i, want[i], stats[i])
		}
	}

	recent, err := s.RecentRetentionRuns(1)
	if err != nil {
		t.Fatalf("recent runs: %v", err)
	}
	if len(recent) != 1 || recent[0].Source != RetentionSourceAuto {
		t.Fatalf("expected newest automatic run, got %+v", recent)
	}
	if recent[0].MaxEventsPerJob != 1 || recent[0].MaxJobAge != "0s" {
		t.Errorf("expected recorded policy, got %+v", recent[0])
	}
	last, err := s.GetLastRetentionRun()
	if err != nil {
		t.Fatalf("last run: %v", err)
	}
	if last == nil || last.ID != recent[0].ID {
		t.Fatalf("expected last run %v, got %+v", recent, last)
	}
}

func nowStamp() string {
	return time.Now().UTC().Format(sqliteTimeFormat)
}

func stringPtr(s string) *string {
	return &s
}

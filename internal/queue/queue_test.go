package queue

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore("file:test_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func newTestStoreWithAging(t *testing.T, interval time.Duration) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore("file:test_"+t.Name()+"?mode=memory&cache=shared",
		WithAgingInterval(interval))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestMigrationAddsPriorityToLegacyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	_, err = raw.Exec(`CREATE TABLE jobs (
		id TEXT PRIMARY KEY,
		kind TEXT NOT NULL,
		payload TEXT NOT NULL,
		state TEXT NOT NULL DEFAULT 'pending',
		retry_count INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL DEFAULT 3,
		idempotency_key TEXT UNIQUE,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		leased_until TEXT
	)`)
	if err != nil {
		raw.Close()
		t.Fatalf("create legacy jobs: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id TEXT NOT NULL,
		event_type TEXT NOT NULL,
		metadata TEXT,
		timestamp TEXT NOT NULL
	)`); err != nil {
		raw.Close()
		t.Fatalf("create legacy events: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close legacy database: %v", err)
	}

	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("migrate legacy database: %v", err)
	}
	defer store.Close()
	job, err := NewQueue(store).Enqueue("test", `{}`, WithPriority(7))
	if err != nil {
		t.Fatalf("enqueue after migration: %v", err)
	}
	if job.Priority != 7 {
		t.Fatalf("expected priority 7, got %d", job.Priority)
	}
}

func TestEnqueueAndInspect(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	job, err := q.Enqueue("test", `{"msg":"hello"}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if job.State != StatePending {
		t.Errorf("expected pending, got %s", job.State)
	}
	if job.Kind != "test" {
		t.Errorf("expected kind test, got %s", job.Kind)
	}

	snap, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(snap.Jobs) != 1 {
		t.Errorf("expected 1 job, got %d", len(snap.Jobs))
	}
	if len(snap.Events) != 1 {
		t.Errorf("expected 1 event, got %d", len(snap.Events))
	}
}

func TestLease(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	q.Enqueue("test", `{"x":1}`)
	ctx := context.Background()
	job, err := q.Lease(ctx, "test", time.Second)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if job == nil {
		t.Fatal("expected job, got nil")
	}
	if job.State != StateLeased {
		t.Errorf("expected leased, got %s", job.State)
	}

	leasedAfter, err := s.GetLeasedJobs()
	if err != nil {
		t.Fatalf("get leased: %v", err)
	}
	if len(leasedAfter) != 1 {
		t.Errorf("expected 1 leased, got %d", len(leasedAfter))
	}
}

func TestAcknowledge(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	q.Enqueue("test", `{}`)
	ctx := context.Background()
	job, _ := q.Lease(ctx, "test", time.Second)

	if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	snap, _ := q.Inspect()
	if snap.Stats[StateCompleted] != 1 {
		t.Errorf("expected 1 completed, got %d", snap.Stats[StateCompleted])
	}
}

func TestFailAndRetry(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	q.Enqueue("test", `{}`, WithMaxAttempts(2))
	ctx := context.Background()

	job1, _ := q.Lease(ctx, "test", time.Second)
	q.Fail(job1.ID, "error 1")

	job2, _ := q.Lease(ctx, "test", time.Second)
	if job2 == nil {
		t.Fatal("expected retry job")
	}
	if job2.ID != job1.ID {
		t.Errorf("expected same job id, got %s", job2.ID)
	}
	if job2.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", job2.RetryCount)
	}

	q.Fail(job2.ID, "error 2")
	job3, _ := q.Lease(ctx, "test", time.Second)
	if job3 != nil {
		t.Errorf("expected no more retries, got job %s", job3.ID)
	}

	snap, _ := q.Inspect()
	if snap.Stats[StateDeadLetter] != 1 {
		t.Errorf("expected 1 dead-lettered, got %d", snap.Stats[StateDeadLetter])
	}
}

func TestIdempotencyKey(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	job1, err := q.Enqueue("test", `{"a":1}`, WithIdempotencyKey("key-1"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job2, err := q.Enqueue("test", `{"a":2}`, WithIdempotencyKey("key-1"))
	if err != nil {
		t.Fatalf("enqueue again: %v", err)
	}
	if job1.ID != job2.ID {
		t.Errorf("expected same id for idempotent enqueue, got %s vs %s", job1.ID, job2.ID)
	}
}

func TestPriorityOrdering(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	low, err := q.Enqueue("test", `{"name":"low"}`, WithPriority(-1))
	if err != nil {
		t.Fatalf("enqueue low: %v", err)
	}
	defaultPriority, err := q.Enqueue("test", `{"name":"default"}`)
	if err != nil {
		t.Fatalf("enqueue default: %v", err)
	}
	urgent, err := q.Enqueue("test", `{"name":"urgent"}`, WithPriority(10))
	if err != nil {
		t.Fatalf("enqueue urgent: %v", err)
	}
	if defaultPriority.Priority != DefaultPriority {
		t.Fatalf("expected default priority %d, got %d", DefaultPriority, defaultPriority.Priority)
	}

	ctx := context.Background()
	for _, want := range []*Job{urgent, defaultPriority, low} {
		got, err := q.Lease(ctx, "test", time.Minute)
		if err != nil {
			t.Fatalf("lease %s: %v", want.ID, err)
		}
		if got == nil || got.ID != want.ID {
			t.Fatalf("expected priority %d job %s, got %+v", want.Priority, want.ID, got)
		}
		if err := q.Acknowledge(got.ID); err != nil {
			t.Fatalf("ack %s: %v", got.ID, err)
		}
	}
}

// TestPriorityAgingLiftsOldJob verifies that a job which has waited for several
// aging intervals gains enough effective priority to overtake a fresher
// higher-priority job. The store backdates the first job so the outcome does
// not depend on wall-clock timing.
func TestPriorityAgingLiftsOldJob(t *testing.T) {
	s := newTestStoreWithAging(t, time.Second)
	q := NewQueue(s)
	ctx := context.Background()

	old, err := q.Enqueue("test", `{"name":"old"}`, WithPriority(0))
	if err != nil {
		t.Fatalf("enqueue old: %v", err)
	}
	// Backdate the old job by five aging intervals so its effective priority
	// becomes 0 + 5 = 5, above the fresh job's priority of 3.
	if _, err := s.db.Exec(
		`UPDATE jobs SET created_at = ? WHERE id = ?`,
		time.Now().UTC().Add(-5*time.Second).Format(sqliteTimeFormat), old.ID); err != nil {
		t.Fatalf("backdate old: %v", err)
	}
	fresh, err := q.Enqueue("test", `{"name":"fresh"}`, WithPriority(3))
	if err != nil {
		t.Fatalf("enqueue fresh: %v", err)
	}

	got, err := q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if got == nil || got.ID != old.ID {
		t.Fatalf("expected aged job %s to lease first, got %+v", old.ID, got)
	}

	got, err = q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease second: %v", err)
	}
	if got == nil || got.ID != fresh.ID {
		t.Fatalf("expected fresh job %s second, got %+v", fresh.ID, got)
	}
}

// TestPriorityAgingDisabledKeepsOrdering verifies that a store without an aging
// interval ignores job age. A backdated low-priority job still leases after a
// fresh high-priority job, so the default behavior is unchanged.
func TestPriorityAgingDisabledKeepsOrdering(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	old, err := q.Enqueue("test", `{"name":"old"}`, WithPriority(0))
	if err != nil {
		t.Fatalf("enqueue old: %v", err)
	}
	if _, err := s.db.Exec(
		`UPDATE jobs SET created_at = ? WHERE id = ?`,
		time.Now().UTC().Add(-5*time.Second).Format(sqliteTimeFormat), old.ID); err != nil {
		t.Fatalf("backdate old: %v", err)
	}
	fresh, err := q.Enqueue("test", `{"name":"fresh"}`, WithPriority(3))
	if err != nil {
		t.Fatalf("enqueue fresh: %v", err)
	}

	got, err := q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if got == nil || got.ID != fresh.ID {
		t.Fatalf("expected fresh job %s first without aging, got %+v", fresh.ID, got)
	}

	got, err = q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease second: %v", err)
	}
	if got == nil || got.ID != old.ID {
		t.Fatalf("expected old job %s second, got %+v", old.ID, got)
	}
}

// TestPriorityAgingTiesUseDeterministicOrder verifies that jobs with the same
// effective priority fall back to the deterministic tie breakers. Two equally
// old jobs of the same priority must lease in creation order.
func TestPriorityAgingTiesUseDeterministicOrder(t *testing.T) {
	s := newTestStoreWithAging(t, time.Second)
	q := NewQueue(s)
	ctx := context.Background()

	first, err := q.Enqueue("test", `{"name":"first"}`, WithPriority(0))
	if err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	second, err := q.Enqueue("test", `{"name":"second"}`, WithPriority(0))
	if err != nil {
		t.Fatalf("enqueue second: %v", err)
	}
	// Backdate both jobs with one explicit base time so the aging boost is
	// equal for both, regardless of the platform clock resolution. The two
	// wait times stay inside the same one-second bucket, so created_at and
	// then id decide the order.
	base := time.Now().UTC().Add(-4 * time.Second)
	if _, err := s.db.Exec(
		`UPDATE jobs SET created_at = ? WHERE id = ?`,
		base.Add(-500*time.Millisecond).Format(sqliteTimeFormat), first.ID); err != nil {
		t.Fatalf("backdate first: %v", err)
	}
	if _, err := s.db.Exec(
		`UPDATE jobs SET created_at = ? WHERE id = ?`,
		base.Add(500*time.Millisecond).Format(sqliteTimeFormat), second.ID); err != nil {
		t.Fatalf("backdate second: %v", err)
	}

	got, err := q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if got == nil || got.ID != first.ID {
		t.Fatalf("expected first job %s to lease first on tie, got %+v", first.ID, got)
	}
}

// TestPriorityAgingRespectsSchedule verifies that aging does not let a future
// scheduled job bypass its run_at time. A scheduled job only ages once it is
// ready, so a backdated run_at but far-future creation is not enough to lease.
func TestPriorityAgingRespectsSchedule(t *testing.T) {
	s := newTestStoreWithAging(t, time.Second)
	q := NewQueue(s)
	ctx := context.Background()

	future, err := q.Enqueue("test", `{}`, WithPriority(100), WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatalf("enqueue future: %v", err)
	}
	ready, err := q.Enqueue("test", `{}`, WithPriority(0))
	if err != nil {
		t.Fatalf("enqueue ready: %v", err)
	}

	got, err := q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if got == nil || got.ID != ready.ID {
		t.Fatalf("expected ready job %s first, got %+v", ready.ID, got)
	}
	if err := q.Acknowledge(got.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	got, err = q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease after ready job: %v", err)
	}
	if got != nil {
		t.Fatalf("expected scheduled job %s to remain pending, got %+v", future.ID, got)
	}
}

func TestFutureHighPriorityJobDoesNotBypassSchedule(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	future, err := q.Enqueue("test", `{}`, WithPriority(100), WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatalf("enqueue future: %v", err)
	}
	ready, err := q.Enqueue("test", `{}`, WithPriority(0))
	if err != nil {
		t.Fatalf("enqueue ready: %v", err)
	}

	got, err := q.Lease(context.Background(), "test", time.Minute)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if got == nil || got.ID != ready.ID {
		t.Fatalf("expected ready job %s, got %+v", ready.ID, got)
	}
	if err := q.Acknowledge(got.ID); err != nil {
		t.Fatalf("ack ready: %v", err)
	}

	got, err = q.Lease(context.Background(), "test", time.Minute)
	if err != nil {
		t.Fatalf("lease after ready job: %v", err)
	}
	if got != nil {
		t.Fatalf("expected scheduled job %s to remain pending, got %+v", future.ID, got)
	}
}

// TestEnqueueScheduledFutureNotLeased confirms that a job with a run_at time
// in the future stays pending and is not leased. The job becomes leasable
// only after its run_at deadline has passed.
func TestEnqueueScheduledFutureNotLeased(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	future := time.Now().Add(time.Hour).UTC()
	job, err := q.Enqueue("test", `{}`, WithRunAt(future))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if job.RunAt == nil || !job.RunAt.Equal(future) {
		t.Fatalf("expected run_at %s, got %v", future, job.RunAt)
	}

	ctx := context.Background()
	got, err := q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if got != nil {
		t.Fatalf("expected no lease before run_at, got %s", got.ID)
	}

	// Move run_at into the past and confirm the job becomes leasable.
	past := time.Now().Add(-time.Minute).UTC()
	if _, err := s.db.Exec(
		`UPDATE jobs SET run_at = ? WHERE id = ?`,
		past.Format(time.RFC3339), job.ID); err != nil {
		t.Fatalf("update run_at: %v", err)
	}

	got, err = q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease after: %v", err)
	}
	if got == nil || got.ID != job.ID {
		t.Fatalf("expected lease of %s, got %v", job.ID, got)
	}
}

// TestScheduledEventLogged verifies that an enqueued scheduled job records a
// scheduled event whose metadata is the run_at timestamp.
func TestScheduledEventLogged(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	at := time.Now().Add(time.Hour).UTC()
	job, err := q.Enqueue("test", `{}`, WithRunAt(at))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	events, err := s.GetJobEvents(job.ID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) != 1 || events[0].EventType != EventScheduled {
		t.Fatalf("expected one scheduled event, got %+v", events)
	}
	if events[0].Metadata == nil {
		t.Fatal("expected non-nil metadata")
	}
	if _, err := time.Parse(time.RFC3339, *events[0].Metadata); err != nil {
		t.Fatalf("metadata is not RFC3339: %v", err)
	}
}

// TestRunAfterZeroIsReady confirms that a zero or negative delay yields a job
// with no run_at that leases immediately.
func TestRunAfterZeroIsReady(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	job, err := q.Enqueue("test", `{}`, WithRunAfter(0))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if job.RunAt != nil {
		t.Fatalf("expected nil run_at, got %v", job.RunAt)
	}
	ctx := context.Background()
	leased, err := q.Lease(ctx, "test", time.Minute)
	if err != nil || leased == nil || leased.ID != job.ID {
		t.Fatalf("expected immediate lease, got %v %v", leased, err)
	}
}

// TestLeaseOrderRespectsRunAt confirms that the lease picks the earliest ready
// job, not the earliest created job. A scheduled job with an earlier effective
// time must be leased before a plain pending job created later.
func TestLeaseOrderRespectsRunAt(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	earlier, err := q.Enqueue("test", `{"n":1}`, WithRunAfter(10*time.Millisecond))
	if err != nil {
		t.Fatalf("enqueue 1: %v", err)
	}

	// Give the first job a run_at in the future, then enqueue a ready job. The
	// ready job should lease first because it is the only ready one.
	time.Sleep(5 * time.Millisecond)
	later, err := q.Enqueue("test", `{"n":2}`)
	if err != nil {
		t.Fatalf("enqueue 2: %v", err)
	}

	ctx := context.Background()
	leased, err := q.Lease(ctx, "test", time.Minute)
	if err != nil || leased == nil || leased.ID != later.ID {
		t.Fatalf("expected ready job %s, got %v %v", later.ID, leased, err)
	}

	// Wait for the scheduled job to become ready and lease it next.
	time.Sleep(10 * time.Millisecond)
	leased, err = q.Lease(ctx, "test", time.Minute)
	if err != nil || leased == nil || leased.ID != earlier.ID {
		t.Fatalf("expected scheduled job %s, got %v %v", earlier.ID, leased, err)
	}
}

func TestRecoverOrphanedLeases(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	q.Enqueue("test", `{}`)
	ctx := context.Background()
	job, _ := q.Lease(ctx, "test", -time.Hour)

	recovered, err := q.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recovered != 1 {
		t.Errorf("expected 1 recovered, got %d", recovered)
	}

	recoveredJob, err := s.GetJob(job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if recoveredJob.State != StatePending {
		t.Errorf("expected pending after recovery, got %s", recoveredJob.State)
	}
	if recoveredJob.RetryCount != 1 {
		t.Errorf("expected retry_count=1 after recovery, got %d", recoveredJob.RetryCount)
	}
}

// TestRecoverExhaustsAttempts verifies that recovery respects the attempt budget.
// A job with a single allowed attempt has no room to retry, so an expired lease
// must move it to the failed state instead of looping forever.
func TestRecoverExhaustsAttempts(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	job, err := q.Enqueue("test", `{}`, WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	leased, err := s.LeaseJobByID(job.ID, -time.Hour)
	if err != nil || leased == nil {
		t.Fatalf("lease by id: %v %v", leased, err)
	}

	n, err := q.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 recovered, got %d", n)
	}

	got, err := s.GetJob(job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.State != StateDeadLetter {
		t.Errorf("expected dead-letter after recovery exhausted attempts, got %s", got.State)
	}

	events, err := s.GetJobEvents(job.ID)
	if err != nil {
		t.Fatalf("get events: %v", err)
	}
	last := events[len(events)-1]
	if last.EventType != EventDeadLettered {
		t.Errorf("expected dead_lettered event, got %s", last.EventType)
	}
	if last.Metadata == nil || !strings.Contains(*last.Metadata, "exhausted") {
		t.Errorf("expected exhausted marker in metadata, got %v", last.Metadata)
	}
}

// TestDeadLetterEventsAndRequeue verifies the full dead-letter workflow. A job
// that exhausts its attempts enters the dead_letter state with a dead_lettered
// event. Requeue returns it to pending with a reset attempt budget and logs a
// requeued event. The requeued job can lease and complete again.
func TestDeadLetterEventsAndRequeue(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	job, err := q.Enqueue("test", `{"name":"flaky"}`, WithMaxAttempts(2))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Attempt 1 fails and is retried.
	leased, err := q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease 1: %v", err)
	}
	if err := q.Fail(leased.ID, "boom 1"); err != nil {
		t.Fatalf("fail 1: %v", err)
	}

	// Attempt 2 fails and exhausts the budget.
	leased, err = q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease 2: %v", err)
	}
	if err := q.Fail(leased.ID, "boom 2"); err != nil {
		t.Fatalf("fail 2: %v", err)
	}

	got, err := s.GetJob(job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.State != StateDeadLetter {
		t.Fatalf("expected dead_letter, got %s", got.State)
	}

	events, err := s.GetJobEvents(job.ID)
	if err != nil {
		t.Fatalf("get events: %v", err)
	}
	want := []EventType{EventEnqueued, EventLeased, EventRetried, EventLeased, EventDeadLettered}
	if len(events) != len(want) {
		t.Fatalf("expected %d events, got %d: %+v", len(want), len(events), events)
	}
	for i, et := range want {
		if events[i].EventType != et {
			t.Errorf("event %d: expected %s, got %s", i, et, events[i].EventType)
		}
	}
	if events[len(events)-1].Metadata == nil ||
		!strings.Contains(*events[len(events)-1].Metadata, "2/2") {
		t.Errorf("expected attempt marker in dead_lettered metadata, got %v",
			events[len(events)-1].Metadata)
	}

	// Requeue resets the job and logs a requeued event.
	requeued, err := q.Requeue(job.ID, RequeueWithPayload(`{"name":"flaky","fixed":true}`))
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if requeued.State != StatePending {
		t.Errorf("expected pending after requeue, got %s", requeued.State)
	}
	if requeued.RetryCount != 0 {
		t.Errorf("expected retry_count 0 after requeue, got %d", requeued.RetryCount)
	}
	if requeued.Payload != `{"name":"flaky","fixed":true}` {
		t.Errorf("expected replaced payload, got %q", requeued.Payload)
	}

	events, err = s.GetJobEvents(job.ID)
	if err != nil {
		t.Fatalf("get events after requeue: %v", err)
	}
	last := events[len(events)-1]
	if last.EventType != EventRequeued {
		t.Errorf("expected requeued event, got %s", last.EventType)
	}

	// The requeued job completes normally.
	leased, err = q.Lease(ctx, "test", time.Minute)
	if err != nil || leased == nil || leased.ID != job.ID {
		t.Fatalf("expected requeued job to lease, got %v %v", leased, err)
	}
	if err := q.Acknowledge(leased.ID); err != nil {
		t.Fatalf("ack requeued: %v", err)
	}
	snap, _ := q.Inspect()
	if snap.Stats[StateCompleted] != 1 {
		t.Errorf("expected 1 completed, got %d", snap.Stats[StateCompleted])
	}
}

// TestRequeueRequiresDeadLetter verifies that Requeue rejects jobs that are not
// in the dead-letter state. A pending job must stay pending.
func TestRequeueRequiresDeadLetter(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	job, err := q.Enqueue("test", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := q.Requeue(job.ID); err == nil {
		t.Fatal("expected error requeueing a pending job")
	}

	snap, _ := q.Inspect()
	if snap.Stats[StatePending] != 1 {
		t.Errorf("expected the pending job to remain, got %+v", snap.Stats)
	}
}

// TestRequeueWithMaxAttempts verifies that the requeue attempt budget override
// is honored and that a requeued job may exhaust its budget again.
func TestRequeueWithMaxAttempts(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	job, err := q.Enqueue("test", `{}`, WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(ctx, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if err := q.Fail(leased.ID, "boom"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	requeued, err := q.Requeue(job.ID, RequeueWithMaxAttempts(5))
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if requeued.MaxAttempts != 5 {
		t.Errorf("expected max_attempts 5, got %d", requeued.MaxAttempts)
	}
	if requeued.RetryCount != 0 {
		t.Errorf("expected retry_count 0, got %d", requeued.RetryCount)
	}
}

// TestGetStateKindCountsAndEventTypeCounts verifies the aggregation queries
// used by the metrics exporter. Jobs group by kind and state, events group by
// type, and both results keep a stable order.
func TestGetStateKindCountsAndEventTypeCounts(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	if _, err := q.Enqueue("email", `{}`); err != nil {
		t.Fatalf("enqueue email: %v", err)
	}
	// Enqueue the failing report job with a higher priority so the first lease
	// of the report kind returns it, independent of enqueue timing.
	flaky, err := q.Enqueue("report", `{}`, WithMaxAttempts(1), WithPriority(1))
	if err != nil {
		t.Fatalf("enqueue flaky: %v", err)
	}
	if _, err := q.Enqueue("report", `{}`); err != nil {
		t.Fatalf("enqueue report: %v", err)
	}

	job, err := q.Lease(ctx, "email", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("lease email: %v %v", job, err)
	}
	if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	leased, err := q.Lease(ctx, "report", time.Minute)
	if err != nil || leased == nil || leased.ID != flaky.ID {
		t.Fatalf("lease flaky: %v %v", leased, err)
	}
	if err := q.Fail(leased.ID, "boom"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	counts, err := s.GetStateKindCounts()
	if err != nil {
		t.Fatalf("kind counts: %v", err)
	}
	wantKinds := []KindStateCount{
		{Kind: "email", State: StateCompleted, Count: 1},
		{Kind: "report", State: StateDeadLetter, Count: 1},
		{Kind: "report", State: StatePending, Count: 1},
	}
	if len(counts) != len(wantKinds) {
		t.Fatalf("expected %d kind counts, got %d: %+v", len(wantKinds), len(counts), counts)
	}
	for i, want := range wantKinds {
		if counts[i] != want {
			t.Errorf("kind count %d: expected %+v, got %+v", i, want, counts[i])
		}
	}

	events, err := s.GetEventTypeCounts()
	if err != nil {
		t.Fatalf("event counts: %v", err)
	}
	byType := map[EventType]int{}
	for _, e := range events {
		byType[e.EventType] = e.Count
	}
	if byType[EventEnqueued] != 3 {
		t.Errorf("expected 3 enqueued events, got %d", byType[EventEnqueued])
	}
	if byType[EventAcknowledged] != 1 {
		t.Errorf("expected 1 acknowledged event, got %d", byType[EventAcknowledged])
	}
	if byType[EventDeadLettered] != 1 {
		t.Errorf("expected 1 dead_lettered event, got %d", byType[EventDeadLettered])
	}
}

// TestGetOldestPendingReadyTime verifies that the oldest pending job is found
// by its ready time, and that an empty queue reports no value.
func TestGetOldestPendingReadyTime(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	if _, ok, err := s.GetOldestPendingReadyTime(); err != nil || ok {
		t.Fatalf("expected no oldest pending on empty queue, ok=%v err=%v", ok, err)
	}

	old, err := q.Enqueue("test", `{}`, WithRunAt(time.Now().Add(-time.Hour)))
	if err != nil {
		t.Fatalf("enqueue old: %v", err)
	}
	if _, err := q.Enqueue("test", `{}`); err != nil {
		t.Fatalf("enqueue new: %v", err)
	}

	ready, ok, err := s.GetOldestPendingReadyTime()
	if err != nil {
		t.Fatalf("oldest pending: %v", err)
	}
	if !ok {
		t.Fatal("expected an oldest pending job")
	}
	if old.RunAt == nil || !ready.Equal(*old.RunAt) {
		t.Errorf("expected ready time %v, got %v", old.RunAt, ready)
	}
}

// TestGetStateKindCountsEmptyQueue verifies the aggregation queries return
// empty results for a fresh database.
func TestGetStateKindCountsEmptyQueue(t *testing.T) {
	s := newTestStore(t)

	counts, err := s.GetStateKindCounts()
	if err != nil {
		t.Fatalf("kind counts: %v", err)
	}
	if len(counts) != 0 {
		t.Errorf("expected no kind counts, got %+v", counts)
	}
	events, err := s.GetEventTypeCounts()
	if err != nil {
		t.Fatalf("event counts: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected no event counts, got %+v", events)
	}
}

// TestConcurrentIdempotency confirms that many goroutines enqueuing the same
// logical job all observe one job ID and only one row exists afterward.
func TestConcurrentIdempotency(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	const goroutines = 30
	var wg sync.WaitGroup
	ids := make([]string, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			job, err := q.Enqueue("test", `{"x":1}`, WithIdempotencyKey("shared-key"))
			if err != nil {
				t.Errorf("enqueue: %v", err)
				return
			}
			ids[i] = job.ID
		}(i)
	}
	close(start)
	wg.Wait()

	first := ids[0]
	for i, id := range ids {
		if id != first {
			t.Errorf("goroutine %d saw id %s, want %s", i, id, first)
		}
	}
	snap, _ := q.Inspect()
	if len(snap.Jobs) != 1 {
		t.Errorf("expected exactly 1 job, got %d", len(snap.Jobs))
	}
}

func TestConcurrentEnqueue(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := q.Enqueue("test", `{"n":`+strconv.Itoa(i)+`}`)
			if err != nil {
				t.Errorf("enqueue: %v", err)
			}
		}(i)
	}
	wg.Wait()

	snap, _ := q.Inspect()
	if snap.Stats[StatePending] != 20 {
		t.Errorf("expected 20 pending, got %d", snap.Stats[StatePending])
	}
}

func TestFullLifecycle(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	q.Enqueue("lifecycle", `{"step":"start"}`)
	ctx := context.Background()

	job, _ := q.Lease(ctx, "lifecycle", time.Second)
	if job == nil {
		t.Fatal("expected job")
	}

	q.Acknowledge(job.ID)

	snap, _ := q.Inspect()
	if snap.Stats[StateCompleted] != 1 {
		t.Errorf("expected 1 completed, got %d", snap.Stats[StateCompleted])
	}

	events, err := s.GetJobEvents(job.ID)
	if err != nil {
		t.Fatalf("get events: %v", err)
	}
	expectedTypes := []EventType{EventEnqueued, EventLeased, EventAcknowledged}
	if len(events) != len(expectedTypes) {
		t.Fatalf("expected %d events, got %d", len(expectedTypes), len(events))
	}
	for i, et := range expectedTypes {
		if events[i].EventType != et {
			t.Errorf("event %d: expected %s, got %s", i, et, events[i].EventType)
		}
	}
}

// TestListJobsFilters verifies that ListJobs narrows jobs by kind and state,
// orders them newest first, and honors the limit. The web UI depends on this
// method for its jobs page and API. The jobs are backdated with explicit times
// so the expected order does not depend on platform clock resolution.
func TestListJobsFilters(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	first, err := q.Enqueue("email", `{"n":1}`)
	if err != nil {
		t.Fatalf("enqueue email: %v", err)
	}
	report, err := q.Enqueue("report", `{"n":2}`)
	if err != nil {
		t.Fatalf("enqueue report: %v", err)
	}
	cleanup, err := q.Enqueue("email", `{"n":3}`)
	if err != nil {
		t.Fatalf("enqueue cleanup: %v", err)
	}
	base := time.Now().UTC()
	backdate := func(id string, ago time.Duration) {
		t.Helper()
		if _, err := s.db.Exec(
			`UPDATE jobs SET created_at = ? WHERE id = ?`,
			base.Add(-ago).Format(sqliteTimeFormat), id); err != nil {
			t.Fatalf("backdate %s: %v", id, err)
		}
	}
	backdate(first.ID, 3*time.Second)
	backdate(report.ID, 2*time.Second)
	backdate(cleanup.ID, time.Second)

	// The full list is newest first.
	all, err := s.ListJobs(JobFilter{})
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(all))
	}
	if all[0].ID != cleanup.ID || all[1].ID != report.ID || all[2].ID != first.ID {
		t.Errorf("expected newest-first order, got %+v", all)
	}

	// A kind filter returns only that kind.
	emails, err := s.ListJobs(JobFilter{Kind: "email"})
	if err != nil {
		t.Fatalf("list emails: %v", err)
	}
	if len(emails) != 2 {
		t.Fatalf("expected 2 email jobs, got %d", len(emails))
	}
	for _, j := range emails {
		if j.Kind != "email" {
			t.Errorf("expected email kind, got %s", j.Kind)
		}
	}

	// A limit caps the result.
	limited, err := s.ListJobs(JobFilter{Limit: 1})
	if err != nil {
		t.Fatalf("list limited: %v", err)
	}
	if len(limited) != 1 || limited[0].ID != cleanup.ID {
		t.Errorf("expected the newest job only, got %+v", limited)
	}
}

// TestListJobsStateFilter verifies that ListJobs narrows jobs by state and
// that an unknown state matches nothing.
func TestListJobsStateFilter(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	job, err := q.Enqueue("test", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(ctx, "test", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease: %v %v", leased, err)
	}
	if err := q.Acknowledge(leased.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	pending, err := s.ListJobs(JobFilter{State: StatePending})
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	completed, err := s.ListJobs(JobFilter{State: StateCompleted})
	if err != nil {
		t.Fatalf("list completed: %v", err)
	}
	missing, err := s.ListJobs(JobFilter{State: StateDeadLetter})
	if err != nil {
		t.Fatalf("list dead letter: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected no pending jobs, got %+v", pending)
	}
	if len(completed) != 1 || completed[0].ID != job.ID {
		t.Errorf("expected the completed job, got %+v", completed)
	}
	if len(missing) != 0 {
		t.Errorf("expected no dead-letter jobs, got %+v", missing)
	}
}

// TestListKinds verifies that ListKinds returns every distinct job kind sorted
// by name, and that an empty queue yields no kinds.
func TestListKinds(t *testing.T) {
	s := newTestStore(t)
	kinds, err := s.ListKinds()
	if err != nil {
		t.Fatalf("list kinds: %v", err)
	}
	if len(kinds) != 0 {
		t.Fatalf("expected no kinds on an empty queue, got %+v", kinds)
	}

	q := NewQueue(s)
	for _, kind := range []string{"report", "email", "cleanup"} {
		if _, err := q.Enqueue(kind, `{}`); err != nil {
			t.Fatalf("enqueue %s: %v", kind, err)
		}
	}
	kinds, err = s.ListKinds()
	if err != nil {
		t.Fatalf("list kinds: %v", err)
	}
	want := []string{"cleanup", "email", "report"}
	if len(kinds) != len(want) {
		t.Fatalf("expected %d kinds, got %+v", len(want), kinds)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Errorf("kind %d: expected %s, got %s", i, want[i], kinds[i])
		}
	}
}

func BenchmarkEnqueue(b *testing.B) {
	s, err := NewSQLiteStore("file:bench_enqueue?mode=memory&cache=shared")
	if err != nil {
		b.Fatalf("new store: %v", err)
	}
	defer s.Close()
	q := NewQueue(s)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := q.Enqueue("bench", `{}`)
		if err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}
}

func BenchmarkLeaseAndAck(b *testing.B) {
	s, err := NewSQLiteStore("file:bench_lease?mode=memory&cache=shared")
	if err != nil {
		b.Fatalf("new store: %v", err)
	}
	defer s.Close()
	q := NewQueue(s)
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		q.Enqueue("bench", `{}`)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		job, _ := q.Lease(ctx, "bench", time.Minute)
		q.Acknowledge(job.ID)
	}
}

// BenchmarkConcurrentLease stresses the serialized lease path. Many goroutines
// race to lease distinct jobs from the same pool. The work is bounded by the
// single database connection, so this benchmark shows the contention cost of
// lease dispatch under concurrency.
func BenchmarkConcurrentLease(b *testing.B) {
	s, err := NewSQLiteStore("file:bench_concurrent?mode=memory&cache=shared")
	if err != nil {
		b.Fatalf("new store: %v", err)
	}
	defer s.Close()
	q := NewQueue(s)
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		q.Enqueue("bench", `{"n":`+strconv.Itoa(i)+`}`)
	}
	b.ResetTimer()

	const workers = 8
	var wg sync.WaitGroup
	jobs := make(chan int, workers*4)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				_ = n
				job, err := q.Lease(ctx, "bench", time.Minute)
				if err != nil || job == nil {
					b.Errorf("lease: %v", err)
					return
				}
				if err := q.Acknowledge(job.ID); err != nil {
					b.Errorf("ack: %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}

func TestMain(m *testing.M) {
	// Ensure tests use UTC
	time.Local = time.UTC
	os.Exit(m.Run())
}

package worker

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

func newTestQueue(t *testing.T) (*queue.Queue, *queue.SQLiteStore) {
	t.Helper()
	s, err := queue.NewSQLiteStore("file:worker_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return queue.NewQueue(s), s
}

func TestWorkerProcessesJobs(t *testing.T) {
	q, _ := newTestQueue(t)

	var count atomic.Int32
	handler := func(ctx context.Context, job queue.Job) error {
		count.Add(1)
		return nil
	}

	w := NewWorker(q, handler, "test",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)

	for i := 0; i < 5; i++ {
		q.Enqueue("test", `{"n":`+string(rune('0'+i))+`}`)
	}

	time.Sleep(200 * time.Millisecond)
	cancel()

	if n := count.Load(); n != 5 {
		t.Errorf("expected 5 processed, got %d", n)
	}

	snap, _ := q.Inspect()
	if snap.Stats[queue.StateCompleted] != 5 {
		t.Errorf("expected 5 completed, got %d", snap.Stats[queue.StateCompleted])
	}
}

func TestWorkerRetriesOnFailure(t *testing.T) {
	q, _ := newTestQueue(t)

	var attempts atomic.Int32
	handler := func(ctx context.Context, job queue.Job) error {
		n := attempts.Add(1)
		if n < 2 {
			return context.DeadlineExceeded
		}
		return nil
	}

	w := NewWorker(q, handler, "test",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
	)

	q.Enqueue("test", `{}`, queue.WithMaxAttempts(3))

	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	cancel()

	snap, _ := q.Inspect()
	if snap.Stats[queue.StateCompleted] != 1 {
		t.Errorf("expected 1 completed, got %d", snap.Stats[queue.StateCompleted])
	}
}

func TestWorkerRecoversOrphans(t *testing.T) {
	q, s := newTestQueue(t)

	q.Enqueue("test", `{}`)

	ctx := context.Background()
	job, _ := q.Lease(ctx, "test", -time.Hour)

	// Manually set the lease to expired in the past
	s.RecoverOrphanedLeases()

	var processed atomic.Int32
	handler := func(ctx context.Context, job queue.Job) error {
		processed.Add(1)
		return nil
	}

	w := NewWorker(q, handler, "test",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
	)

	ctx2, cancel := context.WithCancel(context.Background())
	go w.Run(ctx2)

	time.Sleep(150 * time.Millisecond)
	cancel()

	if n := processed.Load(); n != 1 {
		t.Errorf("expected 1 processed after recovery, got %d", n)
	}

	recoveredJob, _ := s.GetJob(job.ID)
	if recoveredJob.State != queue.StateCompleted {
		t.Errorf("expected completed, got %s", recoveredJob.State)
	}
}

// TestWorkerDeadLetterAndRequeue verifies the end-to-end dead-letter workflow.
// A job that always fails exhausts its attempts and enters the dead-letter
// state. After a requeue with a working handler, the same job completes.
func TestWorkerDeadLetterAndRequeue(t *testing.T) {
	q, _ := newTestQueue(t)

	failing := func(ctx context.Context, job queue.Job) error {
		return context.DeadlineExceeded
	}
	w := NewWorker(q, failing, "test",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
	)

	ctx, cancel := context.WithCancel(context.Background())
	wDone := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(wDone)
	}()

	q.Enqueue("test", `{}`, queue.WithMaxAttempts(3))
	waitFor(t, func() bool {
		snap, _ := q.Inspect()
		return snap.Stats[queue.StateDeadLetter] == 1
	}, 2*time.Second)
	cancel()
	<-wDone

	snap, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	var job queue.Job
	for _, j := range snap.Jobs {
		if j.State == queue.StateDeadLetter {
			job = j
			break
		}
	}
	if job.State != queue.StateDeadLetter {
		t.Fatalf("expected dead_letter after exhausting attempts, got %s", job.State)
	}

	// Requeue the dead letter with a handler that succeeds, then confirm the
	// job completes.
	requeued, err := q.Requeue(job.ID)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if requeued.State != queue.StatePending {
		t.Fatalf("expected pending after requeue, got %s", requeued.State)
	}

	var processed atomic.Int32
	w2 := NewWorker(q, func(ctx context.Context, job queue.Job) error {
		processed.Add(1)
		return nil
	}, "test",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
	)
	ctx2, cancel2 := context.WithCancel(context.Background())
	w2Done := make(chan struct{})
	go func() {
		_ = w2.Run(ctx2)
		close(w2Done)
	}()
	waitFor(t, func() bool {
		snap, _ := q.Inspect()
		return snap.Stats[queue.StateCompleted] == 1
	}, 2*time.Second)
	cancel2()
	<-w2Done

	if n := processed.Load(); n != 1 {
		t.Errorf("expected 1 processed after requeue, got %d", n)
	}
}

// waitFor polls cond until it returns true or the deadline passes. It reports
// a test failure when the deadline passes first.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// TestTwoWorkersShareOneDatabase is the horizontal-scaling showcase. Two
// worker processes, each with its own store connection, process one SQLite
// file at the same time. Every job must run exactly once: the atomic lease
// claim never hands the same job to both workers.
func TestTwoWorkersShareOneDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	storeA, err := queue.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("store a: %v", err)
	}
	defer storeA.Close()
	storeB, err := queue.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("store b: %v", err)
	}
	defer storeB.Close()

	qa, qb := queue.NewQueue(storeA), queue.NewQueue(storeB)

	const jobs = 30
	for i := 0; i < jobs; i++ {
		if _, err := qa.Enqueue("shared", `{}`); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	processed := map[string]int{}
	handler := func(ctx context.Context, job queue.Job) error {
		mu.Lock()
		processed[job.ID]++
		mu.Unlock()
		return nil
	}

	wa := NewWorker(qa, handler, "shared",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
		WithConcurrency(2),
	)
	wb := NewWorker(qb, handler, "shared",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
		WithConcurrency(2),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = wa.Run(ctx)
		_ = wb.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(processed) == jobs
	}, 5*time.Second)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != jobs {
		t.Fatalf("expected %d jobs processed, got %d", jobs, len(processed))
	}
	for id, n := range processed {
		if n != 1 {
			t.Errorf("job %s processed %d times", id, n)
		}
	}
}

// TestRecoveryEventSequence simulates a worker that leases a job and then
// crashes without acknowledging it. A second worker run must recover the
// orphaned lease and then process the job. The append-only event log for that
// job must record the full path: enqueued, leased, recovered, leased,
// acknowledged.
func TestRecoveryEventSequence(t *testing.T) {
	q, s := newTestQueue(t)

	job, err := q.Enqueue("test", `{"n":1}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Worker A leases the job and then "crashes" before it can acknowledge. The
	// negative duration puts the lease deadline in the past so that it is
	// immediately orphaned.
	if _, err := q.Lease(context.Background(), "test", -time.Hour); err != nil {
		t.Fatalf("lease: %v", err)
	}

	var processed atomic.Int32
	handler := func(ctx context.Context, job queue.Job) error {
		processed.Add(1)
		return nil
	}
	w := NewWorker(q, handler, "test",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	if n := processed.Load(); n != 1 {
		t.Fatalf("expected 1 processed after recovery, got %d", n)
	}

	events, err := s.GetJobEvents(job.ID)
	if err != nil {
		t.Fatalf("get events: %v", err)
	}
	want := []queue.EventType{
		queue.EventEnqueued,
		queue.EventLeased,
		queue.EventRecovered,
		queue.EventLeased,
		queue.EventAcknowledged,
	}
	if len(events) != len(want) {
		t.Fatalf("expected %d events, got %d: %+v", len(want), len(events), events)
	}
	for i, et := range want {
		if events[i].EventType != et {
			t.Errorf("event %d: expected %s, got %s", i, et, events[i].EventType)
		}
	}
}

// completeJob leases a specific job and acknowledges it, so the job reaches the
// completed state with a full event timeline. The lease-by-ID claim is atomic,
// so the helper stays deterministic.
func completeJob(t *testing.T, q *queue.Queue, s *queue.SQLiteStore, id string) {
	t.Helper()
	if _, err := s.LeaseJobByID(id, time.Minute); err != nil {
		t.Fatalf("lease %s: %v", id, err)
	}
	if err := q.Acknowledge(id); err != nil {
		t.Fatalf("ack %s: %v", id, err)
	}
}

// ageJob sets a job's last-update time to an explicit value. Auto-retention
// tests use it to decide which jobs the age limit removes without waiting on a
// wall clock.
func ageJob(t *testing.T, s *queue.SQLiteStore, id string, at time.Time) {
	t.Helper()
	if _, err := s.DB().Exec(
		`UPDATE jobs SET updated_at = ? WHERE id = ?`,
		at.UTC().Format(time.RFC3339Nano), id); err != nil {
		t.Fatalf("age job %s: %v", id, err)
	}
}

// TestWorkerAutoRetentionRemovesOldJobs verifies that a worker with a retention
// policy cleans old terminal jobs on its own schedule. No external prune run is
// needed; the worker applies the policy at startup and keeps the store small.
func TestWorkerAutoRetentionRemovesOldJobs(t *testing.T) {
	q, s := newTestQueue(t)

	var keep *queue.Job
	for i := 0; i < 3; i++ {
		job, err := q.Enqueue("test", `{}`)
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		completeJob(t, q, s, job.ID)
		if i < 2 {
			ageJob(t, s, job.ID, time.Now().UTC().Add(-48*time.Hour))
		} else {
			keep = job
		}
	}

	w := NewWorker(q, func(ctx context.Context, job queue.Job) error { return nil }, "test",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
		WithRetention(queue.PrunePolicy{MaxJobAge: time.Hour}, 50*time.Millisecond),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	waitFor(t, func() bool {
		snap, _ := q.Inspect()
		return snap.Stats[queue.StateCompleted] == 1
	}, 2*time.Second)

	if _, err := s.GetJob(keep.ID); err != nil {
		t.Errorf("expected the fresh job to survive auto-retention: %v", err)
	}
	events, err := s.GetAllEvents()
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("expected 2 events to remain for the fresh job, got %d", len(events))
	}
}

// TestWorkerAutoRetentionRunsBesideJobs verifies that the retention loop does
// not disturb the job loop. A fresh pending job is processed normally while an
// old terminal job leaves the store on the retention schedule.
func TestWorkerAutoRetentionRunsBesideJobs(t *testing.T) {
	q, s := newTestQueue(t)

	oldJob, err := q.Enqueue("test", `{"n":"old"}`)
	if err != nil {
		t.Fatalf("enqueue old: %v", err)
	}
	completeJob(t, q, s, oldJob.ID)
	ageJob(t, s, oldJob.ID, time.Now().UTC().Add(-48*time.Hour))

	var processed atomic.Int32
	w := NewWorker(q, func(ctx context.Context, job queue.Job) error {
		processed.Add(1)
		return nil
	}, "test",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
		WithRetention(queue.PrunePolicy{MaxJobAge: time.Hour}, 50*time.Millisecond),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	if _, err := q.Enqueue("test", `{"n":"fresh"}`); err != nil {
		t.Fatalf("enqueue fresh: %v", err)
	}

	waitFor(t, func() bool {
		return processed.Load() == 1
	}, 2*time.Second)
	waitFor(t, func() bool {
		_, err := s.GetJob(oldJob.ID)
		return err != nil
	}, 2*time.Second)

	snap, _ := q.Inspect()
	if snap.Stats[queue.StateCompleted] != 1 {
		t.Errorf("expected 1 completed job to remain, got %d", snap.Stats[queue.StateCompleted])
	}
}

// TestWorkerRetentionOffByDefault verifies that a worker without a retention
// option never removes old jobs. Auto-retention stays opt-in, so a worker
// never changes data beyond the jobs it leases.
func TestWorkerRetentionOffByDefault(t *testing.T) {
	q, s := newTestQueue(t)

	job, err := q.Enqueue("test", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	completeJob(t, q, s, job.ID)
	ageJob(t, s, job.ID, time.Now().UTC().Add(-48*time.Hour))

	w := NewWorker(q, func(ctx context.Context, job queue.Job) error { return nil }, "test",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
	)
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	time.Sleep(150 * time.Millisecond)
	cancel()

	if _, err := s.GetJob(job.ID); err != nil {
		t.Errorf("expected the old job to stay when auto-retention is off: %v", err)
	}
}

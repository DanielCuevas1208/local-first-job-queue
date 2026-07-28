package queue

import (
	"context"
	"os"
	"strconv"
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
	if snap.Stats[StateFailed] != 1 {
		t.Errorf("expected 1 failed, got %d", snap.Stats[StateFailed])
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
	if got.State != StateFailed {
		t.Errorf("expected failed after recovery exhausted attempts, got %s", got.State)
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

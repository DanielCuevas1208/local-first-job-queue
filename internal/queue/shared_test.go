package queue

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// newSharedFileStores opens two stores on the same SQLite file. Two stores
// model two worker processes, because each store owns an independent connection
// to the file. WAL mode lets them read concurrently and serialize writes.
func newSharedFileStores(t *testing.T) (a, b *SQLiteStore, path string) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "shared.db")
	a, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open store a: %v", err)
	}
	t.Cleanup(func() { a.Close() })
	b, err = NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open store b: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return a, b, path
}

// TestConcurrentLeaseAcrossStores verifies the atomic lease claim. Two stores
// sharing one file lease jobs at the same time. Each job must be claimed by
// exactly one store, and no lease may fail with a busy error. The test drives
// both stores from one process to keep the outcome deterministic.
func TestConcurrentLeaseAcrossStores(t *testing.T) {
	a, b, _ := newSharedFileStores(t)
	ctx := context.Background()

	const total = 40
	for i := 0; i < total; i++ {
		if _, err := NewQueue(a).Enqueue("test", fmt.Sprintf(`{"n":%d}`, i)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	claimed := map[string]int{}
	var leaseErrs []error
	var wg sync.WaitGroup

	// Each goroutine is one store. The lease duration is long so no job expires
	// during the run; each job must be acknowledged exactly once.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			store := a
			if w == 1 {
				store = b
			}
			q := NewQueue(store)
			for i := 0; i < total*2; i++ {
				job, err := q.Lease(ctx, "test", time.Minute)
				if err != nil {
					mu.Lock()
					leaseErrs = append(leaseErrs, err)
					mu.Unlock()
					return
				}
				if job == nil {
					return
				}
				mu.Lock()
				claimed[job.ID]++
				mu.Unlock()
				if err := q.Acknowledge(job.ID); err != nil {
					mu.Lock()
					leaseErrs = append(leaseErrs, err)
					mu.Unlock()
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if len(leaseErrs) > 0 {
		t.Fatalf("lease or ack failed: %v", leaseErrs[0])
	}
	if len(claimed) != total {
		t.Errorf("expected %d distinct jobs claimed, got %d", total, len(claimed))
	}
	for id, c := range claimed {
		if c != 1 {
			t.Errorf("job %s claimed %d times", id, c)
		}
	}

	snap, err := NewQueue(a).Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if snap.Stats[StateCompleted] != total {
		t.Errorf("expected %d completed, got %d", total, snap.Stats[StateCompleted])
	}
}

// TestConcurrentRecoverySingleEvent verifies recovery is idempotent when two
// stores try to recover the same orphaned lease at the same time. Exactly one
// recovery may take effect, so the job logs a single recovered event and the
// returned count stays consistent.
func TestConcurrentRecoverySingleEvent(t *testing.T) {
	a, b, _ := newSharedFileStores(t)
	q := NewQueue(a)

	job, err := q.Enqueue("test", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := a.LeaseJobByID(job.ID, -time.Hour); err != nil {
		t.Fatalf("orphan lease: %v", err)
	}

	var recovered []int
	var errs []error
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, store := range []*SQLiteStore{a, b} {
		wg.Add(1)
		go func(s *SQLiteStore) {
			defer wg.Done()
			n, err := NewQueue(s).Recover()
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			recovered = append(recovered, n)
		}(store)
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("recover failed: %v", errs[0])
	}
	sum := 0
	for _, n := range recovered {
		sum += n
	}
	// Both stores may observe the orphan, but the state guard lets only one
	// update win. The event log must record one recovery at most.
	if sum != 1 {
		t.Errorf("expected exactly 1 recovery total, got %v", recovered)
	}

	events, err := a.GetJobEvents(job.ID)
	if err != nil {
		t.Fatalf("get events: %v", err)
	}
	recoveredEvents := 0
	for _, e := range events {
		if e.EventType == EventRecovered {
			recoveredEvents++
		}
	}
	if recoveredEvents != 1 {
		t.Errorf("expected 1 recovered event, got %d in %+v", recoveredEvents, events)
	}
}

// TestLeaseAfterOtherStoreClaim verifies that a job leased by one store cannot
// be leased again by a second store while the lease is active. The second store
// must return no job, even when the first store holds the lease for a long time.
func TestLeaseAfterOtherStoreClaim(t *testing.T) {
	a, b, _ := newSharedFileStores(t)
	ctx := context.Background()

	q := NewQueue(a)
	job, err := q.Enqueue("test", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	first, err := NewQueue(b).Lease(ctx, "test", time.Hour)
	if err != nil {
		t.Fatalf("first lease: %v", err)
	}
	if first == nil || first.ID != job.ID {
		t.Fatalf("expected first lease of %s, got %+v", job.ID, first)
	}

	second, err := NewQueue(a).Lease(ctx, "test", time.Hour)
	if err != nil {
		t.Fatalf("second lease: %v", err)
	}
	if second != nil {
		t.Errorf("expected no second lease while the first is active, got %+v", second)
	}
}

// TestStaleAcknowledgementCannotCompleteReLeasedJob verifies the lease token
// on acknowledgement. Store B leases the job first. Store A takes over the job
// through recovery and leases it again. When the original lease holder tries to
// acknowledge with its stale token, the completion must be rejected so the job
// stays with the new owner.
func TestStaleAcknowledgementCannotCompleteReLeasedJob(t *testing.T) {
	a, b, _ := newSharedFileStores(t)
	ctx := context.Background()

	job, err := NewQueue(a).Enqueue("test", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Worker B leases the job with a deadline in the past, then abandons it.
	bq := NewQueue(b)
	stale, err := bq.Lease(ctx, "test", -time.Hour)
	if err != nil {
		t.Fatalf("stale lease: %v", err)
	}
	if stale == nil || stale.ID != job.ID {
		t.Fatalf("expected stale lease of %s, got %+v", job.ID, stale)
	}

	// Store A recovers the orphan and leases it again with a fresh deadline.
	aq := NewQueue(a)
	if n, err := aq.Recover(); err != nil || n != 1 {
		t.Fatalf("recover: n=%d err=%v", n, err)
	}
	current, err := aq.Lease(ctx, "test", time.Hour)
	if err != nil {
		t.Fatalf("re-lease: %v", err)
	}
	if current == nil || current.ID != job.ID {
		t.Fatalf("expected re-lease of %s, got %+v", job.ID, current)
	}

	// The stale holder acknowledges with its old token. The update must affect
	// no row because the job no longer carries that deadline.
	if err := aq.AcknowledgeLease(job.ID, stale.LeasedUntil); err == nil {
		t.Fatal("expected stale acknowledgement to be rejected")
	}

	got, err := a.GetJob(job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.State != StateLeased {
		t.Errorf("expected job to stay leased, got %s", got.State)
	}
	if got.LeasedUntil == nil || !got.LeasedUntil.Equal(*current.LeasedUntil) {
		t.Errorf("expected the new lease deadline to remain, got %v", got.LeasedUntil)
	}

	// The current owner can still acknowledge and complete the job.
	if err := aq.AcknowledgeLease(job.ID, current.LeasedUntil); err != nil {
		t.Fatalf("current ack: %v", err)
	}
	got, err = a.GetJob(job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.State != StateCompleted {
		t.Errorf("expected completed after valid ack, got %s", got.State)
	}
}

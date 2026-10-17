package queue

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// newSharedFileStores opens two independent stores on one SQLite file. Each
// store owns its own connection, which models two worker processes sharing the
// same database file. The stores exercise real file locking, so the tests
// prove the cross-process guarantees of the atomic lease claim.
func newSharedFileStores(t *testing.T) (*SQLiteStore, *SQLiteStore) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shared.db")
	a, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open store a: %v", err)
	}
	b, err := NewSQLiteStore(path)
	if err != nil {
		a.Close()
		t.Fatalf("open store b: %v", err)
	}
	t.Cleanup(func() {
		b.Close()
		a.Close()
	})
	return a, b
}

// TestSharedFileLeaseClaimsEachJobOnce is the core horizontal-scaling
// guarantee. Two stores (two worker processes) lease from one queue
// concurrently, and each job must be claimed and acknowledged exactly once.
// The old select-then-update lease could hand the same job to both processes;
// the single-statement claim cannot.
func TestSharedFileLeaseClaimsEachJobOnce(t *testing.T) {
	storeA, storeB := newSharedFileStores(t)
	qa := NewQueue(storeA)
	ctx := context.Background()

	const jobs = 200
	for i := 0; i < jobs; i++ {
		if _, err := qa.Enqueue("shared", `{"n":`+strconv.Itoa(i)+`}`); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var (
		mu     sync.Mutex
		leased = map[string]bool{}
		acked  int
	)
	run := func(q *Queue) {
		for {
			job, err := q.Lease(ctx, "shared", time.Minute)
			if err != nil {
				t.Errorf("lease: %v", err)
				return
			}
			if job == nil {
				return
			}
			mu.Lock()
			if leased[job.ID] {
				t.Errorf("job %s was leased while it was already leased", job.ID)
			}
			leased[job.ID] = true
			mu.Unlock()

			if err := q.Acknowledge(job.ID); err != nil {
				t.Errorf("ack %s: %v", job.ID, err)
				return
			}
			mu.Lock()
			acked++
			mu.Unlock()
		}
	}

	var wg sync.WaitGroup
	const perStore = 4
	for i := 0; i < 2*perStore; i++ {
		wg.Add(1)
		q := qa
		if i >= perStore {
			q = NewQueue(storeB)
		}
		go func() {
			defer wg.Done()
			run(q)
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if acked != jobs {
		t.Fatalf("expected %d acknowledgements, got %d", jobs, acked)
	}
	if len(leased) != jobs {
		t.Fatalf("expected %d distinct leases, got %d", jobs, len(leased))
	}
}

// TestSharedFileLeaseIsAtomicUnderContention races two connections against one
// pending job. Exactly one connection must win the lease; the other must see
// no job. The loop repeats to give the scheduler many chances to interleave
// the two claims.
func TestSharedFileLeaseIsAtomicUnderContention(t *testing.T) {
	storeA, storeB := newSharedFileStores(t)
	ctx := context.Background()

	for i := 0; i < 40; i++ {
		job, err := NewQueue(storeA).Enqueue("contend", `{}`)
		if err != nil {
			t.Fatalf("iteration %d enqueue: %v", i, err)
		}

		start := make(chan struct{})
		results := make(chan *Job, 2)
		var wg sync.WaitGroup
		for _, q := range []*Queue{NewQueue(storeA), NewQueue(storeB)} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				got, err := q.Lease(ctx, "contend", time.Minute)
				if err != nil {
					t.Errorf("iteration %d lease: %v", i, err)
					return
				}
				results <- got
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		var winnerID string
		winners := 0
		for got := range results {
			if got != nil {
				winners++
				winnerID = got.ID
			}
		}
		if winners != 1 {
			t.Fatalf("iteration %d: expected exactly one winner, got %d", i, winners)
		}
		if winnerID != job.ID {
			t.Fatalf("iteration %d: winner is %s, want %s", i, winnerID, job.ID)
		}
		if err := NewQueue(storeA).Acknowledge(winnerID); err != nil {
			t.Fatalf("iteration %d ack: %v", i, err)
		}
	}
}

// TestSharedFileRecoveryAppliesOnce verifies that concurrent crash recovery by
// two processes touches each orphan once. The event log must record exactly
// one recovered event, because the store reports only rows its update changed.
func TestSharedFileRecoveryAppliesOnce(t *testing.T) {
	storeA, storeB := newSharedFileStores(t)
	qa := NewQueue(storeA)

	job, err := qa.Enqueue("shared", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// A negative lease duration puts the deadline in the past, so the job is
	// immediately orphaned, exactly as if the worker that claimed it crashed.
	if _, err := qa.Lease(context.Background(), "shared", -time.Hour); err != nil {
		t.Fatalf("lease with expired deadline: %v", err)
	}

	counts := make(chan int, 2)
	var wg sync.WaitGroup
	for _, q := range []*Queue{qa, NewQueue(storeB)} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := q.Recover()
			if err != nil {
				t.Errorf("recover: %v", err)
				return
			}
			counts <- n
		}()
	}
	wg.Wait()
	close(counts)

	total := 0
	for n := range counts {
		total += n
	}
	if total != 1 {
		t.Fatalf("expected recovery to touch 1 job, got %d", total)
	}

	got, err := storeA.GetJob(job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.State != StatePending {
		t.Fatalf("expected pending after recovery, got %s", got.State)
	}
	if got.RetryCount != 1 {
		t.Fatalf("expected retry count 1 after recovery, got %d", got.RetryCount)
	}

	events, err := storeA.GetJobEvents(job.ID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	recovered := 0
	for _, e := range events {
		if e.EventType == EventRecovered {
			recovered++
		}
	}
	if recovered != 1 {
		t.Fatalf("expected exactly 1 recovered event, got %d", recovered)
	}
}

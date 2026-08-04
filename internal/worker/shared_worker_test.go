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

// TestTwoWorkersShareOneFile verifies the horizontal scaling path. Two workers,
// each backed by its own store on the same SQLite file, drain one queue. Every
// job must be processed exactly once, so the lease claim stays atomic when two
// processes race for the same file.
func TestTwoWorkersShareOneFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.db")

	storeA, err := queue.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open store a: %v", err)
	}
	defer storeA.Close()
	storeB, err := queue.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open store b: %v", err)
	}
	defer storeB.Close()

	q := queue.NewQueue(storeA)
	const total = 40
	for i := 0; i < total; i++ {
		if _, err := q.Enqueue("test", `{"n":`+string(rune('0'+i%10))+`}`); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var processed atomic.Int32
	var mu sync.Mutex
	seen := map[string]int{}
	handler := func(_ context.Context, job queue.Job) error {
		processed.Add(1)
		mu.Lock()
		seen[job.ID]++
		mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		return nil
	}

	wa := NewWorker(queue.NewQueue(storeA), handler, "test",
		WithPollInterval(2*time.Millisecond),
		WithLeaseDuration(2*time.Second),
	)
	wb := NewWorker(queue.NewQueue(storeB), handler, "test",
		WithPollInterval(2*time.Millisecond),
		WithLeaseDuration(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	doneA := make(chan struct{})
	doneB := make(chan struct{})
	go func() { _ = wa.Run(ctx); close(doneA) }()
	go func() { _ = wb.Run(ctx); close(doneB) }()

	waitFor(t, func() bool {
		snap, err := q.Inspect()
		return err == nil && snap.Stats[queue.StateCompleted] == total
	}, 10*time.Second)
	cancel()
	<-doneA
	<-doneB

	if n := processed.Load(); n != total {
		t.Errorf("expected %d processed, got %d", total, n)
	}
	for id, c := range seen {
		if c > 1 {
			t.Errorf("job %s processed %d times", id, c)
		}
	}
}

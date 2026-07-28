package cli

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/local-first-job-queue/internal/fault"
	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/worker"
)

// Demo runs a self-contained, deterministic scenario. It creates a fresh
// temporary database, enqueues jobs that each exercise one queue behavior, runs
// a single worker with the fault injector, and then prints the final state.
//
// The scenario covers:
//   - a job that completes on the first attempt,
//   - a job that fails twice and then succeeds,
//   - a job that exhausts its attempts and enters the failed state,
//   - a job that panics on the first attempt and succeeds on the second,
//   - a job that a previous worker leased and abandoned, to show crash
//     recovery on the next worker start.
func Demo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	dbPath := fs.String("db", "", "database path (default: a temp file)")
	keep := fs.Bool("keep", false, "keep the demo database file after the run")
	maxRun := fs.Duration("run", 3*time.Second, "maximum time the worker runs")
	kind := fs.String("kind", "demo", "job kind used by the demo")
	fs.Parse(args)

	path := *dbPath
	cleanup := func() {}
	if path == "" {
		f, err := os.CreateTemp("", "jobqueue-demo-*.db")
		if err != nil {
			return fmt.Errorf("create temp db: %w", err)
		}
		path = f.Name()
		f.Close()
		cleanup = func() {
			if *keep {
				return
			}
			_ = os.Remove(path)
			_ = os.Remove(path + "-journal")
			_ = os.Remove(path + "-wal")
			_ = os.Remove(path + "-shm")
		}
	}
	defer cleanup()

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	store, err := queue.NewSQLiteStore(path)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()
	q := queue.NewQueue(store)

	fmt.Println("== Local-first Durable Job Queue: demo ==")
	fmt.Printf("database: %s\n", path)
	fmt.Printf("kind:     %s\n\n", *kind)

	scenario := []struct {
		label       string
		name        string
		payload     string
		maxAttempts int
	}{
		{"first-try success", "alpha", `{"name":"alpha"}`, 3},
		{"retry twice then ok", "beta", `{"name":"beta","fault":{"mode":"error","fail_until_attempt":2,"message":"rate limited"}}`, 3},
		{"exhausts attempts", "gamma", `{"name":"gamma","fault":{"mode":"error","fail_until_attempt":5,"message":"disk full"}}`, 3},
		{"panic then ok", "epsilon", `{"name":"epsilon","fault":{"mode":"panic","fail_until_attempt":1,"message":"kaboom"}}`, 2},
		{"orphaned by a crash", "delta", `{"name":"delta"}`, 3},
	}

	fmt.Println("enqueuing scenario jobs:")
	var deltaID string
	for _, s := range scenario {
		job, err := q.Enqueue(*kind, s.payload, queue.WithMaxAttempts(s.maxAttempts))
		if err != nil {
			return fmt.Errorf("enqueue %s: %w", s.label, err)
		}
		fmt.Printf("  %-22s %-22s %s\n", s.label, s.name, job.ID)
		if s.name == "delta" {
			deltaID = job.ID
		}
	}

	// Simulate a crashed previous worker: lease delta with a deadline in the
	// past, then never acknowledge it. The next worker run must recover it.
	if _, err := store.LeaseJobByID(deltaID, -time.Hour); err != nil {
		return fmt.Errorf("orphan lease: %w", err)
	}
	fmt.Println("\norphaned job delta was leased and then abandoned.")
	fmt.Println("starting worker; it will recover orphans and process jobs.")
	fmt.Println()

	inner := func(ctx context.Context, job queue.Job) error {
		log.Printf("handled name=%s attempt=%d state-before=%s", payloadName(job.Payload), job.RetryCount+1, job.State)
		return nil
	}
	w := worker.NewWorker(q, fault.New(inner, fault.FromPayload).Handle, *kind,
		worker.WithConcurrency(1),
		worker.WithPollInterval(5*time.Millisecond),
		worker.WithLeaseDuration(2*time.Second),
	)

	ctx, cancel := context.WithTimeout(context.Background(), *maxRun)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	idle := waitUntilIdle(q, 20*time.Millisecond, ctx.Done())
	cancel()
	<-done

	if idle {
		fmt.Println("\nqueue drained before the run deadline.")
	} else {
		fmt.Println("\nreached the run deadline before the queue drained.")
	}

	fmt.Println()
	snap, err := q.Inspect()
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}
	RenderSnapshot(snap, os.Stdout)

	fmt.Printf("\ninspect again with: jobqueue inspect -db %q\n", path)
	fmt.Printf("inspect a job with: jobqueue history <id> -db %q\n", path)
	return nil
}

func payloadName(payload string) string {
	const needle = `"name":"`
	i := strings.Index(payload, needle)
	if i < 0 {
		return "?"
	}
	i += len(needle)
	j := strings.IndexByte(payload[i:], '"')
	if j < 0 {
		return "?"
	}
	return payload[i : i+j]
}

// waitUntilIdle returns true when the queue has no pending and no leased jobs.
// It polls until that condition holds or until done is closed.
func waitUntilIdle(q *queue.Queue, every time.Duration, done <-chan struct{}) bool {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return false
		case <-ticker.C:
			snap, err := q.Inspect()
			if err != nil {
				continue
			}
			if snap.Stats[queue.StatePending] == 0 && snap.Stats[queue.StateLeased] == 0 {
				return true
			}
		}
	}
}

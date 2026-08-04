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
	"github.com/local-first-job-queue/internal/metrics"
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
//   - a job that exhausts its attempts and enters the dead-letter queue,
//   - a job that panics on the first attempt and succeeds on the second,
//   - a job that a previous worker leased and abandoned, to show crash
//     recovery on the next worker start.
//
// After the first worker run, the demo requeues the dead-lettered job with a
// corrected payload and runs the worker again. The requeued job completes, so
// the output shows the full dead-letter workflow.
//
// A short separate segment shows priority aging: a low-priority job scheduled
// in the past overtakes a fresher higher-priority job because it has waited
// for several aging intervals.
func Demo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	dbPath := fs.String("db", "", "database path (default: a temp file)")
	keep := fs.Bool("keep", false, "keep the demo database file after the run")
	maxRun := fs.Duration("run", 3*time.Second, "maximum time each worker run lasts")
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
		priority    int
		runAfter    time.Duration
	}{
		{"first-try success", "alpha", `{"name":"alpha"}`, 3, 0, 0},
		{"priority retry", "beta", `{"name":"beta","fault":{"mode":"error","fail_until_attempt":2,"message":"rate limited"}}`, 3, 10, 0},
		{"exhausts attempts", "gamma", `{"name":"gamma","fault":{"mode":"error","fail_until_attempt":3,"message":"disk full"}}`, 3, 0, 0},
		{"panic then ok", "epsilon", `{"name":"epsilon","fault":{"mode":"panic","fail_until_attempt":1,"message":"kaboom"}}`, 2, 0, 0},
		{"orphaned by a crash", "delta", `{"name":"delta"}`, 3, 0, 0},
		{"delayed run", "omega", `{"name":"omega"}`, 3, 0, 40 * time.Millisecond},
	}

	fmt.Println("enqueuing scenario jobs:")
	var deltaID, gammaID string
	for _, s := range scenario {
		opts := []queue.EnqueueOption{
			queue.WithMaxAttempts(s.maxAttempts),
			queue.WithPriority(s.priority),
		}
		if s.runAfter > 0 {
			opts = append(opts, queue.WithRunAfter(s.runAfter))
		}
		job, err := q.Enqueue(*kind, s.payload, opts...)
		if err != nil {
			return fmt.Errorf("enqueue %s: %w", s.label, err)
		}
		fmt.Printf("  %-22s %-22s priority=%2d %s\n", s.label, s.name, s.priority, job.ID)
		switch s.name {
		case "delta":
			deltaID = job.ID
		case "gamma":
			gammaID = job.ID
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
	newWorker := func() *worker.Worker {
		return worker.NewWorker(q, fault.New(inner, fault.FromPayload).Handle, *kind,
			worker.WithConcurrency(1),
			worker.WithPollInterval(5*time.Millisecond),
			worker.WithLeaseDuration(2*time.Second),
		)
	}

	runWorker := func() bool {
		w := newWorker()
		ctx, cancel := context.WithTimeout(context.Background(), *maxRun)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- w.Run(ctx) }()
		idle := waitUntilIdle(q, 20*time.Millisecond, ctx.Done())
		cancel()
		<-done
		return idle
	}

	if runWorker() {
		fmt.Println("queue drained before the run deadline.")
	} else {
		fmt.Println("reached the run deadline before the queue drained.")
	}

	// gamma exhausted its attempts during the first run. Show the dead-letter
	// queue, then requeue gamma with a corrected payload and run the worker
	// again so the job completes.
	fmt.Println("\nDead-letter queue")
	fmt.Println("-----------------")
	snap, err := q.Inspect()
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}
	deadLetterCount := 0
	for _, j := range snap.Jobs {
		if j.State == queue.StateDeadLetter {
			fmt.Printf("  %s kind=%s priority=%d state=%s attempts=%d/%d\n",
				shortID(j.ID), j.Kind, j.Priority, j.State, j.RetryCount, j.MaxAttempts)
			deadLetterCount++
		}
	}
	if deadLetterCount == 0 {
		fmt.Println("  (empty)")
	}

	fmt.Println("\noperator requeues the dead-lettered job with a corrected payload.")
	if _, err := q.Requeue(gammaID, queue.RequeueWithPayload(`{"name":"gamma","fixed":true}`)); err != nil {
		return fmt.Errorf("requeue gamma: %w", err)
	}
	fmt.Println("starting worker again; it will process the requeued job.")
	fmt.Println()

	if runWorker() {
		fmt.Println("queue drained before the run deadline.")
	} else {
		fmt.Println("reached the run deadline before the queue drained.")
	}

	if err := showPriorityAging(*kind); err != nil {
		return err
	}
	if err := showRetention(*kind); err != nil {
		return err
	}

	fmt.Println()
	snap, err = q.Inspect()
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}
	RenderSnapshot(snap, os.Stdout)

	fmt.Println()
	fmt.Println("Metrics")
	fmt.Println("-------")
	if err := metrics.New(store).Write(os.Stdout); err != nil {
		return fmt.Errorf("render metrics: %w", err)
	}

	fmt.Printf("\ninspect again with: jobqueue inspect -db %q\n", path)
	fmt.Printf("inspect a job with: jobqueue history <id> -db %q\n", path)
	fmt.Printf("requeue a dead letter with: jobqueue requeue <id> -db %q\n", path)
	fmt.Printf("prune old jobs with: jobqueue prune -age 168h -db %q\n", path)
	fmt.Printf("view it in a browser with: jobqueue web -db %q\n", path)
	return nil
}

// showPriorityAging demonstrates that a job gains one priority point per aging
// interval it has waited. A low-priority job scheduled in the past has a higher
// effective priority than a fresher higher-priority job, so it leases first.
// The segment uses its own in-memory store so it does not disturb the scenario
// counts.
func showPriorityAging(kind string) error {
	const agingInterval = 100 * time.Millisecond
	store, err := queue.NewSQLiteStore("file:jobqueue-demo-aging?mode=memory&cache=shared",
		queue.WithAgingInterval(agingInterval))
	if err != nil {
		return fmt.Errorf("open aging store: %w", err)
	}
	defer store.Close()
	q := queue.NewQueue(store)

	fmt.Println("\nPriority aging")
	fmt.Println("--------------")
	fmt.Printf("aging interval: %s; a job gains one priority point per interval it waits.\n", agingInterval)

	aged, err := q.Enqueue(kind, `{"name":"aged"}`, queue.WithPriority(0), queue.WithRunAt(time.Now().Add(-5*agingInterval)))
	if err != nil {
		return fmt.Errorf("enqueue aged: %w", err)
	}
	fresh, err := q.Enqueue(kind, `{"name":"fresh"}`, queue.WithPriority(1))
	if err != nil {
		return fmt.Errorf("enqueue fresh: %w", err)
	}
	fmt.Printf("  %-22s priority=%2d waited=5 intervals effective=%d\n",
		"aged  (low priority)", 0, effectivePriority(*aged, agingInterval))
	fmt.Printf("  %-22s priority=%2d waited=0 intervals effective=%d\n",
		"fresh (high priority)", 1, effectivePriority(*fresh, agingInterval))
	fmt.Println()

	ctx := context.Background()
	first, err := q.Lease(ctx, kind, time.Minute)
	if err != nil {
		return fmt.Errorf("lease first: %w", err)
	}
	second, err := q.Lease(ctx, kind, time.Minute)
	if err != nil {
		return fmt.Errorf("lease second: %w", err)
	}
	if first == nil || second == nil {
		return fmt.Errorf("expected two leases in the aging demo")
	}
	if first.ID != aged.ID {
		return fmt.Errorf("aging demo: expected aged job to lease first")
	}
	if second.ID != fresh.ID {
		return fmt.Errorf("aging demo: expected fresh job to lease second")
	}
	fmt.Printf("lease order: %s (%s) then %s (%s)\n",
		shortID(first.ID), payloadName(first.Payload), shortID(second.ID), payloadName(second.Payload))
	fmt.Println("the waiting job outranks the fresher higher-priority job.")
	return nil
}

// showRetention demonstrates that a retention run removes old terminal jobs
// together with their events. Three jobs complete; two of them are backdated
// beyond the retention age. A prune run deletes the two old jobs and their
// event rows, so the store keeps only recent work.
func showRetention(kind string) error {
	const maxAge = time.Hour
	store, err := queue.NewSQLiteStore("file:jobqueue-demo-retention?mode=memory&cache=shared")
	if err != nil {
		return fmt.Errorf("open retention store: %w", err)
	}
	defer store.Close()
	q := queue.NewQueue(store)
	ctx := context.Background()

	fmt.Println("\nRetention")
	fmt.Println("---------")
	fmt.Printf("retention age: %s; terminal jobs older than that leave with their events.\n", maxAge)

	oldIDs := []string{}
	for i := 0; i < 3; i++ {
		job, err := q.Enqueue(kind, fmt.Sprintf(`{"name":"retention-%d"}`, i+1))
		if err != nil {
			return fmt.Errorf("enqueue retention: %w", err)
		}
		leased, err := q.Lease(ctx, kind, time.Minute)
		if err != nil || leased == nil || leased.ID != job.ID {
			return fmt.Errorf("lease retention job: %v %v", leased, err)
		}
		if err := q.Acknowledge(job.ID); err != nil {
			return fmt.Errorf("ack retention job: %w", err)
		}
		if i < 2 {
			oldIDs = append(oldIDs, job.ID)
		}
	}
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	for _, id := range oldIDs {
		if _, err := store.DB().Exec(
			`UPDATE jobs SET updated_at = ? WHERE id = ?`,
			cutoff.Format(time.RFC3339Nano), id); err != nil {
			return fmt.Errorf("age retention job: %w", err)
		}
	}

	completed, totalEvents := retentionCounts(store)
	fmt.Printf("  completed: %d  events: %d\n", completed, totalEvents)

	res, err := q.Prune(queue.PrunePolicy{MaxJobAge: maxAge})
	if err != nil {
		return fmt.Errorf("prune retention: %w", err)
	}
	completed, totalEvents = retentionCounts(store)
	fmt.Printf("  prune removed %d job(s) and %d event(s)\n", res.JobsRemoved, res.EventsRemoved)
	fmt.Printf("  completed: %d  events: %d\n", completed, totalEvents)
	fmt.Println("old terminal jobs left with their events; the log stays bounded.")
	return nil
}

// retentionCounts reports the completed-job count and the total event rows of
// a store, so the retention segment can show the before and after state.
func retentionCounts(store *queue.SQLiteStore) (completed, events int) {
	if stats, err := store.GetQueueStats(); err == nil {
		completed = stats[queue.StateCompleted]
	}
	if counts, err := store.GetEventTypeCounts(); err == nil {
		for _, c := range counts {
			events += c.Count
		}
	}
	return completed, events
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
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

// effectivePriority reports the priority the lease query would use for a job,
// including the aging boost it has earned by waiting. The store measures the
// wait from COALESCE(run_at, created_at), mirroring the SQL ordering clause.
func effectivePriority(j queue.Job, interval time.Duration) int {
	ready := j.CreatedAt
	if j.RunAt != nil {
		ready = *j.RunAt
	}
	waited := time.Since(ready)
	if waited < 0 {
		waited = 0
	}
	return j.Priority + int(waited/interval)
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

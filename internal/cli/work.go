package cli

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/local-first-job-queue/internal/fault"
	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/worker"
)

type echoHandler struct{}

func (echoHandler) Handle(ctx context.Context, job queue.Job) error {
	log.Printf("processed kind=%s id=%s payload=%s", job.Kind, job.ID, job.Payload)
	return nil
}

// Work starts a worker process for one job kind. On startup it recovers
// orphaned leases left by previous workers. A payload may carry a "fault"
// object; the fault injector honors it so a real worker run can reproduce
// retries, panics, and crash recovery without extra tooling.
func Work(args []string) error {
	fs := flag.NewFlagSet("work", flag.ExitOnError)
	kind := fs.String("kind", "default", "job kind to process")
	dbPath := fs.String("db", "queue.db", "database path")
	concurrency := fs.Int("concurrency", 1, "number of concurrent workers")
	leaseDuration := fs.Duration("lease", 30*time.Second, "lease duration per job")
	pollInterval := fs.Duration("poll", time.Second, "time between lease attempts")
	fs.Parse(args)

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	q := queue.NewQueue(store)
	handler := fault.New(echoHandler{}.Handle, fault.FromPayload).Handle
	w := worker.NewWorker(q, handler, *kind,
		worker.WithConcurrency(*concurrency),
		worker.WithLeaseDuration(*leaseDuration),
		worker.WithPollInterval(*pollInterval),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("worker started kind=%q concurrency=%d lease=%s poll=%s",
		*kind, *concurrency, *leaseDuration, *pollInterval)
	return w.Run(ctx)
}

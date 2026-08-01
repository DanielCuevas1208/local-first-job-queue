package cli

import (
	"flag"
	"fmt"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

func Enqueue(args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	kind := fs.String("kind", "default", "job kind")
	payload := fs.String("payload", "", "job payload")
	idempotencyKey := fs.String("idempotency-key", "", "optional idempotency key")
	maxAttempts := fs.Int("max-attempts", 3, "max attempts, including the first")
	priority := fs.Int("priority", queue.DefaultPriority, "priority; higher values run first")
	runAt := fs.String("run-at", "", "earliest lease time as RFC3339")
	runAfter := fs.Duration("run-after", 0, "delay lease by this duration")
	dbPath := fs.String("db", "queue.db", "database path")
	fs.Parse(args)

	if *payload == "" {
		return fmt.Errorf("payload is required")
	}
	if *runAt != "" && *runAfter != 0 {
		return fmt.Errorf("use only one of -run-at and -run-after")
	}

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	q := queue.NewQueue(store)
	opts := []queue.EnqueueOption{
		queue.WithMaxAttempts(*maxAttempts),
		queue.WithPriority(*priority),
	}
	if *idempotencyKey != "" {
		opts = append(opts, queue.WithIdempotencyKey(*idempotencyKey))
	}
	switch {
	case *runAt != "":
		t, err := time.Parse(time.RFC3339, *runAt)
		if err != nil {
			return fmt.Errorf("parse run-at: %w", err)
		}
		opts = append(opts, queue.WithRunAt(t))
	case *runAfter != 0:
		opts = append(opts, queue.WithRunAfter(*runAfter))
	}
	job, err := q.Enqueue(*kind, *payload, opts...)
	if err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	fmt.Printf("enqueued job %s (%s)\n", job.ID, job.Kind)
	if job.RunAt != nil {
		fmt.Printf("scheduled for %s\n", job.RunAt.UTC().Format(time.RFC3339))
	}
	return nil
}

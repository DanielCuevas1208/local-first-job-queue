package cli

import (
	"flag"
	"fmt"

	"github.com/local-first-job-queue/internal/queue"
)

func Enqueue(args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	kind := fs.String("kind", "default", "job kind")
	payload := fs.String("payload", "", "job payload")
	idempotencyKey := fs.String("idempotency-key", "", "optional idempotency key")
	maxAttempts := fs.Int("max-attempts", 3, "max attempts, including the first")
	dbPath := fs.String("db", "queue.db", "database path")
	fs.Parse(args)

	if *payload == "" {
		return fmt.Errorf("payload is required")
	}

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	q := queue.NewQueue(store)
	opts := []queue.EnqueueOption{queue.WithMaxAttempts(*maxAttempts)}
	if *idempotencyKey != "" {
		opts = append(opts, queue.WithIdempotencyKey(*idempotencyKey))
	}
	job, err := q.Enqueue(*kind, *payload, opts...)
	if err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	fmt.Printf("enqueued job %s (%s)\n", job.ID, job.Kind)
	return nil
}

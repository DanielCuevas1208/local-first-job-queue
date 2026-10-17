package cli

import (
	"flag"
	"fmt"

	"github.com/local-first-job-queue/internal/queue"
)

// Requeue returns a dead-lettered job to the pending state. An operator uses
// this command after fixing the reason the job failed. The job ID may appear
// before or after the flags, like the history command.
func Requeue(args []string) error {
	fs := flag.NewFlagSet("requeue", flag.ExitOnError)
	dbPath := fs.String("db", "queue.db", "database path")
	maxAttempts := fs.Int("max-attempts", 0, "new attempt budget; default keeps the current one")
	payload := fs.String("payload", "", "new payload; default keeps the current one")

	ordered := moveFirstPositionalToEnd(args)
	fs.Parse(ordered)

	if fs.NArg() == 0 {
		return fmt.Errorf("usage: requeue <job-id> [-db path] [-max-attempts n] [-payload json]")
	}
	jobID := fs.Arg(0)

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	q := queue.NewQueue(store)
	opts := []queue.RequeueOption{}
	if *maxAttempts > 0 {
		opts = append(opts, queue.RequeueWithMaxAttempts(*maxAttempts))
	}
	if *payload != "" {
		opts = append(opts, queue.RequeueWithPayload(*payload))
	}

	job, err := q.Requeue(jobID, opts...)
	if err != nil {
		return fmt.Errorf("requeue: %w", err)
	}
	fmt.Printf("requeued job %s (%s) as pending with attempts 0/%d\n",
		job.ID, job.Kind, job.MaxAttempts)
	return nil
}

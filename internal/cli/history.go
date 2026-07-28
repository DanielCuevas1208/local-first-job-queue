package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/local-first-job-queue/internal/queue"
)

// History prints the event log for one job. The log is the append-only record
// of every state change for that job, oldest first. The job ID may appear
// before or after the flags.
func History(args []string) error {
	fs := flag.NewFlagSet("history", flag.ExitOnError)
	dbPath := fs.String("db", "queue.db", "database path")
	jsonOutput := fs.Bool("json", false, "output as JSON")

	// Move the first non-flag argument to the end so the Go flag package parses
	// all flags. This lets both "history <id> -db p" and "history -db p <id>".
	ordered := moveFirstPositionalToEnd(args)
	fs.Parse(ordered)

	if fs.NArg() == 0 {
		return fmt.Errorf("usage: history <job-id> [-db path] [-json]")
	}
	jobID := fs.Arg(0)

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	job, err := store.GetJob(jobID)
	if err != nil {
		return fmt.Errorf("get job: %w", err)
	}
	events, err := store.GetJobEvents(jobID)
	if err != nil {
		return fmt.Errorf("get events: %w", err)
	}

	if *jsonOutput {
		out := map[string]any{"job": job, "events": events}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}

	fmt.Printf("Job %s\n", job.ID)
	fmt.Printf("  kind     : %s\n", job.Kind)
	fmt.Printf("  payload  : %s\n", job.Payload)
	fmt.Printf("  state    : %s\n", job.State)
	fmt.Printf("  attempts : %d / %d\n", job.RetryCount, job.MaxAttempts)
	if job.IdempotencyKey != nil {
		fmt.Printf("  idem key : %s\n", *job.IdempotencyKey)
	}
	if job.RunAt != nil {
		fmt.Printf("  run at   : %s\n", job.RunAt.Format("2006-01-02 15:04:05"))
	}

	fmt.Printf("\nEvents (%d)\n", len(events))
	for _, e := range events {
		md := ""
		if e.Metadata != nil {
			md = *e.Metadata
		}
		fmt.Printf("  %s  %-12s  %s\n",
			e.Timestamp.Format("2006-01-02 15:04:05"), e.EventType, md)
	}
	return nil
}

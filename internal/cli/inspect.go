package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/local-first-job-queue/internal/queue"
)

func Inspect(args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	dbPath := fs.String("db", "queue.db", "database path")
	jsonOutput := fs.Bool("json", false, "output as JSON")
	fs.Parse(args)

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	q := queue.NewQueue(store)
	snap, err := q.Inspect()
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(snap)
	}

	RenderSnapshot(snap, os.Stdout)
	return nil
}

// RenderSnapshot writes a human-readable view of the queue to w. It is shared by
// the inspect command and the demo command so the two stay in sync.
func RenderSnapshot(snap *queue.QueueSnapshot, w io.Writer) {
	fmt.Fprintln(w, "Queue state")
	fmt.Fprintln(w, "-----------")
	if len(snap.Stats) == 0 {
		fmt.Fprintln(w, "  (empty)")
	} else {
		for _, s := range []queue.JobState{
			queue.StatePending, queue.StateLeased,
			queue.StateCompleted, queue.StateFailed,
		} {
			if c, ok := snap.Stats[s]; ok {
				fmt.Fprintf(w, "  %s: %d\n", s, c)
			}
		}
	}

	fmt.Fprintf(w, "\nRecent events (%d)\n", len(snap.Events))
	fmt.Fprintln(w, "--------------")
	for _, e := range snap.Events {
		md := ""
		if e.Metadata != nil {
			md = *e.Metadata
		}
		shortID := e.JobID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}
		fmt.Fprintf(w, "  [%s] %s %s %s\n",
			e.Timestamp.Format("15:04:05"), shortID, e.EventType, md)
	}

	if len(snap.Jobs) > 0 {
		fmt.Fprintf(w, "\nJobs (%d)\n", len(snap.Jobs))
		fmt.Fprintln(w, "--------")
		for _, j := range snap.Jobs {
			ik := ""
			if j.IdempotencyKey != nil {
				ik = fmt.Sprintf(" ik=%s", *j.IdempotencyKey)
			}
			sched := ""
			if j.RunAt != nil {
				sched = fmt.Sprintf(" run_at=%s", j.RunAt.Format("2006-01-02 15:04:05"))
			}
			shortID := j.ID
			if len(shortID) > 8 {
				shortID = shortID[:8]
			}
			fmt.Fprintf(w, "  %s kind=%s priority=%d state=%s attempts=%d/%d%s%s\n",
				shortID, j.Kind, j.Priority, j.State, j.RetryCount, j.MaxAttempts, ik, sched)
		}
	}
}

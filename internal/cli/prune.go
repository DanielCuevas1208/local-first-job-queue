package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

// Prune applies a retention policy to the queue. The -age limit removes
// completed, dead-lettered, and legacy failed jobs whose last update is older
// than the duration, together with their events. The -max-events limit keeps
// only the newest events for every surviving job, which stops the append-only
// log from growing without bound. At least one limit must be set.
func Prune(args []string) error {
	fs := flag.NewFlagSet("prune", flag.ExitOnError)
	dbPath := fs.String("db", "queue.db", "database path")
	age := fs.Duration("age", 0, "remove terminal jobs last updated before now minus this duration (0 disables)")
	maxEvents := fs.Int("max-events", 0, "keep only the newest events per surviving job (0 disables)")
	jsonOutput := fs.Bool("json", false, "output the result as JSON")
	fs.Parse(args)

	if *age <= 0 && *maxEvents <= 0 {
		return fmt.Errorf("set -age or -max-events so the run removes something")
	}

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	res, err := store.Prune(queue.PrunePolicy{
		MaxJobAge:       *age,
		MaxEventsPerJob: *maxEvents,
	})
	if err != nil {
		return fmt.Errorf("prune: %w", err)
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	renderPruneResult(*age, *maxEvents, res, os.Stdout)
	return nil
}

// renderPruneResult writes a human-readable report of one retention run. It is
// separate from Prune so tests can capture the output without a database path.
func renderPruneResult(age time.Duration, maxEvents int, res queue.PruneResult, w io.Writer) {
	fmt.Fprintln(w, "Retention run")
	fmt.Fprintln(w, "-------------")
	fmt.Fprintf(w, "  policy: age=%s max_events=%d\n", age, maxEvents)
	fmt.Fprintf(w, "  jobs removed: %d\n", res.JobsRemoved)
	fmt.Fprintf(w, "  events removed: %d\n", res.EventsRemoved)
}

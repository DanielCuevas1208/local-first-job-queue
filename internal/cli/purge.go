package cli

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

// stringList is a repeatable and comma-separated string flag.
type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			*s = append(*s, part)
		}
	}
	return nil
}

// validStates is the set of states the purge command accepts on the command
// line. The list matches the states the store and the metrics renderer know.
var validStates = []queue.JobState{
	queue.StatePending,
	queue.StateLeased,
	queue.StateCompleted,
	queue.StateFailed,
	queue.StateDeadLetter,
}

// Purge removes finished jobs and their events from the store. An operator
// uses it to enforce a retention policy: keep the queue small and the event
// log bounded. By default the command targets the terminal states: completed,
// failed, and dead_letter. Use -state to name different states, -before to
// keep recent history, and -dry-run to preview what would be removed.
func Purge(args []string) error {
	fs := flag.NewFlagSet("purge", flag.ExitOnError)
	dbPath := fs.String("db", "queue.db", "database path")
	dryRun := fs.Bool("dry-run", false, "report what would be removed without removing it")
	beforeDur := fs.Duration("before", 0, "remove only jobs not updated within this duration; 0 removes every age")
	var stateFlags stringList
	fs.Var(&stateFlags, "state", "job state to purge; repeatable or comma-separated (default: completed, failed, dead_letter)")
	fs.Parse(args)

	states, err := parsePurgeStates(stateFlags)
	if err != nil {
		return err
	}

	var before *time.Time
	if *beforeDur > 0 {
		t := time.Now().UTC().Add(-*beforeDur)
		before = &t
	}

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	q := queue.NewQueue(store)
	return runPurge(os.Stdout, q, states, before, *dryRun)
}

// runPurge applies a retention filter to the queue and writes the outcome to
// w. It is separate from Purge so tests can capture the output without
// touching the process stdout. A nil before means no age filter, and a nil
// state list uses the command default, which is the terminal states.
func runPurge(w io.Writer, q *queue.Queue, states []queue.JobState, before *time.Time, dryRun bool) error {
	opts := []queue.PurgeOption{}
	if len(states) > 0 {
		opts = append(opts, queue.PurgeStates(states...))
	}
	if before != nil {
		opts = append(opts, queue.PurgeBefore(*before))
	}

	if dryRun {
		stats, err := q.PurgeCandidates(opts...)
		if err != nil {
			return fmt.Errorf("preview purge: %w", err)
		}
		fmt.Fprintf(w, "dry run: would remove %d jobs and %d events\n", stats.JobsRemoved, stats.EventsRemoved)
		if stats.JobsRemoved == 0 {
			fmt.Fprintln(w, "no jobs match the current filter.")
		} else {
			fmt.Fprintln(w, "run without -dry-run to apply.")
		}
		return nil
	}

	stats, err := q.Purge(opts...)
	if err != nil {
		return fmt.Errorf("purge: %w", err)
	}
	fmt.Fprintf(w, "removed %d jobs and %d events\n", stats.JobsRemoved, stats.EventsRemoved)
	switch {
	case stats.JobsRemoved == 0:
		fmt.Fprintln(w, "no jobs match the current filter.")
	case stats.JobsRemoved > 0:
		fmt.Fprintln(w, "run 'jobqueue inspect' to confirm the queue state.")
	}
	return nil
}

// parsePurgeStates converts the -state flag values into job states. An empty
// list keeps the command default, which the queue library applies when no
// option is present. Each value may carry one state or a comma-separated list,
// so callers can pass -state twice or as a single list. Unknown state names
// are rejected with the valid list.
func parsePurgeStates(values stringList) ([]queue.JobState, error) {
	if len(values) == 0 {
		return nil, nil
	}
	valid := map[string]bool{}
	for _, st := range validStates {
		valid[string(st)] = true
	}
	var states []queue.JobState
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if !valid[part] {
				return nil, fmt.Errorf("unknown state %q (valid: %s)", part, strings.Join(validStateNames(), ", "))
			}
			states = append(states, queue.JobState(part))
		}
	}
	return states, nil
}

func validStateNames() []string {
	names := make([]string, 0, len(validStates))
	for _, st := range validStates {
		names = append(names, string(st))
	}
	return names
}

package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

// buildPurgeDB returns an in-memory database with two completed jobs and one
// pending job. Tests reuse it to verify the purge command end to end.
func buildPurgeDB(t *testing.T) (*queue.Queue, *queue.SQLiteStore) {
	t.Helper()
	store, err := queue.NewSQLiteStore("file:cli_purge_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	q := queue.NewQueue(store)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		kind := "done" + string(rune('0'+i))
		if _, err := q.Enqueue(kind, `{"n":`+string(rune('0'+i))+`}`); err != nil {
			t.Fatalf("enqueue %s: %v", kind, err)
		}
		job, err := q.Lease(ctx, kind, time.Minute)
		if err != nil || job == nil {
			t.Fatalf("lease %s: job=%v err=%v", kind, job, err)
		}
		if err := q.Acknowledge(job.ID); err != nil {
			t.Fatalf("ack %s: %v", kind, err)
		}
	}
	if _, err := q.Enqueue("waiting", `{}`); err != nil {
		t.Fatalf("enqueue waiting: %v", err)
	}
	return q, store
}

// TestPurgeCommandRemovesTerminalJobs verifies that the purge command removes
// completed jobs, keeps pending work, and reports the removed counts.
func TestPurgeCommandRemovesTerminalJobs(t *testing.T) {
	q, _ := buildPurgeDB(t)

	var b strings.Builder
	if err := runPurge(&b, q, nil, nil, false); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if want := "removed 2 jobs and 6 events"; !strings.Contains(b.String(), want) {
		t.Errorf("expected %q in output, got %q", want, b.String())
	}

	snap, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if snap.Stats[queue.StateCompleted] != 0 {
		t.Errorf("expected 0 completed jobs, got %d", snap.Stats[queue.StateCompleted])
	}
	if snap.Stats[queue.StatePending] != 1 {
		t.Errorf("expected 1 pending job, got %d", snap.Stats[queue.StatePending])
	}
}

// TestPurgeCommandDryRunDoesNotRemove verifies that a dry run reports the same
// counts as a real purge but leaves the store unchanged.
func TestPurgeCommandDryRunDoesNotRemove(t *testing.T) {
	q, _ := buildPurgeDB(t)

	var b strings.Builder
	if err := runPurge(&b, q, nil, nil, true); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if want := "dry run: would remove 2 jobs and 6 events"; !strings.Contains(b.String(), want) {
		t.Errorf("expected %q in output, got %q", want, b.String())
	}
	if !strings.Contains(b.String(), "run without -dry-run to apply") {
		t.Errorf("expected the apply hint in output, got %q", b.String())
	}

	snap, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(snap.Jobs) != 3 {
		t.Errorf("dry run must not remove jobs, got %d", len(snap.Jobs))
	}
}

// TestPurgeCommandStateFilter verifies that a state filter limits the purge to
// the named state and that unknown states are rejected.
func TestPurgeCommandStateFilter(t *testing.T) {
	q, _ := buildPurgeDB(t)

	var b strings.Builder
	if err := runPurge(&b, q, []queue.JobState{queue.StatePending}, nil, false); err != nil {
		t.Fatalf("purge pending: %v", err)
	}
	if want := "removed 1 jobs and 1 events"; !strings.Contains(b.String(), want) {
		t.Errorf("expected %q in output, got %q", want, b.String())
	}

	snap, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if snap.Stats[queue.StatePending] != 0 {
		t.Errorf("expected pending jobs purged, got %d", snap.Stats[queue.StatePending])
	}
	if snap.Stats[queue.StateCompleted] != 2 {
		t.Errorf("expected completed jobs to survive, got %d", snap.Stats[queue.StateCompleted])
	}

	if _, err := parsePurgeStates(stringList{"nonsense"}); err == nil {
		t.Errorf("expected an error for an unknown state")
	}
}

// TestPurgeCommandBeforeKeepsRecent verifies that an age filter keeps jobs
// updated after the cutoff. Every job in the fixture is fresh, so a short
// retention window removes nothing.
func TestPurgeCommandBeforeKeepsRecent(t *testing.T) {
	q, _ := buildPurgeDB(t)

	before := time.Now().UTC().Add(-24 * time.Hour)
	var b strings.Builder
	if err := runPurge(&b, q, nil, &before, false); err != nil {
		t.Fatalf("purge before: %v", err)
	}
	if want := "removed 0 jobs and 0 events"; !strings.Contains(b.String(), want) {
		t.Errorf("expected %q in output, got %q", want, b.String())
	}
	if !strings.Contains(b.String(), "no jobs match") {
		t.Errorf("expected the no-match note, got %q", b.String())
	}
}

// TestParsePurgeStates verifies the state parsing rules: empty is the command
// default, one value works, and comma-separated values expand.
func TestParsePurgeStates(t *testing.T) {
	states, err := parsePurgeStates(nil)
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if len(states) != 0 {
		t.Errorf("empty input must yield no states, got %v", states)
	}

	states, err = parsePurgeStates(stringList{"pending"})
	if err != nil || len(states) != 1 || states[0] != queue.StatePending {
		t.Errorf("single state: states=%v err=%v", states, err)
	}

	states, err = parsePurgeStates(stringList{"pending, completed"})
	if err != nil || len(states) != 2 {
		t.Errorf("comma-separated: states=%v err=%v", states, err)
	}

	if _, err := parsePurgeStates(stringList{"pending", "bogus"}); err == nil {
		t.Errorf("expected an error for an unknown state")
	}
}

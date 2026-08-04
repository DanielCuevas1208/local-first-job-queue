package cli

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

// seedCompletedJob writes one completed job to a database file, then backdates
// its last update so an age-based prune run can remove it.
func seedCompletedJob(t *testing.T, path string) {
	t.Helper()
	s, err := queue.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	q := queue.NewQueue(s)
	ctx := context.Background()
	if _, err := q.Enqueue("email", `{"to":"a@example.com"}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job, err := q.Lease(ctx, "email", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("lease: %v %v", job, err)
	}
	if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if _, err := s.DB().Exec(
		`UPDATE jobs SET updated_at = ? WHERE id = ?`,
		time.Now().UTC().Add(-72*time.Hour).Format(time.RFC3339Nano), job.ID); err != nil {
		t.Fatalf("age job: %v", err)
	}
}

// countJobs reopens a database file and counts its jobs, so tests can verify
// what a prune run removed without depending on the command's stdout.
func countJobs(t *testing.T, path string) int {
	t.Helper()
	s, err := queue.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer s.Close()
	jobs, err := s.GetAllJobs()
	if err != nil {
		t.Fatalf("get jobs: %v", err)
	}
	return len(jobs)
}

// TestPruneCommandRemovesOldJobs verifies the end-to-end CLI path. The command
// opens the database file, applies an age policy, and removes the aged
// completed job. The store reopens cleanly with no rows left.
func TestPruneCommandRemovesOldJobs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	seedCompletedJob(t, path)

	if err := Prune([]string{"-db", path, "-age", "24h"}); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if got := countJobs(t, path); got != 0 {
		t.Errorf("expected 0 jobs after prune, got %d", got)
	}
}

// TestPruneCommandKeepsFreshJobs verifies that the -age limit leaves jobs
// inside the retention window untouched. The command reports success and the
// store still contains the fresh job.
func TestPruneCommandKeepsFreshJobs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s, err := queue.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	q := queue.NewQueue(s)
	if _, err := q.Enqueue("email", `{"to":"a@example.com"}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if err := Prune([]string{"-db", path, "-age", "24h", "-max-events", "1"}); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if got := countJobs(t, path); got != 1 {
		t.Errorf("expected 1 job to stay, got %d", got)
	}
}

// TestPruneCommandRequiresPolicy verifies that a run without any limit is
// rejected. A policy that cannot remove anything would only hide operator
// errors, so the command fails fast.
func TestPruneCommandRequiresPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	seedCompletedJob(t, path)

	err := Prune([]string{"-db", path})
	if err == nil {
		t.Fatal("expected an error when no policy flag is set")
	}
	if !strings.Contains(err.Error(), "-age or -max-events") {
		t.Errorf("error = %q, want a hint to set a policy flag", err)
	}
}

// TestRenderPruneResult verifies the human-readable report is stable. The
// policy and the removed counts appear in a fixed layout.
func TestRenderPruneResult(t *testing.T) {
	var out strings.Builder
	renderPruneResult(time.Hour, 500, queue.PruneResult{JobsRemoved: 2, EventsRemoved: 7}, &out)

	text := out.String()
	for _, want := range []string{
		"Retention run",
		"policy: age=1h0m0s max_events=500",
		"jobs removed: 2",
		"events removed: 7",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("report missing %q in:\n%s", want, text)
		}
	}
}

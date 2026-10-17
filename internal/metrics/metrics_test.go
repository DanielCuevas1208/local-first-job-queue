package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

func newTestStore(t *testing.T) (*queue.SQLiteStore, *queue.Queue) {
	t.Helper()
	s, err := queue.NewSQLiteStore("file:metrics_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, queue.NewQueue(s)
}

func render(t *testing.T, col *Collector) string {
	t.Helper()
	var b strings.Builder
	if err := col.Write(&b); err != nil {
		t.Fatalf("write metrics: %v", err)
	}
	return b.String()
}

// TestEmptyQueueRendersZeroMetrics verifies that an empty queue still produces
// the full metric families with explicit zero values. Absent families would
// hide state from a dashboard, so every known state and event type appears.
func TestEmptyQueueRendersZeroMetrics(t *testing.T) {
	s, _ := newTestStore(t)
	got := render(t, New(s))

	for _, want := range []string{
		"# HELP jobqueue_jobs Number of jobs in each state.",
		"# TYPE jobqueue_jobs gauge",
		`jobqueue_jobs{state="pending"} 0`,
		`jobqueue_jobs{state="leased"} 0`,
		`jobqueue_jobs{state="completed"} 0`,
		`jobqueue_jobs{state="dead_letter"} 0`,
		`jobqueue_jobs{state="failed"} 0`,
		"# TYPE jobqueue_events_total counter",
		`jobqueue_events_total{type="enqueued"} 0`,
		`jobqueue_events_total{type="recovered"} 0`,
		"# TYPE jobqueue_oldest_pending_seconds gauge",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("metrics missing %q in:\n%s", want, got)
		}
	}
	// The value line must be absent: an empty queue has nothing to measure.
	if strings.Contains(got, "jobqueue_oldest_pending_seconds 0") ||
		strings.Contains(got, "jobqueue_oldest_pending_seconds\t0") {
		t.Errorf("expected no oldest pending value when the queue is empty:\n%s", got)
	}
}

// TestWriteReflectsWorkload verifies that completed, dead-lettered, and pending
// jobs produce the expected gauges and that the event counters match the log.
func TestWriteReflectsWorkload(t *testing.T) {
	s, q := newTestStore(t)
	ctx := context.Background()

	if _, err := q.Enqueue("email", `{"to":"a@example.com"}`); err != nil {
		t.Fatalf("enqueue email: %v", err)
	}
	// Enqueue the failing report job with a higher priority so the first lease
	// of the report kind returns it, independent of enqueue timing.
	flaky, err := q.Enqueue("report", `{}`, queue.WithMaxAttempts(1), queue.WithPriority(1))
	if err != nil {
		t.Fatalf("enqueue flaky: %v", err)
	}
	if _, err := q.Enqueue("report", `{}`); err != nil {
		t.Fatalf("enqueue report: %v", err)
	}

	job, err := q.Lease(ctx, "email", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("lease email: %v %v", job, err)
	}
	if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	leased, err := q.Lease(ctx, "report", time.Minute)
	if err != nil || leased == nil || leased.ID != flaky.ID {
		t.Fatalf("lease flaky: %v %v", leased, err)
	}
	if err := q.Fail(leased.ID, "boom"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	got := render(t, New(s))

	for _, want := range []string{
		`jobqueue_jobs{state="pending"} 1`,
		`jobqueue_jobs{state="completed"} 1`,
		`jobqueue_jobs{state="dead_letter"} 1`,
		`jobqueue_jobs_by_kind{kind="email",state="completed"} 1`,
		`jobqueue_jobs_by_kind{kind="report",state="pending"} 1`,
		`jobqueue_jobs_by_kind{kind="report",state="dead_letter"} 1`,
		`jobqueue_events_total{type="enqueued"} 3`,
		`jobqueue_events_total{type="leased"} 2`,
		`jobqueue_events_total{type="acknowledged"} 1`,
		`jobqueue_events_total{type="dead_lettered"} 1`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("metrics missing %q in:\n%s", want, got)
		}
	}
}

// TestOldestPendingAgeIsDeterministic verifies that the oldest-pending metric
// uses the ready time and the injected clock. A scheduled job reports its exact
// age instead of a wall-clock value.
func TestOldestPendingAgeIsDeterministic(t *testing.T) {
	s, q := newTestStore(t)

	now := time.Now().UTC()
	job, err := q.Enqueue("report", `{}`, queue.WithRunAt(now.Add(-30*time.Second)))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if job.State != queue.StatePending {
		t.Fatalf("expected pending, got %s", job.State)
	}

	col := New(s, WithNow(func() time.Time { return now }))
	got := render(t, col)
	if want := "jobqueue_oldest_pending_seconds 30\n"; !strings.Contains(got, want) {
		t.Errorf("expected %q in:\n%s", strings.TrimSpace(want), got)
	}
}

// TestRetentionMetricsReportSourcesAndAge verifies that scrapes expose manual
// and automatic activity with stable labels and a deterministic last-run age.
func TestRetentionMetricsReportSourcesAndAge(t *testing.T) {
	s, q := newTestStore(t)
	job, err := q.Enqueue("test", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(context.Background(), "test", time.Minute)
	if err != nil || leased == nil || leased.ID != job.ID {
		t.Fatalf("lease: %v %v", leased, err)
	}
	if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if _, err := q.Prune(queue.PrunePolicy{MaxEventsPerJob: 1}); err != nil {
		t.Fatalf("manual prune: %v", err)
	}
	if _, err := q.PruneAuto(queue.PrunePolicy{MaxEventsPerJob: 1}); err != nil {
		t.Fatalf("automatic prune: %v", err)
	}

	last, err := s.GetLastRetentionRun()
	if err != nil || last == nil {
		t.Fatalf("last retention run: %v %v", last, err)
	}
	col := New(s, WithNow(func() time.Time { return last.StartedAt.Add(7 * time.Second) }))
	got := render(t, col)
	for _, want := range []string{
		`jobqueue_retention_runs_total{source="manual"} 1`,
		`jobqueue_retention_runs_total{source="auto"} 1`,
		`jobqueue_retention_events_removed_total{source="manual"} 2`,
		`jobqueue_retention_events_removed_total{source="auto"} 0`,
		"jobqueue_retention_last_run_seconds 7",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("metrics missing %q in:\n%s", want, got)
		}
	}
}

// TestHandlerScrapes verifies that the HTTP handler serves the exposition
// format on /metrics and a short landing page at the root.
func TestHandlerScrapes(t *testing.T) {
	s, _ := newTestStore(t)
	h := Handler(s)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("expected text content type, got %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "# TYPE jobqueue_jobs gauge") {
		t.Errorf("expected metrics body, got %q", rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 at root, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "/metrics") {
		t.Errorf("expected landing page to mention /metrics, got %q", rec.Body.String())
	}
}

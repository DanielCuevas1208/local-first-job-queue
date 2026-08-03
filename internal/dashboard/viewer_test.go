package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

func newTestStore(t *testing.T) (*queue.SQLiteStore, *queue.Queue) {
	t.Helper()
	s, err := queue.NewSQLiteStore("file:dashboard_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, queue.NewQueue(s)
}

// TestOverviewEmptyQueue verifies that a fresh store yields an overview with
// explicit zero counts and no oldest-pending value.
func TestOverviewEmptyQueue(t *testing.T) {
	s, _ := newTestStore(t)
	v := NewViewer(s)

	ov, err := v.Overview()
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	if ov.TotalJobs != 0 || ov.TotalEvents != 0 {
		t.Errorf("expected empty totals, got jobs=%d events=%d", ov.TotalJobs, ov.TotalEvents)
	}
	if ov.OldestPending != nil {
		t.Errorf("expected no oldest pending, got %v", *ov.OldestPending)
	}
	for _, state := range []queue.JobState{queue.StatePending, queue.StateLeased, queue.StateCompleted, queue.StateDeadLetter} {
		if ov.Stats[state] != 0 {
			t.Errorf("expected zero %s, got %d", state, ov.Stats[state])
		}
	}
	if len(ov.Jobs) != 0 || len(ov.Events) != 0 {
		t.Errorf("expected no jobs or events, got %d jobs %d events", len(ov.Jobs), len(ov.Events))
	}
}

// TestOverviewReflectsWorkload verifies that the overview aggregates the store
// state. One completed email job and one dead-lettered report job produce the
// expected counts, kind rows, and event totals.
func TestOverviewReflectsWorkload(t *testing.T) {
	s, q := newTestStore(t)
	ctx := context.Background()

	if _, err := q.Enqueue("email", `{"to":"a@example.com"}`); err != nil {
		t.Fatalf("enqueue email: %v", err)
	}
	flaky, err := q.Enqueue("report", `{}`, queue.WithMaxAttempts(1), queue.WithPriority(1))
	if err != nil {
		t.Fatalf("enqueue flaky: %v", err)
	}
	// A second report job stays pending, so the overview always has something
	// to measure and the kind rows include every non-terminal state.
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

	ov, err := NewViewer(s).Overview()
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	if ov.TotalJobs != 3 {
		t.Errorf("expected 3 jobs, got %d", ov.TotalJobs)
	}
	if ov.Stats[queue.StatePending] != 1 || ov.Stats[queue.StateCompleted] != 1 || ov.Stats[queue.StateDeadLetter] != 1 {
		t.Errorf("unexpected stats: %+v", ov.Stats)
	}
	if ov.OldestPending == nil {
		t.Error("expected an oldest pending age for the pending report job")
	}
	// The failing report job consumed its only attempt, so it dead-lettered and
	// the second report job stays pending. The kind rows reflect every state.
	if len(ov.ByKind) != 3 {
		t.Errorf("expected 3 kind rows, got %+v", ov.ByKind)
	}
	byType := map[queue.EventType]int{}
	for _, e := range ov.EventCounts {
		byType[e.EventType] = e.Count
	}
	if byType[queue.EventEnqueued] != 3 {
		t.Errorf("expected 3 enqueued events, got %d", byType[queue.EventEnqueued])
	}
	if byType[queue.EventDeadLettered] != 1 {
		t.Errorf("expected 1 dead_lettered event, got %d", byType[queue.EventDeadLettered])
	}
}

// TestOverviewOldestPendingUsesClock verifies that the oldest-pending age uses
// the injected clock. A scheduled job in the past reports an exact age.
func TestOverviewOldestPendingUsesClock(t *testing.T) {
	s, q := newTestStore(t)

	now := time.Now().UTC()
	if _, err := q.Enqueue("report", `{}`, queue.WithRunAt(now.Add(-45*time.Second))); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ov, err := NewViewer(s, WithNow(func() time.Time { return now })).Overview()
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	if ov.OldestPending == nil || *ov.OldestPending != 45 {
		t.Fatalf("expected oldest pending 45, got %v", ov.OldestPending)
	}
}

func do(t *testing.T, h http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPageRendersDashboard(t *testing.T) {
	s, q := newTestStore(t)
	if _, err := q.Enqueue("email", `{"to":"a@example.com"}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h := Handler(s, WithDBPath("queue.db"), WithRefreshInterval(3*time.Second))
	rec := do(t, h, http.MethodGet, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"<title>Local-first Durable Job Queue</title>",
		"db: queue.db",
		`id="kpi-pending"`,
		"id=\"jobs-table\"",
		"id=\"events-body\"",
		"email",
		"fetch(\"/api/overview\")",
		"/metrics",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("page missing %q", want)
		}
	}
}

// TestPageEscapesUserData verifies that job payloads and metadata cannot inject
// markup into the dashboard. html/template escapes user data automatically.
func TestPageEscapesUserData(t *testing.T) {
	s, q := newTestStore(t)
	if _, err := q.Enqueue("email", `<script>alert("x")</script>`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h := Handler(s)
	rec := do(t, h, http.MethodGet, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "<script>alert(\"x\")</script>") {
		t.Error("page rendered an unescaped script payload")
	}
}

func TestOverviewEndpointJSON(t *testing.T) {
	s, q := newTestStore(t)
	if _, err := q.Enqueue("email", `{}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h := Handler(s)
	rec := do(t, h, http.MethodGet, "/api/overview")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected JSON content type, got %q", ct)
	}
	var ov Overview
	if err := json.Unmarshal(rec.Body.Bytes(), &ov); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if ov.TotalJobs != 1 || ov.Stats[queue.StatePending] != 1 {
		t.Errorf("unexpected overview: %+v", ov)
	}
}

func TestJobsEndpointJSON(t *testing.T) {
	s, q := newTestStore(t)
	job, err := q.Enqueue("email", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h := Handler(s)
	rec := do(t, h, http.MethodGet, "/api/jobs")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var out struct {
		Jobs  []queue.Job `json:"jobs"`
		Total int         `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode jobs: %v", err)
	}
	if out.Total != 1 || len(out.Jobs) != 1 || out.Jobs[0].ID != job.ID {
		t.Errorf("unexpected jobs response: %+v", out)
	}
}

func TestJobDetailEndpointJSON(t *testing.T) {
	s, q := newTestStore(t)
	job, err := q.Enqueue("email", `{"n":1}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h := Handler(s)
	rec := do(t, h, http.MethodGet, "/api/jobs/"+job.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var out struct {
		Job    queue.Job     `json:"job"`
		Events []queue.Event `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	if out.Job.ID != job.ID {
		t.Errorf("expected job %s, got %s", job.ID, out.Job.ID)
	}
	if len(out.Events) != 1 || out.Events[0].EventType != queue.EventEnqueued {
		t.Errorf("expected one enqueued event, got %+v", out.Events)
	}
}

func TestJobDetailEndpointNotFound(t *testing.T) {
	s, _ := newTestStore(t)
	h := Handler(s)
	rec := do(t, h, http.MethodGet, "/api/jobs/does-not-exist")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	var out map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if out["error"] == "" {
		t.Error("expected an error message in the 404 body")
	}
}

func TestMetricsEndpointServed(t *testing.T) {
	s, q := newTestStore(t)
	if _, err := q.Enqueue("email", `{}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h := Handler(s)
	rec := do(t, h, http.MethodGet, "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "jobqueue_jobs") {
		t.Errorf("expected Prometheus families, got %q", rec.Body.String())
	}
}

func TestUnknownPathNotFound(t *testing.T) {
	s, _ := newTestStore(t)
	h := Handler(s)
	if rec := do(t, h, http.MethodGet, "/no-such-path"); rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown path, got %d", rec.Code)
	}
	if rec := do(t, h, http.MethodPost, "/api/overview"); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for POST, got %d", rec.Code)
	}
}

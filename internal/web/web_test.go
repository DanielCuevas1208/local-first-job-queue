package web

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

func newTestServer(t *testing.T) (*queue.SQLiteStore, *Server, http.Handler) {
	t.Helper()
	s, err := queue.NewSQLiteStore("file:web_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	srv, err := New(s)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return s, srv, srv.Handler()
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// seedWorkload builds a deterministic queue state: two pending email jobs, one
// completed report job, and one dead-lettered cleanup job. Every event type
// appears, so the dashboard and API have stable, meaningful numbers.
func seedWorkload(t *testing.T, q *queue.Queue) {
	t.Helper()
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := q.Enqueue("email", `{"to":"user@example.com"}`); err != nil {
			t.Fatalf("enqueue email: %v", err)
		}
	}

	_, err := q.Enqueue("report", `{"name":"daily"}`, queue.WithPriority(5))
	if err != nil {
		t.Fatalf("enqueue report: %v", err)
	}
	leased, err := q.Lease(ctx, "report", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease report: %v %v", leased, err)
	}
	if err := q.Acknowledge(leased.ID); err != nil {
		t.Fatalf("ack report: %v", err)
	}

	dead, err := q.Enqueue("cleanup", `{"name":"stale"}`, queue.WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("enqueue cleanup: %v", err)
	}
	if _, err := q.Lease(ctx, "cleanup", time.Minute); err != nil {
		t.Fatalf("lease cleanup: %v", err)
	}
	if err := q.Fail(dead.ID, "disk full"); err != nil {
		t.Fatalf("fail cleanup: %v", err)
	}
}

// TestDashboardShowsStateCounts verifies the dashboard renders the canonical
// state order with the seeded counts.
func TestDashboardShowsStateCounts(t *testing.T) {
	s, _, h := newTestServer(t)
	seedWorkload(t, queue.NewQueue(s))

	rec := get(t, h, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`data-state="pending">2`,
		`data-state="leased">0`,
		`data-state="completed">1`,
		`data-state="dead_letter">1`,
		`data-state="failed">0`,
		"total",
		"Jobs by kind and state",
		"Recent events",
		"Recent jobs",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard missing %q in:\n%s", want, body)
		}
	}
}

// TestJobsPageFiltersByState verifies the state filter narrows the job table.
func TestJobsPageFiltersByState(t *testing.T) {
	s, _, h := newTestServer(t)
	seedWorkload(t, queue.NewQueue(s))

	rec := get(t, h, "/jobs?state=pending")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if count := strings.Count(body, `<span class="state state-pending">pending</span>`); count != 2 {
		t.Errorf("expected 2 pending rows, got %d", count)
	}
	if strings.Contains(body, "state-completed") {
		t.Errorf("completed jobs must not appear in the pending filter:\n%s", body)
	}
}

// TestJobsPageFiltersByKind verifies the kind filter narrows the job table.
func TestJobsPageFiltersByKind(t *testing.T) {
	s, _, h := newTestServer(t)
	seedWorkload(t, queue.NewQueue(s))

	rec := get(t, h, "/jobs?kind=email")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if count := strings.Count(body, "email"); count < 2 {
		t.Errorf("expected email rows, got:\n%s", body)
	}
	if strings.Contains(body, "cleanup") {
		t.Errorf("cleanup jobs must not appear in the email filter:\n%s", body)
	}
}

// TestJobDetailShowsTimeline verifies the job page lists the full event log.
func TestJobDetailShowsTimeline(t *testing.T) {
	s, _, h := newTestServer(t)
	q := queue.NewQueue(s)
	job, err := q.Enqueue("test", `{"name":"flaky"}`, queue.WithMaxAttempts(2))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	ctx := context.Background()
	if _, err := q.Lease(ctx, "test", time.Minute); err != nil {
		t.Fatalf("lease: %v", err)
	}
	if err := q.Fail(job.ID, "boom"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	rec := get(t, h, "/jobs/"+job.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"Event timeline",
		`&#34;name&#34;:&#34;flaky&#34;`,
		">enqueued<",
		">leased<",
		">retried<",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("job page missing %q in:\n%s", want, body)
		}
	}
}

// TestJobDetailUnknownIDReturns404 verifies an unknown job id yields a 404.
func TestJobDetailUnknownIDReturns404(t *testing.T) {
	s, _, h := newTestServer(t)
	_ = s
	rec := get(t, h, "/jobs/does-not-exist")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// TestHTMLPayloadIsEscaped verifies payload content cannot inject markup.
func TestHTMLPayloadIsEscaped(t *testing.T) {
	s, _, h := newTestServer(t)
	q := queue.NewQueue(s)
	job, err := q.Enqueue("test", `{"x":"<script>alert(1)</script>"}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	rec := get(t, h, "/jobs/"+job.ID)
	body := rec.Body.String()
	if strings.Contains(body, "<script>alert(1)") {
		t.Fatalf("payload must be escaped:\n%s", body)
	}
	if !strings.Contains(body, "&lt;script&gt;") {
		t.Errorf("expected escaped payload in:\n%s", body)
	}
}

// TestAPIOverview verifies the overview endpoint returns the seeded snapshot.
func TestAPIOverview(t *testing.T) {
	s, _, h := newTestServer(t)
	seedWorkload(t, queue.NewQueue(s))

	rec := get(t, h, "/api/overview")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected json content type, got %q", ct)
	}

	var ov Overview
	if err := json.Unmarshal(rec.Body.Bytes(), &ov); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if ov.Total != 4 {
		t.Errorf("expected total 4, got %d", ov.Total)
	}
	byState := map[queue.JobState]int{}
	for _, s := range ov.Stats {
		byState[s.State] = s.Count
	}
	if byState[queue.StatePending] != 2 {
		t.Errorf("expected 2 pending, got %d", byState[queue.StatePending])
	}
	if byState[queue.StateCompleted] != 1 {
		t.Errorf("expected 1 completed, got %d", byState[queue.StateCompleted])
	}
	if byState[queue.StateDeadLetter] != 1 {
		t.Errorf("expected 1 dead_letter, got %d", byState[queue.StateDeadLetter])
	}
	if len(ov.Jobs) != 4 {
		t.Errorf("expected 4 jobs in overview, got %d", len(ov.Jobs))
	}
	if len(ov.Events) == 0 {
		t.Errorf("expected events in overview")
	}
}

// TestAPIJobsFilters verifies the jobs endpoint honors state and kind filters.
func TestAPIJobsFilters(t *testing.T) {
	s, _, h := newTestServer(t)
	seedWorkload(t, queue.NewQueue(s))

	rec := get(t, h, "/api/jobs?state=pending")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var jobs []queue.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("decode jobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("expected 2 pending jobs, got %d", len(jobs))
	}
	for _, j := range jobs {
		if j.State != queue.StatePending {
			t.Errorf("expected pending, got %s", j.State)
		}
	}

	rec = get(t, h, "/api/jobs?kind=cleanup")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("decode jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Kind != "cleanup" {
		t.Errorf("expected one cleanup job, got %+v", jobs)
	}
}

// TestAPIJobDetail verifies the single-job endpoint returns job and events.
func TestAPIJobDetail(t *testing.T) {
	s, _, h := newTestServer(t)
	q := queue.NewQueue(s)
	job, err := q.Enqueue("test", `{"n":1}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := get(t, h, "/api/jobs/"+job.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var out struct {
		Job    queue.Job     `json:"job"`
		Events []queue.Event `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Job.ID != job.ID {
		t.Errorf("expected job %s, got %s", job.ID, out.Job.ID)
	}
	if len(out.Events) != 1 || out.Events[0].EventType != queue.EventEnqueued {
		t.Errorf("expected one enqueued event, got %+v", out.Events)
	}
}

// TestAPIJobDetailUnknownIDReturns404 verifies the single-job endpoint 404s.
func TestAPIJobDetailUnknownIDReturns404(t *testing.T) {
	s, _, h := newTestServer(t)
	_ = s
	rec := get(t, h, "/api/jobs/does-not-exist")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// TestJobsPageRejectsWriteMethods verifies the page accepts only GET.
func TestJobsPageRejectsWriteMethods(t *testing.T) {
	s, _, h := newTestServer(t)
	_ = s
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(""))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

// TestJobsPageRejectsInvalidLimit verifies a malformed limit is rejected.
func TestJobsPageRejectsInvalidLimit(t *testing.T) {
	s, _, h := newTestServer(t)
	_ = s
	rec := get(t, h, "/jobs?limit=abc")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

// TestStaticAssetsServed verifies the stylesheet is embedded and served.
func TestStaticAssetsServed(t *testing.T) {
	s, _, h := newTestServer(t)
	_ = s
	rec := get(t, h, "/static/style.css")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/css") {
		t.Errorf("expected css content type, got %q", ct)
	}
	if !strings.Contains(rec.Body.String(), ".stats") {
		t.Errorf("expected stylesheet content")
	}
}

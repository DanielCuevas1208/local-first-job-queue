package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

func newServer(t *testing.T) (*Server, *queue.SQLiteStore, *queue.Queue) {
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
	return srv, s, queue.NewQueue(s)
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestDashboardServesHTML verifies that the root path renders the dashboard
// shell with the live summary cards.
func TestDashboardServesHTML(t *testing.T) {
	srv, _, q := newServer(t)
	if _, err := q.Enqueue("email", `{"to":"a@example.com"}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := get(t, srv.Handler(), "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"Local-first Durable Job Queue",
		"/api/jobs",
		"pending",
		"dead_letter",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
	// The summary cards reflect the live store.
	if !strings.Contains(body, `class="card-value" data-state="pending">1`) {
		t.Errorf("dashboard missing pending count 1:\n%s", body)
	}
}

// TestAPISummary verifies that the summary endpoint returns the state counts in
// a fixed order together with the known kinds.
func TestAPISummary(t *testing.T) {
	srv, _, q := newServer(t)
	if _, err := q.Enqueue("report", `{}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := get(t, srv.Handler(), "/api/summary")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected JSON content type, got %q", ct)
	}

	var sum struct {
		States      []stateCount `json:"states"`
		Kinds       []string     `json:"kinds"`
		EventsTotal int          `json:"events_total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &sum); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if sum.States[0].State != queue.StatePending || sum.States[0].Count != 1 {
		t.Errorf("expected pending count first, got %+v", sum.States)
	}
	if len(sum.Kinds) != 1 || sum.Kinds[0] != "report" {
		t.Errorf("expected report kind, got %v", sum.Kinds)
	}
	if sum.EventsTotal != 1 {
		t.Errorf("expected 1 event, got %d", sum.EventsTotal)
	}
}

// TestAPIJobsFilters verifies that the jobs endpoint honors state and query
// filters and reports the total match count.
func TestAPIJobsFilters(t *testing.T) {
	srv, _, q := newServer(t)
	ctx := context.Background()

	if _, err := q.Enqueue("email", `{"to":"a@example.com"}`); err != nil {
		t.Fatalf("enqueue email: %v", err)
	}
	flaky, err := q.Enqueue("report", `{"name":"nightly"}`, queue.WithMaxAttempts(1), queue.WithPriority(1))
	if err != nil {
		t.Fatalf("enqueue flaky: %v", err)
	}
	if _, err := q.Enqueue("report", `{"name":"weekly"}`); err != nil {
		t.Fatalf("enqueue report: %v", err)
	}

	leased, err := q.Lease(ctx, "report", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease flaky: %v %v", leased, err)
	}
	if err := q.Fail(leased.ID, "boom"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	h := srv.Handler()

	all := get(t, h, "/api/jobs")
	var page struct {
		Jobs  []queue.Job `json:"jobs"`
		Total int         `json:"total"`
	}
	if err := json.Unmarshal(all.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode jobs: %v", err)
	}
	if page.Total != 3 || len(page.Jobs) != 3 {
		t.Errorf("expected 3 jobs, got total=%d len=%d", page.Total, len(page.Jobs))
	}

	dl := get(t, h, "/api/jobs?state=dead_letter")
	if err := json.Unmarshal(dl.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode dead letters: %v", err)
	}
	if page.Total != 1 || len(page.Jobs) != 1 || page.Jobs[0].ID != flaky.ID {
		t.Errorf("expected one dead letter for %s, got %+v", flaky.ID, page)
	}

	search := get(t, h, "/api/jobs?q=nightly")
	if err := json.Unmarshal(search.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode search: %v", err)
	}
	if page.Total != 1 || page.Jobs[0].ID != flaky.ID {
		t.Errorf("expected the nightly job, got %+v", page)
	}
}

// TestAPIJobsEmptyQueueReturnsEmptyArray verifies that a queue without matching
// jobs reports an empty list rather than null. The dashboard iterates the list
// directly, so null would break its first render on a fresh queue.
func TestAPIJobsEmptyQueueReturnsEmptyArray(t *testing.T) {
	srv, _, _ := newServer(t)

	rec := get(t, srv.Handler(), "/api/jobs")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), `"jobs": null`) {
		t.Fatalf("expected an empty array, got null:\n%s", rec.Body.String())
	}
	var page struct {
		Jobs []queue.Job `json:"jobs"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode jobs: %v", err)
	}
	if page.Jobs == nil || len(page.Jobs) != 0 {
		t.Errorf("expected a non-nil empty array, got %+v", page.Jobs)
	}
}

// TestJobPageEscapesPayload verifies that job payload markup is escaped in the
// rendered page. Go templates escape by default, so a hostile payload cannot
// inject script into the detail view.
func TestJobPageEscapesPayload(t *testing.T) {
	srv, _, q := newServer(t)
	payload := `{"name":"x","x":"<script>alert(1)</script>"}`
	job, err := q.Enqueue("test", payload)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	body := get(t, srv.Handler(), "/job/"+job.ID).Body.String()
	if strings.Contains(body, "<script>") {
		t.Fatalf("payload markup was not escaped:\n%s", body)
	}
	if !strings.Contains(body, "&lt;script&gt;") {
		t.Errorf("expected the escaped payload, got:\n%s", body)
	}
}

// TestAPIJobDetail verifies the single-job endpoint returns the job with its
// event timeline, and that an unknown job yields a 404.
func TestAPIJobDetail(t *testing.T) {
	srv, _, q := newServer(t)
	job, err := q.Enqueue("email", `{"to":"a@example.com"}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h := srv.Handler()
	rec := get(t, h, "/api/jobs/"+job.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var detail struct {
		Job    queue.Job     `json:"job"`
		Events []queue.Event `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	if detail.Job.ID != job.ID {
		t.Errorf("expected job %s, got %s", job.ID, detail.Job.ID)
	}
	if len(detail.Events) != 1 || detail.Events[0].EventType != queue.EventEnqueued {
		t.Errorf("expected one enqueued event, got %+v", detail.Events)
	}

	missing := get(t, h, "/api/jobs/nope")
	if missing.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown job, got %d", missing.Code)
	}
}

// TestJobPageRenders verifies the job detail page shows the timeline and the
// payload, and that an unknown job yields a 404 page.
func TestJobPageRenders(t *testing.T) {
	srv, _, q := newServer(t)
	job, err := q.Enqueue("email", `{"to":"a@example.com","tag":"hello"}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h := srv.Handler()
	rec := get(t, h, "/job/"+job.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	for _, want := range []string{job.ID, "Event log", "enqueued", "hello", "Z"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("job page missing %q", want)
		}
	}
	if !regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}Z`).MatchString(rec.Body.String()) {
		t.Errorf("job page should show a UTC timestamp:\n%s", rec.Body.String())
	}

	missing := get(t, h, "/job/nope")
	if missing.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown job, got %d", missing.Code)
	}
}

// TestStaticAssets verifies the embedded CSS and JS are served.
func TestStaticAssets(t *testing.T) {
	srv, _, _ := newServer(t)
	h := srv.Handler()

	css := get(t, h, "/static/app.css")
	if css.Code != http.StatusOK || !strings.Contains(css.Body.String(), "topbar") {
		t.Errorf("expected css, got %d", css.Code)
	}

	js := get(t, h, "/static/app.js")
	if js.Code != http.StatusOK || !strings.Contains(js.Body.String(), "auto-refresh") {
		t.Errorf("expected js, got %d", js.Code)
	}
}

// TestHealthz verifies the health endpoint reports readiness.
func TestHealthz(t *testing.T) {
	srv, _, _ := newServer(t)
	rec := get(t, srv.Handler(), "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "ok") {
		t.Errorf("expected ok body, got %q", rec.Body.String())
	}
}

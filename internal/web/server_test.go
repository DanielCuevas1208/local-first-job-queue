package web

import (
	"bytes"
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

func postJSON(t *testing.T, h http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
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

// TestAPISummaryIncludesRetentionActivity verifies that the dashboard summary
// exposes source totals and the newest runs after manual and automatic passes.
func TestAPISummaryIncludesRetentionActivity(t *testing.T) {
	srv, _, q := newServer(t)
	job, err := q.Enqueue("retention", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(context.Background(), "retention", time.Minute)
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

	rec := get(t, srv.Handler(), "/api/summary")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var sum summary
	if err := json.Unmarshal(rec.Body.Bytes(), &sum); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if sum.Retention.Runs != 2 || sum.Retention.JobsRemoved != 0 || sum.Retention.EventsRemoved != 2 {
		t.Errorf("unexpected retention totals: %+v", sum.Retention)
	}
	if len(sum.Retention.RecentRuns) != 2 {
		t.Fatalf("expected two recent runs, got %+v", sum.Retention.RecentRuns)
	}
	if sum.Retention.RecentRuns[0].Source != queue.RetentionSourceAuto ||
		sum.Retention.RecentRuns[1].Source != queue.RetentionSourceManual {
		t.Errorf("expected newest runs first by source, got %+v", sum.Retention.RecentRuns)
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

// TestAPIRequeueDeadLetter verifies that the dashboard action resets the
// attempt budget, accepts a corrected payload, and appends an event.
func TestAPIRequeueDeadLetter(t *testing.T) {
	srv, _, q := newServer(t)
	job, err := q.Enqueue("report", `{"broken":true}`, queue.WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(context.Background(), "report", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease: %v %v", leased, err)
	}
	if err := q.Fail(leased.ID, "invalid payload"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	h := srv.Handler()
	detail := get(t, h, "/job/"+job.ID)
	if !strings.Contains(detail.Body.String(), "Requeue job") {
		t.Fatalf("dead-letter detail page has no requeue action:\n%s", detail.Body.String())
	}

	rec := postJSON(t, h, "/api/jobs/"+job.ID+"/requeue", `{"payload":"fixed","max_attempts":2}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var result struct {
		Job queue.Job `json:"job"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode requeue response: %v", err)
	}
	if result.Job.State != queue.StatePending || result.Job.RetryCount != 0 ||
		result.Job.MaxAttempts != 2 || result.Job.Payload != "fixed" {
		t.Errorf("unexpected requeued job: %+v", result.Job)
	}
	events, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(events.Events) != 4 || events.Events[0].EventType != queue.EventRequeued {
		t.Errorf("expected requeued event newest, got %+v", events.Events)
	}
	trailing := postJSON(t, h, "/api/jobs/"+job.ID+"/requeue", "{} null")
	if trailing.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for trailing JSON value, got %d", trailing.Code)
	}

	repeat := postJSON(t, h, "/api/jobs/"+job.ID+"/requeue", `{}`)
	if repeat.Code != http.StatusConflict {
		t.Errorf("expected 409 for a pending job, got %d", repeat.Code)
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

// TestBasicAuthGuardsPages verifies that a server configured with credentials
// rejects anonymous requests with a 401 and the browser prompt header. The
// health endpoint stays open so load balancers can probe readiness.
func TestBasicAuthGuardsPages(t *testing.T) {
	s, err := queue.NewSQLiteStore("file:web_auth_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	srv, err := New(s, WithBasicAuth("operator", "hunter2"))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	h := srv.Handler()

	rec := get(t, h, "/")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for anonymous dashboard, got %d", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); !strings.Contains(got, "Basic") {
		t.Errorf("expected WWW-Authenticate Basic header, got %q", got)
	}

	api := get(t, h, "/api/jobs")
	if api.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for anonymous API, got %d", api.Code)
	}
	write := postJSON(t, h, "/api/jobs/nope/requeue", `{}`)
	if write.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for anonymous requeue, got %d", write.Code)
	}

	health := get(t, h, "/healthz")
	if health.Code != http.StatusOK {
		t.Fatalf("expected healthz to stay open, got %d", health.Code)
	}
}

// TestBasicAuthAcceptsValidCredentials verifies that the configured credentials
// unlock the dashboard, the JSON API, and the static assets.
func TestBasicAuthAcceptsValidCredentials(t *testing.T) {
	s, err := queue.NewSQLiteStore("file:web_auth_ok_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	srv, err := New(s, WithBasicAuth("operator", "hunter2"))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	h := srv.Handler()

	for _, path := range []string{"/", "/api/jobs", "/api/summary", "/static/app.css"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.SetBasicAuth("operator", "hunter2")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("path %s: expected 200 with valid credentials, got %d", path, rec.Code)
		}
	}
}

// TestBasicAuthRejectsWrongCredentials verifies that a wrong password and a
// wrong username both receive a 401, so callers cannot tell them apart.
func TestBasicAuthRejectsWrongCredentials(t *testing.T) {
	s, err := queue.NewSQLiteStore("file:web_auth_wrong_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	srv, err := New(s, WithBasicAuth("operator", "hunter2"))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	h := srv.Handler()

	cases := []struct {
		name, user, pass string
	}{
		{"wrong password", "operator", "wrong"},
		{"wrong username", "admin", "hunter2"},
		{"empty credentials", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
			req.SetBasicAuth(tc.user, tc.pass)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("expected 401, got %d", rec.Code)
			}
		})
	}
}

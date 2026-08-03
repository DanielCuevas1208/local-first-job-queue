package web

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

func newTestServer(t *testing.T, opts ...Option) (*httptest.Server, *queue.Queue) {
	t.Helper()
	s, err := queue.NewSQLiteStore("file:web_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	ts := httptest.NewServer(Handler(s, opts...))
	t.Cleanup(ts.Close)
	return ts, queue.NewQueue(s)
}

// do sends a request to the test server and returns the status and body.
// Redirects are not followed, so tests can assert on a redirect response.
// An optional last argument sets the Content-Type header.
func do(t *testing.T, ts *httptest.Server, method, path, body string, contentType ...string) (int, string) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, ts.URL+path, r)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if len(contentType) > 0 && contentType[0] != "" {
		req.Header.Set("Content-Type", contentType[0])
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(b)
}

// deadLetter runs one job to the dead-letter state through the real queue.
func deadLetter(t *testing.T, q *queue.Queue) *queue.Job {
	t.Helper()
	job, err := q.Enqueue("report", `{"fault":"boom"}`, queue.WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(context.Background(), "report", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease: %v %v", leased, err)
	}
	if err := q.Fail(job.ID, "boom"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	return job
}

func TestDashboardRendersQueueState(t *testing.T) {
	ts, q := newTestServer(t)
	job, err := q.Enqueue("email", `{"to":"a@example.com"}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	status, body := do(t, ts, http.MethodGet, "/", "")
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
	for _, want := range []string{
		"Local-first Durable Job Queue",
		"Dashboard",
		"pending",
		`href="/jobs/` + job.ID + `"`,
		`state-pending`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard missing %q in:\n%s", want, body)
		}
	}
}

func TestDashboardFiltersJobs(t *testing.T) {
	ts, q := newTestServer(t)
	email, err := q.Enqueue("email", `{}`)
	if err != nil {
		t.Fatalf("enqueue email: %v", err)
	}
	if _, err := q.Enqueue("report", `{}`); err != nil {
		t.Fatalf("enqueue report: %v", err)
	}

	status, body := do(t, ts, http.MethodGet, "/?state=pending&kind=email", "")
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
	if !strings.Contains(body, `href="/jobs/`+email.ID+`"`) {
		t.Errorf("filtered dashboard should show the email job:\n%s", body)
	}
	// The recent-events panel may reference other jobs. Assert on the jobs
	// table kind cell, which only renders the filtered kind.
	if strings.Contains(body, "<td>report</td>") {
		t.Errorf("filtered dashboard should hide the report job row:\n%s", body)
	}
	if !strings.Contains(body, "<td>email</td>") {
		t.Errorf("filtered dashboard should show the email job row:\n%s", body)
	}
}

func TestDashboardPaginates(t *testing.T) {
	ts, q := newTestServer(t)
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue("email", `{}`); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	status, body := do(t, ts, http.MethodGet, "/?limit=2", "")
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
	if !strings.Contains(body, "Showing 1&ndash;2 of 5") {
		t.Errorf("expected a pagination summary, got:\n%s", body)
	}
	if !strings.Contains(body, "Older") {
		t.Errorf("expected a next-page link, got:\n%s", body)
	}
}

func TestJobDetailRendersTimeline(t *testing.T) {
	ts, q := newTestServer(t)
	job, err := q.Enqueue("email", `{"to":"a@example.com"}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(context.Background(), "email", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease: %v %v", leased, err)
	}
	if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	status, body := do(t, ts, http.MethodGet, "/jobs/"+job.ID, "")
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
	for _, want := range []string{
		"Events (3)",
		"enqueued",
		"acknowledged",
		"a@example.com",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("job page missing %q in:\n%s", want, body)
		}
	}
}

func TestJobDetailNotFound(t *testing.T) {
	ts, _ := newTestServer(t)
	status, _ := do(t, ts, http.MethodGet, "/jobs/does-not-exist", "")
	if status != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", status)
	}
}

func TestRequeueViaPageForm(t *testing.T) {
	ts, q := newTestServer(t)
	job := deadLetter(t, q)

	status, _ := do(t, ts, http.MethodPost, "/jobs/"+job.ID+"/requeue", "payload="+`{"fixed":true}`, "application/x-www-form-urlencoded")
	if status != http.StatusSeeOther {
		t.Fatalf("expected 303 redirect, got %d", status)
	}

	code, body := do(t, ts, http.MethodGet, "/api/jobs/"+job.ID, "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	var got queue.Job
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode job: %v", err)
	}
	if got.State != queue.StatePending {
		t.Errorf("expected pending after requeue, got %s", got.State)
	}
	if got.RetryCount != 0 {
		t.Errorf("expected attempts reset to 0, got %d", got.RetryCount)
	}
	if got.Payload != `{"fixed":true}` {
		t.Errorf("expected updated payload, got %q", got.Payload)
	}
}

func TestRequeuePageRejectsLiveJob(t *testing.T) {
	ts, q := newTestServer(t)
	job, err := q.Enqueue("email", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	status, _ := do(t, ts, http.MethodPost, "/jobs/"+job.ID+"/requeue", "")
	if status != http.StatusConflict {
		t.Fatalf("expected 409 for a live job, got %d", status)
	}
}

func TestAPIStats(t *testing.T) {
	ts, q := newTestServer(t)
	if _, err := q.Enqueue("email", `{}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	status, body := do(t, ts, http.MethodGet, "/api/stats", "")
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
	var got struct {
		Stats  map[string]int         `json:"stats"`
		ByKind []queue.KindStateCount `json:"by_kind"`
		Events []queue.Event          `json:"events"`
		Oldest *float64               `json:"oldest_pending_seconds"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if got.Stats["pending"] != 1 {
		t.Errorf("expected 1 pending job, got %d", got.Stats["pending"])
	}
	if len(got.ByKind) != 1 || got.ByKind[0].Kind != "email" || got.ByKind[0].Count != 1 {
		t.Errorf("expected one email kind row, got %+v", got.ByKind)
	}
	if len(got.Events) != 1 || got.Events[0].EventType != queue.EventEnqueued {
		t.Errorf("expected one enqueued event, got %+v", got.Events)
	}
}

func TestAPIStatsOldestPendingUsesInjectedClock(t *testing.T) {
	now := time.Now().UTC()
	ts, q := newTestServer(t, WithNow(func() time.Time { return now }))
	if _, err := q.Enqueue("report", `{}`, queue.WithRunAt(now.Add(-30*time.Second))); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	_, body := do(t, ts, http.MethodGet, "/api/stats", "")
	var got struct {
		Oldest float64 `json:"oldest_pending_seconds"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if got.Oldest != 30 {
		t.Errorf("expected oldest pending age 30s, got %v", got.Oldest)
	}
}

func TestAPIJobListFiltersAndPaginates(t *testing.T) {
	ts, q := newTestServer(t)
	var emailID, reportID string
	for i := 0; i < 3; i++ {
		job, err := q.Enqueue("email", `{}`)
		if err != nil {
			t.Fatalf("enqueue email: %v", err)
		}
		emailID = job.ID
	}
	report, err := q.Enqueue("report", `{}`)
	if err != nil {
		t.Fatalf("enqueue report: %v", err)
	}
	reportID = report.ID

	_, body := do(t, ts, http.MethodGet, "/api/jobs?state=pending&kind=email&limit=2&offset=0", "")
	var page struct {
		Jobs  []queue.Job `json:"jobs"`
		Total int         `json:"total"`
	}
	if err := json.Unmarshal([]byte(body), &page); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	if page.Total != 3 {
		t.Errorf("expected total 3 email jobs, got %d", page.Total)
	}
	if len(page.Jobs) != 2 {
		t.Errorf("expected 2 jobs on the page, got %d", len(page.Jobs))
	}
	for _, j := range page.Jobs {
		if j.Kind != "email" || j.State != queue.StatePending {
			t.Errorf("filter leaked a non-matching job: %+v", j)
		}
	}

	_, body = do(t, ts, http.MethodGet, "/api/jobs?kind=report", "")
	if err := json.Unmarshal([]byte(body), &page); err != nil {
		t.Fatalf("decode report page: %v", err)
	}
	if page.Total != 1 || len(page.Jobs) != 1 || page.Jobs[0].ID != reportID {
		t.Errorf("expected the report job, got %+v", page)
	}
	if emailID == "" || reportID == "" {
		t.Fatal("expected captured IDs")
	}
}

func TestAPIJobGetAndNotFound(t *testing.T) {
	ts, q := newTestServer(t)
	job, err := q.Enqueue("email", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	status, body := do(t, ts, http.MethodGet, "/api/jobs/"+job.ID, "")
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
	var got queue.Job
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode job: %v", err)
	}
	if got.ID != job.ID || got.Kind != "email" {
		t.Errorf("expected the enqueued job, got %+v", got)
	}

	status, _ = do(t, ts, http.MethodGet, "/api/jobs/does-not-exist", "")
	if status != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", status)
	}
}

func TestAPIJobEvents(t *testing.T) {
	ts, q := newTestServer(t)
	job, err := q.Enqueue("email", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	status, body := do(t, ts, http.MethodGet, "/api/jobs/"+job.ID+"/events", "")
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
	var got struct {
		Events []queue.Event `json:"events"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode events: %v", err)
	}
	if len(got.Events) != 1 || got.Events[0].EventType != queue.EventEnqueued {
		t.Errorf("expected one enqueued event, got %+v", got.Events)
	}

	status, _ = do(t, ts, http.MethodGet, "/api/jobs/does-not-exist/events", "")
	if status != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", status)
	}
}

func TestAPIRequeueWithPayload(t *testing.T) {
	ts, q := newTestServer(t)
	job := deadLetter(t, q)

	status, body := do(t, ts, http.MethodPost, "/api/jobs/"+job.ID+"/requeue",
		`{"payload":"{\"fixed\":true}","max_attempts":5}`)
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", status, body)
	}
	var got queue.Job
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode requeued job: %v", err)
	}
	if got.State != queue.StatePending {
		t.Errorf("expected pending, got %s", got.State)
	}
	if got.MaxAttempts != 5 {
		t.Errorf("expected attempt budget 5, got %d", got.MaxAttempts)
	}
	if got.Payload != `{"fixed":true}` {
		t.Errorf("expected updated payload, got %q", got.Payload)
	}
}

func TestAPIRequeueRejectsUnknownJob(t *testing.T) {
	ts, _ := newTestServer(t)
	status, _ := do(t, ts, http.MethodPost, "/api/jobs/does-not-exist/requeue", "")
	if status != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", status)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	ts, _ := newTestServer(t)
	status, body := do(t, ts, http.MethodGet, "/metrics", "")
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
	if !strings.Contains(body, "# TYPE jobqueue_jobs gauge") {
		t.Errorf("expected metrics body, got %q", body)
	}
}

func TestStaticAssetServed(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, err := http.Get(ts.URL + "/static/site.css")
	if err != nil {
		t.Fatalf("get css: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/css") {
		t.Errorf("expected css content type, got %q", ct)
	}
}

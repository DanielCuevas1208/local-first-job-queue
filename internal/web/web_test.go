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

func newTestServer(t *testing.T) (*httptest.Server, *queue.SQLiteStore, *queue.Queue) {
	t.Helper()
	s, err := queue.NewSQLiteStore("file:web_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	ts := httptest.NewServer(New(s))
	t.Cleanup(ts.Close)
	return ts, s, queue.NewQueue(s)
}

func get(t *testing.T, url string) (*http.Response, string) {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return res, string(b)
}

// TestDashboardServesPage verifies that the root path returns the dashboard
// HTML and the correct content type.
func TestDashboardServesPage(t *testing.T) {
	ts, _, _ := newTestServer(t)

	res, body := get(t, ts.URL+"/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("expected html content type, got %q", ct)
	}
	for _, want := range []string{
		"Local-first Job Queue",
		"Recent events",
		"/assets/app.js",
		"/assets/style.css",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
}

// TestUnknownPathReturns404 verifies that unmatched paths get a 404 instead of
// the dashboard page.
func TestUnknownPathReturns404(t *testing.T) {
	ts, _, _ := newTestServer(t)
	res, _ := get(t, ts.URL+"/does-not-exist")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", res.StatusCode)
	}
}

// TestAssetsServed verifies that the embedded stylesheet and script are served
// with the correct content types.
func TestAssetsServed(t *testing.T) {
	ts, _, _ := newTestServer(t)

	for _, tc := range []struct {
		path        string
		contentType string
		marker      string
	}{
		{"/assets/style.css", "text/css", "--topbar"},
		{"/assets/app.js", "javascript", "REFRESH_MS"},
	} {
		res, body := get(t, ts.URL+tc.path)
		if res.StatusCode != http.StatusOK {
			t.Errorf("%s: expected 200, got %d", tc.path, res.StatusCode)
		}
		if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, tc.contentType) {
			t.Errorf("%s: expected content type %q, got %q", tc.path, tc.contentType, ct)
		}
		if !strings.Contains(body, tc.marker) {
			t.Errorf("%s: missing marker %q", tc.path, tc.marker)
		}
	}
}

// TestMetricsMounted verifies that the dashboard server also exposes the
// Prometheus endpoint, so one port serves both the dashboard and metrics.
func TestMetricsMounted(t *testing.T) {
	ts, _, _ := newTestServer(t)
	res, body := get(t, ts.URL+"/metrics")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
	if !strings.Contains(body, "# TYPE jobqueue_jobs gauge") {
		t.Errorf("expected metrics body, got %q", body)
	}
}

// TestAPISnapshotReflectsQueue verifies that the snapshot endpoint reports the
// jobs, event counts, and state statistics that exist in the store.
func TestAPISnapshotReflectsQueue(t *testing.T) {
	ts, _, q := newTestServer(t)
	ctx := context.Background()

	if _, err := q.Enqueue("email", `{"to":"a@example.com"}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	flaky, err := q.Enqueue("report", `{}`, queue.WithMaxAttempts(1), queue.WithPriority(1))
	if err != nil {
		t.Fatalf("enqueue flaky: %v", err)
	}
	leased, err := q.Lease(ctx, "report", time.Minute)
	if err != nil || leased == nil || leased.ID != flaky.ID {
		t.Fatalf("lease flaky: %v %v", leased, err)
	}
	if err := q.Fail(leased.ID, "boom"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	res, body := get(t, ts.URL+"/api/snapshot")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected json content type, got %q", ct)
	}

	var snap queue.QueueSnapshot
	if err := json.Unmarshal([]byte(body), &snap); err != nil {
		t.Fatalf("decode snapshot: %v\n%s", err, body)
	}
	if len(snap.Jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(snap.Jobs))
	}
	if snap.Stats[queue.StatePending] != 1 {
		t.Errorf("expected 1 pending, got %d", snap.Stats[queue.StatePending])
	}
	if snap.Stats[queue.StateDeadLetter] != 1 {
		t.Errorf("expected 1 dead_letter, got %d", snap.Stats[queue.StateDeadLetter])
	}
	if len(snap.Events) != 4 {
		t.Errorf("expected 4 events, got %d", len(snap.Events))
	}
}

// TestAPIJobDetail verifies that the job detail endpoint returns the job and
// its full event timeline in order.
func TestAPIJobDetail(t *testing.T) {
	ts, _, q := newTestServer(t)
	ctx := context.Background()

	job, err := q.Enqueue("email", `{"to":"a@example.com"}`, queue.WithMaxAttempts(2))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(ctx, "email", time.Minute)
	if err != nil || leased == nil || leased.ID != job.ID {
		t.Fatalf("lease: %v %v", leased, err)
	}
	if err := q.Fail(leased.ID, "rate limited"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	res, body := get(t, ts.URL+"/api/jobs/"+job.ID)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", res.StatusCode, body)
	}

	var detail struct {
		Job    queue.Job     `json:"job"`
		Events []queue.Event `json:"events"`
	}
	if err := json.Unmarshal([]byte(body), &detail); err != nil {
		t.Fatalf("decode detail: %v\n%s", err, body)
	}
	if detail.Job.ID != job.ID {
		t.Errorf("expected job %s, got %s", job.ID, detail.Job.ID)
	}
	if detail.Job.State != queue.StatePending {
		t.Errorf("expected pending after one retry, got %s", detail.Job.State)
	}
	wantTypes := []queue.EventType{
		queue.EventEnqueued, queue.EventLeased, queue.EventRetried,
	}
	if len(detail.Events) != len(wantTypes) {
		t.Fatalf("expected %d events, got %d", len(wantTypes), len(detail.Events))
	}
	for i, et := range wantTypes {
		if detail.Events[i].EventType != et {
			t.Errorf("event %d: expected %s, got %s", i, et, detail.Events[i].EventType)
		}
	}
}

// TestAPIJobNotFound verifies that an unknown job id returns a 404 with a JSON
// error body.
func TestAPIJobNotFound(t *testing.T) {
	ts, _, _ := newTestServer(t)

	res, body := get(t, ts.URL+"/api/jobs/nope")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", res.StatusCode)
	}
	var errBody map[string]string
	if err := json.Unmarshal([]byte(body), &errBody); err != nil {
		t.Fatalf("decode error body: %v\n%s", err, body)
	}
	if errBody["error"] == "" {
		t.Errorf("expected error message, got %q", body)
	}
}

// TestAPIJobDetailTimelineIsAppendOnly verifies that the job detail endpoint
// returns events in insertion order. A job that crashes, exhausts its budget,
// and is requeued records every transition: enqueued, leased, dead_lettered,
// requeued.
func TestAPIJobDetailTimelineIsAppendOnly(t *testing.T) {
	ts, s, q := newTestServer(t)

	job, err := q.Enqueue("report", `{}`, queue.WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Simulate a worker crash: lease with an expired deadline, then recover.
	// The single attempt budget forces the job into the dead-letter state.
	if _, err := s.LeaseJobByID(job.ID, -time.Hour); err != nil {
		t.Fatalf("orphan lease: %v", err)
	}
	if _, err := q.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if _, err := q.Requeue(job.ID); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	res, body := get(t, ts.URL+"/api/jobs/"+job.ID)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
	var detail struct {
		Job    queue.Job     `json:"job"`
		Events []queue.Event `json:"events"`
	}
	if err := json.Unmarshal([]byte(body), &detail); err != nil {
		t.Fatalf("decode detail: %v\n%s", err, body)
	}
	wantTypes := []queue.EventType{
		queue.EventEnqueued,
		queue.EventDeadLettered,
		queue.EventRequeued,
	}
	if len(detail.Events) != len(wantTypes) {
		t.Fatalf("expected %d events, got %d: %+v", len(wantTypes), len(detail.Events), detail.Events)
	}
	for i, et := range wantTypes {
		if detail.Events[i].EventType != et {
			t.Errorf("event %d: expected %s, got %s", i, et, detail.Events[i].EventType)
		}
	}
	if detail.Job.State != queue.StatePending {
		t.Errorf("expected pending after requeue, got %s", detail.Job.State)
	}
}

// TestAPISnapshotEmptyQueue verifies that the snapshot endpoint returns an
// empty result for a fresh store.
func TestAPISnapshotEmptyQueue(t *testing.T) {
	ts, _, _ := newTestServer(t)
	res, body := get(t, ts.URL+"/api/snapshot")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
	var snap queue.QueueSnapshot
	if err := json.Unmarshal([]byte(body), &snap); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if len(snap.Jobs) != 0 || len(snap.Events) != 0 || len(snap.Stats) != 0 {
		t.Errorf("expected empty snapshot, got %+v", snap)
	}
}

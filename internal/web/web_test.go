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

func newTestServer(t *testing.T) (*Server, *queue.Queue) {
	t.Helper()
	s, err := queue.NewSQLiteStore("file:web_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return New(s, WithDBPath("web_test.db")), queue.NewQueue(s)
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func post(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestDashboardServesPage verifies that the dashboard page renders with the
// configured database path and that the page shell is present.
func TestDashboardServesPage(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := get(t, srv.Handler(), "/")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("expected html content type, got %q", ct)
	}
	for _, want := range []string{
		"Local-first Durable Job Queue",
		"web_test.db",
		`id="jobs"`,
		`id="events"`,
		"/api/snapshot",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
}

// TestDashboardUnknownPathIs404 verifies that the dashboard does not claim
// paths it does not own.
func TestDashboardUnknownPathIs404(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := get(t, srv.Handler(), "/nope")
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

// TestHealthz verifies the liveness endpoint.
func TestHealthz(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := get(t, srv.Handler(), "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if strings.TrimSpace(rec.Body.String()) != "ok" {
		t.Errorf("expected body ok, got %q", rec.Body.String())
	}
}

// TestSnapshotReflectsQueue verifies that the JSON API reports the same jobs,
// events, and state counts that the queue layer sees.
func TestSnapshotReflectsQueue(t *testing.T) {
	srv, q := newTestServer(t)
	ctx := context.Background()

	job, err := q.Enqueue("email", `{"to":"a@example.com"}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(ctx, "email", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease: %v %v", leased, err)
	}
	if err := q.Acknowledge(leased.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	rec := get(t, srv.Handler(), "/api/snapshot")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var snap queue.QueueSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if len(snap.Jobs) != 1 || snap.Jobs[0].ID != job.ID {
		t.Errorf("expected one job with id %s, got %+v", job.ID, snap.Jobs)
	}
	if snap.Stats[queue.StateCompleted] != 1 {
		t.Errorf("expected 1 completed job, got %v", snap.Stats)
	}
	if len(snap.Events) != 3 {
		t.Errorf("expected 3 events (enqueued, leased, acknowledged), got %d", len(snap.Events))
	}
}

// TestJobDetailShowsTimeline verifies that one job's endpoint returns the job
// and its events in insertion order.
func TestJobDetailShowsTimeline(t *testing.T) {
	srv, q := newTestServer(t)
	ctx := context.Background()

	job, err := q.Enqueue("report", `{"n":1}`, queue.WithMaxAttempts(2))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(ctx, "report", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease: %v %v", leased, err)
	}
	if err := q.Fail(leased.ID, "boom"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	rec := get(t, srv.Handler(), "/api/jobs/"+job.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	var d jobDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		t.Fatalf("decode job detail: %v", err)
	}
	if d.Job.ID != job.ID || d.Job.State != queue.StatePending {
		t.Errorf("expected pending job %s, got %+v", job.ID, d.Job)
	}
	if d.Job.RetryCount != 1 {
		t.Errorf("expected one retry, got %d", d.Job.RetryCount)
	}
	if len(d.Events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(d.Events))
	}
	wantTypes := []queue.EventType{queue.EventEnqueued, queue.EventLeased, queue.EventRetried}
	for i, want := range wantTypes {
		if d.Events[i].EventType != want {
			t.Errorf("event %d: expected %s, got %s", i, want, d.Events[i].EventType)
		}
	}
}

// TestJobDetailNotFound verifies that an unknown id is a 404 rather than an
// internal error.
func TestJobDetailNotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := get(t, srv.Handler(), "/api/jobs/does-not-exist")
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

// TestRequeueReturnsDeadLetteredJob verifies the POST endpoint that returns a
// dead-lettered job to the pending state.
func TestRequeueReturnsDeadLetteredJob(t *testing.T) {
	srv, q := newTestServer(t)
	ctx := context.Background()

	job, err := q.Enqueue("email", `{"bad":true}`, queue.WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(ctx, "email", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease: %v %v", leased, err)
	}
	if err := q.Fail(leased.ID, "disk full"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	rec := post(t, srv.Handler(), "/api/jobs/"+job.ID+"/requeue")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	var updated queue.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &updated); err != nil {
		t.Fatalf("decode requeue response: %v", err)
	}
	if updated.State != queue.StatePending {
		t.Errorf("expected pending after requeue, got %s", updated.State)
	}
	if updated.RetryCount != 0 {
		t.Errorf("expected retry count reset, got %d", updated.RetryCount)
	}

	snap, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	found := false
	for _, e := range snap.Events {
		if e.JobID == job.ID && e.EventType == queue.EventRequeued {
			found = true
		}
	}
	if !found {
		t.Error("expected a requeued event in the log")
	}
}

// TestRequeueRejectsLiveJob verifies that the endpoint refuses to requeue a job
// that is not dead-lettered, matching the queue layer's rule.
func TestRequeueRejectsLiveJob(t *testing.T) {
	srv, q := newTestServer(t)
	job, err := q.Enqueue("email", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	rec := post(t, srv.Handler(), "/api/jobs/"+job.ID+"/requeue")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

// TestReadOnlyRoutesRejectPOST verifies that the Go mux returns 405 for writes
// to read-only endpoints.
func TestReadOnlyRoutesRejectPOST(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	for _, path := range []string{"/api/snapshot", "/healthz"} {
		rec := post(t, h, path)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: expected 405, got %d", path, rec.Code)
		}
	}
}

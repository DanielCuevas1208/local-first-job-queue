package web

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

func newTestServer(t *testing.T) (http.Handler, *queue.SQLiteStore, *queue.Queue) {
	t.Helper()
	s, err := queue.NewSQLiteStore("file:web_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	q := queue.NewQueue(s)
	return Handler(s, WithDBPath("queue.db")), s, q
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func mustDecode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode %s: %v; body=%s", rec.Result().Request.URL, err, rec.Body.String())
	}
}

// TestDashboardServesPage verifies that the root route returns the dashboard
// page and shows the configured database path.
func TestDashboardServesPage(t *testing.T) {
	h, _, _ := newTestServer(t)
	rec := get(t, h, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("expected html content type, got %q", ct)
	}
	for _, want := range []string{"Local-first Durable Job Queue", "db: queue.db", "/api/snapshot"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("page missing %q", want)
		}
	}
}

// TestSnapshotReflectsWorkload verifies that /api/snapshot reports the exact
// state of a known workload. Two jobs are enqueued and one completes, so the
// stats, job list, and event counters must match that sequence.
func TestSnapshotReflectsWorkload(t *testing.T) {
	h, _, q := newTestServer(t)
	ctx := context.Background()

	if _, err := q.Enqueue("email", `{"to":"a@example.com"}`); err != nil {
		t.Fatalf("enqueue email: %v", err)
	}
	report, err := q.Enqueue("report", `{"daily":true}`, queue.WithPriority(5))
	if err != nil {
		t.Fatalf("enqueue report: %v", err)
	}
	job, err := q.Lease(ctx, "email", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("lease email: %v %v", job, err)
	}
	if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	rec := get(t, h, "/api/snapshot")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var snap snapshotResponse
	mustDecode(t, rec, &snap)

	if snap.DBPath != "queue.db" {
		t.Errorf("expected db_path queue.db, got %q", snap.DBPath)
	}
	if snap.Stats[queue.StateCompleted] != 1 || snap.Stats[queue.StatePending] != 1 {
		t.Errorf("expected 1 completed and 1 pending, got %+v", snap.Stats)
	}
	if len(snap.Jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(snap.Jobs))
	}
	if snap.Jobs[0].Priority != 5 && snap.Jobs[1].Priority != 5 {
		t.Errorf("expected the report job to carry priority 5, got %+v", snap.Jobs)
	}
	if len(snap.Jobs) != 2 || snap.Jobs[0].ID != report.ID && snap.Jobs[1].ID != report.ID {
		t.Errorf("expected report job %s in snapshot, got %+v", report.ID, snap.Jobs)
	}
}

// TestJobDetailShowsTimeline verifies that /api/jobs/{id} returns one job and
// its events in order. A dead-lettered job must show the full path: enqueued,
// leased, dead_lettered.
func TestJobDetailShowsTimeline(t *testing.T) {
	h, _, q := newTestServer(t)
	ctx := context.Background()

	job, err := q.Enqueue("report", `{}`, queue.WithMaxAttempts(1))
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

	rec := get(t, h, "/api/jobs/"+job.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var d jobDetail
	mustDecode(t, rec, &d)

	if d.Job.ID != job.ID || d.Job.State != queue.StateDeadLetter {
		t.Errorf("expected dead-lettered job %s, got %+v", job.ID, d.Job)
	}
	want := []queue.EventType{queue.EventEnqueued, queue.EventLeased, queue.EventDeadLettered}
	if len(d.Events) != len(want) {
		t.Fatalf("expected %d events, got %d: %+v", len(want), len(d.Events), d.Events)
	}
	for i, et := range want {
		if d.Events[i].EventType != et {
			t.Errorf("event %d: expected %s, got %s", i, et, d.Events[i].EventType)
		}
	}
}

// TestRequeueEndpoint verifies that POST /api/jobs/{id}/requeue returns a
// dead-lettered job to pending and that the store agrees.
func TestRequeueEndpoint(t *testing.T) {
	h, s, q := newTestServer(t)
	ctx := context.Background()

	job, err := q.Enqueue("report", `{"broken":true}`, queue.WithMaxAttempts(1))
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

	body := bytes.NewBufferString(`{"payload":"{\"broken\":false}"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/"+job.ID+"/requeue", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var updated queue.Job
	mustDecode(t, rec, &updated)
	if updated.State != queue.StatePending {
		t.Errorf("expected pending after requeue, got %s", updated.State)
	}
	if updated.Payload != `{"broken":false}` {
		t.Errorf("expected replaced payload, got %q", updated.Payload)
	}
	if updated.RetryCount != 0 {
		t.Errorf("expected retry_count 0, got %d", updated.RetryCount)
	}

	got, err := s.GetJob(job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.State != queue.StatePending {
		t.Errorf("store disagrees: expected pending, got %s", got.State)
	}
}

// TestRequeueRejectsPendingJob verifies that the endpoint rejects a job that is
// not dead-lettered with a client error.
func TestRequeueRejectsPendingJob(t *testing.T) {
	h, _, q := newTestServer(t)
	job, err := q.Enqueue("report", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/jobs/"+job.ID+"/requeue", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestUnknownJobNotFound verifies that the detail endpoint reports 404 for a
// job ID that does not exist.
func TestUnknownJobNotFound(t *testing.T) {
	h, _, _ := newTestServer(t)
	rec := get(t, h, "/api/jobs/does-not-exist")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// TestMetricsEndpointMounted verifies that the dashboard server also serves the
// Prometheus endpoint at /metrics.
func TestMetricsEndpointMounted(t *testing.T) {
	h, _, q := newTestServer(t)
	if _, err := q.Enqueue("email", `{"to":"a@example.com"}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	rec := get(t, h, "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "jobqueue_jobs") {
		t.Errorf("expected metrics body, got %q", rec.Body.String())
	}
}

package web

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
	s, err := queue.NewSQLiteStore("file:web_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, queue.NewQueue(s)
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestDashboardEmptyQueue verifies that a fresh queue renders every state with
// a zero count and the empty-state messages.
func TestDashboardEmptyQueue(t *testing.T) {
	s, _ := newTestStore(t)
	rec := get(t, Handler(s), "/")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"Local-first Job Queue",
		">pending<",
		">leased<",
		">completed<",
		">dead_letter<",
		">failed<",
		"<div class=\"num\">0</div>",
		"No jobs yet.",
		"No events recorded yet.",
		"No jobs in the database.",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard missing %q in:\n%s", want, body)
		}
	}
}

// TestDashboardRendersWorkload verifies that a queue with completed, pending,
// and dead-lettered jobs shows each state count, kind, and recent event.
func TestDashboardRendersWorkload(t *testing.T) {
	s, q := newTestStore(t)
	ctx := context.Background()

	email, err := q.Enqueue("email", `{"to":"a@example.com"}`)
	if err != nil {
		t.Fatalf("enqueue email: %v", err)
	}
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
		t.Fatalf("ack email: %v", err)
	}
	leased, err := q.Lease(ctx, "report", time.Minute)
	if err != nil || leased == nil || leased.ID != flaky.ID {
		t.Fatalf("lease flaky: %v %v", leased, err)
	}
	if err := q.Fail(leased.ID, "boom"); err != nil {
		t.Fatalf("fail flaky: %v", err)
	}

	rec := get(t, Handler(s), "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"email",
		"report",
		"acknowledged",
		"dead_lettered",
		`href="/jobs/` + email.ID + `"`,
		`href="/jobs/` + flaky.ID + `"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard missing %q in:\n%s", want, body)
		}
	}
}

// TestDashboardEscapesPayloads verifies that job payloads are HTML-escaped, so
// a hostile payload cannot inject markup into the page.
func TestDashboardEscapesPayloads(t *testing.T) {
	s, q := newTestStore(t)
	payload := `{"n":1,"x":"<script>alert(1)</script>"}`
	if _, err := q.Enqueue("test", payload); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := get(t, Handler(s), "/")
	body := rec.Body.String()
	if strings.Contains(body, "<script>alert(1)</script>") {
		t.Fatalf("payload was not escaped:\n%s", body)
	}
	if !strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt;") {
		t.Errorf("expected escaped payload, got:\n%s", body)
	}
}

// TestJobPageRendersTimeline verifies that one job page shows the job details
// and its complete event timeline in order.
func TestJobPageRendersTimeline(t *testing.T) {
	s, q := newTestStore(t)
	ctx := context.Background()

	job, err := q.Enqueue("test", `{"n":1}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leased, err := q.Lease(ctx, "test", time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease: %v %v", leased, err)
	}
	if err := q.Acknowledge(leased.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	rec := get(t, Handler(s), "/jobs/"+job.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"Job " + job.ID,
		">completed<",
		"Payload",
		"enqueued",
		"leased",
		"acknowledged",
		"back to dashboard",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("job page missing %q in:\n%s", want, body)
		}
	}
	// The timeline must be oldest first: enqueued before acknowledged.
	if strings.Index(body, ">enqueued<") > strings.Index(body, ">acknowledged<") {
		t.Errorf("expected enqueued event before acknowledged event:\n%s", body)
	}
}

// TestJobPageShowsMetadata verifies that a scheduled job with an idempotency
// key renders both fields on its detail page.
func TestJobPageShowsMetadata(t *testing.T) {
	s, q := newTestStore(t)

	job, err := q.Enqueue("test", `{}`,
		queue.WithIdempotencyKey("dup-123"),
		queue.WithRunAt(time.Now().Add(time.Hour)),
	)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := get(t, Handler(s), "/jobs/"+job.ID)
	body := rec.Body.String()
	for _, want := range []string{
		"Idempotency key",
		"dup-123",
		"Run at",
		">scheduled<",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("job page missing %q in:\n%s", want, body)
		}
	}
}

// TestJobPageUnknownJob404 verifies that a missing job id returns 404.
func TestJobPageUnknownJob404(t *testing.T) {
	s, _ := newTestStore(t)
	rec := get(t, Handler(s), "/jobs/does-not-exist")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// TestDashboardContentType verifies that pages are served as HTML.
func TestDashboardContentType(t *testing.T) {
	s, _ := newTestStore(t)
	rec := get(t, Handler(s), "/")
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("expected text/html content type, got %q", ct)
	}
}

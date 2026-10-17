package web

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

// TestDashboardRendersRetentionActivity verifies that a recorded retention
// pass appears in the server-rendered dashboard before JavaScript runs.
func TestDashboardRendersRetentionActivity(t *testing.T) {
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
		t.Fatalf("prune: %v", err)
	}

	rec := get(t, srv.Handler(), "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	for _, want := range []string{
		`data-retention="runs">1`,
		`data-retention="jobs">0`,
		`data-retention="events">2`,
		"Recent retention runs",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("dashboard missing %q:\n%s", want, rec.Body.String())
		}
	}
}

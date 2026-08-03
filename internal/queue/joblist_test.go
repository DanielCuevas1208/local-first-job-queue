package queue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// pendingState is a stable pointer to the pending state for JobFilter.
var pendingState = StatePending

// TestListJobsFiltersByState verifies that a state filter returns only the
// matching jobs and reports a matching total. Completed jobs must not leak
// into a pending page.
func TestListJobsFiltersByState(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)
	ctx := context.Background()

	pending, err := q.Enqueue("email", `{}`)
	if err != nil {
		t.Fatalf("enqueue pending: %v", err)
	}
	report, err := q.Enqueue("report", `{}`)
	if err != nil {
		t.Fatalf("enqueue report: %v", err)
	}
	job, err := q.Lease(ctx, "report", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("lease report: %v %v", job, err)
	}
	if err := q.Acknowledge(report.ID); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}

	got, total, err := s.ListJobs(JobFilter{State: &pendingState})
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected 1 pending job, got %d", total)
	}
	if len(got) != 1 || got[0].ID != pending.ID {
		t.Fatalf("expected the email job pending, got %+v", got)
	}

	completedState := StateCompleted
	got, total, err = s.ListJobs(JobFilter{State: &completedState})
	if err != nil {
		t.Fatalf("list completed: %v", err)
	}
	if total != 1 || len(got) != 1 || got[0].ID != job.ID {
		t.Fatalf("expected one completed job, got total=%d jobs=%+v", total, got)
	}
}

// TestListJobsFiltersByKind verifies that a kind filter returns only the jobs
// of that kind and ignores the others.
func TestListJobsFiltersByKind(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	email, err := q.Enqueue("email", `{}`)
	if err != nil {
		t.Fatalf("enqueue email: %v", err)
	}
	if _, err := q.Enqueue("report", `{}`); err != nil {
		t.Fatalf("enqueue report: %v", err)
	}

	jobs, total, err := s.ListJobs(JobFilter{Kind: "email"})
	if err != nil {
		t.Fatalf("list email: %v", err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].ID != email.ID {
		t.Fatalf("expected one email job, got total=%d jobs=%+v", total, jobs)
	}
}

// TestListJobsOrdersNewestFirst verifies that the default page is ordered by
// creation time descending. The store is seeded through InsertJob with explicit
// timestamps, so the expected order does not depend on wall-clock resolution.
func TestListJobsOrdersNewestFirst(t *testing.T) {
	s := newTestStore(t)
	base := time.Now().UTC().Add(-time.Hour)

	// Insert from oldest to newest. The ID carries the age, so the order is
	// unambiguous even before reading the timestamps back.
	for i := 0; i < 3; i++ {
		created := base.Add(time.Duration(i) * time.Second)
		j := Job{
			ID:          fmt.Sprintf("job-%d", i),
			Kind:        "email",
			Payload:     `{}`,
			State:       StatePending,
			MaxAttempts: 3,
			CreatedAt:   created,
			UpdatedAt:   created,
		}
		if _, err := s.InsertJob(j); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	jobs, total, err := s.ListJobs(JobFilter{})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if total != 3 || len(jobs) != 3 {
		t.Fatalf("expected 3 jobs, got total=%d len=%d", total, len(jobs))
	}
	for i, want := range []string{"job-2", "job-1", "job-0"} {
		if jobs[i].ID != want {
			t.Errorf("position %d: got %s, want %s", i, jobs[i].ID, want)
		}
	}
}

// TestListJobsPaginates verifies that limit and offset page through a larger
// set without overlap and that the total stays constant across pages.
func TestListJobsPaginates(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue("email", `{}`); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var seen []string
	for offset := 0; offset < 5; offset += 2 {
		jobs, total, err := s.ListJobs(JobFilter{Limit: 2, Offset: offset})
		if err != nil {
			t.Fatalf("list at offset %d: %v", offset, err)
		}
		if total != 5 {
			t.Fatalf("offset %d: expected total 5, got %d", offset, total)
		}
		for _, j := range jobs {
			seen = append(seen, j.ID)
		}
	}
	if len(seen) != 5 {
		t.Fatalf("expected 5 unique jobs across pages, got %d", len(seen))
	}
	unique := map[string]bool{}
	for _, id := range seen {
		if unique[id] {
			t.Fatalf("job %s appeared on more than one page", id)
		}
		unique[id] = true
	}
}

// TestListJobsCapsLimit verifies that a huge limit is clamped to 500 so one
// page cannot read the entire table by accident.
func TestListJobsCapsLimit(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	for i := 0; i < 501; i++ {
		if _, err := q.Enqueue("email", `{}`); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	jobs, total, err := s.ListJobs(JobFilter{Limit: 10000})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if total != 501 {
		t.Fatalf("expected total 501, got %d", total)
	}
	if len(jobs) != 500 {
		t.Fatalf("expected a page of 500 jobs, got %d", len(jobs))
	}
}

// TestListJobsEmptyMatchesAll verifies that a zero-value filter returns every
// job and that an empty queue yields a zero page without an error.
func TestListJobsEmptyMatchesAll(t *testing.T) {
	s := newTestStore(t)
	q := NewQueue(s)

	jobs, total, err := s.ListJobs(JobFilter{})
	if err != nil {
		t.Fatalf("list empty store: %v", err)
	}
	if total != 0 || len(jobs) != 0 {
		t.Fatalf("expected an empty page, got total=%d len=%d", total, len(jobs))
	}

	if _, err := q.Enqueue("email", `{}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	jobs, total, err = s.ListJobs(JobFilter{})
	if err != nil {
		t.Fatalf("list store: %v", err)
	}
	if total != 1 || len(jobs) != 1 {
		t.Fatalf("expected one job, got total=%d len=%d", total, len(jobs))
	}
}

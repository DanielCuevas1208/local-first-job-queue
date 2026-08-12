package queue

import (
	"context"
	"testing"
	"time"
)

func TestGetJobHistoriesOrdersJobsAndEvents(t *testing.T) {
	store, err := NewSQLiteStore("file:history_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()
	q := NewQueue(store)

	first, err := q.Enqueue("email", `{"name":"first"}`)
	if err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if _, err := q.Lease(context.Background(), "email", time.Minute); err != nil {
		t.Fatalf("lease first: %v", err)
	}
	if err := q.Acknowledge(first.ID); err != nil {
		t.Fatalf("acknowledge first: %v", err)
	}
	second, err := q.Enqueue("email", `{"name":"second"}`)
	if err != nil {
		t.Fatalf("enqueue second: %v", err)
	}

	histories, err := store.GetJobHistories(HistoryFilter{})
	if err != nil {
		t.Fatalf("get histories: %v", err)
	}
	if len(histories) != 2 {
		t.Fatalf("got %d histories, want 2", len(histories))
	}
	if histories[0].Job.ID != first.ID || histories[1].Job.ID != second.ID {
		t.Fatalf("history order = %s, %s; want %s, %s", histories[0].Job.ID, histories[1].Job.ID, first.ID, second.ID)
	}

	wantEvents := []EventType{EventEnqueued, EventLeased, EventAcknowledged}
	if len(histories[0].Events) != len(wantEvents) {
		t.Fatalf("first history has %d events, want %d", len(histories[0].Events), len(wantEvents))
	}
	for i, want := range wantEvents {
		if got := histories[0].Events[i].EventType; got != want {
			t.Errorf("event %d = %s, want %s", i, got, want)
		}
	}
	if len(histories[1].Events) != 1 || histories[1].Events[0].EventType != EventEnqueued {
		t.Fatalf("second history events = %+v, want one enqueued event", histories[1].Events)
	}
}

func TestGetJobHistoriesFiltersKindAndState(t *testing.T) {
	store, err := NewSQLiteStore("file:history_filter_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()
	q := NewQueue(store)

	completed, err := q.Enqueue("email", `{}`)
	if err != nil {
		t.Fatalf("enqueue completed: %v", err)
	}
	if _, err := q.Lease(context.Background(), "email", time.Minute); err != nil {
		t.Fatalf("lease completed: %v", err)
	}
	if err := q.Acknowledge(completed.ID); err != nil {
		t.Fatalf("acknowledge completed: %v", err)
	}
	if _, err := q.Enqueue("report", `{}`); err != nil {
		t.Fatalf("enqueue report: %v", err)
	}

	histories, err := store.GetJobHistories(HistoryFilter{Kind: "email", State: StateCompleted})
	if err != nil {
		t.Fatalf("get filtered histories: %v", err)
	}
	if len(histories) != 1 || histories[0].Job.ID != completed.ID {
		t.Fatalf("filtered histories = %+v, want completed email job", histories)
	}
}

package cli

import (
	"reflect"
	"strings"
	"testing"

	"github.com/local-first-job-queue/internal/queue"
)

func TestMoveFirstPositionalToEnd(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"id first", []string{"abc123", "-db", "queue.db"}, []string{"-db", "queue.db", "abc123"}},
		{"db before id", []string{"-db", "queue.db", "abc123"}, []string{"-db", "queue.db", "abc123"}},
		{"json and id first", []string{"abc123", "-json", "-db", "queue.db"}, []string{"-json", "-db", "queue.db", "abc123"}},
		{"json before id", []string{"-json", "abc123", "-db", "queue.db"}, []string{"-json", "-db", "queue.db", "abc123"}},
		{"value flag form", []string{"abc123", "-db=queue.db"}, []string{"-db=queue.db", "abc123"}},
		{"no positional", []string{"-db", "queue.db"}, []string{"-db", "queue.db"}},
		{"empty", nil, nil},
		{"double dash", []string{"--", "abc123", "-db", "queue.db"}, []string{"--", "abc123", "-db", "queue.db"}},
		{"id between flags", []string{"-db", "x.db", "abc123", "-json"}, []string{"-db", "x.db", "-json", "abc123"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := moveFirstPositionalToEnd(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("moveFirstPositionalToEnd(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestRenderMetricsSnapshot verifies that the metrics -once path renders the
// queue state as Prometheus text. The collector output is deterministic, so a
// fresh store with one pending job yields a stable snapshot.
func TestRenderMetricsSnapshot(t *testing.T) {
	s, err := queue.NewSQLiteStore("file:cli_metrics_" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	q := queue.NewQueue(s)
	if _, err := q.Enqueue("email", `{"to":"a@example.com"}`); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var b strings.Builder
	if err := renderMetrics(&b, s); err != nil {
		t.Fatalf("render metrics: %v", err)
	}
	for _, want := range []string{
		`jobqueue_jobs{state="pending"} 1`,
		`jobqueue_jobs_by_kind{kind="email",state="pending"} 1`,
		`jobqueue_events_total{type="enqueued"} 1`,
		"# TYPE jobqueue_oldest_pending_seconds gauge",
	} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("metrics output missing %q in:\n%s", want, b.String())
		}
	}
}

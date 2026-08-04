// Package metrics renders queue state in the Prometheus text exposition
// format. An operator can scrape a live endpoint, or print a one-shot snapshot
// with the metrics command. The package reads the shared SQLite store, so it
// observes every job and event without an extra collection pipeline.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

// The canonical state order keeps the exposition stable across scrapes. The
// failed state appears for legacy databases that predate the dead-letter
// queue, so exporters still report it.
var stateOrder = []queue.JobState{
	queue.StatePending,
	queue.StateLeased,
	queue.StateCompleted,
	queue.StateDeadLetter,
	queue.StateFailed,
}

var eventTypeOrder = []queue.EventType{
	queue.EventEnqueued,
	queue.EventScheduled,
	queue.EventLeased,
	queue.EventAcknowledged,
	queue.EventFailed,
	queue.EventRetried,
	queue.EventRecovered,
	queue.EventDeadLettered,
	queue.EventRequeued,
}

// The canonical retention source order keeps the exposition stable across
// scrapes. Manual runs come from prune commands; automatic runs come from
// workers that apply a policy on a schedule.
var retentionSourceOrder = []queue.RetentionSource{
	queue.RetentionSourceManual,
	queue.RetentionSourceAuto,
}

// Option configures a Collector.
type Option func(*Collector)

// WithNow overrides the clock used for age calculations. Tests use it to make
// the oldest-pending metric deterministic.
func WithNow(fn func() time.Time) Option {
	return func(c *Collector) {
		c.now = fn
	}
}

// Collector reads queue state from the store and renders it in the Prometheus
// text format. Each call to Write computes a fresh snapshot, so no counters are
// kept between scrapes.
type Collector struct {
	store *queue.SQLiteStore
	now   func() time.Time
}

// New returns a Collector that reads from store.
func New(store *queue.SQLiteStore, opts ...Option) *Collector {
	c := &Collector{store: store, now: time.Now}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Write renders the current queue state in the Prometheus text exposition
// format. The output is deterministic: families appear in a fixed order and
// label values are sorted.
func (c *Collector) Write(w io.Writer) error {
	stats, err := c.store.GetQueueStats()
	if err != nil {
		return fmt.Errorf("queue stats: %w", err)
	}
	byKind, err := c.store.GetStateKindCounts()
	if err != nil {
		return fmt.Errorf("kind counts: %w", err)
	}
	evCounts, err := c.store.GetEventTypeCounts()
	if err != nil {
		return fmt.Errorf("event counts: %w", err)
	}

	if err := c.writeStateGauge(w, stats); err != nil {
		return err
	}
	if err := c.writeKindGauge(w, byKind); err != nil {
		return err
	}
	if err := c.writeEventCounters(w, evCounts); err != nil {
		return err
	}
	if err := c.writeOldestPending(w); err != nil {
		return err
	}
	return c.writeRetention(w)
}

func (c *Collector) writeStateGauge(w io.Writer, stats map[queue.JobState]int) error {
	if _, err := fmt.Fprintln(w, "# HELP jobqueue_jobs Number of jobs in each state."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE jobqueue_jobs gauge"); err != nil {
		return err
	}
	for _, state := range stateOrder {
		if _, err := fmt.Fprintf(w, "jobqueue_jobs{state=%q} %d\n", state, stats[state]); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) writeKindGauge(w io.Writer, byKind []queue.KindStateCount) error {
	if _, err := fmt.Fprintln(w, "# HELP jobqueue_jobs_by_kind Number of jobs per kind and state."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE jobqueue_jobs_by_kind gauge"); err != nil {
		return err
	}
	for _, k := range byKind {
		if _, err := fmt.Fprintf(w, "jobqueue_jobs_by_kind{kind=%q,state=%q} %d\n", k.Kind, k.State, k.Count); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) writeEventCounters(w io.Writer, evCounts []queue.EventTypeCount) error {
	byType := map[queue.EventType]int{}
	for _, c := range evCounts {
		byType[c.EventType] = c.Count
	}
	if _, err := fmt.Fprintln(w, "# HELP jobqueue_events_total Number of events per event type."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE jobqueue_events_total counter"); err != nil {
		return err
	}
	for _, et := range eventTypeOrder {
		if _, err := fmt.Fprintf(w, "jobqueue_events_total{type=%q} %d\n", et, byType[et]); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) writeOldestPending(w io.Writer) error {
	ready, ok, err := c.store.GetOldestPendingReadyTime()
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# HELP jobqueue_oldest_pending_seconds Age in seconds of the oldest pending job."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE jobqueue_oldest_pending_seconds gauge"); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	age := c.now().Sub(ready).Seconds()
	_, err = fmt.Fprintf(w, "jobqueue_oldest_pending_seconds %g\n", age)
	return err
}

// writeRetention renders the retention activity families. The run, job, and
// event counters report cumulative activity per source, so an operator can see
// whether retention is running automatically or only on demand. The last-run
// gauge reports how recently a retention pass happened.
func (c *Collector) writeRetention(w io.Writer) error {
	stats, err := c.store.GetRetentionStats()
	if err != nil {
		return fmt.Errorf("retention stats: %w", err)
	}
	bySource := map[queue.RetentionSource]queue.RetentionSourceCount{}
	for _, s := range stats {
		bySource[s.Source] = s
	}

	for _, fam := range []struct {
		name, help string
		value      func(queue.RetentionSourceCount) int
	}{
		{"jobqueue_retention_runs_total", "Number of retention runs per source.", func(c queue.RetentionSourceCount) int { return c.Runs }},
		{"jobqueue_retention_jobs_removed_total", "Jobs removed by retention per source.", func(c queue.RetentionSourceCount) int { return c.JobsRemoved }},
		{"jobqueue_retention_events_removed_total", "Events removed by retention per source.", func(c queue.RetentionSourceCount) int { return c.EventsRemoved }},
	} {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n", fam.name, fam.help); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "# TYPE %s counter\n", fam.name); err != nil {
			return err
		}
		for _, src := range retentionSourceOrder {
			if _, err := fmt.Fprintf(w, "%s{source=%q} %d\n", fam.name, src, fam.value(bySource[src])); err != nil {
				return err
			}
		}
	}

	if _, err := fmt.Fprintln(w, "# HELP jobqueue_retention_last_run_seconds Seconds since the most recent retention run."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE jobqueue_retention_last_run_seconds gauge"); err != nil {
		return err
	}
	last, err := c.store.GetLastRetentionRun()
	if err != nil {
		return fmt.Errorf("last retention run: %w", err)
	}
	if last == nil {
		return nil
	}
	age := c.now().Sub(last.StartedAt).Seconds()
	if age < 0 {
		age = 0
	}
	_, err = fmt.Fprintf(w, "jobqueue_retention_last_run_seconds %g\n", age)
	return err
}

// Handler returns an HTTP handler that serves the queue metrics at /metrics.
// A request to any other path explains how to scrape. Each scrape calls Write,
// so the numbers always reflect the current database state.
func Handler(store *queue.SQLiteStore) http.Handler {
	col := New(store)
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := col.Write(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintf(w, "Local-first Durable Job Queue metrics.\nScrape GET /metrics.\n")
	})
	return mux
}

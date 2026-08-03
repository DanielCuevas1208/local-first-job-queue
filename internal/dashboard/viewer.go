// Package dashboard serves a read-only web interface for the queue. The
// interface renders queue state as one HTML page and exposes the same state as
// JSON endpoints for live refresh. An operator can watch leases, retries, and
// the append-only event log without opening the SQLite file.
package dashboard

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/local-first-job-queue/internal/metrics"
	"github.com/local-first-job-queue/internal/queue"
)

// MaxJobs limits the jobs rendered by one page refresh. The endpoint still
// reads the full store; the cap only bounds the payload.
const MaxJobs = 500

// MaxEvents limits the events rendered by one page refresh. The event log is
// append-only, so a long-lived queue can grow far beyond this window.
const MaxEvents = 200

// DefaultRefreshInterval is the client-side auto-refresh interval.
const DefaultRefreshInterval = 2 * time.Second

// Option configures a Viewer.
type Option func(*Viewer)

// WithNow overrides the clock used for age calculations. Tests use it to make
// the dashboard output deterministic.
func WithNow(fn func() time.Time) Option {
	return func(v *Viewer) {
		v.now = fn
	}
}

// WithRefreshInterval sets the client-side auto-refresh interval. The default
// is two seconds.
func WithRefreshInterval(d time.Duration) Option {
	return func(v *Viewer) {
		v.refresh = d
	}
}

// WithDBPath labels the page with the database file it inspects. The path is
// for display only; the store opens the database itself.
func WithDBPath(path string) Option {
	return func(v *Viewer) {
		v.dbPath = path
	}
}

// Viewer reads queue state from a store and renders it for the web. Each
// request computes a fresh snapshot, so no counters are kept between requests.
type Viewer struct {
	store   *queue.SQLiteStore
	now     func() time.Time
	refresh time.Duration
	dbPath  string
}

// NewViewer returns a Viewer that reads from store.
func NewViewer(store *queue.SQLiteStore, opts ...Option) *Viewer {
	v := &Viewer{
		store:   store,
		now:     time.Now,
		refresh: DefaultRefreshInterval,
	}
	for _, o := range opts {
		o(v)
	}
	return v
}

// Overview is the aggregate state rendered by the dashboard. One method call
// computes it from the store, so the page always reflects the current state.
type Overview struct {
	Stats         map[queue.JobState]int `json:"stats"`
	ByKind        []queue.KindStateCount `json:"by_kind"`
	EventCounts   []queue.EventTypeCount `json:"event_counts"`
	Jobs          []queue.Job            `json:"jobs"`
	Events        []queue.Event          `json:"events"`
	OldestPending *float64               `json:"oldest_pending_seconds,omitempty"`
	TotalJobs     int                    `json:"total_jobs"`
	TotalEvents   int                    `json:"total_events"`
	GeneratedAt   time.Time              `json:"generated_at"`
}

// Overview computes a fresh snapshot of the queue. The result is deterministic
// for a fixed store and clock: states and event types keep a stable order, and
// jobs sort by creation time with the newest first.
func (v *Viewer) Overview() (Overview, error) {
	stats, err := v.store.GetQueueStats()
	if err != nil {
		return Overview{}, fmt.Errorf("queue stats: %w", err)
	}
	byKind, err := v.store.GetStateKindCounts()
	if err != nil {
		return Overview{}, fmt.Errorf("kind counts: %w", err)
	}
	eventCounts, err := v.store.GetEventTypeCounts()
	if err != nil {
		return Overview{}, fmt.Errorf("event counts: %w", err)
	}
	jobs, err := v.store.GetAllJobs()
	if err != nil {
		return Overview{}, fmt.Errorf("jobs: %w", err)
	}
	if len(jobs) > MaxJobs {
		jobs = jobs[:MaxJobs]
	}
	events, err := v.store.GetAllEvents()
	if err != nil {
		return Overview{}, fmt.Errorf("events: %w", err)
	}
	if len(events) > MaxEvents {
		events = events[:MaxEvents]
	}

	ov := Overview{
		Stats:       stats,
		ByKind:      byKind,
		EventCounts: eventCounts,
		Jobs:        jobs,
		Events:      events,
		GeneratedAt: v.now().UTC(),
	}
	for _, s := range stats {
		ov.TotalJobs += s
	}
	for _, e := range eventCounts {
		ov.TotalEvents += e.Count
	}
	if ready, ok, err := v.store.GetOldestPendingReadyTime(); err != nil {
		return Overview{}, fmt.Errorf("oldest pending: %w", err)
	} else if ok {
		age := v.now().Sub(ready).Seconds()
		ov.OldestPending = &age
	}
	return ov, nil
}

// Handler returns an HTTP handler that serves the dashboard. The routes are:
//
//	GET /                 the HTML dashboard
//	GET /api/overview     the queue snapshot as JSON
//	GET /api/jobs         the full job list as JSON
//	GET /api/jobs/{id}    one job and its event timeline as JSON
//	GET /metrics          the Prometheus exposition format
//
// The dashboard is read-only. No route modifies the queue.
func Handler(store *queue.SQLiteStore, opts ...Option) http.Handler {
	v := NewViewer(store, opts...)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", v.page)
	mux.HandleFunc("GET /api/overview", v.apiOverview)
	mux.HandleFunc("/api/overview", methodNotAllowed)
	mux.HandleFunc("GET /api/jobs", v.apiJobs)
	mux.HandleFunc("/api/jobs", methodNotAllowed)
	mux.HandleFunc("GET /api/jobs/{id}", v.apiJob)
	mux.HandleFunc("/api/jobs/{id}", methodNotAllowed)
	mux.Handle("GET /metrics", metrics.Handler(store))
	mux.HandleFunc("/metrics", methodNotAllowed)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	return mux
}

// methodNotAllowed answers every request that targets a known route with a
// method the route does not accept. GET-only routes stay read-only.
func methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method %s not allowed", r.Method))
}

// page renders the dashboard HTML. The page is server-rendered from a fresh
// snapshot so it shows data even before the client-side script runs.
func (v *Viewer) page(w http.ResponseWriter, r *http.Request) {
	ov, err := v.Overview()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardTemplate.Execute(w, pageData{
		Overview: ov,
		DBPath:   v.dbPath,
		Refresh:  v.refresh,
	}); err != nil {
		log.Printf("dashboard render: %v", err)
	}
}

type pageData struct {
	Overview Overview
	DBPath   string
	Refresh  time.Duration
}

func (v *Viewer) apiOverview(w http.ResponseWriter, r *http.Request) {
	ov, err := v.Overview()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, ov)
}

func (v *Viewer) apiJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := v.store.GetAllJobs()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "total": len(jobs)})
}

// apiJob serves one job and its event timeline. A job that does not exist
// produces a JSON 404, so scripts can tell a missing ID from a broken store.
func (v *Viewer) apiJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := v.store.GetJob(id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, fmt.Errorf("job %s not found", id))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	events, err := v.store.GetJobEvents(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job, "events": events})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

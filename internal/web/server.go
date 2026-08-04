// Package web serves a read-only inspection dashboard for the queue over HTTP.
// It reads the same SQLite store as the CLI commands, so a browser shows live
// queue state without stopping a worker. The package also exposes a small JSON
// API under /api for scripts and other tools.
package web

import (
	"crypto/subtle"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

// defaultPageSize caps how many jobs one dashboard view renders. The cap keeps
// the page responsive for large queues; filters narrow the result first.
const defaultPageSize = 200

// maxPageSize is the largest page a client may request through the API.
const maxPageSize = 1000

// orderedStates fixes the display order of state summaries. The failed state
// appears for legacy databases that predate the dead-letter queue.
var orderedStates = []queue.JobState{
	queue.StatePending,
	queue.StateLeased,
	queue.StateCompleted,
	queue.StateDeadLetter,
	queue.StateFailed,
}

// Server renders the dashboard and JSON API against one store.
type Server struct {
	store *queue.SQLiteStore
	tmpls *template.Template
	auth  *basicAuth
}

// Option configures a Server.
type Option func(*Server)

// WithBasicAuth guards every dashboard page and JSON endpoint with HTTP Basic
// authentication. The health endpoint stays open so load balancers can probe
// readiness without credentials. Use it when the dashboard is reachable from a
// network that is not fully trusted.
func WithBasicAuth(username, password string) Option {
	return func(s *Server) {
		s.auth = &basicAuth{username: username, password: password}
	}
}

// basicAuth holds the credentials that a protected dashboard accepts. The
// comparison uses constant-time routines so a timing probe cannot recover the
// stored password.
type basicAuth struct {
	username string
	password string
}

// check reports whether the supplied credentials match the configured ones.
func (a *basicAuth) check(user, pass string) bool {
	if a == nil {
		return true
	}
	userOK := subtle.ConstantTimeCompare([]byte(user), []byte(a.username))
	passOK := subtle.ConstantTimeCompare([]byte(pass), []byte(a.password))
	return userOK&passOK == 1
}

// New returns a Server that reads from store. Templates and static assets are
// embedded in the binary, so the command runs from any directory. Options
// configure behaviour such as access control.
func New(store *queue.SQLiteStore, opts ...Option) (*Server, error) {
	tmpls, err := template.New("").Funcs(template.FuncMap{"fmtTime": fmtTime}).
		ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse templates: %w", err)
	}
	s := &Server{store: store, tmpls: tmpls}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// fmtTime renders a timestamp in the same UTC form the dashboard table uses.
// It accepts both time.Time and *time.Time so templates can pass optional
// fields directly. A nil or zero value renders as an empty cell.
func fmtTime(v any) string {
	switch t := v.(type) {
	case time.Time:
		return formatTime(t)
	case *time.Time:
		if t == nil {
			return ""
		}
		return formatTime(*t)
	}
	return ""
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05Z")
}

// Handler returns the HTTP handler with all dashboard and API routes. When the
// server has credentials configured, every route except /healthz requires valid
// Basic authentication. The health probe stays open so load balancers can check
// readiness without a credential.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.handleIndex)
	mux.HandleFunc("GET /job/{id}", s.handleJob)
	mux.HandleFunc("GET /api/summary", s.handleAPISummary)
	mux.HandleFunc("GET /api/jobs", s.handleAPIJobs)
	mux.HandleFunc("GET /api/jobs/{id}", s.handleAPIJob)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.Handle("GET /static/", http.FileServer(http.FS(staticFS)))

	if s.auth == nil {
		return mux
	}
	return s.requireAuth(mux)
}

// requireAuth wraps a handler so every request must present the configured
// credentials. A missing or wrong Authorization header yields a 401 with the
// WWW-Authenticate header that makes browsers show their credential prompt.
func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		user, pass, ok := r.BasicAuth()
		if !ok || !s.auth.check(user, pass) {
			w.Header().Set("WWW-Authenticate", `Basic realm="local-first-job-queue"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// stateCount reports how many jobs share one state.
type stateCount struct {
	State queue.JobState `json:"state"`
	Count int            `json:"count"`
}

// summary is the JSON and template shape for the queue overview. States use a
// fixed order so the dashboard renders a stable card row. Retention reports the
// cumulative retention activity so an operator sees what pruning has done.
type summary struct {
	States      []stateCount     `json:"states"`
	Kinds       []string         `json:"kinds"`
	EventsTotal int              `json:"events_total"`
	Retention   retentionSummary `json:"retention"`
}

// retentionSummary carries the aggregate retention activity and a short history
// of recent runs for the dashboard.
type retentionSummary struct {
	Runs          int                  `json:"runs"`
	JobsRemoved   int                  `json:"jobs_removed"`
	EventsRemoved int                  `json:"events_removed"`
	LastRunAt     *time.Time           `json:"last_run_at,omitempty"`
	RecentRuns    []queue.RetentionRun `json:"recent_runs"`
}

// recentRetentionLimit bounds how many retention runs the dashboard lists.
const recentRetentionLimit = 5

// buildSummary computes the current overview from the store.
func (s *Server) buildSummary() (summary, error) {
	stats, err := s.store.GetQueueStats()
	if err != nil {
		return summary{}, fmt.Errorf("queue stats: %w", err)
	}
	kinds, err := s.store.GetKinds()
	if err != nil {
		return summary{}, fmt.Errorf("kinds: %w", err)
	}
	eventCounts, err := s.store.GetEventTypeCounts()
	if err != nil {
		return summary{}, fmt.Errorf("event counts: %w", err)
	}
	retStats, err := s.store.GetRetentionStats()
	if err != nil {
		return summary{}, fmt.Errorf("retention stats: %w", err)
	}
	recent, err := s.store.RecentRetentionRuns(recentRetentionLimit)
	if err != nil {
		return summary{}, fmt.Errorf("recent retention runs: %w", err)
	}

	states := make([]stateCount, 0, len(orderedStates))
	for _, st := range orderedStates {
		states = append(states, stateCount{State: st, Count: stats[st]})
	}
	total := 0
	for _, ec := range eventCounts {
		total += ec.Count
	}
	ret := retentionSummary{RecentRuns: recent}
	for _, rc := range retStats {
		ret.Runs += rc.Runs
		ret.JobsRemoved += rc.JobsRemoved
		ret.EventsRemoved += rc.EventsRemoved
	}
	if len(recent) > 0 {
		at := recent[0].StartedAt
		ret.LastRunAt = &at
	}
	return summary{States: states, Kinds: kinds, EventsTotal: total, Retention: ret}, nil
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	sum, err := s.buildSummary()
	if err != nil {
		s.internalError(w, err)
		return
	}
	data := struct {
		Title   string
		Summary summary
	}{Title: "Queue dashboard", Summary: sum}
	if err := s.tmpls.ExecuteTemplate(w, "index.html", data); err != nil {
		log.Printf("render index: %v", err)
	}
}

func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(id)
	if err == sql.ErrNoRows {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s.internalError(w, err)
		return
	}
	events, err := s.store.GetJobEvents(id)
	if err != nil {
		s.internalError(w, err)
		return
	}
	if err := s.tmpls.ExecuteTemplate(w, "job.html", jobDetail{Title: "Job " + job.ID, Job: job, Events: events}); err != nil {
		log.Printf("render job: %v", err)
	}
}

// jobDetail carries one job and its event timeline to the detail template.
type jobDetail struct {
	Title  string
	Job    queue.Job
	Events []queue.Event
}

func (s *Server) handleAPISummary(w http.ResponseWriter, r *http.Request) {
	sum, err := s.buildSummary()
	if err != nil {
		s.internalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (s *Server) handleAPIJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := queue.JobFilter{
		State: queue.JobState(q.Get("state")),
		Kind:  q.Get("kind"),
		Query: q.Get("q"),
	}
	limit := defaultPageSize
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= maxPageSize {
			limit = n
		}
	}
	offset := 0
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			offset = n
		}
	}
	filter.Limit = limit
	filter.Offset = offset

	jobs, err := s.store.SearchJobs(filter)
	if err != nil {
		s.internalError(w, err)
		return
	}
	total, err := s.store.CountJobs(filter)
	if err != nil {
		s.internalError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"jobs":   jobs,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) handleAPIJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(id)
	if err == sql.ErrNoRows {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	if err != nil {
		s.internalError(w, err)
		return
	}
	events, err := s.store.GetJobEvents(id)
	if err != nil {
		s.internalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job, "events": events})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintln(w, "ok")
}

func (s *Server) internalError(w http.ResponseWriter, err error) {
	log.Printf("web error: %v", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

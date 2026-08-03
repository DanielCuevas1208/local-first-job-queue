// Package web serves a read-only browser interface for the job queue. Each
// page renders a fresh snapshot from the SQLite store, so the interface shows
// the same state as the inspect command without a separate pipeline.
//
// The interface has three pages and one JSON API:
//
//   - the dashboard shows state counts, per-kind breakdowns, recent events,
//     and recent jobs;
//   - the jobs page lists jobs and filters them by kind and state;
//   - the job page shows one job and its complete event timeline.
//
// The pages never mutate the queue. Operators keep using the requeue command
// to change job state, so the browser stays a safe inspection surface.
package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

//go:embed ui/templates ui/static
var uiFS embed.FS

const (
	// overviewEvents is the number of recent events the dashboard and its
	// overview API show at once.
	overviewEvents = 25
	// overviewJobs is the number of recent jobs the dashboard shows at once.
	overviewJobs = 15
	// maxListLimit caps the jobs page and jobs API so a single request cannot
	// read an unbounded result set from a large queue.
	maxListLimit = 500
)

// StateCount pairs one job state with the number of jobs in that state. The
// order is canonical so the dashboard and the API stay stable across reloads.
type StateCount struct {
	State queue.JobState `json:"state"`
	Count int            `json:"count"`
}

// Overview is the shared view for the dashboard and the overview API.
type Overview struct {
	Total  int                    `json:"total"`
	Stats  []StateCount           `json:"stats"`
	Kinds  []queue.KindStateCount `json:"kinds"`
	Events []queue.Event          `json:"events"`
	Jobs   []queue.Job            `json:"jobs"`
}

// Server renders HTML pages and JSON responses from one store.
type Server struct {
	store *queue.SQLiteStore
	pages map[string]*template.Template
}

// New returns a Server that reads from store. Templates are parsed once and
// reused by every request.
func New(store *queue.SQLiteStore) (*Server, error) {
	pages, err := parsePages()
	if err != nil {
		return nil, err
	}
	return &Server{store: store, pages: pages}, nil
}

// Handler returns the HTTP handler for the web interface.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.handleDashboard)
	mux.HandleFunc("GET /jobs", s.handleJobs)
	mux.HandleFunc("GET /jobs/{id}", s.handleJob)
	mux.HandleFunc("GET /api/overview", s.handleAPIOverview)
	mux.HandleFunc("GET /api/jobs", s.handleAPIJobs)
	mux.HandleFunc("GET /api/jobs/{id}", s.handleAPIJob)

	staticFS, err := fs.Sub(uiFS, "ui/static")
	if err != nil {
		// The embed directive guarantees the directory exists.
		panic(fmt.Sprintf("web: missing static files: %v", err))
	}
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))
	return mux
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	ov, err := s.overview()
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	s.render(w, r, "dashboard", ov)
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	filter, err := jobFilterFromQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	jobs, err := s.store.ListJobs(filter)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	kinds, err := s.store.ListKinds()
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	data := struct {
		Jobs   []queue.Job
		Kinds  []string
		States []queue.JobState
		State  queue.JobState
		Kind   string
	}{
		Jobs:   jobs,
		Kinds:  kinds,
		States: queue.StateOrder,
		State:  filter.State,
		Kind:   filter.Kind,
	}
	s.render(w, r, "jobs", data)
}

func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(id)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			http.NotFound(w, r)
			return
		}
		s.internalError(w, r, err)
		return
	}
	events, err := s.store.GetJobEvents(id)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	data := struct {
		Job    queue.Job
		Events []queue.Event
	}{Job: job, Events: events}
	s.render(w, r, "job", data)
}

func (s *Server) handleAPIOverview(w http.ResponseWriter, r *http.Request) {
	ov, err := s.overview()
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	s.writeJSON(w, r, ov)
}

func (s *Server) handleAPIJobs(w http.ResponseWriter, r *http.Request) {
	filter, err := jobFilterFromQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	jobs, err := s.store.ListJobs(filter)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	s.writeJSON(w, r, jobs)
}

func (s *Server) handleAPIJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(id)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			http.NotFound(w, r)
			return
		}
		s.internalError(w, r, err)
		return
	}
	events, err := s.store.GetJobEvents(id)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	s.writeJSON(w, r, map[string]any{"job": job, "events": events})
}

// overview assembles one dashboard snapshot. The state order is canonical, the
// kind counts come sorted from the store, and events and jobs are bounded.
func (s *Server) overview() (*Overview, error) {
	stats, err := s.store.GetQueueStats()
	if err != nil {
		return nil, fmt.Errorf("queue stats: %w", err)
	}
	kinds, err := s.store.GetStateKindCounts()
	if err != nil {
		return nil, fmt.Errorf("kind counts: %w", err)
	}
	events, err := s.store.GetAllEvents()
	if err != nil {
		return nil, fmt.Errorf("events: %w", err)
	}
	jobs, err := s.store.ListJobs(queue.JobFilter{Limit: overviewJobs})
	if err != nil {
		return nil, fmt.Errorf("jobs: %w", err)
	}

	ordered := make([]StateCount, 0, len(queue.StateOrder))
	total := 0
	for _, state := range queue.StateOrder {
		count := stats[state]
		ordered = append(ordered, StateCount{State: state, Count: count})
		total += count
	}

	if len(events) > overviewEvents {
		events = events[:overviewEvents]
	}
	return &Overview{
		Total:  total,
		Stats:  ordered,
		Kinds:  kinds,
		Events: events,
		Jobs:   jobs,
	}, nil
}

// jobFilterFromQuery reads the state, kind, and limit filters from a request.
// An unknown state value is ignored, so a stale filter link degrades to the
// unfiltered list instead of an error.
func jobFilterFromQuery(r *http.Request) (queue.JobFilter, error) {
	q := r.URL.Query()
	filter := queue.JobFilter{Kind: q.Get("kind")}
	if state := q.Get("state"); state != "" {
		for _, known := range queue.StateOrder {
			if string(known) == state {
				filter.State = known
				break
			}
		}
	}
	if raw := q.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 {
			return queue.JobFilter{}, fmt.Errorf("limit must be a positive integer")
		}
		if limit > maxListLimit {
			limit = maxListLimit
		}
		filter.Limit = limit
	}
	return filter, nil
}

func (s *Server) render(w http.ResponseWriter, r *http.Request, page string, data any) {
	tmpl, ok := s.pages[page]
	if !ok {
		s.internalError(w, r, fmt.Errorf("web: unknown page %q", page))
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if err := tmpl.ExecuteTemplate(w, "layout", data); err != nil {
		log.Printf("web: render %s: %v", page, err)
	}
}

func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("web: encode json: %v", err)
	}
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	log.Printf("web: %s %s: %v", r.Method, r.URL.Path, err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

// parsePages builds one template set per page. Each set shares the layout
// template and adds the page body, so every page keeps the same chrome.
func parsePages() (map[string]*template.Template, error) {
	funcs := template.FuncMap{
		"shortID":        shortID,
		"utcTime":        utcTime,
		"payloadPreview": payloadPreview,
		"stateClass":     stateClass,
	}
	pages := map[string]*template.Template{}
	for _, page := range []string{"dashboard.html", "jobs.html", "job.html"} {
		name := strings.TrimSuffix(page, ".html")
		tmpl, err := template.New(name).Funcs(funcs).ParseFS(uiFS,
			"ui/templates/layout.html", "ui/templates/"+page)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", page, err)
		}
		pages[name] = tmpl
	}
	return pages, nil
}

// shortID shortens a job ID for compact table cells.
func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// utcTime formats a timestamp in UTC, matching how the store writes it.
func utcTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05") + " UTC"
}

// payloadPreview truncates a payload for table cells. Full payloads remain on
// the job detail page.
func payloadPreview(payload string) string {
	const max = 60
	if len(payload) <= max {
		return payload
	}
	return payload[:max] + "..."
}

// stateClass returns the CSS class for a job state badge.
func stateClass(s queue.JobState) string {
	return "state state-" + string(s)
}

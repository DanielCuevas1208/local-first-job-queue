// Package web serves an HTML dashboard and a small JSON API for queue
// inspection. The dashboard reads the same SQLite store as the other tools, so
// it shows the live state of every job and event. The JSON endpoints are also
// useful on their own, because scripts can poll them without parsing HTML.
//
// The dashboard renders entirely in the browser. Each page load fetches one
// snapshot from /api/snapshot and re-renders the tables. A timer refreshes the
// snapshot every few seconds, so a worker's progress appears without a reload.
package web

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"

	"github.com/local-first-job-queue/internal/queue"
)

//go:embed dashboard.html
var dashboardHTML string

var dashboardTpl = template.Must(template.New("dashboard").Parse(dashboardHTML))

// Server exposes queue state over HTTP. It owns no worker state, so any number
// of servers may read the same database file.
type Server struct {
	store  *queue.SQLiteStore
	q      *queue.Queue
	dbPath string
}

// Option configures a Server.
type Option func(*Server)

// WithDBPath sets the database path shown in the dashboard header. The value is
// cosmetic: the store still reads from the path the caller opened.
func WithDBPath(path string) Option {
	return func(s *Server) {
		s.dbPath = path
	}
}

// New returns a Server that reads queue state from store.
func New(store *queue.SQLiteStore, opts ...Option) *Server {
	s := &Server{store: store, q: queue.NewQueue(store)}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Handler returns the HTTP routes for the dashboard and its JSON API.
//
//	GET  /                          dashboard page
//	GET  /api/snapshot              queue snapshot as JSON
//	GET  /api/jobs/{id}             one job and its event timeline
//	POST /api/jobs/{id}/requeue     return a dead-lettered job to pending
//	GET  /healthz                   liveness probe
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.dashboard)
	mux.HandleFunc("GET /api/snapshot", s.snapshot)
	mux.HandleFunc("GET /api/jobs/{id}", s.jobDetail)
	mux.HandleFunc("POST /api/jobs/{id}/requeue", s.requeue)
	mux.HandleFunc("GET /healthz", s.health)
	return mux
}

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	page := struct{ DBPath string }{DBPath: s.dbPath}
	if err := dashboardTpl.Execute(w, page); err != nil {
		log.Printf("render dashboard: %v", err)
	}
}

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := s.q.Inspect()
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("snapshot: %w", err))
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// jobDetail returns one job and its event timeline. An unknown id produces a
// 404 so a dashboard can tell a missing job from a failed read.
func (s *Server) jobDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, fmt.Errorf("job %s not found", id))
			return
		}
		writeError(w, http.StatusInternalServerError, fmt.Errorf("get job %s: %w", id, err))
		return
	}
	events, err := s.store.GetJobEvents(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("get events for %s: %w", id, err))
		return
	}
	writeJSON(w, http.StatusOK, jobDetail{Job: job, Events: events})
}

// requeue returns a dead-lettered job to the pending state. The queue layer
// rejects jobs that are not dead-lettered, so the endpoint reports a 400 when
// the caller asks for an invalid transition.
func (s *Server) requeue(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.q.Requeue(id)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintln(w, "ok")
}

type jobDetail struct {
	Job    queue.Job     `json:"job"`
	Events []queue.Event `json:"events"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode json: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	fmt.Fprintln(w, err)
}

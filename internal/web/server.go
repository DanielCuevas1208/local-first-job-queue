// Package web serves a local dashboard for queue inspection. A single embedded
// HTML page shows the queue state, the job list, and the event log. The page
// refreshes itself on a timer and reads every snapshot from the shared SQLite
// store, so it always matches the CLI tools.
//
// The package also exposes a small JSON API. The dashboard uses it, and scripts
// can reuse it. Every endpoint is read-only except the requeue action, which
// returns a dead-lettered job to the pending state.
package web

import (
	"bytes"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"time"

	"github.com/local-first-job-queue/internal/metrics"
	"github.com/local-first-job-queue/internal/queue"
)

//go:embed assets/index.html
var assets embed.FS

var indexTemplate = template.Must(template.ParseFS(assets, "assets/index.html"))

// Server holds the store behind the dashboard. One Server serves the page, the
// JSON API, and the Prometheus endpoint.
type Server struct {
	store  *queue.SQLiteStore
	q      *queue.Queue
	dbPath string
}

// Option configures a Server.
type Option func(*Server)

// WithDBPath sets the database path shown on the dashboard. The default is an
// empty string, which the page renders as "in-memory".
func WithDBPath(p string) Option {
	return func(s *Server) {
		s.dbPath = p
	}
}

// Handler returns the HTTP handler for the dashboard, its JSON API, and the
// Prometheus metrics endpoint. The store must stay open for the life of the
// handler.
func Handler(store *queue.SQLiteStore, opts ...Option) http.Handler {
	s := &Server{store: store, q: queue.NewQueue(store)}
	for _, o := range opts {
		o(s)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.serveDashboard)
	mux.HandleFunc("GET /api/snapshot", s.serveSnapshot)
	mux.HandleFunc("GET /api/jobs/{id}", s.serveJobDetail)
	mux.HandleFunc("POST /api/jobs/{id}/requeue", s.serveRequeue)
	mux.Handle("/metrics", metrics.Handler(store))
	return mux
}

func (s *Server) serveDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data := struct{ DBPath string }{DBPath: s.dbPath}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := indexTemplate.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// snapshotResponse is the payload of GET /api/snapshot. It mirrors the queue
// snapshot and adds the database path and the server clock for the dashboard.
type snapshotResponse struct {
	DBPath string                 `json:"db_path"`
	Now    time.Time              `json:"now"`
	Stats  map[queue.JobState]int `json:"stats"`
	Jobs   []queue.Job            `json:"jobs"`
	Events []queue.Event          `json:"events"`
}

func (s *Server) serveSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := s.q.Inspect()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, snapshotResponse{
		DBPath: s.dbPath,
		Now:    time.Now().UTC(),
		Stats:  snap.Stats,
		Jobs:   snap.Jobs,
		Events: snap.Events,
	})
}

// jobDetail is the payload of GET /api/jobs/{id}. It carries the job and its
// complete event timeline, oldest first.
type jobDetail struct {
	Job    queue.Job     `json:"job"`
	Events []queue.Event `json:"events"`
}

func (s *Server) serveJobDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(id)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, fmt.Sprintf("job %s not found", id), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	events, err := s.store.GetJobEvents(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, jobDetail{Job: job, Events: events})
}

// requeueRequest is the optional JSON body of POST /api/jobs/{id}/requeue. An
// empty body keeps the job data and attempt budget.
type requeueRequest struct {
	Payload     *string `json:"payload"`
	MaxAttempts *int    `json:"max_attempts"`
}

func (s *Server) serveRequeue(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req requeueRequest
	if r.Body != nil {
		defer r.Body.Close()
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read request body", http.StatusBadRequest)
			return
		}
		if len(bytes.TrimSpace(body)) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				http.Error(w, "invalid JSON body", http.StatusBadRequest)
				return
			}
		}
	}

	var opts []queue.RequeueOption
	if req.Payload != nil {
		opts = append(opts, queue.RequeueWithPayload(*req.Payload))
	}
	if req.MaxAttempts != nil && *req.MaxAttempts >= 1 {
		opts = append(opts, queue.RequeueWithMaxAttempts(*req.MaxAttempts))
	}

	job, err := s.q.Requeue(id, opts...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

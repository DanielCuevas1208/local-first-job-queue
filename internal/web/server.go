// Package web serves a web dashboard for queue inspection and a JSON API for
// scripts. The server embeds its templates and styles, so the binary is
// self-contained. Requeue is the only write operation; it is exposed as a
// POST handler and as a JSON endpoint. The server also renders Prometheus
// metrics at /metrics, matching the metrics command.
package web

import (
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/local-first-job-queue/internal/metrics"
	"github.com/local-first-job-queue/internal/queue"
)

//go:embed templates/*.html
var templatesFS embed.FS

//go:embed static
var staticFS embed.FS

// pageSize is the number of jobs shown on one dashboard page.
const pageSize = 25

// maxRecentEvents caps the events shown on the dashboard.
const maxRecentEvents = 25

// Option configures a Server.
type Option func(*Server)

// WithNow overrides the clock used for age calculations. Tests use it to make
// the oldest-pending metric deterministic.
func WithNow(fn func() time.Time) Option {
	return func(s *Server) {
		s.now = fn
	}
}

// Server holds the store and the parsed templates shared by every handler.
type Server struct {
	store    *queue.SQLiteStore
	q        *queue.Queue
	now      func() time.Time
	dashTmpl *template.Template
	jobTmpl  *template.Template
}

var templateFuncs = template.FuncMap{
	"shortID": shortID,
	// formatTime prints time.Time and *time.Time values as UTC. A nil pointer
	// renders as an empty string so optional fields stay tidy.
	"formatTime": func(v any) string {
		switch t := v.(type) {
		case time.Time:
			return t.UTC().Format("2006-01-02 15:04:05")
		case *time.Time:
			if t == nil {
				return ""
			}
			return t.UTC().Format("2006-01-02 15:04:05")
		}
		return ""
	},
	// asString renders typed strings such as JobState and EventType as their
	// plain form, so templates can build CSS classes and compare values.
	"asString": func(v any) string {
		return fmt.Sprintf("%v", v)
	},
	// stateOptions lists the filterable states in canonical order.
	"stateOptions": stateOptions,
}

// Handler returns an HTTP handler that serves the dashboard, the JSON API, and
// the Prometheus metrics endpoint. Templates are embedded, so parsing cannot
// fail at runtime unless the binary is corrupted.
func Handler(store *queue.SQLiteStore, opts ...Option) http.Handler {
	s := &Server{store: store, q: queue.NewQueue(store), now: time.Now}
	for _, o := range opts {
		o(s)
	}

	// Each page template is parsed together with the layout into its own set.
	// Pages share the content and title definition names, so a page must never
	// bleed into the set of another page.
	parse := func(files ...string) *template.Template {
		return template.Must(template.New("layout.html").Funcs(templateFuncs).ParseFS(templatesFS, files...))
	}
	s.dashTmpl = parse("templates/layout.html", "templates/dashboard.html")
	s.jobTmpl = parse("templates/layout.html", "templates/job.html")

	mux := http.NewServeMux()
	mux.Handle("GET /static/", http.FileServer(http.FS(staticFS)))
	mux.HandleFunc("GET /", s.handleDashboard)
	mux.HandleFunc("GET /jobs/{id}", s.handleJobDetail)
	mux.HandleFunc("POST /jobs/{id}/requeue", s.handleRequeuePage)

	mux.HandleFunc("GET /api/stats", s.handleStats)
	mux.HandleFunc("GET /api/jobs", s.handleJobList)
	mux.HandleFunc("GET /api/jobs/{id}", s.handleJobGet)
	mux.HandleFunc("GET /api/jobs/{id}/events", s.handleJobEvents)
	mux.HandleFunc("POST /api/jobs/{id}/requeue", s.handleRequeueAPI)

	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return mux
}

// handleMetrics serves the queue state in the Prometheus text format. The
// server clock is shared with the dashboard so both stay consistent.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := metrics.New(s.store, metrics.WithNow(s.now)).Write(w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

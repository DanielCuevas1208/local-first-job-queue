// Package web serves a read-only HTML dashboard for the queue. An operator
// opens the dashboard in a browser to inspect queue state, jobs, and the
// append-only event log. Every page reads the shared SQLite store, so it shows
// the same data as the inspect and metrics commands.
package web

import (
	"bytes"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

//go:embed *.html
var templateFiles embed.FS

// Each page is its own template set. A shared layout file defines the page
// frame, and the page file fills the title and content blocks. Keeping the sets
// separate stops the page-specific block names from overwriting each other.
var dashboardTemplates = template.Must(
	template.New("layout.html").Funcs(templateFuncs).ParseFS(templateFiles, "layout.html", "dashboard.html"),
)
var jobTemplates = template.Must(
	template.New("layout.html").Funcs(templateFuncs).ParseFS(templateFiles, "layout.html", "job.html"),
)

var templateFuncs = template.FuncMap{
	"shortID":    shortID,
	"fmtTime":    fmtTime,
	"stateClass": stateClass,
}

// recentEventsLimit caps the events shown on the dashboard. The event log is
// append-only and grows without bound, so the dashboard keeps the newest rows.
const recentEventsLimit = 25

// The canonical state order keeps the stat cards stable across reloads. The
// failed state appears for legacy databases that predate the dead-letter queue.
var stateOrder = []queue.JobState{
	queue.StatePending,
	queue.StateLeased,
	queue.StateCompleted,
	queue.StateDeadLetter,
	queue.StateFailed,
}

type stateStat struct {
	State queue.JobState
	Count int
}

// dashboardPage carries the data rendered by the dashboard template.
type dashboardPage struct {
	GeneratedAt time.Time
	Stats       []stateStat
	Kinds       []queue.KindStateCount
	Events      []queue.Event
	Jobs        []queue.Job
}

// jobPage carries the data rendered by the job detail template.
type jobPage struct {
	GeneratedAt time.Time
	Job         queue.Job
	Events      []queue.Event
}

// Handler returns the HTTP handler for the dashboard. The root path shows the
// queue overview and /jobs/{id} shows one job with its event timeline.
func Handler(store *queue.SQLiteStore) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", dashboard(store))
	mux.HandleFunc("GET /jobs/{id}", job(store))
	return mux
}

func dashboard(store *queue.SQLiteStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := store.GetQueueStats()
		if err != nil {
			serverError(w)
			return
		}
		kinds, err := store.GetStateKindCounts()
		if err != nil {
			serverError(w)
			return
		}
		events, err := store.GetAllEvents()
		if err != nil {
			serverError(w)
			return
		}
		jobs, err := store.GetAllJobs()
		if err != nil {
			serverError(w)
			return
		}
		if len(events) > recentEventsLimit {
			events = events[:recentEventsLimit]
		}
		write(w, dashboardTemplates, dashboardPage{
			GeneratedAt: time.Now().UTC(),
			Stats:       orderedStats(stats),
			Kinds:       kinds,
			Events:      events,
			Jobs:        jobs,
		})
	}
}

func job(store *queue.SQLiteStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		job, err := store.GetJob(id)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				http.NotFound(w, r)
				return
			}
			serverError(w)
			return
		}
		events, err := store.GetJobEvents(id)
		if err != nil {
			serverError(w)
			return
		}
		write(w, jobTemplates, jobPage{
			GeneratedAt: time.Now().UTC(),
			Job:         job,
			Events:      events,
		})
	}
}

func orderedStats(stats map[queue.JobState]int) []stateStat {
	out := make([]stateStat, 0, len(stateOrder))
	for _, s := range stateOrder {
		out = append(out, stateStat{State: s, Count: stats[s]})
	}
	return out
}

// write renders a page into a buffer before sending it. A template failure
// therefore produces a clean 500 instead of a truncated page.
func write(w http.ResponseWriter, t *template.Template, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "layout.html", data); err != nil {
		serverError(w)
		return
	}
	_, _ = w.Write(buf.Bytes())
}

func serverError(w http.ResponseWriter) {
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// fmtTime renders a timestamp in a fixed UTC form. It accepts a time.Time or a
// pointer, so templates can pass nullable fields directly.
func fmtTime(t any) string {
	switch v := t.(type) {
	case time.Time:
		return v.UTC().Format("2006-01-02 15:04:05")
	case *time.Time:
		if v == nil {
			return ""
		}
		return v.UTC().Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprint(t)
	}
}

// stateClass maps a job state to a CSS class used for badges and stat cards.
func stateClass(s queue.JobState) string {
	switch s {
	case queue.StatePending:
		return "pending"
	case queue.StateLeased:
		return "leased"
	case queue.StateCompleted:
		return "completed"
	case queue.StateDeadLetter, queue.StateFailed:
		return "dead"
	}
	return ""
}

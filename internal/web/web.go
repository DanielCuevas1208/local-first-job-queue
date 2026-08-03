// Package web serves a local browser dashboard for queue inspection. The
// dashboard reads the shared SQLite store through the queue API and renders
// state counts, jobs, recent events, and per-job timelines. Assets are
// embedded in the binary, so the dashboard needs no build step or network.
package web

import (
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"

	"github.com/local-first-job-queue/internal/metrics"
	"github.com/local-first-job-queue/internal/queue"
)

//go:embed assets
var assets embed.FS

var indexHTML = readIndex()

// assetRoot is the embedded filesystem rooted at the assets directory. It
// serves style.css and app.js at /assets/<name> after the prefix strip.
var assetRoot = mustSub()

func mustSub() fs.FS {
	root, err := fs.Sub(assets, "assets")
	if err != nil {
		panic("web: embedded assets missing: " + err.Error())
	}
	return root
}

func readIndex() []byte {
	b, err := assets.ReadFile("assets/index.html")
	if err != nil {
		panic("web: embedded index.html missing: " + err.Error())
	}
	return b
}

// New returns an HTTP handler for the inspection dashboard.
//
// Routes:
//
//	/                the dashboard page
//	/assets/*        embedded styles and scripts
//	/api/snapshot    queue state as JSON
//	/api/jobs/{id}   one job and its event timeline
//	/metrics         the Prometheus exposition endpoint
func New(store *queue.SQLiteStore) http.Handler {
	q := queue.NewQueue(store)
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/snapshot", func(w http.ResponseWriter, r *http.Request) {
		snap, err := q.Inspect()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, snap)
	})

	mux.HandleFunc("GET /api/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		job, err := store.GetJob(id)
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("job not found"))
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		events, err := store.GetJobEvents(id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, map[string]any{"job": job, "events": events})
	})

	mux.Handle("GET /metrics", metrics.Handler(store))
	mux.Handle("GET /assets/", http.StripPrefix("/assets/", http.FileServer(http.FS(assetRoot))))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})

	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("%v", err)})
}

package web

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/local-first-job-queue/internal/queue"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// handleStats serves a JSON snapshot of queue state: per-state counts, per
// kind and state counts, per event-type counts, the newest events, and the age
// of the oldest pending job. Scripts use this endpoint to build dashboards.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.GetQueueStats()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	byKind, err := s.store.GetStateKindCounts()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	evCounts, err := s.store.GetEventTypeCounts()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	events, err := s.store.GetAllEvents()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(events) > maxRecentEvents {
		events = events[:maxRecentEvents]
	}

	var oldest *float64
	if ready, ok, err := s.store.GetOldestPendingReadyTime(); err == nil && ok {
		v := s.now().Sub(ready).Seconds()
		oldest = &v
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"stats":                  stats,
		"by_kind":                byKind,
		"event_types":            evCounts,
		"events":                 events,
		"oldest_pending_seconds": oldest,
	})
}

// handleJobList serves one page of jobs as JSON. The state, kind, limit, and
// offset query parameters map onto a JobFilter. Invalid numbers fall back to
// the defaults, so a broken URL cannot fail the request.
func (s *Server) handleJobList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := queue.JobFilter{
		Kind:   q.Get("kind"),
		Limit:  parseInt(q.Get("limit"), pageSize),
		Offset: parseInt(q.Get("offset"), 0),
	}
	if st := q.Get("state"); st != "" {
		v := queue.JobState(st)
		f.State = &v
	}

	jobs, total, err := s.store.ListJobs(f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "total": total})
}

// handleJobGet serves one job as JSON.
func (s *Server) handleJobGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("job %s not found", id))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// handleJobEvents serves the event timeline of one job as JSON.
func (s *Server) handleJobEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.GetJob(id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, fmt.Sprintf("job %s not found", id))
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	events, err := s.store.GetJobEvents(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job_id": id, "events": events})
}

// handleRequeueAPI returns a dead-lettered job to the pending state. The
// optional JSON body may carry a new payload and a new attempt budget.
func (s *Server) handleRequeueAPI(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Payload     *string `json:"payload"`
		MaxAttempts int     `json:"max_attempts"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}

	opts := []queue.RequeueOption{}
	if body.Payload != nil {
		opts = append(opts, queue.RequeueWithPayload(*body.Payload))
	}
	if body.MaxAttempts > 0 {
		opts = append(opts, queue.RequeueWithMaxAttempts(body.MaxAttempts))
	}

	job, err := s.q.Requeue(id, opts...)
	if err != nil {
		writeJSON(w, requeueStatus(err), map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// requeueStatus maps a Requeue error to an HTTP status code. An unknown job is
// a 404; a job that is not dead-lettered is a conflict.
func requeueStatus(err error) int {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return http.StatusNotFound
	case isNotDeadLettered(err):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func isNotDeadLettered(err error) bool {
	return strings.Contains(err.Error(), "not dead-lettered")
}

// parseInt parses a query number. Empty and invalid values fall back to def.
func parseInt(raw string, def int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

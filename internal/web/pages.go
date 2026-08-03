package web

import (
	"bytes"
	"database/sql"
	"errors"
	"html/template"
	"net/http"
	"net/url"
	"strconv"

	"github.com/local-first-job-queue/internal/queue"
)

// stateOrder mirrors the metrics state order so the dashboard and the exporter
// present the same view of the queue.
var stateOrder = []queue.JobState{
	queue.StatePending,
	queue.StateLeased,
	queue.StateCompleted,
	queue.StateDeadLetter,
	queue.StateFailed,
}

// stateOptions returns the filterable states as plain strings. Templates use it
// to build the state dropdown in the same order every time.
func stateOptions() []string {
	opts := make([]string, 0, len(stateOrder))
	for _, st := range stateOrder {
		opts = append(opts, string(st))
	}
	return opts
}

// stateCard renders one clickable summary card on the dashboard.
type stateCard struct {
	State  queue.JobState
	Count  int
	URL    string
	Active bool
}

// kindRow is one row of the kind breakdown, with the job count per state.
type kindRow struct {
	Kind   string
	Counts map[string]int
}

// dashboardView is the data passed to the dashboard template.
type dashboardView struct {
	StateCards []stateCard
	Kinds      []kindRow
	Jobs       []queue.Job
	Total      int
	Events     []queue.Event
	State      string
	Kind       string
	Limit      int
	Offset     int
	FirstItem  int
	LastItem   int
	ShowPrev   bool
	ShowNext   bool
	Filtered   bool
	PrevURL    string
	NextURL    string
	ClearURL   string
}

// jobView is the data passed to the job detail template.
type jobView struct {
	Job    queue.Job
	Events []queue.Event
}

// handleDashboard renders the queue at a glance: state cards, a kind
// breakdown, a filterable and paged job table, and the newest events.
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.GetQueueStats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	byKind, err := s.store.GetStateKindCounts()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	events, err := s.store.GetAllEvents()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(events) > maxRecentEvents {
		events = events[:maxRecentEvents]
	}

	q := r.URL.Query()
	state := q.Get("state")
	kind := q.Get("kind")
	f := queue.JobFilter{
		Kind:   kind,
		Limit:  parseInt(q.Get("limit"), pageSize),
		Offset: parseInt(q.Get("offset"), 0),
	}
	if state != "" {
		v := queue.JobState(state)
		f.State = &v
	}
	jobs, total, err := s.store.ListJobs(f)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	kinds := pivotKinds(byKind)

	var cards []stateCard
	for _, st := range stateOrder {
		cards = append(cards, stateCard{
			State:  st,
			Count:  stats[st],
			URL:    s.dashboardURL(state, kind, f.Limit, 0, st),
			Active: string(st) == state,
		})
	}

	firstItem, lastItem := itemRange(f.Offset, len(jobs))
	view := dashboardView{
		StateCards: cards,
		Kinds:      kinds,
		Jobs:       jobs,
		Total:      total,
		Events:     events,
		State:      state,
		Kind:       kind,
		Limit:      f.Limit,
		Offset:     f.Offset,
		FirstItem:  firstItem,
		LastItem:   lastItem,
		ShowPrev:   f.Offset > 0,
		ShowNext:   lastItem < total,
		Filtered:   state != "" || kind != "",
		PrevURL:    s.dashboardURL(state, kind, f.Limit, maxInt(0, f.Offset-f.Limit), ""),
		NextURL:    s.dashboardURL(state, kind, f.Limit, f.Offset+f.Limit, ""),
		ClearURL:   s.dashboardURL("", "", f.Limit, 0, ""),
	}
	s.render(w, s.dashTmpl, view)
}

// pivotKinds turns the flat (kind, state, count) rows into one row per kind.
// The store returns rows sorted by kind, so the output order is stable.
func pivotKinds(rows []queue.KindStateCount) []kindRow {
	byKind := map[string]*kindRow{}
	var order []string
	for _, r := range rows {
		k, ok := byKind[r.Kind]
		if !ok {
			k = &kindRow{Kind: r.Kind, Counts: map[string]int{}}
			byKind[r.Kind] = k
			order = append(order, r.Kind)
		}
		k.Counts[string(r.State)] = r.Count
	}
	out := make([]kindRow, 0, len(order))
	for _, name := range order {
		out = append(out, *byKind[name])
	}
	return out
}

// handleJobDetail renders one job and its complete event timeline.
func (s *Server) handleJobDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(id)
	if errors.Is(err, sql.ErrNoRows) {
		http.NotFound(w, r)
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
	s.render(w, s.jobTmpl, jobView{Job: job, Events: events})
}

// handleRequeuePage returns a dead-lettered job to the pending state from the
// browser form, then redirects back to the job page. The optional payload field
// replaces the job payload for the retry.
func (s *Server) handleRequeuePage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	opts := []queue.RequeueOption{}
	if payload := r.FormValue("payload"); payload != "" {
		opts = append(opts, queue.RequeueWithPayload(payload))
	}
	if _, err := s.q.Requeue(id, opts...); err != nil {
		http.Error(w, err.Error(), requeueStatus(err))
		return
	}
	http.Redirect(w, r, "/jobs/"+id, http.StatusSeeOther)
}

// render executes the given template set and writes the result. Rendering into
// a buffer first keeps partial template failures from corrupting a response.
func (s *Server) render(w http.ResponseWriter, t *template.Template, data any) {
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "layout", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(buf.Bytes())
}

// dashboardURL builds a dashboard link that preserves the current filters.
// When targetState is non-empty, it overrides the current state filter so the
// state cards act as quick filters.
func (s *Server) dashboardURL(state, kind string, limit, offset int, targetState queue.JobState) string {
	v := url.Values{}
	activeState := state
	if targetState != "" {
		activeState = string(targetState)
	}
	if activeState != "" {
		v.Set("state", activeState)
	}
	if kind != "" {
		v.Set("kind", kind)
	}
	v.Set("limit", strconv.Itoa(limit))
	if offset > 0 {
		v.Set("offset", strconv.Itoa(offset))
	}
	return "/?" + v.Encode()
}

func itemRange(offset, pageLen int) (first, last int) {
	first = offset + 1
	last = offset + pageLen
	if pageLen == 0 {
		return 0, 0
	}
	return first, last
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

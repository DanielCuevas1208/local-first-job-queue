package dashboard

import (
	"fmt"
	"html/template"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

// embeddedPage is the dashboard HTML. The page is self-contained: CSS and
// JavaScript live inline, so the binary serves it without external assets. The
// server renders the page from a fresh snapshot, and the script keeps it fresh
// by polling the JSON endpoints.
const embeddedPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Local-first Durable Job Queue</title>
<style>
:root {
  --bg: #0d1117;
  --panel: #161b22;
  --border: #30363d;
  --text: #e6edf3;
  --muted: #8b949e;
  --accent: #58a6ff;
  --pending: #d29922;
  --leased: #58a6ff;
  --completed: #3fb950;
  --dead: #f85149;
  --failed: #bc8cff;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  padding: 24px;
  background: var(--bg);
  color: var(--text);
  font-family: "Segoe UI", system-ui, sans-serif;
  line-height: 1.5;
}
header { display: flex; align-items: baseline; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }
h1 { font-size: 20px; margin: 0; }
header .meta { color: var(--muted); font-size: 13px; }
.kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 24px; }
.kpi { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.kpi .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .05em; }
.kpi .value { font-size: 30px; font-weight: 600; margin-top: 4px; font-variant-numeric: tabular-nums; }
.kpi.pending .value { color: var(--pending); }
.kpi.leased .value { color: var(--leased); }
.kpi.completed .value { color: var(--completed); }
.kpi.dead_letter .value { color: var(--dead); }
.kpi.failed .value { color: var(--failed); }
.kpi.muted .value { color: var(--muted); }
.columns { display: grid; grid-template-columns: 2fr 1fr; gap: 24px; align-items: start; }
@media (max-width: 1000px) { .columns { grid-template-columns: 1fr; } }
.panel { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 24px; }
.panel h2 { font-size: 14px; margin: 0 0 12px; text-transform: uppercase; letter-spacing: .05em; color: var(--muted); }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--border); }
th { color: var(--muted); font-weight: 500; }
td.mono, code, .mono { font-family: "Cascadia Mono", Consolas, monospace; }
.state { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 12px; background: #21262d; }
.state.pending { color: var(--pending); }
.state.leased { color: var(--leased); }
.state.completed { color: var(--completed); }
.state.dead_letter { color: var(--dead); }
.state.failed { color: var(--failed); }
footer { color: var(--muted); font-size: 12px; margin-top: 24px; }
a { color: var(--accent); text-decoration: none; }
#live { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: var(--completed); margin-right: 6px; }
.empty { color: var(--muted); font-size: 13px; }
</style>
</head>
<body>
<header>
  <h1>Local-first Durable Job Queue</h1>
  <span class="meta mono">{{if .DBPath}}db: {{.DBPath}}{{else}}db: (temp){{end}}</span>
  <span class="meta" id="refresh-note">refresh: {{printf "%.0f" (seconds .Refresh)}}s</span>
  <span class="meta"><span id="live"></span><span id="generated">connecting</span></span>
</header>

<section class="kpis">
  <div class="kpi pending"><div class="label">pending</div><div class="value" id="kpi-pending">{{statecount .Overview "pending"}}</div></div>
  <div class="kpi leased"><div class="label">leased</div><div class="value" id="kpi-leased">{{statecount .Overview "leased"}}</div></div>
  <div class="kpi completed"><div class="label">completed</div><div class="value" id="kpi-completed">{{statecount .Overview "completed"}}</div></div>
  <div class="kpi dead_letter"><div class="label">dead letter</div><div class="value" id="kpi-dead">{{statecount .Overview "dead_letter"}}</div></div>
  <div class="kpi failed"><div class="label">failed</div><div class="value" id="kpi-failed">{{statecount .Overview "failed"}}</div></div>
  <div class="kpi muted"><div class="label">oldest pending</div><div class="value" id="kpi-oldest">{{age .Overview.OldestPending}}</div></div>
  <div class="kpi muted"><div class="label">events logged</div><div class="value" id="kpi-events">{{.Overview.TotalEvents}}</div></div>
</section>

<div class="columns">
  <div>
    <section class="panel">
      <h2>Jobs <span class="mono" id="job-count">({{.Overview.TotalJobs}})</span></h2>
      <table id="jobs-table">
        <thead><tr><th>id</th><th>kind</th><th>priority</th><th>state</th><th>attempts</th><th>created</th><th>run at</th></tr></thead>
        <tbody id="jobs-body">
        {{range .Overview.Jobs}}
          <tr>
            <td class="mono"><a href="/api/jobs/{{.ID}}">{{shortid .ID}}</a></td>
            <td>{{.Kind}}</td>
            <td class="mono">{{.Priority}}</td>
            <td><span class="state {{.State}}">{{.State}}</span></td>
            <td class="mono">{{.RetryCount}}/{{.MaxAttempts}}</td>
            <td class="mono">{{timefmt .CreatedAt}}</td>
            <td class="mono">{{optionaltime .RunAt}}</td>
          </tr>
        {{else}}
          <tr><td colspan="7" class="empty">No jobs yet.</td></tr>
        {{end}}
        </tbody>
      </table>
    </section>

    <section class="panel">
      <h2>Per kind</h2>
      {{if .Overview.ByKind}}
      <table>
        <thead><tr><th>kind</th><th>state</th><th>count</th></tr></thead>
        <tbody>
        {{range .Overview.ByKind}}
          <tr><td>{{.Kind}}</td><td><span class="state {{.State}}">{{.State}}</span></td><td class="mono">{{.Count}}</td></tr>
        {{end}}
        </tbody>
      </table>
      {{else}}
        <span class="empty">No jobs.</span>
      {{end}}
    </section>
  </div>

  <div>
    <section class="panel">
      <h2>Recent events</h2>
      <table>
        <thead><tr><th>time</th><th>job</th><th>type</th><th>details</th></tr></thead>
        <tbody id="events-body">
        {{range .Overview.Events}}
          <tr>
            <td class="mono">{{timefmt .Timestamp}}</td>
            <td class="mono">{{shortid .JobID}}</td>
            <td>{{.EventType}}</td>
            <td class="mono">{{ptrstr .Metadata}}</td>
          </tr>
        {{else}}
          <tr><td colspan="4" class="empty">No events yet.</td></tr>
        {{end}}
        </tbody>
      </table>
    </section>
  </div>
</div>

<footer>
  Read-only dashboard. Scrape <a href="/metrics">/metrics</a> for Prometheus output.
  Use the CLI for writes: enqueue, work, inspect, history, requeue.
</footer>

<script>
(function () {
  var refreshMs = {{.Refresh.Milliseconds}};
  if (refreshMs < 500) { refreshMs = 500; }
  function esc(s) {
    var d = document.createElement("div");
    d.textContent = String(s == null ? "" : s);
    return d.innerHTML;
  }
  function shortid(id) {
    if (!id) { return ""; }
    return id.length > 8 ? id.slice(0, 8) : id;
  }
  function timefmt(ts) {
    if (!ts) { return ""; }
    return ts.replace("T", " ").replace("Z", "").slice(0, 19);
  }
  function stateBadge(s) {
    return '<span class="state ' + esc(s) + '">' + esc(s) + "</span>";
  }
  function setText(id, v) {
    document.getElementById(id).textContent = v;
  }
  function render(ov) {
    setText("kpi-pending", ov.stats.pending || 0);
    setText("kpi-leased", ov.stats.leased || 0);
    setText("kpi-completed", ov.stats.completed || 0);
    setText("kpi-dead", ov.stats.dead_letter || 0);
    setText("kpi-failed", ov.stats.failed || 0);
    setText("kpi-oldest", ov.oldest_pending_seconds == null ? "-" : Number(ov.oldest_pending_seconds).toFixed(1) + "s");
    setText("kpi-events", ov.total_events);
    setText("job-count", "(" + ov.total_jobs + ")");
    setText("generated", "updated " + new Date(ov.generated_at).toLocaleTimeString());
    document.getElementById("live").style.background = "var(--completed)";

    var jobsBody = document.getElementById("jobs-body");
    jobsBody.innerHTML = "";
    if (!ov.jobs || ov.jobs.length === 0) {
      jobsBody.innerHTML = '<tr><td colspan="7" class="empty">No jobs yet.</td></tr>';
      return;
    }
    ov.jobs.forEach(function (j) {
      var tr = document.createElement("tr");
      var runAt = j.run_at ? timefmt(j.run_at) : "";
      tr.innerHTML =
        '<td class="mono"><a href="/api/jobs/' + esc(j.id) + '">' + esc(shortid(j.id)) + "</a></td>" +
        "<td>" + esc(j.kind) + "</td>" +
        '<td class="mono">' + esc(j.priority) + "</td>" +
        "<td>" + stateBadge(j.state) + "</td>" +
        '<td class="mono">' + esc(j.retry_count) + "/" + esc(j.max_attempts) + "</td>" +
        '<td class="mono">' + esc(timefmt(j.created_at)) + "</td>" +
        '<td class="mono">' + esc(runAt) + "</td>";
      jobsBody.appendChild(tr);
    });

    var eventsBody = document.getElementById("events-body");
    eventsBody.innerHTML = "";
    if (!ov.events || ov.events.length === 0) {
      eventsBody.innerHTML = '<tr><td colspan="4" class="empty">No events yet.</td></tr>';
      return;
    }
    ov.events.forEach(function (e) {
      var tr = document.createElement("tr");
      tr.innerHTML =
        '<td class="mono">' + esc(timefmt(e.timestamp)) + "</td>" +
        '<td class="mono">' + esc(shortid(e.job_id)) + "</td>" +
        "<td>" + esc(e.event_type) + "</td>" +
        '<td class="mono">' + esc(e.metadata || "") + "</td>";
      eventsBody.appendChild(tr);
    });
  }
  function poll() {
    fetch("/api/overview")
      .then(function (r) { return r.json(); })
      .then(render)
      .catch(function () {
        document.getElementById("live").style.background = "var(--dead)";
      });
  }
  setInterval(poll, refreshMs);
  poll();
})();
</script>
</body>
</html>
`

// templateFuncs are helpers used by the dashboard page. Keeping the helpers
// here keeps the embedded template free of presentation logic.
var templateFuncs = template.FuncMap{
	"shortid": func(id string) string {
		if len(id) > 8 {
			return id[:8]
		}
		return id
	},
	"timefmt": func(t time.Time) string {
		return t.UTC().Format("2006-01-02 15:04:05")
	},
	"optionaltime": func(t *time.Time) string {
		if t == nil {
			return ""
		}
		return t.UTC().Format("2006-01-02 15:04:05")
	},
	"ptrstr": func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	},
	"age": func(f *float64) string {
		if f == nil {
			return "-"
		}
		return fmt.Sprintf("%.1fs", *f)
	},
	"seconds": func(d time.Duration) float64 {
		return d.Seconds()
	},
	"statecount": func(ov Overview, state string) int {
		return ov.Stats[queue.JobState(state)]
	},
}

var dashboardTemplate = template.Must(template.New("dashboard").Funcs(templateFuncs).Parse(embeddedPage))

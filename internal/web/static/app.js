// app.js drives the read-only dashboard. It fetches the JSON API and renders
// the job table, then refreshes the view while auto-refresh is enabled. All
// user data is written with textContent, never as HTML, so job payloads cannot
// inject markup.
(function () {
  "use strict";

  var REFRESH_MS = 5000;
  var SHORT_ID_LEN = 8;

  var stateSelect = document.getElementById("filter-state");
  var kindSelect = document.getElementById("filter-kind");
  var qInput = document.getElementById("filter-q");
  var autoRefresh = document.getElementById("auto-refresh");
  var refreshNow = document.getElementById("refresh-now");
  var jobsBody = document.getElementById("jobs-body");
  var jobsMeta = document.getElementById("jobs-meta");
  var jobsEmpty = document.getElementById("jobs-empty");
  var lastUpdated = document.getElementById("last-updated");

  var debounceTimer = null;
  var timer = null;

  function shortID(id) {
    return id.length > SHORT_ID_LEN ? id.slice(0, SHORT_ID_LEN) : id;
  }

  function fmtTime(value) {
    if (!value) return "";
    var d = new Date(value);
    if (isNaN(d.getTime())) return value;
    return d.toLocaleString();
  }

  function fmtUTC(value) {
    if (!value) return "";
    var d = new Date(value);
    if (isNaN(d.getTime())) return value;
    return d.toISOString().replace("T", " ").slice(0, 19) + "Z";
  }

  function queryString() {
    var params = new URLSearchParams();
    if (stateSelect.value) params.set("state", stateSelect.value);
    if (kindSelect.value) params.set("kind", kindSelect.value);
    if (qInput.value.trim()) params.set("q", qInput.value.trim());
    return params.toString();
  }

  function renderJob(job) {
    var tr = document.createElement("tr");

    var idCell = document.createElement("td");
    idCell.className = "id-cell";
    var link = document.createElement("a");
    link.href = "/job/" + encodeURIComponent(job.id);
    link.textContent = shortID(job.id);
    link.title = job.id;
    idCell.appendChild(link);
    tr.appendChild(idCell);

    appendText(tr, job.kind);

    var stateCell = document.createElement("td");
    var stateBadge = document.createElement("span");
    stateBadge.className = "badge state-" + job.state;
    stateBadge.textContent = job.state;
    stateCell.appendChild(stateBadge);
    tr.appendChild(stateCell);

    appendNum(tr, job.priority);
    appendNum(tr, job.retry_count + "/" + job.max_attempts);
    appendText(tr, fmtUTC(job.created_at));
    appendText(tr, fmtUTC(job.updated_at));
    appendText(tr, fmtUTC(job.run_at));
    appendText(tr, fmtUTC(job.leased_until));

    return tr;
  }

  function appendText(tr, text) {
    var td = document.createElement("td");
    td.textContent = text || "";
    tr.appendChild(td);
    return td;
  }

  function appendNum(tr, text) {
    var td = appendText(tr, text);
    td.className = "num";
    return td;
  }

  function renderSummary(summary) {
    summary.states.forEach(function (sc) {
      var card = document.querySelector('.card-value[data-state="' + sc.state + '"]');
      if (card) card.textContent = sc.count;
    });
  }

  function fetchJSON(url) {
    return fetch(url).then(function (res) {
      if (!res.ok) throw new Error("HTTP " + res.status);
      return res.json();
    });
  }

  function refresh() {
    var query = queryString();
    var jobsURL = "/api/jobs" + (query ? "?" + query : "");
    return Promise.all([fetchJSON("/api/summary"), fetchJSON(jobsURL)])
      .then(function (results) {
        renderSummary(results[0]);
        var page = results[1];
        jobsBody.textContent = "";
        page.jobs.forEach(function (job) {
          jobsBody.appendChild(renderJob(job));
        });
        jobsMeta.textContent = "showing " + page.jobs.length + " of " + page.total;
        jobsEmpty.classList.toggle("hidden", page.jobs.length > 0);
        lastUpdated.textContent = "last updated " + fmtTime(new Date().toISOString());
      })
      .catch(function (err) {
        jobsMeta.textContent = "refresh failed: " + err.message;
      });
  }

  function scheduleNext() {
    if (timer) clearTimeout(timer);
    if (!autoRefresh.checked) return;
    timer = setTimeout(function () {
      refresh().then(scheduleNext);
    }, REFRESH_MS);
  }

  function onFilterChange() {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(function () {
      refresh().then(scheduleNext);
    }, 250);
  }

  stateSelect.addEventListener("change", onFilterChange);
  kindSelect.addEventListener("change", onFilterChange);
  qInput.addEventListener("input", onFilterChange);
  autoRefresh.addEventListener("change", function () {
    if (autoRefresh.checked) {
      scheduleNext();
    } else if (timer) {
      clearTimeout(timer);
    }
  });
  refreshNow.addEventListener("click", function () {
    refresh().then(scheduleNext);
  });

  refresh().then(scheduleNext);
})();

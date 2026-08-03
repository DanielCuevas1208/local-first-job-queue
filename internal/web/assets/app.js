(function () {
  "use strict";

  var STATE_ORDER = ["pending", "leased", "completed", "dead_letter", "failed"];
  var STATE_LABELS = {
    pending: "Pending",
    leased: "Leased",
    completed: "Completed",
    dead_letter: "Dead letter",
    failed: "Failed"
  };
  var REFRESH_MS = 3000;

  var els = {};
  var jobsById = {};
  var currentFilter = "all";
  var paused = false;

  function $(id) {
    return document.getElementById(id);
  }

  function el(tag, className, text) {
    var node = document.createElement(tag);
    if (className) {
      node.className = className;
    }
    if (text !== undefined && text !== null) {
      node.textContent = text;
    }
    return node;
  }

  function init() {
    els.lastUpdated = $("last-updated");
    els.refreshState = $("refresh-state");
    els.pauseBtn = $("pause-btn");
    els.stats = $("stats");
    els.jobsBody = $("jobs-table-body");
    els.jobsEmpty = $("jobs-empty");
    els.events = $("events");
    els.filter = $("state-filter");
    els.drawer = $("drawer");
    els.drawerTitle = $("drawer-title");
    els.drawerBody = $("drawer-body");

    els.pauseBtn.addEventListener("click", togglePause);
    els.drawer.addEventListener("click", function (ev) {
      if (ev.target === els.drawer) {
        closeDrawer();
      }
    });
    $("drawer-close").addEventListener("click", closeDrawer);

    renderFilter();
    load();
    window.setInterval(function () {
      if (!paused) {
        load();
      }
    }, REFRESH_MS);
  }

  function togglePause() {
    paused = !paused;
    els.pauseBtn.textContent = paused ? "Resume" : "Pause";
    if (!paused) {
      load();
    }
  }

  function markFresh() {
    els.refreshState.className = "refresh-dot is-fresh";
    els.lastUpdated.textContent = "updated " + clockTime();
  }

  function markStale(message) {
    els.refreshState.className = "refresh-dot is-stale";
    els.lastUpdated.textContent = "refresh failed: " + message;
  }

  function clockTime() {
    var d = new Date();
    var hh = String(d.getHours()).padStart(2, "0");
    var mm = String(d.getMinutes()).padStart(2, "0");
    var ss = String(d.getSeconds()).padStart(2, "0");
    return hh + ":" + mm + ":" + ss;
  }

  function load() {
    fetch("/api/snapshot")
      .then(function (res) {
        if (!res.ok) {
          throw new Error("HTTP " + res.status);
        }
        return res.json();
      })
      .then(function (snap) {
        jobsById = {};
        (snap.jobs || []).forEach(function (job) {
          jobsById[job.id] = job;
        });
        renderStats(snap.stats || {});
        renderJobs();
        renderEvents(snap.events || []);
        markFresh();
      })
      .catch(function (err) {
        markStale(err.message);
      });
  }

  function renderStats(stats) {
    els.stats.innerHTML = "";
    STATE_ORDER.forEach(function (state) {
      var count = stats[state] || 0;
      var card = el("div", "stat-card stat-" + state);
      card.appendChild(el("span", "stat-count", String(count)));
      card.appendChild(el("span", "stat-label", STATE_LABELS[state]));
      els.stats.appendChild(card);
    });
  }

  function renderFilter() {
    els.filter.innerHTML = "";
    ["all"].concat(STATE_ORDER).forEach(function (name) {
      var btn = el(
        "button",
        "filter-btn" + (name === currentFilter ? " is-active" : ""),
        name === "all" ? "All" : STATE_LABELS[name]
      );
      btn.type = "button";
      btn.dataset.state = name;
      btn.addEventListener("click", function () {
        currentFilter = name;
        renderFilter();
        renderJobs();
      });
      els.filter.appendChild(btn);
    });
  }

  function renderJobs() {
    var jobs = [];
    Object.keys(jobsById).forEach(function (key) {
      jobs.push(jobsById[key]);
    });
    jobs.sort(function (a, b) {
      return String(b.created_at).localeCompare(String(a.created_at));
    });

    var visible =
      currentFilter === "all"
        ? jobs
        : jobs.filter(function (job) {
            return job.state === currentFilter;
          });

    els.jobsBody.innerHTML = "";
    els.jobsEmpty.hidden = visible.length !== 0;

    visible.forEach(function (job) {
      var tr = el("tr", "job-row");
      tr.tabIndex = 0;
      tr.addEventListener("click", function () {
        openJob(job.id);
      });
      tr.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter" || ev.key === " ") {
          openJob(job.id);
        }
      });

      tr.appendChild(el("td", "job-id", shortID(job.id)));
      tr.appendChild(el("td", "", job.kind));
      tr.appendChild(el("td", "", badgeFor(job.state)));
      tr.appendChild(el("td", "num", String(job.priority)));
      tr.appendChild(el("td", "num", job.retry_count + "/" + job.max_attempts));
      tr.appendChild(el("td", "", formatTime(job.created_at)));
      tr.appendChild(el("td", "", job.run_at ? formatTime(job.run_at) : "now"));

      els.jobsBody.appendChild(tr);
    });
  }

  function badgeFor(state) {
    var label = STATE_LABELS[state] || state;
    var node = el("span", "badge badge-" + (state || "pending"), label);
    return node;
  }

  function renderEvents(events) {
    els.events.innerHTML = "";
    if (!events.length) {
      els.events.appendChild(el("li", "empty", "No events yet."));
      return;
    }
    events.forEach(function (event) {
      var li = el("li", "");
      li.appendChild(el("span", "ts", "[" + formatTime(event.timestamp) + "] "));
      li.appendChild(el("span", "event-type", event.event_type));
      if (event.metadata) {
        li.appendChild(el("span", "event-meta", " " + event.metadata));
      }
      els.events.appendChild(li);
    });
  }

  function openJob(id) {
    fetch("/api/jobs/" + encodeURIComponent(id))
      .then(function (res) {
        if (!res.ok) {
          throw new Error("HTTP " + res.status);
        }
        return res.json();
      })
      .then(function (data) {
        renderJobDetail(data.job, data.events || []);
        els.drawer.hidden = false;
      })
      .catch(function (err) {
        window.alert("Could not load job: " + err.message);
      });
  }

  function closeDrawer() {
    els.drawer.hidden = true;
    els.drawerBody.innerHTML = "";
  }

  function renderJobDetail(job, events) {
    els.drawerTitle.textContent = "Job " + shortID(job.id);

    var dl = el("dl", "detail-grid");
    addDetail(dl, "ID", job.id);
    addDetail(dl, "Kind", job.kind);
    addDetail(dl, "State", job.state);
    addDetail(dl, "Priority", String(job.priority));
    addDetail(dl, "Attempts", job.retry_count + "/" + job.max_attempts);
    if (job.idempotency_key) {
      addDetail(dl, "Idempotency", job.idempotency_key);
    }
    addDetail(dl, "Created", formatTime(job.created_at));
    addDetail(dl, "Updated", formatTime(job.updated_at));
    if (job.leased_until) {
      addDetail(dl, "Leased until", formatTime(job.leased_until));
    }
    if (job.run_at) {
      addDetail(dl, "Run at", formatTime(job.run_at));
    }
    addDetail(dl, "Payload", job.payload);

    var section = el("div", "detail-section");
    section.appendChild(el("h3", "", "Event log (" + events.length + ")"));
    var ol = el("ol", "timeline");
    events.forEach(function (event) {
      var li = el("li", "");
      li.appendChild(el("span", "ts", formatTime(event.timestamp) + "  "));
      li.appendChild(el("span", "event-type", event.event_type));
      if (event.metadata) {
        li.appendChild(el("span", "event-meta", "  " + event.metadata));
      }
      ol.appendChild(li);
    });
    section.appendChild(ol);

    els.drawerBody.innerHTML = "";
    els.drawerBody.appendChild(dl);
    els.drawerBody.appendChild(section);
  }

  function addDetail(dl, label, value) {
    dl.appendChild(el("dt", "", label));
    dl.appendChild(el("dd", "", value === null || value === undefined ? "" : String(value)));
  }

  function shortID(id) {
    if (!id) {
      return "";
    }
    return id.length > 8 ? id.slice(0, 8) : id;
  }

  function formatTime(value) {
    if (!value) {
      return "";
    }
    var d = new Date(value);
    if (isNaN(d.getTime())) {
      return String(value);
    }
    return d.toLocaleString();
  }

  document.addEventListener("DOMContentLoaded", init);
})();

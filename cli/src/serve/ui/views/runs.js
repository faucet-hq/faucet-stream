// Runs (#998): the run list beside the selected run's detail. `#/runs` and
// `#/runs/<id>` are one view, so selecting a run keeps the list, its filters
// and its scroll position.
import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml, fmtInt, fmtDuration, fmtCompact, fmtAge, TERMINAL_RUN_STATUSES as TERMINAL } from "../utils.js";
import { attachDatePicker } from "./date-picker.js";
import { formatTs } from "../tz.js";
import { currentTenant, tenantList, withTenant } from "../tenant.js";
import { icon, statStrip, listRow, tabs } from "../components.js";
import { splitPane, sectionList } from "./split.js";
import { mountRunDetail } from "./detail.js";

// Every RunStatus the API accepts — pending/sharded appear in cluster mode.
const STATUSES = ["queued", "pending", "running", "sharded", "completed", "failed", "cancelled"];
// The API's cap on label filters per query.
const MAX_LABEL_FILTERS = 10;
const PAGE = 100;

const TABS = [
  { id: "all", label: "All runs", statuses: [] },
  { id: "active", label: "Active", statuses: ["queued", "pending", "running", "sharded"] },
  { id: "failed", label: "Failed", statuses: ["failed"] },
];

const SECTIONS = [
  ["running", "Running", ["running", "sharded"]],
  ["queued", "Queued", ["queued", "pending"]],
  ["failed", "Failed", ["failed"]],
  ["completed", "Succeeded", ["completed"]],
  ["cancelled", "Cancelled", ["cancelled"]],
];

const RING = {
  running: ["info", "half"],
  sharded: ["info", "half"],
  queued: ["warn", "hollow"],
  pending: ["warn", "hollow"],
  completed: ["ok", "full"],
  failed: ["bad", "full"],
  cancelled: ["muted", "hollow"],
};

export async function renderRuns(container, params = {}) {
  let cursor = null;
  let runs = [];
  let filters = { status: "", name: "", since: "", until: "" };
  // `key:value` / `key` label filters, kept in the URL hash (`#/runs?label=…`).
  const labelFilters = [];
  for (const l of params.query?.getAll("label") || []) {
    if (l && !labelFilters.includes(l)) labelFilters.push(l);
  }
  const statusSel = new Set();
  let tab = "all";
  let pollTimer = null;
  let selected = params.id || null;
  let detailStop = null;
  let alive = true;

  const pane = splitPane(container, {
    name: "Runs",
    listHead: `
      <div class="sp-tabs">
        ${tabs(TABS, tab)}
        <span class="sp-tools">
          <button type="button" class="icon-btn" id="f-toggle" title="Filters" aria-label="Filters" aria-expanded="false">${icon("filter")}<span class="sp-dot" hidden></span></button>
          <button type="button" class="icon-btn" id="f-refresh" title="Refresh" aria-label="Refresh">${icon("refresh")}</button>
          <a class="icon-btn" id="r-submit" href="#/submit" data-perm="config_submit" title="Submit a run" aria-label="Submit a run">${icon("plus")}</a>
        </span>
      </div>
      <div class="sp-filters" id="f-panel" hidden>
        <details class="dd" id="f-status-dd">
          <summary id="f-status-sum">status ▾</summary>
          <div class="dd-menu">
            ${STATUSES.map((s) => `<label class="dd-opt"><input type="checkbox" value="${s}" /> ${s}</label>`).join("")}
          </div>
        </details>
        <input id="f-name" placeholder="name" />
        <input id="f-since" class="date-input" type="text" readonly placeholder="from…" />
        <input id="f-until" class="date-input" type="text" readonly placeholder="to…" />
        <input id="f-label" placeholder="label key:value" title="Filter by label: key:value for an exact value, key for any value. Enter adds it." />
        <button class="btn-ghost" id="f-apply">Apply</button>
        <button class="btn-ghost" id="f-clear">Clear</button>
      </div>
      <div class="label-filters" id="f-labels" hidden></div>
      <div id="r-stats"></div>`,
  });
  const $ = (sel) => pane.head.querySelector(sel);
  const labelBox = $("#f-labels");
  const labelInput = $("#f-label");
  const list = sectionList(pane.body, "faucet.runs.sections");

  function queryString() {
    const p = new URLSearchParams();
    for (const l of labelFilters) p.append("label", l);
    const qs = p.toString();
    return qs ? `?${qs}` : "";
  }
  function hrefFor(id) {
    return `#/runs/${encodeURIComponent(id)}${queryString()}`;
  }
  // Mirror the label filters into the hash without re-rendering the view.
  function syncHash() {
    history.replaceState(null, "", selected ? hrefFor(selected) : `#/runs${queryString()}`);
  }

  function renderLabelFilters() {
    labelBox.hidden = !labelFilters.length;
    labelBox.innerHTML = labelFilters
      .map((l, i) => `<span class="label-filter" title="${escapeHtml(l)}"><span class="label-filter-text">${escapeHtml(l)}</span><button type="button" class="label-filter-x" data-i="${i}" aria-label="Remove label filter ${escapeHtml(l)}">×</button></span>`)
      .join("");
    labelBox.querySelectorAll(".label-filter-x").forEach((b) => {
      b.onclick = () => {
        labelFilters.splice(Number(b.dataset.i), 1);
        renderLabelFilters();
        syncHash();
        load("fresh");
      };
    });
    markFiltered();
  }

  // Returns false (with a toast) for an entry the API would refuse.
  function addLabelFilter(raw) {
    const l = raw.trim();
    if (!l) return true;
    if (l.startsWith(":")) {
      toast("a label filter needs a key: key:value or key", "error");
      return false;
    }
    if (labelFilters.includes(l)) return true;
    if (labelFilters.length >= MAX_LABEL_FILTERS) {
      toast(`at most ${MAX_LABEL_FILTERS} label filters`, "error");
      return false;
    }
    labelFilters.push(l);
    renderLabelFilters();
    syncHash();
    return true;
  }

  function markFiltered() {
    const on = !!(filters.name || filters.since || filters.until || labelFilters.length || (statusSel.size && !TABS.some((t) => t.id === tab && t.statuses.length)));
    $("#f-toggle .sp-dot").hidden = !on;
  }

  labelInput.onkeydown = (e) => {
    if (e.key !== "Enter") return;
    e.preventDefault();
    if (addLabelFilter(labelInput.value)) {
      labelInput.value = "";
      load("fresh");
    }
  };
  renderLabelFilters();
  attachDatePicker($("#f-since"));
  attachDatePicker($("#f-until"));

  const toggle = $("#f-toggle");
  toggle.onclick = () => {
    const panel = $("#f-panel");
    panel.hidden = !panel.hidden;
    toggle.setAttribute("aria-expanded", String(!panel.hidden));
    toggle.classList.toggle("on", !panel.hidden);
  };

  const statusDd = $("#f-status-dd");
  const statusSum = $("#f-status-sum");
  function syncStatusBoxes() {
    statusDd.querySelectorAll("input[type=checkbox]").forEach((cb) => (cb.checked = statusSel.has(cb.value)));
    statusSum.textContent = statusSel.size ? `status (${statusSel.size}) ▾` : "status ▾";
  }
  statusDd.querySelectorAll("input[type=checkbox]").forEach((cb) => {
    cb.onchange = () => {
      if (cb.checked) statusSel.add(cb.value);
      else statusSel.delete(cb.value);
      statusSum.textContent = statusSel.size ? `status (${statusSel.size}) ▾` : "status ▾";
    };
  });
  const closeStatusDd = (e) => {
    if (statusDd.open && !statusDd.contains(e.target)) statusDd.open = false;
  };
  document.addEventListener("click", closeStatusDd);

  function setTab(id) {
    tab = id;
    pane.head.querySelectorAll(".c-tab").forEach((b) => {
      const on = b.dataset.tab === id;
      b.classList.toggle("on", on);
      b.setAttribute("aria-selected", String(on));
    });
  }
  pane.head.querySelectorAll(".c-tab").forEach((b) => {
    b.onclick = () => {
      const t = TABS.find((x) => x.id === b.dataset.tab);
      setTab(t.id);
      statusSel.clear();
      t.statuses.forEach((s) => statusSel.add(s));
      syncStatusBoxes();
      filters.status = [...statusSel].join(",");
      markFiltered();
      load("fresh");
    };
  });
  function tabForStatuses() {
    const key = [...statusSel].sort().join(",");
    const t = TABS.find((x) => [...x.statuses].sort().join(",") === key);
    setTab(t ? t.id : "");
  }

  // `fresh` starts over at one page; a refresh re-reads every row already
  // shown, so rows don't vanish; `more` appends the next page.
  function query(mode) {
    const p = new URLSearchParams();
    if (filters.status) p.set("status", filters.status);
    if (filters.name) p.set("name", filters.name);
    if (filters.since) p.set("since", new Date(filters.since).toISOString());
    if (filters.until) p.set("until", new Date(filters.until).toISOString());
    p.set("limit", String(mode === "refresh" ? Math.min(500, Math.max(PAGE, runs.length)) : PAGE));
    if (mode === "more" && cursor) p.set("cursor", cursor);
    for (const l of labelFilters) p.append("label", l);
    withTenant(p);
    return p.toString();
  }

  let seq = 0;
  async function load(mode) {
    if (mode === "more" && !cursor) return;
    const mine = ++seq;
    try {
      const data = await api(`/v1/runs?${query(mode)}`);
      if (!alive || mine !== seq) return;
      runs = mode === "more" ? runs.concat(data.runs) : data.runs;
      cursor = data.next_cursor || null;
      paint();
      maybePoll();
    } catch (e) {
      if (!alive || mine !== seq) return;
      toast(e.message, "error");
      maybePoll(true);
    }
  }

  function maybePoll(retry = false) {
    clearTimeout(pollTimer);
    if (!alive) return;
    if (retry || runs.some((r) => !TERMINAL.includes(r.status))) pollTimer = setTimeout(() => load("refresh"), 3000);
    else pollTimer = setTimeout(() => load("refresh"), 15000);
  }

  function paint() {
    const more = cursor ? "+" : "";
    const count = (sts) => runs.filter((r) => sts.includes(r.status)).length;
    const records = runs.reduce((n, r) => n + (r.records_written || 0), 0);
    $("#r-stats").innerHTML = statStrip([
      { label: "Running", value: `${fmtInt(count(["running", "sharded"]))}${more}`, tone: "info" },
      { label: "Succeeded", value: `${fmtInt(count(["completed"]))}${more}`, tone: "ok" },
      { label: "Failed", value: `${fmtInt(count(["failed"]))}${more}`, tone: count(["failed"]) ? "bad" : "" },
      { label: "Records", value: `${fmtCompact(records)}${more}` },
    ]);
    const now = Date.now();
    const filtered = !!(filters.status || filters.name || filters.since || filters.until || labelFilters.length);
    const empty = filtered
      ? `<div class="sp-empty"><b>No runs match these filters.</b><span>Clear the filters, or widen the date range.</span></div>`
      : `<div class="sp-empty"><b>No runs yet.</b><span>Submit a pipeline config on the <a href="#/submit">Submit</a> page, trigger a template, or <code>POST /v1/runs</code>.</span></div>`;
    list.render(
      SECTIONS.map(([id, label, sts]) => {
        const rows = runs.filter((r) => sts.includes(r.status));
        return { id, label, count: rows.length, rows: rows.map((r) => runRow(r, now)) };
      }),
      {
        empty,
        footer: cursor ? `<button type="button" class="btn-ghost load-more" id="r-more">Load more</button>` : "",
      },
    );
  }

  function runRow(r, now) {
    const [tone, shape] = RING[r.status] || ["muted", "full"];
    const facts = rowFacts(r);
    return listRow({
      id: r.run_id,
      href: hrefFor(r.run_id),
      tone,
      shape,
      title: runTitle(r),
      tooltip: runTitle(r) !== r.run_id ? `${runTitle(r)}\n${r.run_id}` : r.run_id,
      age: fmtAge(r.submitted_at, now),
      ageTitle: `submitted ${fmtTime(r.submitted_at)}`,
      metaLead: facts[0],
      meta: facts.slice(1).join(" · "),
      selected: r.run_id === selected,
    });
  }

  pane.body.addEventListener("click", (e) => {
    if (e.target.closest("#r-more")) load("more");
  });

  $("#f-apply").onclick = () => {
    if (!addLabelFilter(labelInput.value)) return;
    labelInput.value = "";
    filters = {
      status: [...statusSel].join(","),
      name: $("#f-name").value.trim(),
      since: $("#f-since").dataset.value || "",
      until: $("#f-until").dataset.value || "",
    };
    tabForStatuses();
    markFiltered();
    load("fresh");
  };
  $("#f-clear").onclick = () => {
    statusSel.clear();
    syncStatusBoxes();
    $("#f-name").value = "";
    for (const id of ["#f-since", "#f-until"]) {
      const el = $(id);
      el.value = "";
      delete el.dataset.value;
    }
    labelFilters.length = 0;
    labelInput.value = "";
    filters = { status: "", name: "", since: "", until: "" };
    setTab("all");
    renderLabelFilters();
    syncHash();
    load("fresh");
  };
  $("#f-refresh").onclick = () => load("refresh");

  function showDetail(id) {
    detailStop?.();
    detailStop = null;
    selected = id;
    pane.setSelected(!!id);
    list.select(id);
    if (!id) {
      pane.detail.innerHTML = runs.length
        ? `<div class="sp-none"><b>Select a run</b><span>Pick a run on the left to see its invocations, row accounting and live logs.</span></div>`
        : `<div class="sp-none"><b>Nothing to show yet</b><span>Runs appear here as soon as one is submitted.</span></div>`;
      return;
    }
    detailStop = mountRunDetail(pane.detail, id, {
      backHref: `#/runs${queryString()}`,
      onLabel: (l) => {
        if (addLabelFilter(l)) load("fresh");
      },
      onChange: () => load("refresh"),
      onDeleted: () => {
        runs = runs.filter((r) => r.run_id !== id);
        navigate(`#/runs${queryString()}`);
        load("refresh");
      },
    });
  }

  await load("fresh");
  if (!alive) return;
  showDetail(selected);

  return {
    teardown() {
      alive = false;
      clearTimeout(pollTimer);
      detailStop?.();
      document.removeEventListener("click", closeStatusDd);
    },
    update(p) {
      const ls = [...new Set((p.query?.getAll("label") || []).filter(Boolean))];
      if (ls.join("\n") !== labelFilters.join("\n")) {
        labelFilters.splice(0, labelFilters.length, ...ls);
        renderLabelFilters();
        load("fresh");
      }
      const id = p.id || null;
      if (id === selected) return;
      showDetail(id);
      if (!id) paint();
    },
  };
}

/** A run's display name: its name, else the pipeline it ran, else its id. */
export function runTitle(r) {
  return r.name || (r.labels || {}).pipeline || r.run_id;
}

/** `rest → file` from the first invocation's usage record, when it has one. */
export function runKinds(r) {
  const u = (r.invocations || []).find((i) => i.usage && i.usage.source_kind)?.usage;
  return u ? `${u.source_kind} → ${u.sink_kind}` : "";
}

// The facts a row's meta line shows, most telling first.
function rowFacts(r) {
  const l = r.labels || {};
  const facts = [];
  const recs = `${fmtInt(r.records_written ?? 0)} records`;
  const invErr = (r.invocations || []).find((i) => i.error && !i.skipped)?.error;
  if (r.status === "failed") facts.push(firstLine(invErr || r.error) || "failed");
  else if (l.template) facts.push(`template ${l.template}${l.template_version ? `@v${l.template_version}` : ""}`);
  else if (l.backfill_unit) facts.push(`backfill ${l.backfill_unit}`);
  else if (l.dlq_replay) facts.push("DLQ replay");
  else if (runKinds(r)) facts.push(runKinds(r));
  else facts.push(recs);
  if (r.status !== "failed" && runKinds(r) && !facts.includes(runKinds(r))) facts.push(runKinds(r));
  if (facts[0] !== recs) facts.push(recs);
  const inv = (r.invocations || []).length;
  if (inv > 1) facts.push(`${fmtInt(inv)} invocations`);
  if (r.elapsed_secs != null) facts.push(r.elapsed_secs < 1 ? "<1s" : fmtDuration(r.elapsed_secs));
  if (r.pool && r.pool !== "default") facts.push(`pool ${r.pool}`);
  if (r.tenant && !currentTenant()) {
    const t = (tenantList() || []).find((x) => x.id === r.tenant);
    facts.push(t?.name || r.tenant);
  }
  if (r.attempt) facts.push(`attempt ${r.attempt + 1}`);
  return facts;
}

function firstLine(s) {
  return String(s || "").split("\n")[0].trim();
}

export function fmtTime(s) {
  return formatTs(s); // formats in the user's selected display timezone (default local)
}

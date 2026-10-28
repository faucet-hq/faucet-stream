import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml, fmtInt, fmtDuration, fmtCompact, labelChip, sortedLabels, TERMINAL_RUN_STATUSES as TERMINAL } from "../utils.js";
import { attachDatePicker } from "./date-picker.js";
import { formatTs } from "../tz.js";
import { currentTenant, tenantList, withTenant } from "../tenant.js";

// Every RunStatus the API accepts — pending/sharded appear in cluster mode.
const STATUSES = ["queued", "pending", "running", "sharded", "completed", "failed", "cancelled"];

// Label chips shown per row before the rest collapse into "+N".
const ROW_LABELS = 3;
// The API's cap on label filters per query.
const MAX_LABEL_FILTERS = 10;

export async function renderRuns(container, params = {}) {
  let cursor = null;
  let filters = { status: "", name: "", since: "", until: "" };
  // `key:value` / `key` label filters, kept in the URL hash (`#/runs?label=…`).
  const labelFilters = [];
  for (const l of params.query?.getAll("label") || []) {
    if (l && !labelFilters.includes(l)) labelFilters.push(l);
  }
  const statusSel = new Set(); // selected statuses (empty = all)
  let pollTimer = null;

  container.innerHTML = `
    <div class="page">
      <div class="page-head">
        <h1>Runs</h1>
        <button class="btn-primary" id="r-submit" data-perm="config_submit">+ Submit run</button>
      </div>
      <div class="filters">
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
        <button class="btn-ghost" id="f-refresh">↻</button>
        <div class="label-filters" id="f-labels" hidden></div>
      </div>
      <table class="ds-table runs-table">
        <thead><tr>
          <th>status</th><th>name</th><th>duration</th><th>rows</th><th>submitted</th>
        </tr></thead>
        <tbody id="runs-list"></tbody>
      </table>
      <button class="btn-ghost load-more" id="r-more" hidden>Load more</button>
    </div>`;

  const list = container.querySelector("#runs-list");
  const labelBox = container.querySelector("#f-labels");
  const labelInput = container.querySelector("#f-label");

  // Mirror the label filters into the hash without re-rendering the view.
  function syncHash() {
    const p = new URLSearchParams();
    for (const l of labelFilters) p.append("label", l);
    const qs = p.toString();
    history.replaceState(null, "", qs ? `#/runs?${qs}` : "#/runs");
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
        load(true);
      };
    });
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

  labelInput.onkeydown = (e) => {
    if (e.key !== "Enter") return;
    e.preventDefault();
    if (addLabelFilter(labelInput.value)) {
      labelInput.value = "";
      load(true);
    }
  };

  // A label chip in a row filters by it instead of opening the run.
  list.addEventListener("click", (e) => {
    const chip = e.target.closest(".label-chip");
    if (!chip) return;
    e.stopPropagation();
    if (addLabelFilter(chip.dataset.label)) load(true);
  });
  renderLabelFilters();
  container.querySelector("#r-submit").onclick = () => navigate("#/submit");
  attachDatePicker(container.querySelector("#f-since"));
  attachDatePicker(container.querySelector("#f-until"));

  // Multi-select status filter: check any combination (empty = all). Applied on
  // the Apply button, alongside name/date, so one round-trip covers every filter.
  const statusDd = container.querySelector("#f-status-dd");
  const statusSum = container.querySelector("#f-status-sum");
  statusDd.querySelectorAll("input[type=checkbox]").forEach((cb) => {
    cb.onchange = () => {
      if (cb.checked) statusSel.add(cb.value);
      else statusSel.delete(cb.value);
      statusSum.textContent = statusSel.size ? `status (${statusSel.size}) ▾` : "status ▾";
    };
  });
  // Named handler so the view teardown can remove it — a per-render anonymous
  // listener would accumulate on `document` across navigations.
  const closeStatusDd = (e) => {
    if (statusDd.open && !statusDd.contains(e.target)) statusDd.open = false;
  };
  document.addEventListener("click", closeStatusDd);

  function query(reset) {
    if (reset) cursor = null;
    const p = new URLSearchParams();
    if (filters.status) p.set("status", filters.status);
    if (filters.name) p.set("name", filters.name);
    if (filters.since) p.set("since", new Date(filters.since).toISOString());
    if (filters.until) p.set("until", new Date(filters.until).toISOString());
    p.set("limit", "50");
    if (cursor) p.set("cursor", cursor);
    for (const l of labelFilters) p.append("label", l);
    withTenant(p);
    return p.toString();
  }

  async function load(reset) {
    if (!reset && !cursor) return; // no next page — don't re-fetch page 1 into the list
    try {
      const data = await api(`/v1/runs?${query(reset)}`);
      if (reset) list.innerHTML = "";
      if (!data.runs.length && reset)
        list.innerHTML = `<tr><td colspan="5" class="empty">${labelFilters.length ? "No runs carry these labels." : "No runs yet."}</td></tr>`;
      for (const r of data.runs) list.appendChild(row(r));
      cursor = data.next_cursor || null;
      container.querySelector("#r-more").hidden = !cursor;
      maybePoll(data.runs);
    } catch (e) {
      toast(e.message, "error");
    }
  }

  function maybePoll(runs) {
    const active = runs.some((r) => !TERMINAL.includes(r.status));
    clearTimeout(pollTimer);
    if (active) pollTimer = setTimeout(() => load(true), 3000);
  }

  container.querySelector("#f-apply").onclick = () => {
    if (!addLabelFilter(labelInput.value)) return;
    labelInput.value = "";
    filters = {
      status: [...statusSel].join(","),
      name: container.querySelector("#f-name").value.trim(),
      since: container.querySelector("#f-since").dataset.value || "",
      until: container.querySelector("#f-until").dataset.value || "",
    };
    load(true);
  };
  container.querySelector("#f-refresh").onclick = () => load(true);
  container.querySelector("#r-more").onclick = () => load(false);

  await load(true);
  return () => {
    clearTimeout(pollTimer);
    document.removeEventListener("click", closeStatusDd);
  }; // teardown
}

// Which tenant a run belongs to, shown while the list is not scoped to one.
function tenantTag(r) {
  if (!r.tenant || currentTenant()) return "";
  const t = (tenantList() || []).find((x) => x.id === r.tenant);
  const label = t?.name || r.tenant;
  return ` <span class="run-tenant" title="tenant ${escapeHtml(r.tenant)}">${escapeHtml(label)}</span>`;
}

// Up to ROW_LABELS chips, caller labels first; the rest fold into "+N".
function rowLabels(r) {
  const all = sortedLabels(r.labels);
  if (!all.length) return "";
  const shown = all.slice(0, ROW_LABELS).map(([k, v, p]) => labelChip(k, v, p)).join("");
  const rest = all.slice(ROW_LABELS);
  const more = rest.length
    ? `<span class="label-more" title="${escapeHtml(rest.map(([k, v]) => `${k}: ${v}`).join("\n"))}">+${rest.length}</span>`
    : "";
  return `<div class="run-labels">${shown}${more}</div>`;
}

function row(r) {
  const el = document.createElement("tr");
  el.className = "ds-tr";
  el.onclick = () => navigate(`#/runs/${r.run_id}`);
  const secs = r.elapsed_secs;
  const elapsed = secs != null ? fmtDuration(secs) : "—";
  const elapsedExact = secs != null ? `${secs.toFixed(1)}s` : "";
  const recs = r.records_written ?? 0;
  el.innerHTML = `
    <td><span class="pill pill-${r.status}">${r.status}</span></td>
    <td class="ds-uri"><div class="ds-uri-in">${escapeHtml(r.name || r.run_id)}${tenantTag(r)}</div>${rowLabels(r)}</td>
    <td class="ds-meta ds-num" title="${elapsedExact}">${elapsed}</td>
    <td class="ds-meta ds-num" title="${fmtInt(recs)}">${fmtCompact(recs)}</td>
    <td class="ds-meta ds-time">${fmtTime(r.submitted_at)}</td>`;
  return el;
}

export function fmtTime(s) {
  return formatTs(s); // formats in the user's selected display timezone (default local)
}

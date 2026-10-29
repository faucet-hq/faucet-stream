// Cost & usage accounting (#704): what the server's runs moved — records,
// estimated bytes, backend round trips — and what that is estimated to cost,
// grouped by pipeline / row / dataset / sink / day. Every figure is an
// estimate priced from each run's `usage:` table; the page says so.
import { api } from "../api.js";
import { escapeHtml, fmtInt, fmtCompact } from "../utils.js";
import { icon, statStrip } from "../components.js";
import { catalogUnavailable } from "./datasets.js";
import { withTenant, tenantList } from "../tenant.js";

const BASE_GROUPS = ["pipeline", "row", "dataset", "sink", "day"];
// `tenant` is offered only where the server has tenants (#709).
const groups = () => (tenantList() ? [...BASE_GROUPS, "tenant"] : BASE_GROUPS);

export function fmtBytes(b) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = Number(b) || 0;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return i === 0 ? `${fmtInt(v)} B` : `${v.toFixed(1)} ${units[i]}`;
}

function fmtMoney(v, currency) {
  const n = Number(v) || 0;
  const digits = n !== 0 && n < 0.01 ? 4 : 2;
  return `${escapeHtml(currency)} ${n.toFixed(digits)}`;
}

function fmtSecs(ms) {
  const s = (Number(ms) || 0) / 1000;
  return s >= 60 ? `${(s / 60).toFixed(1)} min` : `${s.toFixed(1)} s`;
}

export async function renderUsage(container) {
  container.innerHTML = `
    <div class="page p3">
      <div class="page-head">
        <h1>Usage</h1>
      </div>
      <p class="p3-sub">What the server's runs moved and what that is estimated to cost — every figure is an estimate.</p>
      <div class="filters filters-1line usage-filters">
        <label class="usage-by">by
          <select id="u-by">${groups().map((g) => `<option value="${g}">${g}</option>`).join("")}</select>
        </label>
        <input id="u-pipeline" placeholder="pipeline" />
        <input id="u-from" class="date-input" type="text" placeholder="from YYYY-MM-DD" />
        <input id="u-to" class="date-input" type="text" placeholder="to YYYY-MM-DD" />
        <button class="icon-btn" id="u-refresh" title="Refresh" aria-label="Refresh">${icon("refresh")}</button>
      </div>
      <div id="u-summary"></div>
      <section class="c-card">
        <div class="c-card-h">${icon("usage")}<b>By <span id="u-by-label">pipeline</span></b><span class="c-card-aside" id="u-window"></span></div>
        <div class="d-scroll-x"><table class="usage-table">
          <thead><tr>
            <th id="u-key-head">pipeline</th><th>runs</th><th>rows in</th><th>rows out</th>
            <th>bytes out</th><th>duration</th><th>requests</th><th>est. cost</th><th>hosted eq.</th>
          </tr></thead>
          <tbody id="u-rows"></tbody>
          <tfoot id="u-total"></tfoot>
        </table></div>
      <div class="c-card-note usage-note">Estimates use each run's <code>usage.pricing</code> table (public list prices by default,
        labelled as estimates). <em>hosted eq.</em> is what a per-row-priced hosted ELT service would charge for the
        same rows written. A connector that reports no cost signal is listed as “compute not reported”, never as zero.</div>
      </section>
    </div>`;

  const by = container.querySelector("#u-by");
  const pipeline = container.querySelector("#u-pipeline");
  const from = container.querySelector("#u-from");
  const to = container.querySelector("#u-to");
  const rows = container.querySelector("#u-rows");
  const total = container.querySelector("#u-total");
  const summary = container.querySelector("#u-summary");
  const keyHead = container.querySelector("#u-key-head");

  async function load() {
    rows.innerHTML = `<tr><td colspan="9" class="empty">loading…</td></tr>`;
    total.innerHTML = "";
    const p = new URLSearchParams();
    p.set("by", by.value);
    if (pipeline.value.trim()) p.set("pipeline", pipeline.value.trim());
    if (from.value.trim()) p.set("since", from.value.trim());
    if (to.value.trim()) p.set("until", to.value.trim());
    withTenant(p);
    let data;
    try {
      data = await api(`/v1/usage?${p}`);
    } catch (e) {
      rows.innerHTML = `<tr><td colspan="9" class="empty">${
        catalogUnavailable(e)
          ? "Usage accounting is not available on this server (faucet was built without the `catalog` feature)."
          : escapeHtml(e.message)
      }</td></tr>`;
      return;
    }
    const r = data.report;
    keyHead.textContent = r.by;
    container.querySelector("#u-by-label").textContent = r.by;
    container.querySelector("#u-window").textContent = `${fmtInt(r.rows.length)} group${r.rows.length === 1 ? "" : "s"}${r.since || r.until ? ` · ${r.since ? r.since.slice(0, 10) : "…"} → ${r.until ? r.until.slice(0, 10) : "now"}` : ""}`;
    summary.innerHTML = statStrip([
      { label: "Invocations", value: r.records },
      { label: "Rows written", value: fmtCompact(r.total.records_written) },
      { label: "Bytes written", value: fmtBytes(r.total.bytes_written) },
      { label: "Estimated cost", value: fmtMoney(r.total.cost, r.currency) },
      { label: "Hosted equivalent", value: fmtMoney(r.total.hosted_equivalent, r.currency) },
    ]);
    if (!r.rows.length) {
      rows.innerHTML = `<tr><td colspan="9" class="empty">No usage recorded for this window — run a pipeline first.</td></tr>`;
      return;
    }
    const line = (row, cls = "") => {
      // `not_reported` is omitted from the JSON when empty.
      const nr = row.not_reported || [];
      return `<tr class="${cls}">
      <td class="usage-key" title="${escapeHtml(row.key)}">${escapeHtml(row.key)}${
        nr.length
          ? `<span class="usage-nr" title="compute not reported by ${escapeHtml(nr.join(", "))}">compute not reported</span>`
          : ""
      }</td>
      <td>${fmtInt(row.runs)}${row.failed_runs ? ` <span class="usage-failed">(${fmtInt(row.failed_runs)} failed)</span>` : ""}</td>
      <td>${fmtInt(row.records_read)}</td>
      <td>${fmtInt(row.records_written)}</td>
      <td>${fmtBytes(row.bytes_written)}</td>
      <td>${fmtSecs(row.duration_ms)}</td>
      <td>${fmtInt(row.roundtrips)}</td>
      <td>${fmtMoney(row.cost, r.currency)}</td>
      <td>${fmtMoney(row.hosted_equivalent, r.currency)}</td>
    </tr>`;
    };
    rows.innerHTML = r.rows.map((row) => line(row)).join("");
    total.innerHTML = line(r.total, "usage-total");
  }

  container.querySelector("#u-refresh").onclick = load;
  by.onchange = load;
  for (const el of [pipeline, from, to]) {
    el.onchange = load;
    el.onkeydown = (e) => {
      if (e.key === "Enter") load();
    };
  }
  await load();
}

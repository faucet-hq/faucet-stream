// Usage (#998, #704): canvas report — what runs moved and what it is estimated
// to cost, grouped by pipeline / row / dataset / sink / day / tenant. Filters,
// group and sort live in the URL; every figure is an estimate and says so.
import { api } from "../api.js";
import { html, raw, v, ic, ibtn, empty, alert, skelCards, head, stats, card, tbl, chip, kv, grid, btn, lnk, input, spark, tag } from "../components.js";
import { layout, setQuery, patch, errorAlert, hashFor } from "../page.js";
import { setCrumbs, setContext } from "../shell.js";
import { fmtInt, fmtCompact } from "../utils.js";
import { href, datasetName } from "../links.js";
import { withTenant, tenantList } from "../tenant.js";

const BASE_GROUPS = [["pipeline", "Pipeline"], ["row", "Row"], ["dataset", "Dataset"], ["sink", "Sink"], ["day", "Day"]];
const groups = () => (tenantList() ? [...BASE_GROUPS, ["tenant", "Tenant"]] : BASE_GROUPS);
const COLS = [
  ["key", "", ""],
  ["runs", "Runs", "num"],
  ["records_read", "Rows in", "num"],
  ["records_written", "Rows out", "num"],
  ["bytes_written", "Bytes out", "num"],
  ["duration_ms", "Duration", "num"],
  ["roundtrips", "Requests", "num"],
  ["cost", "Est. cost", "num"],
  ["hosted_equivalent", "Hosted eq.", "num"],
];
const NOTE = html`Estimates use each run’s <code>usage.pricing</code> table (public list prices by default). <b>Hosted eq.</b> is what a per-row-priced hosted ELT service would charge for the same rows. A connector that reports no compute signal is marked, never priced as zero.`;

export function fmtBytes(b) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let x = Number(b) || 0;
  let i = 0;
  while (x >= 1024 && i < units.length - 1) { x /= 1024; i += 1; }
  return i === 0 ? `${fmtInt(x)} B` : `${x.toFixed(i >= 3 ? 2 : 1)} ${units[i]}`;
}
function money(x, cur) {
  const n = Number(x) || 0;
  const digits = n !== 0 && Math.abs(n) < 0.01 ? 4 : 2;
  const s = n.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
  return cur === "USD" || !cur ? `$${s}` : `${cur} ${s}`;
}
function dur(ms) {
  const n = Number(ms) || 0;
  if (n > 0 && n < 1000) return "<1s";
  const s = Math.round(n / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(s % 60).padStart(2, "0")}s`;
  return `${Math.floor(m / 60)}h ${String(m % 60).padStart(2, "0")}m`;
}
const isoDay = (d) => d.toISOString().slice(0, 10);
const daysAgo = (n) => isoDay(new Date(Date.now() - n * 86400000));
const prettyDay = (s) => (s ? new Date(`${s}T00:00:00Z`).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" }) : "");

export async function renderUsage(host, params) {
  const L = layout(host, "canvas");
  const read = (p) => ({
    by: p.query.get("by") || "pipeline",
    pipeline: p.query.get("pipeline") || "",
    since: p.query.has("since") ? p.query.get("since") : daysAgo(29),
    until: p.query.get("until") || "",
    sort: p.query.get("sort") || "cost",
    dir: p.query.get("dir") || "desc",
    tenant: p.query.get("tenant") || "",
  });
  let state = read(params);
  let report = null;
  let err = null;
  let trend = null; // per-day rows for the drilled pipeline, or for every pipeline
  let off = false;
  let gen = 0;

  function apiQuery(extra = {}) {
    const p = new URLSearchParams({ by: state.by });
    // Drilling into a pipeline keeps every pipeline in the table, the drilled one selected.
    if (state.pipeline && !(state.by === "pipeline" && !("by" in extra))) p.set("pipeline", state.pipeline);
    if (state.since) p.set("since", state.since);
    if (state.until) p.set("until", state.until);
    if (state.tenant) p.set("tenant", state.tenant); else withTenant(p);
    for (const [k, x] of Object.entries(extra)) p.set(k, x);
    return p;
  }

  function filters() {
    const seg = html`<span class="seg" role="tablist">${groups().map(([val, label]) => html`<span role="tab" tabindex="0" aria-selected="${String(val === state.by)}" class="${val === state.by ? "on" : ""}" data-by="${val}">${label}</span>`)}</span>`;
    const drill = state.pipeline ? html`<div class="note" style="display:flex;gap:6px;align-items:center">Showing ${chip(`pipeline: ${state.pipeline}`, "x", { "data-clear": "pipeline", role: "button", title: "Remove this filter" })}${state.tenant ? chip(`tenant: ${state.tenant}`, "x", { "data-clear": "tenant", role: "button", title: "Remove this filter" }) : ""}<a class="lnk" href="#" data-clear="all" style="margin-left:6px">clear</a></div>` : state.tenant ? html`<div class="note" style="display:flex;gap:6px;align-items:center">Showing ${chip(`tenant: ${state.tenant}`, "x", { "data-clear": "tenant", role: "button", title: "Remove this filter" })}</div>` : "";
    return html`<form class="card" id="us-filters"><div class="pad us-bar"><span class="muted">Group by</span>${seg}
      <label class="input" style="flex:1;min-width:180px">${ic("search")}<input type="search" name="pipeline" placeholder="Pipeline name" value="${state.pipeline}" autocomplete="off"></label>
      ${input(state.since, { name: "since", type: "date", a: { "aria-label": "Since", title: "Since (inclusive)" } })}<span class="muted">→</span>${input(state.until, { name: "until", type: "date", a: { "aria-label": "Until", title: "Until (inclusive)" } })}
      ${ibtn("refresh", { title: "Refresh", data: { reload: "1" } })}${ibtn("download", { title: "Download this table as CSV", data: { csv: "1" } })}</div>${drill}</form>`;
  }

  const statRow = () => {
    if (!report) return raw(`<div class="stats" style="--n:5">${'<div><span class="skel w60"></span><span class="skel w30" style="margin-top:8px;height:16px"></span></div>'.repeat(5)}</div>`);
    const t = report.total || {};
    return stats([["Invocations", fmtInt(report.records || 0)], ["Rows written", fmtCompact(t.records_written || 0)], ["Bytes written", fmtBytes(t.bytes_written || 0)], ["Estimated cost", money(t.cost, report.currency)], ["Hosted equivalent", money(t.hosted_equivalent, report.currency)]]);
  };

  function keyCell(r) {
    const k = r.key;
    const nr = (r.not_reported || []).length ? html` <span class="tag" title="${`compute not reported by ${r.not_reported.join(", ")}: never priced as zero`}">compute not reported</span>` : "";
    let cell;
    if (state.by === "pipeline") cell = lnk(k, hashFor(href.usage(), { ...urlState(), pipeline: k }), false, `Drill into ${k}`);
    else if (state.by === "row") cell = lnk(k, href.runs({ name: String(k).split("::")[0] }), true, "Runs of this pipeline");
    else if (state.by === "dataset") cell = lnk(datasetName(k), href.datasets({ q: k }), false, k);
    else if (state.by === "sink") cell = lnk(k, href.schema("sink", k), true);
    else if (state.by === "day") cell = lnk(prettyDay(k), hashFor(href.usage(), { ...urlState(), by: "pipeline", since: k, until: k }), false, `Pipelines on ${k}`);
    else if (state.by === "tenant") cell = k && !String(k).startsWith("(") ? lnk(k, href.tenant(k)) : html`<span class="muted">${k}</span>`;
    else cell = html`${k}`;
    return html`${cell}${nr}`;
  }
  const cellOf = (r, col, cur) => {
    switch (col) {
      case "runs": return html`${fmtInt(r.runs)}${r.failed_runs ? html` <span class="bad">· ${fmtInt(r.failed_runs)} failed</span>` : ""}`;
      case "records_read": case "records_written": return fmtCompact(r[col] || 0);
      case "bytes_written": return fmtBytes(r.bytes_written);
      case "duration_ms": return dur(r.duration_ms);
      case "roundtrips": return r.roundtrips ? fmtInt(r.roundtrips) : "—";
      case "cost": case "hosted_equivalent": return money(r[col], cur);
      default: return "";
    }
  };
  function sorted(rows) {
    const k = state.sort === "key" ? "key" : state.sort;
    const dir = state.dir === "asc" ? 1 : -1;
    return rows.slice().sort((a, b) => {
      const x = a[k], y = b[k];
      if (typeof x === "number" && typeof y === "number") return (x - y) * dir;
      return String(x).localeCompare(String(y)) * dir;
    });
  }
  function table(selKey = "") {
    const cur = report.currency;
    const label = (groups().find(([g]) => g === state.by) || [state.by, state.by])[1];
    const th = COLS.map(([k, l, c]) => {
      const text = k === "key" ? label : l;
      const on = state.sort === k;
      return html`<th class="${c} sortable${on ? " sorted" : ""}" data-sort="${k}" aria-sort="${on ? (state.dir === "asc" ? "ascending" : "descending") : "none"}" title="Sort by ${text}">${text}${on ? (state.dir === "asc" ? " ↑" : " ↓") : ""}</th>`;
    });
    const rows = sorted(report.rows || []).map((r) => html`<tr class="${String(r.key) === selKey ? "sel" : ""}">${COLS.map(([k, , c]) => html`<td class="${c}">${k === "key" ? keyCell(r) : cellOf(r, k, cur)}</td>`)}</tr>`);
    const t = report.total || {};
    const foot = html`<tfoot><tr><td>Total · ${fmtInt((report.rows || []).length)} ${label.toLowerCase()}${(report.rows || []).length === 1 ? "" : "s"}</td>${COLS.slice(1).map(([k, , c]) => html`<td class="${c}">${cellOf(t, k, cur)}</td>`)}</tr></tfoot>`;
    return html`<div class="tbl-wrap"><table class="us-tbl"><thead><tr>${th}</tr></thead><tbody>${rows}</tbody>${foot}</table></div>`;
  }
  const windowText = () => `${fmtInt((report.rows || []).length)} group${(report.rows || []).length === 1 ? "" : "s"} · ${state.since ? prettyDay(state.since) : "all time"} → ${state.until ? prettyDay(state.until) : "today"}`;

  function dayBars(rows) {
    const got = rows.slice().sort((a, b) => String(a.key).localeCompare(String(b.key)));
    if (!got.length) return "";
    // One slot per day of the window, so a quiet day shows as a gap.
    const byKey = new Map(got.map((r) => [String(r.key), r]));
    const start = new Date(`${state.since || got[0].key}T00:00:00Z`);
    const end = new Date(`${state.until || isoDay(new Date())}T00:00:00Z`);
    const pts = [];
    for (let d = start; d <= end && pts.length < 92; d = new Date(d.getTime() + 86400000)) {
      const k = isoDay(d);
      pts.push(byKey.get(k) || { key: k, records_written: 0 });
    }
    if (!pts.length) pts.push(...got);
    const max = Math.max(...pts.map((r) => r.records_written || 0), 1);
    const n = pts.length;
    const w = 900 / n;
    const bars = pts.map((r, i) => {
      const h = Math.max(2, ((r.records_written || 0) / max) * 106);
      return `<rect x="${(i * w + w * 0.12).toFixed(1)}" y="${(110 - h).toFixed(1)}" width="${(w * 0.76).toFixed(1)}" height="${h.toFixed(1)}" rx="2" class="${i === n - 1 ? "us-bar-last" : "us-bar-col"}"><title>${prettyDay(r.key)}: ${fmtInt(r.records_written || 0)} rows</title></rect>`;
    }).join("");
    const peak = pts.reduce((a, b) => ((b.records_written || 0) > (a.records_written || 0) ? b : a));
    return card({ icon: "usage", title: "Rows written per day", right: `peak ${fmtCompact(peak.records_written || 0)} on ${prettyDay(peak.key)}`, body: html`<div class="pad"><svg width="100%" height="120" viewBox="0 0 900 120" preserveAspectRatio="none" style="display:block" role="img" aria-label="Rows written per day">${raw(bars)}</svg><div class="us-axis"><span>${prettyDay(pts[0].key)}</span>${n > 2 ? html`<span>${prettyDay(pts[Math.floor(n / 2)].key)}</span>` : ""}<span>${prettyDay(pts[n - 1].key)}</span></div></div>` });
  }

  function drillCards() {
    const r = (report.rows || []).find((x) => x.key === state.pipeline);
    if (!r) return "";
    const days = (trend || []).slice().sort((a, b) => String(a.key).localeCompare(String(b.key)));
    const rowsPts = days.map((d) => d.records_written || 0);
    const costPts = days.map((d) => d.cost || 0);
    const summary = card({
      icon: "runs", title: r.key, right: lnk(`${fmtInt(r.runs)} run${r.runs === 1 ? "" : "s"}`, href.runs({ name: r.key })),
      body: grid(2, [kv("Rows written", fmtCompact(r.records_written || 0), rowsPts.length > 1 ? spark(rowsPts, "var(--text-2)", 170, 30) : ""), kv("Estimated cost", money(r.cost, report.currency), costPts.length > 1 ? spark(costPts, "var(--text-2)", 170, 30) : ""), kv("Bytes written", fmtBytes(r.bytes_written), `${fmtCompact(r.records_read || 0)} rows read`), kv("Failed runs", fmtInt(r.failed_runs || 0), r.failed_runs ? html`<a class="lnk" href="${href.runs({ name: r.key, status: "failed" })}">open them</a>` : "none")]),
    });
    const byDay = card({ title: "By day", right: `${days.length} day${days.length === 1 ? "" : "s"}`, body: days.length ? tbl([["Day", ""], ["Runs", "num"], ["Rows out", "num"], ["Est. cost", "num"]], days.slice().reverse().slice(0, 8).map((d) => [prettyDay(d.key), fmtInt(d.runs), fmtCompact(d.records_written || 0), money(d.cost, report.currency)])) : html`<div class="empty-line">No daily figures.</div>` });
    return html`<div class="two">${summary}${byDay}</div>`;
  }

  function body() {
    if (off) return empty("Usage accounting is not available on this server", html`faucet was built without the <code>catalog</code> feature, which keeps usage history.`, { icon: "usage" });
    if (err) return errorAlert("usage", err);
    if (!report) return skelCards(2);
    if (!(report.rows || []).length) {
      return card({ body: empty("No usage recorded for this window", html`Nothing ran between ${state.since ? prettyDay(state.since) : "the start"} and ${state.until ? prettyDay(state.until) : "today"}, or this server keeps no usage history. Widen the dates or run a pipeline first.`, { icon: "usage", btns: html`${btn("Last 90 days", { href: hashFor(href.usage(), { ...urlState(), since: daysAgo(89), until: "" }) })}${btn("Open Runs", { icon: "runs", href: href.runs() })}` }) });
    }
    const label = (groups().find(([g]) => g === state.by) || [state.by, state.by])[1];
    const drilling = state.by === "pipeline" && state.pipeline;
    const main = card({ icon: "usage", title: drilling ? "All pipelines" : `By ${label.toLowerCase()}`, aside: windowText(), right: "click a column to sort", body: table(state.pipeline), note: NOTE });
    if (drilling && !(report.rows || []).some((x) => x.key === state.pipeline)) return html`${alert(`No usage for ${state.pipeline} in this window.`, "The name must match a pipeline exactly; pick one below.", "warn")}${statRow()}${main}`;
    if (state.by === "day") return html`${statRow()}${dayBars(report.rows)}${main}`;
    if (state.by === "pipeline" && state.pipeline && (report.rows || []).some((x) => x.key === state.pipeline)) return html`${drillCards()}${main}`;
    return html`${statRow()}${main}`;
  }

  function urlState() {
    return { by: state.by !== "pipeline" ? state.by : "", pipeline: state.pipeline, since: state.since, until: state.until, sort: state.sort !== "cost" ? state.sort : "", dir: state.dir !== "desc" ? state.dir : "", tenant: state.tenant };
  }

  function render() {
    setCrumbs(["Govern", "Usage"]);
    setContext(chip(`${state.since ? prettyDay(state.since) : "all time"} → ${state.until ? prettyDay(state.until) : "today"}`));
    if (!L.detail.querySelector("#us-top")) {
      L.detail.innerHTML = v(html`<div id="us-top">${head("Usage", [html`<span>every figure is an estimate</span>`, html`<span>pricing: list prices, ${report ? report.currency : "USD"}</span>`])}</div><div id="us-filters-slot"></div><div id="us-body" class="stack"></div>`);
    }
    patch(L.detail.querySelector("#us-filters-slot"), filters());
    patch(L.detail.querySelector("#us-body"), body());
    L.detail.querySelector("[data-retry]")?.addEventListener("click", load);
  }

  async function load() {
    const g = ++gen;
    err = null;
    render();
    try {
      const r = await api(`/v1/usage?${apiQuery()}`);
      if (g !== gen) return;
      report = r.report;
    } catch (e) {
      if (g !== gen) return;
      if (e.status === 404) off = true; else { err = e; report = null; }
    }
    trend = null;
    render();
    if (report && state.by === "pipeline" && state.pipeline) {
      try {
        const r = await api(`/v1/usage?${apiQuery({ by: "day" })}`);
        if (g !== gen) return;
        trend = (r.report && r.report.rows) || [];
        render();
      } catch { /* the drill-down keeps its numbers without a trend */ }
    }
  }

  function toCsv() {
    const label = (groups().find(([g]) => g === state.by) || [state.by])[1];
    const head0 = [label, "Runs", "Failed runs", "Rows in", "Rows out", "Bytes out", "Duration ms", "Requests", "Est. cost", "Hosted eq.", "Compute not reported"];
    const q = (s) => `"${String(s ?? "").replace(/"/g, '""')}"`;
    const lines = [head0.map(q).join(",")];
    for (const r of sorted(report.rows || [])) lines.push([r.key, r.runs, r.failed_runs, r.records_read, r.records_written, r.bytes_written, r.duration_ms, r.roundtrips, r.cost, r.hosted_equivalent, (r.not_reported || []).join(" ")].map(q).join(","));
    const blob = new Blob([lines.join("\n") + "\n"], { type: "text/csv" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `faucet-usage-by-${state.by}${state.since ? `-${state.since}` : ""}${state.until ? `-${state.until}` : ""}.csv`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  }

  L.detail.addEventListener("click", (e) => {
    const b = e.target.closest("[data-by]");
    if (b) return setQuery({ by: b.dataset.by === "pipeline" ? "" : b.dataset.by, sort: "", dir: "" });
    const s = e.target.closest("th[data-sort]");
    if (s) {
      const k = s.dataset.sort;
      const dir = state.sort === k ? (state.dir === "desc" ? "asc" : "desc") : k === "key" ? "asc" : "desc";
      return setQuery({ sort: k === "cost" ? "" : k, dir: dir === "desc" ? "" : dir }, { replace: true });
    }
    if (e.target.closest("[data-reload]")) return load();
    if (e.target.closest("[data-csv]")) { if (report) toCsv(); return; }
    const c = e.target.closest("[data-clear]");
    if (c) { e.preventDefault(); setQuery(c.dataset.clear === "all" ? { pipeline: "", tenant: "" } : { [c.dataset.clear]: "" }); }
  });
  L.detail.addEventListener("keydown", (e) => {
    if ((e.key === "Enter" || e.key === " ") && e.target.closest("[data-by], [data-clear]")) { e.preventDefault(); e.target.click(); }
  });
  L.detail.addEventListener("change", (e) => {
    if (e.target.name === "since" || e.target.name === "until") setQuery({ [e.target.name]: e.target.value || (e.target.name === "since" ? "" : "") });
  });
  L.detail.addEventListener("submit", (e) => {
    e.preventDefault();
    const f = e.target;
    setQuery({ pipeline: f.elements.pipeline.value.trim() });
  });

  await load();

  return {
    update(p) {
      const prev = JSON.stringify(state);
      const next = read(p);
      const onlySort = prev.replace(/"sort":"[^"]*","dir":"[^"]*"/, "") === JSON.stringify(next).replace(/"sort":"[^"]*","dir":"[^"]*"/, "");
      state = next;
      if (onlySort) render(); else load();
    },
  };
}

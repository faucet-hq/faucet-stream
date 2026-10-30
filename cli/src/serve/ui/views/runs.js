// Runs (#998): split pane — the run list (tabs · stats · filters · sections)
// beside one run's detail (Overview · Invocations · Logs · Config · Dead-letter
// queue · Compare). `#/runs` and `#/runs/<id>` are one view: selecting a run
// keeps the list, its filters and scroll. Filters and the detail tab live in the
// URL (`?status=&q=&since=&until=&pool=&by=&mine=&label=…&f=1&tab=`).
import { api } from "../api.js";
import { navigate } from "../router.js";
import { streamLogs } from "../sse.js";
import {
  html, raw, v, ic, row, ring, sect, tabs, dtabs, dhead, ibtn, btn, input, select, check, toggle, field,
  empty, alert, skelRows, skelCards, head, stats, card, tbl, pill, chip, kv, grid, code, diff, lnk,
  cpi, shortId, bar, legend, meter, spark, menu, meta, dotText, join,
} from "../components.js";
import { layout, setQuery, patch, errorAlert, hashFor, currentHash, poll, ago, at, dialog, toast, sections } from "../page.js";
import { setCrumbs, setContext, readiness, refreshBadges } from "../shell.js";
import { fmtInt, fmtCompact, fmtAge, TERMINAL_RUN_STATUSES as TERMINAL, sortedLabels } from "../utils.js";
import { href, datasetName } from "../links.js";
import { withTenant, currentTenant, tenantList } from "../tenant.js";
import { whoami, can } from "../access.js";
import { formatTs, zonedWallToUtc, utcToZonedWall } from "../tz.js";

const STATUSES = ["queued", "pending", "running", "sharded", "completed", "failed", "cancelled"];
const ACTIVE = ["queued", "pending", "running", "sharded"];
const MAX_LABEL_FILTERS = 10;
const PAGE = 100;
const SECTIONS = [
  ["running", "Running", ["running", "sharded"]],
  ["queued", "Queued", ["queued", "pending"]],
  ["failed", "Failed", ["failed"]],
  ["completed", "Succeeded", ["completed"]],
  ["cancelled", "Cancelled", ["cancelled"]],
];
const RING = {
  running: ["info", "half"], sharded: ["info", "half"],
  queued: ["", "hollow"], pending: ["", "hollow"],
  completed: ["ok", ""], failed: ["bad", ""], cancelled: ["", ""],
};
const PILL = {
  running: ["Running", "info", "play"], sharded: ["Sharded", "info", "play"],
  queued: ["Queued", "", "clock"], pending: ["Pending", "", "clock"],
  completed: ["Completed", "ok", "check"], failed: ["Failed", "bad", "x"], cancelled: ["Cancelled", "", "x"],
};
const DLQ_REASONS = [["", "all reasons"], ["partial", "partial batches"], ["dlq_all", "whole batches"], ["quality", "quality"], ["schema_drift", "schema drift"], ["contract", "contract"]];
const ACCOUNT = [
  ["accepted", "accepted", "var(--ok)"],
  ["coalesced", "coalesced", "var(--info)"],
  ["skipped", "skipped", "var(--faint)"],
  ["filtered", "filtered", "var(--warn)"],
  ["failed", "DLQ", "var(--bad)"],
  ["quarantined", "quarantined", "var(--bad)"],
  ["dropped", "dropped", "var(--bad)"],
  ["unaccounted", "unaccounted", "var(--bad)"],
];

// ---- small helpers ------------------------------------------------------------------

const isLive = (r) => !TERMINAL.includes(r.status);
/** A run's display name: its name, else the pipeline it ran, else its id. */
export const runTitle = (r) => r.name || (r.labels || {}).pipeline || r.run_id;
/** `rest → file` from the first invocation's usage record, when it has one. */
export function runKinds(r) {
  const u = (r.invocations || []).find((i) => i.usage && i.usage.source_kind)?.usage;
  return u ? `${u.source_kind} → ${u.sink_kind}` : "";
}
const firstLine = (s) => String(s || "").split("\n")[0].trim();
const pad2 = (n) => String(n).padStart(2, "0");
/** Seconds as `38s`, `4m 10s`, `2h 05m`. */
export function fmtSecs(s0) {
  const s = Math.max(0, Math.round(Number(s0) || 0));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${pad2(s % 60)}s`;
  return `${Math.floor(s / 3600)}h ${pad2(Math.floor((s % 3600) / 60))}m`;
}
const secsText = (x) => (x < 1 ? "<1s" : fmtSecs(x));
const fmtMs = (ms) => (ms == null ? "—" : ms < 1000 ? `${ms}ms` : fmtSecs(ms / 1000));
const secsBetween = (a, b) => (a && b ? Math.max(0, (Date.parse(b) - Date.parse(a)) / 1000) : null);
function runSecs(r) {
  if (r.elapsed_secs != null && !isLive(r)) return r.elapsed_secs;
  if (r.started_at) return secsBetween(r.started_at, r.finished_at || new Date().toISOString());
  return null;
}
/** The short error code a failed run gives (`rate_limited`), else its first line. */
export function errorCode(r) {
  const inv = (r.invocations || []).find((i) => i.error && !i.skipped);
  const text = firstLine((inv && inv.error) || r.error);
  const m = text.match(/^([a-z][a-z0-9_]{2,40})(?::|\b)/);
  return m && m[1].includes("_") ? m[1] : text.length > 48 ? `${text.slice(0, 47)}…` : text || "failed";
}
function invStatus(i, live) {
  const s = i.status || (i.skipped ? "skipped" : i.error ? "failed" : live ? "running" : "succeeded");
  return [{ succeeded: "ok", failed: "bad", skipped: "", running: "ok" }[s] ?? "", s];
}
const costOf = (r) => (r.invocations || []).reduce((n, i) => n + ((i.usage && i.usage.cost && i.usage.cost.total) || 0), 0);
const currencyOf = (r) => (r.invocations || []).find((i) => i.usage && i.usage.cost)?.usage.cost.currency || "USD";
export const money = (n, cur = "USD") => (cur === "USD" ? `$${n.toFixed(n && n < 0.01 ? 4 : 2)}` : `${cur} ${n.toFixed(2)}`);
function bytes(n) {
  const u = ["B", "KiB", "MiB", "GiB", "TiB"];
  let x = Number(n) || 0;
  let i = 0;
  while (x >= 1024 && i < u.length - 1) { x /= 1024; i++; }
  return i ? `${x.toFixed(1)} ${u[i]}` : `${x} B`;
}
function lagText(l) {
  if (!l) return "—";
  const out = [];
  if (l.seconds != null) out.push(fmtSecs(l.seconds));
  if (l.events != null) out.push(`${fmtInt(l.events)} events`);
  if (l.bytes != null) out.push(bytes(l.bytes));
  return out.join(" · ") || "—";
}
/** Row accounting summed over invocations (#879). */
function accounting(invs) {
  const s = { offered: 0, any: false };
  for (const i of invs) {
    const r = i.rows;
    if (!r) continue;
    s.any = true;
    s.offered += r.offered || 0;
    s.filtered = (s.filtered || 0) + Math.max(0, (r.transform_in || 0) - (r.transform_out || 0));
    for (const [k] of ACCOUNT) if (k !== "filtered") s[k] = (s[k] || 0) + (r[k] || 0);
  }
  return s;
}
function accountingNote(invs) {
  const s = accounting(invs);
  if (!s.any || !(s.offered || s.filtered)) return "";
  const total = ACCOUNT.reduce((n, [k]) => n + (s[k] || 0), 0) || 1;
  const ok = !s.unaccounted;
  return html`<div style="display:flex;justify-content:space-between"><span>Row accounting · every row offered is accounted for</span><span class="${ok ? "ok" : "bad"}">${ok ? "balanced" : `${fmtInt(s.unaccounted)} unaccounted`}</span></div>${bar(ACCOUNT.filter(([k]) => s[k]).map(([k, l, c]) => [(s[k] / total) * 100, c, `${l} ${fmtInt(s[k])}`]))}${legend(ACCOUNT.filter(([k]) => s[k]).map(([k, l, c]) => [`${l} ${fmtInt(s[k])}`, c]))}`;
}
function rowsBar(r) {
  if (!r || !(r.offered || r.unaccounted)) return html`<span class="faint">—</span>`;
  const filtered = Math.max(0, (r.transform_in || 0) - (r.transform_out || 0));
  const parts = [["accepted", "var(--ok)"], ["coalesced", "var(--info)"], ["skipped", "var(--faint)"], ["failed", "var(--bad)"], ["quarantined", "var(--bad)"], ["dropped", "var(--bad)"], ["unaccounted", "var(--bad)"]];
  const total = parts.reduce((n, [k]) => n + (r[k] || 0), 0) || 1;
  const words = [r.coalesced && `${fmtInt(r.coalesced)} merged`, r.skipped && `${fmtInt(r.skipped)} skipped`, r.failed && html`<span class="bad">${fmtInt(r.failed)} DLQ</span>`, r.quarantined && html`<span class="bad">${fmtInt(r.quarantined)} quarantined</span>`, r.dropped && html`<span class="bad">${fmtInt(r.dropped)} dropped</span>`, r.unaccounted && html`<span class="bad">${fmtInt(r.unaccounted)} unaccounted</span>`].filter(Boolean);
  const tip = [`${fmtInt(r.offered || 0)} offered`, `${fmtInt(r.accepted || 0)} accepted`, filtered && `${fmtInt(filtered)} filtered by transforms`, r.non_finite && `${fmtInt(r.non_finite)} NaN / infinite written as null`].filter(Boolean).join(" · ");
  return html`<div class="ru-acct" title="${tip}">${bar(parts.filter(([k]) => r[k]).map(([k, c]) => [((r[k] || 0) / total) * 100, c]))}<span class="muted">${words.length ? join(words, " · ") : `${fmtInt(r.accepted || 0)} accepted`}</span></div>`;
}
function batchCell(b) {
  if (!b || !b.attempted) return html`<span class="faint">—</span>`;
  const extra = [b.dlq_partial && html`<span class="bad">${fmtInt(b.dlq_partial)} partial</span>`, b.dlq_all && html`<span class="bad">${fmtInt(b.dlq_all)} whole to DLQ</span>`, b.failed && html`<span class="bad">${fmtInt(b.failed)} failed</span>`].filter(Boolean);
  return html`<span class="ru-nowrap" title="${fmtInt(b.attempted)} sink writes">${fmtInt(b.committed)}${extra.length ? html` · ${join(extra, " · ")}` : ""}</span>`;
}
const LEVEL_RE = /\b(TRACE|DEBUG|INFO|WARN|ERROR)\b/;
/** Split one log line into time, level and message. */
function parseLog(text) {
  let rest = text;
  let ts = "";
  const m = rest.match(/^\s*(\d{4}-\d\d-\d\dT[\d:.]+Z?)\s+/);
  if (m) {
    const d = new Date(m[1]);
    ts = Number.isNaN(d.getTime()) ? m[1] : formatTs(d.toISOString()).split(", ").pop();
    rest = rest.slice(m[0].length);
  }
  const lv = rest.match(LEVEL_RE);
  if (lv && lv.index < 12) return { ts, lv: lv[1], msg: rest.slice(lv.index + lv[1].length).replace(/^\s*/, "") };
  return { ts, lv: "", msg: rest };
}
function logLine(l) {
  if (l.note) return html`<div class="end">${l.text}</div>`;
  const p = parseLog(l.text);
  return html`<div>${p.ts ? html`<span class="muted">${p.ts}</span> ` : ""}${p.lv ? html`<span class="lv ${{ INFO: "info", WARN: "warn", ERROR: "bad" }[p.lv] || ""}">${p.lv}</span> ` : ""}${p.msg}</div>`;
}

/** Lines of `a` and `b` as a diff with one line of context around each change. */
function lineDiff(a, b) {
  const A = String(a).split("\n");
  const B = String(b).split("\n");
  if (A.length * B.length > 400000) return [["@", "the configs are too long to compare here"]];
  const n = A.length, m = B.length;
  const L = Array.from({ length: n + 1 }, () => new Uint16Array(m + 1));
  for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) L[i][j] = A[i] === B[j] ? L[i + 1][j + 1] + 1 : Math.max(L[i + 1][j], L[i][j + 1]);
  const ops = [];
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (A[i] === B[j]) { ops.push(["", A[i]]); i++; j++; }
    else if (L[i + 1][j] >= L[i][j + 1]) ops.push(["-", A[i++]]);
    else ops.push(["+", B[j++]]);
  }
  while (i < n) ops.push(["-", A[i++]]);
  while (j < m) ops.push(["+", B[j++]]);
  const keep = ops.map((o, k) => o[0] !== "" || (ops[k - 1] && ops[k - 1][0] !== "") || (ops[k + 1] && ops[k + 1][0] !== ""));
  const out = [];
  let gap = false;
  ops.forEach((o, k) => { if (keep[k]) { if (gap && out.length) out.push(["@", "⋯"]); gap = false; out.push(o); } else gap = true; });
  return out.length ? out : [["@", "the two configs are identical"]];
}

// Datasets the catalog last saw a run touch (best effort, shared 30 s cache).
let catalogCache = null;
let catalogAt = 0;
async function catalogDatasets() {
  if (catalogCache && Date.now() - catalogAt < 30000) return catalogCache;
  try { catalogCache = ((await api("/v1/catalog/datasets?limit=1000")) || {}).datasets || []; } catch { catalogCache = []; }
  catalogAt = Date.now();
  return catalogCache;
}

// Date-time filter fields: the display timezone's wall clock ⇄ UTC ISO.
function isoToWall(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  const w = utcToZonedWall(d);
  return `${w.y}-${pad2(w.mo + 1)}-${pad2(w.d)}T${pad2(w.h)}:${pad2(w.mi)}`;
}
function wallToIso(s) {
  const m = String(s || "").match(/^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d)/);
  return m ? zonedWallToUtc(+m[1], +m[2] - 1, +m[3], +m[4], +m[5]).toISOString() : "";
}
const MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
/** `28 Oct, 00:00` in the display timezone. */
function shortDate(iso) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const w = utcToZonedWall(d);
  return `${w.d} ${MON[w.mo]}, ${pad2(w.h)}:${pad2(w.mi)}`;
}

// ---- the page ---------------------------------------------------------------------------

export async function renderRuns(host, params) {
  const L = layout(host, "split");
  const secs = sections("faucet.runs.sections");
  const hasPrincipal = () => { const me = whoami(); return !!(me && me.principal && me.principal !== "anonymous"); };

  let f = readFilters(params.query);
  let selected = params.id || null;
  let dtab = params.query.get("tab") || "overview";
  let runs = [];
  let counts = null;
  let cursor = null;
  let loaded = false;
  let listErr = null;
  let alive = true;
  let seq = 0;
  let summary = null;

  function readFilters(q) {
    const status = (q.get("status") || "").split(",").filter((s) => STATUSES.includes(s));
    return { status, q: q.get("q") || "", since: q.get("since") || "", until: q.get("until") || "", pool: q.get("pool") || "", by: q.get("by") || "", mine: q.get("mine") === "1" && !q.get("by"), labels: [...new Set(q.getAll("label").filter(Boolean))], open: q.get("f") === "1" };
  }
  const tabFor = () => {
    if (f.mine && !f.status.length) return "Mine";
    const k = [...f.status].sort().join(",");
    if (!k) return "All";
    if (k === [...ACTIVE].sort().join(",")) return "Active";
    if (k === "failed") return "Failed";
    if (k === "completed") return "Succeeded";
    return "";
  };
  const extraFilters = () => [f.q, f.since, f.until, f.pool, f.by, ...f.labels, f.status.length && !tabFor()].filter(Boolean).length;
  const filtered = () => !!(f.status.length || f.q || f.since || f.until || f.pool || f.by || f.mine || f.labels.length);
  const listQuery = (extra = {}) => ({ status: f.status.join(","), q: f.q, since: f.since, until: f.until, pool: f.pool, by: f.by, mine: f.mine ? "1" : "", label: f.labels, f: f.open ? "1" : "", ...extra });
  const runPath = (rid) => `#/runs/${encodeURIComponent(rid)}`;
  const runHref = (rid) => hashFor(runPath(rid), listQuery({ tab: dtab !== "overview" ? dtab : "" }));
  const listHref = () => hashFor("#/runs", listQuery());

  // ---- list data ----
  function apiQuery(mode) {
    const p = new URLSearchParams();
    if (f.status.length) p.set("status", f.status.join(","));
    if (f.mine) p.set("mine", "true");
    if (f.by) p.set("submitted_by", f.by);
    if (f.q) p.set("name_contains", f.q);
    if (f.pool) p.set("pool", f.pool);
    if (f.since) p.set("since", f.since);
    if (f.until) p.set("until", f.until);
    p.set("limit", String(mode === "refresh" ? Math.min(500, Math.max(PAGE, runs.length)) : PAGE));
    p.set("counts", "true");
    if (mode === "more" && cursor) p.set("cursor", cursor);
    for (const l of f.labels) p.append("label", l);
    withTenant(p);
    return p.toString();
  }
  async function loadList(mode = "fresh") {
    if (mode === "more" && !cursor) return;
    const mine = ++seq;
    if (mode === "fresh") { loaded = false; runs = []; paintList(); }
    try {
      const data = await api(`/v1/runs?${apiQuery(mode)}`);
      if (!alive || mine !== seq) return;
      runs = mode === "more" ? runs.concat(data.runs || []) : data.runs || [];
      counts = data.counts || null;
      cursor = data.next_cursor || null;
      loaded = true;
      listErr = null;
    } catch (e) {
      if (!alive || mine !== seq) return;
      listErr = e;
      loaded = true;
    }
    paintList();
    if (!selected) paintSummary();
  }

  // ---- list paint ----
  function rowMeta(r) {
    const l = r.labels || {};
    const kinds = runKinds(r);
    const recs = `${fmtCompact(r.records_written ?? 0)} rows`;
    const facts = [];
    if (r.status === "failed") facts.push(errorCode(r), kinds);
    else if (r.status === "queued" || r.status === "pending") facts.push(r.status, `waiting for a slot in pool ${r.pool || "default"}`);
    else if (r.status === "cancelled") facts.push("cancelled", kinds, recs);
    else if (l.template) facts.push(`template ${l.template}${l.template_version ? `@v${l.template_version}` : ""}`, (r.invocations || []).length > 1 ? `${fmtInt(r.invocations.length)} streams` : kinds, recs);
    else if (l.backfill_unit) facts.push(`backfill ${l.backfill_unit}`, kinds, recs);
    else if (l.dlq_replay) facts.push("DLQ replay", kinds, recs);
    else if (!kinds && isLive(r)) facts.push(r.status, `${recs} so far`);
    else facts.push(kinds || recs, kinds ? recs : "");
    const inv = (r.invocations || []).length;
    if (inv > 1 && !l.template) facts.push(`${fmtInt(inv)} invocations`);
    if (!isLive(r) && r.status !== "cancelled" && runSecs(r) != null) facts.push(runSecs(r) < 1 ? "<1s" : fmtSecs(runSecs(r)));
    if (r.pool && r.pool !== "default" && r.status !== "queued") facts.push(`pool ${r.pool}`);
    if (r.tenant && !currentTenant()) facts.push((tenantList() || []).find((x) => x.id === r.tenant)?.name || r.tenant);
    if (r.attempt) facts.push(`attempt ${r.attempt + 1}`);
    return meta(facts.filter(Boolean));
  }
  function runRow(r) {
    const [tone, shape] = RING[r.status] || ["", ""];
    const title = runTitle(r);
    return row(ring(tone, shape), title, html`<time datetime="${r.submitted_at}" title="submitted ${formatTs(r.submitted_at)}">${fmtAge(r.submitted_at)}</time>`, rowMeta(r), {
      href: runHref(r.run_id), id: r.run_id, on: r.run_id === selected, tip: title !== r.run_id ? `${title}\n${r.run_id}` : r.run_id,
    });
  }
  const count = (sts) => (counts ? sts.reduce((n, s) => n + (counts[s] || 0), 0) : runs.filter((r) => sts.includes(r.status)).length);
  const shownRuns = () => runs;

  function statStrip() {
    const plus = !counts && cursor ? "+" : "";
    const recs = runs.reduce((n, r) => n + (r.records_written || 0), 0);
    const key = [...f.status].sort().join(",");
    const on = key === "failed" ? 2 : key === "completed" ? 1 : key === "running,sharded" ? 0 : -1;
    return stats([
      ["Running", `${fmtInt(count(["running", "sharded"]))}${plus}`, "info", "running,sharded"],
      ["Succeeded", `${fmtInt(count(["completed"]))}${plus}`, "ok", "completed"],
      ["Failed", `${fmtInt(count(["failed"]))}${plus}`, count(["failed"]) ? "bad" : "", "failed"],
      ["Records", `${fmtCompact(recs)}${cursor ? "+" : ""}`],
    ], on);
  }
  function filterPanel() {
    const statusLabel = f.status.length ? `Status · ${f.status.join(", ")}` : "Status · any";
    const pools = [...new Set(["default", ...(window.__faucetPools || []), ...runs.map((r) => r.pool || "default"), f.pool].filter(Boolean))].sort();
    return html`<form class="filters" id="ru-filters" autocomplete="off">
      ${input(f.q, { icon: "search", ph: "Name contains", name: "q", type: "search" })}
      <div class="r2">
        <details class="menu left ru-status"><summary class="select"><span>${statusLabel}</span></summary><div class="pop" role="menu">${STATUSES.map((s) => html`<div class="mi">${check(s, f.status.includes(s), { name: "st", a: { value: s } })}</div>`)}</div></details>
        ${select([["", "Pool · any"], ...pools.map((p) => [p, `Pool · ${p}`])], f.pool, { name: "pool" })}
      </div>
      <div class="r2">
        <label class="input" title="Since, in the display timezone"><input type="text" name="since" data-wall="${isoToWall(f.since)}" value="${f.since ? `Since · ${shortDate(f.since)}` : ""}" placeholder="Since" aria-label="Since"></label>
        <label class="input" title="Until, in the display timezone"><input type="text" name="until" data-wall="${isoToWall(f.until)}" value="${f.until ? `Until · ${shortDate(f.until)}` : ""}" placeholder="Until" aria-label="Until"></label>
      </div>
      ${input("", { ph: "label key:value — Enter to add", name: "label", mono: true })}
      <div style="display:flex;gap:8px;justify-content:flex-end">${btn("Clear", { kind: "quiet", data: { act: "clear" } })}${btn("Apply", { kind: "primary", type: "submit" })}</div>
    </form>`;
  }
  function activeChips() {
    const c = [];
    const x = (text, key, value, mono = false) => chip(text, mono ? "mono x" : "x", { "data-remove": key, "data-value": value ?? "", role: "button", tabindex: "0", title: `Remove ${text}` });
    if (f.status.length && (!tabFor() || f.open)) c.push(x(`status: ${f.status.join(", ")}`, "status"));
    if (f.mine) c.push(x("submitted by me", "mine"));
    if (f.by) c.push(x(`by ${f.by}`, "by"));
    if (f.q) c.push(x(`name: ${f.q}`, "q"));
    if (f.pool) c.push(x(`pool: ${f.pool}`, "pool"));
    if (f.since) c.push(x(`since ${shortDate(f.since)}`, "since"));
    if (f.until) c.push(x(`until ${shortDate(f.until)}`, "until"));
    for (const l of f.labels) c.push(x(l.replace(":", "="), "label", l, true));
    return c.length ? html`<div class="active-f">${c}</div>` : "";
  }

  function paintList() {
    const tabList = [["All"], ...(hasPrincipal() ? [["Mine"]] : []), ["Active"], ["Failed"], ["Succeeded"]];
    const tools = html`${ibtn("filter", { on: f.open, dot: extraFilters() > 0, title: "Filters", data: { act: "filters" } })}${ibtn("refresh", { title: "Refresh", data: { act: "refresh" } })}${ibtn("plus", { title: "Submit a run", href: href.submit(), perm: "config_submit" })}`;
    const top = tabs(tabList, tabFor(), tools);
    let body;
    if (!loaded) {
      body = html`<div class="stats" style="--n:4">${raw('<div><span class="skel w60"></span><span class="skel" style="margin-top:8px;height:16px;width:40%"></span></div>'.repeat(4))}</div>${skelRows(8)}`;
    } else {
      const errBox = listErr ? alert("Couldn’t load runs.", `${listErr.status ? `The server answered ${listErr.status}: ${listErr.message}.` : listErr.message}${runs.length ? " Showing the last list loaded." : ""}`, "bad", btn("Retry", { icon: "refresh", cls: "sm", data: { act: "refresh" } })) : "";
      const shown = shownRuns();
      let rowsHtml;
      if (!shown.length && !listErr) {
        rowsHtml = filtered()
          ? empty("No runs match these filters", "Clear a filter, or widen the date range.", { btns: btn("Clear filters", { data: { act: "clear" } }) })
          : empty("No runs yet", html`Submit a pipeline config, trigger a template, or POST it to <code>/v1/runs</code>.`, { icon: "runs", btns: html`${btn("Submit a run", { kind: "primary", icon: "submit", href: href.submit(), perm: "config_submit" })}${btn("Browse templates", { href: href.templates() })}` });
      } else {
        rowsHtml = join(SECTIONS.map(([sid, label, sts]) => {
          const rs = shown.filter((r) => sts.includes(r.status));
          if (!rs.length) return "";
          const closed = secs.isClosed(sid);
          return html`${sect(label, fmtInt(counts ? count(sts) : rs.length), { id: sid, closed })}<div class="sect-rows rows${closed ? " closed" : ""}">${rs.map(runRow)}</div>`;
        }));
        if (cursor) rowsHtml = html`${rowsHtml}${btn("Load more", { kind: "quiet", cls: "load-more", data: { act: "more" } })}`;
      }
      body = html`${errBox}${statStrip()}${f.open ? filterPanel() : ""}${activeChips()}${rowsHtml}`;
    }
    if (!L.list.querySelector(".ru-top")) {
      L.list.innerHTML = '<div class="ru-top" style="display:contents"></div><div class="ru-body" style="display:contents"></div>';
    }
    patch(L.list.querySelector(".ru-top"), top);
    const bodyEl = L.list.querySelector(".ru-body");
    // Keep a half-typed filter form across polls: re-use the live form while the URL filters are unchanged.
    const form = bodyEl.querySelector("#ru-filters");
    const key = JSON.stringify(f);
    if (form && form.dataset.key === key) {
      const holder = document.createElement("div");
      holder.innerHTML = v(body);
      const fresh = holder.querySelector("#ru-filters");
      const markup = holder.innerHTML;
      if (bodyEl.__html !== markup) {
        if (fresh) fresh.replaceWith(form);
        const top0 = L.list.scrollTop;
        bodyEl.replaceChildren(...holder.childNodes);
        bodyEl.__html = markup;
        L.list.scrollTop = top0;
      }
    } else {
      const top0 = L.list.scrollTop;
      patch(bodyEl, body);
      L.list.scrollTop = top0;
      const nf = bodyEl.querySelector("#ru-filters");
      if (nf) nf.dataset.key = key;
    }
    const n = extraFilters() + (f.mine ? 1 : 0);
    if (!selected) setContext(html`${n ? chip(`${n} filter${n === 1 ? "" : "s"}`) : ""}${clusterChip()}`);
  }
  function clusterChip() {
    const cl = (readiness() || {}).cluster || {};
    return cl.enabled ? chip(html`<span class="dot"></span>cluster · ${fmtInt(cl.instances || 0)} live`) : "";
  }

  // ---- list events ----
  L.list.addEventListener("click", (e) => {
    const t = e.target.closest("[data-tab]");
    if (t && L.list.querySelector(".ru-top").contains(t)) {
      const name = t.dataset.tab;
      const status = name === "Active" ? ACTIVE.join(",") : name === "Failed" ? "failed" : name === "Succeeded" ? "completed" : "";
      // `mine` and `by` are one filter: picking Mine drops a named submitter.
      setQuery({ status, mine: name === "Mine" ? "1" : "", ...(name === "Mine" ? { by: "" } : {}) });
      return;
    }
    const s = e.target.closest(".stats > div[data-q]");
    if (s) { setQuery({ status: [...f.status].sort().join(",") === s.dataset.q ? "" : s.dataset.q, mine: "" }); return; }
    const x = e.target.closest("[data-remove]");
    if (x) { removeFilter(x.dataset.remove, x.dataset.value); return; }
    const a = e.target.closest("[data-act]");
    if (!a) return;
    if (a.dataset.act === "filters") setQuery({ f: f.open ? "" : "1" }, { replace: true });
    else if (a.dataset.act === "refresh") { loadList(runs.length ? "refresh" : "fresh"); if (selected) loadDetail(); }
    else if (a.dataset.act === "more") loadList("more");
    else if (a.dataset.act === "clear") clearFilters();
  });
  L.list.addEventListener("keydown", (e) => {
    const x = e.target.closest("[data-remove]");
    if (x && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); removeFilter(x.dataset.remove, x.dataset.value); }
    const s = e.target.closest(".stats > div[data-q]");
    if (s && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); s.click(); }
    if (e.target.name === "label" && e.key === "Enter") { e.preventDefault(); if (addLabel(e.target.value)) e.target.value = ""; }
  });
  L.list.addEventListener("change", (e) => {
    if (e.target.name === "st") {
      const box = e.target.closest(".ru-status");
      const sel = [...box.querySelectorAll("input[name=st]:checked")].map((i) => i.value);
      box.querySelector("summary span").textContent = sel.length ? `Status · ${sel.join(", ")}` : "Status · any";
      e.target.closest(".check").classList.toggle("on", e.target.checked);
    }
  });
  L.list.addEventListener("submit", (e) => {
    e.preventDefault();
    const form = e.target;
    const lbl = form.elements.label.value.trim().replace(/^([^:=]+)=/, "$1:");
    const labels = [...f.labels];
    if (lbl) {
      const err = labelError(lbl, labels);
      if (err) { toast(err, "bad"); return; }
      if (!labels.includes(lbl)) labels.push(lbl);
    }
    setQuery({
      q: form.elements.q.value.trim(),
      status: [...form.querySelectorAll("input[name=st]:checked")].map((i) => i.value).join(","),
      pool: form.elements.pool.value,
      since: wallToIso(dateValue(form.elements.since)),
      until: wallToIso(dateValue(form.elements.until)),
      label: labels,
      mine: "",
    });
  });
  // Date fields read as "Since · 28 Oct, 00:00" and turn into a native
  // date-time picker (display timezone) while focused.
  const dateValue = (el) => (el.type === "datetime-local" ? el.value : el.dataset.wall || "");
  L.list.addEventListener("focusin", (e) => {
    const el = e.target;
    if ((el.name === "since" || el.name === "until") && el.type === "text") { el.type = "datetime-local"; el.value = el.dataset.wall || ""; }
  });
  L.list.addEventListener("focusout", (e) => {
    const el = e.target;
    if ((el.name === "since" || el.name === "until") && el.type === "datetime-local") {
      el.dataset.wall = el.value;
      const iso = wallToIso(el.value);
      el.type = "text";
      el.value = iso ? `${el.name === "since" ? "Since" : "Until"} · ${shortDate(iso)}` : "";
    }
  });
  function labelError(l, labels) {
    if (l.startsWith(":")) return "A label filter needs a key: key:value or key.";
    if (labels.length >= MAX_LABEL_FILTERS && !labels.includes(l)) return `At most ${MAX_LABEL_FILTERS} label filters.`;
    return "";
  }
  function addLabel(raw0) {
    const l = raw0.trim().replace(/^([^:=]+)=/, "$1:");
    if (!l) return true;
    const err = labelError(l, f.labels);
    if (err) { toast(err, "bad"); return false; }
    if (!f.labels.includes(l)) setQuery({ label: [...f.labels, l] });
    return true;
  }
  function removeFilter(key, value) {
    if (key === "label") setQuery({ label: f.labels.filter((l) => l !== value) });
    else if (key === "mine") setQuery({ mine: "" });
    else setQuery({ [key]: "" });
  }
  function clearFilters() { setQuery({ status: "", q: "", since: "", until: "", pool: "", by: "", mine: "", label: [] }); }
  L.list.addEventListener("sect-toggle", (e) => secs.toggle(e.detail.id));

  // ---- summary (nothing selected) ----
  async function loadSummary() {
    const since = new Date(Date.now() - 86400000).toISOString();
    summary = summary || {};
    try {
      summary.ov = await api(`/v1/overview?${withTenant(new URLSearchParams({ since: "24h", bucket: "hour" }))}`);
    } catch {
      summary.ov = null;
    }
    try {
      const recent = await api(`/v1/runs?${withTenant(new URLSearchParams({ since, limit: "500" }))}`);
      summary.recent = recent.runs || [];
      summary.recentMore = !!recent.next_cursor;
    } catch { summary.recent = summary.recent || []; }
    summary.at = new Date().toISOString();
    if (alive && !selected) paintSummary();
  }
  const hasDlq = (r) => (r.invocations || []).some((i) => (i.rows && (i.rows.failed || i.rows.quarantined)) || (i.batches && (i.batches.dlq_partial || i.batches.dlq_all)));
  function hourly(rs) {
    const now = Date.now();
    const b = new Array(24).fill(0);
    for (const r of rs) { const h = Math.floor((now - Date.parse(r.submitted_at)) / 3600000); if (h >= 0 && h < 24) b[23 - h]++; }
    return b;
  }
  function paintSummary() {
    if (selected) return;
    L.select(false);
    setCrumbs(["Operate", "Runs"]);
    if (!loaded) { patch(L.detail, html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`); return; }
    if (listErr && !runs.length) { patch(L.detail, empty("Pick a run", "The list may be stale until the server recovers.", { icon: "runs" })); return; }
    if (!runs.length && !filtered()) {
      patch(L.detail, empty("Nothing to show yet", "Runs appear here as soon as one is submitted. Each shows its invocations, row accounting and live logs.", { icon: "logs" }));
      return;
    }
    if (filtered()) {
      const n = shownRuns().length;
      const failedOnly = f.status.join(",") === "failed" && !extraFilters();
      if (failedOnly && n) {
        const codes = {};
        shownRuns().forEach((r) => { const c = errorCode(r); codes[c] = (codes[c] || 0) + 1; });
        const top = Object.entries(codes).sort((a, b) => b[1] - a[1]).slice(0, 2);
        patch(L.detail, empty(`${fmtInt(n)}${cursor ? "+" : ""} failed run${n === 1 ? "" : "s"}`, html`Most common: ${join(top.map(([c, k]) => html`<b>${c}</b> (${k})`), " and ")}. Open one to see the error and the failed invocation.`, { icon: "alert" }));
        return;
      }
      patch(L.detail, n
        ? empty("Pick a run", `${n === 1 ? "One run matches" : `${fmtInt(n)}${cursor ? "+" : ""} runs match`} these filters. Open one to see its invocations, row accounting and live logs.`, { icon: "runs" })
        : empty("No run selected", "", { icon: "runs" }));
      return;
    }
    if (!summary) { loadSummary(); patch(L.detail, html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(2)}`); return; }
    const s = summary;
    const recent = s.recent || [];
    const ov = s.ov;
    const finished = recent.filter((r) => ["completed", "failed"].includes(r.status));
    const total = ov ? ov.runs.total : recent.length;
    const okN = ov ? ov.runs.by_status.completed || 0 : recent.filter((r) => r.status === "completed").length;
    const finN = ov ? (ov.runs.by_status.completed || 0) + (ov.runs.by_status.failed || 0) : finished.length;
    const rate = finN ? `${((okN / finN) * 100).toFixed(1)}%` : "—";
    const recs = ov ? ov.records.total : recent.reduce((n, r) => n + (r.records_written || 0), 0);
    const durs = recent.filter((r) => !isLive(r) && r.elapsed_secs != null).map((r) => r.elapsed_secs).sort((a, b) => a - b);
    const q = (p) => durs[Math.min(durs.length - 1, Math.floor(p * durs.length))];
    const queued = ov ? ov.runs.queued : runs.filter((r) => r.status === "queued" || r.status === "pending").length;
    const cost = ov && ov.cost ? money(ov.cost.estimated, ov.cost.currency) : money(recent.reduce((n, r) => n + costOf(r), 0));
    const series = ov ? ov.runs.series.map((b) => b.succeeded + b.failed + b.cancelled) : hourly(recent);
    const att = recent.filter((r) => r.status === "failed").slice(0, 5).map((r) => [lnk(runTitle(r), runHref(r.run_id)), html`<span class="bad"><span class="dot bad"></span>${errorCode(r)}</span>${runKinds(r) ? html` · ${runKinds(r)}` : ""}`, html`<time title="${formatTs(r.finished_at || r.submitted_at)}">${fmtAge(r.finished_at || r.submitted_at)}</time>`, btn(hasDlq(r) ? "Inspect DLQ" : "Open", { cls: "sm", href: hashFor(runPath(r.run_id), listQuery({ tab: hasDlq(r) ? "dlq" : "" })) })]);
    for (const r of runs.filter((x) => x.status === "queued" || x.status === "pending").slice(0, 3)) {
      att.push([lnk(runTitle(r), runHref(r.run_id)), html`<span class="warn"><span class="dot warn"></span>${r.status}</span> ${fmtAge(r.submitted_at)} · pool ${r.pool || "default"}`, html`<time title="${formatTs(r.submitted_at)}">${fmtAge(r.submitted_at)}</time>`, btn("Open", { cls: "sm", href: runHref(r.run_id) })]);
    }
    patch(L.detail, html`${head("Runs", [html`<span>last 24 hours</span>`, lnk("all time", href.usage())])}
      ${card({ icon: "runs", title: `${fmtInt(total)}${s.recentMore && !ov ? "+" : ""} runs · ${fmtCompact(recs)} records`, right: html`<span>updated ${ago(s.at)} ago</span>`,
        body: grid(4, [kv("Success rate", rate, `${fmtInt(okN)} of ${fmtInt(finN)} finished`), kv("Median duration", durs.length ? secsText(q(0.5)) : "—", durs.length ? `p95 ${secsText(q(0.95))}` : "no finished run yet"), kv("Queued now", fmtInt(queued), queued ? "waiting for a free slot" : "nothing waiting"), kv("Estimated cost", cost, lnk("Usage", href.usage()))]),
        note: series.length > 1 && series.some(Boolean) ? html`<div class="ru-spark">${spark(series, "var(--text-2)", 640, 40, null, 0)}</div>` : "" })}
      ${card({ icon: "alert", title: "Needs attention", right: fmtInt(att.length), body: att.length ? tbl(["Run", "Problem", ["Since", "num"], ""], att) : html`<div class="empty-line">Nothing failed or waiting in the last 24 hours.</div>` })}
      <div class="muted ru-keys"><span><kbd>j</kbd> <kbd>k</kbd> move</span><span><kbd>↵</kbd> open</span><span><kbd>/</kbd> search</span><span><kbd>esc</kbd> back to the list</span><span><kbd>⌘K</kbd> go to anything</span></div>`);
  }

  // ---- detail ----
  let rec = null;
  let recErr = null;
  let cfg; // undefined = not asked, null = unavailable, else {format, body, redacted}
  let logs = [];
  let logState = "connecting";
  let logCtrl = null;
  let logLink = "";
  const ui = { logQ: "", logLv: "All", follow: true, invSt: "all", rb: null, rbOpen: false, rbReport: null, rbErr: null, rbBusy: false, rbNeedsConfig: false, dlqLoc: "", dlqReason: "", dlqRes: null, dlqErr: null, dlqBusy: false, dlqAt: "", dlqAuto: "", replayDry: true, replayRes: null, replayCfg: "", replayEdit: false, cmpWith: "", prev: null, cmpCfg: undefined, cmpCfgFor: "" };

  function resetDetail() {
    rec = null; recErr = null; cfg = undefined; logs = []; logState = "connecting"; logLink = "";
    logCtrl?.abort(); logCtrl = null;
    touchedFor = ""; touchedRows = [];
    Object.assign(ui, { logQ: "", invSt: "all", rb: null, rbOpen: false, rbReport: null, rbErr: null, rbNeedsConfig: false, dlqLoc: "", dlqRes: null, dlqErr: null, dlqAuto: "", replayRes: null, replayCfg: "", replayEdit: false, cmpWith: "", prev: null, cmpCfg: undefined, cmpCfgFor: "" });
  }
  function startLogs(rid) {
    logCtrl = streamLogs(rid, {
      onLog: (l) => { logState = "streaming"; pushLog({ text: l }); },
      onTruncated: (m) => pushLog({ text: `— ${m} —`, note: true }),
      onEnd: () => { logState = "ended"; pushLog({ text: "end of logs", note: true }); },
      onExpired: () => { logState = "expired"; paintLogs(); },
      onLink: (url) => { logState = "in the log service"; logLink = url; pushLog({ text: "the local copy of these logs has aged out; open them in the log service", note: true }); },
      onError: (e) => { logState = "stream error"; pushLog({ text: `log stream error: ${e.message}`, note: true }); },
    });
  }
  let logPaintQueued = false;
  function pushLog(l) {
    logs.push(l);
    if (logs.length > 5000) logs.splice(0, logs.length - 5000);
    if (!logPaintQueued) { logPaintQueued = true; requestAnimationFrame(() => { logPaintQueued = false; paintLogs(); }); }
  }
  const lvOf = (l) => (l.note ? "" : parseLog(l.text).lv);
  function filteredLogs() {
    const q = ui.logQ.toLowerCase();
    return logs.filter((l) => (ui.logLv === "All" || l.note || lvOf(l) === ui.logLv.toUpperCase()) && (!q || l.text.toLowerCase().includes(q)));
  }
  function paintLogs() {
    const tail = L.detail.querySelector("#ru-tail");
    if (tail) patch(tail, logs.length ? join(logs.slice(-6).map(logLine)) : html`<div class="end">${logState === "connecting" ? "waiting for log lines…" : logState === "expired" ? "these logs have expired" : "no log lines"}</div>`);
    const box = L.detail.querySelector("#ru-log");
    if (box) {
      const shown = filteredLogs();
      const atEnd = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
      patch(box, shown.length ? html`${join(shown.map(logLine))}${rec && isLive(rec) && ui.follow ? html`<div class="end">following · new lines appear here</div>` : ""}` : html`<div class="end">${logs.length ? "No line matches." : logState === "expired" ? "These logs have expired from the server." : "No log lines yet."}</div>`);
      if (ui.follow && atEnd) box.scrollTop = box.scrollHeight;
      const c = L.detail.querySelector("#ru-log-count");
      if (c) c.textContent = `${fmtInt(logs.filter((l) => !l.note).length)} lines${ui.logQ || ui.logLv !== "All" ? ` · ${fmtInt(shown.filter((l) => !l.note).length)} match` : ""}`;
      const st = L.detail.querySelector("#ru-log-state");
      if (st) st.textContent = logState;
    }
    const ts = L.detail.querySelector("#ru-tail-state");
    if (ts) ts.textContent = logState;
  }

  async function loadDetail() {
    const rid = selected;
    if (!rid) return;
    try {
      const r = await api(`/v1/runs/${encodeURIComponent(rid)}`);
      if (!alive || rid !== selected) return;
      const before = rec && rec.status;
      rec = r;
      recErr = null;
      if (before && before !== r.status) { loadList("refresh"); refreshBadges(true); }
      if (r.log_export && r.log_export.link && !logLink) logLink = r.log_export.link;
      if (!ui.dlqLoc && r.dlq_location) ui.dlqLoc = r.dlq_location;
    } catch (e) {
      if (!alive || rid !== selected) return;
      recErr = e;
    }
    paintDetail();
  }
  async function loadConfig() {
    if (cfg !== undefined || !rec) return;
    cfg = null;
    const rid = rec.run_id;
    let got = null;
    try {
      const c = await api(`/v1/runs/${encodeURIComponent(rid)}/config`);
      if (c && c.body) got = c;
    } catch { /* older server or no stored body */ }
    if (!got && rec.config_body) got = { format: rec.config_format || "yaml", body: rec.config_body, redacted: false };
    if (rid !== selected) return;
    cfg = got;
    paintDetail();
  }

  function tabsFor(r) {
    const n = (r.invocations || []).length;
    const items = [["Overview", "", "overview"], ["Invocations", n || "", "invocations"], ["Logs", "", "logs"], ["Config", "", "config"]];
    // The DLQ tab shows when the run sent rows to one (or names one); Compare once it finished.
    if (r.dlq_location || r.replay_of || hasDlq(r) || dtab === "dlq") items.push(["Dead-letter queue", "", "dlq"]);
    if (!isLive(r)) items.push(["Compare", "", "compare"]);
    return items;
  }
  const tabHref = (t) => hashFor(currentHash().path, listQuery({ tab: t === "overview" ? "" : t }));
  const backLink = () => ({ href: listHref(), label: "Runs" });

  function actions(r) {
    const live = isLive(r);
    if (!can("run_write") && !can("rollback") && !can("config_submit")) return chip("read-only");
    const undoable = !live && (r.invocations || []).some((i) => i.run_id);
    const written = (r.invocations || []).find((i) => i.usage && i.usage.dataset_id)?.usage;
    const more = menu("more", [
      { label: "Copy run id", icon: "copy", hint: shortId(r.run_id), data: { copy: r.run_id } },
      { label: "Copy link to this run", icon: "link", data: { copy: `${location.origin}${location.pathname}${runPath(r.run_id)}` } },
      cfg && cfg.body ? { label: "Copy config", icon: "code", data: { copy: cfg.body } } : null,
      "-",
      r.labels && r.labels.pipeline ? { label: "Filter runs by this pipeline", icon: "filter", action: "filter-pipeline" } : { label: "Filter runs by this name", icon: "filter", action: "filter-name" },
      written ? { label: "Open written dataset", icon: "db", href: href.dataset(written.dataset_id) } : null,
      written ? { label: "Open in lineage", icon: "lineage", href: href.lineage(written.dataset_id) } : null,
      { label: "Inspect a dead-letter queue", icon: "alert", href: tabHref("dlq") },
      "-",
      { label: "Roll back…", icon: "undo", cls: undoable ? "" : "dis", hint: undoable ? "" : live ? "when finished" : "nothing to undo", action: "rollback", perm: "rollback" },
      { label: "Delete run", icon: "trash", cls: live ? "danger dis" : "danger", hint: live ? "when finished" : "", action: "delete", perm: "run_write" },
    ]);
    if (live) return html`${btn("Cancel run", { icon: "x", perm: "run_write", data: { action: "cancel" } })}${more}`;
    if (r.status === "failed") return html`${cfg && cfg.body ? btn("Re-run", { icon: "play", perm: "config_submit", data: { action: "rerun" } }) : ""}${more}`;
    if (undoable) return html`${btn("Roll back…", { icon: "undo", perm: "rollback", cls: ui.rbOpen ? "open" : "", data: { action: "rollback" } })}${more}`;
    return html`${btn("Delete", { kind: "danger", icon: "trash", perm: "run_write", data: { action: "delete" } })}${more}`;
  }

  function runHead(r) {
    const [label, tone, i] = PILL[r.status] || [r.status, "", ""];
    const by = r.submitted_by;
    const byLink = by ? (by.startsWith("trigger:") ? lnk(by, href.trigger(by.slice(8))) : lnk(by, hashFor("#/runs", { by }))) : "";
    const byPart = by ? html` by ${byLink}` : "";
    const when = isLive(r)
      ? r.started_at ? html`<span>started ${ago(r.started_at)} ago${byPart}</span>` : html`<span>submitted ${ago(r.submitted_at)} ago${byPart}</span>`
      : r.status === "failed" ? html`<span>failed ${ago(r.finished_at || r.submitted_at)} ago${runSecs(r) != null ? ` after ${secsText(runSecs(r))}` : ""}</span>`
      : html`<span>finished ${ago(r.finished_at || r.submitted_at)} ago${runSecs(r) != null ? ` · ${secsText(runSecs(r))}` : ""}</span>`;
    return head(runTitle(r), [pill(label, tone, i), runKinds(r) ? html`<span>${runKinds(r)}</span>` : "", when, r.attempt ? html`<span>attempt ${r.attempt + 1}</span>` : ""]);
  }

  function errorBox(r) {
    const invErrs = (r.invocations || []).filter((i) => i.error && !i.skipped);
    if (!r.error && !invErrs.length) return "";
    const generic = invErrs.length && /^\d+ (pipeline )?invocation\(s\) failed\b/.test(String(r.error || "").trim());
    const main = invErrs[0] ? invErrs[0].error : r.error;
    const others = (r.invocations || []).length - invErrs.length;
    const text = html`<div class="ru-err"><span class="mono">${main}</span>${cpi(main)}</div>${!generic && r.error && invErrs[0] && r.error !== invErrs[0].error ? html`<div class="ru-err"><span class="mono">${r.error}</span></div>` : ""}${invErrs.length ? html`<div style="margin-top:4px">Failed invocation${invErrs.length > 1 ? "s" : ""}: ${join(invErrs.map((i) => html`<span class="mono">${i.row_id}${i.parent_record_key ? ` (${i.parent_record_key})` : ""}</span>`), ", ")}.${others > 0 ? ` ${fmtInt(others)} other invocation${others === 1 ? "" : "s"} succeeded and kept their bookmarks.` : ""}</div>` : ""}`;
    return alert(errorCode(r), text, "bad", html`${btn("Open logs at the error", { icon: "logs", href: tabHref("logs") })}${!isLive(r) ? btn("Compare with last good run", { icon: "diff", href: tabHref("compare") }) : ""}`);
  }

  function movedCard(r) {
    const invs = r.invocations || [];
    const live = isLive(r);
    const recs = r.records_written ?? 0;
    const sec = runSecs(r);
    const lag = invs.map((i) => i.source_lag).find(Boolean);
    const cost = costOf(r);
    const bw = invs.reduce((n, i) => n + ((i.usage && i.usage.usage && i.usage.usage.bytes_written) || 0), 0);
    const title = r.status === "failed" ? `${fmtInt(recs)} rows moved before the failure` : `${fmtInt(recs)} rows moved${live ? " so far" : ""}`;
    const ts = (iso) => (iso ? formatTs(iso).split(", ") : ["—", ""]);
    const body = live
      ? grid(4, [
          kv("Throughput", sec ? `${fmtInt(Math.round(recs / Math.max(1, sec)))} rows/s` : "—", "average since start"),
          kv("Lag behind source", lag ? lagText(lag).split(" · ")[0] : "—", lag ? lagText(lag).split(" · ").slice(1).join(" · ") || "when the last invocation ended" : "reported when an invocation ends"),
          kv("Running for", sec != null ? fmtSecs(sec) : "waiting", r.started_at ? html`since <time title="${formatTs(r.started_at)}">${formatTs(r.started_at).split(", ").pop()}</time>` : "not started yet"),
          kv("Estimated cost", money(cost, currencyOf(r)), bw ? `bytes written ${bytes(bw)}` : "so far"),
        ])
      : grid(4, [
          kv("Submitted", ts(r.submitted_at).pop(), ts(r.submitted_at)[0]),
          kv("Started", r.started_at ? ts(r.started_at).pop() : "—", r.started_at ? (secsBetween(r.submitted_at, r.started_at) >= 1 ? `queued ${fmtSecs(secsBetween(r.submitted_at, r.started_at))}` : "queued under 1s") : ""),
          kv("Finished", r.finished_at ? ts(r.finished_at).pop() : "—", r.finished_at ? ts(r.finished_at)[0] : ""),
          kv("Duration", sec != null ? (sec < 1 ? "<1s" : fmtSecs(sec)) : "—", cost ? `estimated ${money(cost, currencyOf(r))}` : ""),
        ]);
    const delivery = (r.labels || {}).delivery;
    return card({ icon: "runs", title, tag: delivery ? delivery.replace("_", "-") : "", aside: `${fmtInt(invs.length)} invocation${invs.length === 1 ? "" : "s"}`, right: live ? html`<span>updated ${ago(new Date().toISOString())} ago</span>` : r.finished_at ? html`<span>finished ${ago(r.finished_at)} ago</span>` : "", body, note: accountingNote(invs) });
  }

  function invSummary(r) {
    const invs = (r.invocations || []).slice().sort((a, b) => (b.duration_ms || 0) - (a.duration_ms || 0));
    if (!invs.length) return card({ title: "Invocations", body: html`<div class="empty-line">${isLive(r) ? "No invocation has finished yet; rows appear as each one ends." : "This run recorded no invocations."}</div>` });
    const top = invs.slice(0, 6);
    return card({ title: "Invocations", right: lnk(invs.length > top.length ? `all ${fmtInt(invs.length)} rows` : `${fmtInt(invs.length)} row${invs.length === 1 ? "" : "s"}`, tabHref("invocations")),
      body: tbl(["Row", "Status", ["Rows", "num"], ["Duration", "num"]], top.map((i) => { const [t, s] = invStatus(i, isLive(r)); return { cells: [html`<span class="mono" title="${i.row_id}${i.parent_record_key ? ` · ${i.parent_record_key}` : ""}">${i.row_id}${i.parent_record_key ? html`<span class="muted"> · ${i.parent_record_key}</span>` : ""}</span>`, dotText(t, s), fmtInt(i.records_written ?? 0), fmtMs(i.duration_ms)] }; })) });
  }

  function governance(r) {
    const s = accounting(r.invocations || []);
    const dlq = (r.invocations || []).reduce((n, i) => n + ((i.usage && i.usage.usage && i.usage.usage.dlq_records_written) || 0), 0);
    if (!s.any) return card({ title: "Governance", body: html`<div class="empty-line">Reported when an invocation ends.</div>` });
    return card({ title: "Governance", right: "from row accounting", body: grid(2, [
      kv("Quarantined", fmtInt(s.quarantined || 0), "quality, contract or drift"),
      kv("Dead-lettered", fmtInt(s.failed || 0), dlq ? `${fmtInt(dlq)} written to the DLQ` : "per-row write failures"),
      kv("Filtered", fmtInt(s.filtered || 0), "removed by transforms"),
      kv("Unaccounted", html`<span class="${s.unaccounted ? "bad" : ""}">${fmtInt(s.unaccounted || 0)}</span>`, s.unaccounted ? "rows lost without a reason" : "every row explained"),
    ]) });
  }

  let touchedFor = "";
  let touchedRows = [];
  async function loadTouched(r) {
    if (touchedFor === r.run_id + r.status) return;
    touchedFor = r.run_id + r.status;
    const all = await catalogDatasets();
    const rows = new Map();
    for (const i of r.invocations || []) {
      const u = i.usage;
      if (u && u.dataset_id) rows.set(u.dataset_id, { id: u.dataset_id, uri: u.dataset_uri, role: "written", rows: (rows.get(u.dataset_id)?.rows || 0) + (i.records_written || 0) });
    }
    for (const d of all.filter((x) => x.last_run_id === r.run_id)) {
      const cur = rows.get(d.id);
      rows.set(d.id, { id: d.id, uri: d.uri, role: cur ? cur.role : (d.roles || []).includes("sink") ? "written" : "read", rows: cur ? cur.rows : d.last_records, schema: d.schema_versions });
    }
    for (const x of rows.values()) if (x.schema == null) x.schema = all.find((d) => d.id === x.id)?.schema_versions;
    touchedRows = [...rows.values()].sort((a, b) => (a.role === b.role ? 0 : a.role === "read" ? -1 : 1));
    if (selected && r.run_id === selected) paintDetail();
  }
  function touched() {
    if (!touchedRows.length) return "";
    const written = touchedRows.find((x) => x.role === "written");
    return card({ icon: "layers", title: "What this run touches", right: written ? lnk("Lineage", href.lineage(written.id)) : "", body: tbl(["Dataset", "Role", ["Rows", "num"], "Schema"], touchedRows.map((d) => [html`${lnk(datasetName(d.uri), href.dataset(d.id), false, d.uri)}${cpi(d.uri)}`, d.role, d.rows != null ? fmtInt(d.rows) : "—", d.schema ? `v${d.schema}` : "—"])) });
  }

  function provenance(r) {
    const l = r.labels || {};
    const items = [];
    if (l.template) items.push(kv("Template", html`${lnk(l.template, href.template(l.template, l.template_version ? { version: l.template_version } : {}))}${l.template_version ? html` <span class="mono">v${l.template_version}</span>` : ""}`, l.sink_template ? html`into ${lnk(l.sink_template, href.template(l.sink_template))}` : ""));
    if (r.tenant) items.push(kv("Tenant", lnk((tenantList() || []).find((t) => t.id === r.tenant)?.name || r.tenant, href.tenant(r.tenant)), `pool ${r.pool || "default"}`));
    else if (r.pool) items.push(kv("Pool", r.pool, "execution pool"));
    if (r.submitted_by) items.push(kv("Submitted by", r.submitted_by.startsWith("trigger:") ? lnk(r.submitted_by, href.trigger(r.submitted_by.slice(8))) : r.submitted_by, "the principal that submitted it"));
    if (l.change) items.push(kv("Approved change", lnk(shortId(l.change), href.change(l.change))));
    if (r.idempotency_key) items.push(kv("Idempotency key", html`<span class="mono">${r.idempotency_key}</span>${cpi(r.idempotency_key)}`, r.timeout_secs ? `timeout ${fmtSecs(r.timeout_secs)}` : ""));
    else if (r.timeout_secs) items.push(kv("Timeout", fmtSecs(r.timeout_secs)));
    if (r.replay_of) items.push(kv("Replayed from", lnk(r.replay_of, tabHref("dlq"), true), "the DLQ location this run replayed"));
    if (r.clock) items.push(kv("Clock", html`<span class="mono">${r.clock}</span>`, "the run’s ${now.*} clock"));
    const deps = Object.entries(r.depends_on || {}).filter(([, a]) => (a || []).length);
    if (deps.length) items.push(kv("Depends on", join(deps.map(([rw, after]) => html`<div class="mono">${rw} ← ${after.join(", ")}</div>`)), "completion order"));
    items.push(kv("Run id", html`<span class="mono" title="${r.run_id}">${shortId(r.run_id)}</span>${cpi(r.run_id)}`, r.attempt ? `attempt ${r.attempt + 1}` : ""));
    const labels = sortedLabels(r.labels);
    const chips = labels.length ? html`<div class="ru-labels">${labels.map(([k, val, prov]) => html`<button type="button" class="chip mono${prov ? " ru-prov" : ""}" data-label="${k}:${val}" title="Filter runs by ${k}=${val}">${k}=${val.length > 40 ? `${val.slice(0, 39)}…` : val}</button>`)}<span class="muted" style="margin-left:auto">click a label to filter runs</span></div>` : "";
    return card({ title: "Provenance", body: grid(4, items), note: chips });
  }

  function logExport(r) {
    const le = r.log_export;
    if (!le || (le.status === "not_configured" && !le.link)) return "";
    const tone = { exported: "ok", pending: "warn", failed: "bad", partially_dropped: "warn" }[le.status] || "";
    const items = [];
    if (le.status !== "not_configured") {
      items.push(kv("Pending", fmtInt(le.pending_lines ?? 0), "lines buffered locally"));
      if (le.dropped_lines) items.push(kv("Dropped", html`<span class="bad">${fmtInt(le.dropped_lines)}</span>`, "before delivery"));
      if (le.last_error) items.push(kv("Last error", html`<span class="mono">${le.last_error}</span>`));
    }
    if (le.link) items.push(kv("Log service", html`<a class="lnk" href="${le.link}" target="_blank" rel="noopener noreferrer">View logs</a>`));
    return card({ icon: "logs", title: "Log export", right: pill(le.status.replace("_", " "), tone), body: items.length ? grid(4, items) : "" });
  }

  const liveLog = (r) => card({ icon: "logs", title: isLive(r) ? "Live log" : "Log tail", right: html`<span id="ru-tail-state">${logState}</span> · ${lnk("open Logs", tabHref("logs"))}`, body: html`<div class="log" id="ru-tail"></div>` });

  function rollbackPanel(r) {
    if (!ui.rbOpen) return "";
    const invs = (r.invocations || []).filter((i) => i.run_id);
    if (!ui.rb) ui.rb = { inv: invs[0] ? invs[0].run_id : "", dry: true, force: false, config: "" };
    const rb = ui.rb;
    return html`${card({ icon: "undo", title: "Roll back this run", aside: "undo what the run wrote, then rewind the bookmark", body: html`<div class="form three">
        ${field("Invocation", select(invs.map((i) => [i.run_id, `${i.row_id} · ${shortId(i.run_id)}`]), rb.inv, { name: "rb-inv" }))}
        <div class="field"><label>Options</label><div style="display:flex;gap:16px;height:30px;align-items:center">${check("Dry run", rb.dry, { name: "rb-dry" })}${check("Force past conflicts", rb.force, { name: "rb-force" })}</div></div>
        <div class="field"><label>&nbsp;</label><div style="display:flex;justify-content:flex-end;gap:8px">${btn("Cancel", { kind: "quiet", data: { action: "rb-close" } })}${btn(rb.dry ? "Dry run" : "Roll back", { kind: rb.dry ? "primary" : "danger", data: { action: "rb-go" }, disabled: ui.rbBusy })}</div></div>
        ${ui.rbNeedsConfig || rb.config ? html`<div class="wide">${field("Config the run was made with", html`<textarea class="area mono" name="rb-config" rows="6" placeholder="The server did not keep this run’s config: paste the pipeline config (YAML) it ran with">${rb.config}</textarea>`, { help: "Needed only when the server did not store the run’s config." })}</div>` : ""}
      </div>`, note: html`Deletes the rows the invocation appended, restores the before-images of keys it upserted, or swaps back the table it overwrote. A key a later run changed is a conflict and blocks the rollback unless forced. The run must have been made with a <code>rollback:</code> block.` })}
      ${ui.rbErr ? alert("Couldn’t roll back.", ui.rbErr, "bad") : ""}
      ${ui.rbReport ? rollbackReport(ui.rbReport) : ""}`;
  }
  function rollbackReport(rp) {
    const title = rp.dry_run ? "Dry run · nothing changed yet" : rp.applied ? "Rolled back" : "Blocked · nothing changed";
    const safe = rp.dry_run && !rp.conflicts;
    const verdict = rp.dry_run
      ? rp.conflicts ? html`<span class="warn">${fmtInt(rp.conflicts)} key${rp.conflicts === 1 ? "" : "s"} changed by a later run.</span> Force restores them anyway.` : html`<span class="ok">Safe to roll back.</span> ${rp.note || "Nothing a later run changed is in the way."}`
      : rp.applied ? html`<span class="ok">Done.</span> Bookmark ${rp.bookmark_rewound ? "rewound" : "unchanged"}; exactly-once watermark ${rp.token_rewound ? "rewound" : "not applicable"}.${rp.note ? ` ${rp.note}` : ""}`
      : html`<span class="bad">Blocked by ${fmtInt(rp.conflicts)} conflict${rp.conflicts === 1 ? "" : "s"}.</span> ${rp.note || "Run it with force to restore past them."}`;
    return card({ icon: rp.applied || rp.dry_run ? "check" : "alert", title, tag: rp.mode, right: html`${rp.row ? html`<span class="mono">${rp.row}</span> · ` : ""}just now`, body: grid(4, [
      kv("Rows deleted", fmtInt(rp.deleted || 0), rp.mode === "upsert" ? "keys this run created" : "appended by this run"),
      kv("Rows restored", fmtInt(rp.restored || 0), rp.mode === "overwrite" ? "the kept previous table" : "from the run journal"),
      kv("Conflicts", fmtInt(rp.conflicts || 0), rp.conflicts ? "keys a later run changed" : "no later run touched these keys"),
      kv("Bookmark", rp.dry_run ? "would rewind" : rp.bookmark_rewound ? "rewound" : "unchanged", rp.sink_kind ? `${rp.sink_kind} ${rp.dataset || ""}` : ""),
    ]), note: html`<div class="note-act"><span>${verdict}</span>${safe ? btn("Roll back for real", { kind: "danger", data: { action: "rb-real" }, perm: "rollback" }) : ""}</div>` });
  }

  const overviewTab = (r) => html`${rollbackPanel(r)}${errorBox(r)}${movedCard(r)}<div class="two">${invSummary(r)}${governance(r)}</div>${touched()}${provenance(r)}${logExport(r)}${liveLog(r)}`;

  function invocationsTab(r) {
    const live = isLive(r);
    const all = (r.invocations || []).slice().sort((a, b) => (b.duration_ms || 0) - (a.duration_ms || 0));
    if (!all.length) return empty("No invocations yet", live ? "Each matrix row’s invocation is listed here when it ends." : "This run recorded no invocations.", { icon: "runs" });
    const pick = all.filter((i) => ui.invSt === "all" || invStatus(i, live)[1] === ui.invSt);
    const maxMs = Math.max(1, ...all.map((i) => i.duration_ms || 0));
    const sts = [...new Set(all.map((i) => invStatus(i, live)[1]))];
    const main = pick.filter((i) => !i.parent_record_key);
    const kids = pick.filter((i) => i.parent_record_key);
    const dur = (i) => html`<div class="ru-dur">${fmtMs(i.duration_ms)}${meter(((i.duration_ms || 0) / maxMs) * 100)}</div>`;
    const rowOf = (i) => [
      html`<span class="mono">${i.row_id}</span>${i.run_id ? html`<div class="muted ru-nowrap ru-sub"><span class="mono" title="${i.run_id}">${shortId(i.run_id)}</span>${cpi(i.run_id)}</div>` : ""}`,
      fmtInt(i.records_written ?? 0), rowsBar(i.rows), batchCell(i.batches), lagText(i.source_lag), dur(i),
    ];
    const errs = pick.filter((i) => i.error && !i.skipped);
    const filterSel = select([["all", "All statuses"], ...sts.map((s) => [s, s])], ui.invSt, { name: "inv-st", cls: "ru-sm" });
    return html`${main.length ? card({ title: "Invocations", aside: "slowest first", right: filterSel, body: html`<div class="tbl-wrap">${tbl(["Row", ["Records", "num"], "Row accounting", "Batches", ["Lag", "num"], "Duration"], main.map(rowOf))}</div>`, note: "Row accounting per invocation: offered = accepted + merged + skipped + DLQ + quarantined + dropped. Hover a bar for offered, filtered and non-finite counts." }) : ""}
      ${kids.length ? card({ title: "Matrix fan-out", aside: "child invocations, one per parent record", right: `${fmtInt(kids.length)} of ${fmtInt(all.filter((i) => i.parent_record_key).length)}`, body: html`<div class="tbl-wrap">${tbl(["Row", "Parent key", ["Records", "num"], "Status", "Duration"], kids.map((i) => { const [t, s] = invStatus(i, live); return [html`<span class="mono">${i.row_id}</span>`, html`<span class="mono">${i.parent_record_key}</span>`, fmtInt(i.records_written ?? 0), dotText(t, s), dur(i)]; }))}</div>` }) : ""}
      ${!main.length && !kids.length ? card({ title: "Invocations", right: filterSel, body: html`<div class="empty-line">No invocation is ${ui.invSt}.</div>` }) : ""}
      ${errs.length ? card({ icon: "alert", title: "Errors", right: fmtInt(errs.length), body: tbl(["Row", "Error"], errs.map((i) => [html`<span class="mono">${i.row_id}${i.parent_record_key ? ` · ${i.parent_record_key}` : ""}</span>`, html`<span class="mono bad">${i.error}</span>${cpi(i.error)}`])), note: lnk("Open the logs", tabHref("logs")) }) : ""}`;
  }

  function logsTab(r) {
    const le = r.log_export;
    const exp = le && le.status !== "not_configured" ? `export · ${fmtInt(le.pending_lines ?? 0)} pending · ${le.status.replace("_", " ")}` : "";
    return html`<div class="ru-logbar">
        <label class="input" style="width:220px">${ic("search")}<input type="search" name="log-q" placeholder="Search the log" value="${ui.logQ}" autocomplete="off"></label>
        <span class="seg" role="group" aria-label="Level">${["All", "Info", "Warn", "Error"].map((x) => html`<span class="${ui.logLv === x ? "on" : ""}" data-lv="${x}" role="button" tabindex="0">${x}</span>`)}</span>
        ${toggle("Follow", ui.follow, { name: "follow" })}
        <span style="margin-left:auto;display:flex;gap:8px">${btn("Download", { icon: "download", data: { action: "log-dl" } })}${logLink ? btn("Log service", { icon: "link", href: logLink }) : ""}</span>
      </div>
      ${card({ icon: "logs", title: isLive(r) ? "Live log" : "Log", tag: html`<span id="ru-log-state">${logState}</span>`, aside: html`<span id="ru-log-count"></span>`, right: exp, body: html`<div class="log ru-logbox" id="ru-log"></div>` })}`;
  }

  function configTab(r) {
    if (cfg === undefined) { loadConfig(); return skelCards(1); }
    if (!cfg) return card({ icon: "code", title: "Config this run used", body: empty("The server did not keep this run’s config", "A config is stored for runs submitted as a config body on a clustered server. Rerun it from Submit, or from the template it came from.", { icon: "code", btns: btn("Open Submit", { href: href.submit(), perm: "config_submit" }) }) });
    const fp = cfg.fingerprint;
    return card({ icon: "code", title: "Config this run used", tag: cfg.redacted ? "redacted" : "", aside: cfg.redacted ? "secrets and sealed params stay masked" : cfg.format || "yaml", right: fp ? html`fingerprint <span class="mono">${shortId(fp)}</span>${cpi(fp)}` : "",
      body: code(cfg.body, { box: false, lang: cfg.format === "json" ? "json" : "yaml" }),
      note: html`Re-run submits this exact config as a new run (needs <b>config_submit</b>). Open in Submit loads it into the form so you can change it first.` });
  }

  function dlqForm() {
    return html`<div class="pair ru-dlqform">${field("Location", input(ui.dlqLoc, { mono: true, name: "dlq-loc", ph: "./dlq/dead-letters.jsonl" }))}<div class="ru-row">${field("Reason", select(DLQ_REASONS, ui.dlqReason, { name: "dlq-reason" }), { cls: "ru-grow" })}${btn("Inspect", { data: { action: "dlq-inspect" } })}</div></div>`;
  }
  function dlqTab(r) {
    const res = ui.dlqRes;
    const head0 = res
      ? card({ icon: "alert", title: `${fmtInt(res.total_envelopes)} row${res.total_envelopes === 1 ? "" : "s"} in the dead-letter queue`, aside: html`<span class="mono">${ui.dlqLoc}</span>`, right: html`<span>inspected ${ago(ui.dlqAt)} ago</span>`,
          body: grid(4, [
            ...Object.entries(res.by_reason || {}).sort((a, b) => b[1] - a[1]).slice(0, 3).map(([k, n]) => kv(k.replace("_", " "), fmtInt(n), "by reason")),
            ...Object.entries(res.by_error_kind || {}).sort((a, b) => b[1] - a[1]).slice(0, Math.max(0, 3 - Object.keys(res.by_reason || {}).length)).map(([k, n]) => kv(k, fmtInt(n), "by error kind")),
            kv("Files", fmtInt(res.files_read || 0), `${fmtInt(res.malformed || 0)} malformed · ${fmtInt(res.non_envelope || 0)} other lines`),
          ]),
          note: dlqForm() })
      : card({ icon: "alert", title: "Dead-letter queue", aside: r.dlq_location ? "this run’s DLQ" : "pick the location to inspect", body: html`<div class="pad">${ui.dlqErr ? alert("Couldn’t inspect this location.", ui.dlqErr, "bad") : html`<span class="muted">${ui.dlqLoc ? "Inspecting…" : "Enter a server-local DLQ location: a .jsonl file, a directory of *.jsonl, or a glob."}</span>`}</div>`, note: dlqForm() });
    const samples = res && (res.sample || []).length ? card({ title: "Samples", right: `${fmtInt(res.sample.length)} of ${fmtInt(res.total_envelopes)}`, body: html`<div class="tbl-wrap">${tbl(["#", "Reason", "Error", "Payload"], res.sample.map((e, k) => [String(k + 1), e.reason || "?", html`<span class="mono ru-clip" title="${e.error_kind ? `${e.error_kind}: ` : ""}${e.error_message || ""}">${String(e.error_message || e.error_kind || "").replace(/^Contract \S+ violated: /, "")}</span>`, html`<span class="ru-nowrap"><span class="mono ru-payload" title="${JSON.stringify(e.payload)}">${JSON.stringify(e.payload)}</span>${cpi(JSON.stringify(e.payload))}</span>`]))}</div>` }) : "";
    const ownCfg = cfg && cfg.body && !ui.replayEdit;
    const replay = card({ icon: "refresh", title: "Replay", aside: "send the rows through a pipeline again", body: html`<div class="form">
        ${field("Replay through", ownCfg ? select([["run", `${runTitle(r)} · this run’s config`], ["paste", "paste another config…"]], "run", { name: "rp-src" }) : html`<textarea class="area mono" name="rp-config" rows="5" placeholder="Paste the pipeline config (YAML) whose sink, transforms, quality and contract to replay through">${ui.replayCfg}</textarea>`, { cls: ownCfg ? "" : "wide" })}
        ${field("Rows", select(DLQ_REASONS, ui.dlqReason, { name: "rp-reason" }))}
        <div class="wide">${check("Dry run first: show what would be written", ui.replayDry, { name: "rp-dry" })}</div>
      </div>${ui.replayRes ? html`<div class="pad" style="padding-top:0">${alert(ui.replayRes.dry ? "Dry run." : "Replayed.", ui.replayRes.text, "ok")}</div>` : ""}`,
      note: html`<div class="note-act"><span>Rows that fail again go to a fresh DLQ beside the source.</span>${btn(ui.replayDry ? "Dry run" : "Replay", { kind: "primary", data: { action: "replay" }, disabled: !ui.dlqLoc || ui.dlqBusy })}</div>` });
    return html`${head0}${samples}<div class="stack" data-perm="dlq_manage">${replay}</div>`;
  }

  function compareTab(r) {
    if (!ui.prev) { loadPrev(r); return skelCards(2); }
    const cands = ui.prev.filter((x) => x.run_id !== r.run_id && Date.parse(x.submitted_at) < Date.parse(r.submitted_at) && !isLive(x));
    if (!cands.length) return empty("Nothing to compare with", `No earlier finished run of ${runTitle(r)} is in the history.`, { icon: "diff" });
    const other = cands.find((x) => x.run_id === ui.cmpWith) || cands.find((x) => x.status === "completed") || cands[0];
    const pct = (a, b) => (b ? Math.round(((a - b) / b) * 100) : null);
    const delta = (a, b, badWhen = "down") => {
      if (a === b) return html`<span class="faint">same</span>`;
      const p = pct(a, b);
      if (p == null) return html`+${fmtInt(a)}`;
      const worse = badWhen === "down" ? p < 0 : p > 0;
      return html`<span class="${Math.abs(p) >= 10 ? (worse ? "bad" : "ok") : ""}">${p > 0 ? "+" : "−"}${Math.abs(p)}%</span>`;
    };
    const sa = accounting(r.invocations || []), sb = accounting(other.invocations || []);
    const fa = (r.invocations || []).filter((i) => i.error && !i.skipped).length, fb = (other.invocations || []).filter((i) => i.error && !i.skipped).length;
    const durA = runSecs(r) || 0, durB = runSecs(other) || 0;
    const dqA = (sa.failed || 0) + (sa.quarantined || 0), dqB = (sb.failed || 0) + (sb.quarantined || 0);
    const rows = [
      ["Records written", fmtInt(r.records_written || 0), fmtInt(other.records_written || 0), delta(r.records_written || 0, other.records_written || 0)],
      ["Duration", secsText(durA), secsText(durB), Math.round(durA) === Math.round(durB) ? html`<span class="faint">same</span>` : `${durA > durB ? "+" : "−"}${fmtSecs(Math.abs(durA - durB))}`],
      ["Rows offered", fmtInt(sa.offered || 0), fmtInt(sb.offered || 0), delta(sa.offered || 0, sb.offered || 0)],
      ["Dead-lettered or quarantined", fmtInt(dqA), fmtInt(dqB), delta(dqA, dqB, "up")],
      ["Estimated cost", money(costOf(r), currencyOf(r)), money(costOf(other), currencyOf(other)), delta(costOf(r), costOf(other), "up")],
      ["Invocations failed", `${fa} of ${(r.invocations || []).length}`, `${fb} of ${(other.invocations || []).length}`, ""],
    ];
    if (ui.cmpCfgFor !== other.run_id) loadCmpConfig(other);
    const cfgCard = ui.cmpCfg === undefined ? "" : cfg && cfg.body && ui.cmpCfg
      ? card({ icon: "code", title: "Config difference", aside: "only lines that changed, previous → this run", body: diff(lineDiff(ui.cmpCfg, cfg.body)) })
      : card({ icon: "code", title: "Config difference", body: html`<div class="empty-line">The server did not keep both configs, so they can’t be compared.</div>` });
    return html`${card({ icon: "diff", title: "What changed between the two runs", aside: html`against ${lnk(`the run ${fmtAge(other.submitted_at, Date.parse(r.submitted_at))} earlier`, runHref(other.run_id))}`, right: select(cands.slice(0, 20).map((x) => [x.run_id, `vs. ${shortDate(x.submitted_at)} · ${x.status === "completed" ? "succeeded" : x.status}`]), other.run_id, { name: "cmp-with", cls: "ru-sm" }),
      body: tbl(["", ["This run", "num"], ["Previous", "num"], ["Change", "num"]], rows) })}${cfgCard}`;
  }
  async function loadPrev(r) {
    ui.prev = [];
    const p = new URLSearchParams({ limit: "50" });
    if (r.labels && r.labels.pipeline) p.append("label", `pipeline:${r.labels.pipeline}`);
    else if (r.name) p.set("name", r.name);
    withTenant(p);
    try { ui.prev = ((await api(`/v1/runs?${p}`)).runs || []).filter((x) => runTitle(x) === runTitle(r)); } catch { ui.prev = []; }
    if (r.run_id === selected) paintDetail();
  }
  async function loadCmpConfig(other) {
    ui.cmpCfgFor = other.run_id;
    ui.cmpCfg = undefined;
    loadConfig();
    let body = null;
    try { body = (await api(`/v1/runs/${encodeURIComponent(other.run_id)}/config`)).body || null; } catch { body = other.config_body || null; }
    if (ui.cmpCfgFor !== other.run_id) return;
    ui.cmpCfg = body;
    paintDetail();
  }

  function paintDetail() {
    if (!selected) return;
    L.select(true);
    if (recErr && !rec) {
      setCrumbs(["Operate", { label: "Runs", href: listHref() }, shortId(selected)]);
      const nf = recErr.status === 404;
      patch(L.detail, html`${dhead("", "", backLink())}${nf ? empty("Run not found", "It may have been deleted, or it aged out of the run history.", { icon: "runs", btns: btn("All", { href: listHref() }) }) : errorAlert("this run", recErr)}`);
      L.detail.querySelector("[data-retry]")?.addEventListener("click", loadDetail);
      return;
    }
    if (!rec) { patch(L.detail, html`${dhead("", "", backLink())}<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`); return; }
    const r = rec;
    setCrumbs(["Operate", { label: "Runs", href: listHref() }, runTitle(r)]);
    const t = tabsFor(r).map((x) => x[2]).includes(dtab) ? dtab : "overview";
    if (t === "overview") loadTouched(r);
    if (cfg === undefined && (t !== "overview" || !isLive(r))) loadConfig();
    if (t === "dlq" && ui.dlqLoc && !ui.dlqRes && !ui.dlqErr && !ui.dlqBusy && ui.dlqAuto !== ui.dlqLoc) { ui.dlqAuto = ui.dlqLoc; inspectDlq(); }
    let right = actions(r);
    if (t === "config" && cfg && cfg.body) right = html`${btn("Open in Submit", { icon: "edit", href: href.submit({ from: r.run_id }), perm: "config_submit" })}${btn("Re-run", { kind: "primary", icon: "play", perm: "config_submit", data: { action: "rerun" } })}`;
    if (t === "dlq") right = btn("Discard…", { kind: "danger", icon: "trash", perm: "dlq_manage", data: { action: "dlq-discard" }, disabled: !ui.dlqLoc });
    const body = t === "invocations" ? invocationsTab(r) : t === "logs" ? logsTab(r) : t === "config" ? configTab(r) : t === "dlq" ? dlqTab(r) : t === "compare" ? compareTab(r) : overviewTab(r);
    patch(L.detail, html`${dhead(dtabs(tabsFor(r), t, { hrefFor: tabHref }), right, backLink())}${runHead(r)}${body}`);
    paintLogs();
    setContext(html`${clusterChip()}${chip(html`run ${shortId(r.run_id)}${cpi(r.run_id)}`, "mono")}`);
  }

  // ---- detail events ----
  L.detail.addEventListener("click", (e) => {
    const lb = e.target.closest("[data-label]");
    if (lb) { addLabel(lb.dataset.label); return; }
    const lv = e.target.closest("[data-lv]");
    if (lv) { ui.logLv = lv.dataset.lv; L.detail.querySelectorAll("[data-lv]").forEach((x) => x.classList.toggle("on", x === lv)); paintLogs(); return; }
    const a = e.target.closest("[data-action]");
    if (!a || a.disabled || a.classList.contains("dis")) return;
    const act = a.dataset.action;
    if (act === "cancel") cancelRun();
    else if (act === "delete") deleteRun();
    else if (act === "rerun") rerun();
    else if (act === "rollback") { ui.rbOpen = !ui.rbOpen; if (dtab !== "overview") navigate(tabHref("overview")); else paintDetail(); }
    else if (act === "rb-close") { ui.rbOpen = false; ui.rbReport = null; ui.rbErr = null; paintDetail(); }
    else if (act === "rb-go") doRollback(ui.rb.dry);
    else if (act === "rb-real") doRollback(false);
    else if (act === "filter-pipeline") setQuery({ label: [...new Set([...f.labels, `pipeline:${rec.labels.pipeline}`])] });
    else if (act === "filter-name") setQuery({ q: rec.name || "" });
    else if (act === "log-dl") downloadLogs();
    else if (act === "dlq-inspect") { ui.dlqRes = null; ui.dlqErr = null; inspectDlq(); }
    else if (act === "dlq-discard") discardDlq();
    else if (act === "replay") replayDlq();
  });
  L.detail.addEventListener("keydown", (e) => {
    const lv = e.target.closest("[data-lv]");
    if (lv && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); lv.click(); }
    if (e.target.name === "dlq-loc" && e.key === "Enter") { e.preventDefault(); ui.dlqRes = null; ui.dlqErr = null; inspectDlq(); }
  });
  L.detail.addEventListener("input", (e) => {
    const n = e.target.name;
    if (n === "log-q") { ui.logQ = e.target.value; paintLogs(); }
    else if (n === "dlq-loc") ui.dlqLoc = e.target.value.trim();
    else if (n === "rb-config" && ui.rb) ui.rb.config = e.target.value;
    else if (n === "rp-config") ui.replayCfg = e.target.value;
  });
  L.detail.addEventListener("change", (e) => {
    const n = e.target.name;
    const box = e.target.closest(".check, .toggle");
    if (box) box.classList.toggle("on", e.target.checked);
    if (n === "follow") { ui.follow = e.target.checked; paintLogs(); }
    else if (n === "inv-st") { ui.invSt = e.target.value; paintDetail(); }
    else if (n === "rb-inv") ui.rb.inv = e.target.value;
    else if (n === "rb-dry") { ui.rb.dry = e.target.checked; paintDetail(); }
    else if (n === "rb-force") ui.rb.force = e.target.checked;
    else if (n === "dlq-reason" || n === "rp-reason") { ui.dlqReason = e.target.value; ui.dlqRes = null; inspectDlq(); }
    else if (n === "rp-dry") { ui.replayDry = e.target.checked; paintDetail(); }
    else if (n === "rp-src" && e.target.value === "paste") { ui.replayEdit = true; ui.replayCfg = ""; paintDetail(); }
    else if (n === "cmp-with") { ui.cmpWith = e.target.value; paintDetail(); }
  });

  async function cancelRun() {
    const r = rec;
    const res = await dialog(`Cancel ${runTitle(r)}?`, html`<p>The run stops at the next page boundary and flushes what it has written, so the sink and the bookmark stay consistent. Nothing written so far is undone.</p>${card({ body: grid(2, [kv("Rows moved", fmtInt(r.records_written || 0)), kv("Invocations finished", fmtInt((r.invocations || []).length), "they resume from their bookmarks next run")]) })}`, html`${btn("Keep running", { kind: "quiet", data: { act: "no" } })}${btn("Cancel run", { kind: "danger", icon: "x", data: { act: "yes" } })}`, { icon: "alert" });
    if (!res || res.act !== "yes") return;
    try { await api(`/v1/runs/${encodeURIComponent(r.run_id)}/cancel`, { method: "POST" }); toast(html`Cancel requested for <b>${runTitle(r)}</b>`, "ok"); loadDetail(); loadList("refresh"); }
    catch (e) { toast(`Couldn’t cancel: ${e.message}`, "bad"); }
  }
  async function deleteRun() {
    const r = rec;
    const res = await dialog("Delete this run from history?", html`<p>Removes the run record, its invocations and its log tail from the server. The data it wrote stays where it is; to undo the data use <b>Roll back</b>.</p>`, html`${btn("Keep", { kind: "quiet", data: { act: "no" } })}${btn("Delete run", { kind: "danger", icon: "trash", data: { act: "yes" } })}`, { icon: "trash" });
    if (!res || res.act !== "yes") return;
    try {
      await api(`/v1/runs/${encodeURIComponent(r.run_id)}`, { method: "DELETE" });
      toast(html`Deleted <b>${runTitle(r)}</b> from the history`, "ok");
      runs = runs.filter((x) => x.run_id !== r.run_id);
      navigate(listHref());
      loadList("refresh");
    } catch (e) { toast(`Couldn’t delete: ${e.message}`, "bad"); }
  }
  async function rerun() {
    if (!cfg || !cfg.body) return;
    try {
      const body = { config: cfg.body, config_format: cfg.format === "json" ? "json" : "yaml" };
      if (rec.name) body.name = rec.name;
      const resp = await api("/v1/runs", { method: "POST", body });
      if (resp.status === "pending_approval") { toast("The re-run needs approval", "info", { href: href.change(resp.change_id), label: "open the change" }); return; }
      toast(html`Run <b>${runTitle(rec)}</b> submitted`, "ok", { href: href.run(resp.run_id), label: "open" });
      loadList("refresh");
    } catch (e) { toast(`Couldn’t re-run: ${e.message}`, "bad"); }
  }
  async function doRollback(dry) {
    const rb = ui.rb;
    if (!dry) {
      const ok = await dialog("Roll back for real?", html`<p>This deletes and restores rows in the destination and rewinds the bookmark. It can’t be undone from here.</p>`, html`${btn("Keep", { kind: "quiet", data: { act: "no" } })}${btn("Roll back", { kind: "danger", icon: "undo", data: { act: "yes" } })}`, { icon: "undo" });
      if (!ok || ok.act !== "yes") return;
    }
    ui.rbBusy = true; ui.rbErr = null; paintDetail();
    const body = { invocation_id: rb.inv || undefined, dry_run: !!dry, force: !!rb.force };
    if (rb.config.trim()) body.config = rb.config;
    try {
      ui.rbReport = await api(`/v1/runs/${encodeURIComponent(rec.run_id)}/rollback`, { method: "POST", body });
      if (!dry) { toast(ui.rbReport.applied ? "Rolled back" : "Rollback blocked", ui.rbReport.applied ? "ok" : "bad"); loadDetail(); }
    } catch (e) {
      ui.rbErr = e.message;
      if (e.status === 422) ui.rbNeedsConfig = true;
    }
    ui.rbBusy = false;
    paintDetail();
  }
  function downloadLogs() {
    const blob = new Blob([logs.filter((l) => !l.note).map((l) => l.text).join("\n") + "\n"], { type: "text/plain" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `${runTitle(rec).replace(/[^\w.-]+/g, "_")}-${rec.run_id}.log`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  }
  async function inspectDlq() {
    if (!ui.dlqLoc) { paintDetail(); return; }
    ui.dlqBusy = true;
    try {
      ui.dlqRes = await api("/v1/dlq/inspect", { method: "POST", body: { location: ui.dlqLoc, reason: ui.dlqReason || undefined, limit: 5 } });
      ui.dlqAt = new Date().toISOString();
      ui.dlqErr = null;
    } catch (e) { ui.dlqErr = e.message; ui.dlqRes = null; }
    ui.dlqBusy = false;
    paintDetail();
  }
  async function discardDlq() {
    if (!ui.dlqLoc) return;
    const res = await dialog("Discard these dead-letter rows?", html`<p>Matching envelopes at <span class="mono">${ui.dlqLoc}</span>${ui.dlqReason ? ` with reason ${ui.dlqReason}` : ""} are moved to an archive beside the file, or deleted.</p>${check("Delete them instead of archiving", false, { name: "del" })}`, html`${btn("Keep", { kind: "quiet", data: { act: "no" } })}${btn("Discard", { kind: "danger", icon: "trash", data: { act: "yes" } })}`, { icon: "trash", onOpen: (el) => el.addEventListener("change", (e) => e.target.closest(".check")?.classList.toggle("on", e.target.checked)) });
    if (!res || res.act !== "yes") return;
    try {
      const o = await api("/v1/dlq/discard", { method: "POST", body: { location: ui.dlqLoc, reason: ui.dlqReason || undefined, delete: !!res.values.del } });
      toast(`Discarded ${fmtInt(o.discarded)} row${o.discarded === 1 ? "" : "s"} across ${fmtInt(o.files_rewritten)} file${o.files_rewritten === 1 ? "" : "s"}`, "ok");
      ui.dlqRes = null;
      inspectDlq();
    } catch (e) { toast(`Couldn’t discard: ${e.message}`, "bad"); }
  }
  async function replayDlq() {
    const own = cfg && cfg.body && !ui.replayEdit;
    const config = own ? cfg.body : ui.replayCfg.trim();
    if (!config) { toast("Paste a config to replay through.", "bad"); return; }
    ui.dlqBusy = true; paintDetail();
    try {
      const o = await api("/v1/dlq/replay", { method: "POST", body: { config, config_format: own && cfg.format === "json" ? "json" : "yaml", from: ui.dlqLoc, reason: ui.dlqReason || undefined, dry_run: ui.replayDry } });
      ui.replayRes = { dry: ui.replayDry, text: ui.replayDry ? `${fmtInt(o.candidates ?? 0)} row(s) would be replayed.` : `${fmtInt(o.records_written ?? 0)} record(s) written.` };
      if (!ui.replayDry) { toast(`Replayed ${fmtInt(o.records_written ?? 0)} record(s)`, "ok"); ui.dlqRes = null; inspectDlq(); }
    } catch (e) { toast(`Couldn’t replay: ${e.message}`, "bad"); }
    ui.dlqBusy = false;
    paintDetail();
  }

  function choose(rid) {
    resetDetail();
    selected = rid;
    L.list.querySelectorAll(".row").forEach((x) => { const on = x.dataset.id === rid; x.classList.toggle("on", on); if (on) x.setAttribute("aria-current", "true"); else x.removeAttribute("aria-current"); });
    L.detail.__html = "";
    L.detail.scrollTop = 0;
    paintList();
    if (!rid) { paintSummary(); return; }
    paintDetail();
    loadDetail();
    startLogs(rid);
  }

  // ---- boot ----
  paintList();
  if (!selected) paintSummary();
  await loadList("fresh");
  if (!alive) return undefined;
  choose(selected);
  const stopList = poll(() => loadList(runs.length ? "refresh" : "fresh"), () => (listErr ? 3000 : runs.some(isLive) ? 3000 : 15000));
  const stopDetail = poll(() => (selected && (!rec || isLive(rec) || recErr) ? loadDetail() : null), 3000);
  const stopSummary = poll(() => (!selected && summary ? loadSummary() : null), 30000);

  return {
    teardown() { alive = false; stopList(); stopDetail(); stopSummary(); logCtrl?.abort(); },
    update(p) {
      const next = readFilters(p.query);
      const listChanged = JSON.stringify({ ...next, open: 0 }) !== JSON.stringify({ ...f, open: 0 });
      const openChanged = next.open !== f.open;
      f = next;
      dtab = p.query.get("tab") || "overview";
      if (listChanged) { cursor = null; loadList("fresh"); }
      if ((p.id || null) !== selected) choose(p.id || null);
      else {
        if (openChanged || listChanged) paintList();
        if (selected) paintDetail(); else paintSummary();
      }
    },
  };
}

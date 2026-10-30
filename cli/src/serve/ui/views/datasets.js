// Datasets (#998): split pane — the Data Movement Catalog on the left, one
// dataset on the right (#/catalog/<id>?tab=overview|schema|profile|lineage|
// activity|outputs). Filters, tabs, the selected column, the compared schema
// versions and the annotate form all live in the URL.
import { api } from "../api.js";
import {
  html, raw, v, ic, row, ring, sect, tabs, dtabs, dhead, ibtn, input, select, check, toggle, field, empty, skelRows, skelCards,
  head, stats, card, tbl, pill, chip, tag, ch, kv, grid, btn, lnk, cpi, spark, alert, diff, menu, meta, join, shortId,
} from "../components.js";
import { layout, setQuery, patch, errorAlert, hashFor, ago, at, poll, sections, dialog, confirmAction, toast } from "../page.js";
import { setCrumbs, setContext } from "../shell.js";
import { fmtInt, fmtCompact, fmtAge } from "../utils.js";
import { formatTs, zonedWallToUtc, utcToZonedWall } from "../tz.js";
import { href, datasetName } from "../links.js";
import { withTenant } from "../tenant.js";

const DAY = 86400000;
const SECTION_CAP = 200;
const MAX_FETCH_PAGES = 100;
const CELL_MAX = 240;
const PREVIEWABLE = new Set(["jsonl", "csv", "parquet", "file"]);
const CONSUMER_KINDS = ["dashboard", "model", "export", "report", "notebook", "service", "other"];
const DTABS = [["Overview", "overview"], ["Schema", "schema"], ["Profile", "profile"], ["Lineage", "lineage"], ["Activity", "activity"], ["Outputs", "outputs"]];
const ROLE_TAB = { All: "", Destinations: "sink", Sources: "source" };

/** A route that is not wired (server built without `catalog`) is a bare 404. */
export const catalogUnavailable = (e) => !!e && e.status === 404 && !e.code;
const CATALOG_MISSING = "The Data Movement Catalog is not available on this server: faucet was built without the catalog feature.";

const ageMs = (iso) => (iso ? Date.now() - Date.parse(iso) : Infinity);
const isSink = (d) => (d.roles || []).includes("sink");
const colsOf = (schema) => Object.keys((schema && schema.properties) || {});
const pct = (x) => `${(x * 100).toFixed(x > 0 && x < 0.01 ? 2 : 1)}%`;
const tw = (t) => html`<div class="ds-tw">${t}</div>`;
const plural = (n, w) => `${fmtInt(n)} ${w}${n === 1 ? "" : "s"}`;

/** Freshness of a dataset: fresh within a day, stale after, never succeeded. */
function freshness(d) {
  if (!d.last_success) return { key: "never", tone: "", shape: "hollow", pill: pill("Never succeeded", "", "clock") };
  const a = ageMs(d.last_success);
  if (a > DAY) return { key: "stale", tone: "warn", shape: "half", pill: pill(`Stale · ${fmtAge(d.last_success)}`, "warn", "clock") };
  return { key: "fresh", tone: "ok", shape: "", pill: pill(`Fresh · ${fmtAge(d.last_success)}`, "ok", "check") };
}

/** Median gap between recorded runs, in ms (null under 3 runs). */
function cadence(stats) {
  const t = (stats || []).map((s) => Date.parse(s.recorded_at)).filter(Number.isFinite).sort((a, b) => a - b);
  if (t.length < 3) return null;
  const gaps = t.slice(1).map((x, i) => x - t[i]).sort((a, b) => a - b);
  return gaps[Math.floor(gaps.length / 2)];
}
const dur = (ms) => {
  const s = Math.round(ms / 1000);
  if (s < 90) return `${s}s`;
  if (s < 5400) return `${Math.round(s / 60)} min`;
  if (s < 172800) return `${Math.round(s / 3600)}h`;
  return `${Math.round(s / 86400)}d`;
};

const diffName = (c) => (typeof c === "string" ? c : c.column || c.name || "");
function diffParts(df) {
  if (!df) return [];
  const t = (x) => (x == null ? "" : typeof x === "string" ? x : typeText(x));
  const out = [];
  for (const c of df.added || []) out.push(["ok", "+", diffName(c), t(c.to ?? c.type)]);
  for (const c of df.widened || []) out.push(["info", "~", diffName(c), c.from != null && c.to != null ? `${t(c.from)} → ${t(c.to)}` : ""]);
  for (const c of df.changed || []) out.push(["warn", "!", diffName(c), c.from != null && c.to != null ? `${t(c.from)} → ${t(c.to)}` : ""]);
  for (const c of df.removed || []) out.push(["bad", "−", diffName(c), "removed"]);
  return out;
}
const diffBadges = (df) => join(diffParts(df).map(([tone, sym, name, extra]) => html`<span class="${tone}" style="white-space:nowrap">${sym} <span class="mono">${name}</span></span>${extra ? html` <span class="muted">${extra}</span>` : ""}`), html`<br>`);

function typeText(p) {
  if (!p) return "any";
  const t = Array.isArray(p.type) ? p.type.filter((x) => x !== "null") : p.type ? [p.type] : [];
  const base = t[0] || (p.anyOf || p.oneOf ? "one of" : "any");
  return p.format ? (p.format === "date-time" ? "timestamp" : p.format) : base;
}
const nullable = (p) => !!p && ((Array.isArray(p.type) && p.type.includes("null")) || (p.anyOf || p.oneOf || []).some((a) => a.type === "null"));

/** A line diff (longest common subsequence) between two arrays of lines. */
function lineDiff(a, b) {
  const n = a.length, m = b.length;
  const dp = Array.from({ length: n + 1 }, () => new Array(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
  const out = [];
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) { out.push(["", a[i]]); i++; j++; }
    else if (dp[i + 1][j] >= dp[i][j + 1]) out.push(["-", a[i++]]);
    else out.push(["+", b[j++]]);
  }
  while (i < n) out.push(["-", a[i++]]);
  while (j < m) out.push(["+", b[j++]]);
  return out;
}
// Sorted by name: the recorded property order is not stable between versions.
const schemaLines = (s) => colsOf(s).sort().map((k) => `  ${k}: ${JSON.stringify(s.properties[k])}`);

/** How a destination column was derived from its source columns. */
function howDerived(into, from) {
  if (!from || !from.length) return "no source";
  if (from.length > 1) return "combined";
  return from[0] === into ? "identity" : "rename";
}

// ── data ─────────────────────────────────────────────────────────────────────

async function fetchAllDatasets() {
  const acc = [];
  const seen = new Set();
  let cursor = null;
  for (let i = 0; i < MAX_FETCH_PAGES; i++) {
    const p = new URLSearchParams({ limit: "200" });
    if (cursor) p.set("cursor", cursor);
    const data = await api(`/v1/catalog/datasets?${p}`);
    for (const d of data.datasets || []) if (!seen.has(d.id)) { seen.add(d.id); acc.push(d); }
    cursor = data.next_cursor || null;
    if (!cursor) break;
  }
  return acc;
}

/** Recent runs that wrote `id`: `GET /v1/runs?dataset_id=`, else the newest
 *  200 runs filtered by their invocations' dataset. */
async function runsWriting(id) {
  try {
    const r = await api(`/v1/runs?${withTenant(new URLSearchParams({ dataset_id: id, limit: "50" }))}`);
    return { runs: r.runs || [], exact: true };
  } catch (e) {
    if (e.status !== 400) throw e;
    const r = await api(`/v1/runs?${withTenant(new URLSearchParams({ limit: "200" }))}`);
    const all = r.runs || [];
    return { runs: all.filter((x) => (x.invocations || []).some((i) => i.usage && i.usage.dataset_id === id)), all, exact: false };
  }
}

const runTone = (s) => ({ running: "info", sharded: "info", queued: "", pending: "", completed: "ok", failed: "bad", cancelled: "warn" })[s] || "";
const runDot = (s) => html`<span class="${runTone(s)}" style="white-space:nowrap">● ${s}</span>`;
const runRows = (r, id) => (r.invocations || []).filter((i) => !id || (i.usage && i.usage.dataset_id === id)).reduce((n, i) => n + (i.records_written || 0), 0) || r.records_written || 0;

// ── local outputs (also used server-wide from the catalogue) ─────────────────

const STATE_HINT = {
  present: "on disk",
  expired: "cleaned; the run record is kept",
  external: "faucet wrote this file but did not create it, so it is never cleaned and not previewed",
  replaced: "faucet overwrote a file it did not create: previewable, never cleaned",
};
const STATE_TONE = { present: "ok", expired: "muted", external: "muted", replaced: "" };
function skipReason(o) {
  return {
    pre_existing: "not deleted: faucet wrote this file but did not create it",
    already_deleted: "already cleaned",
    not_on_disk: "already gone from disk; marked cleaned",
    in_flight: "a run is still writing it; will be retried",
    delete_failed: `could not delete: ${o.error || "unknown error"}`,
  }[o.skipped] || "skipped";
}
function fmtBytes(b) {
  if (b < 1024) return `${b} B`;
  if (b < 1048576) return `${(b / 1024).toFixed(1)} KiB`;
  if (b < 1073741824) return `${(b / 1048576).toFixed(1)} MiB`;
  return `${(b / 1073741824).toFixed(1)} GiB`;
}
function report(rep, what) {
  if (rep.deleted > 0) toast(`${what}: ${plural(rep.deleted, "file")}, ${fmtBytes(rep.bytes || 0)} reclaimed`, "ok");
  const skipped = (rep.outputs || []).filter((o) => o.skipped);
  if (!rep.deleted && !skipped.length) toast("Nothing to clean.", "info");
  for (const o of skipped.slice(0, 3)) toast(`${datasetName(`file://${o.path}`)}: ${skipReason(o)}`, o.skipped === "delete_failed" ? "bad" : "info");
  if (skipped.length > 3) toast(`…and ${skipped.length - 3} more skipped.`, "info");
}
const shortPath = (p) => { const s = String(p || "").replace(/\/\.\//g, "/").split("/").filter(Boolean); return s.slice(-3).join("/"); };

function cellHtml(x) {
  if (x === undefined) return html`<span class="null" title="this record has no such field">—</span>`;
  if (x === null) return html`<span class="null">null</span>`;
  const t = typeof x === "string" ? x : JSON.stringify(x);
  return t.length <= CELL_MAX ? html`${t}` : html`<span title="${t.slice(0, 2000)}">${t.slice(0, CELL_MAX)}…</span>`;
}

/** The local-outputs card + preview, scoped to one dataset or the whole server. */
function mountOutputs(host, { datasetId = null, onCount = null } = {}) {
  let showExpired = false;
  let data = null;
  let err = null;
  let previewId = "";
  let preview = null;
  let previewErr = null;
  let rowsWanted = "";

  const paint = () => {
    if (err) { host.innerHTML = v(catalogUnavailable(err) ? alert("Local outputs are not available.", "This server was built without the catalog feature.", "") : errorAlert("the local outputs", err)); host.querySelector("[data-retry]")?.addEventListener("click", load); return; }
    if (!data) { host.innerHTML = v(skelCards(1)); return; }
    const outs = data.outputs || [];
    const manage = data.can_manage ? menu(html`${ic("more")}Manage`, [
      { heading: "Manage local outputs" },
      { label: datasetId ? "Purge this dataset’s outputs older than…" : "Purge older than…", icon: "clock", hint: data.gc_enabled ? `${data.retention_days} days` : "", action: "lo-purge" },
      datasetId ? { label: "Clean this dataset now", icon: "trash", action: "lo-clean-ds" } : null,
      "-",
      { label: "Clean every local output", icon: "trash", cls: "danger", action: "lo-clean-all" },
    ], { perm: "local_output_manage" }) : "";
    const rowsHtml = outs.map((o) => {
      const canPrev = data.preview_enabled && PREVIEWABLE.has(o.kind) && (o.state === "present" || o.state === "replaced");
      return {
        cls: o.id === previewId ? "sel" : "",
        cells: [
          html`<span style="white-space:nowrap"><span class="mono" title="${o.path}">${shortPath(o.path)}</span>${cpi(o.path)}</span>`,
          html`<span style="white-space:nowrap" title="${STATE_HINT[o.state] || ""}"><span class="dot ${STATE_TONE[o.state] ?? "muted"}"></span>${o.state}</span>`,
          o.last_written_at ? ago(o.last_written_at, " ago") : "—",
          datasetId ? lnk(shortId(o.run_id), href.run(o.run_id), true, o.run_id) : lnk(datasetName(o.dataset_uri), href.dataset(o.dataset_id, "outputs")),
          html`<span style="display:inline-flex;gap:6px">${canPrev ? btn(o.id === previewId ? "Hide" : "Preview", { cls: "sm", data: { preview: o.id } }) : ""}${data.can_manage && o.state === "present" ? btn("Delete", { cls: "sm", kind: "danger", perm: "local_output_manage", data: { del: o.id } }) : ""}</span>`,
        ],
      };
    });
    const sub = `${plural(outs.length, "tracked")}${data.gc_enabled ? ` · cleaned after ${plural(data.retention_days, "day")}` : " · automatic cleanup off"}`;
    const listCard = html`<div class="card"><div class="h">${ic("download")}<b>Local outputs</b><span class="muted">${sub}</span><span class="r">${toggle("Show cleaned", showExpired, { a: { "data-lo-expired": "1" } })}${ibtn("refresh", { title: "Reload", data: { "lo-refresh": "1" } })}${manage}</span></div>${
      outs.length ? tbl(["Path", "State", "Written", datasetId ? "Run" : "Dataset", ""], rowsHtml) : html`<div class="empty-line">No local output files tracked${datasetId ? " for this dataset" : ""} yet. A run with a file, jsonl, csv or parquet sink adds them here.</div>`}</div>`;
    let prevCard = "";
    const po = outs.find((o) => o.id === previewId);
    if (po) {
      const caps = { def: data.preview_default_rows, max: data.preview_max_rows };
      const controls = html`<label class="input" style="height:26px;width:76px"><input type="number" name="lo-rows" min="1"${raw(caps.max ? ` max="${caps.max}"` : "")} value="${rowsWanted || caps.def || 50}" aria-label="Rows to load"></label>${btn("Load", { cls: "sm", data: { "lo-load": "1" } })}${btn("All", { cls: "sm", kind: "quiet", data: { "lo-all": "1" }, title: caps.max ? `up to this server’s ceiling of ${caps.max} rows` : "the whole output" })}`;
      let body;
      let aside = shortPath(po.path).split("/").pop();
      if (previewErr) body = html`<div class="pad">${alert("Couldn’t preview this output.", previewErr.message || String(previewErr), "bad")}</div>`;
      else if (!preview) body = html`<div class="pad">${skelRows(3)}</div>`;
      else if (!preview.row_count) body = html`<div class="empty-line">This output has no rows.</div>`;
      else {
        const cols = preview.columns && preview.columns.length ? preview.columns : null;
        const isObj = (r) => r !== null && typeof r === "object" && !Array.isArray(r);
        body = html`<div style="overflow:auto"><table class="ds-pre"><thead><tr>${cols ? cols.map((c) => html`<th>${c}</th>`) : html`<th>value</th>`}</tr></thead><tbody>${preview.rows.map((r) => (!cols || !isObj(r) ? html`<tr><td${raw(cols ? ` colspan="${cols.length}"` : "")}>${cellHtml(r)}</td></tr>` : html`<tr>${cols.map((c) => html`<td class="${r[c] == null ? "null" : ""}">${cellHtml(r[c])}</td>`)}</tr>`))}</tbody></table></div>`;
        aside = `${aside} · ${preview.truncated ? `first ${fmtInt(preview.row_count)} rows · output has more` : `${plural(preview.row_count, "row")} · whole output`} · ${preview.elapsed_ms} ms`;
      }
      prevCard = card({ icon: "columns", title: "Preview", aside, right: controls, body, note: html`<b>—</b> is a missing field; <b>null</b> is an explicit null. Long values are cut at ${CELL_MAX} characters.` });
    }
    patch(host, html`${listCard}${prevCard}`);
  };

  async function load() {
    const p = new URLSearchParams();
    if (datasetId) p.set("dataset_id", datasetId);
    if (showExpired) p.set("include_expired", "true");
    try { data = await api(`/v1/local-outputs?${p}`); err = null; } catch (e) { err = e; }
    if (onCount && data) onCount((data.outputs || []).length);
    paint();
  }
  async function loadPreview(n) {
    preview = null; previewErr = null; paint();
    try { preview = await api(`/v1/local-outputs/${encodeURIComponent(previewId)}/preview?row_count_to_load=${n}`); } catch (e) { previewErr = e; }
    paint();
  }
  async function cleanup(payload, what) {
    try { report(await api("/v1/local-outputs/cleanup", { method: "POST", body: payload }), what); await load(); } catch (e) { toast(e.message, "bad"); }
  }

  host.addEventListener("change", (e) => {
    if (e.target.closest("[data-lo-expired]")) { showExpired = e.target.checked; load(); }
    if (e.target.name === "lo-rows") rowsWanted = e.target.value;
  });
  host.addEventListener("click", async (e) => {
    const t = e.target.closest("[data-preview], [data-del], [data-lo-refresh], [data-lo-load], [data-lo-all], [data-action]");
    if (!t) return;
    if (t.dataset.loRefresh) return load();
    if (t.dataset.preview) {
      if (previewId === t.dataset.preview) { previewId = ""; preview = null; paint(); return; }
      previewId = t.dataset.preview;
      return loadPreview(data.preview_default_rows || 50);
    }
    if (t.dataset.loLoad) {
      const n = Math.floor(Number(host.querySelector("input[name=lo-rows]")?.value));
      const capped = !Number.isFinite(n) || n < 1 ? data.preview_default_rows || "all" : data.preview_max_rows ? Math.min(n, data.preview_max_rows) : n;
      return loadPreview(capped);
    }
    if (t.dataset.loAll) return loadPreview("all");
    if (t.dataset.del) {
      const o = (data.outputs || []).find((x) => x.id === t.dataset.del);
      if (!(await confirmAction("Delete this output now?", html`<span class="mono">${o ? o.path : t.dataset.del}</span> is removed from disk. Run history, the catalogue and lineage are kept.`, { label: "Delete", danger: true }))) return;
      try { report(await api(`/v1/local-outputs/${encodeURIComponent(t.dataset.del)}`, { method: "DELETE" }), "deleted"); await load(); } catch (er) { toast(er.message, "bad"); }
      return;
    }
    const act = t.dataset.action;
    if (act === "lo-purge") {
      const r = await dialog(datasetId ? "Purge this dataset’s outputs" : "Purge local outputs", html`<div>Delete tracked outputs older than this many days. Only files faucet created are deleted.</div>${field("Older than (days)", input(String(data.retention_days ?? 7), { type: "number", name: "days", a: { min: "0" } }), { help: "0 means every tracked output, even inside its retention window." })}`, html`${btn("Cancel", { kind: "quiet", data: { act: "no" } })}${btn("Purge", { kind: "danger", data: { act: "yes" } })}`, { icon: "clock" });
      if (!r || r.act !== "yes") return;
      const days = Number(r.values.days);
      if (!Number.isFinite(days) || days < 0) return toast("Enter a number of days (0 or more).", "bad");
      if (datasetId) return cleanup({ dataset_id: datasetId, older_than_days: days }, `purged outputs older than ${plural(days, "day")}`);
      if (days === 0 && !(await confirmAction("Delete every tracked output?", "This includes files still inside their retention window. Run history, catalogue entries and lineage are not touched.", { label: "Delete all", danger: true }))) return;
      return cleanup({ older_than_days: days, confirm: days === 0 }, `purged outputs older than ${plural(days, "day")}`);
    }
    if (act === "lo-clean-ds" || act === "lo-clean-all") {
      const all = act === "lo-clean-all";
      if (!(await confirmAction(all ? "Clean every local output?" : "Clean this dataset’s outputs?", "Files still inside their retention window are deleted too. Only files faucet created are deleted; run history, catalogue entries and lineage are not touched.", { label: "Clean", danger: true }))) return;
      return cleanup(all ? { all: true, confirm: true } : { dataset_id: datasetId }, "cleaned local outputs");
    }
  });
  load();
  return { reload: load };
}

// ── page ─────────────────────────────────────────────────────────────────────

export async function renderDatasets(host, params) {
  const L = layout(host, "split");
  const sec = sections("faucet.datasets.sections");
  let all = null;
  let listErr = null;
  let edges = null; // whole lineage, for readers / ownership gaps (best effort)
  let cap = SECTION_CAP;
  let d = null;
  let dErr = null;
  let dId = "";
  let outputsCount = null;
  let activity = null;
  let changes = null;
  let outputsHost = null;
  let annotateErrors = {};
  let q = readQuery(params);

  function readQuery(p) {
    const s = p.query;
    return {
      id: p.id || "", role: s.get("role") || "All", q: s.get("q") || "", kind: (s.get("kind") || "").split(",").filter(Boolean),
      from: s.get("from") || "", to: s.get("to") || "", filters: s.get("filters") === "1", fresh: s.get("fresh") || "",
      tab: s.get("tab") || "overview", field: s.get("field") || "", cmp: s.get("cmp") || "", a: s.get("a") || "", b: s.get("b") || "",
      annotate: s.get("annotate") === "1", outputs: s.get("outputs") === "1", edge: s.get("edge") || "",
    };
  }
  const listQuery = () => ({ role: q.role !== "All" ? q.role : "", q: q.q, kind: q.kind.join(","), from: q.from, to: q.to, filters: q.filters ? "1" : "", fresh: q.fresh });
  const dsHref = (id, extra = {}) => hashFor(`#/catalog/${encodeURIComponent(id)}`, { ...listQuery(), ...extra });
  const tabHref = (tab) => dsHref(dId, { tab: tab === "overview" ? "" : tab });
  const filtersOn = () => !!(q.q || q.kind.length || q.from || q.to || q.fresh);

  // ── list ──
  function baseFiltered() {
    const role = ROLE_TAB[q.role] || "";
    return (all || []).filter((x) => {
      if (role === "sink" && !isSink(x)) return false;
      if (role === "source" && isSink(x)) return false;
      const ts = x.last_success || "";
      if (q.from && (!ts || ts < q.from)) return false;
      if (q.to && ts && ts > q.to) return false;
      return true;
    });
  }
  function filtered() {
    const s = q.q.toLowerCase();
    return baseFiltered().filter((x) => (!q.kind.length || q.kind.includes(x.kind)) && (!s || `${x.uri} ${x.pipeline || ""} ${(x.owners || []).join(" ")}`.toLowerCase().includes(s)) && (!q.fresh || freshness(x).key === q.fresh || (q.fresh === "evolved" && (x.schema_versions || 0) > 1)));
  }
  function dsRow(x) {
    const f = freshness(x);
    const owner = (x.owners || []).length ? `owner ${x.owners[0]}` : "";
    const facts = f.key === "stale"
      ? [x.kind, "stale", `last success ${fmtAge(x.last_success)} ago`]
      : f.key === "never" ? [x.kind, "never succeeded"]
      : [x.kind, `${fmtCompact(x.total_records ?? x.last_records ?? 0)} rows`, x.schema_versions ? `schema v${x.schema_versions}` : "", (x.consumers || []).length ? plural(x.consumers.length, "consumer") : "", owner];
    return row(ring(f.tone, f.shape), datasetName(x.uri), x.last_success ? ago(x.last_success) : "", meta(...facts), {
      on: x.id === q.id, href: dsHref(x.id), id: x.id, tip: `${x.uri}\nlast success ${x.last_success ? formatTs(x.last_success) : "never"}`,
    });
  }
  function wallValue(iso) {
    if (!iso) return "";
    const w = utcToZonedWall(new Date(iso));
    const p = (n) => String(n).padStart(2, "0");
    return `${w.y}-${p(w.mo + 1)}-${p(w.d)} ${p(w.h)}:${p(w.mi)}`;
  }
  function filterPanel() {
    const kinds = [...new Set(baseFiltered().map((x) => x.kind))].sort();
    const count = (k) => baseFiltered().filter((x) => x.kind === k).length;
    const kindSum = q.kind.length ? q.kind.join(", ") : "Every kind";
    const kindMenu = html`<details class="menu left ds-kind"><summary class="select" title="Kind">${kindSum}</summary><div class="pop ds-kindpop"><div class="mlab">Kind · ${kinds.length} in view</div>${kinds.length ? kinds.map((k) => html`<label class="ds-opt">${check(k, q.kind.includes(k), { a: { "data-kind": k } })}<span class="muted">${count(k)}</span></label>`) : html`<div class="mlab">No kinds under these filters</div>`}</div></details>`;
    const chips = [];
    if (q.q) chips.push(chip(`search: ${q.q}`, "x", { "data-remove": "q", role: "button", title: "Remove" }));
    if (q.kind.length) chips.push(chip(`kind: ${q.kind.join(", ")}`, "x", { "data-remove": "kind", role: "button", title: "Remove" }));
    if (q.role !== "All") chips.push(chip(`role: ${q.role === "Destinations" ? "destination" : "source"}`, "x", { "data-remove": "role", role: "button", title: "Remove" }));
    if (q.from) chips.push(chip(`since ${formatTs(q.from)}`, "x", { "data-remove": "from", role: "button", title: "Remove" }));
    if (q.to) chips.push(chip(`until ${formatTs(q.to)}`, "x", { "data-remove": "to", role: "button", title: "Remove" }));
    if (q.fresh) chips.push(chip(`${q.fresh}`, "x", { "data-remove": "fresh", role: "button", title: "Remove" }));
    return html`<div class="filters ds-filters">
      ${input(q.q, { icon: "search", ph: "Search name, pipeline or owner", name: "q", type: "search" })}
      <div class="r2">${kindMenu}${select([["All", "Every role"], ["Destinations", "Destinations"], ["Sources", "Sources"]], q.role, { name: "role" })}</div>
      <div class="r2"><label class="input" title="Last success from (YYYY-MM-DD HH:MM, display timezone)">${ic("clock")}<input type="text" name="from" value="${wallValue(q.from)}" placeholder="from · any time" aria-label="Last success from" autocomplete="off" spellcheck="false"></label><label class="input" title="Last success until (YYYY-MM-DD HH:MM, display timezone)">${ic("clock")}<input type="text" name="to" value="${wallValue(q.to)}" placeholder="to · now" aria-label="Last success until" autocomplete="off" spellcheck="false"></label></div>
      ${chips.length ? html`<div class="active-f">${chips}<a class="lnk" style="font-size:12px" href="${hashFor("#/catalog", { filters: "1" })}">clear all</a></div>` : ""}
    </div>`;
  }

  function renderList() {
    const tools = html`${ibtn("filter", { on: q.filters, dot: filtersOn(), title: "Filters", data: { "toggle-filters": "1" } })}${ibtn("lineage", { title: "Lineage graph", href: href.lineage() })}${ibtn("download", { title: "Local outputs", href: hashFor("#/catalog", { ...listQuery(), outputs: "1" }) })}${ibtn("refresh", { title: "Reload", data: { reload: "1" } })}`;
    if (listErr) {
      patch(L.list, html`${tabs(["All", "Destinations", "Sources"], q.role, tools)}${catalogUnavailable(listErr) ? empty("The catalogue is not available", CATALOG_MISSING, { icon: "db" }) : errorAlert("the catalogue", listErr)}`);
      L.list.querySelector("[data-retry]")?.addEventListener("click", loadList);
      return;
    }
    if (!all) {
      patch(L.list, html`${tabs(["All", "Destinations", "Sources"], q.role, tools)}<div class="skel-card" style="padding:12px"><span class="skel w80"></span></div>${skelRows(8)}`);
      return;
    }
    const rows = filtered();
    const sinks = rows.filter(isSink);
    const srcs = rows.filter((x) => !isSink(x));
    const cnt = (k) => rows.filter((x) => freshness(x).key === k).length;
    const totalNote = filtersOn() || q.role !== "All" ? html`${fmtInt(rows.length)} <em>of ${fmtInt(all.length)}</em>` : fmtInt(all.length);
    const fIdx = { fresh: 1, stale: 2, evolved: 3 }[q.fresh] ?? -1;
    const strip = stats([["Datasets", totalNote], ["Fresh", cnt("fresh"), "ok", "fresh"], ["Stale", cnt("stale"), "warn", "stale"], ["Evolved", rows.filter((x) => (x.schema_versions || 0) > 1).length, "", "evolved"]], fIdx);
    const block = (id, label, list) => (list.length ? html`${sect(label, list.length, { id, closed: sec.isClosed(id) })}<div class="sect-rows rows${sec.isClosed(id) ? " closed" : ""}">${list.slice(0, cap).map(dsRow)}</div>` : "");
    const more = Math.max(sinks.length, srcs.length) > cap ? btn(`Show ${fmtInt(Math.max(sinks.length, srcs.length) - cap)} more`, { kind: "quiet", cls: "load-more", data: { more: "1" } }) : "";
    const body = rows.length
      ? html`${block("sink", "Destinations", sinks)}${block("source", "Sources", srcs)}${more}`
      : all.length
        ? empty("No datasets match", "Clear a filter or pick another tab.", { icon: "search", btns: btn("Clear filters", { href: hashFor("#/catalog", {}) }) })
        : "";
    const roleTabs = tabs(["All", "Destinations", "Sources"], q.role, tools);
    patch(L.list, html`${roleTabs}${strip}${q.filters || filtersOn() ? filterPanel() : ""}${body}`);
  }

  L.list.addEventListener("click", (e) => {
    const t = e.target.closest("[data-tab]");
    if (t) return setQuery({ role: t.dataset.tab === "All" ? "" : t.dataset.tab });
    if (e.target.closest("[data-toggle-filters]")) return setQuery({ filters: q.filters ? "" : "1" }, { replace: true });
    if (e.target.closest("[data-reload]")) { loadList(); if (q.id) loadDetail(true); return; }
    if (e.target.closest("[data-more]")) { cap += SECTION_CAP; renderList(); return; }
    const st = e.target.closest(".stats [data-q]");
    if (st) return setQuery({ fresh: q.fresh === st.dataset.q ? "" : st.dataset.q });
    const rm = e.target.closest("[data-remove]");
    if (rm) return setQuery({ [rm.dataset.remove]: "" });
  });
  L.list.addEventListener("keydown", (e) => {
    const st = e.target.closest(".stats [data-q]");
    if (st && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); st.click(); }
  });
  L.list.addEventListener("change", (e) => {
    const k = e.target.closest("[data-kind]");
    if (k) {
      const set = new Set(q.kind);
      if (e.target.checked) set.add(k.dataset.kind); else set.delete(k.dataset.kind);
      return setQuery({ kind: [...set].join(",") }, { replace: true });
    }
    if (e.target.name === "role") return setQuery({ role: e.target.value === "All" ? "" : e.target.value });
    if (e.target.name === "from" || e.target.name === "to") {
      const raw0 = (e.target.value || "").trim();
      const m = raw0.match(/^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2}))?$/);
      if (raw0 && !m) { e.target.closest(".input").classList.add("err"); e.target.title = "Use YYYY-MM-DD or YYYY-MM-DD HH:MM"; return; }
      const end = e.target.name === "to" && m && m[4] == null;
      const iso = m ? zonedWallToUtc(+m[1], +m[2] - 1, +m[3], m[4] != null ? +m[4] : end ? 23 : 0, m[5] != null ? +m[5] : end ? 59 : 0).toISOString() : "";
      return setQuery({ [e.target.name]: iso }, { replace: true });
    }
    const lab = e.target.closest(".check, .toggle");
    if (lab) lab.classList.toggle("on", e.target.checked);
  });
  L.list.addEventListener("input", (e) => { if (e.target.name === "q") setQuery({ q: e.target.value }, { replace: true }); });
  L.list.addEventListener("sect-toggle", (e) => sec.toggle(e.detail.id));

  // ── catalogue (nothing selected) ──
  function catalogueDetail() {
    setCrumbs(["Data", "Datasets"]);
    setContext("");
    const right = html`${btn("Lineage graph", { icon: "lineage", href: href.lineage() })}${btn("Local outputs", { icon: "download", href: hashFor("#/catalog", { ...listQuery(), outputs: "1" }) })}`;
    if (q.outputs) {
      setCrumbs(["Data", { label: "Datasets", href: hashFor("#/catalog", listQuery()) }, "Local outputs"]);
      return html`${dhead("", btn("Catalogue", { icon: "db", href: hashFor("#/catalog", listQuery()) }), { href: hashFor("#/catalog", listQuery()), label: "Datasets" })}${head("Local outputs", [html`<span>every file a run wrote on this server’s disk</span>`])}<div class="stack" id="ds-outputs"></div>`;
    }
    if (listErr) return catalogUnavailable(listErr) ? empty("The catalogue is not available", CATALOG_MISSING, { icon: "db" }) : "";
    if (!all) return html`<span class="skel t"></span><span class="skel w60"></span>${skelCards(3)}`;
    if (!all.length) {
      return empty("Nothing catalogued yet", "A dataset appears here after the first successful run that reads or writes it. Every run records its source, destination, schema and row counts.", { icon: "db", btns: html`${btn("Submit a run", { kind: "primary", icon: "submit", href: href.submit(), perm: "config_submit" })}${btn("How the catalogue works", { href: "https://faucet-hq.github.io/faucet-stream/cookbook/catalog.html" })}` });
    }
    const by = (k) => all.filter((x) => freshness(x).key === k);
    const stale = by("stale").sort((a, b) => ageMs(b.last_success) - ageMs(a.last_success));
    const never = by("never");
    const ages = all.filter((x) => x.last_success).map((x) => ageMs(x.last_success)).sort((a, b) => a - b);
    const median = ages.length ? ages[Math.floor(ages.length / 2)] : null;
    const nd = all.filter(isSink).length;
    const attention = [
      ...stale.map((x) => [lnk(datasetName(x.uri), dsHref(x.id)), html`<span class="warn">stale · last success ${fmtAge(x.last_success)} ago</span>`, fmtAge(x.last_success)]),
      ...never.map((x) => [lnk(datasetName(x.uri), dsHref(x.id)), "never succeeded", x.first_seen ? fmtAge(x.first_seen) : "—"]),
      ...all.filter((x) => (x.schema_versions || 0) > 1 && ageMs(x.last_seen) < 7 * DAY).map((x) => [lnk(datasetName(x.uri), dsHref(x.id, { tab: "schema" })), `schema v${x.schema_versions} · changed`, fmtAge(x.last_seen)]),
    ];
    const readers = (x) => (edges ? edges.filter((e) => e.src_id === x.id).length : null);
    const noOwner = all.filter((x) => !(x.owners || []).length);
    const kinds = [...new Set(all.map((x) => x.kind))].sort();
    return html`${dhead("", right)}
      ${head("Catalogue", [html`<span>${plural(all.length, "dataset")}</span>`, html`<span>${fmtInt(nd)} destinations · ${fmtInt(all.length - nd)} sources</span>`, html`<span>recorded by every successful run</span>`])}
      ${card({ icon: "clock", title: "Freshness", right: "last success within a day", body: grid(4, [
        kv("Fresh", html`<span class="ok">${fmtInt(by("fresh").length)}</span>`, "within 24h"),
        kv("Stale", html`<span class="warn">${fmtInt(stale.length)}</span>`, stale[0] ? `${datasetName(stale[0].uri)} · ${fmtAge(stale[0].last_success)}` : "none"),
        kv("Never succeeded", fmtInt(never.length), never[0] ? datasetName(never[0].uri) : "none"),
        kv("Median age", median == null ? "—" : dur(median), "since last success"),
      ]) })}
      <div class="pair">
        ${card({ icon: "alert", title: "Needs attention", right: fmtInt(attention.length), body: attention.length ? tw(tbl(["Dataset", "Why", ["Since", "num"]], attention.slice(0, 8))) : html`<div class="empty-line">Nothing stale, failing or recently changed.</div>` })}
        ${card({ icon: "user", title: "Ownership gaps", right: `${fmtInt(noOwner.length)} without an owner`, body: noOwner.length ? tw(tbl(["Dataset", "Kind", ["Readers", "num"]], noOwner.slice(0, 6).map((x) => [lnk(datasetName(x.uri), dsHref(x.id)), x.kind, readers(x) == null ? "—" : fmtInt(readers(x))]))) : html`<div class="empty-line">Every dataset has an owner.</div>`, note: html`Impact analysis can only name owners it knows. Annotate a dataset to add one.` })}
      </div>
      ${card({ icon: "layers", title: "By kind", body: tbl(["Kind", ["Datasets", "num"], ["Rows last 24h", "num"], ["Evolved", "num"]], kinds.map((k) => {
        const xs = all.filter((x) => x.kind === k);
        return [html`<a class="lnk" href="${hashFor("#/catalog", { ...listQuery(), kind: k, filters: "1" })}">${k}</a>`, fmtInt(xs.length), fmtCompact(xs.filter((x) => ageMs(x.last_success) < DAY).reduce((n, x) => n + (x.last_records || 0), 0)), fmtInt(xs.filter((x) => (x.schema_versions || 0) > 1).length)];
      })) })}`;
  }

  // ── one dataset ──
  function miniGraph() {
    const up = (d.upstream || []).slice(0, 3);
    const down = (d.downstream || []).slice(0, 3);
    const rows = Math.max(1, up.length, down.length);
    const H = 38, gap = 34, h = rows * (H + gap) - gap + 40;
    const y = (i, n) => 20 + i * (H + gap) + ((rows - n) * (H + gap)) / 2;
    const cy = 20 + ((rows - 1) * (H + gap)) / 2;
    const byId = new Map((all || []).map((x) => [x.id, x]));
    const node = (x, yy, nid, uri, sub, cls = "") => {
      const ds = byId.get(nid);
      const f = ds ? freshness(ds) : { tone: "" };
      const nm = datasetName(uri);
      return html`<a href="${href.dataset(nid)}"><g class="gn ${cls}"><title>${uri}</title><rect x="${x}" y="${yy}" width="180" height="${H}" rx="8"/><circle class="gdot ${f.tone}" cx="${x + 13}" cy="${yy + 13}" r="3.5"/><text class="gt" x="${x + 24}" y="${yy + 17}">${nm.length > 26 ? `${nm.slice(0, 25)}…` : nm}</text><text class="gs" x="${x + 24}" y="${yy + 31}">${sub}</text></g></a>`;
    };
    const path = (x1, y1, x2, y2) => { const m = (x1 + x2) / 2; return html`<path class="ge" d="M${x1} ${y1} C${m} ${y1} ${m} ${y2} ${x2} ${y2}"/>`; };
    const parts = [];
    up.forEach((e, i) => parts.push(path(200, y(i, up.length) + H / 2, 290, cy + H / 2)));
    down.forEach((e, i) => parts.push(path(470, cy + H / 2, 560, y(i, down.length) + H / 2)));
    up.forEach((e, i) => parts.push(node(20, y(i, up.length), e.src_id, e.src_uri, `${byId.get(e.src_id)?.kind || "source"} · ${e.pipeline}`)));
    down.forEach((e, i) => parts.push(node(560, y(i, down.length), e.dst_id, e.dst_uri, `${byId.get(e.dst_id)?.kind || "dataset"} · ${e.pipeline}`)));
    parts.push(html`<g class="gn sel"><title>${d.uri}</title><rect x="290" y="${cy}" width="180" height="${H}" rx="8"/><circle class="gdot ${freshness(d).tone}" cx="303" cy="${cy + 13}" r="3.5"/><text class="gt" x="314" y="${cy + 17}">${datasetName(d.uri).length > 26 ? `${datasetName(d.uri).slice(0, 25)}…` : datasetName(d.uri)}</text><text class="gs" x="314" y="${cy + 31}">${d.kind} · ${fmtCompact(d.total_records ?? 0)} rows</text></g>`);
    if (!up.length && !down.length) return html`<div class="empty-line">No lineage edges recorded for this dataset yet.</div>`;
    return html`<svg class="ds-mini" viewBox="0 0 760 ${h}" height="${Math.min(240, h + 20)}" preserveAspectRatio="xMidYMid meet" role="img" aria-label="Neighbours of this dataset">${parts}</svg>`;
  }

  function detailHead(tab) {
    const prof = d.profile;
    const drift = prof && prof.latest ? (prof.latest.drift || []).length : 0;
    const tl = d.schema_timeline || [];
    const items = [["Overview", null, "overview"], ["Schema", tl.length, "schema"], ["Profile", drift || null, "profile"], ["Lineage", (d.upstream || []).length + (d.downstream || []).length, "lineage"], ["Activity", null, "activity"], ["Outputs", outputsCount, "outputs"]];
    const actions = html`${btn("Annotate", { icon: "edit", href: dsHref(dId, { tab: tab === "overview" ? "" : tab, annotate: "1" }), perm: "catalog_annotate" })}${btn("Open in lineage", { icon: "lineage", href: href.lineage(dId) })}`;
    const f = freshness(d);
    const wrote = d.pipeline ? html`<span>${d.kind} · ${isSink(d) ? "written" : "read"} by ${lnk(d.pipeline, href.runs({ label: `pipeline:${d.pipeline}` }))}</span>` : html`<span>${d.kind}</span>`;
    return html`${dhead(dtabs(items, tab, { hrefFor: tabHref }), actions, { href: hashFor("#/catalog", listQuery()), label: "Datasets" })}
      ${head(datasetName(d.uri), [f.key === "fresh" && drift && tab === "profile" ? pill("Profile drift", "bad", "alert") : f.pill, wrote, d.first_seen ? html`<span>first seen ${fmtAge(d.first_seen)} ago</span>` : "", html`<span class="mono" title="${d.uri}">${d.uri.length > 46 ? `…${d.uri.slice(-44)}` : d.uri}</span>${cpi(d.uri)}`])}`;
  }

  function volumeCard() {
    const tl = d.schema_timeline || [];
    const latest = tl[tl.length - 1];
    const ncols = latest ? colsOf(latest.schema).length : colsOf(d.current_schema).length;
    const st = d.stats || [];
    const recs = st.map((s) => s.records || 0);
    const avg = recs.length ? recs.reduce((a, b) => a + b, 0) / recs.length : 0;
    const prof = d.profile;
    let nullKv;
    if (prof && prof.latest) {
      const cols = Object.entries(prof.latest.profile.columns || {}).sort((a, b) => b[1].null_rate - a[1].null_rate);
      const worst = cols[0];
      const hist = (prof.history || []).slice().reverse();
      nullKv = worst ? kv(html`Null rate · <span class="mono">${worst[0]}</span>`, pct(worst[1].null_rate), hist.length >= 2 ? spark(hist.map((r) => (r.profile.columns[worst[0]] ? r.profile.columns[worst[0]].null_rate : 0)), "var(--text-2)", 150, 30, null, 0) : `highest of ${plural(cols.length, "column")}`) : kv("Null rate", "—", "no columns profiled");
    } else {
      nullKv = kv("Total rows", fmtCompact(d.total_records ?? 0), `over ${plural(d.runs || 0, "run")}`);
    }
    const cad = cadence(st);
    const consumers = d.consumers || [];
    let note = "";
    if (latest) {
      const parts = diffParts(latest.diff);
      if (parts.length && tl.length > 1) {
        const bad = parts.some(([t]) => t === "bad" || t === "warn");
        note = html`<span class="${bad ? "warn" : "ok"}">Schema v${latest.version}</span> · ${join(parts.slice(0, 4).map(([, s, n, x]) => html`${s === "+" ? "added" : s === "−" ? "removed" : s === "~" ? "widened" : "changed"} <span class="mono">${n}</span>${x && s !== "−" ? ` ${x}` : ""}`), ", ")}${parts.length > 4 ? " …" : ""} · ${fmtAge(latest.recorded_at)} ago.`;
      } else note = html`Schema v${latest.version} since ${at(latest.recorded_at)}.`;
    }
    return card({
      icon: "db", title: `${fmtCompact(d.total_records ?? 0)} rows · ${plural(ncols, "column")}`, tag: latest ? `schema v${latest.version}` : "", right: latest ? html`last change ${ago(latest.recorded_at, " ago")}` : "",
      body: grid(4, [
        kv("Rows per run", fmtCompact(d.last_records ?? 0), recs.length >= 2 ? spark(recs.slice(-30), "var(--text-2)", 150, 30) : `average ${fmtCompact(Math.round(avg))}`),
        nullKv,
        kv("Usual cadence", cad ? `every ${dur(cad)}` : "—", d.last_success ? html`last success ${ago(d.last_success, " ago")}` : "never succeeded"),
        kv("Consumers", fmtInt(consumers.length), consumers.length ? [...new Set(consumers.map((c) => c.kind || c.name))].slice(0, 3).join(" · ") : "none declared"),
      ]),
      note,
    });
  }

  function cadenceAlert() {
    const cad = cadence(d.stats);
    const a = ageMs(d.last_success);
    const f = freshness(d);
    if (f.key === "stale") return alert("Stale.", html`Last success ${fmtAge(d.last_success)} ago${cad ? html`; the usual gap between runs is ${dur(cad)}` : ""}.${d.pipeline ? html` Check the latest runs of ${lnk(d.pipeline, href.runs({ label: `pipeline:${d.pipeline}` }))}.` : ""}`, "warn", btn("Runs that write it", { href: tabHref("activity"), cls: "sm" }));
    if (cad && a > 2 * cad && a > 3600000) return alert("Behind its usual cadence.", html`Last success ${fmtAge(d.last_success)} ago; runs usually land every ${dur(cad)}.`, "warn", btn("Runs that write it", { href: tabHref("activity"), cls: "sm" }));
    return "";
  }

  function stewardCard() {
    const owners = d.owners || [];
    const consumers = d.consumers || [];
    return card({ icon: "user", title: "Owners & consumers", right: btn("Annotate", { cls: "sm", icon: "edit", href: dsHref(dId, { tab: q.tab === "overview" ? "" : q.tab, annotate: "1" }), perm: "catalog_annotate" }), body: html`<div class="pad">${owners.length ? join(owners.map((o) => chip(o)), " ") : html`<span class="muted">No owner declared.</span>`}</div>${consumers.length
      ? tbl(["Consumer", "Kind", "Reads"], consumers.map((c) => [html`<span title="${[c.contact, c.registered_by ? `registered by ${c.registered_by}` : ""].filter(Boolean).join(" · ")}">${c.name}</span>`, c.kind || "—", c.columns && c.columns.length ? html`<span class="mono">${c.columns.join(", ")}</span>` : "every column"]))
      : html`<div class="empty-line">No declared consumers. Pipelines that read it are listed under Lineage.</div>`}` });
  }

  function barsCard() {
    const st = (d.stats || []).slice(-30);
    const max = Math.max(1, ...st.map((s) => s.records || 0));
    return card({ title: "Rows per run", aside: `last ${plural(st.length, "run")}`, right: lnk("runs that wrote it", tabHref("activity")), body: st.length
      ? html`<div class="ds-vol" style="padding-top:14px">${st.map((s, i) => html`<i class="${i === st.length - 1 ? "last" : ""}" style="height:${raw(Math.max(3, (s.records / max) * 100).toFixed(1))}%" title="${fmtInt(s.records)} rows · ${formatTs(s.recorded_at)} · run ${s.run_id}"></i>`)}</div>`
      : html`<div class="empty-line">No runs recorded yet.</div>` });
  }

  function overviewTab() {
    return html`${cadenceAlert()}${volumeCard()}
      ${card({ icon: "lineage", title: "Lineage", right: html`${fmtInt((d.upstream || []).length)} up · ${fmtInt((d.downstream || []).length)} down · ${lnk("open graph", href.lineage(dId))}`, body: miniGraph() })}
      <div class="two">${stewardCard()}${barsCard()}</div>`;
  }

  function annotateTab() {
    const owners = d.owners || [];
    const e = annotateErrors;
    const nErr = Object.keys(e).length;
    return html`${card({ icon: "edit", title: `Annotate ${datasetName(d.uri)}`, aside: "owners and declared consumers are told when a change to this dataset is planned", body: html`<form class="form" id="ds-annotate" novalidate>
        ${field("Owners", input(owners.join(", "), { name: "owners", ph: "team-data, alice@example.com" }), { help: "Comma-separated teams or people. Empty leaves them unchanged." })}
        ${field(raw("&nbsp;"), html`<div style="height:30px;display:flex;align-items:center">${check("Clear every owner", false, { name: "clear" })}</div>`)}
        <div class="wide" style="font-weight:600">Add a consumer</div>
        ${field("Name", input("", { name: "name", ph: "revenue-dashboard", cls: e.name ? "err" : "" }), { req: true, err: e.name })}
        ${field("Kind", select(CONSUMER_KINDS, "dashboard", { name: "kind" }))}
        ${field("Contact", input("", { name: "contact", ph: "#bi-team or a URL", cls: e.contact ? "err" : "" }), { err: e.contact, help: "A team, channel or URL." })}
        ${field("Columns read", input("", { name: "columns", mono: true, ph: "every column", cls: e.columns ? "err" : "" }), { err: e.columns, help: "Comma-separated. Empty means every column." })}
      </form>` })}
      <div class="card"><div class="actions" style="border-top:0"><span class="muted">${nErr ? `${plural(nErr, "field")} need${nErr === 1 ? "s" : ""} attention` : "Saved annotations apply immediately."}</span>${btn("Cancel", { href: dsHref(dId, { tab: q.tab === "overview" ? "" : q.tab }) })}${btn("Save annotations", { kind: "primary", data: { save: "1" }, perm: "catalog_annotate" })}</div></div>
      ${volumeCard()}`;
  }

  function validateAnnotate(form) {
    const val = (n) => (form.querySelector(`[name=${n}]`)?.value || "").trim();
    const split = (s) => s.split(",").map((x) => x.trim()).filter(Boolean);
    const errs = {};
    const name = val("name"), contact = val("contact"), cols = split(val("columns"));
    if (!name && (contact || cols.length)) errs.name = "Name the consumer, or clear its other fields.";
    if (contact && /@/.test(contact) && !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(contact)) errs.contact = "Use a team name or a URL; this looks like an incomplete address.";
    const known = colsOf(d.current_schema);
    const bad = cols.filter((c) => known.length && !known.includes(c));
    if (bad.length) {
      const guess = known.find((k) => k.startsWith(bad[0]) || bad[0].startsWith(k));
      errs.columns = html`<span class="mono">${bad.join(", ")}</span> ${bad.length === 1 ? "is not a column" : "are not columns"} of the current schema.${guess ? html` Did you mean <span class="mono">${guess}</span>?` : ""}`;
    }
    const body = {};
    const owners = d.owners || [];
    const next = split(val("owners"));
    if (form.querySelector("[name=clear]")?.checked) { if (owners.length) body.owners = []; }
    else if (next.length && next.join("|") !== owners.join("|")) body.owners = next;
    if (name) {
      const c = { name, kind: val("kind") || undefined };
      if (contact) c.contact = contact;
      if (cols.length) c.columns = cols;
      body.consumers = [c];
    }
    return { errs, body };
  }

  function schemaTab() {
    const tl = (d.schema_timeline || []).slice().reverse();
    if (!tl.length) return card({ title: "Schema timeline", body: html`<div class="empty-line">No schema observed yet. The catalogue samples records from each successful run.</div>` });
    const latest = tl[0];
    const timeline = tw(tbl(["Version", "Change", ["Columns", "num"], "Run", ["When", "num"]], tl.map((s, i) => [
      html`<span class="mono">v${s.version}</span>`,
      diffParts(s.diff).length ? diffBadges(s.diff) : html`<span class="muted">${i === tl.length - 1 ? "first observed" : "no change"}</span>`,
      fmtInt(colsOf(s.schema).length),
      s.run_id ? lnk(shortId(s.run_id), href.run(s.run_id), true, s.run_id) : "—",
      ago(s.recorded_at),
    ])));
    if (q.cmp) {
      const pick = (val, dflt) => tl.find((s) => String(s.version) === val) || dflt;
      const A = pick(q.a, tl[Math.min(tl.length - 1, 1)] || latest);
      const B = pick(q.b, latest);
      const lines = lineDiff(schemaLines(A.schema), schemaLines(B.schema));
      const opts = tl.map((s) => [String(s.version), `v${s.version} · ${fmtAge(s.recorded_at)} ago`]);
      const colsA = colsOf(A.schema), colsB = colsOf(B.schema);
      const added = colsB.filter((c) => !colsA.includes(c)).length;
      const removed = colsA.filter((c) => !colsB.includes(c)).length;
      const changed = colsB.filter((c) => colsA.includes(c) && JSON.stringify(A.schema.properties[c]) !== JSON.stringify(B.schema.properties[c])).length;
      const sel = html`<span class="ds-sel-pair">${select(opts, String(A.version), { name: "cmp-a" })}<span class="muted">→</span>${select(opts, String(B.version), { name: "cmp-b" })}</span>`;
      return html`${card({ icon: "diff", title: "Compare schema versions", right: html`${sel}${btn("Close", { cls: "sm", kind: "quiet", href: tabHref("schema") })}`, body: diff([["@", "properties"], ...lines.map(([op, t]) => [op, t])]), note: html`<b>${plural(Math.abs(B.version - A.version), "version")} apart</b> · ${fmtInt(added)} added, ${fmtInt(changed)} changed, ${fmtInt(removed)} removed.` })}
        ${card({ title: "Schema timeline", right: plural(tl.length, "version"), body: timeline })}`;
    }
    const props = (latest.schema && latest.schema.properties) || {};
    const names = Object.keys(props);
    const since = (c) => { let v0 = latest.version; for (const s of tl) { if (colsOf(s.schema).includes(c)) v0 = s.version; else break; } return v0; };
    const prof = d.profile && d.profile.latest ? d.profile.latest.profile.columns || {} : {};
    const sel = names.includes(q.field) ? q.field : names[0];
    const colRows = names.map((c) => ({ cls: c === sel ? "sel" : "", href: dsHref(dId, { tab: "schema", field: c }), cells: [html`<span class="mono">${c}</span>`, chip(typeText(props[c])), nullable(props[c]) ? "yes" : "no", prof[c] ? pct(prof[c].null_rate) : "—", `v${since(c)}`] }));
    const p = sel ? props[sel] : null;
    const hist = sel ? tl.slice().reverse().filter((s) => s.schema && s.schema.properties && s.schema.properties[sel]).map((s) => `v${s.version} ${typeText(s.schema.properties[sel])}`) : [];
    const types = [...new Set(hist.map((h) => h.split(" ")[1]))];
    const addedIn = sel ? tl.find((s) => s.version === since(sel)) : null;
    const readers = (d.consumers || []).filter((c) => !c.columns || !c.columns.length || c.columns.includes(sel));
    const fromCols = (d.upstream || []).flatMap((e) => ((e.column_lineage && e.column_lineage.fields && e.column_lineage.fields[sel]) || []).map((src) => ({ e, src })));
    const pc = sel ? prof[sel] : null;
    const fieldCard = sel ? card({ icon: "columns", title: sel, tag: `${typeText(p)}${nullable(p) ? " · nullable" : ""}`, body: html`${grid(2, [
      kv("Added in", `v${since(sel)}`, addedIn ? html`${ago(addedIn.recorded_at, " ago")}${addedIn.run_id ? html`, run ${lnk(shortId(addedIn.run_id), href.run(addedIn.run_id), true)}` : ""}` : ""),
      kv("Null rate", pc ? pct(pc.null_rate) : "—", pc ? "latest profile" : "not profiled"),
      kv("Distinct", pc && pc.distinct != null ? fmtInt(pc.distinct) : "—", pc && pc.top_values && pc.top_values[0] ? `top: ${pc.top_values[0].value} (${pct(pc.top_values[0].share)})` : ""),
      kv("Type history", types.length > 1 ? types.join(" → ") : types[0] || typeText(p), plural(hist.length, "version")),
    ])}<div class="note">${readers.length ? html`Read by ${join(readers.map((c) => html`<b>${c.name}</b>`), ", ")}.` : "No declared consumer reads it."}${fromCols.length ? html` Column lineage: ← ${join(fromCols.slice(0, 3).map(({ e, src }) => html`${lnk(datasetName(e.src_uri), href.dataset(e.src_id))}<span class="mono">.${src}</span>`), ", ")}${fromCols.length === 1 ? ` (${howDerived(sel, [fromCols[0].src])})` : ""}.` : ""}</div>` }) : "";
    const ff = html`<label class="input" style="height:26px;width:170px">${ic("search")}<input type="search" name="colq" placeholder="Filter columns" autocomplete="off"></label>`;
    return html`<div class="two">
      <div class="stack">
        ${card({ title: "Schema timeline", right: html`${plural(tl.length, "version")}${tl.length > 1 ? btn("Compare versions", { cls: "sm", icon: "diff", href: dsHref(dId, { tab: "schema", cmp: "1" }) }) : ""}`, body: timeline })}
        ${card({ title: "Columns", aside: `v${latest.version} · ${plural(names.length, "column")}`, right: ff, body: html`<div class="ds-cols ds-tw">${tbl(["Column", "Type", "Nullable", ["Null rate", "num"], ["Since", "num"]], colRows)}</div>` })}
      </div>
      ${fieldCard}
    </div>`;
  }

  function profileTab() {
    const prof = d.profile;
    if (!prof || !prof.latest) return card({ icon: "columns", title: "Column profile", body: empty("No column profiles yet", html`Add a <code>profiling:</code> block to the pipeline that writes this dataset; each run then records per-column statistics and flags drift against a rolling baseline.`, { icon: "columns" }) });
    const latest = prof.latest;
    const hist = (prof.history || []).slice().reverse();
    const cols = Object.entries(latest.profile.columns || {});
    const drift = latest.drift || [];
    const driftFor = (c) => drift.filter((x) => x.column === c);
    const stable = (() => { let n = 0; for (const r of (prof.history || []).slice(1)) { if ((r.drift || []).length) break; n++; } return n; })();
    const byCol = [...new Set(drift.map((x) => x.column))];
    const al = drift.length ? alert(`Drift in ${plural(byCol.length, "column")}.`, html`${join(byCol.slice(0, 3).map((c) => html`<span class="mono">${c}</span> ${[...new Set(driftFor(c).map((x) => x.metric.replace(/_/g, " ")))].join(", ")}`), "; ")}${byCol.length > 3 ? ` and ${byCol.length - 3} more` : ""}, against a baseline of ${plural(latest.baseline_runs ?? hist.length, "run")}. Seen in run ${lnk(shortId(latest.run_id), href.run(latest.run_id), true)} ${ago(latest.recorded_at, " ago")}.`, "bad", btn("Open that run", { icon: "runs", cls: "sm", href: href.run(latest.run_id) })) : "";
    const rows = cols.map(([name, c]) => {
      const fl = driftFor(name);
      const types = Object.entries(c.types || {}).filter(([t, n]) => t !== "null" && n > 0).sort((a, b) => b[1] - a[1]).map(([t]) => t).join("/") || "null";
      const pts = hist.map((r) => (r.profile.columns[name] ? r.profile.columns[name].null_rate : 0));
      const range = c.numeric ? `${fmtNum(c.numeric.min)} – ${fmtNum(c.numeric.max)}` : c.string ? `len ${c.string.len_min}–${c.string.len_max}` : "—";
      const top = c.high_cardinality ? "high cardinality" : c.numeric ? `mean ${fmtNum(c.numeric.mean)}` : (c.top_values || []).slice(0, 2).map((t) => `${t.value === "" ? "(empty)" : t.value} (${pct(t.share)})`).join(" · ") || "—";
      return { id: name, cells: [
        html`<span class="mono">${name}</span>${fl.length ? html` ${join([...new Set(fl.map((x) => x.metric))].map((m) => tag(m.replace(/_/g, " "), "bad")), " ")}` : ""}`,
        types,
        html`<span class="ds-null"><span class="${fl.some((x) => x.metric === "null_rate") ? "bad" : ""}">${pct(c.null_rate)}</span>${pts.length >= 2 ? spark(pts, "var(--text-2)", 90, 20, fl.length ? pts.length - 1 : null) : ""}</span>`,
        fmtInt(c.distinct ?? 0), range, top,
      ] };
    });
    const ff = html`<label class="input" style="height:26px;width:170px">${ic("search")}<input type="search" name="colq" placeholder="Filter columns" autocomplete="off"></label>`;
    return html`${al}
      ${card({ icon: "columns", title: "Latest profile", aside: html`run ${shortId(latest.run_id)} · ${ago(latest.recorded_at, " ago")}`, right: `baseline ${plural(latest.baseline_runs ?? hist.length, "run")}`, body: grid(4, [
        kv("Rows", fmtInt(latest.profile.rows ?? 0)),
        kv("Columns profiled", fmtInt(cols.length), latest.profile.skipped_columns ? `+ ${fmtInt(latest.profile.skipped_columns)} skipped beyond max_columns` : ""),
        kv("Drifting", drift.length ? html`<span class="bad">${fmtInt(new Set(drift.map((x) => x.column)).size)}</span>` : html`<span class="ok">0</span>`, drift.length ? [...new Set(drift.map((x) => x.metric))].join(", ") : "stable"),
        kv("Stable for", plural(stable, "run"), drift.length ? "before this one" : "recorded"),
      ]) })}
      ${card({ title: "Columns", right: ff, body: html`<div class="ds-cols ds-tw">${tbl(["Column", "Types", "Null rate", ["Distinct", "num"], "Range", "Top values"], rows)}</div>` })}`;
  }
  const fmtNum = (x) => (typeof x === "number" ? (Math.abs(x) >= 1000 ? fmtInt(Math.round(x)) : String(+x.toFixed(2))) : String(x ?? "—"));

  function lineageTab() {
    const up = d.upstream || [], down = d.downstream || [];
    const byId = new Map((all || []).map((x) => [x.id, x]));
    const edgeRows = (es, end) => es.map((e) => [lnk(datasetName(e[`${end}_uri`]), href.dataset(e[`${end}_id`]), false, e[`${end}_uri`]), lnk(e.pipeline, href.runs({ label: `pipeline:${e.pipeline}` })), fmtInt(e.runs || 0), fmtCompact(e.last_records || 0)]);
    const withCols = up.filter((e) => e.column_lineage && e.column_lineage.fields);
    const pick = withCols.find((e) => e.src_id === q.edge) || withCols[0];
    let colCard = "";
    if (pick) {
      const f = pick.column_lineage.fields;
      colCard = card({ icon: "columns", title: "Column lineage", aside: `${pick.pipeline} · ${datasetName(pick.src_uri)} → ${datasetName(d.uri)}`, right: withCols.length > 1 ? select(withCols.map((e) => [e.src_id, datasetName(e.src_uri)]), pick.src_id, { name: "edge" }) : "",
        body: tbl(["Column here", "From", "How"], Object.entries(f).map(([into, from]) => [html`<span class="mono">${into}</span>`, from.length ? html`<span class="mono">${from.join(", ")}</span>` : html`<span class="faint">—</span>`, howDerived(into, from)])),
        note: "Recorded from the pipeline’s transforms; opaque steps are marked, never guessed." });
    } else if (up.length) colCard = card({ icon: "columns", title: "Column lineage", body: html`<div class="empty-line">No column lineage was recorded for the edges into this dataset (an opaque transform hides it).</div>` });
    return html`${card({ icon: "lineage", title: "Neighbours", right: btn("Open in lineage", { cls: "sm", icon: "focus", href: href.lineage(dId) }), body: miniGraph() })}
      <div class="pair">
        ${card({ title: "Upstream", right: fmtInt(up.length), body: up.length ? tw(tbl(["Dataset", "Pipeline", ["Runs", "num"], ["Last rows", "num"]], edgeRows(up, "src"))) : html`<div class="empty-line">Nothing feeds this dataset${byId.size ? "" : ""}: it is a source.</div>` })}
        ${card({ title: "Downstream", right: fmtInt(down.length), body: down.length ? tw(tbl(["Dataset", "Pipeline", ["Runs", "num"], ["Last rows", "num"]], edgeRows(down, "dst"))) : html`<div class="empty-line">No pipeline reads it yet.</div>` })}
      </div>${colCard}`;
  }

  function activityTab() {
    if (!activity) return html`${skelCards(2)}`;
    if (activity.err) return errorAlert("the runs for this dataset", activity.err);
    const wrote = activity.wrote;
    const readerPipes = new Set((d.downstream || []).map((e) => e.pipeline));
    const read = (activity.all || []).filter((r) => readerPipes.has((r.labels || {}).pipeline) || readerPipes.has(r.name)).slice(0, 8);
    const tpl = {};
    for (const r of wrote) { const t = (r.labels || {}).template; if (t) { tpl[t] = tpl[t] || { n: 0, v: (r.labels || {}).template_version }; tpl[t].n++; } }
    const tplLink = (r) => { const t = (r.labels || {}).template; return t ? lnk(`${t}@v${(r.labels || {}).template_version || "?"}`, href.template(t)) : "—"; };
    return html`${card({ icon: "runs", title: `Recent runs that ${isSink(d) ? "wrote" : "read"} it`, right: lnk("all in Runs", d.pipeline ? href.runs({ label: `pipeline:${d.pipeline}` }) : href.runs()), body: wrote.length
        ? tbl(["Run", "Status", "Template", ["Rows", "num"], ["When", "num"]], wrote.slice(0, 10).map((r) => ({ href: href.run(r.run_id), cells: [lnk(r.name || shortId(r.run_id), href.run(r.run_id)), runDot(r.status), tplLink(r), fmtInt(runRows(r, dId)), ago(r.submitted_at)] })))
        : html`<div class="empty-line">${activity.exact ? "No run on record wrote it." : "None of the newest 200 runs wrote it."}</div>`, note: activity.exact ? "" : "Matched against the newest 200 runs." })}
      <div class="pair">
        ${card({ icon: "runs", title: "Recent runs that read it", body: read.length ? tbl(["Run", "Status", ["When", "num"]], read.map((r) => ({ href: href.run(r.run_id), cells: [lnk(r.name || shortId(r.run_id), href.run(r.run_id)), runDot(r.status), ago(r.submitted_at)] }))) : html`<div class="empty-line">No reading run among the recent runs.</div>` })}
        ${card({ icon: "tpl", title: "Templates that write it", body: Object.keys(tpl).length ? tbl(["Template", "Version", ["Runs", "num"]], Object.entries(tpl).map(([t, x]) => [lnk(t, href.template(t)), ch(`v${x.v || "?"}`, false), fmtInt(x.n)])) : html`<div class="empty-line">No template run wrote it recently.</div>` })}
      </div>
      ${card({ icon: "changes", title: "Pending changes that affect it", right: changes ? fmtInt(changes.length) : "", body: !changes ? skelRows(1) : changes.length ? tbl(["Change", "Kind", "Requested"], changes.map((c) => ({ href: href.change(c.id), cells: [lnk(c.reason || `${c.kind} ${shortId(c.id)}`, href.change(c.id)), c.kind, html`${c.requester || "—"} · ${ago(c.created_at)}`] }))) : html`<div class="empty-line">No pending change names this dataset or its pipeline.</div>` })}`;
  }

  function detailBody() {
    if (q.annotate) return annotateTab();
    switch (q.tab) {
      case "schema": return schemaTab();
      case "profile": return profileTab();
      case "lineage": return lineageTab();
      case "activity": return activityTab();
      case "outputs": return html`<div class="stack" id="ds-outputs"></div>`;
      default: return overviewTab();
    }
  }

  async function loadActivity() {
    activity = null;
    changes = null;
    const id = dId;
    try {
      const r = await runsWriting(id);
      let allRuns = r.all;
      if (!allRuns) allRuns = (await api(`/v1/runs?${withTenant(new URLSearchParams({ limit: "200" }))}`)).runs || [];
      if (id !== dId) return;
      activity = { wrote: r.runs, all: allRuns, exact: r.exact };
    } catch (e) {
      if (id !== dId) return;
      activity = { err: e };
    }
    renderDetail();
    try {
      const cs = await api(`/v1/changes?${withTenant(new URLSearchParams({ status: "pending", limit: "100" }))}`);
      const list = Array.isArray(cs) ? cs : cs.changes || [];
      const keys = [d.id, d.uri, d.pipeline].filter(Boolean);
      changes = list.filter((c) => { const s = JSON.stringify(c); return keys.some((k) => s.includes(k)); });
    } catch { changes = []; }
    if (id === dId) renderDetail();
  }

  function renderDetail() {
    L.select(!!q.id || q.outputs);
    if (!q.id) {
      outputsHost = null;
      patch(L.detail, catalogueDetail());
      if (q.outputs) {
        const h = L.detail.querySelector("#ds-outputs");
        if (h && !h.__mounted) { h.__mounted = true; mountOutputs(h); }
      }
      return;
    }
    if (dErr) {
      setCrumbs(["Data", { label: "Datasets", href: hashFor("#/catalog", listQuery()) }, q.id]);
      const notFound = dErr.status === 404 && !catalogUnavailable(dErr);
      patch(L.detail, html`${dhead(dtabs(DTABS.map(([l, k]) => [l, null, k]), "overview", { hrefFor: tabHref }), "", { href: hashFor("#/catalog", listQuery()), label: "Datasets" })}${head(notFound ? "Dataset not found" : shortId(q.id))}${notFound
        ? empty("No dataset with this id", "It may have been recorded on another server, or the history was reset.", { icon: "db", btns: btn("All datasets", { href: hashFor("#/catalog", listQuery()) }) })
        : catalogUnavailable(dErr) ? alert("The catalogue is not available.", CATALOG_MISSING, "") : alert("Couldn’t load this dataset.", html`${dErr.status ? `The server answered ${dErr.status}: ${dErr.message}` : dErr.message} (<span class="mono">GET /v1/catalog/datasets/${shortId(q.id)}</span>). The list keeps its last good state.`, "bad", html`${btn("Retry", { icon: "refresh", data: { "retry-detail": "1" } })}${btn("Copy error", { icon: "copy", kind: "quiet", data: { copy: `GET /v1/catalog/datasets/${q.id}: ${dErr.status || ""} ${dErr.message}` } })}`)}`);
      return;
    }
    if (!d) {
      patch(L.detail, html`${dhead(dtabs(DTABS.map(([l, k]) => [l, null, k]), q.tab, { hrefFor: tabHref }))}<div><span class="skel t"></span><span class="skel w30" style="margin-top:10px"></span></div>${skelCards(3)}`);
      return;
    }
    const name = datasetName(d.uri);
    setCrumbs(["Data", { label: "Datasets", href: hashFor("#/catalog", listQuery()) }, q.annotate ? `${name} · annotate` : name]);
    setContext((d.owners || []).length ? chip(`owner ${d.owners[0]}${d.owners.length > 1 ? ` +${d.owners.length - 1}` : ""}`) : chip("no owner"));
    const changed = patch(L.detail, html`${detailHead(q.annotate ? "overview" : q.tab)}${detailBody()}`);
    if (changed) applyColumnFilter();
    if (q.tab === "outputs" && !q.annotate) {
      const h = L.detail.querySelector("#ds-outputs");
      if (h && !h.__mounted) { h.__mounted = true; outputsHost = mountOutputs(h, { datasetId: dId, onCount: (n) => { if (outputsCount !== n) { outputsCount = n; const t = L.detail.querySelector('.dhead a[href*="tab=outputs"]'); if (t) t.innerHTML = v(html`Outputs<span class="n">${n}</span>`); } } }); }
    }
  }
  let colFilter = "";
  function applyColumnFilter() {
    const inp = L.detail.querySelector("input[name=colq]");
    if (inp) inp.value = colFilter;
    const s = colFilter.toLowerCase();
    L.detail.querySelectorAll(".ds-cols tbody tr").forEach((tr) => { tr.hidden = !!s && !tr.textContent.toLowerCase().includes(s); });
  }
  L.detail.addEventListener("input", (e) => { if (e.target.name === "colq") { colFilter = e.target.value; applyColumnFilter(); } });
  L.detail.addEventListener("change", (e) => {
    const lab = e.target.closest(".check, .toggle");
    if (lab && e.target.type === "checkbox") lab.classList.toggle("on", e.target.checked);
    if (e.target.name === "cmp-a" || e.target.name === "cmp-b") return setQuery({ [e.target.name === "cmp-a" ? "a" : "b"]: e.target.value }, { replace: true });
    if (e.target.name === "edge") return setQuery({ edge: e.target.value }, { replace: true });
  });
  L.detail.addEventListener("click", async (e) => {
    if (e.target.closest("[data-retry-detail]")) return loadDetail(true);
    if (e.target.closest("[data-retry]") && !q.id) return loadList();
    if (e.target.closest("[data-save]")) {
      const form = L.detail.querySelector("#ds-annotate");
      const { errs, body } = validateAnnotate(form);
      annotateErrors = errs;
      if (Object.keys(errs).length) {
        const keep = Object.fromEntries([...form.querySelectorAll("input, select")].map((x) => [x.name, x.type === "checkbox" ? x.checked : x.value]));
        renderDetail();
        const f2 = L.detail.querySelector("#ds-annotate");
        for (const [k, val] of Object.entries(keep)) { const el = f2.querySelector(`[name=${k}]`); if (el) { if (el.type === "checkbox") el.checked = val; else el.value = val; } }
        return;
      }
      if (body.owners === undefined && !body.consumers) return toast("Nothing to save: change the owners or name a consumer.", "info");
      try {
        await api(`/v1/catalog/datasets/${encodeURIComponent(dId)}/consumers`, { method: "POST", body });
        toast("Annotations saved", "ok");
        annotateErrors = {};
        setQuery({ annotate: "" });
        loadDetail(true);
        loadList();
      } catch (er) { toast(er.message, "bad"); }
    }
  });

  async function loadList() {
    try { all = await fetchAllDatasets(); listErr = null; } catch (e) { listErr = e; if (!all) all = null; }
    renderList();
    if (!q.id) renderDetail();
    if (!edges && all && all.length) api("/v1/catalog/lineage").then((r) => { edges = r.edges || []; if (!q.id) renderDetail(); }).catch(() => { edges = []; });
  }
  async function loadDetail(force = false) {
    if (!q.id) { d = null; dErr = null; dId = ""; renderDetail(); return; }
    if (!force && dId === q.id && (d || dErr)) { renderDetail(); return; }
    const id = q.id;
    const changedId = dId !== id;
    dId = id;
    if (changedId) { d = null; dErr = null; outputsCount = null; activity = null; changes = null; L.detail.__html = ""; annotateErrors = {}; colFilter = ""; }
    renderDetail();
    try { const x = await api(`/v1/catalog/datasets/${encodeURIComponent(id)}`); if (id !== dId) return; d = x; dErr = null; } catch (e) { if (id !== dId) return; dErr = e; }
    renderDetail();
    if (d && q.tab === "activity" && !activity) loadActivity();
    if (d && outputsCount == null) api(`/v1/local-outputs?dataset_id=${encodeURIComponent(id)}`).then((r) => { if (id === dId) { outputsCount = (r.outputs || []).length; if (q.tab !== "outputs") renderDetail(); } }).catch(() => {});
  }

  renderList();
  renderDetail();
  await Promise.all([loadList(), loadDetail()]);
  const stop = poll(async () => {
    const fresh = await fetchAllDatasets();
    all = fresh;
    listErr = null;
    renderList();
    if (!q.id) renderDetail();
  }, 30000);

  return {
    teardown: stop,
    update(p) {
      const prev = q;
      q = readQuery(p);
      const listKeys = ["role", "q", "from", "to", "filters", "fresh", "id"];
      if (listKeys.some((k) => prev[k] !== q[k]) || prev.kind.join() !== q.kind.join()) { if (prev.role !== q.role || prev.q !== q.q) cap = SECTION_CAP; renderList(); }
      if (prev.id !== q.id) loadDetail();
      else {
        if (prev.annotate !== q.annotate) annotateErrors = {};
        renderDetail();
        if (q.tab === "activity" && !activity && d) loadActivity();
      }
    },
  };
}


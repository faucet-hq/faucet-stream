// Data Movement Catalog (#279): every dataset the server's pipelines have
// touched, beside the selected dataset's detail — volume and null-rate trends,
// the latest column profile, the schema timeline with diffs, owners and
// consumers, and its lineage edges. With nothing selected the detail pane
// holds the server's local outputs.
//
// Also the control surface for local-output retention (#587/#588) and the
// dataset **preview** of those outputs (#586) — the rows a run actually wrote,
// read back through the matching source connector. Cleanup of
// *data artifacts* belongs next to the data artifacts, not on the Runs tab,
// which is about execution history. The model the UI has to convey:
// **data artifacts are disposable; run history is durable** — cleaning an output
// removes the file and leaves the run record alone, so a cleaned output renders
// as `expired`, never as a broken row.
import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml, fmtCompact, fmtInt, fmtAge as ageOf } from "../utils.js";
import { attachDatePicker } from "./date-picker.js";
import { fmtTime } from "./runs.js";
import { setCrumbTitle } from "../shell.js";
import { icon, statStrip, listRow, tabs, pill, sparkline as trend } from "../components.js";
import { splitPane, sectionList } from "./split.js";

const CATALOG_MISSING =
  "The Data Movement Catalog endpoints are not available on this server " +
  "(faucet was built without the `catalog` feature).";

export function catalogUnavailable(e) {
  // A route that is not wired returns a bare 404 (no ApiError envelope code).
  return e && e.status === 404 && !e.code;
}

const DAY = 86400e3;
const LIST_TABS = [
  { id: "", label: "All" },
  { id: "sink", label: "Destinations" },
  { id: "source", label: "Sources" },
];
// The catalog is a bounded set; walk the cursor once and filter client-side.
const MAX_FETCH_PAGES = 100;
// Rows per section before a "show more" footer.
const SECTION_CAP = 200;

/** A URI without its scheme and credentials-free authority noise, for titles. */
function shortUri(uri) {
  return String(uri || "").replace(/^[a-z][a-z0-9+.-]*:\/\//i, "") || String(uri || "");
}

export async function renderDatasets(container, params = {}) {
  let all = [];
  let unavailable = "";
  let role = "";
  let selected = params.id || null;
  let alive = true;
  let detailStop = null;
  let overviewForced = false;
  let cap = SECTION_CAP;
  const kinds = new Set();

  const pane = splitPane(container, {
    name: "Datasets",
    listHead: `
      <div class="sp-tabs">
        ${tabs(LIST_TABS.map((t) => ({ ...t, id: t.id || "all" })), "all")}
        <span class="sp-tools">
          <button type="button" class="icon-btn" id="f-toggle" title="Filters" aria-label="Filters" aria-expanded="false">${icon("filter")}<span class="sp-dot" hidden></span></button>
          <button type="button" class="icon-btn" id="f-refresh" title="Refresh" aria-label="Refresh">${icon("refresh")}</button>
          <a class="icon-btn" href="#/lineage" title="Lineage graph" aria-label="Lineage graph">${icon("lineage")}</a>
          <button type="button" class="icon-btn" id="f-outputs" title="Local outputs" aria-label="Local outputs">${icon("logs")}</button>
        </span>
      </div>
      <div class="sp-filters" id="f-panel" hidden>
        <input id="f-q" placeholder="search URI" />
        <details class="dd" id="f-kind-dd">
          <summary id="f-kind-sum">kind ▾</summary>
          <div class="dd-menu" id="f-kind-menu"><span class="dd-empty">run a pipeline first</span></div>
        </details>
        <input id="f-from" class="date-input" type="text" readonly placeholder="last success from…" />
        <input id="f-to" class="date-input" type="text" readonly placeholder="to…" />
      </div>
      <div id="d-stats"></div>`,
  });
  const $ = (sel) => pane.head.querySelector(sel);
  const list = sectionList(pane.body, "faucet.datasets.sections");

  pane.head.querySelectorAll(".c-tab").forEach((b) => {
    b.onclick = () => {
      role = b.dataset.tab === "all" ? "" : b.dataset.tab;
      pane.head.querySelectorAll(".c-tab").forEach((x) => {
        x.classList.toggle("on", x === b);
        x.setAttribute("aria-selected", String(x === b));
      });
      cap = SECTION_CAP;
      paint();
    };
  });
  const toggle = $("#f-toggle");
  toggle.onclick = () => {
    const panel = $("#f-panel");
    panel.hidden = !panel.hidden;
    toggle.setAttribute("aria-expanded", String(!panel.hidden));
    toggle.classList.toggle("on", !panel.hidden);
  };
  const refilter = () => {
    cap = SECTION_CAP;
    paint();
  };
  $("#f-q").oninput = refilter;
  attachDatePicker($("#f-from"));
  attachDatePicker($("#f-to"));
  $("#f-from").addEventListener("change", refilter);
  $("#f-to").addEventListener("change", refilter);
  $("#f-refresh").onclick = () => {
    loadAll();
    if (selected) showDetail(selected);
  };
  $("#f-outputs").onclick = () => {
    overviewForced = true;
    if (selected) navigate("#/catalog");
    else showDetail(null);
  };
  const kindDd = $("#f-kind-dd");
  const closeKindDd = (e) => {
    if (kindDd.open && !kindDd.contains(e.target)) kindDd.open = false;
  };
  document.addEventListener("click", closeKindDd);

  // Datasets matching every filter except kind — the base the kind facet is
  // computed from, so it lists only kinds the other filters still reach.
  function baseFiltered() {
    const from = $("#f-from").dataset.value || "";
    const to = $("#f-to").dataset.value || "";
    return all.filter((d) => {
      if (role && !(d.roles || []).includes(role)) return false;
      const ts = d.last_success || "";
      if (from && ts && ts < from) return false;
      if (to && ts && ts > to) return false;
      return true;
    });
  }
  function filtered() {
    const q = $("#f-q").value.trim().toLowerCase();
    return baseFiltered()
      .filter((d) => !kinds.size || kinds.has(d.kind))
      .filter((d) => !q || (d.uri || "").toLowerCase().includes(q));
  }
  function populateKinds() {
    const menu = $("#f-kind-menu");
    const distinct = [...new Set(baseFiltered().map((d) => d.kind))].sort();
    for (const k of [...kinds]) if (!distinct.includes(k)) kinds.delete(k);
    if (!distinct.length) return;
    menu.innerHTML = distinct
      .map((k) => `<label class="dd-opt"><input type="checkbox" value="${escapeHtml(k)}"${kinds.has(k) ? " checked" : ""} /> ${escapeHtml(k)}</label>`)
      .join("");
    menu.querySelectorAll("input").forEach((cb) => {
      cb.onchange = () => {
        if (cb.checked) kinds.add(cb.value);
        else kinds.delete(cb.value);
        $("#f-kind-sum").textContent = kinds.size ? `kind (${kinds.size}) ▾` : "kind ▾";
        refilter();
      };
    });
  }

  async function loadAll() {
    try {
      const acc = [];
      const seen = new Set();
      let cursor = null;
      for (let i = 0; i < MAX_FETCH_PAGES; i++) {
        const p = new URLSearchParams({ limit: "200" });
        if (cursor) p.set("cursor", cursor);
        const data = await api(`/v1/catalog/datasets?${p}`);
        for (const d of data.datasets) if (!seen.has(d.id)) { seen.add(d.id); acc.push(d); }
        cursor = data.next_cursor || null;
        if (!cursor) break;
      }
      if (!alive) return;
      all = acc;
      unavailable = "";
    } catch (e) {
      if (!alive) return;
      all = [];
      unavailable = catalogUnavailable(e) ? CATALOG_MISSING : e.message;
      if (!catalogUnavailable(e)) toast(e.message, "error");
    }
    paint();
  }

  function paint() {
    populateKinds();
    const now = Date.now();
    const recent = all.filter((d) => now - Date.parse(d.last_success) < DAY).length;
    const changed = all.filter((d) => (d.schema_versions || 0) > 1).length;
    const owned = all.filter((d) => (d.owners || []).length).length;
    $("#d-stats").innerHTML = statStrip([
      { label: "Datasets", value: all.length },
      { label: "Last 24h", value: recent, tone: "ok" },
      { label: "Evolved", value: changed, tone: changed ? "warn" : "" },
      { label: "Owned", value: owned },
    ]);
    const filters = !!($("#f-q").value.trim() || kinds.size || $("#f-from").dataset.value || $("#f-to").dataset.value);
    $("#f-toggle .sp-dot").hidden = !filters;
    const rows = filtered();
    const sinks = rows.filter((d) => (d.roles || []).includes("sink"));
    const sources = rows.filter((d) => !(d.roles || []).includes("sink"));
    const more = (n) => (n > cap ? `<button type="button" class="btn-ghost load-more" data-more>Show ${fmtInt(n - cap)} more</button>` : "");
    const empty = unavailable
      ? `<div class="sp-empty"><b>The catalog is not available.</b><span>${escapeHtml(unavailable)}</span></div>`
      : all.length
        ? `<div class="sp-empty"><b>No datasets match the filters.</b><span>Clear the filters or pick another tab.</span></div>`
        : `<div class="sp-empty"><b>No datasets catalogued yet.</b><span>Every dataset a run reads or writes is recorded here — submit a run on the <a href="#/submit">Submit</a> page.</span></div>`;
    list.render(
      [
        { id: "sink", label: "Destinations", count: sinks.length, rows: sinks.slice(0, cap).map((d) => datasetRow(d, now)) },
        { id: "source", label: "Sources", count: sources.length, rows: sources.slice(0, cap).map((d) => datasetRow(d, now)) },
      ],
      { empty, footer: more(Math.max(sinks.length, sources.length)) },
    );
    if (!selected) showOverviewCounts();
  }
  pane.body.addEventListener("click", (e) => {
    if (e.target.closest("[data-more]")) {
      cap += SECTION_CAP;
      paint();
    }
  });

  function datasetRow(d, now) {
    const fresh = now - Date.parse(d.last_success) < DAY;
    const meta = [
      `${fmtCompact(d.last_records)} rows`,
      d.schema_versions ? `schema v${d.schema_versions}` : "",
      (d.consumers || []).length ? `${fmtInt(d.consumers.length)} consumer${d.consumers.length === 1 ? "" : "s"}` : "",
      (d.owners || []).length ? `owner ${d.owners[0]}` : "",
      d.pipeline ? `by ${d.pipeline}` : "",
    ].filter(Boolean);
    return listRow({
      id: d.id,
      href: `#/catalog/${encodeURIComponent(d.id)}`,
      tone: fresh ? "ok" : "muted",
      shape: fresh ? "full" : "hollow",
      title: shortUri(d.uri),
      tooltip: `${d.uri}\nlast success ${fmtTime(d.last_success)}`,
      age: ageOf(d.last_success, now),
      ageTitle: `last success ${fmtTime(d.last_success)}`,
      metaLead: d.kind,
      meta: meta.join(" · "),
      selected: d.id === selected,
    });
  }

  let loTeardown = null;
  function showOverviewCounts() {
    const n = pane.detail.querySelector("#d-ov-count");
    if (n) n.textContent = `${fmtInt(all.length)} dataset${all.length === 1 ? "" : "s"} catalogued`;
  }
  async function showOverview() {
    pane.setSelected(overviewForced);
    pane.detail.innerHTML = `
      <div class="c-dhead"><a class="sp-back" href="#/catalog" id="d-ov-back">${icon("back")}Datasets</a></div>
      <div><h1 class="c-title">Data movement catalog</h1>
        <div class="c-sub"><span id="d-ov-count"></span><span class="c-sub-sep">·</span><span>pick a dataset on the left for its volume, profile, schema timeline and lineage</span><span class="c-sub-sep">·</span><a href="#/lineage">Open the lineage graph</a></div></div>
      <section class="c-card"><div class="c-card-h">${icon("logs")}<b>Local outputs</b><span class="c-card-aside">files the server's runs wrote to its own disk</span></div><div class="c-card-pad" id="d-ov-lo"></div></section>`;
    showOverviewCounts();
    pane.detail.querySelector("#d-ov-back").onclick = (e) => {
      e.preventDefault();
      overviewForced = false;
      pane.setSelected(false);
    };
    loTeardown = await renderLocalOutputs(pane.detail.querySelector("#d-ov-lo"), { bare: true });
  }

  function showDetail(id) {
    detailStop?.();
    detailStop = null;
    loTeardown?.();
    loTeardown = null;
    selected = id;
    list.select(id);
    if (!id) {
      showOverview();
      return;
    }
    overviewForced = false;
    pane.setSelected(true);
    detailStop = mountDatasetDetail(pane.detail, id, { onChanged: () => loadAll() });
  }

  await loadAll();
  if (!alive) return;
  showDetail(selected);

  return {
    teardown() {
      alive = false;
      detailStop?.();
      loTeardown?.();
      document.removeEventListener("click", closeKindDd);
    },
    update(p) {
      const id = p.id || null;
      if (id === selected) return;
      showDetail(id);
    },
  };
}

/** Mount one dataset's page into `el`; returns a stop function. */
export function mountDatasetDetail(el, id, hooks = {}) {
  let alive = true;
  let tab = "overview";
  let loStop = null;
  let gen = 0;

  async function load() {
    const mine = ++gen;
    if (!el.querySelector(".c-dhead")) el.innerHTML = `<div class="sp-loading">loading…</div>`;
    let d;
    try {
      d = await api(`/v1/catalog/datasets/${encodeURIComponent(id)}`);
    } catch (e) {
      if (!alive || mine !== gen) return;
      el.innerHTML = `<div class="c-dhead"><a class="sp-back" href="#/catalog">${icon("back")}Datasets</a></div><div class="sp-none"><b>${e.status === 404 && !catalogUnavailable(e) ? "Dataset not found" : "Could not load this dataset"}</b><span>${escapeHtml(catalogUnavailable(e) ? CATALOG_MISSING : e.message)}</span></div>`;
      return;
    }
    if (!alive || mine !== gen) return;
    render(d);
  }

  function render(d) {
    loStop?.();
    loStop = null;
    setCrumbTitle(shortUri(d.uri));
    const stats = d.stats || [];
    const timeline = d.schema_timeline || [];
    const latest = timeline[timeline.length - 1];
    const cols = latest ? Object.keys((latest.schema && latest.schema.properties) || {}).length : null;
    const fresh = Date.now() - Date.parse(d.last_success) < DAY;
    const prof = d.profile;
    const drift = prof ? prof.latest.drift || [] : [];
    const sub = [
      pill(`Last success ${ageOf(d.last_success)} ago`, fresh ? "ok" : "", fresh ? "check" : ""),
      `<span>${escapeHtml(d.kind)} · ${escapeHtml((d.roles || []).join(" + "))}</span>`,
      d.pipeline ? `<span>written by <b>${escapeHtml(d.pipeline)}</b></span>` : "",
      `<span title="${escapeHtml(fmtTime(d.first_seen))}">first seen ${escapeHtml(ageOf(d.first_seen))} ago</span>`,
    ].filter(Boolean);
    const upCount = (d.upstream || []).length;
    const downCount = (d.downstream || []).length;
    el.innerHTML = `
      <div class="c-dhead">
        <a class="sp-back" href="#/catalog">${icon("back")}Datasets</a>
        ${tabs(
          [
            { id: "overview", label: "Overview" },
            { id: "schema", label: "Schema", count: timeline.length || null },
            { id: "profile", label: "Profile", count: drift.length || null },
            { id: "lineage", label: "Lineage", count: upCount + downCount || null },
            { id: "outputs", label: "Outputs" },
          ],
          tab,
        )}
        <span class="c-dhead-r">
          <a class="c-btn" href="#/lineage/${encodeURIComponent(d.id)}">${icon("lineage")}Lineage graph</a>
        </span>
      </div>
      <div><h1 class="c-title" title="${escapeHtml(d.uri)}">${escapeHtml(shortUri(d.uri))}</h1><div class="c-sub">${sub.join('<span class="c-sub-sep">·</span>')}</div></div>
      <div class="d-pane" data-pane="overview">
        ${volumeCard(d, stats, latest, cols, prof)}
        <section class="c-card"><div class="c-card-h">${icon("user")}<b>Owners &amp; consumers</b><span class="c-card-aside">who is told when a change to this dataset is planned</span></div><div id="stewardship"></div></section>
        ${lineageCard(d)}
      </div>
      <div class="d-pane" data-pane="schema" hidden>${schemaCard(timeline)}</div>
      <div class="d-pane" data-pane="profile" hidden><section class="c-card"><div class="c-card-h"><b>Column profiles</b><span class="c-card-aside">learned per run by a <code>profiling:</code> block</span></div><div id="profile" class="c-card-pad"></div></section></div>
      <div class="d-pane" data-pane="lineage" hidden>${lineageCard(d)}</div>
      <div class="d-pane" data-pane="outputs" hidden><section class="c-card"><div class="c-card-h">${icon("logs")}<b>Local outputs</b><span class="c-card-aside">this dataset's files on the server's disk</span></div><div class="c-card-pad" id="lo-section"></div></section></div>`;

    const showTab = (t) => {
      tab = t;
      el.querySelectorAll(".c-dhead .c-tab").forEach((b) => {
        b.classList.toggle("on", b.dataset.tab === t);
        b.setAttribute("aria-selected", String(b.dataset.tab === t));
      });
      el.querySelectorAll(".d-pane").forEach((p) => (p.hidden = p.dataset.pane !== t));
    };
    el.querySelectorAll(".c-dhead .c-tab").forEach((b) => (b.onclick = () => showTab(b.dataset.tab)));
    el.querySelectorAll("[data-tab-link]").forEach((a) => {
      a.onclick = (e) => {
        e.preventDefault();
        showTab(a.dataset.tabLink);
      };
    });
    showTab(tab);

    renderProfile(el.querySelector("#profile"), prof);
    renderStewardship(el.querySelector("#stewardship"), d, () => {
      load();
      hooks.onChanged?.();
    });
    renderLocalOutputs(el.querySelector("#lo-section"), { datasetId: d.id, bare: true }).then((stop) => {
      if (alive) loStop = stop;
      else stop?.();
    });
  }

  load();
  return () => {
    alive = false;
    loStop?.();
  };
}

function volumeCard(d, stats, latest, cols, prof) {
  const pts = stats.map((s) => s.records);
  const last = stats[stats.length - 1];
  const avg = pts.length ? pts.reduce((a, b) => a + b, 0) / pts.length : 0;
  const nullCols = prof ? Object.entries(prof.latest.profile.columns || {}) : [];
  const worst = nullCols.sort((a, b) => b[1].null_rate - a[1].null_rate)[0];
  const history = prof ? prof.history.slice().reverse() : [];
  const items = [
    {
      k: "Rows per run",
      html: `<b>${fmtCompact(last ? last.records : d.last_records)}</b>${pts.length >= 2 ? `<span class="tpl-spark" title="${escapeHtml(`rows per run, oldest → newest: ${pts.map(fmtInt).join(", ")}`)}">${trend(pts, { width: 170, height: 30, color: "var(--text-2)" })}</span>` : ""}<div class="c-kv-s">${pts.length ? `avg ${fmtCompact(Math.round(avg))} over ${fmtInt(pts.length)} run${pts.length === 1 ? "" : "s"}` : "no runs recorded"}</div>`,
    },
    worst
      ? {
          k: `Null rate · ${worst[0]}`,
          html: `<b>${(worst[1].null_rate * 100).toFixed(1)}%</b>${history.length >= 2 ? `<span class="tpl-spark" title="${escapeHtml(`null rate of ${worst[0]} per profiled run: ${history.map((r) => (r.profile.columns[worst[0]] ? `${(r.profile.columns[worst[0]].null_rate * 100).toFixed(1)}%` : "—")).join(", ")}`)}">${trend(history.map((r) => (r.profile.columns[worst[0]] ? r.profile.columns[worst[0]].null_rate : 0)), { width: 170, height: 30, color: "var(--text-2)", min: 0 })}</span>` : ""}<div class="c-kv-s">highest of ${fmtInt(nullCols.length)} profiled column${nullCols.length === 1 ? "" : "s"}</div>`,
        }
      : { k: "Total rows", html: `<b>${fmtCompact(d.total_records)}</b><div class="c-kv-s">${fmtInt(d.total_records)} across every run</div>` },
    { k: "Runs", html: `<b>${fmtInt(d.runs)}</b><div class="c-kv-s">last <span class="mono">${escapeHtml(String(d.last_run_id || "").slice(0, 8))}</span> · ${escapeHtml(fmtTime(d.last_success))}</div>` },
    { k: "Consumers", html: `<b>${fmtInt((d.consumers || []).length)}</b><div class="c-kv-s">${escapeHtml((d.consumers || []).map((c) => c.kind || c.name).slice(0, 3).join(" · ") || "none declared")}</div>` },
  ];
  const head = `${fmtCompact(last ? last.records : d.last_records)} rows last run${cols != null ? ` · ${fmtInt(cols)} column${cols === 1 ? "" : "s"}` : ""}`;
  let note = "";
  let tone = "";
  if (latest && latest.diff) {
    const df = latest.diff;
    const parts = [
      ...(df.added || []).map((c) => `+${typeof c === "string" ? c : c.column}`),
      ...(df.widened || []).map((c) => `~${typeof c === "string" ? c : c.column}`),
      ...(df.changed || []).map((c) => `!${typeof c === "string" ? c : c.column}`),
      ...(df.removed || []).map((c) => `−${typeof c === "string" ? c : c.column}`),
    ];
    if (parts.length) {
      note = `Schema v${latest.version} · ${parts.slice(0, 6).join(" ")}${parts.length > 6 ? " …" : ""} · ${ageOf(latest.recorded_at)} ago`;
      tone = (df.removed || []).length || (df.changed || []).length ? "warn" : "ok";
    }
  }
  if (!note && latest) note = `Schema v${latest.version} since ${fmtTime(latest.recorded_at)}`;
  return `<section class="c-card"><div class="c-card-h">${icon("db")}<b>${escapeHtml(head)}</b>${latest ? `<span class="c-tag">schema v${latest.version}</span>` : ""}<span class="c-card-r">last change ${latest ? `${escapeHtml(ageOf(latest.recorded_at))} ago` : "—"}</span></div><div class="c-kvgrid" style="--c-cols:4">${items
    .map((i) => `<div class="c-kv"><small>${escapeHtml(i.k)}</small>${i.html}</div>`)
    .join("")}</div>${note ? `<div class="c-card-note ${tone}">${escapeHtml(note)}</div>` : ""}${volumeBars(stats)}</section>`;
}

function volumeBars(stats) {
  if (stats.length < 3) return "";
  const max = Math.max(1, ...stats.map((s) => s.records));
  return `<div class="c-card-note c-vol"><div class="volume-bars">${stats
    .map(
      (s) =>
        `<div class="volume-bar" title="${escapeHtml(`${fmtInt(s.records)} rows — ${fmtTime(s.recorded_at)} (run ${s.run_id})`)}" style="height:${Math.max(4, Math.round((s.records / max) * 56))}px"></div>`,
    )
    .join("")}</div></div>`;
}

function lineageCard(d) {
  const side = (title, edges, idOf, uriOf) => `
    <div class="ds-edges"><div class="ds-edges-h">${escapeHtml(title)} <span class="c-muted">${fmtInt(edges.length)}</span></div>${
      edges.length
        ? edges
            .map(
              (e) =>
                `<a class="ds-edge" href="#/catalog/${encodeURIComponent(idOf(e))}" title="${escapeHtml(uriOf(e))}"><span class="c-ell">${escapeHtml(shortUri(uriOf(e)))}</span><span class="c-muted">${fmtInt(e.runs)} run${e.runs === 1 ? "" : "s"} · ${fmtCompact(e.last_records)} rows</span></a>`,
            )
            .join("")
        : `<div class="c-muted ds-edge-none">none recorded</div>`
    }</div>`;
  return `<section class="c-card"><div class="c-card-h">${icon("lineage")}<b>Lineage</b><span class="c-card-aside">pipelines that read or write this dataset</span><a class="c-card-r" href="#/lineage/${encodeURIComponent(d.id)}">open the graph</a></div><div class="ds-lineage">${side("Upstream", d.upstream || [], (e) => e.src_id, (e) => e.src_uri)}<div class="ds-lineage-mid" title="${escapeHtml(d.uri)}"><span class="c-ell">${escapeHtml(shortUri(d.uri))}</span><span class="c-muted">${escapeHtml(d.kind)}</span></div>${side("Downstream", d.downstream || [], (e) => e.dst_id, (e) => e.dst_uri)}</div></section>`;
}

function schemaCard(timeline) {
  if (!timeline.length) {
    return `<section class="c-card"><div class="c-card-h"><b>Schema timeline</b></div><div class="c-card-empty">No schema observed yet — the catalog samples records from each successful run.</div></section>`;
  }
  const rows = timeline
    .slice()
    .reverse()
    .map((v) => {
      const cols = Object.keys((v.schema && v.schema.properties) || {});
      const badges = diffBadges(v.diff);
      return `<tr class="ds-sv" data-v="${v.version}"><td class="mono">v${v.version}</td><td>${badges || `<span class="c-muted">${v.version === timeline[0].version ? "first observed" : "no change"}</span>`}</td><td class="num">${fmtInt(cols.length)}</td><td class="c-muted c-nowrap" title="${escapeHtml(fmtTime(v.recorded_at))}">${escapeHtml(ageOf(v.recorded_at))} ago</td><td><button type="button" class="c-btn ds-sv-btn">JSON</button></td></tr><tr class="ds-sv-json" hidden><td colspan="5"><pre class="mono schema-json">${escapeHtml(JSON.stringify(v.schema, null, 2))}</pre></td></tr>`;
    })
    .join("");
  return `<section class="c-card"><div class="c-card-h"><b>Schema timeline</b><span class="c-card-r">${fmtInt(timeline.length)} version${timeline.length === 1 ? "" : "s"} · newest first</span></div><div class="d-scroll-x"><table class="c-tbl ds-sv-tbl"><colgroup><col style="width:72px" /><col /><col style="width:84px" /><col style="width:110px" /><col style="width:84px" /></colgroup><thead><tr><th>Version</th><th>Change</th><th class="num">Columns</th><th>When</th><th></th></tr></thead><tbody>${rows}</tbody></table></div></section>`;
}

document.addEventListener("click", (e) => {
  const b = e.target.closest(".ds-sv-btn");
  if (!b) return;
  const next = b.closest("tr").nextElementSibling;
  if (next) next.hidden = !next.hidden;
});


// ── Local outputs (#587/#588) ───────────────────────────────────────────────

const LOCAL_OUTPUTS_MISSING =
  "Local-output retention is not available on this server " +
  "(faucet was built without the `catalog` feature).";

/** The confirm text for a scope that ignores retention windows. */
const CLEAN_ALL_PROMPT = (what) =>
  `Delete ${what}, including files still inside their retention window?\n\n` +
  "Only files faucet created are deleted. Run history, catalog entries, " +
  "and lineage are not touched.";

/** State labels + one-line meanings, so the UI never shows a bare word. */
const STATE_HINT = {
  present: "on disk",
  expired: "cleaned — the run record is kept",
  external:
    "faucet wrote this file but did not create it, so it is never cleaned — and not previewed either",
  replaced:
    "faucet overwrote a file it did not create: previewable, since it holds only faucet's output, but never cleaned",
};

/**
 * Render the "Local outputs" panel into `host`.
 *
 * `scope.datasetId` narrows it to one dataset (the detail view); omitted lists
 * everything (the browser). Destructive controls are rendered only when the
 * server says the caller holds the manage scope — a viewer sees the list and no
 * buttons, rather than buttons that can only 403.
 */
export async function renderLocalOutputs(host, scope = {}) {
  if (!host) return null;
  const datasetId = scope.datasetId || null;
  // Collapse the Manage disclosure when clicking anywhere outside it. One
  // delegated document listener per panel render — installed here (not inside
  // `load()`, which re-runs on every refresh/toggle and would accumulate one
  // listener per click) and removed by the returned teardown. (Named
  // `removeListeners` — `cleanup` is already the local-outputs sweep helper.)
  const collapseManage = (e) => {
    const manage = host.querySelector(".lo-manage");
    if (manage && manage.open && !manage.contains(e.target)) manage.open = false;
  };
  document.addEventListener("click", collapseManage);
  const removeListeners = () => document.removeEventListener("click", collapseManage);
  let showExpired = false;

  host.innerHTML = `
    ${scope.bare ? "" : `<h2 class="lo-head">Local outputs</h2>`}
    <div class="lo-body"><div class="sp-loading">loading…</div></div>`;
  const body = host.querySelector(".lo-body");

  async function load() {
    const p = new URLSearchParams();
    if (datasetId) p.set("dataset_id", datasetId);
    if (showExpired) p.set("include_expired", "true");
    let data;
    try {
      data = await api(`/v1/local-outputs?${p}`);
    } catch (e) {
      body.innerHTML = `<div class="empty">${
        catalogUnavailable(e) ? LOCAL_OUTPUTS_MISSING : escapeHtml(e.message)
      }</div>`;
      return;
    }
    paint(data);
  }

  // On a dataset's page the age purge is scoped to that dataset (the server
  // combines `dataset_id` with `older_than_days`), and the label says so.
  function purgeLabel(n) {
    const days = n === "" ? "N days" : `${n} day${n === "1" ? "" : "s"}`;
    return datasetId ? `Purge this dataset's outputs older than ${days}` : `Purge older than ${days}`;
  }

  function paint(data) {
    const {
      outputs,
      retention_days: retention,
      gc_enabled: gcOn,
      can_manage: canManage,
      preview_enabled: canPreview,
      preview_default_rows: previewDefault,
      preview_max_rows: previewMax,
    } = data;
    // Either cap may be null — the server's way of saying "no limit".
    const caps = { defaultRows: previewDefault, maxRows: previewMax };
    body.innerHTML = `
      <div class="lo-bar">
        <span class="run-meta">${outputs.length} tracked${
          gcOn
            ? ` · auto-cleaned after ${retention} day${retention === 1 ? "" : "s"}`
            : " · automatic cleanup disabled"
        }</span>
        <details class="lo-manage">
          <summary>Manage ▾</summary>
          <div class="lo-manage-menu">
            <label class="lo-toggle"><input type="checkbox" id="lo-expired" ${
              showExpired ? "checked" : ""
            } /> show cleaned</label>
            ${
              canManage
                ? `<span class="lo-purge">
                     <input type="number" id="lo-days" min="0" value="${retention}" />
                     <button class="btn-warn" id="lo-purge-btn">${purgeLabel(String(retention))}</button>
                   </span>
                   <button class="btn-danger" id="lo-all">${
                     datasetId ? "Clean this dataset's outputs" : "Clean all local outputs"
                   }</button>`
                : ""
            }
            <button class="btn-ghost" id="lo-refresh">↻ refresh</button>
          </div>
        </details>
      </div>
      <div class="lo-list">${
        outputs.length
          ? outputs.map((o) => outputRow(o, canManage, canPreview)).join("")
          : `<div class="empty">No local output files tracked${
              datasetId ? " for this dataset" : ""
            } yet — run a pipeline with a file, jsonl, csv, or parquet sink.</div>`
      }</div>`;

    body.querySelector("#lo-expired").onchange = (e) => {
      showExpired = e.target.checked;
      load();
    };
    body.querySelector("#lo-refresh").onclick = () => load();



    body.querySelectorAll(".lo-preview-btn").forEach((btn) => {
      btn.onclick = () => togglePreview(btn, caps);
    });

    if (!canManage) return;
    const daysInput = body.querySelector("#lo-days");
    const purgeBtn = body.querySelector("#lo-purge-btn");
    // Keep the button label in sync with the typed window, so N is never a
    // placeholder — it always names the exact days that will be purged.
    daysInput.oninput = () => {
      purgeBtn.textContent = purgeLabel(daysInput.value.trim());
    };
    purgeBtn.onclick = () => {
      const days = Number(daysInput.value);
      if (!Number.isFinite(days) || days < 0) {
        toast("Enter a number of days (0 or more).", "error");
        return;
      }
      // The typed window *is* the intent, so no dialog — except for 0, which
      // means "everything" and clears the same gate as "clean all".
      // A dataset-scoped purge is bounded by the dataset, so it needs no gate.
      if (datasetId) {
        cleanup(
          { dataset_id: datasetId, older_than_days: days },
          `purged this dataset's outputs older than ${days} day(s)`,
        );
        return;
      }
      if (days === 0 && !confirm(CLEAN_ALL_PROMPT("every tracked local output"))) return;
      cleanup(
        { older_than_days: days, confirm: days === 0 },
        `purged outputs older than ${days} day(s)`,
      );
    };
    body.querySelector("#lo-all").onclick = () => {
      // "Clean all" removes files that are still inside their retention window,
      // so it is never a bare one-click.
      const what = datasetId ? "this dataset's tracked outputs" : "every tracked local output";
      if (!confirm(CLEAN_ALL_PROMPT(what))) return;
      // `confirm: true` is what the server's own gate requires for a scope that
      // can delete files still inside their retention window — the dialog above
      // is what earns it. A scripted caller has to opt in deliberately.
      cleanup(
        datasetId ? { dataset_id: datasetId } : { all: true, confirm: true },
        "cleaned local outputs",
      );
    };

    body.querySelectorAll(".lo-del").forEach((btn) => {
      btn.onclick = async () => {
        btn.disabled = true;
        try {
          report(await api(`/v1/local-outputs/${encodeURIComponent(btn.dataset.id)}`, {
            method: "DELETE",
          }));
          await load();
        } catch (e) {
          toast(e.message, "error");
          btn.disabled = false;
        }
      };
    });
  }

  async function cleanup(payload, ok) {
    try {
      report(await api("/v1/local-outputs/cleanup", { method: "POST", body: payload }), ok);
      await load();
    } catch (e) {
      toast(e.message, "error");
    }
  }

  /**
   * Turn a sweep report into one toast. A refusal is a successful request with
   * `deleted: 0`, so "nothing happened" must always come with the reason —
   * otherwise the button looks broken when it was in fact protecting a file.
   */
  function report(rep, fallback) {
    if (rep.deleted > 0) {
      toast(`${fallback || "cleaned"}: ${rep.deleted} file(s), ${fmtBytes(rep.bytes)} reclaimed`);
    }
    const skipped = rep.outputs.filter((o) => o.skipped);
    if (!rep.deleted && !skipped.length) {
      toast("Nothing to clean.");
    }
    for (const o of skipped.slice(0, 3)) {
      toast(`${o.path}: ${skipReason(o)}`, o.skipped === "delete_failed" ? "error" : "info");
    }
    if (skipped.length > 3) toast(`…and ${skipped.length - 3} more skipped.`);
  }

  await load();
  return removeListeners;
}

function outputRow(o, canManage, canPreview) {
  // The row and its preview panel are wrapped together so the panel can expand
  // *below* the row instead of becoming another grid cell inside it.
  return `
    <div class="lo-item" data-id="${escapeHtml(o.id)}">
      <div class="run-row lo-row lo-${escapeHtml(o.state)}">
        <span class="pill">${escapeHtml(o.kind)}</span>
        <span class="run-name mono" title="${escapeHtml(o.path)}">${escapeHtml(o.path)}</span>
        <span class="pill lo-state" title="${escapeHtml(STATE_HINT[o.state] || "")}">${escapeHtml(
          o.state,
        )}</span>
        <span class="run-meta">${escapeHtml(fmtAge(o.age_secs))}</span>
        <span class="run-meta run-time" title="last written">${fmtTime(o.last_written_at)}</span>
        ${
          canPreview && PREVIEWABLE.has(o.kind) && (o.state === "present" || o.state === "replaced")
            ? `<button class="btn-ghost lo-preview-btn">Preview</button>`
            : `<span class="run-meta"></span>`
        }
        ${
          canManage && o.state === "present"
            ? `<button class="btn-danger lo-del" data-id="${escapeHtml(o.id)}">Delete now</button>`
            : `<span class="run-meta"></span>`
        }
      </div>
      <div class="lo-preview" hidden></div>
    </div>`;
}

// ── Dataset preview (#586) ──────────────────────────────────────────────────
//
// "N records written" is not the same information as "here are the records". A
// preview is a **source-backed capped read**: the server reads the output back
// through the matching source connector (csv → source-csv, parquet →
// source-parquet, jsonl → its reader) and returns the first N rows. The client
// never names a path — only the ledger id of an output the server already
// tracks — so there is nothing here that could point the read somewhere else.
//
// The server owns the caps; this panel only *reflects* them (`max` on the input,
// the pre-filled default), so a typed number is never silently clamped without
// the user having been told the ceiling.
//
// Only a `present` or `replaced` output gets a Preview button (a `replaced` file
// already existed, but faucet truncated it, so it holds only faucet's output).
// An `expired` one has no file left, and an `external` one is a file faucet wrote to but did not create — the server refuses
// to serve its contents for the same reason the GC refuses to delete it — so
// offering either would be offering a button that can only fail.

/** Sink kinds that have a reader on the server. Anything else gets no button. */
const PREVIEWABLE = new Set(["jsonl", "csv", "parquet", "file"]);

/** Truncation point for one cell's rendered text. */
const CELL_MAX = 240;

/**
 * Why a read stopped short, in the user's words. A partial answer that does not
 * say it is partial is the one genuinely dangerous thing a preview can do —
 * every one of these must read as "there is more", not as an error.
 */
const CAPPED_HINT = {
  rows: "stopped at the row limit — the dataset has more",
  bytes: "stopped at this server's response-size budget — the dataset has more",
  time: "stopped at this server's read deadline — the dataset has more",
};

/**
 * Expand / collapse an output's preview panel. The first expansion builds the
 * panel and loads; later ones just re-show what was already fetched, so
 * collapsing is not a reason to re-read the file.
 */
function togglePreview(btn, caps) {
  const item = btn.closest(".lo-item");
  const panel = item.querySelector(".lo-preview");
  if (!panel.hidden) {
    panel.hidden = true;
    btn.textContent = "Preview";
    return;
  }
  panel.hidden = false;
  btn.textContent = "Hide";
  if (panel.dataset.ready) return;
  panel.dataset.ready = "1";
  // The ceiling is the server's, so the input advertises it rather than letting
  // someone type a number that comes back quietly clamped.
  const ceiling = caps.maxRows
    ? ` max="${caps.maxRows}" title="this server caps a preview at ${caps.maxRows} rows"`
    : ` title="this server sets no row ceiling"`;
  panel.innerHTML = `
    <div class="dp-bar">
      <label class="dp-rows-label">rows
        <input type="number" class="dp-rows" min="1"${ceiling} value="${
          caps.defaultRows ?? ""
        }" placeholder="all" />
      </label>
      <button class="btn-ghost dp-load">Load</button>
      <button class="btn-ghost dp-load-all" title="${
        caps.maxRows
          ? `load up to this server's preview ceiling of ${caps.maxRows} rows`
          : "every row in the dataset"
      }">${caps.maxRows ? `Max (${caps.maxRows})` : "All rows"}</button>
      <span class="run-meta dp-status"></span>
    </div>
    <div class="dp-body"></div>`;
  const load = (all) => loadPreview(item.dataset.id, panel, caps, all);
  panel.querySelector(".dp-load").onclick = () => load(false);
  panel.querySelector(".dp-load-all").onclick = () => load(true);
  panel.querySelector(".dp-rows").onkeydown = (e) => {
    if (e.key === "Enter") load(false);
  };
  load(false);
}

/**
 * Fetch one page and render it. Errors land in the panel, not a toast.
 *
 * `all` asks for the whole dataset (`row_count_to_load=all`). The server decides
 * what that means: with a ceiling configured it comes back clamped and the status
 * line says so, which is why this asks rather than computing a big number.
 */
async function loadPreview(id, panel, caps, all) {
  const input = panel.querySelector(".dp-rows");
  const want = all ? "all" : rowsParam(input.value, caps);
  const status = panel.querySelector(".dp-status");
  const out = panel.querySelector(".dp-body");
  status.textContent = "loading…";
  try {
    const data = await api(
      `/v1/local-outputs/${encodeURIComponent(id)}/preview?row_count_to_load=${want}`,
    );
    // Reflect what the server actually resolved to — an out-of-range entry (or
    // "all" against a ceiling) visibly becomes the number that was really used.
    input.value = data.row_limit === null ? "" : String(data.row_limit);
    status.textContent = previewStatus(data, caps);
    out.innerHTML = previewTable(data);
  } catch (e) {
    // Every documented failure here is expected and explainable — the file was
    // cleaned (#587), previews are disabled, the last line is half-written — so
    // the server's message is the useful thing to show.
    status.textContent = "";
    out.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
  }
}

/**
 * The `row_count_to_load` value for a typed row count. An empty or nonsensical
 * entry falls back to the server's default (or `all` where that *is* the
 * default) rather than being sent as-is for the server to reject.
 */
function rowsParam(raw, caps) {
  const n = Math.floor(Number(raw));
  if (!Number.isFinite(n) || n < 1) return caps.defaultRows ?? "all";
  return caps.maxRows ? Math.min(n, caps.maxRows) : n;
}

function previewStatus(d, caps) {
  const n = d.row_count;
  const ms = `${d.elapsed_ms} ms`;
  if (d.truncated) {
    const cap = caps && caps.maxRows ? ` (UI cap ${fmtInt(caps.maxRows)})` : "";
    return `${fmtInt(n)} row${n === 1 ? "" : "s"}${cap} · ${ms} · output has more`;
  }
  return `${fmtInt(n)} row${n === 1 ? "" : "s"} · ${ms} · whole output`;
}

function previewTable(d) {
  if (!d.row_count) return `<div class="empty">This output has no rows.</div>`;
  const cols = d.columns.length ? d.columns : null;
  const head = cols
    ? cols.map((c) => `<th>${escapeHtml(c)}</th>`).join("")
    : `<th>value</th>`;
  const body = d.rows.map((r) => bodyRow(r, cols)).join("");
  return `<div class="dp-scroll"><table class="dp-table">
      <thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function bodyRow(r, cols) {
  // A record that is not an object has no columns to slot into. Rendering it
  // across the row keeps its value visible instead of showing a line of blanks.
  if (!cols || !isPlainObject(r)) {
    return `<tr><td class="dp-raw"${cols ? ` colspan="${cols.length}"` : ""}>${cell(r)}</td></tr>`;
  }
  return `<tr>${cols.map((c) => `<td>${cell(r[c])}</td>`).join("")}</tr>`;
}

function isPlainObject(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

/**
 * Render one cell. A *missing* field and a `null` one are different facts about
 * the record, so they must not render identically.
 */
function cell(v) {
  if (v === undefined) {
    return `<span class="dp-absent" title="this record has no such field">—</span>`;
  }
  if (v === null) return `<span class="dp-absent">null</span>`;
  const text = typeof v === "string" ? v : JSON.stringify(v);
  if (text.length <= CELL_MAX) return escapeHtml(text);
  // One enormous value (a blob, an embedded document) must not blow up the
  // table; the full text stays reachable in the tooltip.
  return `<span title="${escapeHtml(text.slice(0, 2000))}">${escapeHtml(
    text.slice(0, CELL_MAX),
  )}…</span>`;
}

/** Why a file was left alone, in the user's words rather than the enum's. */
function skipReason(o) {
  switch (o.skipped) {
    case "pre_existing":
      return "not deleted — faucet wrote this file but did not create it";
    case "already_deleted":
      return "already cleaned";
    case "not_on_disk":
      return "already gone from disk — marked cleaned";
    case "in_flight":
      return "a run is still writing it — will be retried";
    case "delete_failed":
      return `could not delete — ${o.error || "unknown error"}`;
    default:
      return "skipped";
  }
}

function fmtAge(secs) {
  if (secs < 60) return "just now";
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m old`;
  const hours = Math.floor(mins / 60);
  if (hours < 48) return `${hours}h old`;
  return `${Math.floor(hours / 24)}d old`;
}

function fmtBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MiB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(1)} GiB`;
}

// Owners + declared consumers (#707): who is told when a change to this
// dataset is planned. Pipelines that read it are consumers automatically (the
// lineage edges below); dashboards, models and exports are declared here.
// The form is gated on `catalog_annotate` (operator+) — a viewer only reads.
function renderStewardship(el, d, reload) {
  const owners = d.owners || [];
  const consumers = d.consumers || [];
  const consumerRow = (c) => `
    <div class="consumer-row">
      <span class="consumer-name">${escapeHtml(c.name)}</span>
      ${c.kind ? `<span class="pill">${escapeHtml(c.kind)}</span>` : ""}
      <span class="run-meta">${
        c.columns && c.columns.length
          ? `reads ${escapeHtml(c.columns.join(", "))}`
          : "reads every column"
      }</span>
      ${c.contact ? `<span class="run-meta mono">${escapeHtml(c.contact)}</span>` : ""}
      <span class="run-meta">by ${escapeHtml(c.registered_by || "?")}</span>
    </div>`;
  el.innerHTML = `
    <div class="stewardship">
      <div class="stewardship-owners">
        <label>Owners</label>
        ${
          owners.length
            ? owners.map((o) => `<span class="pill owner-pill">${escapeHtml(o)}</span>`).join(" ")
            : `<span class="run-meta">none declared</span>`
        }
      </div>
      <div class="stewardship-consumers">
        <label>Declared consumers</label>
        ${consumers.length ? consumers.map(consumerRow).join("") : `<div class="empty">none declared — pipelines reading this dataset are listed under Lineage</div>`}
      </div>
      <details class="annotate-form" data-perm="catalog_annotate">
        <summary>Annotate…</summary>
        <div class="annotate-fields">
          <label>owners (comma-separated; empty = unchanged)
            <input id="an-owners" value="${escapeHtml(owners.join(", "))}" placeholder="team-data, alice@example.com" />
          </label>
          <label class="inline-check"><input type="checkbox" id="an-clear-owners" /> clear all owners</label>
          <label>consumer name <input id="an-name" placeholder="revenue-dashboard" /></label>
          <label>kind <input id="an-kind" placeholder="dashboard / model / export" /></label>
          <label>contact <input id="an-contact" placeholder="#bi-team" /></label>
          <label>columns it reads (comma-separated; empty = all) <input id="an-columns" placeholder="amount, currency" /></label>
        </div>
        <div class="submit-actions">
          <button id="an-save" class="btn-primary">Save</button>
          <span id="an-out" class="run-meta"></span>
        </div>
      </details>
    </div>`;
  const out = el.querySelector("#an-out");
  el.querySelector("#an-save").onclick = async () => {
    const split = (v) => v.split(",").map((s) => s.trim()).filter(Boolean);
    const ownersRaw = el.querySelector("#an-owners").value;
    const body = {};
    const nextOwners = split(ownersRaw);
    if (el.querySelector("#an-clear-owners").checked) {
      if (owners.length) body.owners = [];
    } else if (nextOwners.length && nextOwners.join("|") !== owners.join("|")) {
      body.owners = nextOwners;
    }
    const name = el.querySelector("#an-name").value.trim();
    if (name) {
      const c = { name };
      const kind = el.querySelector("#an-kind").value.trim();
      const contact = el.querySelector("#an-contact").value.trim();
      const cols = split(el.querySelector("#an-columns").value);
      if (kind) c.kind = kind;
      if (contact) c.contact = contact;
      if (cols.length) c.columns = cols;
      body.consumers = [c];
    }
    if (body.owners === undefined && !body.consumers) {
      out.textContent = "nothing to save — change the owners or name a consumer";
      return;
    }
    try {
      await api(`/v1/catalog/datasets/${encodeURIComponent(d.id)}/consumers`, {
        method: "POST",
        body,
      });
      toast("annotation saved");
      reload();
    } catch (e) {
      out.textContent = `✗ ${e.message}`;
    }
  };
}

// Learned column profiles (#708): the latest run's per-column statistics with
// a null-rate sparkline over the recorded history and the drift findings.
function renderProfile(el, profile) {
  if (!profile) {
    el.innerHTML = `<div class="empty">no column profiles yet — add a <code>profiling:</code> block to the pipeline that writes this dataset</div>`;
    return;
  }
  const latest = profile.latest;
  const history = profile.history.slice().reverse(); // oldest first
  const columns = Object.entries(latest.profile.columns || {});
  const drift = latest.drift || [];
  const driftFor = (col) => drift.filter((d) => d.column === col);
  const driftRuns = (col) =>
    history.map((r) => (r.drift || []).some((d) => d.column === col));

  const head = `<div class="profile-head">
      <span>latest run <span class="mono">${escapeHtml(latest.run_id)}</span> · ${fmtTime(latest.recorded_at)}</span>
      <span>${fmtInt(latest.profile.rows)} rows · ${columns.length} column${columns.length === 1 ? "" : "s"}${
        latest.profile.skipped_columns ? ` (+${fmtInt(latest.profile.skipped_columns)} beyond max_columns)` : ""
      }</span>
      <span>baseline ${fmtInt(latest.baseline_runs)} run${latest.baseline_runs === 1 ? "" : "s"} · ${history.length} recorded</span>
      ${
        drift.length
          ? `<span class="pill drift-flag">${drift.length} drift finding${drift.length === 1 ? "" : "s"}</span>`
          : `<span class="pill diff-added">stable</span>`
      }
    </div>`;

  const findings = drift.length
    ? `<ul class="profile-drift">${drift
        .map(
          (d) =>
            `<li><span class="pill drift-flag">${escapeHtml(d.column)}.${escapeHtml(d.metric)}</span> <span class="run-meta">${escapeHtml(d.detail)}</span></li>`,
        )
        .join("")}</ul>`
    : "";

  const rows = columns
    .map(([name, c]) => {
      const types = Object.entries(c.types || {})
        .filter(([t, n]) => t !== "null" && n > 0)
        .sort((a, b) => b[1] - a[1])
        .map(([t]) => t)
        .join("/");
      const summary = c.numeric
        ? `min ${fmtNum(c.numeric.min)} · mean ${fmtNum(c.numeric.mean)} · max ${fmtNum(c.numeric.max)}`
        : c.string
          ? `len ${c.string.len_min}–${c.string.len_max} · mean ${fmtNum(c.string.len_mean)}`
          : "";
      const top = c.high_cardinality
        ? `<span class="run-meta">high cardinality</span>`
        : (c.top_values || [])
            .slice(0, 4)
            .map(
              (t) =>
                `<span class="profile-chip" title="${escapeHtml(`${fmtInt(t.count)} occurrences`)}">${escapeHtml(
                  t.value,
                )} <b>${(t.share * 100).toFixed(1)}%</b></span>`,
            )
            .join("");
      const flagged = driftFor(name);
      return `<tr class="${flagged.length ? "profile-drifted" : ""}">
          <td class="mono">${escapeHtml(name)}${
            flagged.length
              ? `<div class="profile-flags">${[...new Set(flagged.map((d) => d.metric))]
                  .map(
                    (m) =>
                      `<span class="pill drift-flag" title="${escapeHtml(
                        flagged
                          .filter((d) => d.metric === m)
                          .map((d) => d.detail)
                          .join("\n"),
                      )}">${escapeHtml(m)}</span>`,
                  )
                  .join("")}</div>`
              : ""
          }</td>
          <td><span class="pill">${escapeHtml(types || "null")}</span></td>
          <td class="profile-null">${(c.null_rate * 100).toFixed(1)}% ${sparkline(
            history.map((r) => (r.profile.columns[name] ? r.profile.columns[name].null_rate : null)),
            driftRuns(name),
          )}</td>
          <td class="num">${fmtInt(c.distinct)}</td>
          <td class="run-meta">${escapeHtml(summary)}</td>
          <td>${top}</td>
        </tr>`;
    })
    .join("");

  el.innerHTML = `${head}${findings}
    <div class="table-scroll"><table class="ds-table profile-table">
      <thead><tr>
        <th>column</th><th>type</th><th>null rate</th><th>distinct</th><th>values</th><th>top values</th>
      </tr></thead>
      <tbody>${rows || `<tr><td colspan="6" class="run-meta">no columns profiled</td></tr>`}</tbody>
    </table></div>`;
}

function fmtNum(x) {
  if (x === null || x === undefined || !Number.isFinite(x)) return "–";
  return Number.isInteger(x) ? fmtInt(x) : x.toFixed(Math.abs(x) < 10 ? 3 : 1);
}

// A tiny inline null-rate trend, oldest → newest, with a marker on every run
// that raised drift on the column. Missing points (the column was absent that
// run) break the line.
function sparkline(values, flagged) {
  const n = values.length;
  if (n < 2) return "";
  const w = 84;
  const h = 18;
  const max = Math.max(0.01, ...values.filter((v) => v !== null));
  const x = (i) => (n === 1 ? w / 2 : (i / (n - 1)) * (w - 4) + 2);
  const y = (v) => h - 2 - (v / max) * (h - 4);
  let d = "";
  let pen = false;
  values.forEach((v, i) => {
    if (v === null) {
      pen = false;
      return;
    }
    d += `${pen ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)} `;
    pen = true;
  });
  const dots = values
    .map((v, i) =>
      v !== null && flagged[i]
        ? `<circle cx="${x(i).toFixed(1)}" cy="${y(v).toFixed(1)}" r="2.2" class="spark-drift"/>`
        : "",
    )
    .join("");
  const last = values[n - 1];
  const end =
    last !== null
      ? `<circle cx="${x(n - 1).toFixed(1)}" cy="${y(last).toFixed(1)}" r="1.8" class="spark-end"/>`
      : "";
  const tip = values.map((v) => (v === null ? "—" : `${(v * 100).toFixed(1)}%`)).join(", ");
  return `<svg class="spark" viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" role="img" aria-label="null rate per run"><title>null rate per profiled run, oldest → newest: ${tip}${flagged.some(Boolean) ? " (dots: runs that raised drift)" : ""}</title><path d="${d.trim()}"/>${dots}${end}</svg>`;
}

function diffBadges(diff) {
  if (!diff) return "";
  const name = (c) => (typeof c === "string" ? c : c.column);
  const badge = (cls, sym, items) =>
    (items || [])
      .map((c) => `<span class="pill diff-${cls}">${sym}${escapeHtml(name(c))}</span>`)
      .join("");
  return (
    badge("added", "+", diff.added) +
    badge("widened", "~", diff.widened) +
    badge("changed", "!", diff.changed) +
    badge("removed", "−", diff.removed)
  );
}

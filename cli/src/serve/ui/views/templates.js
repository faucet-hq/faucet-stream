// Pipeline template registry (#444): the templates, by kind, beside the
// selected template's versions page — one row per registered version showing
// which channels point at it, with the release controls (assign a channel,
// launch, roll back, deprecate) and a typed trigger form built from the
// template's `params:` block. With nothing selected the detail pane holds the
// registry-wide tools: the compatibility matrix, the register editor and sync.
//
// The lifecycle model this view renders:
//   • registering a version never moves callers — it only extends the build list
//   • `launch` is the one mindful step that moves `stable` (and so unpinned runs)
//   • `stable` / `previous` / `newest` are derived; `dev`…`prod` are assignable
import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { copyButton, escapeHtml, mdInline, fmtInt, fmtAge } from "../utils.js";
import { fmtTime } from "./runs.js";
import { setCrumbTitle } from "../shell.js";
import { can } from "../access.js";
import { icon, statStrip, listRow, tabs, pill, chip, card, kvGrid } from "../components.js";
import { splitPane, sectionList } from "./split.js";

const TEMPLATES_MISSING =
  "The pipeline template endpoints are not available on this server " +
  "(faucet was built without the `templates` feature, or no --template-store was configured).";

/** Channels a user may point at a version. Derived ones are never assignable. */
export const ASSIGNABLE = ["dev", "test", "staging", "pre-prod", "canary", "prod"];

/** A route is not wired at all when the feature is compiled out: bare 404. */
export function templatesUnavailable(e) {
  return e && e.status === 404 && !e.code;
}

/** Template kind → label. Old rows without a stored kind are pipelines. */
const KINDS = ["source-template", "sink-template", "deployment", "pipeline"];
const KIND_LABEL = { "source-template": "source", "sink-template": "sink", deployment: "deployment", pipeline: "pipeline" };
const KIND_TITLE = {
  "source-template": "source template — a system and its streams; runs composed with a sink template",
  "sink-template": "sink template — a destination; composed into a source template's run",
  deployment: "deployment overlay — state, DLQ, notifications and SLA applied over a composed run",
  pipeline: "complete pipeline config",
};
function kindOf(t) {
  return KINDS.includes(t.kind) ? t.kind : "pipeline";
}

const SECTIONS = [
  ["source-template", "Source templates"],
  ["sink-template", "Sink templates"],
  ["deployment", "Deployments"],
  ["pipeline", "Pipelines"],
];

const LIST_TABS = [
  { id: "all", label: "All", statuses: ["launched", "draft"] },
  { id: "launched", label: "Launched", statuses: ["launched"] },
  { id: "draft", label: "Draft", statuses: ["draft"] },
];

/** The newest version's test status, from the list summary. */
function newestTests(st) {
  const ts = st.test_status || {};
  return st.newest != null ? ts[st.newest] || ts[String(st.newest)] || null : null;
}

/** Ring tone + shape and the lead fact a template's list row shows. */
function rowState(t) {
  const st = t.state || {};
  const status = st.status || "draft";
  const nt = newestTests(st);
  if (status === "deprecated") return { tone: "muted", shape: "hollow", lead: "deprecated" };
  if (nt === "failing" || nt === "unknown") return { tone: "bad", shape: "full", lead: `v${st.newest} tests ${nt}` };
  if (status === "draft") return { tone: "warn", shape: "hollow", lead: "draft" };
  return { tone: "ok", shape: "full", lead: `stable v${st.stable}` };
}

function rowMeta(t) {
  const st = t.state || {};
  const parts = [];
  if ((st.status || "draft") === "draft") parts.push("never launched");
  for (const [tag, v] of Object.entries(st.tags || {})) parts.push(`${tag} v${v}`);
  if (st.newest != null && st.stable != null && st.newest !== st.stable) parts.push(`newest v${st.newest}`);
  const nt = newestTests(st);
  if (nt === "passing") parts.push("tests passed");
  else if (nt === "untested") parts.push("tests not run");
  else if (nt === "no_tests" || (!st.has_tests && !nt)) parts.push("no tests");
  const n = Object.keys(t.params || {}).length;
  if (n) parts.push(`${fmtInt(n)} param${n === 1 ? "" : "s"}`);
  return parts.join(" · ");
}

// ── list ────────────────────────────────────────────────────────────────────

export async function renderTemplates(container, params = {}) {
  let all = [];
  let syncOrigins = [];
  let unavailable = "";
  let selected = params.id || null;
  let query = params.query || new URLSearchParams();
  let tab = "all";
  let search = "";
  let sortBy = "name";
  let showRetired = false;
  let overviewTab = "matrix";
  let overviewForced = false;
  let detailStop = null;
  let alive = true;

  const pane = splitPane(container, {
    name: "Templates",
    listHead: `
      <div class="sp-tabs">
        ${tabs(LIST_TABS, tab)}
        <span class="sp-tools">
          <button type="button" class="icon-btn" id="t-filter" title="Search and sort" aria-label="Search and sort" aria-expanded="false">${icon("filter")}<span class="sp-dot" hidden></span></button>
          <button type="button" class="icon-btn" id="t-refresh" title="Refresh" aria-label="Refresh">${icon("refresh")}</button>
          <button type="button" class="icon-btn" id="t-matrix" title="Compatibility matrix" aria-label="Compatibility matrix">${icon("matrix")}</button>
          <button type="button" class="icon-btn" id="t-new" data-perm="template_admin" title="Register a template" aria-label="Register a template">${icon("plus")}</button>
        </span>
      </div>
      <div class="sp-filters sp-filters-1" id="t-filters" hidden>
        <input id="t-search" type="search" autocomplete="off" placeholder="search by id or description" />
        <label class="sp-sort">sort
          <select id="t-sort"><option value="name">name</option><option value="updated">last updated</option></select>
        </label>
        <label class="sp-check"><input type="checkbox" id="t-retired" /> include deprecated templates</label>
      </div>
      <div id="t-stats"></div>`,
  });
  const $ = (sel) => pane.head.querySelector(sel);
  const list = sectionList(pane.body, "faucet.templates.sections");

  pane.head.querySelectorAll(".c-tab").forEach((b) => {
    b.onclick = () => {
      tab = b.dataset.tab;
      pane.head.querySelectorAll(".c-tab").forEach((x) => {
        x.classList.toggle("on", x === b);
        x.setAttribute("aria-selected", String(x === b));
      });
      paint();
    };
  });
  const filterBtn = $("#t-filter");
  filterBtn.onclick = () => {
    const panel = $("#t-filters");
    panel.hidden = !panel.hidden;
    filterBtn.setAttribute("aria-expanded", String(!panel.hidden));
    filterBtn.classList.toggle("on", !panel.hidden);
    if (!panel.hidden) $("#t-search").focus();
  };
  $("#t-search").oninput = (e) => {
    search = e.target.value.trim().toLowerCase();
    markFiltered();
    paint();
  };
  $("#t-retired").onchange = (e) => {
    showRetired = e.target.checked;
    markFiltered();
    paint();
  };
  const markFiltered = () => ($("#t-filter .sp-dot").hidden = !(search || showRetired));
  $("#t-sort").onchange = (e) => {
    sortBy = e.target.value;
    paint();
  };
  $("#t-refresh").onclick = () => {
    load();
    if (selected) showDetail(selected);
  };
  const openOverview = (t) => {
    overviewTab = t;
    overviewForced = true;
    if (selected) navigate("#/templates");
    else showDetail(null);
  };
  $("#t-matrix").onclick = () => openOverview("matrix");
  $("#t-new").onclick = () => openOverview("register");

  async function load() {
    try {
      const data = await api("/v1/templates");
      if (!alive) return;
      all = data.templates || [];
      syncOrigins = (data.sync && data.sync.origins) || [];
      unavailable = "";
    } catch (e) {
      if (!alive) return;
      all = [];
      unavailable = templatesUnavailable(e) ? TEMPLATES_MISSING : e.message;
      if (!templatesUnavailable(e)) toast(e.message, "error");
    }
    paint();
  }

  function paint() {
    const status = (t) => (t.state || {}).status || "draft";
    const failing = all.filter((t) => ["failing", "unknown"].includes(newestTests(t.state || {}))).length;
    $("#t-stats").innerHTML = statStrip([
      { label: "Templates", value: all.length },
      { label: "Launched", value: all.filter((t) => status(t) === "launched").length, tone: "ok" },
      { label: "Draft", value: all.filter((t) => status(t) === "draft").length, tone: "warn" },
      { label: "Failing", value: failing, tone: failing ? "bad" : "" },
    ]);
    const want = [...(LIST_TABS.find((x) => x.id === tab) || LIST_TABS[0]).statuses, ...(showRetired ? ["deprecated"] : [])];
    const rows = all.filter(
      (t) =>
        want.includes(status(t)) &&
        (!search || (t.id || "").toLowerCase().includes(search) || (t.description || "").toLowerCase().includes(search)),
    );
    const key = sortBy === "updated" ? (t) => -(Date.parse(t.created_at) || 0) : (t) => (t.id || "").toLowerCase();
    rows.sort((a, b) => (key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0));
    const now = Date.now();
    const empty = unavailable
      ? `<div class="sp-empty"><b>Templates are not available.</b><span>${escapeHtml(unavailable)}</span></div>`
      : all.length
        ? `<div class="sp-empty"><b>No templates match.</b><span>${search ? `Nothing matches “${escapeHtml(search)}”.` : "Nothing in this tab."} Try the All tab or clear the search.</span></div>`
        : `<div class="sp-empty"><b>No templates registered yet.</b><span>Register a <b>source template</b> (a system and its streams), a <b>sink template</b> (a destination), or a complete pipeline to give operators something versioned to trigger.</span></div>`;
    list.render(
      SECTIONS.map(([kind, label]) => ({
        id: kind,
        label,
        rows: rows.filter((t) => kindOf(t) === kind).map((t) => templateRow(t, now)),
      })),
      { empty },
    );
  }

  function templateRow(t, now) {
    const s = rowState(t);
    const st = t.state || {};
    return listRow({
      id: t.id,
      href: `#/templates/${encodeURIComponent(t.id)}`,
      tone: s.tone,
      shape: s.shape,
      title: t.id,
      tooltip: `${t.id}${t.description ? `\n${t.description}` : ""}\nregistered ${fmtAge(t.created_at, now)} ago`,
      age: `v${st.newest ?? t.version}`,
      ageTitle: "newest registered version",
      metaLead: s.lead,
      meta: rowMeta(t),
      selected: t.id === selected,
    });
  }

  function showOverview() {
    pane.setSelected(overviewForced);
    const host = pane.detail;
    const tabsList = [
      { id: "matrix", label: "Compatibility" },
      { id: "register", label: "Register", perm: "template_admin" },
      { id: "sync", label: "Sync", perm: "template_admin", hidden: !syncOrigins.length },
    ];
    if (overviewTab === "sync" && !syncOrigins.length) overviewTab = "matrix";
    host.innerHTML = `
      <div class="c-dhead">
        <a class="sp-back" href="#/templates" id="t-ov-back">${icon("back")}Templates</a>
        ${tabs(tabsList, overviewTab)}
      </div>
      <div><h1 class="c-title">Template registry</h1>
        <div class="c-sub"><span>${fmtInt(all.length)} template${all.length === 1 ? "" : "s"}</span><span class="c-sub-sep">·</span><span>pick one on the left for its versions, tests and trigger form</span></div></div>
      <div class="d-pane" data-pane="matrix"><div id="t-ov-matrix"><div class="sp-loading">loading…</div></div></div>
      <div class="d-pane" data-pane="register" hidden><section class="c-card"><div class="c-card-h">${icon("plus")}<b>Register a template</b><span class="c-card-aside">registering never moves stable</span></div><div class="c-card-pad" id="t-ov-register"></div></section></div>
      <div class="d-pane" data-pane="sync" hidden><section class="c-card"><div class="c-card-h">${icon("sync")}<b>Sync from origins</b><span class="c-card-aside">pull templates from the configured remote origins</span></div><div class="c-card-pad" id="t-ov-sync"></div></section></div>`;
    host.querySelector("#t-ov-back").onclick = (e) => {
      e.preventDefault();
      overviewForced = false;
      pane.setSelected(false);
    };
    const show = (t) => {
      overviewTab = t;
      host.querySelectorAll(".c-dhead .c-tab").forEach((b) => {
        b.classList.toggle("on", b.dataset.tab === t);
        b.setAttribute("aria-selected", String(b.dataset.tab === t));
      });
      host.querySelectorAll(".d-pane").forEach((p) => (p.hidden = p.dataset.pane !== t));
    };
    host.querySelectorAll(".c-dhead .c-tab").forEach((b) => (b.onclick = () => show(b.dataset.tab)));
    show(overviewTab);
    host.querySelector("#t-ov-register").appendChild(registerPanel(() => load()));
    if (syncOrigins.length) host.querySelector("#t-ov-sync").appendChild(syncControls(syncOrigins, load));
    loadMatrix(host.querySelector("#t-ov-matrix"));
  }

  async function loadMatrix(el) {
    const hasSources = all.some((t) => kindOf(t) === "source-template");
    const hasSinks = all.some((t) => kindOf(t) === "sink-template");
    if (!hasSources || !hasSinks) {
      el.innerHTML = `<div class="sp-none"><b>No pairings to show</b><span>The compatibility matrix appears once the registry holds at least one source template and one sink template.</span></div>`;
      return;
    }
    try {
      const idx = await api("/v1/templates/matrix");
      if (!el.isConnected) return;
      el.innerHTML = "";
      el.appendChild(renderMatrix(idx));
    } catch (e) {
      el.innerHTML = `<div class="sp-none"><b>Could not compose the matrix</b><span>${escapeHtml(e.message)}</span></div>`;
    }
  }

  function showDetail(id) {
    detailStop?.();
    detailStop = null;
    selected = id;
    list.select(id);
    if (!id) {
      showOverview();
      return;
    }
    overviewForced = false;
    pane.setSelected(true);
    detailStop = mountTemplateDetail(pane.detail, id, {
      query,
      onChanged: () => load(),
      onDeleted: () => {
        navigate("#/templates");
        load();
      },
    });
  }

  await load();
  if (!alive) return;
  showDetail(selected);

  return {
    teardown() {
      alive = false;
      detailStop?.();
    },
    update(p) {
      const id = p.id || null;
      const q = p.query || new URLSearchParams();
      if (id === selected && q.toString() === query.toString()) return;
      query = q;
      showDetail(id);
    },
  };
}

/** Source × sink compatibility grid (RFC 0008 / #677). A ✓ cell opens the
 *  source template's page with that sink preselected; a partial cell shows
 *  how many streams have a viable write mode; the tooltip lists the plan. */
function renderMatrix(idx) {
  const sources = idx.sources || [];
  const sinks = idx.sinks || [];
  const cells = new Map((idx.matrix || []).map((c) => [`${c.source}\u0000${c.sink}`, c]));
  const idOf = (t) => t.id || t.name;
  const el = document.createElement("section");
  el.className = "tpl-matrix";

  // Owner / Type filters per axis: rows (sources) and columns (sinks) filter
  // independently, since an acme source into a faucet-hq sink is a real pairing.
  // An empty selection means "all".
  const filters = {
    srcOwner: new Set(), srcType: new Set(),
    sinkOwner: new Set(), sinkType: new Set(),
  };
  const values = (list, key) =>
    [...new Set(list.map((t) => (key === "owner" ? t.owner || "(none)" : t[key] || "")).filter(Boolean))].sort();
  const pass = (t, owners, types, typeKey) =>
    (!owners.size || owners.has(t.owner || "(none)")) && (!types.size || types.has(t[typeKey] || ""));

  const head = (k) => `<th title="${escapeHtml(idOf(k))}${k.description ? ` — ${escapeHtml(k.description)}` : ""}"><a href="#/templates/${encodeURIComponent(idOf(k))}" class="mono">${escapeHtml(k.name)}</a><span class="tpl-matrix-kind">${k.owner ? `@${escapeHtml(k.owner)} · ` : ""}${escapeHtml(k.sink_type || "")}</span></th>`;
  const cell = (s, k) => {
    const c = cells.get(`${idOf(s)}\u0000${idOf(k)}`);
    if (!c) return `<td class="tpl-cell tpl-cell-none">—</td>`;
    const total = (s.streams || []).length;
    const plan = (c.streams || [])
      .map((p) => `${p.stream}: ${p.write_mode}${p.satisfies ? ` (for ${p.satisfies})` : ""}`)
      .concat((c.incompatible || []).map((i) => `${i.stream}: ✗ ${i.reason}`))
      .join("\n");
    const href = `#/templates/${encodeURIComponent(idOf(s))}?sink=${encodeURIComponent(idOf(k))}`;
    if (c.compatible) {
      return `<td class="tpl-cell tpl-cell-ok" title="${escapeHtml(plan)}"><a href="${href}" aria-label="run ${escapeHtml(idOf(s))} into ${escapeHtml(idOf(k))}">✓</a></td>`;
    }
    // A partial pairing is shown but not linked: composition refuses a pairing
    // with any stream the sink cannot write, so the run form could only fail.
    // The tooltip names the streams that do and do not fit.
    const ok = (c.streams || []).length;
    const why = `${plan}\n\nNot runnable: every stream needs a write mode this sink supports.`;
    return `<td class="tpl-cell ${ok ? "tpl-cell-partial" : "tpl-cell-bad"}" title="${escapeHtml(why)}">${ok ? `<span>${ok}/${total}</span>` : "✗"}</td>`;
  };
  const row = (s, ks) => {
    const n = (s.streams || []).length;
    return `<tr><th scope="row" title="${escapeHtml(idOf(s))}"><a href="#/templates/${encodeURIComponent(idOf(s))}" class="mono">${escapeHtml(s.name)}</a><span class="tpl-matrix-kind">${s.owner ? `@${escapeHtml(s.owner)} · ` : ""}${escapeHtml(s.source_type || "")} · ${n} stream${n === 1 ? "" : "s"}</span></th>${ks.map((k) => cell(s, k)).join("")}</tr>`;
  };

  el.innerHTML = `
    <div class="tpl-matrix-head">
      <h2 class="tpl-h2">Compatibility</h2>
      <span class="run-meta">source × sink — ✓ every stream has a write mode the sink supports; click a cell to run that pairing</span>
    </div>
    <div class="tpl-facets" role="group" aria-label="Filter the compatibility grid">
      <span class="tpl-facet-group"><span class="tpl-filter-label">sources</span>
        <span data-facet="srcOwner"></span><span data-facet="srcType"></span></span>
      <span class="tpl-facets-sep" aria-hidden="true"></span>
      <span class="tpl-facet-group"><span class="tpl-filter-label">sinks</span>
        <span data-facet="sinkOwner"></span><span data-facet="sinkType"></span></span>
      <span class="run-meta tpl-facets-count"></span>
      <button type="button" class="linkish tpl-facets-clear" hidden>Clear filters</button>
    </div>
    <div class="tpl-matrix-grid"></div>`;

  const grid = el.querySelector(".tpl-matrix-grid");
  const count = el.querySelector(".tpl-facets-count");
  const clear = el.querySelector(".tpl-facets-clear");
  const draw = () => {
    const rs = sources.filter((t) => pass(t, filters.srcOwner, filters.srcType, "source_type"));
    const ks = sinks.filter((t) => pass(t, filters.sinkOwner, filters.sinkType, "sink_type"));
    const active = Object.values(filters).some((f) => f.size);
    clear.hidden = !active;
    count.textContent = active
      ? `${rs.length} of ${sources.length} sources × ${ks.length} of ${sinks.length} sinks`
      : `${sources.length} sources × ${sinks.length} sinks`;
    grid.innerHTML = rs.length && ks.length
      ? `<div class="tpl-matrix-scroll">
          <table class="tbl tpl-matrix-table">
            <thead><tr><th class="tpl-matrix-corner">source \\ sink</th>${ks.map(head).join("")}</tr></thead>
            <tbody>${rs.map((s) => row(s, ks)).join("")}</tbody>
          </table>
        </div>`
      : `<div class="empty">No ${rs.length ? "sink" : "source"} templates match these filters.</div>`;
  };

  const facets = [
    ["srcOwner", "Owner", values(sources, "owner"), (v) => v],
    ["srcType", "Type", values(sources, "source_type"), (v) => v],
    ["sinkOwner", "Owner", values(sinks, "owner"), (v) => v],
    ["sinkType", "Type", values(sinks, "sink_type"), (v) => v],
  ];
  const menus = [];
  for (const [key, label, opts, show] of facets) {
    const host = el.querySelector(`[data-facet="${key}"]`);
    host.className = "tpl-facet";
    host.innerHTML = `
      <button type="button" class="tpl-facet-btn" aria-haspopup="true" aria-expanded="false">
        <span>${label}</span><span class="tpl-facet-n" hidden></span><span class="tpl-facet-caret" aria-hidden="true"></span>
      </button>
      <div class="tpl-facet-menu" hidden>
        ${opts.map((v) => `<label class="tpl-facet-opt"><input type="checkbox" value="${escapeHtml(v)}" /> <span class="mono">${escapeHtml(show(v))}</span></label>`).join("")}
      </div>`;
    const btn = host.querySelector(".tpl-facet-btn");
    const menu = host.querySelector(".tpl-facet-menu");
    const n = host.querySelector(".tpl-facet-n");
    menus.push([btn, menu]);
    const sync = () => {
      n.hidden = !filters[key].size;
      n.textContent = filters[key].size;
      btn.classList.toggle("is-on", filters[key].size > 0);
      for (const box of menu.querySelectorAll("input")) box.checked = filters[key].has(box.value);
    };
    btn.onclick = (ev) => {
      ev.stopPropagation();
      const open = menu.hidden;
      for (const [b, m] of menus) { m.hidden = true; b.setAttribute("aria-expanded", "false"); }
      menu.hidden = !open;
      btn.setAttribute("aria-expanded", String(open));
    };
    menu.onclick = (ev) => ev.stopPropagation();
    menu.onchange = (ev) => {
      const v = ev.target.value;
      if (ev.target.checked) filters[key].add(v);
      else filters[key].delete(v);
      sync();
      draw();
    };
    host.sync = sync;
  }
  clear.onclick = () => {
    for (const f of Object.values(filters)) f.clear();
    for (const h of el.querySelectorAll(".tpl-facet")) h.sync();
    draw();
  };
  // Close any open menu on an outside click or Escape; the listener retires
  // itself once this section has been replaced by a reload.
  const close = (ev) => {
    if (!el.isConnected) {
      document.removeEventListener("click", close);
      document.removeEventListener("keydown", close);
      return;
    }
    if (ev.type === "keydown" && ev.key !== "Escape") return;
    for (const [b, m] of menus) { m.hidden = true; b.setAttribute("aria-expanded", "false"); }
  };
  document.addEventListener("click", close);
  document.addEventListener("keydown", close);
  draw();
  return el;
}

/** The sync panel (RFC 0006): the server's remote origins, a dry-run / pull
 *  button pair, and the resulting per-origin plan. Only shown when the server
 *  was started with `--templates-sync`. */
function syncControls(origins, reload) {
  const el = document.createElement("div");
  el.className = "tpl-sync";
  const rows = origins
    .map(
      (o) => `
      <div class="tpl-sync-origin">
        <b class="mono">${escapeHtml(o.name)}</b>
        <span class="run-meta">${escapeHtml(o.kind)}</span>
        <span class="run-meta" title="id namespace this origin owns">prefix <code>${escapeHtml(o.prefix || "(none)")}</code></span>
        <span class="run-meta" title="whether a pull may move stable">launch: ${escapeHtml(o.launch)}</span>
        <span class="run-meta" title="what happens to a template that vanished upstream">prune: ${escapeHtml(o.prune)}</span>
        ${o.interval_secs ? `<span class="run-meta">every ${o.interval_secs}s</span>` : ""}
      </div>`
    )
    .join("");
  el.innerHTML = `
    <div class="tpl-sync-head">
      <h2>Remote origins</h2>
      <label class="tpl-sync-pick">origin
        <select id="ts-origin">
          <option value="">all</option>
          ${origins.map((o) => `<option value="${escapeHtml(o.name)}">${escapeHtml(o.name)}</option>`).join("")}
        </select>
      </label>
      <button class="btn-ghost" id="ts-dry">Dry run</button>
      <button class="btn-primary" id="ts-run">Pull now</button>
    </div>
    <div class="tpl-sync-origins">${rows}</div>
    <div id="ts-out" class="tpl-sync-out" hidden></div>`;
  const out = el.querySelector("#ts-out");
  async function go(dryRun) {
    const origin = el.querySelector("#ts-origin").value || undefined;
    el.querySelectorAll("button").forEach((b) => (b.disabled = true));
    try {
      const res = await api("/v1/templates/sync", { method: "POST", body: { origin, dry_run: dryRun } });
      out.innerHTML = renderSyncResult(res);
      out.hidden = false;
      const changed = res.reports.reduce((n, r) => n + (r.outcome ? countMutations(r.outcome) : r.plan.filter(isMutation).length), 0);
      toast(dryRun ? `Dry run: ${changed} change(s) planned` : `Synced: ${changed} change(s)`, res.origin_errors.length ? "error" : "info");
      if (!dryRun) await reload();
    } catch (e) {
      toast(e.message, "error");
    } finally {
      el.querySelectorAll("button").forEach((b) => (b.disabled = false));
    }
  }
  el.querySelector("#ts-dry").onclick = () => go(true);
  el.querySelector("#ts-run").onclick = () => go(false);
  return el;
}

const isMutation = (a) => ["register", "launch", "revive", "deprecate"].includes(a.action);
const countMutations = (o) => o.registered.length + o.launched.length + o.revived.length + o.deprecated.length;

function renderSyncResult(res) {
  const verb = res.dry_run ? "would" : "did";
  const blocks = res.reports.map((r) => {
    const lines = r.plan
      .map((a) => {
        const cls = isMutation(a) ? "tpl-sync-mut" : "";
        switch (a.action) {
          case "register":
            return `<li class="${cls}">register <b class="mono">${escapeHtml(a.id)}</b>${a.replaces != null ? ` (after v${a.replaces})` : ""}${a.launch ? " → launch" : ""}</li>`;
          case "launch":
            return `<li class="${cls}">launch <b class="mono">${escapeHtml(a.id)}</b> v${a.version}</li>`;
          case "revive":
            return `<li class="${cls}">revive <b class="mono">${escapeHtml(a.id)}</b></li>`;
          case "deprecate":
            return `<li class="${cls}">deprecate <b class="mono">${escapeHtml(a.id)}</b> (gone upstream)</li>`;
          case "orphaned":
            return `<li>orphaned <b class="mono">${escapeHtml(a.id)}</b> (gone upstream; kept)</li>`;
          case "unchanged":
            return `<li class="tpl-sync-quiet">unchanged <b class="mono">${escapeHtml(a.id)}</b> (v${a.version})</li>`;
          case "skipped":
            return `<li class="tpl-sync-warn">skipped <b class="mono">${escapeHtml(a.name)}</b>: ${escapeHtml(a.reason)}</li>`;
          default:
            return `<li>${escapeHtml(a.action)}</li>`;
        }
      })
      .join("");
    const warnings = (r.warnings || []).map((w) => `<li class="tpl-sync-warn">${escapeHtml(w)}</li>`).join("");
    const failed = ((r.outcome && r.outcome.failed) || [])
      .map((f) => `<li class="tpl-sync-fail">failed <b class="mono">${escapeHtml(f.id)}</b>: ${escapeHtml(f.error)}</li>`)
      .join("");
    return `<div class="tpl-sync-report">
      <div class="tpl-sync-report-head"><b class="mono">${escapeHtml(r.origin)}</b> <span class="run-meta">${escapeHtml(r.kind)} · ${verb} change ${
        r.outcome ? countMutations(r.outcome) : r.plan.filter(isMutation).length
      }</span></div>
      <ul>${lines}${warnings}${failed}${!lines && !warnings ? "<li class=\"tpl-sync-quiet\">nothing at this origin</li>" : ""}</ul>
    </div>`;
  });
  const errs = (res.origin_errors || [])
    .map((e) => `<div class="tpl-sync-report"><div class="tpl-sync-report-head"><b class="mono">${escapeHtml(e.origin)}</b></div><ul><li class="tpl-sync-fail">${escapeHtml(e.error)}</li></ul></div>`)
    .join("");
  return blocks.join("") + errs;
}

/** The register panel: a raw config editor plus id / description / launch.
 *  `opts` lets the same panel add a *new version* to an existing template from
 *  the detail page — the id is fixed, the editor is seeded with the current
 *  config, and staying on the detail page just reloads it rather than navigating. */
function registerPanel(onDone, opts = {}) {
  const { presetId = "", presetBody = "", presetDesc = "", lockId = false, newVersion = false } = opts;
  const el = document.createElement("div");
  el.className = "tpl-register";
  el.innerHTML = `
    ${newVersion ? `<p class="tpl-desc">Appends a new version to <b class="mono">${escapeHtml(presetId)}</b> — registering does <b>not</b> change what <code>stable</code> resolves to. Tick “launch” to make the new version live immediately.</p>` : ""}
    <textarea id="tr-cfg" class="code" spellcheck="false" placeholder="kind: source-template        # or sink-template, deployment, or pipeline
name: acme-billing
params:
  api_token: { type: string, required: true, secret: true }
source:
  type: rest
  config: { base_url: https://api.example.com/v1, auth: { type: bearer, config: { token: '\${param.api_token}' } } }
streams:
  - { name: invoices, source: { config: { path: /invoices } }, primary_keys: [id], write: [overwrite, upsert] }">${escapeHtml(presetBody)}</textarea>
    <p class="tpl-desc">A <code>kind:</code> line says what the document is: <b>source-template</b> (a system and its streams — run it with any registered sink template), <b>sink-template</b> (a destination), <b>deployment</b> (state, DLQ, notifications and SLA applied over a source × sink run), or <b>pipeline</b> (a complete config). A document without <code>kind:</code> is registered as a pipeline with a deprecation notice.</p>
    <fieldset class="submit-opts">
      <label>id <input id="tr-id" value="${escapeHtml(presetId)}" ${lockId ? "readonly" : ""} placeholder="derived from name:" /></label>
      <label>format
        <select id="tr-format"><option value="yaml">yaml</option><option value="json">json</option></select>
      </label>
      <label>description <input id="tr-desc" value="${escapeHtml(presetDesc)}" /></label>
      <label><input id="tr-launch" type="checkbox" /> launch it (make live now)</label>
    </fieldset>
    <div class="submit-actions"><button id="tr-go" class="btn-primary">${newVersion ? "Register new version" : "Register"}</button></div>
    <pre id="tr-out" class="submit-out" hidden></pre>`;

  const out = el.querySelector("#tr-out");
  el.querySelector("#tr-go").onclick = async () => {
    const body = {
      config: el.querySelector("#tr-cfg").value,
      config_format: el.querySelector("#tr-format").value,
    };
    const id = el.querySelector("#tr-id").value.trim();
    const desc = el.querySelector("#tr-desc").value.trim();
    if (id) body.id = id;
    if (desc) body.description = desc;
    if (el.querySelector("#tr-launch").checked) body.launch = true;
    try {
      const resp = await api("/v1/templates", { method: "POST", body });
      out.hidden = true;
      const inherited = resp.tests_inherited_from != null ? ` (tests inherited from v${resp.tests_inherited_from})` : "";
      if (resp.tests && !resp.tests.passed) toast(`${resp.id} v${resp.version} registered${inherited} — its tests fail, so it can take no label`, "error");
      else toast(`${resp.id} v${resp.version} registered${inherited}${resp.tests ? " — tests passing" : ""}`);
      // Adding a version from the detail page stays put and reloads; the
      // list-page flow navigates into the freshly-registered template.
      if (!newVersion) navigate(`#/templates/${encodeURIComponent(resp.id)}`);
      onDone();
    } catch (e) {
      out.hidden = false;
      out.textContent = `✗ ${e.message}\n\n` + (e.details ? JSON.stringify(e.details, null, 2) : "");
    }
  };
  return el;
}

// ── detail / versions page ──────────────────────────────────────────────────

/** Version options for the template's own trigger form: newest first, each
 *  labelled with the channels that point at it; the live version selected. */
function ownVersionOptions(st) {
  const pick = st.stable ?? st.newest;
  return [...st.versions]
    .sort((a, b) => b - a)
    .map((v) => {
      const ch = channelsFor(v, st);
      const retired = (st.deprecated_versions || []).some((x) => x.version === v);
      const note = [...ch, retired ? "deprecated" : ""].filter(Boolean).join(", ");
      return `<option value="${v}"${v === pick ? " selected" : ""}>v${v}${note ? ` · ${escapeHtml(note)}` : ""}</option>`;
    })
    .join("");
}

const TEST_TONE = { passing: "ok", failing: "bad", untested: "warn", no_tests: "", unknown: "bad" };
const TEST_WORD = { passing: "passed", failing: "failing", untested: "not run", no_tests: "no tests", unknown: "unreadable" };

function testCell(t) {
  if (!t) return `<span class="c-muted">—</span>`;
  const tone = TEST_TONE[t.status] || "";
  return `<span class="c-nowrap"><span class="c-dot ${tone}"></span>${escapeHtml(TEST_WORD[t.status] || t.status)}</span>`;
}

/** Mount one template's page into `el`; returns a stop function. */
export function mountTemplateDetail(el, id, hooks = {}) {
  let alive = true;
  let gen = 0;
  let detailTab = "versions";
  const preselectSink = hooks.query && hooks.query.get("sink");
  if (preselectSink) detailTab = "run";

  async function load() {
    const mine = ++gen;
    if (!el.querySelector(".c-dhead")) el.innerHTML = `<div class="sp-loading">loading…</div>`;
    let d;
    try {
      // `newest` (not the default `stable`) so a draft template — which has no
      // stable version at all — still opens.
      d = await api(`/v1/templates/${encodeURIComponent(id)}?version=newest`);
    } catch (e) {
      if (!alive || mine !== gen) return;
      const msg = templatesUnavailable(e) ? TEMPLATES_MISSING : e.message;
      el.innerHTML = `<div class="c-dhead"><a class="sp-back" href="#/templates">${icon("back")}Templates</a></div><div class="sp-none"><b>${e.status === 404 && !templatesUnavailable(e) ? "Template not found" : "Could not load this template"}</b><span>${escapeHtml(msg)}</span></div>`;
      return;
    }
    if (!alive || mine !== gen) return;
    render(d);
  }

  const reload = () => {
    load();
    hooks.onChanged?.();
  };

  function render(d) {
    const kind = kindOf(d);
    const st = {
      status: d.status,
      versions: d.versions,
      stable: d.stable,
      previous: d.previous,
      newest: d.newest,
      tags: d.tags || {},
      deprecation: d.deprecation,
      deprecated_versions: d.deprecated_versions || [],
    };
    setCrumbTitle(d.id);
    const runnable = !["sink-template", "deployment"].includes(kind);
    const runLabel = { "sink-template": "Pairings", deployment: "Use" }[kind] || "Run";
    const cand = st.newest;
    const candTests = cand != null ? testsFor(d, cand) : null;
    const candRetired = (st.deprecated_versions || []).some((x) => x.version === cand);
    const canLaunch = cand != null && cand !== st.stable && !candRetired && st.status !== "deprecated" && canTakeLabel(candTests);
    const statusTone = { launched: "ok", draft: "warn", deprecated: "" }[st.status] || "";
    const nParams = Object.keys(d.params || {}).length;
    const sub = [
      pill(st.status === "launched" ? `Launched · v${st.stable}` : st.status === "draft" ? "Draft · never launched" : "Deprecated", statusTone, st.status === "launched" ? "check" : ""),
      `<span title="${escapeHtml(KIND_TITLE[kind])}">${escapeHtml(KIND_LABEL[kind])}${kind === "pipeline" ? "" : " template"}</span>`,
      `<span>${fmtInt(st.versions.length)} version${st.versions.length === 1 ? "" : "s"} · ${fmtInt(nParams)} param${nParams === 1 ? "" : "s"}</span>`,
      d.created_by ? `<span>v${d.version} registered by <b>${escapeHtml(d.created_by)}</b></span>` : `<span title="${escapeHtml(fmtTime(d.created_at))}">v${d.version} registered ${escapeHtml(fmtAge(d.created_at))} ago</span>`,
      d.name && d.name !== d.id ? `<span>config name <b>${escapeHtml(d.name)}</b></span>` : "",
    ].filter(Boolean);
    const showHealth = kind === "pipeline" && st.stable != null;

    el.innerHTML = `
      <div class="c-dhead">
        <a class="sp-back" href="#/templates">${icon("back")}Templates</a>
        ${tabs(
          [
            { id: "versions", label: "Versions", count: st.versions.length },
            { id: "run", label: runLabel, perm: runnable ? "run_write" : "" },
            { id: "health", label: "Health", hidden: !showHealth, perm: "status_read" },
          ],
          detailTab,
        )}
        <span class="c-dhead-r">
          <button class="c-btn" id="t-newver" data-perm="template_admin" title="register a new version of this template">${icon("plus")}New version</button>
          <button class="c-btn" id="t-rollback" data-perm="template_admin" ${st.previous == null ? "disabled" : ""}
            title="${st.previous == null ? "no earlier launch to roll back to" : `re-launch v${st.previous}`}">${icon("undo")}Roll back</button>
          <button class="c-btn${st.status === "deprecated" ? "" : " danger"}" id="t-deprecate" data-perm="template_admin">${st.status === "deprecated" ? "Revive" : "Deprecate"}</button>
          ${canLaunch ? `<button class="c-btn primary" id="t-launch" data-perm="template_admin" title="make v${cand} live for unpinned runs">${icon("play")}Launch v${cand}</button>` : ""}
        </span>
      </div>
      <div><h1 class="c-title" title="${escapeHtml(d.id)}">${escapeHtml(d.id)}</h1><div class="c-sub">${sub.join('<span class="c-sub-sep">·</span>')}</div></div>
      ${d.description ? `<div class="c-quote">${escapeHtml(d.description)}</div>` : ""}
      ${st.status === "draft" ? `<div class="c-alert warn">This template is a <b>draft</b> — nothing has been launched, so a run without an explicit version is refused. Launch a version to make it live.</div>` : ""}
      ${st.status === "deprecated" ? `<div class="c-alert warn"><b>Deprecated</b>${d.deprecation && d.deprecation.reason ? ` — ${escapeHtml(d.deprecation.reason)}` : ""}. Existing callers still resolve <code>stable</code>, but every trigger warns.</div>` : ""}
      <div id="t-newver-host" hidden></div>
      <div class="d-pane" data-pane="versions">
        ${gateCard(d, st, cand, candTests)}
        <section class="c-card">
          <div class="c-card-h"><b>Versions</b><span class="c-card-aside">${runnable ? (can("run_write") ? "Run opens the trigger form with that version" : "") : "a sink or deployment runs through a source template"}</span><span class="c-card-r">runs: last 7 days</span></div>
          <div class="d-scroll-x"><table class="c-tbl tpl-vtbl">
            <colgroup><col style="width:72px" /><col /><col style="width:104px" /><col style="width:112px" /><col style="width:78px" /><col style="width:208px" /></colgroup>
            <thead><tr><th>Version</th><th title="when the version was registered, and by whom">Registered</th><th>Channels</th><th>Tests</th><th>Last tested</th><th class="num" title="runs submitted from this version in the last 7 days">Runs</th><th></th></tr></thead>
            <tbody id="t-vrows"></tbody>
          </table></div>
        </section>
        ${launchCard(d.launches || [])}
      </div>
      <div class="d-pane" data-pane="run" hidden><div id="t-trigger"></div></div>
      <div class="d-pane" data-pane="health" hidden>${showHealth ? `<section class="c-card" data-perm="status_read"><div id="t-health" class="c-card-pad"></div></section>` : ""}</div>`;

    const showTab = (t) => {
      detailTab = t;
      el.querySelectorAll(".c-dhead .c-tab").forEach((b) => {
        b.classList.toggle("on", b.dataset.tab === t);
        b.setAttribute("aria-selected", String(b.dataset.tab === t));
      });
      el.querySelectorAll(".d-pane").forEach((p) => (p.hidden = p.dataset.pane !== t));
    };
    el.querySelectorAll(".c-dhead .c-tab").forEach((b) => (b.onclick = () => showTab(b.dataset.tab)));
    if (detailTab === "health" && !showHealth) detailTab = "versions";
    showTab(detailTab);

    // "New version" — the register panel pinned to this id and seeded with the
    // newest version's config, so a new build is an edit of the current one.
    const newverHost = el.querySelector("#t-newver-host");
    el.querySelector("#t-newver").onclick = async () => {
      if (!newverHost.hidden) {
        newverHost.hidden = true;
        newverHost.innerHTML = "";
        return;
      }
      let seed = "";
      try {
        const rec = await api(`/v1/templates/${encodeURIComponent(id)}?version=${st.newest}`);
        if (rec.body_withheld) {
          toast(`v${st.newest} config is withheld: only an unscoped admin can read stored template configs`, "error");
          return;
        }
        seed = rec.body || "";
      } catch (e) {
        toast(`could not load v${st.newest} config: ${e.message}`, "error");
      }
      newverHost.innerHTML = `<section class="c-card"><div class="c-card-h">${icon("plus")}<b>New version of ${escapeHtml(id)}</b><button type="button" class="c-btn c-card-r" id="t-newver-x">${icon("x")}Close</button></div><div class="c-card-pad"></div></section>`;
      newverHost.querySelector(".c-card-pad").appendChild(
        registerPanel(reload, { presetId: id, presetBody: seed, presetDesc: d.description || "", lockId: true, newVersion: true }),
      );
      newverHost.querySelector("#t-newver-x").onclick = () => el.querySelector("#t-newver").click();
      newverHost.hidden = false;
    };

    el.querySelector("#t-rollback").onclick = async () => {
      if (st.previous == null) return;
      try {
        const r = await api(`/v1/templates/${encodeURIComponent(id)}/rollback`, { method: "POST", body: {} });
        toast(`rolled back to v${r.version}`);
        reload();
      } catch (e) {
        toast(e.message, "error");
      }
    };

    el.querySelector("#t-deprecate").onclick = async () => {
      const undo = st.status === "deprecated";
      const body = { undo };
      if (!undo) {
        const reason = prompt("Why is this template being retired? (optional)");
        if (reason === null) return;
        if (reason.trim()) body.reason = reason.trim();
      }
      try {
        const r = await api(`/v1/templates/${encodeURIComponent(id)}/deprecate`, { method: "POST", body });
        toast(`${id} is now ${r.status}`);
        reload();
      } catch (e) {
        toast(e.message, "error");
      }
    };

    const launch = el.querySelector("#t-launch");
    if (launch) launch.onclick = () => launchVersion(cand);

    let trigger = null;
    const pickToRun = (v) => {
      showTab("run");
      trigger?.pick(v);
    };
    renderVersions(el.querySelector("#t-vrows"), id, st, d, { reload, launchVersion, onRun: runnable ? pickToRun : null, onDeleted: hooks.onDeleted });
    loadRuns7d(el.querySelector("#t-vrows"), id);
    const trig = el.querySelector("#t-trigger");
    if (kind === "sink-template") renderSinkPairings(trig, id);
    else if (kind === "deployment") renderDeploymentUse(trig, id);
    else {
      Promise.resolve(renderTrigger(trig, id, st, d, kind === "source-template", preselectSink)).then((t) => {
        trigger = t || null;
      });
    }
    const healthHost = el.querySelector("#t-health");
    if (healthHost) renderHealth(healthHost, id);
  }

  async function launchVersion(v) {
    try {
      const r = await api(`/v1/templates/${encodeURIComponent(id)}/launch`, { method: "POST", body: { version: v } });
      toast(r.already_launched ? `v${r.version} was already live` : `v${r.version} is live${r.replaced != null ? ` (was v${r.replaced})` : ""}`);
      reload();
    } catch (e) {
      toast(e.message, "error");
    }
  }

  load();
  return () => {
    alive = false;
  };
}

/** The launch-gate card: the candidate version's latest test result and what
 *  launching it would do. */
function gateCard(d, st, cand, t) {
  if (cand == null) return "";
  const r = t && t.results && t.results[0];
  const passed = r ? r.cases.filter((c) => c.passed).length : 0;
  const head = r
    ? `v${cand} ${r.passed ? "passed" : "failed"} ${fmtInt(passed)} of ${fmtInt(r.cases.length)} case${r.cases.length === 1 ? "" : "s"}`
    : t && t.status === "no_tests"
      ? `v${cand} has no test bundle`
      : `v${cand} · no test result yet`;
  const items = [];
  if (r) {
    const byOrigin = new Map();
    for (const c of r.cases) {
      const k = c.origin || c.source || "case";
      const e = byOrigin.get(k) || [0, 0];
      e[0] += c.passed ? 1 : 0;
      e[1] += 1;
      byOrigin.set(k, e);
    }
    for (const [k, [p, n]] of byOrigin) items.push({ k: `${k[0].toUpperCase()}${k.slice(1)} cases`, v: `${fmtInt(p)} / ${fmtInt(n)}` });
    const retried = r.cases.filter((c) => c.attempts > 1).length;
    if (retried) items.push({ k: "Retried", v: retried });
    if ((r.suites || []).length) items.push({ k: "Shared suites", v: r.suites.map((s) => `${s.name}${s.release ? `@${s.release}` : s.version != null ? ` v${s.version}` : ""}`).join(", ") });
    items.push({ k: "Faucet", v: r.faucet_version, s: r.recorded_by ? `by ${r.recorded_by}` : "" });
  }
  let note = "";
  let tone = "";
  const stable = st.stable;
  if (cand === stable) {
    note = `v${cand} is stable · unpinned triggers resolve it`;
    tone = "ok";
  } else if (t && t.status === "passing") {
    note = stable != null
      ? `Launch gate open · v${cand} can become stable. Unpinned triggers keep resolving v${stable} until it does.`
      : `Launch gate open · v${cand} can be launched. Until a version is, unpinned triggers are refused.`;
    tone = "ok";
  } else if (t && t.status === "untested") {
    note = `No result for v${cand} under this faucet version · launching it runs its tests first.`;
    tone = "warn";
  } else {
    note = `Launch blocked · v${cand} ${t && t.status === "no_tests" ? "carries no tests: block, and only a version whose tests pass can be launched or take a channel" : t && t.status === "failing" ? "fails its tests" : "has unreadable tests"}`;
    tone = "bad";
  }
  const failing = t && (t.failing || []).length
    ? `<ul class="tpl-fail-list">${t.failing.map((f) => `<li class="mono">${escapeHtml(f)}</li>`).join("")}</ul>`
    : "";
  return card({
    title: head,
    iconName: r && r.passed ? "check" : r || (t && t.status === "no_tests") ? "x" : "",
    tag: r ? `${r.body_sha256.slice(0, 8)}` : "",
    right: r ? `tested ${fmtAge(r.recorded_at)} ago` : "",
    html: (items.length ? kvGrid(items, 4) : `<div class="c-card-empty">${t && t.reason ? mdInline(t.reason) : `No test result recorded for v${cand}.`}</div>`) + failing,
    footnote: note,
    footTone: tone,
  });
}

function launchCard(launches) {
  if (!launches.length) {
    return `<section class="c-card"><div class="c-card-h"><b>Launch history</b></div><div class="c-card-empty">Never launched.</div></section>`;
  }
  return `<section class="c-card"><div class="c-card-h"><b>Launch history</b><span class="c-card-r">${fmtInt(launches.length)} launch${launches.length === 1 ? "" : "es"}</span></div><div class="c-log c-timeline">${launches
    .map(
      (l) =>
        `<div><span class="c-log-ts" title="${escapeHtml(fmtTime(l.launched_at))}">${escapeHtml(fmtAge(l.launched_at))} ago</span>&nbsp; #${fmtInt(l.seq)} <b>v${l.version}</b> launched by <b>${escapeHtml(l.launched_by || "cli")}</b>${l.tests_skipped ? ` · <span class="c-warn-t">tests skipped: ${escapeHtml(l.tests_skipped)}</span>` : l.evidence ? " · tests passed" : ""}</div>`,
    )
    .join("")}</div></section>`;
}

/** Fill the "Runs 7d" column: runs submitted from each version in a week. */
async function loadRuns7d(tbody, id) {
  const since = new Date(Date.now() - 7 * 86400e3).toISOString();
  const p = new URLSearchParams({ label: `template:${id}`, since, limit: "500" });
  let runs = [];
  try {
    runs = (await api(`/v1/runs?${p}`)).runs || [];
  } catch {
    return;
  }
  const by = new Map();
  for (const r of runs) {
    const v = (r.labels || {}).template_version;
    if (v != null) by.set(String(v), (by.get(String(v)) || 0) + 1);
  }
  tbody.querySelectorAll("tr[data-version] .tpl-runs7").forEach((td) => {
    const n = by.get(td.closest("tr").dataset.version) || 0;
    td.textContent = `${fmtInt(n)}${runs.length >= 500 ? "+" : ""}`;
  });
}

// When and by whom a version was registered; "—" for what the server didn't record.
function registeredCell(reg) {
  if (!reg) return `<td class="c-muted">—</td>`;
  const who = reg.registered_by || "";
  const title = `registered ${fmtTime(reg.registered_at)}${who ? ` by ${who}` : ""}`;
  return `<td class="c-muted c-nowrap" title="${escapeHtml(title)}">${escapeHtml(fmtAge(reg.registered_at))} ago${
    who ? ` · <span class="mono">${escapeHtml(who)}</span>` : ""
  }</td>`;
}

function renderVersions(tbody, id, st, d, { reload, launchVersion, onRun, onDeleted }) {
  if (!st.versions.length) {
    tbody.innerHTML = `<tr><td colspan="7" class="c-muted">No versions stored.</td></tr>`;
    return;
  }
  tbody.innerHTML = "";
  for (const v of [...st.versions].sort((a, b) => b - a)) {
    const retired = (st.deprecated_versions || []).find((x) => x.version === v);
    const t = testsFor(d, v);
    const tb = testsBlock(t);
    const blocked = !canTakeLabel(t);
    const launchTitle = st.stable === v ? "already live" : retired ? `v${v} is deprecated — revive it to launch it` : blocked ? `only a version whose tests pass can be launched (v${v} is ${t ? t.status : "unknown"})` : `make v${v} live for unpinned runs`;
    const chips = channelChips(v, st) + (retired ? chip("deprecated", "", retired.reason ? `deprecated: ${retired.reason}` : "deprecated") : "");
    const when = t && (t.result_at || (t.results && t.results[0] && t.results[0].recorded_at));
    const row = document.createElement("tr");
    row.dataset.version = String(v);
    row.className = "tpl-vrow" + (st.stable === v ? " is-live" : "") + (retired ? " is-retired" : "");
    row.innerHTML = `
      <td class="mono">v${v}</td>
      ${registeredCell((d.registrations || []).find((x) => x.version === v))}
      <td><span class="tpl-chips">${chips || `<span class="c-muted">no channel</span>`}</span></td>
      <td title="${escapeHtml((t && t.reason) || "")}">${testCell(t)}</td>
      <td class="c-muted c-nowrap" title="${when ? escapeHtml(fmtTime(when)) : ""}">${when ? `${escapeHtml(fmtAge(when))} ago` : "—"}</td>
      <td class="num tpl-runs7 c-muted">—</td>
      <td class="tpl-vact">
        ${onRun ? `<button class="c-btn tpl-run" data-perm="run_write" title="open the trigger form with v${v}">${icon("play")}Run</button>` : ""}
        <button class="c-btn tpl-launch" data-perm="template_admin" ${st.stable === v || retired || blocked ? "disabled" : ""} title="${escapeHtml(launchTitle)}">Launch</button>
        <details class="dd tpl-more"${(t && (t.results || []).length) || can("template_admin") ? "" : " hidden"}>
          <summary class="c-btn" title="more actions for v${v}">More</summary>
          <div class="dd-menu dd-menu-r">
            <label class="tpl-assign-l" data-perm="template_admin">channel
              <select class="tpl-assign" ${blocked ? "disabled" : ""} title="${blocked ? `only a version whose tests pass can take a channel` : `point a channel at v${v}`}">
                <option value="">assign…</option>
                ${ASSIGNABLE.map((c) => `<option value="${c}">${c}</option>`).join("")}
              </select>
            </label>
            ${t && (t.results || []).length ? `<button class="dd-item tpl-history">Test history</button>` : ""}
            ${t && t.has_tests ? `<button class="dd-item tpl-runtests" data-perm="template_admin" title="run v${v}'s tests: bundle and record the result">Run tests</button>` : ""}
            <button class="dd-item tpl-view" data-perm="template_admin">Show config</button>
            <button class="dd-item tpl-view-clean" data-perm="template_admin" title="comments stripped, canonical YAML">Show clean config</button>
            <button class="dd-item tpl-vdeprecate" data-perm="template_admin" title="${retired ? `make v${v} usable again` : `retire v${v}: pinned runs still work but warn, newest skips it, launch refuses it`}">${retired ? "Revive" : "Deprecate"}</button>
            <button class="dd-item danger tpl-del" data-perm="template_admin">Delete v${v}</button>
          </div>
        </details>
      </td>`;
    const extra = document.createElement("tr");
    extra.className = "tpl-vextra";
    extra.innerHTML = `<td colspan="7">${tb.body}<div class="tpl-body-wrap" hidden><pre class="tpl-body"></pre></div></td>`;
    const history = extra.querySelector(".tpl-tests-runs");
    const hasWhy = !!extra.querySelector(".tpl-tests-why");
    const syncExtra = () => {
      extra.hidden = !hasWhy && (!history || history.hidden) && extra.querySelector(".tpl-body-wrap").hidden;
    };
    if (history) history.hidden = true;
    syncExtra();
    const histBtn = row.querySelector(".tpl-history");
    if (histBtn) {
      histBtn.onclick = () => {
        more.open = false;
        history.hidden = !history.hidden;
        history.open = !history.hidden;
        histBtn.textContent = history.hidden ? "Test history" : "Hide test history";
        syncExtra();
      };
    }

    const more = row.querySelector(".tpl-more");
    const closeMore = (e) => {
      if (!more.isConnected) {
        document.removeEventListener("click", closeMore);
        return;
      }
      if (more.open && !more.contains(e.target)) more.open = false;
    };
    document.addEventListener("click", closeMore);
    // The table scrolls sideways, which would clip the menu: pin it to the viewport.
    more.addEventListener("toggle", () => {
      const menu = more.querySelector(".dd-menu");
      if (!more.open) return;
      const r = more.querySelector("summary").getBoundingClientRect();
      menu.style.position = "fixed";
      menu.style.right = `${Math.max(8, window.innerWidth - r.right)}px`;
      const below = window.innerHeight - r.bottom;
      const h = menu.offsetHeight;
      if (below < h + 12 && r.top > h + 12) {
        menu.style.top = "auto";
        menu.style.bottom = `${window.innerHeight - r.top + 4}px`;
      } else {
        menu.style.bottom = "auto";
        menu.style.top = `${r.bottom + 4}px`;
      }
      const scroller = more.closest(".sp-detail");
      const shut = () => {
        more.open = false;
        scroller?.removeEventListener("scroll", shut);
      };
      scroller?.addEventListener("scroll", shut, { once: true });
    });

    const runBtn = row.querySelector(".tpl-run");
    if (runBtn) runBtn.onclick = () => onRun(v);

    const runTests = row.querySelector(".tpl-runtests");
    if (runTests) {
      runTests.onclick = async () => {
        runTests.disabled = true;
        runTests.textContent = "Running…";
        try {
          const r = await api(`/v1/templates/${encodeURIComponent(id)}/versions/${v}/test`, { method: "POST", body: {} });
          const failed = r.cases.filter((c) => !c.passed).length;
          if (r.passed) toast(`v${v}: ${r.cases.length} case(s) passed`);
          else toast(`v${v}: ${failed} of ${r.cases.length} case(s) failed`, "error");
          reload();
        } catch (e) {
          toast(e.message, "error");
          runTests.disabled = false;
          runTests.textContent = "Run tests";
        }
      };
    }

    row.querySelector(".tpl-assign").onchange = async (ev) => {
      const tag = ev.target.value;
      if (!tag) return;
      try {
        await api(`/v1/templates/${encodeURIComponent(id)}/tags`, { method: "POST", body: { tag, version: v } });
        toast(`${tag} → v${v}`);
        reload();
      } catch (e) {
        toast(e.message, "error");
        ev.target.value = "";
      }
    };

    row.querySelector(".tpl-vdeprecate").onclick = async () => {
      more.open = false;
      const body = { undo: !!retired };
      if (!retired) {
        const reason = prompt(`Why is v${v} being retired? (optional)`);
        if (reason === null) return;
        if (reason.trim()) body.reason = reason.trim();
      }
      try {
        await api(`/v1/templates/${encodeURIComponent(id)}/versions/${v}/deprecate`, { method: "POST", body });
        toast(retired ? `v${v} is live again` : `v${v} is deprecated`);
        reload();
      } catch (e) {
        toast(e.message, "error");
      }
    };

    row.querySelector(".tpl-launch").onclick = () => launchVersion(v);

    const body = extra.querySelector(".tpl-body");
    const wrap = extra.querySelector(".tpl-body-wrap");
    const copy = copyButton(() => body.textContent, "Copy config");
    copy.addEventListener("copy-failed", (ev) => toast(ev.detail.message, "error"));
    wrap.appendChild(copy);
    // Config = raw stored body; Clean = comments stripped, canonical YAML (the
    // server renders it via ?clean=true, sharing the CLI's clean_config_yaml).
    const showConfig = async (clean) => {
      more.open = false;
      const mode = clean ? "clean" : "raw";
      if (!wrap.hidden && body.dataset.mode === mode) {
        wrap.hidden = true;
        syncExtra();
        return;
      }
      try {
        const q = clean ? `?version=${v}&clean=true` : `?version=${v}`;
        const rec = await api(`/v1/templates/${encodeURIComponent(id)}${q}`);
        body.textContent = rec.body_withheld
          ? "Config withheld: only an unscoped admin can read a template's stored config."
          : rec.body;
        body.classList.toggle("tpl-body-withheld", !!rec.body_withheld);
        copy.hidden = !!rec.body_withheld;
        copy.title = clean ? "Copy clean config" : "Copy config";
        copy.setAttribute("aria-label", copy.title);
        body.dataset.mode = mode;
        wrap.hidden = false;
        syncExtra();
      } catch (e) {
        toast(e.message, "error");
      }
    };
    row.querySelector(".tpl-view").onclick = () => showConfig(false);
    row.querySelector(".tpl-view-clean").onclick = () => showConfig(true);

    row.querySelector(".tpl-del").onclick = async () => {
      more.open = false;
      if (!confirm(`Delete ${id} v${v}? Channels pointing at it are dropped too.`)) return;
      try {
        await api(`/v1/templates/${encodeURIComponent(id)}?version=${v}`, { method: "DELETE" });
        toast(`v${v} deleted`);
        if (st.versions.length === 1) onDeleted?.();
        else reload();
      } catch (e) {
        toast(e.message, "error");
      }
    };

    tbody.appendChild(row);
    tbody.appendChild(extra);
  }
}

/** A source template the selected sink or deployment can be used with, as a
 *  list row that opens it. */
function sourceRows(sources, sinkId) {
  const now = Date.now();
  return sources
    .map((s) => {
      const r = rowState(s);
      return listRow({
        id: s.id,
        href: `#/templates/${encodeURIComponent(s.id)}${sinkId ? `?sink=${encodeURIComponent(sinkId)}` : ""}`,
        tone: r.tone,
        shape: r.shape,
        title: s.id,
        tooltip: s.description || s.id,
        age: fmtAge(s.created_at, now),
        metaLead: r.lead,
        meta: s.description || rowMeta(s),
      });
    })
    .join("");
}

/** A deployment overlay is never run on its own: it is picked in a source
 *  template's trigger form, alongside the sink. */
async function renderDeploymentUse(host, id) {
  host.innerHTML = `<div class="sp-loading">loading…</div>`;
  let sources = [];
  try {
    const data = await api("/v1/templates?kind=source-template");
    sources = data.templates || [];
  } catch (e) {
    host.innerHTML = `<div class="c-alert bad">${escapeHtml(e.message)}</div>`;
    return;
  }
  const how = `A deployment overlay adds the operational blocks — state, DLQ, notifications, SLA — to a <b>source × sink</b> run: open a source template, pick a sink, and choose <b class="mono">${escapeHtml(id)}</b> as its deployment.`;
  host.innerHTML = `<section class="c-card"><div class="c-card-h"><b>Apply to a run</b><span class="c-card-r">${fmtInt(sources.length)} source template${sources.length === 1 ? "" : "s"}</span></div><div class="c-card-note c-card-note-top">${how}</div><div class="tpl-src-rows">${sources.length ? sourceRows(sources) : `<div class="c-card-empty">No source templates are registered yet.</div>`}</div></section>`;
}

/** A sink template has no streams of its own: instead of a trigger form, list
 *  the registered source templates it can be composed with. */
async function renderSinkPairings(host, id) {
  host.innerHTML = `<div class="sp-loading">loading…</div>`;
  let sources = [];
  try {
    const data = await api("/v1/templates?kind=source-template");
    sources = data.templates || [];
  } catch (e) {
    host.innerHTML = `<div class="c-alert bad">${escapeHtml(e.message)}</div>`;
    return;
  }
  const how = `A sink template is run <b>through a source template</b>: open one below — <b class="mono">${escapeHtml(id)}</b> is preselected as its sink — and trigger.`;
  host.innerHTML = `<section class="c-card"><div class="c-card-h"><b>Compose with a source template</b><span class="c-card-r">${fmtInt(sources.length)} source template${sources.length === 1 ? "" : "s"}</span></div><div class="c-card-note c-card-note-top">${how}</div><div class="tpl-src-rows">${sources.length ? sourceRows(sources, id) : `<div class="c-card-empty">No source templates are registered yet.</div>`}</div></section>`;
}

const HEALTH_PILL = {
  ok: "pill-completed",
  running: "pill-running",
  warming: "pill-queued",
  unknown: "pill-cancelled",
  degraded: "pill-queued",
  failed: "pill-failed",
};

function healthPill(h) {
  return `<span class="pill ${HEALTH_PILL[h] || "pill-cancelled"}">${escapeHtml(h)}</span>`;
}

function bookmarkText(b) {
  if (b == null) return "—";
  if (typeof b === "object" && !Array.isArray(b)) {
    return Object.entries(b)
      .map(([k, v]) => `${k}=${typeof v === "string" ? v : JSON.stringify(v)}`)
      .join(" ");
  }
  return typeof b === "string" ? b : JSON.stringify(b);
}

/** The launched version's `faucet status` screen (#732), one row per pipeline row. */
async function renderHealth(host, id) {
  host.innerHTML = `<div class="health-head"><h2 class="tpl-h2">Health</h2></div><div class="empty">loading…</div>`;
  let r;
  try {
    r = await api(`/v1/status?template=${encodeURIComponent(id)}`);
  } catch (e) {
    host.innerHTML = `<div class="health-head"><h2 class="tpl-h2">Health</h2></div>
      <p class="tpl-desc">Health is unavailable for the live version — ${escapeHtml(String(e.message).split(" — ")[0])}.</p>`;
    return;
  }
  const rows = r.rows
    .map((row) => {
      const dlq = !row.dlq.configured ? "—" : row.dlq.readable ? String(row.dlq.count) : "?";
      const lag = row.lag ? row.lag.human : "—";
      const lagTitle = row.lag
        ? `${row.lag.measured === "probe" ? "queried" : "reported at the end of the last run"} ${fmtTime(row.lag.at)}`
        : "the source does not report lag";
      const b = row.batches;
      const batchNote =
        b && b.failed > 0 && !(row.reasons || []).some((r) => r.startsWith("last run: of"))
          ? [`last run: ${b.failed} of ${b.attempted} sink write(s) failed`]
          : [];
      const detail = [...(row.reasons || []), ...batchNote, ...(row.errors || []).map((e) => `unreadable: ${e}`)];
      return `<tr class="health-row health-${escapeHtml(row.health)}">
        <td class="mono">${escapeHtml(row.row)}</td>
        <td>${healthPill(row.health)}</td>
        <td class="ds-meta">${row.last_success ? fmtTime(row.last_success.at) : "never"}</td>
        <td class="ds-meta health-bm" title="${escapeHtml(bookmarkText(row.bookmark))}">${escapeHtml(bookmarkText(row.bookmark))}</td>
        <td class="health-num health-lag" title="${escapeHtml(lagTitle)}">${escapeHtml(lag)}</td>
        <td class="health-num">${dlq}</td>
        <td class="health-resume" title="${escapeHtml(row.resume)}">${escapeHtml(row.resume)}</td>
      </tr>${detail.length ? `<tr class="health-why"><td></td><td colspan="6">${detail.map(escapeHtml).join(" · ")}</td></tr>` : ""}`;
    })
    .join("");
  host.innerHTML = `
    <div class="health-head">
      <h2 class="tpl-h2">Health</h2>
      ${healthPill(r.health)}
      <span class="run-meta">state: ${escapeHtml((r.state.kinds || []).join(", ") || "none")} · as of ${fmtTime(r.generated_at)}</span>
    </div>
    ${r.state.note ? `<p class="tpl-desc">${escapeHtml(r.state.note)}</p>` : ""}
    <div class="table-scroll"><table class="ds-table health-table">
      <thead><tr><th>row</th><th>status</th><th>last success</th><th>bookmark</th><th>lag</th><th>dlq</th><th>next run resumes at</th></tr></thead>
      <tbody>${rows || `<tr><td colspan="7" class="run-meta">no rows</td></tr>`}</tbody>
    </table></div>`;
}

/** Which channels — derived and assigned — currently point at `v`. */
function channelsFor(v, st) {
  const out = [];
  if (st.stable === v) out.push("stable");
  if (st.previous === v) out.push("previous");
  if (st.newest === v) out.push("newest");
  for (const [tag, target] of Object.entries(st.tags)) {
    if (target === v) out.push(tag);
  }
  return out;
}

/** The channel chips for `v`; `stable` is the only highlighted one. */
function channelChips(v, st) {
  return channelsFor(v, st)
    .map((c) => chip(c, c === "stable" ? "stable" : ""))
    .join("");
}

/** One vocabulary for a version's test status (#856). */
const TEST_PILL = {
  passing: ["passing", "pill-completed"],
  failing: ["failing", "pill-failed"],
  untested: ["untested", "pill-queued"],
  no_tests: ["no tests", "pill-cancelled"],
  unknown: ["unknown", "pill-failed"],
};

/** A small test-status pill, with an optional prefix. */
function testPill(status, prefix = "") {
  const [label, cls] = TEST_PILL[status] || [status, "pill-cancelled"];
  return `<span class="pill ${cls}" title="tests: ${escapeHtml(label)}">${escapeHtml(prefix + label)}</span>`;
}

/** The test status of one version (#856), from the detail response. */
function testsFor(d, v) {
  return (d.tests && (d.tests.versions || []).find((x) => x.version === v)) || null;
}

/** Whether a label (launch, a channel) may move onto a version: `passing`,
 *  or `untested` — whose tests then run first and decide. */
function canTakeLabel(t) {
  return !!t && (t.status === "passing" || t.status === "untested");
}

/** The pill, and the "why no label" / run-history block under a version row. */
function testsBlock(t) {
  if (!t) return { pill: "", body: "" };
  const pill = testPill(t.status, "tests ");
  const lines = [];
  if (t.status === "failing") {
    const cases = (t.failing || []).map((f) => `<li class="mono">${escapeHtml(f)}</li>`).join("");
    lines.push(`<div class="tpl-tests-why"><b>Can't be launched or take a channel</b> — its tests fail.${cases ? `<ul>${cases}</ul>` : ""}</div>`);
  } else if (t.status === "unknown") {
    lines.push(`<div class="tpl-tests-why"><b>Can't be launched or take a channel</b> — ${mdInline(t.reason || "its tests or results cannot be read.")}</div>`);
  } else if (t.status === "untested") {
    lines.push(`<div class="tpl-tests-why tpl-tests-warn">No test result under this faucet version — launching or assigning a channel runs its tests first.</div>`);
  }
  if (t.results && t.results.length) {
    const rows = t.results
      .map((r) => {
        const failed = r.cases.filter((c) => !c.passed).length;
        const retried = r.cases.filter((c) => c.attempts > 1).length;
        return `<tr><td class="tpl-launch-when">${fmtTime(r.recorded_at)}</td><td><span class="pill ${r.passed ? "pill-completed" : "pill-failed"}">${r.passed ? "pass" : "fail"}</span></td><td class="tpl-tests-cases">${r.cases.length - failed}/${r.cases.length}${retried ? ` · ${retried} retried` : ""}${r.error ? ` · ${escapeHtml(r.error)}` : ""}</td><td class="mono">${escapeHtml(r.faucet_version)}</td><td>${escapeHtml(r.recorded_by || "cli")}</td></tr>`;
      })
      .join("");
    lines.push(`<details class="tpl-tests-runs"><summary>${t.results.length} recorded test run${t.results.length === 1 ? "" : "s"}</summary><div class="tpl-launch-wrap"><table class="tbl tpl-tests-tbl"><thead><tr><th>run</th><th>result</th><th>cases</th><th>faucet</th><th>by</th></tr></thead><tbody>${rows}</tbody></table></div></details>`);
  }
  return { pill, body: lines.length ? `<div class="tpl-tests">${lines.join("")}</div>` : "" };
}

/** A typed form over the template's declared `params:`, plus a version selector.
 *  For a source template (`withSink`) the form also picks a registered sink
 *  template + version, and the param fields are the union of both. */
async function renderTrigger(host, id, st, d, withSink = false, preselectSink = null) {
  const ownParams = d.params || {};
  let sinks = [];
  let overlays = [];
  if (withSink) {
    try {
      const data = await api("/v1/templates?kind=sink-template");
      sinks = (data.templates || []).filter((s) => ((s.state || {}).status || "draft") !== "deprecated");
      const ov = await api("/v1/templates?kind=deployment");
      overlays = (ov.templates || []).filter((o) => ((o.state || {}).status || "draft") !== "deprecated");
    } catch (e) {
      host.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
      return;
    }
    if (!sinks.length) {
      host.innerHTML = `<div class="tpl-notice tpl-notice-warn">This is a <b>source template</b> — it runs composed with a <b>sink template</b>, and none is registered. Register one (<code>kind: sink-template</code>) and come back.</div>`;
      return;
    }
  }
  const sinkOptions = sinks
    .map((s) => `<option value="${escapeHtml(s.id)}" title="${escapeHtml(s.description || "")}">${escapeHtml(s.id)}</option>`)
    .join("");
  // One section per template the run composes: the source (pinned from the
  // version list above), the sink and the deployment (each picked here, with
  // its own version), then the run name. Each section lists the parameters its
  // own template declares.
  const segment = (key, title, head, hint) => `
    <section class="tpl-seg" data-seg="${key}">
      <header class="tpl-seg-head"><h3>${title}</h3>${hint ? `<span class="tpl-seg-hint">${hint}</span>` : ""}</header>
      <div class="tpl-seg-fields">${head}</div>
      <div class="tpl-params" data-params="${key}"></div>
    </section>`;
  host.innerHTML = `
    <div class="tpl-trigger">
      ${withSink ? `<p class="tpl-desc">Every stream of <b class="mono">${escapeHtml(id)}</b> lands in the chosen sink; the write mode per stream is resolved against the sink's capabilities when the run is submitted.</p>` : ""}
      ${segment(
        "source",
        withSink ? "Source" : "Template",
        `<label class="tpl-field-wide">template <span class="tpl-picked"><b class="mono">${escapeHtml(id)}</b></span></label>
         <label>version <select id="tg-version">${ownVersionOptions(st)}</select></label>
         <span id="tg-version-show" class="tpl-picked"></span>`,
        "defaults to the live version",
      )}
      ${withSink ? segment(
        "sink",
        "Sink",
        `<label class="tpl-field-wide">template <select id="tg-sink">${sinkOptions}</select></label>
         <label>version <select id="tg-sink-version"></select></label>`,
      ) : ""}
      ${withSink ? segment(
        "overlay",
        "Deployment",
        `<label class="tpl-field-wide">template
           <select id="tg-overlay">
             <option value="">none</option>
             ${overlays.map((o) => `<option value="${escapeHtml(o.id)}" title="${escapeHtml(o.description || "")}">${escapeHtml(o.id)}</option>`).join("")}
           </select>
         </label>
         <label>version <select id="tg-overlay-version" disabled><option value="">—</option></select></label>`,
        "state, DLQ, notifications and SLA for this run",
      ) : ""}
      <section class="tpl-seg" id="tg-rows-seg" hidden>
        <header class="tpl-seg-head"><h3>${withSink ? "Streams" : "Rows"}</h3><span class="tpl-seg-hint">run all of them, or a subset</span></header>
        <div class="tpl-rows-bar">
          <label class="tpl-rows-parents">include parents
            <select id="tg-parents" title="what happens when a checked row's parent or dependency is not checked">
              <option value="off">off — refuse</option>
              <option value="eligible">eligible — pull in runnable ones</option>
              <option value="all">all — pull in every one</option>
            </select>
          </label>
          <span class="tpl-rows-quick">
            <button type="button" class="tpl-chip" data-pick="default">defaults</button>
            <button type="button" class="tpl-chip" data-pick="all">all</button>
            <button type="button" class="tpl-chip" data-pick="none">none</button>
          </span>
        </div>
        <div class="tpl-rows" id="tg-rows"></div>
        <div class="tpl-rows-preview" id="tg-rows-preview"></div>
      </section>
      <section class="tpl-seg">
        <header class="tpl-seg-head"><h3>Run</h3></header>
        <div class="tpl-seg-fields"><label class="tpl-field-wide">run name <input id="tg-name" placeholder="optional" /></label></div>
        <div class="tpl-untested" id="tg-untested" hidden>
          <span id="tg-untested-why"></span>
          <label class="tpl-untested-opt"><input type="checkbox" id="tg-allow-untested" /> run anyway — the run is labelled <code>allow_untested</code> and audited</label>
        </div>
      </section>
      <div class="submit-actions"><button id="tg-go" class="btn-primary">Run</button></div>
      <pre id="tg-out" class="submit-out" hidden></pre>
    </div>`;

  const paramHosts = {
    source: host.querySelector('[data-params="source"]'),
    sink: host.querySelector('[data-params="sink"]'),
    overlay: host.querySelector('[data-params="overlay"]'),
  };
  const sinkSel = host.querySelector("#tg-sink");
  const overlaySel = host.querySelector("#tg-overlay");
  if (sinkSel && preselectSink && sinks.some((s) => s.id === preselectSink)) sinkSel.value = preselectSink;
  const overlayVersionSel = host.querySelector("#tg-overlay-version");
  // A sink's or deployment's version list is its own: rebuild the picker
  // whenever the template changes, defaulting to the live release.
  const fillVersions = (sel, tpl) => {
    if (!sel) return;
    const state = (tpl && tpl.state) || null;
    sel.disabled = !state;
    sel.innerHTML = state ? versionOptions(state) : `<option value="">—</option>`;
  };
  const refillSinkVersions = () => fillVersions(host.querySelector("#tg-sink-version"), sinks.find((s) => s.id === sinkSel.value));
  const refillOverlayVersions = () =>
    fillVersions(overlayVersionSel, overlaySel.value ? overlays.find((o) => o.id === overlaySel.value) : null);
  if (sinkSel) refillSinkVersions();
  if (overlaySel) refillOverlayVersions();
  const versionSel = host.querySelector("#tg-version");
  const sinkVersionSel = host.querySelector("#tg-sink-version");
  const recordCache = new Map();
  const paramsOf = async (tid, version) => {
    const key = `${tid}@${version}`;
    if (!recordCache.has(key)) {
      recordCache.set(
        key,
        api(`/v1/templates/${encodeURIComponent(tid)}?version=${encodeURIComponent(version)}`).then(
          (r) => r.params || {},
          (e) => { recordCache.delete(key); throw e; },
        ),
      );
    }
    return recordCache.get(key);
  };
  // Every param the run binds, by name — the same merge the server performs.
  // A name two templates share is one value, shown in the first section that
  // declares it.
  let params = ownParams;
  let renderSeq = 0;
  const allInputs = () => host.querySelectorAll("[data-params] [data-name]");
  const renderParams = async () => {
    const seq = ++renderSeq;
    const typed = {};
    for (const el of allInputs()) if (el.value !== "") typed[el.dataset.name] = el.value;
    const sections = [];
    try {
      sections.push(["source", await paramsOf(id, versionSel.value)]);
      if (sinkSel) sections.push(["sink", await paramsOf(sinkSel.value, sinkVersionSel.value)]);
      if (overlaySel) {
        sections.push(["overlay", overlaySel.value ? await paramsOf(overlaySel.value, overlayVersionSel.value) : null]);
      }
    } catch (e) {
      if (seq !== renderSeq) return;
      paramHosts.source.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
      return;
    }
    if (seq !== renderSeq) return;
    params = {};
    for (const [key, declared] of sections) {
      const target = paramHosts[key];
      target.innerHTML = "";
      if (declared === null) continue;
      // Computed params are derived from other params, not supplied — exclude
      // them from the trigger form (supplying one is rejected server-side, #573).
      const names = Object.keys(declared).filter((n) => declared[n].computed == null && !(n in params));
      for (const n of names) params[n] = declared[n];
      if (!names.length) {
        target.innerHTML = `<p class="tpl-desc">No parameters.</p>`;
        continue;
      }
      for (const name of names) {
        const field = paramField(name, declared[name] || {});
        const input = field.querySelector("[data-name]");
        if (input && typed[name] !== undefined) input.value = typed[name];
        target.appendChild(field);
      }
    }
  };
  const markPicked = () => {
    const picked = Number(versionSel.value);
    host.querySelector("#tg-version-show").innerHTML = channelChips(picked, st);
    // After a launch, an unlabelled version whose tests do not pass runs only
    // with allow_untested (the server refuses it otherwise).
    const t = testsFor(d, picked);
    const labelled = st.stable === picked || st.newest === picked || Object.values(st.tags || {}).includes(picked);
    const gated = st.stable != null && !labelled && !!t && t.status !== "passing";
    const untested = host.querySelector("#tg-untested");
    untested.hidden = !gated;
    if (!gated) host.querySelector("#tg-allow-untested").checked = false;
    host.querySelector("#tg-untested-why").innerHTML = gated
      ? `v${picked} holds no label and its tests are ${testPill(t.status)}.`
      : "";
  };
  // The row picker (#741): the template's rows from the rows API, checked by
  // default the way a bare run would pick them, with a live preview of the
  // run set the server resolves for the current choice.
  const rowsSeg = host.querySelector("#tg-rows-seg");
  const rowsList = host.querySelector("#tg-rows");
  const parentsSel = host.querySelector("#tg-parents");
  const preview = host.querySelector("#tg-rows-preview");
  let rowData = [];
  let rowsSeq = 0;
  let previewSeq = 0;
  const rowsQuery = () => {
    const q = new URLSearchParams({ version: versionSel.value });
    if (sinkSel && sinkSel.value) {
      q.set("sink", sinkSel.value);
      if (sinkVersionSel && sinkVersionSel.value) q.set("sink_version", sinkVersionSel.value);
    }
    if (overlaySel && overlaySel.value) {
      q.set("overlay", overlaySel.value);
      if (overlayVersionSel.value) q.set("overlay_version", overlayVersionSel.value);
    }
    return q;
  };
  const rowInputs = () => [...rowsList.querySelectorAll("input[data-row]")];
  const checkedRows = () => rowInputs().filter((el) => el.checked).map((el) => el.dataset.row);
  const ancestorsOf = (ids) => {
    const byId = new Map(rowData.map((r) => [r.id, r]));
    const out = new Set();
    const stack = [...ids];
    while (stack.length) {
      const r = byId.get(stack.pop());
      if (!r) continue;
      for (const a of [r.parent, ...(r.depends_on || [])].filter(Boolean)) {
        if (!out.has(a)) { out.add(a); stack.push(a); }
      }
    }
    return out;
  };
  const refreshPreview = async () => {
    const seq = ++previewSeq;
    const picked = checkedRows();
    for (const item of rowsList.querySelectorAll("[data-row-item]")) {
      item.classList.remove("tpl-pick-pulled", "tpl-pick-blocked");
      item.querySelector("[data-mark]").textContent = "";
    }
    const anc = ancestorsOf(picked);
    for (const item of rowsList.querySelectorAll("[data-row-item]")) {
      const id2 = item.dataset.rowItem;
      item.classList.toggle("tpl-pick-ancestor", anc.has(id2) && !picked.includes(id2));
    }
    if (!picked.length) {
      preview.className = "tpl-rows-preview tpl-rows-preview-err";
      preview.textContent = "choose at least one row";
      return;
    }
    const q = rowsQuery();
    q.set("state", "false");
    q.set("select", picked.join(","));
    q.set("include_parents", parentsSel.value);
    let rep;
    try {
      rep = await api(`/v1/templates/${encodeURIComponent(id)}/rows?${q}`);
    } catch (e) {
      if (seq !== previewSeq) return;
      preview.className = "tpl-rows-preview tpl-rows-preview-err";
      preview.textContent = e.message;
      return;
    }
    if (seq !== previewSeq) return;
    for (const r of rep.rows || []) {
      const item = rowsList.querySelector(`[data-row-item="${CSS.escape(r.id)}"]`);
      if (!item) continue;
      const mark = item.querySelector("[data-mark]");
      if (r.pulled_in) {
        item.classList.add("tpl-pick-pulled");
        mark.textContent = `+ pulled in for ${r.pulled_in.because}`;
      } else if (r.blocked) {
        item.classList.add("tpl-pick-blocked");
        mark.textContent = `blocked — ${r.blocked}`;
      }
    }
    if (rep.error) {
      preview.className = "tpl-rows-preview tpl-rows-preview-err";
      preview.textContent = rep.error;
    } else {
      preview.className = "tpl-rows-preview";
      preview.innerHTML = `<span class="tpl-rows-preview-label">runs</span> ${(rep.run_set || [])
        .map((r) => `<span class="mono">${escapeHtml(r)}</span>`)
        .join(`<span class="tpl-rows-arrow">→</span>`)}`;
    }
  };
  const pick = (mode) => {
    for (const el of rowInputs()) {
      if (el.disabled) { el.checked = false; continue; }
      const r = rowData.find((x) => x.id === el.dataset.row) || {};
      el.checked = mode === "all" ? true : mode === "none" ? false : !!r.default_selected;
    }
    refreshPreview();
  };
  const loadRows = async () => {
    const seq = ++rowsSeq;
    let rep;
    try {
      rep = await api(`/v1/templates/${encodeURIComponent(id)}/rows?${rowsQuery()}`);
    } catch (e) {
      if (seq !== rowsSeq) return;
      rowData = [];
      rowsSeg.hidden = false;
      rowsList.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
      preview.textContent = "";
      return;
    }
    if (seq !== rowsSeq) return;
    const before = rowInputs().length ? new Set(checkedRows()) : null;
    rowData = rep.selectable ? rep.rows || [] : [];
    rowsSeg.hidden = rowData.length < 2;
    rowsList.innerHTML = rowData.map(rowItem).join("");
    for (const el of rowInputs()) {
      const r = rowData.find((x) => x.id === el.dataset.row);
      el.checked = !el.disabled && (before ? before.has(r.id) : !!r.default_selected);
      el.onchange = refreshPreview;
    }
    if (!rowsSeg.hidden) refreshPreview();
  };
  parentsSel.onchange = refreshPreview;
  for (const b of host.querySelectorAll("[data-pick]")) b.onclick = () => pick(b.dataset.pick);
  const reloadForm = () => { renderParams(); loadRows(); };
  versionSel.onchange = () => { markPicked(); reloadForm(); };

  markPicked();
  reloadForm();
  if (sinkSel) sinkSel.onchange = () => { refillSinkVersions(); reloadForm(); };
  if (sinkVersionSel) sinkVersionSel.onchange = reloadForm;
  if (overlaySel) overlaySel.onchange = () => { refillOverlayVersions(); reloadForm(); };
  if (overlayVersionSel) overlayVersionSel.onchange = reloadForm;

  const out = host.querySelector("#tg-out");
  host.querySelector("#tg-go").onclick = async () => {
    const supplied = {};
    for (const el of allInputs()) {
      const raw = el.value;
      if (raw === "") continue; // omitted → the template's default (or a typed error)
      const type = (params[el.dataset.name] || {}).type;
      const bad = invalidParam(raw, type);
      if (bad) {
        toast(`${el.dataset.name}: ${bad}`, "error");
        return;
      }
      supplied[el.dataset.name] = coerce(raw, type);
    }
    const body = { version: host.querySelector("#tg-version").value };
    if (sinkSel) {
      body.sink = sinkSel.value;
      body.sink_version = host.querySelector("#tg-sink-version").value;
    }
    if (overlaySel && overlaySel.value) {
      body.overlay = overlaySel.value;
      body.overlay_version = overlayVersionSel.value;
    }
    if (Object.keys(supplied).length) body.params = supplied;
    if (!rowsSeg.hidden) {
      const picked = checkedRows();
      if (!picked.length) {
        toast("choose at least one row to run", "error");
        return;
      }
      // A run without a selection takes only the default rows, so the
      // selection may be left out only when exactly those are checked.
      const defaults = rowData.filter((r) => r.default_selected).map((r) => r.id);
      const isDefault =
        picked.length === defaults.length && defaults.every((d) => picked.includes(d));
      if (!isDefault || parentsSel.value !== "off") {
        body.selection = { select: picked, include_parents: parentsSel.value };
      }
    }
    const name = host.querySelector("#tg-name").value.trim();
    if (name) body.name = name;
    if (!host.querySelector("#tg-untested").hidden) {
      if (!host.querySelector("#tg-allow-untested").checked) {
        toast("this version's tests do not pass — tick \"run anyway\" to run it", "error");
        return;
      }
      body.allow_untested = true;
    }
    try {
      // The submit response is flattened into the trigger response, so `run_id`
      // and `status` sit at the top level alongside `template_version`.
      const resp = await api(`/v1/templates/${encodeURIComponent(id)}/runs`, { method: "POST", body });
      if (resp.deprecated) toast(`deprecated template: ${resp.deprecated}`, "error");
      const via =
        (resp.sink_template ? ` → ${resp.sink_template} v${resp.sink_template_version}` : "") +
        (resp.overlay ? ` · deployment ${resp.overlay}` : "");
      for (const w of resp.warnings || []) toast(w, "error");
      const streams = (resp.streams || []).length;
      toast(`run ${resp.run_id} from v${resp.template_version}${via}${streams ? ` (${streams} stream${streams === 1 ? "" : "s"})` : ""}`);
      navigate(`#/runs/${resp.run_id}`);
    } catch (e) {
      out.hidden = false;
      out.textContent = `✗ ${e.message}\n\n` + (e.details ? JSON.stringify(e.details, null, 2) : "");
      toast(e.message, "error");
    }
  };
  return {
    pick(v) {
      versionSel.value = String(v);
      versionSel.onchange();
    },
  };
}

const ROW_STATUS_PILL = {
  mandatory: "pill-running",
  active: "pill-completed",
  available: "pill-queued",
  draft: "pill-queued",
  archived: "pill-cancelled",
};

/** One checkbox row of the trigger form's row picker (#741). */
function rowItem(r) {
  const w = r.write || {};
  const unsupported = w.supported === false;
  const facts = [];
  if (r.parent) facts.push(`child of <span class="mono">${escapeHtml(r.parent)}</span>`);
  if ((r.depends_on || []).length) facts.push(`after ${r.depends_on.map((d) => `<span class="mono">${escapeHtml(d)}</span>`).join(", ")}`);
  if (w.resolved) facts.push(escapeHtml(w.alias_applied || w.resolved));
  else if ((w.requested || []).length) facts.push(`wants ${escapeHtml(w.requested.join(" | "))}`);
  if (r.state && r.state.health) facts.push(`health ${escapeHtml(r.state.health)}`);
  const tags = (r.tags || []).map((t) => `<span class="tpl-pick-tag">${escapeHtml(t)}</span>`).join("");
  const title = [r.description, unsupported ? w.unsupported_reason : null].filter(Boolean).join(" — ");
  return `
    <label class="tpl-pick${unsupported ? " tpl-pick-disabled" : ""}" data-row-item="${escapeHtml(r.id)}"${title ? ` title="${escapeHtml(title)}"` : ""}>
      <input type="checkbox" data-row="${escapeHtml(r.id)}"${unsupported ? " disabled" : ""} />
      <span class="tpl-pick-main">
        <span class="tpl-pick-id mono">${escapeHtml(r.id)}</span>
        <span class="pill ${ROW_STATUS_PILL[r.status] || ""}">${escapeHtml(r.status)}</span>
        ${tags}
      </span>
      <span class="tpl-pick-facts">${facts.join(" · ")}</span>
      ${unsupported ? `<span class="tpl-pick-why">cannot run on this sink — ${escapeHtml(w.unsupported_reason || "")}</span>` : ""}
      <span class="tpl-pick-mark" data-mark></span>
    </label>`;
}

/** One typed input for a declared param. `fromSink` marks a field the selected
 *  sink template contributed. */
function paramField(name, p) {
  const field = document.createElement("label");
  field.className = "tpl-param";
  const type = p.type || "string";
  const input =
    type === "bool"
      ? `<select data-name="${escapeHtml(name)}"><option value="">—</option><option value="true">true</option><option value="false">false</option></select>`
      : `<input data-name="${escapeHtml(name)}" ${p.secret ? 'type="password"' : type === "int" || type === "float" ? 'type="number"' : ""}
           placeholder="${p.default !== undefined && p.default !== null ? escapeHtml(String(p.default)) : type}" />`;
  field.innerHTML = `
    <span class="tpl-param-name mono">${escapeHtml(name)}</span>
    <span class="tpl-param-tags">
      <span class="pill">${escapeHtml(type)}</span>
      ${p.required ? `<span class="pill pill-failed">required</span>` : ""}
      ${p.secret ? `<span class="pill pill-cancelled">secret</span>` : ""}
      ${p.trusted ? `<span class="pill pill-queued" title="the template's author lets this value reach paths, URLs and commands without the server's caller reference policy">trusted</span>` : ""}
      ${p.fromSink ? `<span class="pill tpl-kind tpl-kind-sink-template" title="declared by sink template ${escapeHtml(p.fromSink)}">sink</span>` : ""}
      ${p.fromOverlay ? `<span class="pill tpl-kind tpl-kind-deployment" title="declared by deployment ${escapeHtml(p.fromOverlay)}">deployment</span>` : ""}
    </span>
    ${input}
    ${p.description ? `<span class="help">${mdInline(p.description)}</span>` : ""}`;
  return field;
}

/** Options for a version picker: every channel that resolves (with its target),
 *  then every stored version pinned. `stable` comes first when launched, else
 *  `newest`, so the default selection is always runnable. */
function versionOptions(state) {
  const channels = ["stable", "newest", "previous", ...Object.keys(state.tags || {}).sort()].filter(
    (c) => channelTarget(c, state) != null,
  );
  return [
    ...channels.map((c) => `<option value="${escapeHtml(c)}">${escapeHtml(c)} (v${channelTarget(c, state)})</option>`),
    ...(state.versions || []).map((v) => {
      const retired = (state.deprecated_versions || []).some((x) => x.version === v);
      return `<option value="${v}">v${v} (pinned${retired ? ", deprecated" : ""})</option>`;
    }),
  ].join("");
}

/** The version a channel currently resolves to, or null when unset. */
function channelTarget(channel, st) {
  if (channel === "stable") return st.stable;
  if (channel === "previous") return st.previous;
  if (channel === "newest") return st.newest;
  const v = (st.tags || {})[channel];
  return v === undefined ? null : v;
}

/** Send the wire type the server expects; it accepts strings too, but a typed
 *  value keeps the error messages about the value rather than its spelling. */
function invalidParam(raw, type) {
  if (type === "int" && !/^\s*-?\d+\s*$/.test(raw)) return "expected a whole number";
  if (type === "float" && !Number.isFinite(Number(raw.trim() || "x"))) return "expected a finite number";
  return null;
}

// Numbers go as their exact text so the server parses them without float rounding.
function coerce(raw, type) {
  if (type === "int" || type === "float") return raw.trim();
  if (type === "bool") return raw === "true";
  return raw;
}

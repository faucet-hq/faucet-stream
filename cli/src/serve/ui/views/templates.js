// Pipeline template registry (#444): the Templates browser and the per-template
// **versions page** — one row per registered version showing which channels point
// at it, with the release controls (assign a channel, launch, roll back,
// deprecate) and a typed trigger form built from the template's `params:` block.
//
// The lifecycle model this view renders:
//   • registering a version never moves callers — it only extends the build list
//   • `launch` is the one mindful step that moves `stable` (and so unpinned runs)
//   • `stable` / `previous` / `newest` are derived; `dev`…`prod` are assignable
import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml, mdInline } from "../utils.js";
import { fmtTime } from "./runs.js";

const TEMPLATES_MISSING =
  "The pipeline template endpoints are not available on this server " +
  "(faucet was built without the `templates` feature, or no --template-store was configured).";

/** Channels a user may point at a version. Derived ones are never assignable. */
export const ASSIGNABLE = ["dev", "test", "staging", "pre-prod", "canary", "prod"];

/** A route is not wired at all when the feature is compiled out: bare 404. */
export function templatesUnavailable(e) {
  return e && e.status === 404 && !e.code;
}

/** Template kind → pill. Old rows without a stored kind are pipelines. */
const KINDS = ["source-template", "sink-template", "deployment", "pipeline"];
const KIND_LABEL = { "source-template": "source", "sink-template": "sink", deployment: "deployment", pipeline: "pipeline" };
function kindOf(t) {
  return KINDS.includes(t.kind) ? t.kind : "pipeline";
}
function kindPill(kind) {
  const k = KINDS.includes(kind) ? kind : "pipeline";
  const title = {
    "source-template": "source template — a system and its streams; runs composed with a sink template",
    "sink-template": "sink template — a destination; composed into a source template's run",
    deployment: "deployment overlay — state, DLQ, notifications and SLA applied over a composed run",
    pipeline: "complete pipeline config",
  }[k];
  return `<span class="pill tpl-kind tpl-kind-${escapeHtml(k)}" title="${escapeHtml(title)}"><span class="pill-label">${KIND_LABEL[k]}</span></span>`;
}

function statusPill(status) {
  const cls = { launched: "pill-completed", draft: "pill-queued", deprecated: "pill-cancelled" };
  return `<span class="pill ${cls[status] || ""}">${escapeHtml(status)}</span>`;
}

// ── list ────────────────────────────────────────────────────────────────────

export async function renderTemplates(container) {
  container.innerHTML = `
    <div class="page">
      <div class="page-head">
        <h1>Templates</h1>
        <button class="btn-ghost" id="t-refresh">↻</button>
        <button class="btn-ghost" id="t-sync" data-perm="template_admin" hidden title="Pull templates from the configured remote origins">Sync from origins</button>
        <button class="btn-primary" id="t-new" data-perm="template_admin">Register a template</button>
      </div>
      <div id="t-register" hidden></div>
      <div id="t-sync-panel" hidden></div>
      <div id="t-matrix" hidden></div>
      <div class="filters" id="t-filters" hidden>
        <input id="t-search" type="search" autocomplete="off"
          placeholder="search templates by id or description…" />
        <div class="tpl-status-filter" id="t-status-filter" role="group" aria-label="Filter by status">
          <span class="tpl-filter-label">status</span>
          <button type="button" class="tpl-chip is-on" data-status="launched">launched</button>
          <button type="button" class="tpl-chip is-on" data-status="draft">draft</button>
          <button type="button" class="tpl-chip" data-status="deprecated">deprecated</button>
        </div>
        <div class="tpl-status-filter" id="t-kind-filter" role="group" aria-label="Filter by kind">
          <span class="tpl-filter-label">kind</span>
          <button type="button" class="tpl-chip is-on" data-kind="source-template">source</button>
          <button type="button" class="tpl-chip is-on" data-kind="sink-template">sink</button>
          <button type="button" class="tpl-chip" data-kind="deployment">deployment</button>
          <button type="button" class="tpl-chip" data-kind="pipeline">pipeline</button>
        </div>
      </div>
      <div class="tpl-list-head" id="t-list-head" hidden>
        <span>status</span>
        <button type="button" class="tpl-sort" data-sort="name">name<span class="tpl-sort-caret"></span></button>
        <span>kind</span>
        <button type="button" class="tpl-sort tpl-col-r" data-sort="updated">last updated<span class="tpl-sort-caret"></span></button>
        <span class="tpl-col-r">live</span>
        <span class="tpl-col-r">newest</span>
        <span class="tpl-col-r">params</span>
      </div>
      <div id="t-list" class="runs-list"></div>
    </div>`;

  const list = container.querySelector("#t-list");
  const registerHost = container.querySelector("#t-register");
  const filters = container.querySelector("#t-filters");
  const listHead = container.querySelector("#t-list-head");
  const search = container.querySelector("#t-search");
  let all = [];

  // Column sort — alphabetical by name is the default; clicking a sortable header
  // sets the column and toggles asc/desc on repeat clicks. Status is a filter
  // (below), not a sort column.
  const sort = { col: "name", dir: 1 };
  const sortKey = {
    name: (t) => (t.id || "").toLowerCase(),
    updated: (t) => new Date(t.created_at || 0).getTime() || 0,
  };
  listHead.querySelectorAll(".tpl-sort").forEach((btn) => {
    btn.onclick = () => {
      const col = btn.dataset.sort;
      if (sort.col === col) sort.dir *= -1;
      else { sort.col = col; sort.dir = 1; }
      render();
    };
  });

  // Status filter — launched + draft on by default; deprecated hidden until its
  // chip is toggled on.
  const statusFilter = new Set(["launched", "draft"]);
  container.querySelectorAll("#t-status-filter .tpl-chip").forEach((chip) => {
    chip.onclick = () => {
      const s = chip.dataset.status;
      if (statusFilter.has(s)) statusFilter.delete(s);
      else statusFilter.add(s);
      chip.classList.toggle("is-on", statusFilter.has(s));
      render();
    };
  });

  // Kind filter — source and sink templates on by default; deployments and
  // pipelines are one chip away.
  const kindFilter = new Set(["source-template", "sink-template"]);
  container.querySelectorAll("#t-kind-filter .tpl-chip").forEach((chip) => {
    chip.onclick = () => {
      const k = chip.dataset.kind;
      if (kindFilter.has(k)) kindFilter.delete(k);
      else kindFilter.add(k);
      chip.classList.toggle("is-on", kindFilter.has(k));
      render();
    };
  });

  container.querySelector("#t-new").onclick = () => {
    registerHost.hidden = !registerHost.hidden;
    if (!registerHost.hidden && !registerHost.childElementCount) {
      registerHost.appendChild(registerPanel(() => load()));
    }
  };
  container.querySelector("#t-refresh").onclick = () => load();
  search.oninput = () => render();

  // Client-side filter over the loaded set — match id or description,
  // case-insensitive. The registry is small, so there's no server round-trip.
  function render() {
    const q = search.value.trim().toLowerCase();
    const rows = all.filter((t) => {
      const status = (t.state || {}).status || "draft";
      if (!statusFilter.has(status)) return false;
      if (!kindFilter.has(kindOf(t))) return false;
      if (q && !((t.id || "").toLowerCase().includes(q) || (t.description || "").toLowerCase().includes(q))) return false;
      return true;
    });
    const key = sortKey[sort.col];
    if (key) {
      rows.sort((a, b) => {
        const av = key(a), bv = key(b);
        return (av < bv ? -1 : av > bv ? 1 : 0) * sort.dir;
      });
    }
    updateSortCarets();
    list.innerHTML = "";
    listHead.hidden = !rows.length; // only show the column header when rows are shown
    if (!rows.length) {
      const why = q
        ? `No templates match “${escapeHtml(search.value.trim())}”.`
        : "No templates match the selected status / kind filters.";
      list.innerHTML = `<div class="empty">${why}</div>`;
      return;
    }
    for (const t of rows) list.appendChild(listRow(t));
  }

  // Reflect the active sort on the header: ▲/▼ on the sorted column, a faint ↕
  // hint on the other sortable columns.
  function updateSortCarets() {
    listHead.querySelectorAll(".tpl-sort").forEach((btn) => {
      const active = sort.col === btn.dataset.sort;
      btn.classList.toggle("is-active", active);
      btn.setAttribute("aria-sort", active ? (sort.dir > 0 ? "ascending" : "descending") : "none");
      btn.querySelector(".tpl-sort-caret").textContent = active ? (sort.dir > 0 ? " ▲" : " ▼") : " ↕";
    });
  }

  const syncBtn = container.querySelector("#t-sync");
  const syncPanel = container.querySelector("#t-sync-panel");
  syncBtn.onclick = () => {
    if (!syncPanel.hidden) {
      syncPanel.hidden = true;
      return;
    }
    syncPanel.innerHTML = "";
    syncPanel.appendChild(syncControls(syncOrigins, load));
    syncPanel.hidden = false;
  };
  let syncOrigins = [];

  const matrixHost = container.querySelector("#t-matrix");
  // The source × sink compatibility matrix — shown as soon as the registry
  // holds at least one of each kind, fetched separately so the list never
  // waits on composition.
  async function loadMatrix() {
    const hasSources = all.some((t) => kindOf(t) === "source-template");
    const hasSinks = all.some((t) => kindOf(t) === "sink-template");
    if (!hasSources || !hasSinks) {
      matrixHost.hidden = true;
      matrixHost.innerHTML = "";
      return;
    }
    try {
      const idx = await api("/v1/templates/matrix");
      matrixHost.innerHTML = "";
      matrixHost.appendChild(renderMatrix(idx));
      matrixHost.hidden = false;
    } catch (e) {
      matrixHost.hidden = true;
      toast(`matrix: ${e.message}`, "error");
    }
  }

  async function load() {
    try {
      const data = await api("/v1/templates");
      all = data.templates || [];
      syncOrigins = (data.sync && data.sync.origins) || [];
      syncBtn.hidden = syncOrigins.length === 0;
      loadMatrix();
      list.innerHTML = "";
      if (!all.length) {
        filters.hidden = true;
        listHead.hidden = true;
        list.innerHTML = `<div class="empty">No templates registered yet — register a <b>source template</b> (a system and its streams), a <b>sink template</b> (a destination), or a complete pipeline to give operators something versioned to trigger.</div>`;
        return;
      }
      filters.hidden = false;
      render(); // keeps any active search term across a refresh
    } catch (e) {
      filters.hidden = true;
      listHead.hidden = true;
      if (templatesUnavailable(e)) list.innerHTML = `<div class="empty">${TEMPLATES_MISSING}</div>`;
      else toast(e.message, "error");
    }
  }

  await load();
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

function listRow(t) {
  const st = t.state || {};
  const el = document.createElement("div");
  el.className = "run-row tpl-row";
  el.onclick = () => navigate(`#/templates/${encodeURIComponent(t.id)}`);
  const params = Object.keys(t.params || {}).length;
  const live = st.stable == null ? "—" : `v${st.stable}`;
  el.innerHTML = `
    ${statusPill(st.status || "draft")}
    <span class="tpl-row-id">
      <span class="tpl-row-title"><b class="mono">${escapeHtml(t.id)}</b></span>
      ${t.description ? `<span class="tpl-row-desc">${escapeHtml(t.description)}</span>` : ""}
    </span>
    <span class="tpl-row-kind">${kindPill(kindOf(t))}</span>
    <span class="run-meta" data-l="updated" title="last registered / updated">${fmtTime(t.created_at)}</span>
    <span class="run-meta" data-l="live" title="live version — what an unpinned run uses">${live}</span>
    <span class="run-meta" data-l="newest" title="newest registered build">v${st.newest ?? t.version}</span>
    <span class="run-meta" data-l="params" title="declared params">${params}</span>`;
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
      <label><input id="tr-test" type="checkbox" /> run its tests: bundle</label>
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
    if (el.querySelector("#tr-test").checked) body.test = true;
    try {
      const resp = await api("/v1/templates", { method: "POST", body });
      out.hidden = true;
      if (resp.tests && !resp.tests.passed) toast(`${resp.id} v${resp.version} registered — its tests failed`, "error");
      else toast(`${resp.id} v${resp.version} registered${resp.tests ? " — tests passed" : ""}`);
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

export async function renderTemplateDetail(container, { id, query }) {
  const preselectSink = query && query.get("sink");
  container.innerHTML = `<div class="page"><div class="empty">loading…</div></div>`;

  // The detail response carries the whole release state, so one request is
  // enough to render every version row and every control.
  let d;
  try {
    // `newest` (not the default `stable`) so a draft template — which has no
    // stable version at all — still opens.
    d = await api(`/v1/templates/${encodeURIComponent(id)}?version=newest`);
  } catch (e) {
    const msg = templatesUnavailable(e) ? TEMPLATES_MISSING : e.message;
    container.innerHTML = `<div class="page"><div class="empty">${escapeHtml(msg)}</div></div>`;
    return;
  }

  const reload = () => renderTemplateDetail(container, { id, query });
  const st = { status: d.status, versions: d.versions, stable: d.stable, previous: d.previous, newest: d.newest, tags: d.tags || {}, deprecation: d.deprecation, deprecated_versions: d.deprecated_versions || [] };
  // A pipeline or source template runs from this page; the sink/deployment
  // panels in the same slot are read-only information, visible to everyone.
  const runnable = !["sink-template", "deployment"].includes(kindOf(d));

  container.innerHTML = `
    <div class="page">
      <div class="page-head">
        <button class="btn-ghost" id="t-back">← Templates</button>
        <h1 class="dataset-title mono">${escapeHtml(d.id)}</h1>
        ${kindPill(kindOf(d))}
        ${statusPill(st.status)}
        <div class="detail-actions">
          <button class="btn-primary" id="t-newver" data-perm="template_admin" title="register a new version of this template">+ New version</button>
          <button class="btn-ghost" id="t-rollback" data-perm="template_admin" ${st.previous == null ? "disabled" : ""}
            title="${st.previous == null ? "no earlier launch to roll back to" : `re-launch v${st.previous}`}">Roll back</button>
          <button class="${st.status === "deprecated" ? "btn-ghost" : "btn-warn"}" id="t-deprecate" data-perm="template_admin">${st.status === "deprecated" ? "Revive" : "Deprecate"}</button>
        </div>
      </div>

      <div id="t-newver-host" hidden></div>

      ${st.status === "draft" ? `<div class="tpl-notice">This template is a <b>draft</b> — nothing has been launched, so a run without an explicit version is refused. Launch a version to make it live.</div>` : ""}
      ${st.status === "deprecated" ? `<div class="tpl-notice tpl-notice-warn">Deprecated${d.deprecation && d.deprecation.reason ? ` — ${escapeHtml(d.deprecation.reason)}` : ""}. Existing callers still resolve <code>stable</code>, but every trigger warns.</div>` : ""}

      <div class="detail-grid">
        <div><label>kind</label><b>${escapeHtml(KIND_LABEL[kindOf(d)])}</b></div>
        <div><label>status</label><b>${escapeHtml(st.status)}</b></div>
        <div><label>live (stable)</label><b>${st.stable == null ? "—" : `v${st.stable}`}</b></div>
        <div><label>previous</label><b>${st.previous == null ? "—" : `v${st.previous}`}</b></div>
        <div><label>newest</label><b>${st.newest == null ? "—" : `v${st.newest}`}</b></div>
        <div><label>versions</label><b>${st.versions.length}</b></div>
        ${d.name && d.name !== d.id ? `<div><label>config name</label><b>${escapeHtml(d.name)}</b></div>` : ""}
      </div>
      ${d.description ? `<p class="tpl-desc">${escapeHtml(d.description)}</p>` : ""}

      ${kindOf(d) === "pipeline" && st.stable != null ? `<div id="t-health" class="health-card" data-perm="status_read"></div>` : ""}

      <h2 class="tpl-h2">Versions</h2>
      <p class="tpl-desc tpl-versions-hint"${runnable ? ' data-perm="run_write"' : ""}>Click a version to run it below.</p>
      <div id="t-versions" class="tpl-versions"></div>

      <div${runnable ? ' data-perm="run_write"' : ""}>
        <h2 class="tpl-h2">${{ "sink-template": "Compose with a source template", deployment: "Apply to a run" }[kindOf(d)] || "Trigger a run"}</h2>
        <div id="t-trigger"></div>
      </div>

      <h2 class="tpl-h2">Launch history</h2>
      <div id="t-launches"></div>
    </div>`;

  container.querySelector("#t-back").onclick = () => navigate("#/templates");

  // "+ New version" — open the same register panel, but pinned to this id and
  // seeded with the newest version's config so a new build is an edit of the
  // current one rather than a blank slate.
  const newverHost = container.querySelector("#t-newver-host");
  container.querySelector("#t-newver").onclick = async () => {
    if (!newverHost.hidden) { newverHost.hidden = true; newverHost.innerHTML = ""; return; }
    let seed = "";
    try {
      const rec = await api(`/v1/templates/${encodeURIComponent(id)}?version=${st.newest}`);
      seed = rec.body || "";
    } catch (e) { toast(`could not load v${st.newest} config: ${e.message}`, "error"); }
    newverHost.innerHTML = "";
    newverHost.appendChild(
      registerPanel(reload, { presetId: id, presetBody: seed, presetDesc: d.description || "", lockId: true, newVersion: true }),
    );
    newverHost.hidden = false;
  };

  container.querySelector("#t-rollback").onclick = async () => {
    if (st.previous == null) return;
    try {
      const r = await api(`/v1/templates/${encodeURIComponent(id)}/rollback`, { method: "POST", body: {} });
      toast(`rolled back to v${r.version}`);
      reload();
    } catch (e) { toast(e.message, "error"); }
  };

  container.querySelector("#t-deprecate").onclick = async () => {
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
    } catch (e) { toast(e.message, "error"); }
  };

  renderVersions(container.querySelector("#t-versions"), id, st, d, reload);
  const kind = kindOf(d);
  if (kind === "sink-template") renderSinkPairings(container.querySelector("#t-trigger"), id);
  else if (kind === "deployment") renderDeploymentUse(container.querySelector("#t-trigger"), id);
  else renderTrigger(container.querySelector("#t-trigger"), id, st, d, kind === "source-template", preselectSink);
  renderLaunches(container.querySelector("#t-launches"), d.launches || []);
  const healthHost = container.querySelector("#t-health");
  if (healthHost) renderHealth(healthHost, id);
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
  if (st.stable === v) out.push(["stable", "pill-completed"]);
  if (st.previous === v) out.push(["previous", "pill-cancelled"]);
  if (st.newest === v) out.push(["newest", "pill-running"]);
  for (const [tag, target] of Object.entries(st.tags)) {
    if (target === v) out.push([tag, "pill-queued"]);
  }
  return out;
}

const TEST_PILL = {
  passed: ["tests passed", "pill-completed"],
  failed: ["tests failed", "pill-failed"],
  stale: ["tests stale", "pill-queued"],
  not_run: ["tests not run", "pill-queued"],
  no_tests: ["no tests", "pill-cancelled"],
};

/** The test-gate standing of one version (#856), from the detail response. */
function testsFor(d, v) {
  return (d.tests && (d.tests.versions || []).find((x) => x.version === v)) || null;
}

/** The pill, and the blocked-launch / history block under a version row. */
function testsBlock(t, requireTests) {
  if (!t) return { pill: "", body: "" };
  const [label, cls] = TEST_PILL[t.gate.status] || [t.gate.status, "pill-cancelled"];
  const pill = `<span class="pill ${cls}" title="${escapeHtml(t.gate.reason || "the latest run passed under this faucet version")}">${escapeHtml(label)}</span>`;
  const lines = [];
  if (!t.gate.allowed && t.gate.failing && t.gate.failing.length) {
    lines.push(`<div class="tpl-tests-why"><b>Launch blocked</b> — failing cases:<ul>${t.gate.failing.map((f) => `<li class="mono">${escapeHtml(f)}</li>`).join("")}</ul></div>`);
  } else if (!t.gate.allowed && t.gate.reason) {
    lines.push(`<div class="tpl-tests-why"><b>Launch blocked</b> — ${mdInline(t.gate.reason)}</div>`);
  } else if (!requireTests && t.gate.status === "no_tests") {
    lines.push(`<div class="tpl-tests-why tpl-tests-warn">No <code>tests:</code> block — this version launches untested. A server with <code>--require-template-tests</code> would refuse it.</div>`);
  } else if (!requireTests && t.gate.status !== "passed") {
    lines.push(`<div class="tpl-tests-why tpl-tests-warn">${mdInline(t.gate.reason || "Tests have not passed.")} Launching is allowed only because this server does not require tests.</div>`);
  }
  if (t.results && t.results.length) {
    const rows = t.results
      .map((r) => {
        const failed = r.cases.filter((c) => !c.passed).length;
        const retried = r.cases.filter((c) => c.attempts > 1).length;
        return `<tr><td class="tpl-launch-when">${fmtTime(r.recorded_at)}</td><td><span class="pill ${r.passed ? "pill-completed" : "pill-failed"}">${r.passed ? "pass" : "fail"}</span></td><td class="tpl-tests-cases">${r.cases.length - failed}/${r.cases.length}${retried ? ` · ${retried} retried` : ""}${r.error ? ` · ${escapeHtml(r.error)}` : ""}</td><td class="mono">${escapeHtml(r.faucet_version)}</td><td>${escapeHtml(r.recorded_by || "cli")}</td></tr>`;
      })
      .join("");
    lines.push(`<div class="tpl-launch-wrap"><table class="tbl tpl-tests-tbl"><thead><tr><th>run</th><th>result</th><th>cases</th><th>faucet</th><th>by</th></tr></thead><tbody>${rows}</tbody></table></div>`);
  }
  return { pill, body: lines.length ? `<div class="tpl-tests">${lines.join("")}</div>` : "" };
}

function renderVersions(host, id, st, d, reload) {
  host.innerHTML = "";
  const requireTests = !!(d.tests && d.tests.require_tests);
  if (!st.versions.length) {
    host.innerHTML = `<div class="empty">No versions stored.</div>`;
    return;
  }
  for (const v of st.versions) {
    const retired = (st.deprecated_versions || []).find((x) => x.version === v);
    const row = document.createElement("div");
    row.className = "tpl-version" + (st.stable === v ? " tpl-version-live" : "") + (retired ? " tpl-version-retired" : "");
    row.dataset.version = String(v);
    const pills =
      (retired
        ? `<span class="pill pill-cancelled" title="${escapeHtml(retired.reason ? `deprecated: ${retired.reason}` : "deprecated")}">deprecated</span>`
        : "") +
      channelsFor(v, st)
        .map(([name, cls]) => `<span class="pill ${cls}">${escapeHtml(name)}</span>`)
        .join("");
    const t = testsFor(d, v);
    const tb = testsBlock(t, requireTests);
    const gated = !!t && !t.gate.allowed && st.stable !== v;
    const launchTitle = st.stable === v ? "already live" : retired ? `v${v} is deprecated — revive it to launch it` : gated ? `tests block the launch: ${t.gate.reason || "see below"}` : `make v${v} live for unpinned runs`;
    row.innerHTML = `
      <span class="tpl-vnum mono">v${v}</span>
      <span class="tpl-vchannels">${pills || `<span class="run-meta">no channel</span>`}${tb.pill}</span>
      <select class="tpl-assign" data-perm="template_admin" title="point a channel at v${v}">
        <option value="">assign channel</option>
        ${ASSIGNABLE.map((c) => `<option value="${c}">${c}</option>`).join("")}
      </select>
      <button class="btn-ghost tpl-launch" data-perm="template_admin" ${st.stable === v || retired || gated ? "disabled" : ""}
        title="${escapeHtml(launchTitle)}">Launch</button>
      ${t && t.has_tests ? `<button class="btn-ghost tpl-runtests" data-perm="template_admin" title="run v${v}'s tests: bundle and record the result">Run tests</button>` : ""}
      <button class="btn-ghost tpl-view">Config</button>
      <button class="btn-ghost tpl-view-clean" title="comments stripped, canonical YAML">Clean</button>
      <button class="${retired ? "btn-ghost" : "btn-warn"} tpl-vdeprecate" data-perm="template_admin"
        title="${retired ? `make v${v} usable again` : `retire v${v}: pinned runs still work but warn, newest skips it, launch refuses it`}">${retired ? "Revive" : "Deprecate"}</button>
      <button class="btn-danger tpl-del" data-perm="template_admin">Delete</button>
      ${tb.body}
      <pre class="tpl-body" hidden></pre>`;

    const runTests = row.querySelector(".tpl-runtests");
    if (runTests) {
      runTests.onclick = async (ev) => {
        ev.stopPropagation();
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
      } catch (e) { toast(e.message, "error"); ev.target.value = ""; }
    };

    row.querySelector(".tpl-vdeprecate").onclick = async () => {
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
      } catch (e) { toast(e.message, "error"); }
    };

    row.querySelector(".tpl-launch").onclick = async () => {
      try {
        const r = await api(`/v1/templates/${encodeURIComponent(id)}/launch`, { method: "POST", body: { version: v } });
        toast(r.already_launched ? `v${r.version} was already live` : `v${r.version} is live${r.replaced != null ? ` (was v${r.replaced})` : ""}`);
        reload();
      } catch (e) { toast(e.message, "error"); }
    };

    const body = row.querySelector(".tpl-body");
    // Config = raw stored body; Clean = comments stripped, canonical YAML (the
    // server renders it via ?clean=true, sharing the CLI's clean_config_yaml).
    // Each toggles the shared <pre>; switching modes refetches so the two views
    // never collide.
    const showConfig = async (clean) => {
      const mode = clean ? "clean" : "raw";
      if (!body.hidden && body.dataset.mode === mode) {
        body.hidden = true;
        return;
      }
      try {
        const q = clean ? `?version=${v}&clean=true` : `?version=${v}`;
        const rec = await api(`/v1/templates/${encodeURIComponent(id)}${q}`);
        body.textContent = rec.body;
        body.dataset.mode = mode;
        body.hidden = false;
      } catch (e) {
        toast(e.message, "error");
      }
    };
    row.querySelector(".tpl-view").onclick = () => showConfig(false);
    row.querySelector(".tpl-view-clean").onclick = () => showConfig(true);

    row.querySelector(".tpl-del").onclick = async () => {
      if (!confirm(`Delete ${id} v${v}? Channels pointing at it are dropped too.`)) return;
      try {
        await api(`/v1/templates/${encodeURIComponent(id)}?version=${v}`, { method: "DELETE" });
        toast(`v${v} deleted`);
        if (st.versions.length === 1) navigate("#/templates");
        else reload();
      } catch (e) { toast(e.message, "error"); }
    };

    host.appendChild(row);
  }
}

/** A sink template has no streams of its own: instead of a trigger form, list
 *  the registered source templates it can be composed with. */
/** A deployment overlay is never run on its own: it is picked in a source
 *  template's trigger form, alongside the sink. */
async function renderDeploymentUse(host, id) {
  host.innerHTML = `<div class="empty">loading…</div>`;
  let sources = [];
  try {
    const data = await api("/v1/templates?kind=source-template");
    sources = data.templates || [];
  } catch (e) {
    host.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
    return;
  }
  const how = `A deployment overlay adds the operational blocks — state, DLQ, notifications, SLA — to a <b>source × sink</b> run: open a source template, pick a sink, and choose <b class="mono">${escapeHtml(id)}</b> as its deployment.`;
  if (!sources.length) {
    host.innerHTML = `<div class="tpl-notice">${how} No source templates are registered yet.</div>`;
    return;
  }
  host.innerHTML = `<p class="tpl-desc">${how}</p><div class="runs-list" id="td-list"></div>`;
  const list = host.querySelector("#td-list");
  for (const s of sources) list.appendChild(listRow(s));
}

async function renderSinkPairings(host, id) {
  host.innerHTML = `<div class="empty">loading…</div>`;
  let sources = [];
  try {
    const data = await api("/v1/templates?kind=source-template");
    sources = data.templates || [];
  } catch (e) {
    host.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
    return;
  }
  if (!sources.length) {
    host.innerHTML = `<div class="tpl-notice">A sink template is run <b>through a source template</b>: open one, pick <b class="mono">${escapeHtml(id)}</b> as its sink, and trigger. No source templates are registered yet.</div>`;
    return;
  }
  host.innerHTML = `
    <p class="tpl-desc">A sink template is run <b>through a source template</b>: open one below, pick <b class="mono">${escapeHtml(id)}</b> as its sink, and trigger.</p>
    <div class="runs-list" id="tp-list"></div>`;
  const list = host.querySelector("#tp-list");
  for (const s of sources) list.appendChild(listRow(s));
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
         <label>version
           <input type="hidden" id="tg-version" value="${st.stable ?? st.newest}" />
           <span id="tg-version-show" class="tpl-picked"></span>
         </label>`,
        "pick the version in the list above",
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
  // The version rows above the form pick the source version to run: clicking
  // one selects it here, and the row that matches the selection stays marked.
  const rows = [...(host.closest(".page") || document).querySelectorAll(".tpl-version[data-version]")];
  const markPicked = () => {
    const picked = Number(versionSel.value);
    const pills = channelsFor(picked, st)
      .map(([name, cls]) => `<span class="pill ${cls}">${escapeHtml(name)}</span>`)
      .join("");
    host.querySelector("#tg-version-show").innerHTML = `<b class="mono">v${picked}</b>${pills}`;
    for (const r of rows) r.classList.toggle("tpl-version-picked", Number(r.dataset.version) === picked);
  };
  for (const r of rows) {
    r.title = `run v${r.dataset.version}`;
    r.onclick = (ev) => {
      if (ev.target.closest("button, select, a, pre, .tpl-tests")) return;
      versionSel.value = r.dataset.version;
      markPicked();
      renderParams();
    };
  }
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
  for (const r of rows) {
    const choose = r.onclick;
    r.onclick = (ev) => { const before = versionSel.value; choose(ev); if (versionSel.value !== before) loadRows(); };
  }

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
      supplied[el.dataset.name] = coerce(raw, (params[el.dataset.name] || {}).type);
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
      if (picked.length !== rowData.length || parentsSel.value !== "off") {
        body.selection = { select: picked, include_parents: parentsSel.value };
      }
    }
    const name = host.querySelector("#tg-name").value.trim();
    if (name) body.name = name;
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
function coerce(raw, type) {
  if (type === "int") return Number.parseInt(raw, 10);
  if (type === "float") return Number.parseFloat(raw);
  if (type === "bool") return raw === "true";
  return raw;
}

function renderLaunches(host, launches) {
  if (!launches.length) {
    host.innerHTML = `<div class="empty">Never launched.</div>`;
    return;
  }
  host.innerHTML = `
    <div class="tpl-launch-wrap"><table class="tbl tpl-launch-tbl">
      <thead><tr><th>#</th><th>version</th><th>when</th><th>by</th></tr></thead>
      <tbody>
        ${launches
          .map(
            (l) => `<tr><td class="mono">${l.seq}</td><td class="mono">v${l.version}</td>
              <td class="tpl-launch-when">${fmtTime(l.launched_at)}</td><td>${escapeHtml(l.launched_by || "cli")}</td></tr>`,
          )
          .join("")}
      </tbody>
    </table></div>`;
}

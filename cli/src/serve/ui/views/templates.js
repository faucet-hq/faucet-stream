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
        <button class="btn-primary" id="t-new">Register a template</button>
      </div>
      <div id="t-register" hidden></div>
      <div class="filters" id="t-filters" hidden>
        <input id="t-search" type="search" autocomplete="off"
          placeholder="search templates by id or description…" />
        <div class="tpl-status-filter" id="t-status-filter" role="group" aria-label="Filter by status">
          <button type="button" class="tpl-chip is-on" data-status="launched">launched</button>
          <button type="button" class="tpl-chip is-on" data-status="draft">draft</button>
          <button type="button" class="tpl-chip" data-status="deprecated">deprecated</button>
        </div>
      </div>
      <div class="tpl-list-head" id="t-list-head" hidden>
        <span>status</span>
        <button type="button" class="tpl-sort" data-sort="name">name<span class="tpl-sort-caret"></span></button>
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
        : "No templates match the selected status filter.";
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

  async function load() {
    try {
      const data = await api("/v1/templates");
      all = data.templates || [];
      list.innerHTML = "";
      if (!all.length) {
        filters.hidden = true;
        listHead.hidden = true;
        list.innerHTML = `<div class="empty">No templates registered yet — register one to give operators a parameterized, versioned pipeline to trigger.</div>`;
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
      <b class="mono">${escapeHtml(t.id)}</b>
      ${t.description ? `<span class="tpl-row-desc">${escapeHtml(t.description)}</span>` : ""}
    </span>
    <span class="run-meta" title="last registered / updated">${fmtTime(t.created_at)}</span>
    <span class="run-meta" title="live version — what an unpinned run uses">${live}</span>
    <span class="run-meta" title="newest registered build">v${st.newest ?? t.version}</span>
    <span class="run-meta" title="declared params">${params}</span>`;
  return el;
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
    <textarea id="tr-cfg" class="code" spellcheck="false" placeholder="version: 1
params:
  table: { type: string, required: true }
pipeline:
  source: { type: postgres, config: { query: 'select * from \${param.table}' } }
  sink: { type: jsonl, config: { path: ./out.jsonl } }">${escapeHtml(presetBody)}</textarea>
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
      toast(`${resp.id} v${resp.version} registered`);
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

export async function renderTemplateDetail(container, { id }) {
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

  const reload = () => renderTemplateDetail(container, { id });
  const st = { status: d.status, versions: d.versions, stable: d.stable, previous: d.previous, newest: d.newest, tags: d.tags || {}, deprecation: d.deprecation };

  container.innerHTML = `
    <div class="page">
      <div class="page-head">
        <button class="btn-ghost" id="t-back">← Templates</button>
        <h1 class="dataset-title mono">${escapeHtml(d.id)}</h1>
        ${statusPill(st.status)}
        <div class="detail-actions">
          <button class="btn-primary" id="t-newver" title="register a new version of this template">+ New version</button>
          <button class="btn-ghost" id="t-rollback" ${st.previous == null ? "disabled" : ""}
            title="${st.previous == null ? "no earlier launch to roll back to" : `re-launch v${st.previous}`}">Roll back</button>
          <button class="${st.status === "deprecated" ? "btn-ghost" : "btn-warn"}" id="t-deprecate">${st.status === "deprecated" ? "Revive" : "Deprecate"}</button>
        </div>
      </div>

      <div id="t-newver-host" hidden></div>

      ${st.status === "draft" ? `<div class="tpl-notice">This template is a <b>draft</b> — nothing has been launched, so a run without an explicit version is refused. Launch a version to make it live.</div>` : ""}
      ${st.status === "deprecated" ? `<div class="tpl-notice tpl-notice-warn">Deprecated${d.deprecation && d.deprecation.reason ? ` — ${escapeHtml(d.deprecation.reason)}` : ""}. Existing callers still resolve <code>stable</code>, but every trigger warns.</div>` : ""}

      <div class="detail-grid">
        <div><label>status</label><b>${escapeHtml(st.status)}</b></div>
        <div><label>live (stable)</label><b>${st.stable == null ? "—" : `v${st.stable}`}</b></div>
        <div><label>previous</label><b>${st.previous == null ? "—" : `v${st.previous}`}</b></div>
        <div><label>newest</label><b>${st.newest == null ? "—" : `v${st.newest}`}</b></div>
        <div><label>versions</label><b>${st.versions.length}</b></div>
        ${d.name && d.name !== d.id ? `<div><label>config name</label><b>${escapeHtml(d.name)}</b></div>` : ""}
      </div>
      ${d.description ? `<p class="tpl-desc">${escapeHtml(d.description)}</p>` : ""}

      <h2 class="tpl-h2">Versions</h2>
      <div id="t-versions" class="tpl-versions"></div>

      <h2 class="tpl-h2">Trigger a run</h2>
      <div id="t-trigger"></div>

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
  renderTrigger(container.querySelector("#t-trigger"), id, st, d);
  renderLaunches(container.querySelector("#t-launches"), d.launches || []);
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

function renderVersions(host, id, st, d, reload) {
  host.innerHTML = "";
  if (!st.versions.length) {
    host.innerHTML = `<div class="empty">No versions stored.</div>`;
    return;
  }
  for (const v of st.versions) {
    const row = document.createElement("div");
    row.className = "tpl-version" + (st.stable === v ? " tpl-version-live" : "");
    const pills = channelsFor(v, st)
      .map(([name, cls]) => `<span class="pill ${cls}">${escapeHtml(name)}</span>`)
      .join("");
    row.innerHTML = `
      <span class="tpl-vnum mono">v${v}</span>
      <span class="tpl-vchannels">${pills || `<span class="run-meta">no channel</span>`}</span>
      <select class="tpl-assign" title="point a channel at v${v}">
        <option value="">assign channel</option>
        ${ASSIGNABLE.map((c) => `<option value="${c}">${c}</option>`).join("")}
      </select>
      <button class="btn-ghost tpl-launch" ${st.stable === v ? "disabled" : ""}
        title="${st.stable === v ? "already live" : `make v${v} live for unpinned runs`}">Launch</button>
      <button class="btn-ghost tpl-view">Config</button>
      <button class="btn-ghost tpl-view-clean" title="comments stripped, canonical YAML">Clean</button>
      <button class="btn-danger tpl-del">Delete</button>
      <pre class="tpl-body" hidden></pre>`;

    row.querySelector(".tpl-assign").onchange = async (ev) => {
      const tag = ev.target.value;
      if (!tag) return;
      try {
        await api(`/v1/templates/${encodeURIComponent(id)}/tags`, { method: "POST", body: { tag, version: v } });
        toast(`${tag} → v${v}`);
        reload();
      } catch (e) { toast(e.message, "error"); ev.target.value = ""; }
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

/** A typed form over the template's declared `params:`, plus a version selector. */
function renderTrigger(host, id, st, d) {
  const params = d.params || {};
  // Computed params are derived from other params, not supplied — exclude them
  // from the trigger form (supplying one is rejected server-side, #573).
  const names = Object.keys(params).filter((n) => params[n].computed == null);
  // Only offer channels that actually resolve — an unset one would just 422.
  const choices = ["stable", "newest", "previous", ...Object.keys(st.tags).sort()].filter(
    (c) => channelTarget(c, st) != null,
  );
  host.innerHTML = `
    <div class="tpl-trigger">
      <fieldset class="submit-opts">
        <label>version
          <select id="tg-version">
            ${choices.map((c) => `<option value="${escapeHtml(c)}">${escapeHtml(c)}${channelTarget(c, st) != null ? ` (v${channelTarget(c, st)})` : ""}</option>`).join("")}
            ${st.versions.map((v) => `<option value="${v}">v${v} (pinned)</option>`).join("")}
          </select>
        </label>
        <label>run name <input id="tg-name" placeholder="optional" /></label>
      </fieldset>
      <div id="tg-params" class="tpl-params"></div>
      <div class="submit-actions"><button id="tg-go" class="btn-primary">Run</button></div>
      <pre id="tg-out" class="submit-out" hidden></pre>
    </div>`;

  const paramHost = host.querySelector("#tg-params");
  if (!names.length) {
    paramHost.innerHTML = `<p class="tpl-desc">This template declares no parameters.</p>`;
  }
  for (const name of names) {
    const p = params[name] || {};
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
      </span>
      ${input}
      ${p.description ? `<span class="help">${mdInline(p.description)}</span>` : ""}`;
    paramHost.appendChild(field);
  }

  const out = host.querySelector("#tg-out");
  host.querySelector("#tg-go").onclick = async () => {
    const supplied = {};
    for (const el of paramHost.querySelectorAll("[data-name]")) {
      const raw = el.value;
      if (raw === "") continue; // omitted → the template's default (or a typed error)
      supplied[el.dataset.name] = coerce(raw, (params[el.dataset.name] || {}).type);
    }
    const body = { version: host.querySelector("#tg-version").value };
    if (Object.keys(supplied).length) body.params = supplied;
    const name = host.querySelector("#tg-name").value.trim();
    if (name) body.name = name;
    try {
      // The submit response is flattened into the trigger response, so `run_id`
      // and `status` sit at the top level alongside `template_version`.
      const resp = await api(`/v1/templates/${encodeURIComponent(id)}/runs`, { method: "POST", body });
      if (resp.deprecated) toast(`deprecated template: ${resp.deprecated}`, "error");
      toast(`run ${resp.run_id} from v${resp.template_version}`);
      navigate(`#/runs/${resp.run_id}`);
    } catch (e) {
      out.hidden = false;
      out.textContent = `✗ ${e.message}\n\n` + (e.details ? JSON.stringify(e.details, null, 2) : "");
      toast(e.message, "error");
    }
  };
}

/** The version a channel currently resolves to, or null when unset. */
function channelTarget(channel, st) {
  if (channel === "stable") return st.stable;
  if (channel === "previous") return st.previous;
  if (channel === "newest") return st.newest;
  const v = st.tags[channel];
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
    <table class="tbl">
      <thead><tr><th>#</th><th>version</th><th>when</th><th>by</th></tr></thead>
      <tbody>
        ${launches
          .map(
            (l) => `<tr><td class="mono">${l.seq}</td><td class="mono">v${l.version}</td>
              <td>${fmtTime(l.launched_at)}</td><td>${escapeHtml(l.launched_by || "cli")}</td></tr>`,
          )
          .join("")}
      </tbody>
    </table>`;
}

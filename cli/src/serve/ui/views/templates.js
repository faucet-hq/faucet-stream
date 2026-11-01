// Templates (#998): split pane organised around the lifecycle (registered →
// tested → launched → channels) and the source × sink pairing. Registry tools
// (overview, matrix, register, sync) live on the page shown when nothing is
// selected. Built to console-redesign/views/templates.js.
//
// The lifecycle model:
//   • registering a version never moves callers — it only extends the build list
//   • `launch` is the one step that moves `stable` (and so unpinned runs)
//   • `stable` / `previous` / `newest` are derived; `dev`…`prod` are assignable
//   • a version takes a label (launch, a channel) only when its tests pass
//
// URL state: list `lt` `q` `owner` `sort` `dep` `f`; template `tab` `version`
// `sink` `cmp=A..B` `cfg` `clean` `newver` `tr` `launch` `rollback`; registry
// `tab` `cell` `sowner` `stype` `kowner` `ktype`.
import { api } from "../api.js";
import {
  html, raw, join, ic, ring, row, sect, stats, tabs, dtabs, dhead, kv, grid, card, tbl, pill, chip, ch, tag,
  btn, ibtn, input, select, check, toggle, field, alert, empty, skelRows, skelCards, lnk, code, diff, head, steps,
  meter, meta, dotText, attrs,
} from "../components.js";
import { layout, setQuery, patch, errorAlert, hashFor, ago, dialog, confirmAction, toast, poll, sections } from "../page.js";
import { setCrumbs, setDelta, setContext } from "../shell.js";
import { navigate } from "../router.js";
import { fmtInt, mdInline, copyText } from "../utils.js";
import { href } from "../links.js";
import { can } from "../access.js";
import { withTenant } from "../tenant.js";
import { isSecretField } from "../secret-field.js";

/** Channels a person may point at a version. Derived ones are never assignable. */
export const ASSIGNABLE = ["dev", "test", "staging", "pre-prod", "canary", "prod"];
const KINDS = ["source-template", "sink-template", "deployment", "pipeline"];
const SECTIONS = [["source-template", "Source templates"], ["sink-template", "Sink templates"], ["deployment", "Deployments"], ["pipeline", "Pipelines"]];
const kindOf = (t) => (KINDS.includes(t && t.kind) ? t.kind : "pipeline");
const enc = encodeURIComponent;
const TMPL_MISSING = "The template endpoints are not available on this server: it was built without the `templates` feature.";
const unavailable = (e) => e && e.status === 404 && !e.code;
const plural = (n, w, ws = `${w}s`) => `${fmtInt(n)} ${n === 1 ? w : ws}`;
const TEST_TONE = { passing: "ok", failing: "bad", untested: "warn", no_tests: "muted", unknown: "bad" };
const TEST_WORD = { passing: "passed", failing: "failing", untested: "not run", no_tests: "no tests", unknown: "unreadable" };
const testCell = (s) => (s ? dotText(TEST_TONE[s] || "muted", TEST_WORD[s] || s) : html`<span class="muted">—</span>`);

// ── template state helpers ─────────────────────────────────────────────────────

/** The list summary's state, or the detail record flattened the same way. */
function stateOf(t) {
  if (t.state) return t.state;
  return { status: t.status, versions: t.versions || [], stable: t.stable, previous: t.previous, newest: t.newest, tags: t.tags || {}, test_status: t.test_status || {}, has_tests: t.has_tests, deprecation: t.deprecation, deprecated_versions: t.deprecated_versions || [] };
}
const testStatus = (st, ver) => (ver == null ? null : (st.test_status || {})[ver] || (st.test_status || {})[String(ver)] || null);
const retiredVersion = (st, ver) => (st.deprecated_versions || []).find((x) => x.version === ver) || null;
/** Whether a label may move onto a version: passing, or untested (its tests run first). */
const canTakeLabel = (s) => s === "passing" || s === "untested";
const nextVersion = (st) => (Math.max(0, ...(st.versions || [])) || 0) + 1;

/** Every channel (derived and assigned) that points at `ver`. */
function channelsFor(ver, st) {
  const out = [];
  if (st.stable === ver) out.push("stable");
  for (const [t, target] of Object.entries(st.tags || {})) if (target === ver) out.push(t);
  if (st.newest === ver) out.push("newest");
  if (st.previous === ver) out.push("previous");
  return out;
}
/** The version a selector resolves to (`stable`, `newest`, a channel, or a number). */
function resolveSel(sel, st) {
  if (sel == null || sel === "") return st.stable ?? st.newest;
  if (/^\d+$/.test(String(sel))) return Number(sel);
  if (sel === "stable") return st.stable;
  if (sel === "newest") return st.newest;
  if (sel === "previous") return st.previous;
  const t = (st.tags || {})[sel];
  return t === undefined ? null : t;
}
/** Options for a version picker: every channel that resolves, then each version pinned. */
function versionOptions(st) {
  const chans = ["stable", "newest", "previous", ...Object.keys(st.tags || {}).sort()].filter((c) => resolveSel(c, st) != null);
  return [
    ...chans.map((c) => [c, `${c} · v${resolveSel(c, st)}`]),
    ...[...(st.versions || [])].sort((a, b) => b - a).map((n) => [String(n), `v${n}${retiredVersion(st, n) ? " · deprecated" : ""}`]),
  ];
}
const defaultSel = (st) => (st.stable != null ? "stable" : "newest");

/** Ring, lead fact and tone of a template's list row. */
function rowState(t) {
  const st = stateOf(t);
  const nt = testStatus(st, st.newest);
  if (st.status === "deprecated") return { tone: "", shape: "hollow", lead: "deprecated" };
  if (nt === "failing" || nt === "unknown") return { tone: "bad", shape: "", lead: "tests failing" };
  if (st.status === "draft") return { tone: "warn", shape: "hollow", lead: "draft" };
  return { tone: "ok", shape: "", lead: `stable v${st.stable}` };
}
const datasetLabel = (uri) => {
  const s = String(uri || "");
  const m = s.match(/^[a-z0-9+.-]+:\/\/(.*)$/i);
  const parts = (m ? m[1] : s).split("/").filter(Boolean);
  return parts.slice(-2).join("/") || s;
};

/** A menu whose items may also be raw markup (`{raw}`), e.g. channel chips. */
function menuEl(summary, items, { title = "More actions" } = {}) {
  const body = items.filter(Boolean).map((it) => {
    if (it === "-") return html`<div class="msep"></div>`;
    if (it.raw) return it.raw;
    if (it.heading != null) return html`<div class="mlab">${it.heading}</div>`;
    const inner = html`${it.icon ? ic(it.icon) : ""}${it.label}${it.hint ? html`<span class="muted">${it.hint}</span>` : ""}`;
    const a = attrs({ "data-perm": it.perm || undefined, "data-act": it.action || undefined, ...Object.fromEntries(Object.entries(it.data || {}).map(([k, x]) => [`data-${k}`, x])) });
    if (it.href) return html`<a class="mi ${it.cls || ""}" href="${it.href}"${raw(a)}>${inner}</a>`;
    return html`<button type="button" class="mi ${it.cls || ""}"${raw(a)}${raw(it.cls === "dis" ? " disabled" : "")}>${inner}</button>`;
  });
  const sum = summary === "more"
    ? html`<summary class="btn" title="${title}" aria-label="${title}">${ic("more")}</summary>`
    : html`<summary class="btn sm" title="${title}" aria-label="${title}">${summary}</summary>`;
  return html`<details class="menu">${sum}<div class="pop" role="menu">${body}</div></details>`;
}

// ── page ──────────────────────────────────────────────────────────────────────

export async function renderTemplates(host, params) {
  const L = layout(host, "split");
  const C = {
    all: null, sync: [], listErr: null, matrix: null, matrixErr: null,
    details: new Map(), detailErr: new Map(), runs: new Map(),
    triggers: undefined, changes: null, regDetails: null,
  };
  let P = { id: params.id || "", q: params.query };
  const Q = (k) => P.q.get(k) || "";
  const runForms = new Map();
  const sects = sections("faucet.templates.sections");
  let alive = true;

  // ── data ───────────────────────────────────────────────────────────────
  async function loadList() {
    try {
      const data = await api("/v1/templates");
      C.all = data.templates || [];
      C.sync = (data.sync && data.sync.origins) || [];
      C.listErr = null;
    } catch (e) {
      C.listErr = e;
    }
  }
  async function loadMatrix(force = false) {
    if (C.matrix && !force) return C.matrix;
    try {
      C.matrix = await api("/v1/templates/matrix");
      C.matrixErr = null;
    } catch (e) {
      C.matrixErr = e;
    }
    return C.matrix;
  }
  async function loadDetail(id, force = false) {
    if (C.details.has(id) && !force) return C.details.get(id);
    try {
      const d = await api(`/v1/templates/${enc(id)}?version=newest`);
      C.details.set(id, d);
      C.detailErr.delete(id);
      return d;
    } catch (e) {
      C.detailErr.set(id, e);
      return null;
    }
  }
  async function loadTemplateRuns(id) {
    let runs;
    try {
      const r = await api(`/v1/templates/${enc(id)}/runs?limit=200`);
      runs = r.runs || r;
      if (!Array.isArray(runs)) throw new Error("unexpected shape");
    } catch {
      try {
        const p = withTenant(new URLSearchParams({ label: `template:${id}`, limit: "200" }));
        runs = (await api(`/v1/runs?${p}`)).runs || [];
      } catch {
        runs = [];
      }
    }
    C.runs.set(id, { runs });
    return runs;
  }
  async function loadTriggers() {
    if (C.triggers !== undefined) return C.triggers;
    try {
      const r = await api("/v1/triggers");
      C.triggers = Array.isArray(r) ? r : r.triggers || [];
    } catch {
      C.triggers = null;
    }
    return C.triggers;
  }
  async function loadChanges() {
    try {
      const r = await api(`/v1/changes?${withTenant(new URLSearchParams({ status: "pending", limit: "200" }))}`);
      C.changes = Array.isArray(r) ? r : r.changes || [];
    } catch {
      C.changes = [];
    }
  }
  async function loadRegistryDetails() {
    if (C.regDetails || !C.all) return;
    const ids = C.all.map((t) => t.id).slice(0, 60);
    const out = new Map();
    let i = 0;
    await Promise.all(Array.from({ length: 6 }, async () => {
      while (i < ids.length) {
        const id = ids[i++];
        const d = await loadDetail(id);
        if (d) out.set(id, d);
      }
    }));
    C.regDetails = out;
  }
  const triggersFor = (id) => (C.triggers || []).filter((t) => t.template && (t.template === id || t.template.id === id));
  const findT = (id) => (C.all || []).find((t) => t.id === id);
  const matrixSource = (id) => (C.matrix && (C.matrix.sources || []).find((s) => s.id === id)) || null;
  const matrixSink = (id) => (C.matrix && (C.matrix.sinks || []).find((s) => s.id === id)) || null;
  const cellOf = (src, snk) => (C.matrix && (C.matrix.matrix || []).find((c) => c.source === src && c.sink === snk)) || null;
  const fitOf = (c) => {
    const n = (c.streams || []).length;
    return { n, m: n + (c.incompatible || []).length };
  };
  const listQuery = () => ({ lt: Q("lt"), q: Q("q"), owner: Q("owner"), sort: Q("sort"), dep: Q("dep"), f: Q("f") });
  const tHref = (id, q = {}) => hashFor(href.template(id), { ...listQuery(), ...q });
  const regHref = (q = {}) => hashFor("#/templates", { ...listQuery(), ...q });

  // ── list pane ────────────────────────────────────────────────────────────
  function rowMeta(t) {
    const st = stateOf(t);
    const k = kindOf(t);
    const nt = testStatus(st, st.newest);
    const facts = [];
    if (st.status === "deprecated") {
      facts.push(st.deprecation && st.deprecation.reason ? st.deprecation.reason : `stable v${st.stable ?? "—"}`);
    } else if (nt === "failing" || nt === "unknown") {
      facts.push(`v${st.newest}`, "launch blocked");
    } else if (st.status === "draft") {
      facts.push("never launched", nt === "passing" ? "tests passed" : nt === "untested" ? "tests not run" : st.has_tests ? "" : "no tests");
    } else {
      for (const [tg, n] of Object.entries(st.tags || {})) facts.push(`${tg} v${n}`);
      if (st.newest != null && st.newest !== st.stable) facts.push(`newest v${st.newest}`);
    }
    if (st.status !== "deprecated") {
      if (k === "source-template") {
        const s = matrixSource(t.id);
        if (s) facts.push(plural((s.streams || []).length, "stream"));
      } else if (k === "sink-template") {
        const s = matrixSink(t.id);
        if (s) facts.push(...(s.write_modes || []));
      } else {
        const n = Object.keys(t.params || {}).length;
        if (n) facts.push(plural(n, "param"));
      }
      if (st.status === "launched" && nt === "passing" && k === "source-template") facts.push("tests passed");
    }
    return meta(rowState(t).lead, ...facts.filter(Boolean));
  }

  function filtered() {
    const lt = Q("lt");
    const q = Q("q").toLowerCase();
    const owner = Q("owner");
    const dep = Q("dep") === "1";
    const rows = (C.all || []).filter((t) => {
      const s = stateOf(t).status || "draft";
      if (s === "deprecated" && !dep) return false;
      if (lt === "launched" && s !== "launched") return false;
      if (lt === "draft" && s !== "draft") return false;
      if (owner && t.id.split("/")[0] !== owner) return false;
      if (q && !`${t.id} ${t.description || ""}`.toLowerCase().includes(q)) return false;
      return true;
    });
    if (Q("sort") === "updated") return rows.sort((a, b) => (Date.parse(b.created_at) || 0) - (Date.parse(a.created_at) || 0));
    return rows.sort((a, b) => a.id.localeCompare(b.id));
  }

  function listTools() {
    const fOn = Q("f") === "1";
    const active = !!(Q("q") || Q("owner") || Q("dep") || Q("sort"));
    return join([
      ibtn("filter", { on: fOn, dot: active && !fOn, title: "Search, owner and sort", data: { act: "filters" } }),
      ibtn("matrix", { title: "Compatibility matrix", href: regHref({ tab: "matrix" }) }),
      ibtn("sync", { title: "Sync from origins", href: regHref({ tab: "sync" }), perm: "template_admin" }),
      ibtn("plus", { title: "Register a template", href: regHref({ tab: "register" }), perm: "template_admin" }),
    ]);
  }

  function renderList() {
    const lt = Q("lt") || "";
    const top = tabs([["All", null, ""], ["Launched", null, "launched"], ["Draft", null, "draft"]], lt, listTools());
    if (C.all === null && !C.listErr) {
      patch(L.list, html`${top}${stats([["Templates", "—"], ["Launched", "—"], ["Draft", "—"], ["Failing", "—"]])}${skelRows(8)}`);
      return;
    }
    if (C.listErr && !C.all) {
      patch(L.list, html`${top}${unavailable(C.listErr) ? empty("Templates are not available", TMPL_MISSING, { icon: "tpl" }) : errorAlert("the templates", C.listErr)}`);
      return;
    }
    const all = C.all;
    const status = (t) => stateOf(t).status || "draft";
    const isFailing = (t) => ["failing", "unknown"].includes(testStatus(stateOf(t), stateOf(t).newest));
    const rows = filtered();
    const filtering = !!(Q("q") || Q("owner") || lt);
    const base = filtering ? rows : all;
    const failing = base.filter(isFailing).length;
    const statOn = lt === "launched" ? 1 : lt === "draft" ? 2 : -1;
    const strip = stats([
      [filtering ? "Matching" : "Templates", base.length],
      ["Launched", base.filter((t) => status(t) === "launched").length, "ok", "launched"],
      ["Draft", base.filter((t) => status(t) === "draft").length, "", "draft"],
      ["Failing", failing, failing ? "bad" : ""],
    ], statOn);
    const owners = [...new Set(all.map((t) => (t.id.includes("/") ? t.id.split("/")[0] : "")).filter(Boolean))].sort();
    const filters = Q("f") === "1"
      ? html`<div class="filters">${input(Q("q"), { ph: "Search id or description", icon: "search", type: "search", name: "q" })}<div class="r2">${select([["", "Owner · any"], ...owners.map((o) => [o, `Owner · ${o}`])], Q("owner"), { name: "owner" })}${select([["", "Sort · name"], ["updated", "Sort · last updated"]], Q("sort"), { name: "sort" })}</div>${check("Include deprecated", Q("dep") === "1", { name: "dep" })}</div>`
      : "";
    const chips = [];
    const fchip = (label, key) => chip(label, "x", { "data-clear": key, role: "button", tabindex: "0", title: "Remove filter" });
    if (lt) chips.push(fchip(lt, "lt"));
    if (Q("owner")) chips.push(fchip(`owner ${Q("owner")}`, "owner"));
    if (Q("q")) chips.push(fchip(`“${Q("q")}”`, "q"));
    if (Q("dep")) chips.push(fchip("with deprecated", "dep"));
    if (Q("sort")) chips.push(fchip("last updated", "sort"));
    const activeF = chips.length && (chips.length > 1 || Q("f") === "1")
      ? html`<div class="active-f">${chips}<a class="lnk" href="${hashFor(P.id ? href.template(P.id) : "#/templates", { tab: Q("tab"), f: Q("f") })}" style="font-size:12px;margin-left:4px">Clear</a></div>`
      : "";
    const body = rows.length
      ? join(SECTIONS.map(([k, label]) => {
          const items = rows.filter((t) => kindOf(t) === k);
          if (!items.length) return "";
          return html`${sect(label, items.length, { id: k, closed: sects.isClosed(k) })}<div class="sect-rows rows${sects.isClosed(k) ? " closed" : ""}">${items.map((t) => {
            const s = rowState(t);
            return row(ring(s.tone, s.shape), t.id, `v${stateOf(t).newest ?? t.version}`, rowMeta(t), { on: t.id === P.id, id: t.id, href: tHref(t.id), tip: `${t.id}${t.description ? ` — ${t.description}` : ""}` });
          })}</div>`;
        }))
      : all.length
        ? empty("No templates match", html`Nothing matches these filters. <a class="lnk" href="#/templates">Clear them</a>`, { icon: "search" })
        : empty("No templates yet", "Register one, or pull the catalogue from a template origin.", { icon: "tpl" });
    patch(L.list, html`${top}${filters}${activeF}${strip}${body}`);
  }

  L.list.addEventListener("click", (e) => {
    const t = e.target.closest("[data-tab]");
    if (t) return setQuery({ lt: t.dataset.tab });
    if (e.target.closest('[data-act="filters"]')) return setQuery({ f: Q("f") === "1" ? "" : "1" });
    const c = e.target.closest("[data-clear]");
    if (c) return setQuery({ [c.dataset.clear]: "" });
    const s = e.target.closest(".stats > div[data-q]");
    if (s) return setQuery({ lt: Q("lt") === s.dataset.q ? "" : s.dataset.q });
  });
  L.list.addEventListener("sect-toggle", (e) => { if (sects.isClosed(e.detail.id) !== e.detail.closed) sects.toggle(e.detail.id); L.list.__html = ""; });
  L.list.addEventListener("input", (e) => {
    if (e.target.name === "q") setQuery({ q: e.target.value }, { replace: true });
  });
  L.list.addEventListener("change", (e) => {
    if (e.target.name === "owner" || e.target.name === "sort") setQuery({ [e.target.name]: e.target.value });
    if (e.target.name === "dep") setQuery({ dep: e.target.checked ? "1" : "" });
  });

  // ── detail: shared pieces ─────────────────────────────────────────────────
  const backLink = () => ({ href: regHref(), label: "Templates" });

  function tabsFor(d) {
    const k = kindOf(d);
    const st = stateOf(d);
    return [
      ["Overview", null, "overview"],
      ["Versions", st.versions.length, "versions"],
      ["Tests", null, "tests"],
      (k === "source-template" || k === "pipeline") && can("run_write") ? ["Run", null, "run"] : null,
      k === "pipeline" ? null : [k === "deployment" ? "Use" : "Pairings", null, "pairings"],
      ["Activity", null, "activity"],
    ].filter(Boolean);
  }

  function gateInfo(d) {
    const st = stateOf(d);
    const cand = st.newest;
    const ts = testStatus(st, cand);
    const ok = cand != null && cand !== st.stable && !retiredVersion(st, cand) && st.status !== "deprecated" && canTakeLabel(ts);
    return { st, cand, ts, can: ok };
  }

  function moreMenu(d) {
    const st = stateOf(d);
    return menuEl("more", [
      { heading: d.id },
      can("template_admin") ? { label: "New version", icon: "plus", href: tHref(d.id, { tab: "versions", newver: "1" }) } : null,
      { label: "Copy id", icon: "copy", data: { copy: d.id } },
      { label: "Copy a link to this template", icon: "link", data: { copy: location.href } },
      can("template_admin") ? "-" : null,
      st.status === "deprecated"
        ? { label: "Revive template", icon: "undo", action: "revive", perm: "template_admin" }
        : { label: "Deprecate template", icon: "pause", action: "deprecate", perm: "template_admin" },
      { label: "Delete template", icon: "trash", cls: "danger", action: "delete-all", perm: "template_admin" },
    ]);
  }

  function headerActions(d) {
    const { st, cand, ts, can: canLaunch } = gateInfo(d);
    if (st.status === "deprecated") return html`${btn("Revive", { icon: "undo", perm: "template_admin", data: { act: "revive" } })}${moreMenu(d)}`;
    if (st.status === "draft" && !canLaunch && ts !== "no_tests") return html`${btn("Run tests", { kind: "primary", icon: "flask", perm: "template_admin", data: { act: "test", v: cand } })}${moreMenu(d)}`;
    return html`${st.previous != null ? btn("Roll back", { icon: "undo", perm: "template_admin", data: { act: "rollback" }, title: `re-launch v${st.previous}` }) : ""}${canLaunch ? btn(`Launch v${cand}`, { kind: "primary", icon: "rocket", perm: "template_admin", data: { act: "launch", v: cand } }) : ""}${moreMenu(d)}`;
  }

  function registeredBy(d, ver) {
    const r = (d.registrations || []).find((x) => x.version === ver);
    if (r) return { at: r.registered_at, by: r.registered_by || "" };
    if (ver === d.version) return { at: d.created_at, by: d.created_by || "" };
    return null;
  }
  const who = (name) => (name ? (/^(trigger|sync|system):/.test(name) || name === "anonymous" ? html`<b>${name}</b>` : lnk(name, href.audit({ principal: name }))) : "");

  function templateHead(d) {
    const st = stateOf(d);
    const k = kindOf(d);
    const status = st.status === "launched" ? pill("Launched", "ok", "check") : st.status === "draft" ? pill("Draft · never launched", "warn") : pill("Deprecated", "", "pause");
    let what;
    if (k === "source-template") {
      const s = matrixSource(d.id);
      what = `${s && s.source_type ? `${s.source_type} source` : "source template"}${s ? ` · ${plural((s.streams || []).length, "stream")}` : ""}`;
    } else if (k === "sink-template") {
      const s = matrixSink(d.id);
      what = `${s && s.sink_type ? `${s.sink_type} sink` : "sink template"}${s && (s.write_modes || []).length ? ` · ${s.write_modes.join(" · ")}` : ""}`;
    } else what = k === "deployment" ? "deployment overlay" : "pipeline";
    const sub = [status, html`<span>${what} · ${plural(Object.keys(d.params || {}).length, "param")}</span>`];
    const reg = registeredBy(d, st.newest);
    if (d.name && d.name !== d.id) sub.push(html`<span>config name <b>${d.name}</b></span>`);
    if (reg && reg.by) sub.push(html`<span>registered by ${who(reg.by)}</span>`);
    else if (reg) sub.push(html`<span>registered ${ago(reg.at, " ago")}</span>`);
    return head(d.id, sub, d.description || "");
  }

  function setTop(d) {
    setCrumbs(["Build", { label: "Templates", href: "#/templates" }, d.id]);
    setDelta(html`<span class="muted">${d.kind || "pipeline"}</span>`);
    const n = C.triggers ? triggersFor(d.id).length : 0;
    setContext(n ? html`<a class="chip" href="${href.triggers()}">${ic("trig")}${plural(n, "trigger")}</a>` : "");
  }

  // ── Overview tab ───────────────────────────────────────────────────────────
  function lifecycleCard(d) {
    const { st, cand, ts, can: canLaunch } = gateInfo(d);
    if (cand == null) return "";
    const tv = (d.tests && (d.tests.versions || []).find((x) => x.version === cand)) || null;
    const r = tv && tv.results && tv.results[0];
    const passed = r ? r.cases.filter((c) => c.passed).length : 0;
    const total = r ? r.cases.length : 0;
    const reg = registeredBy(d, cand);
    const lastLaunch = (d.launches || [])[0];
    const prodOn = (st.tags || {}).prod;
    const launched = cand === st.stable;
    const stepTests = ts === "passing" ? [`Tested · ${passed} of ${total}`, "done"] : ts === "failing" || ts === "unknown" ? ["Tests · failing", "bad"] : ts === "untested" ? ["Tests · not run", launched ? "" : "cur"] : ["Tests · none", ""];
    const stepGate = launched ? [`Launched · v${cand}`, "done"] : canLaunch ? ["Launch gate · open", "cur"] : ["Launch gate · blocked", ""];
    const chanNames = Object.entries(st.tags || {}).map(([t, n]) => `${t} on v${n}`);
    const stepChan = [chanNames.length ? `Channels · ${prodOn != null ? `prod on v${prodOn}` : chanNames[0]}` : "Channels · none", launched ? "cur" : ""];
    const gateWord = launched ? html`<span class="ok">launched</span>` : canLaunch ? html`<span class="ok">open</span>` : html`<span class="muted">${ts === "failing" ? "blocked" : "waiting"}</span>`;
    const gateWhy = launched ? `v${cand} is stable` : canLaunch ? (ts === "untested" ? "launching runs its tests first" : "tests pass on this version") : ts === "failing" ? `v${cand} fails its tests` : ts === "no_tests" ? "a version needs tests to launch" : "needs a passing result";
    let note;
    if (st.status === "deprecated") note = html`<span>This template is deprecated. Revive it to launch a version.</span>`;
    else if (launched) note = html`<span><span class="ok">v${cand} is stable.</span> Unpinned triggers resolve <b>v${cand}</b> from their next start.${prodOn != null && prodOn !== cand ? html` Next: move <b>prod</b> to v${cand} when it looks good.` : ""}</span>`;
    else if (canLaunch) note = html`<div class="tpl-next"><span><span class="ok">Next step: launch v${cand}.</span> ${st.stable != null ? html`Unpinned triggers keep resolving <b>v${st.stable}</b> until you do.` : "Until a version is launched, unpinned runs are refused."}</span>${btn(`Launch v${cand}`, { kind: "primary", icon: "rocket", perm: "template_admin", data: { act: "launch", v: cand } })}</div>`;
    else if (ts === "failing" || ts === "unknown") note = html`<div class="tpl-next"><span><span class="bad">Launch blocked:</span> v${cand} fails its tests.</span>${btn("See failures", { href: tHref(d.id, { tab: "tests" }) })}</div>`;
    else if (ts === "no_tests") note = html`<span><span class="warn">Launch blocked:</span> v${cand} carries no <code>tests:</code> block, and only a version whose tests pass can be launched.</span>`;
    else note = html`<div class="tpl-next"><span><b>Next step: run the tests.</b> A passing result opens the launch gate.</span>${btn("Run tests", { kind: "primary", icon: "flask", perm: "template_admin", data: { act: "test", v: cand } })}</div>`;
    return card({
      icon: "layers", title: `Lifecycle of v${cand}`, tag: "newest", aside: launched ? "" : `what it takes to make v${cand} the stable release`,
      right: reg ? html`registered ${ago(reg.at, " ago")}` : "",
      body: html`${steps([[`Registered · v${cand}`, "done"], stepTests, stepGate, stepChan])}
        <div class="tpl-ev">${kv("Registered", `v${cand}`, reg ? html`${ago(reg.at, " ago")}${reg.by ? html` by ${who(reg.by)}` : ""}` : "")}${kv("Tests", ts === "passing" ? `${passed} / ${total} passed` : ts === "failing" ? html`<span class="bad">${total - passed} of ${total} failing</span>` : html`<span class="muted">${ts === "no_tests" ? "no tests" : "not run"}</span>`, r ? `same body sha · faucet ${r.faucet_version}` : "")}${kv("Launch gate", gateWord, gateWhy)}${kv("Stable today", st.stable != null ? `v${st.stable}` : "—", lastLaunch ? html`launched ${ago(lastLaunch.launched_at, " ago")} by ${lastLaunch.launched_by || "cli"}` : "none launched")}</div>`,
      note,
    });
  }

  function channelsCard(d) {
    const st = stateOf(d);
    const launches = d.launches || [];
    const sinceOf = (ver) => { const l = launches.find((x) => x.version === ver); return l ? ago(l.launched_at, " ago") : ""; };
    const rows = [];
    if (st.stable != null) rows.push(["stable", ch(`v${st.stable}`, true), sinceOf(st.stable)]);
    for (const [t, n] of Object.entries(st.tags || {}).sort()) rows.push([t, ch(`v${n}`), ""]);
    if (st.newest != null) rows.push(["newest", ch(`v${st.newest}`), ""]);
    if (st.previous != null) rows.push(["previous", ch(`v${st.previous}`), sinceOf(st.previous)]);
    const unassigned = ASSIGNABLE.filter((c) => !(c in (st.tags || {})));
    if (unassigned.length) rows.push([unassigned.join(" · "), html`<span class="faint">unassigned</span>`, ""]);
    return card({ title: "Channels", aside: "what each name resolves to", right: btn("Assign", { cls: "sm", icon: "plus", href: tHref(d.id, { tab: "versions" }), perm: "template_admin" }), body: tbl(["Channel", "Version", "Since"], rows) });
  }

  function paramsCard(d) {
    const ps = Object.entries(d.params || {});
    return card({ icon: "sliders", title: "Parameters", right: plural(ps.length, "param"), body: ps.length ? tbl(["Name", "Type", "Default"], ps.map(([n, p]) => [html`<span class="mono">${n}</span>${p.required ? html` <span class="req" title="required"></span>` : ""}${p.secret ? html` ${tag("secret")}` : ""}${p.description ? html`<div class="muted" style="font-size:12px">${raw(mdInline(p.description))}</div>` : ""}`, pill(p.type || "string"), p.default != null ? html`<code>${String(p.default)}</code>` : html`<span class="faint">—</span>`])) : html`<div class="empty-line">This template declares no parameters.</div>` });
  }

  function pairsCard(d) {
    const k = kindOf(d);
    if (k !== "source-template" && k !== "sink-template") return paramsCard(d);
    if (!C.matrix) return card({ icon: "matrix", title: "Pairs with", body: C.matrixErr ? html`<div class="empty-line">${C.matrixErr.message}</div>` : html`<div class="pad">${skelRows(3)}</div>` });
    const src = k === "source-template";
    const cells = (C.matrix.matrix || []).filter((c) => (src ? c.source === d.id : c.sink === d.id));
    const rows = cells.slice(0, 6).map((c) => {
      const f = fitOf(c);
      const tone = f.n === f.m ? "ok" : f.n ? "warn" : "bad";
      const other = src ? c.sink : c.source;
      const why = (c.incompatible || []).length ? `${(c.incompatible || []).length} cannot run` : [...new Set((c.streams || []).map((s) => s.write_mode))].join(" · ");
      return [lnk(other, src ? tHref(other) : tHref(other, { tab: "run", sink: d.id })), html`<span class="${tone}">${f.n} / ${f.m}</span>`, why];
    });
    return card({ icon: "matrix", title: "Pairs with", aside: "source × sink", right: lnk(cells.length > 6 ? `all ${cells.length} pairings` : "all pairings", tHref(d.id, { tab: "pairings" })), body: rows.length ? tbl([src ? "Sink" : "Source", "Streams", ""], rows) : html`<div class="empty-line">No ${src ? "sink" : "source"} template is registered yet.</div>` });
  }

  function overviewTab(d) {
    const st = stateOf(d);
    const parts = [];
    if (st.status === "draft") parts.push(alert("Unpinned runs and triggers are refused until a version is launched.", html`Pin a version (for example <code>v${st.newest}</code>) to try it, or run its tests and launch it.`, "warn"));
    if (st.status === "deprecated") {
      const dep = st.deprecation || d.deprecation || {};
      parts.push(alert(html`Deprecated${dep.deprecated_at ? html` ${ago(dep.deprecated_at, " ago")}` : ""}${dep.deprecated_by ? ` by ${dep.deprecated_by}` : ""}${dep.reason ? `: ${dep.reason}.` : "."}`, html`Unpinned runs still resolve stable <b>v${st.stable ?? "—"}</b> and warn. Deleting the template stops them.`, ""));
      if (C.triggers) {
        const trig = triggersFor(d.id);
        parts.push(card({ title: "Still in use", right: plural(trig.length, "trigger"), body: trig.length ? tbl(["Trigger", "Resolves", "Last fire"], trig.map((t) => [lnk(t.name, href.trigger(t.name)), t.template && t.template.version ? `pinned ${t.template.version}` : `stable → v${st.stable}`, t.last_fire ? ago(t.last_fire, " ago") : "—"])) : html`<div class="empty-line">No trigger uses it: it is safe to delete.</div>` }));
      }
    }
    const k = kindOf(d);
    const second = k === "deployment" || k === "pipeline" ? paramsCard(d) : pairsCard(d);
    if (st.status === "launched") return html`${parts}${lifecycleCard(d)}<div class="pair">${channelsCard(d)}${second}</div>`;
    const versionsMini = card({ title: "Versions", right: fmtInt(st.versions.length), body: tbl(["Version", "Channels", "Tests", "Registered"], [...st.versions].sort((a, b) => b - a).slice(0, 5).map((n) => {
      const reg = registeredBy(d, n);
      return { href: tHref(d.id, { tab: "versions" }), cells: [html`<span class="mono${retiredVersion(st, n) ? " faint" : ""}">v${n}</span>`, html`<div class="tpl-chips">${channelsFor(n, st).map((c) => ch(c, c === "stable"))}${retiredVersion(st, n) ? tag("deprecated") : ""}</div>`, testCell(testStatus(st, n)), reg ? html`${ago(reg.at, " ago")}${reg.by ? ` · ${reg.by}` : ""}` : "—"] };
    })) });
    return html`${parts}${st.status === "deprecated" ? "" : lifecycleCard(d)}${versionsMini}${second}`;
  }

  // ── Versions tab ───────────────────────────────────────────────────────────
  function versionsTab(d) {
    if (Q("newver") === "1") return newVersionView(d);
    if (Q("cmp")) return compareView(d);
    const st = stateOf(d);
    const id = d.id;
    const rr = C.runs.get(id);
    const runs7 = new Map();
    if (rr) {
      const since = Date.now() - 7 * 86400e3;
      for (const r of rr.runs) {
        if (Date.parse(r.submitted_at) < since) continue;
        const vv = String((r.labels || {}).template_version ?? "");
        runs7.set(vv, (runs7.get(vv) || 0) + 1);
      }
    }
    const runnable = ["source-template", "pipeline"].includes(kindOf(d));
    const admin = can("template_admin");
    const rows = [...st.versions].sort((a, b) => b - a).map((n) => {
      const retired = retiredVersion(st, n);
      const ts = testStatus(st, n);
      const tv = (d.tests && (d.tests.versions || []).find((x) => x.version === n)) || null;
      const reg = registeredBy(d, n);
      const chans = channelsFor(n, st);
      const blocked = !canTakeLabel(ts);
      const cmpTo = st.stable != null && st.stable !== n ? st.stable : st.newest !== n ? st.newest : null;
      const items = [
        { heading: `v${n}${chans.length ? ` · ${chans.join(" · ")}` : ""}` },
        runnable && can("run_write") && !retired ? { label: `Run v${n}`, icon: "play", href: tHref(id, { tab: "run", version: String(n) }) } : null,
        tv && tv.has_tests && admin ? { label: "Run tests", icon: "flask", hint: tv.result_at ? html`last ${tv.status === "passing" ? "passed" : "ran"} ${ago(tv.result_at, " ago")}` : "", action: "test", data: { v: n } } : null,
        cmpTo != null ? { label: `Compare with ${cmpTo === st.stable ? "stable " : ""}v${cmpTo}`, icon: "diff", href: tHref(id, { tab: "versions", cmp: `${Math.min(cmpTo, n)}..${Math.max(cmpTo, n)}` }) } : null,
        { label: "Show config", icon: "code", href: tHref(id, { tab: "versions", cfg: String(n) }) },
        { label: "Show clean config", icon: "code", href: tHref(id, { tab: "versions", cfg: String(n), clean: "1" }) },
        admin ? "-" : null,
        admin ? { heading: blocked ? "Assign channel · its tests must pass" : "Assign channel" } : null,
        admin ? { raw: html`<div class="tpl-assign">${ASSIGNABLE.map((c) => html`<button type="button" class="ch${(st.tags || {})[c] === n ? " stable" : ""}" data-act="assign" data-tag="${c}" data-v="${n}" title="${(st.tags || {})[c] === n ? `${c} is on v${n}` : `point ${c} at v${n}`}"${raw(blocked ? " disabled" : "")}>${c}</button>`)}</div>` } : null,
        admin ? "-" : null,
        admin && st.stable !== n && !retired && !blocked && st.status !== "deprecated" ? { label: `Launch v${n}`, icon: "rocket", action: "launch", data: { v: n } } : null,
        admin ? { label: retired ? `Revive v${n}` : `Deprecate v${n}`, icon: retired ? "undo" : "pause", action: retired ? "vrevive" : "vdeprecate", data: { v: n } } : null,
        admin ? { label: `Delete v${n}`, icon: "trash", cls: "danger", action: "vdelete", data: { v: n } } : null,
      ];
      return {
        cls: Q("cfg") === String(n) ? "sel" : "",
        cells: [
          html`<span class="mono${retired ? " faint" : ""}">v${n}</span>`,
          html`<div class="tpl-chips">${chans.map((c) => ch(c, c === "stable"))}${retired ? tag("deprecated") : ""}</div>`,
          html`<span title="${tv && tv.reason ? tv.reason : ""}">${testCell(ts)}</span>`,
          reg ? html`${ago(reg.at, " ago")}${reg.by ? ` · ${reg.by}` : ""}` : html`<span class="muted">—</span>`,
          rr ? `${fmtInt(runs7.get(String(n)) || 0)}${rr.runs.length >= 200 ? "+" : ""}` : html`<span class="faint">—</span>`,
          html`<span class="tpl-vact">${runnable && !retired ? btn("Run", { cls: "sm", icon: "play", href: tHref(id, { tab: "run", version: String(n) }), perm: "run_write" }) : ""}${menuEl(ic("more"), items, { title: `Actions for v${n}` })}</span>`,
        ],
      };
    });
    const sorted = [...st.versions].sort((a, b) => a - b);
    const cmpDefault = st.stable != null && st.newest !== st.stable ? `${st.stable}..${st.newest}` : sorted.length > 1 ? sorted.slice(-2).join("..") : "";
    const versionsCard = card({
      title: "Versions", aside: "newest first",
      right: html`${cmpDefault ? btn("Compare", { cls: "sm", icon: "diff", href: tHref(id, { tab: "versions", cmp: cmpDefault }) }) : ""}${btn("New version", { cls: "sm", icon: "plus", href: tHref(id, { tab: "versions", newver: "1" }), perm: "template_admin" })}`,
      body: html`<div class="tpl-vtbl">${tbl(["Version", "Channels", "Tests", "Registered", ["Runs (7d)", "num"], ["", "num"]], rows)}</div>`,
    });
    const cfg = Q("cfg") ? configCard(d, Number(Q("cfg")), Q("clean") === "1") : "";
    return html`${versionsCard}${cfg}${launchHistory(d)}`;
  }

  function launchHistory(d) {
    const ls = d.launches || [];
    return card({ title: "Launch history", right: ls.length ? plural(ls.length, "launch", "launches") : "", body: ls.length ? html`<div class="log">${ls.map((l) => html`<div>${ago(l.launched_at, " ago")}&nbsp; v${l.version} launched by <b>${l.launched_by || "cli"}</b>${l.tests_skipped ? html` · <span class="warn">tests skipped: ${l.tests_skipped}</span>` : l.evidence ? " · tests passed" : ""}</div>`)}</div>` : html`<div class="empty-line">Never launched.</div>` });
  }

  // Version bodies: key → { p: promise, done?: {body, format} | {withheld} }
  const bodies = new Map();
  function bodyOf(id, n, clean) {
    const key = `${id}@${n}@${clean ? 1 : 0}`;
    if (!bodies.has(key)) {
      const ent = {};
      ent.p = api(`/v1/templates/${enc(id)}?version=${n}${clean ? "&clean=true" : ""}`).then(
        (r) => { ent.done = r.body_withheld ? { withheld: true } : { body: r.body || "", format: r.format }; return ent.done; },
        (e) => { bodies.delete(key); throw e; },
      );
      bodies.set(key, ent);
    }
    return bodies.get(key);
  }
  const whenBody = (ents) => Promise.all(ents.map((x) => x.p)).then(() => alive && renderDetail(), (e) => toast(e.message, "bad"));

  function configCard(d, n, clean) {
    const ent = bodyOf(d.id, n, clean);
    let body = html`<div class="pad">${skelRows(3)}</div>`;
    if (ent.done) body = ent.done.withheld ? html`<div class="empty-line">Config withheld: only an unscoped admin can read a template’s stored config.</div>` : code(ent.done.body, { box: false, lang: ent.done.format === "json" ? "json" : "yaml" });
    else whenBody([ent]);
    return card({ icon: "code", title: `${clean ? "Clean config" : "Config"} of v${n}`, aside: clean ? "comments stripped, canonical YAML" : "as registered", right: html`${toggle("Clean", clean, { name: "cfg-clean" })}${lnk("close", tHref(d.id, { tab: "versions" }))}`, body });
  }

  function lineDiff(a, b) {
    const A = a.split("\n"), B = b.split("\n");
    const n = A.length, m = B.length;
    if (n * m > 4e6) return [["@", "these versions are too large to compare here"]];
    const T = Array.from({ length: n + 1 }, () => new Uint32Array(m + 1));
    for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) T[i][j] = A[i] === B[j] ? T[i + 1][j + 1] + 1 : Math.max(T[i + 1][j], T[i][j + 1]);
    const out = [];
    let i = 0, j = 0;
    while (i < n && j < m) {
      if (A[i] === B[j]) { out.push(["", A[i]]); i++; j++; } else if (T[i + 1][j] >= T[i][j + 1]) out.push(["-", A[i++]]); else out.push(["+", B[j++]]);
    }
    while (i < n) out.push(["-", A[i++]]);
    while (j < m) out.push(["+", B[j++]]);
    const folded = [];
    for (let k = 0; k < out.length; k++) {
      if (out[k][0] !== "") { folded.push(out[k]); continue; }
      if (out.slice(Math.max(0, k - 2), k + 3).some((x) => x[0] !== "")) folded.push(out[k]);
      else if (!folded.length || folded[folded.length - 1][0] !== "@") folded.push(["@", "⋯"]);
    }
    return folded;
  }

  function compareView(d) {
    const st = stateOf(d);
    const [a, b] = Q("cmp").split("..").map(Number);
    const clean = Q("clean") !== "0";
    const opts = [...st.versions].sort((x, y) => y - x).map((n) => [String(n), `v${n}${channelsFor(n, st).length ? ` · ${channelsFor(n, st)[0]}` : ""}`]);
    const ea = bodyOf(d.id, a, clean), eb = bodyOf(d.id, b, clean);
    let body = html`<div class="pad">${skelRows(4)}</div>`;
    let note = "";
    let right = "";
    if (ea.done && eb.done) {
      if (ea.done.withheld || eb.done.withheld) body = html`<div class="empty-line">Config withheld: only an unscoped admin can read stored template configs.</div>`;
      else {
        const lines = lineDiff(ea.done.body, eb.done.body);
        const plus = lines.filter((x) => x[0] === "+").length, minus = lines.filter((x) => x[0] === "-").length;
        right = html`<span class="muted">+${plus} −${minus} lines</span>${btn(`Copy v${b}`, { cls: "sm", icon: "copy", data: { copy: eb.done.body } })}`;
        body = plus + minus ? diff(lines) : html`<div class="empty-line">v${a} and v${b} are identical${clean ? " once comments and order are ignored" : ""}.</div>`;
        const names = (op) => lines.filter((x) => x[0] === op && /^\s*-\s*name:/.test(x[1])).map((x) => x[1].split("name:")[1].trim());
        const added = names("+"), removed = names("-");
        note = plus + minus ? html`<b>What changed:</b> ${added.length ? `${plural(added.length, "stream")} added (${added.join(", ")}). ` : ""}${removed.length ? `${plural(removed.length, "stream")} removed (${removed.join(", ")}). ` : ""}${plural(plus, "line")} added and ${plural(minus, "line")} removed in v${b}.` : "";
      }
    } else whenBody([ea, eb]);
    return card({
      icon: "diff", title: "Compare versions", right: lnk("back to versions", tHref(d.id, { tab: "versions" })),
      body: html`<div class="tpl-bar">${select(opts, String(a), { name: "cmp-a" })}<span class="muted">→</span>${select(opts, String(b), { name: "cmp-b" })}${toggle("Clean config (comments and order ignored)", clean, { name: "cmp-clean" })}<span class="r">${right}</span></div>${body}`,
      note,
    });
  }

  const nv = { id: "", text: "", desc: "", seeded: false, err: null, busy: false };
  function newVersionView(d) {
    const st = stateOf(d);
    const nn = nextVersion(st);
    const ent = bodyOf(d.id, st.newest, false);
    let editor = html`<div class="pad">${skelRows(4)}</div>`;
    if (ent.done) {
      if (!nv.seeded || nv.id !== d.id) Object.assign(nv, { text: ent.done.withheld ? "" : ent.done.body, seeded: true, id: d.id, desc: d.description || "", err: null });
      editor = html`<div class="form">${field("Id", input(d.id, { mono: true, cls: "dis", a: { readonly: true } }), { help: "a new version keeps the id; register a new template to change it" })}${field("Description", input(nv.desc, { name: "nv-desc" }))}
        <div class="field wide tpl-ed"><label>Body</label><textarea class="area mono" name="nv-body" spellcheck="false" rows="18">${nv.text}</textarea></div></div>
        ${nv.err ? html`<div style="padding:0 16px 14px">${registerError(nv.err)}</div>` : ""}`;
    } else whenBody([ent]);
    return html`${head(`New version of ${d.id}`, [pill(`v${nn} · draft`), html`<span>seeded from newest v${st.newest}</span>`])}
      ${card({ icon: "edit", title: "Document", tag: "id locked", right: lnk("back to versions", tHref(d.id, { tab: "versions" })), body: editor, note: html`<span class="muted">Registering appends v${nn} and moves nobody: ${st.stable != null ? `stable stays v${st.stable}` : "nothing is launched"} until you launch. A version with a <code>tests:</code> block is tested as it registers.</span>` })}`;
  }

  function registerError(e) {
    const m = String(e.message || e);
    const line = m.match(/line (\d+)/i);
    const details = e.details ? JSON.stringify(e.details, null, 2) : "";
    return alert(line ? `Line ${line[1]} · couldn’t register.` : "Couldn’t register.", html`${m}${details ? code(details, { lang: "json", copy: false }) : ""}`, "bad");
  }

  // ── Tests tab ───────────────────────────────────────────────────────────────
  function testsTab(d) {
    const { st, cand, ts } = gateInfo(d);
    const tv = (d.tests && (d.tests.versions || []).find((x) => x.version === cand)) || null;
    const r = tv && tv.results && tv.results[0];
    let gate = "";
    if (cand != null && !r) {
      gate = card({ icon: "flask", title: ts === "no_tests" ? `v${cand} has no test bundle` : `v${cand} · no test result yet`, right: tv && tv.has_tests ? btn("Run tests", { cls: "sm", icon: "play", perm: "template_admin", data: { act: "test", v: cand } }) : "", body: html`<div class="empty-line">${tv && tv.reason ? raw(mdInline(tv.reason)) : ts === "no_tests" ? "Add a tests: block (fixtures, a parameter sweep or a shared suite). Only a version whose tests pass can be launched or take a channel." : `No result recorded for v${cand} under this faucet version. Launching it runs its tests first.`}</div>` });
    } else if (r) {
      const passed = r.cases.filter((c) => c.passed).length;
      const byOrigin = new Map();
      for (const c of r.cases) { const k = c.origin || c.source || "case"; const e = byOrigin.get(k) || [0, 0]; e[0] += c.passed ? 1 : 0; e[1] += 1; byOrigin.set(k, e); }
      const items = [...byOrigin].slice(0, 2).map(([k, [p, n]]) => kv(`${k[0].toUpperCase()}${k.slice(1)} cases`, `${p} / ${n}`));
      items.push(kv("Shared suites", (r.suites || []).length ? r.suites.map((s) => `${s.name}${s.release ? `@${s.release}` : ""}`).join(", ") : "none"));
      items.push(kv("Pairings checked", (r.companions || []).length ? plural(r.companions.length, "companion") : "none", (r.companions || []).join(" · ")));
      const open = ts === "passing" && cand !== st.stable;
      gate = card({
        icon: "flask", title: `v${cand} ${r.passed ? "passed" : "failed"} ${passed} of ${r.cases.length} cases`, tag: `sha ${r.body_sha256.slice(0, 8)}`, aside: `faucet ${r.faucet_version}`,
        right: html`tested ${ago(r.recorded_at, " ago")}${tv.has_tests ? btn("Run tests", { cls: "sm", icon: "play", perm: "template_admin", data: { act: "test", v: cand } }) : ""}`,
        body: grid(4, items.slice(0, 4)),
        note: cand === st.stable ? html`<span class="ok">v${cand} is stable.</span> A result counts only for the same body and the running faucet major.` : open ? html`<span class="ok">Launch gate open</span> · v${cand} can become stable. A result counts only for the same body and the running faucet major.` : ts === "untested" ? html`<span class="warn">No result under this faucet version</span> · that one was recorded on faucet ${r.faucet_version}; launching v${cand} runs its tests again first.` : html`<span class="bad">Launch gate closed</span> · v${cand} ${ts === "failing" ? "fails its tests" : "has no passing result"}.`,
      });
    }
    const all = [];
    for (const vt of (d.tests && d.tests.versions) || []) for (const res of vt.results || []) all.push({ ...res, version: vt.version });
    all.sort((x, y) => Date.parse(y.recorded_at) - Date.parse(x.recorded_at));
    const sel = Q("tr") || (all.find((x) => !x.passed) || {}).result_id;
    const rows = [];
    for (const res of all) {
      const failed = res.cases.filter((c) => !c.passed);
      const retried = res.cases.filter((c) => c.attempts > 1).length;
      const open = res.result_id === sel && (failed.length || res.error);
      rows.push({ cls: open ? "sel" : "", id: res.result_id, cells: [ago(res.recorded_at, " ago"), html`<span class="mono">v${res.version}</span>`, res.passed ? dotText("ok", "passed") : dotText("bad", "failed"), `${res.cases.length - failed.length} / ${res.cases.length}`, fmtInt(retried), res.faucet_version, res.recorded_by || "cli"] });
      if (open) rows.push({ cls: "tpl-sub", span: html`<ul>${res.error ? html`<li>${res.error}</li>` : ""}${failed.map((c) => html`<li><span class="mono">${c.name}</span>${c.error || c.message ? `: ${c.error || c.message}` : ""}</li>`)}</ul>` });
    }
    const runsCard = card({ title: "Test runs", aside: "all versions · click a failed run for its cases", right: plural(all.length, "run"), body: all.length ? html`<div class="tpl-trtbl">${tbl(["When", "Version", "Result", ["Cases", "num"], ["Retried", "num"], "Faucet", "By"], rows)}</div>` : html`<div class="empty-line">No test has run on any version yet.</div>` });
    return html`${gate}${runsCard}`;
  }

  // ── Run tab ─────────────────────────────────────────────────────────────────
  function runState(d) {
    if (!runForms.has(d.id)) runForms.set(d.id, { version: defaultSel(stateOf(d)), sink: "", sinkVersion: "", overlay: "", overlayVersion: "", name: "", params: {}, touched: new Set(), tried: false, rows: null, rowsErr: null, picked: null, parents: "eligible", preview: null, previewErr: null, allow: false, declared: null, declaredErr: null, busy: false, submitErr: null, key: "", qv: "", qs: "" });
    return runForms.get(d.id);
  }
  const sinkList = () => (C.all || []).filter((t) => kindOf(t) === "sink-template" && stateOf(t).status !== "deprecated");
  const overlayList = () => (C.all || []).filter((t) => kindOf(t) === "deployment" && stateOf(t).status !== "deprecated");

  const paramCache = new Map();
  function paramsOf(tid, version) {
    const key = `${tid}@${version}`;
    if (!paramCache.has(key)) paramCache.set(key, api(`/v1/templates/${enc(tid)}?version=${enc(version)}`).then((r) => r.params || {}, (e) => { paramCache.delete(key); throw e; }));
    return paramCache.get(key);
  }
  function rowsQuery(d, R) {
    const q = new URLSearchParams({ version: R.version });
    if (kindOf(d) === "source-template") {
      if (R.sink) { q.set("sink", R.sink); if (R.sinkVersion) q.set("sink_version", R.sinkVersion); }
      if (R.overlay) { q.set("overlay", R.overlay); if (R.overlayVersion) q.set("overlay_version", R.overlayVersion); }
    }
    return q;
  }

  /** (Re)load the declared params and the rows for the form's current picks. */
  async function refreshRunInputs(d, R) {
    const isSource = kindOf(d) === "source-template";
    const key = JSON.stringify([R.version, R.sink, R.sinkVersion, R.overlay, R.overlayVersion]);
    if (R.key === key) return;
    R.key = key;
    try {
      const secs = [["source", d.id, await paramsOf(d.id, R.version)]];
      if (isSource && R.sink) secs.push(["sink", R.sink, await paramsOf(R.sink, R.sinkVersion || "stable")]);
      if (isSource && R.overlay) secs.push(["overlay", R.overlay, await paramsOf(R.overlay, R.overlayVersion || "stable")]);
      if (R.key !== key) return;
      const merged = {};
      for (const [from, tid, ps] of secs) for (const [n, p] of Object.entries(ps)) if (p.computed == null && !(n in merged)) merged[n] = { ...p, from, tid };
      R.declared = merged;
      R.declaredErr = null;
    } catch (e) {
      if (R.key !== key) return;
      R.declaredErr = e;
    }
    if (alive) renderDetail();
    if (isSource && !R.sink) return;
    try {
      const rep = await api(`/v1/templates/${enc(d.id)}/rows?${rowsQuery(d, R)}`);
      if (R.key !== key) return;
      R.rows = rep.selectable ? rep.rows || [] : [];
      R.rowsErr = null;
      const ok = new Set(R.rows.filter((r) => !(r.write && r.write.supported === false)).map((r) => r.id));
      R.picked = R.picked ? new Set([...R.picked].filter((x) => ok.has(x))) : new Set(R.rows.filter((r) => r.default_selected && ok.has(r.id)).map((r) => r.id));
    } catch (e) {
      if (R.key !== key) return;
      R.rows = [];
      R.rowsErr = e;
    }
    if (alive) renderDetail();
    refreshPreview(d, R);
  }

  async function refreshPreview(d, R) {
    if (!R.rows || R.rows.length < 2 || !R.picked || !R.picked.size) { R.preview = null; return; }
    const q = rowsQuery(d, R);
    q.set("state", "false");
    q.set("select", [...R.picked].join(","));
    q.set("include_parents", R.parents);
    const seq = (R.pseq = (R.pseq || 0) + 1);
    try {
      const rep = await api(`/v1/templates/${enc(d.id)}/rows?${q}`);
      if (seq !== R.pseq) return;
      R.preview = rep;
      R.previewErr = rep.error || null;
    } catch (e) {
      if (seq !== R.pseq) return;
      R.previewErr = e.message;
    }
    if (alive) renderDetail();
  }

  function paramError(p, val0) {
    const val = String(val0 ?? "");
    if (val === "" || val === "false" && p.type === "bool") return p.required && p.default == null && val === "" ? "required" : "";
    if (p.type === "int" && !/^\s*-?\d+\s*$/.test(val)) return "must be a whole number";
    if (p.type === "float" && !Number.isFinite(Number(val.trim() || "x"))) return "must be a number";
    if (p.values && !p.values.map(String).includes(val)) return `must be one of ${p.values.join(", ")}`;
    return "";
  }
  const coerce = (val, type) => (type === "int" || type === "float" ? String(val).trim() : type === "bool" ? val === true || val === "true" : val);
  const secretParam = (n, p) => !!(p && p.secret) || isSecretField(n, "");

  function runBody(d, R, { mask = false } = {}) {
    const body = { version: R.version };
    if (kindOf(d) === "source-template") {
      body.sink = R.sink;
      body.sink_version = R.sinkVersion || "stable";
      if (R.overlay) { body.overlay = R.overlay; body.overlay_version = R.overlayVersion || "stable"; }
    }
    if (R.name.trim()) body.name = R.name.trim();
    const ps = {};
    for (const [n, p] of Object.entries(R.declared || {})) {
      const val = R.params[n];
      if (val === undefined || val === "") continue;
      ps[n] = mask && secretParam(n, p) ? "••••" : coerce(val, p.type);
    }
    if (Object.keys(ps).length) body.params = ps;
    if (R.rows && R.rows.length > 1 && R.picked) {
      const defaults = R.rows.filter((r) => r.default_selected).map((r) => r.id);
      const picked = [...R.picked];
      const isDefault = picked.length === defaults.length && defaults.every((x) => R.picked.has(x));
      if (!isDefault || R.parents !== "off") body.selection = { select: picked, include_parents: R.parents };
    }
    if (R.allow) body.allow_untested = true;
    return body;
  }
  function toYamlish(o, ind = "") {
    return Object.entries(o).map(([k, x]) => (x && typeof x === "object" && !Array.isArray(x)
      ? `${ind}${k}:\n${toYamlish(x, `${ind}  `)}`
      : `${ind}${k}: ${Array.isArray(x) ? `[${x.join(", ")}]` : typeof x === "string" ? (/^[\w./:@-]+$/.test(x) ? x : JSON.stringify(x)) : String(x)}`)).join("\n");
  }

  function untestedGate(d, R) {
    const st = stateOf(d);
    const n = resolveSel(R.version, st);
    if (n == null || st.stable == null) return null;
    const labelled = st.stable === n || st.newest === n || Object.values(st.tags || {}).includes(n);
    const ts = testStatus(st, n);
    if (labelled || !ts || ts === "passing") return null;
    return { n, ts };
  }

  function untestedAlert(d, R) {
    const g = untestedGate(d, R);
    if (!g) return "";
    return alert(`v${g.n} has no passing test result on this server’s faucet.`, html`Its tests are ${TEST_WORD[g.ts] || g.ts}. You can run it anyway; the run is labelled <code>allow_untested</code> and audited.<div class="btns">${check("Run anyway", R.allow, { name: "r-allow" })}</div>`, "warn");
  }

  function runTab(d) {
    const st = stateOf(d);
    const R = runState(d);
    const isSource = kindOf(d) === "source-template";
    const sinks = sinkList();
    if (isSource && !sinks.length) return alert("No sink template is registered.", html`A source template runs composed with a <b>sink template</b>. Register one (<code>kind: sink-template</code>) and come back.`, "warn");
    if (Q("version") && R.qv !== Q("version")) { R.version = Q("version"); R.qv = Q("version"); R.allow = false; }
    if (isSource && (!R.sink || (Q("sink") && R.qs !== Q("sink")))) {
      const pre = Q("sink");
      R.qs = pre;
      const s = sinks.find((x) => x.id === pre) || sinks.find((x) => x.id === R.sink) || sinks.find((x) => stateOf(x).status === "launched") || sinks[0];
      if (s.id !== R.sink) { R.sink = s.id; R.sinkVersion = stateOf(s).stable != null ? "stable" : "newest"; R.picked = null; }
    }
    refreshRunInputs(d, R);
    const sinkT = sinks.find((x) => x.id === R.sink);
    const ovT = overlayList().find((x) => x.id === R.overlay);
    const what = isSource
      ? html`<div class="form tpl-five">${field("Source version", select(versionOptions(st), R.version, { name: "r-version" }), { help: "resolved when the run starts" })}${field("Sink template", select(sinks.map((s) => [s.id, s.id]), R.sink, { name: "r-sink" }), { req: true })}${field("Sink version", select(sinkT ? versionOptions(stateOf(sinkT)) : [["", "—"]], R.sinkVersion, { name: "r-sinkv" }))}${field("Deployment overlay", select([["", "none"], ...overlayList().map((o) => [o.id, o.id])], R.overlay, { name: "r-overlay" }), { help: ovT ? ovT.description || "state, DLQ, notifications, SLA" : "state, DLQ, notifications, SLA" })}${field("Run name", input(R.name, { ph: "optional", name: "r-name" }), { help: "optional" })}</div>`
      : html`<div class="form three">${field("Version", select(versionOptions(st), R.version, { name: "r-version" }), { help: "resolved when the run starts" })}${field("Run name", input(R.name, { ph: "optional", name: "r-name" }), { help: "optional" })}</div>`;
    const errs = [];
    let paramsBody;
    if (R.declaredErr) paramsBody = html`<div class="pad">${errorAlert("the parameters", R.declaredErr)}</div>`;
    else if (!R.declared) paramsBody = html`<div class="pad">${skelRows(3)}</div>`;
    else if (!Object.keys(R.declared).length) paramsBody = html`<div class="empty-line">No parameters to supply.</div>`;
    else {
      paramsBody = html`<div class="form">${Object.entries(R.declared).map(([n, p]) => {
        const val = R.params[n] ?? "";
        const e = paramError(p, val);
        const show = e && (R.tried || R.touched.has(n));
        if (e) errs.push(n);
        const secret = secretParam(n, p);
        const bits = [];
        if (secret) bits.push(html`${tag("secret")} never stored in the run record`);
        if (p.description) bits.push(raw(mdInline(p.description)));
        if (p.trusted) bits.push(html`<span title="the template author lets this value reach paths, URLs and commands without the caller reference policy">${tag("trusted")}</span>`);
        if (p.from !== "source") bits.push(`from ${p.from} ${p.tid}`);
        if (p.default != null && val === "") bits.push(`default ${String(p.default)}`);
        let control;
        if (p.type === "bool") control = toggle(p.description ? String(p.description).split(/[.\n]/)[0] : n, val === true || val === "true" || (val === "" && p.default === true), { name: `p:${n}` });
        else if (p.values) control = select([["", p.default != null ? `default · ${p.default}` : "choose…"], ...p.values.map((x) => [String(x), String(x)])], String(val), { name: `p:${n}` });
        else control = input(val, { name: `p:${n}`, type: secret ? "password" : "text", ph: p.default != null ? String(p.default) : p.type || "string", cls: show ? "err" : "" });
        return field(n, control, { code: true, req: !!p.required, help: join(bits, " · "), err: show ? e : "" });
      })}</div>`;
    }
    const shown = errs.filter((n) => R.tried || R.touched.has(n));
    const reqCard = card({ icon: "code", title: "Request", aside: "POST …/runs", body: code(toYamlish(runBody(d, R, { mask: true })), { box: false }) });
    const paramsPart = card({ title: "Parameters", aside: isSource ? "from the source, sink and overlay" : "declared by the template", right: R.declared ? plural(Object.keys(R.declared).length, "param") : "", body: paramsBody, note: shown.length ? html`<span class="bad">${plural(shown.length, "field needs", "fields need")} attention</span> · ${shown.join(", ")}` : "" });
    let rowsPart;
    if (R.rowsErr) rowsPart = alert(`Couldn’t list the ${isSource ? "streams" : "rows"}.`, R.rowsErr.message, "bad");
    else if (R.rows && R.rows.length > 1) {
      const pickedN = R.picked ? R.picked.size : 0;
      const items = R.rows.map((r) => {
        const w = r.write || {};
        const uns = w.supported === false;
        const on = !!(R.picked && R.picked.has(r.id));
        const pv = R.preview && (R.preview.rows || []).find((x) => x.id === r.id);
        const facts = [uns ? "unsupported" : w.alias_applied || w.resolved || (w.requested || []).join(" | "), !uns && r.primary_keys && r.primary_keys.length ? `key ${r.primary_keys.join(", ")}` : "", r.parent ? `child of ${r.parent}` : "", (r.depends_on || []).length ? `after ${r.depends_on.join(", ")}` : "", r.state && r.state.health ? r.state.health : "", r.status && r.status !== "active" ? r.status : ""].filter(Boolean);
        const why = uns ? w.unsupported_reason || "this sink cannot write it" : pv && pv.blocked ? `blocked: ${pv.blocked}` : "";
        return html`<label class="tpl-pick${uns ? " dis" : ""}${r.parent ? " child" : ""}" title="${r.description || ""}"><input class="vh" type="checkbox" name="row" value="${r.id}"${raw(on ? " checked" : "")}${raw(uns ? " disabled" : "")}><span class="check${on ? " on" : ""}"><i></i></span><span><span class="mono">${r.id}</span> <span class="muted" style="font-size:12px">${facts.join(" · ")}</span></span><span></span>${why ? html`<span class="why">${why}</span>` : ""}</label>`;
      });
      const streams = card({ title: isSource ? "Streams to run" : "Rows to run", aside: isSource ? `into ${R.sink}` : "", right: html`${btn("Defaults", { cls: "sm", data: { act: "pick", pick: "default" } })}${btn("All", { cls: "sm", data: { act: "pick", pick: "all" } })}${btn("None", { cls: "sm", data: { act: "pick", pick: "none" } })}`, body: html`<div class="tpl-bar">${select([["off", "Include parents · off"], ["eligible", "Include parents · when eligible"], ["all", "Include parents · all"]], R.parents, { name: "r-parents" })}<span class="r muted">${pickedN} of ${R.rows.length} selected</span></div>${items}` });
      let resolved;
      if (!pickedN) resolved = card({ title: "Resolved run set", body: html`<div class="empty-line"><span class="bad">Choose at least one ${isSource ? "stream" : "row"}.</span></div>` });
      else if (R.previewErr) resolved = card({ title: "Resolved run set", body: html`<div class="empty-line"><span class="bad">${R.previewErr}</span></div>` });
      else if (!R.preview) resolved = card({ title: "Resolved run set", body: html`<div class="pad">${skelRows(2)}</div>` });
      else {
        const rs = R.preview.run_set || [];
        resolved = card({ title: "Resolved run set", right: plural(rs.length, "row"), body: tbl(["Row", "Why", "Write"], rs.map((rid) => {
          const r = (R.preview.rows || []).find((x) => x.id === rid) || {};
          const w = r.write || {};
          return [html`<span class="mono">${rid}</span>`, r.pulled_in ? html`<span class="info" title="${r.pulled_in.because ? `for ${r.pulled_in.because}` : ""}">pulled in</span>` : "selected", w.alias_applied || w.resolved || ""];
        })) });
      }
      rowsPart = html`<div class="two">${streams}<div class="stack">${resolved}${untestedAlert(d, R)}</div></div>`;
    } else rowsPart = untestedAlert(d, R);
    const submitErr = R.submitErr ? alert("The server refused the run.", html`${R.submitErr.message}${R.submitErr.details ? code(JSON.stringify(R.submitErr.details, null, 2), { lang: "json", copy: false }) : ""}`, "bad") : "";
    return html`${submitErr}${card({ icon: "tpl", title: "What to run", aside: isSource ? "source, sink and deployment" : "version and name", body: what })}<div class="two">${paramsPart}${reqCard}</div>${rowsPart}`;
  }

  function runActions(d) {
    const R = runState(d);
    const n = resolveSel(R.version, stateOf(d));
    const g = untestedGate(d, R);
    return html`${btn("Copy as API call", { icon: "code", data: { act: "copy-api" } })}${btn(R.busy ? "Submitting…" : g ? `Run v${n} anyway` : `Run v${n ?? "?"}`, { kind: "primary", icon: "play", perm: "run_write", data: { act: "run-submit" }, disabled: R.busy || (g && !R.allow) || !!(R.rows && R.rows.length > 1 && R.picked && !R.picked.size) })}`;
  }

  async function submitRun(d) {
    const R = runState(d);
    R.tried = true;
    const bad = Object.entries(R.declared || {}).filter(([n, p]) => paramError(p, R.params[n] ?? ""));
    if (bad.length) { renderDetail(); toast(`${plural(bad.length, "field needs", "fields need")} attention: ${bad.map(([n]) => n).join(", ")}`, "bad"); return; }
    if (R.rows && R.rows.length > 1 && (!R.picked || !R.picked.size)) { toast("Choose at least one stream to run", "bad"); return; }
    R.busy = true;
    R.submitErr = null;
    renderDetail();
    try {
      const resp = await api(`/v1/templates/${enc(d.id)}/runs`, { method: "POST", body: runBody(d, R) });
      if (resp.status === "pending_approval") {
        toast("Approval requested: the run waits for an approver", "info", resp.change_id ? { href: href.change(resp.change_id), label: "open" } : null);
      } else {
        for (const w of resp.warnings || []) toast(w, "warn");
        if (resp.deprecated) toast(`Deprecated template: ${resp.deprecated}`, "warn");
        const runId = resp.run_id || (resp.run && resp.run.run_id);
        toast(html`Run from <b>v${resp.template_version}</b> submitted${resp.sink_template ? html` · into ${resp.sink_template}` : ""}`, "ok", runId ? { href: href.run(runId), label: "open" } : null);
        if (runId) navigate(href.run(runId));
      }
    } catch (e) {
      R.submitErr = e;
      toast(e.message, "bad");
    } finally {
      R.busy = false;
      if (alive) renderDetail();
    }
  }

  // ── Pairings tab ────────────────────────────────────────────────────────────
  function pairingsTab(d) {
    const k = kindOf(d);
    if (k === "deployment") {
      const sources = (C.all || []).filter((t) => kindOf(t) === "source-template" && stateOf(t).status !== "deprecated");
      return card({ title: "Apply to a run", right: plural(sources.length, "source template"), body: sources.length ? tbl(["Source template", "Tests", ""], sources.map((s) => ({ href: tHref(s.id, { tab: "run" }), cells: [lnk(s.id, tHref(s.id, { tab: "run" })), testCell(testStatus(stateOf(s), stateOf(s).newest)), btn("Open its run form", { cls: "sm", icon: "play", href: tHref(s.id, { tab: "run" }), perm: "run_write" })] }))) : html`<div class="empty-line">No source templates are registered yet.</div>`, note: html`A deployment overlay adds state, DLQ, notifications and SLA to a <b>source × sink</b> run: open a source template, pick a sink, and choose <b>${d.id}</b> as its deployment.` });
    }
    if (!C.matrix) return C.matrixErr ? errorAlert("the compatibility matrix", C.matrixErr) : skelCards(1);
    const plan = (c) => html`<div class="tpl-chips">${(c.streams || []).map((s) => html`<span class="chip"><span class="mono">${s.stream}</span> ${s.satisfies && s.satisfies !== s.write_mode ? `${s.write_mode} ← ${s.satisfies}` : s.write_mode}</span>`)}${(c.incompatible || []).map((s) => html`<span class="chip" title="${s.reason}"><span class="mono">${s.stream}</span> <span class="warn">cannot write</span></span>`)}</div>`;
    const fit = (c) => { const f = fitOf(c); return dotText(f.n === f.m ? "ok" : f.n ? "warn" : "bad", `${f.n} / ${f.m}`); };
    const src = k === "source-template";
    const cells = (C.matrix.matrix || []).filter((c) => (src ? c.source === d.id : c.sink === d.id));
    const rows = cells.map((c) => {
      const other = src ? c.sink : c.source;
      const t = findT(other);
      const tst = t ? stateOf(t) : {};
      const typ = src ? (matrixSink(other) || {}).sink_type : (matrixSource(other) || {}).source_type;
      const sub = `${tst.stable != null ? `stable v${tst.stable}` : `${tst.status || "draft"} v${tst.newest ?? "?"}`}${typ ? ` · ${typ}` : ""}`;
      const f = fitOf(c);
      const runHref = src ? tHref(d.id, { tab: "run", sink: c.sink }) : tHref(c.source, { tab: "run", sink: d.id });
      return [html`${lnk(other, tHref(other))}<div class="muted" style="font-size:12px">${sub}</div>`, plan(c), fit(c), f.n ? btn(f.n === f.m ? "Run into this sink" : `Run ${plural(f.n, "stream")}`, { cls: "sm", icon: "play", href: runHref, perm: "run_write" }) : ""];
    });
    return card({
      icon: "matrix", title: src ? "Sinks this template can write to" : "Source templates that write here", aside: "per-stream write plan", right: lnk("open the full matrix", regHref({ tab: "matrix" })),
      body: rows.length ? tbl([src ? "Sink template" : "Source template", "Plan per stream", "Fit", ""], rows) : html`<div class="empty-line">No ${src ? "sink" : "source"} template is registered yet.</div>`,
      note: src ? html`<b>append ← upsert</b> means the sink stands in for the requested mode. A stream the sink cannot write is left out of the run, never written wrongly.` : html`A sink template runs <b>through a source template</b>: open one and <b>${d.id}</b> is preselected as its sink.`,
    });
  }

  // ── Activity tab ────────────────────────────────────────────────────────────
  const healthCache = new Map();
  function activityTab(d) {
    const rr = C.runs.get(d.id);
    const runs = rr ? rr.runs : null;
    const ST = { completed: "ok", failed: "bad", cancelled: "muted", running: "info", queued: "muted", pending: "muted", sharded: "info" };
    const runRows = (runs || []).slice(0, 8).map((r) => ({ href: href.run(r.run_id), cells: [lnk(r.name || r.run_id, href.run(r.run_id)), html`<span class="mono">v${(r.labels || {}).template_version ?? "?"}</span>`, dotText(ST[r.status] || "muted", r.status === "completed" ? "succeeded" : r.status), (r.labels || {}).sink_template || "—", fmtInt(r.records_written || 0), ago(r.submitted_at, " ago")] }));
    const runsCard = card({ icon: "runs", title: "Recent runs", aside: "every version, newest first", right: lnk(runs && runs.length ? `all ${fmtInt(runs.length)}${runs.length >= 200 ? "+" : ""} in Runs` : "Runs", href.runs({ label: `template:${d.id}` })), body: runs === null ? html`<div class="pad">${skelRows(3)}</div>` : runRows.length ? tbl(["Run", "Version", "Status", "Sink", ["Rows", "num"], "When"], runRows) : html`<div class="empty-line">No run of this template yet.</div>` });
    let health = "";
    const h = healthCache.get(d.id);
    if (h === undefined && stateOf(d).stable != null && can("status_read")) {
      healthCache.set(d.id, null);
      api(`/v1/status?template=${enc(d.id)}`).then((r) => { healthCache.set(d.id, r); if (alive) renderDetail(); }, () => healthCache.set(d.id, false));
    } else if (h) {
      const HT = { ok: "ok", running: "info", warming: "warn", degraded: "warn", failed: "bad", unknown: "muted" };
      const bm = (b) => (b == null ? "—" : typeof b === "object" ? Object.entries(b).map(([k, x]) => `${k} ${typeof x === "string" ? x : JSON.stringify(x)}`).join(" ") : String(b));
      const bad = (h.rows || []).filter((x) => x.health === "degraded" || x.health === "failed");
      health = card({ icon: "shield", title: `Stable v${stateOf(d).stable} in production`, tag: "status read", right: html`checked ${ago(h.generated_at, " ago")}`, body: tbl(["Row", "Health", "Last success", "Bookmark", ["Lag", "num"], ["DLQ", "num"], "Next resume"], (h.rows || []).map((x) => [html`<span class="mono">${x.row}</span>`, dotText(HT[x.health] || "muted", x.health), x.last_success ? ago(x.last_success.at, " ago") : "never", html`<span class="mono" title="${bm(x.bookmark)}">${bm(x.bookmark).slice(0, 40)}</span>`, x.lag ? x.lag.human : "—", !x.dlq || !x.dlq.configured ? "—" : x.dlq.readable ? fmtInt(x.dlq.count) : "?", x.resume || ""])), note: bad.length ? html`<span class="warn">${bad.map((x) => x.row).join(", ")} ${bad.length === 1 ? "needs" : "need"} attention</span>${(bad[0].reasons || []).length ? ` · ${bad[0].reasons.join(" · ")}` : ""}` : h.state && h.state.note ? h.state.note : "" });
    }
    const ds = new Map();
    for (const r of runs || []) for (const inv of r.invocations || []) {
      const u = inv.usage || {};
      if (!u.dataset_id) continue;
      const okAt = r.status === "completed" ? r.finished_at || r.submitted_at : null;
      const prev = ds.get(u.dataset_id);
      if (!prev) ds.set(u.dataset_id, { uri: u.dataset_uri, last: okAt });
      else if (okAt && (!prev.last || Date.parse(okAt) > Date.parse(prev.last))) prev.last = okAt;
    }
    const dsCard = card({ icon: "db", title: "Datasets it produces", right: ds.size ? fmtInt(ds.size) : "", body: ds.size ? tbl(["Dataset", "Last success"], [...ds].slice(0, 8).map(([dsid, x]) => [lnk(datasetLabel(x.uri), href.dataset(dsid), false, x.uri), x.last ? ago(x.last, " ago") : html`<span class="faint">never</span>`])) : html`<div class="empty-line">${runs === null ? "Reading its runs…" : "No dataset recorded for its runs yet."}</div>` });
    const pend = (C.changes || []).filter((c) => (c.template && (c.template === d.id || c.template.id === d.id)) || (c.payload && c.payload.id === d.id));
    const trig = C.triggers ? triggersFor(d.id) : null;
    const chCard = card({ icon: "changes", title: "Pending changes", right: fmtInt(pend.length), body: pend.length ? tbl(["Change", "Approvals"], pend.map((c) => ({ href: href.change(c.id), cells: [html`${lnk(c.reason || `${String(c.kind).replace("template_", "")}${c.payload && c.payload.version != null ? ` v${c.payload.version}` : ""}`, href.change(c.id))}<div class="muted" style="font-size:12px">${c.requester || ""} · ${ago(c.created_at, " ago")}</div>`, `${(c.approvals || []).length} of ${c.required_approvals || 1}`] }))) : html`<div class="empty-line">No change waiting for approval.</div>`, note: trig ? (trig.length ? html`Triggers using it: ${join(trig.map((t) => lnk(t.name, href.trigger(t.name))), ", ")}` : "No trigger uses it.") : "" });
    return html`${runsCard}${health}<div class="pair">${dsCard}${chCard}</div>`;
  }

  // ── registry (nothing selected) ───────────────────────────────────────────
  const regTabs = () => [["Overview", null, "overview"], ["Matrix", null, "matrix"], can("template_admin") ? ["Register", null, "register"] : null, ["Sync", null, "sync"]].filter(Boolean);
  function registrySub() {
    const all = C.all || [];
    const n = (k) => all.filter((t) => kindOf(t) === k).length;
    const parts = [html`<span>${plural(all.length, "template")} · ${n("source-template")} source · ${n("sink-template")} sink · ${plural(n("deployment"), "deployment")}${n("pipeline") ? ` · ${plural(n("pipeline"), "pipeline")}` : ""}</span>`];
    parts.push(C.sync.length ? html`<span>synced from ${join(C.sync.map((o) => lnk(o.name, regHref({ tab: "sync" }))), ", ")}</span>` : html`<span>no sync origins</span>`);
    return parts;
  }

  function registryView() {
    let tab = Q("tab") || "overview";
    if (!regTabs().some((t) => t[2] === tab)) tab = "overview";
    const actions = tab === "overview" ? html`${btn("Register", { icon: "plus", href: regHref({ tab: "register" }), perm: "template_admin" })}${C.sync.length ? btn("Pull from origins", { icon: "sync", perm: "template_admin", data: { act: "sync-pull" } }) : ""}` : "";
    const dh = dhead(dtabs(regTabs(), tab, { hrefFor: (x) => regHref({ tab: x === "overview" ? "" : x }) }), actions);
    setCrumbs(["Build", ...(tab === "overview" ? ["Templates"] : [{ label: "Templates", href: "#/templates" }, { matrix: "Matrix", register: "Register", sync: "Sync" }[tab]])]);
    setDelta("");
    setContext("");
    if (C.all === null && !C.listErr) return html`${dh}<div><span class="skel t"></span><div style="height:10px"></div><span class="skel w30"></span></div>${skelCards(3)}`;
    if (C.listErr && !C.all) return html`${dh}${head("Template registry")}${unavailable(C.listErr) ? empty("Templates are not available", TMPL_MISSING, { icon: "tpl" }) : errorAlert("the templates", C.listErr)}`;
    if (!C.all.length && tab === "overview") {
      return html`${dh}${head("Template registry", [html`<span>no templates</span>`, html`<span>${C.sync.length ? plural(C.sync.length, "sync origin") : "no sync origins configured"}</span>`])}${empty("Register your first template", html`A template is a reusable source, sink, deployment or pipeline with typed parameters. Register one here, or pull a catalogue from an origin with <code>faucet serve --templates-sync</code>.`, { icon: "tpl", btns: html`${btn("Register a template", { kind: "primary", icon: "plus", href: regHref({ tab: "register" }), perm: "template_admin" })}${btn("Browse the hub", { icon: "link", href: "https://github.com/faucet-hq/template-hub" })}` })}`;
    }
    const body = { overview: registryOverview, matrix: matrixView, register: registerView, sync: syncView }[tab]();
    return html`${dh}${head("Template registry", registrySub())}${body}`;
  }

  function registryOverview() {
    const all = C.all;
    const total = all.length || 1;
    const s = stateOf;
    const draft = all.filter((t) => s(t).status === "draft");
    const tested = all.filter((t) => s(t).status !== "deprecated" && s(t).newest !== s(t).stable && testStatus(s(t), s(t).newest) === "passing");
    const launched = all.filter((t) => s(t).status === "launched");
    const chans = all.filter((t) => Object.keys(s(t).tags || {}).length);
    const funnel = [["Draft", draft.length, "registered, never launched"], ["Tested", tested.length, "gate open, waiting to launch"], ["Launched", launched.length, "stable version live"], ["On channels", chans.length, "a channel assigned"]];
    const attention = [];
    for (const t of all) {
      const st = s(t);
      const nt = testStatus(st, st.newest);
      if (nt === "failing" || nt === "unknown") attention.push([0, lnk(t.id, tHref(t.id)), html`${dotText("bad", "tests failing")} · v${st.newest} · launch blocked`, lnk("see failures", tHref(t.id, { tab: "tests" }))]);
      else if (st.status !== "deprecated" && st.newest !== st.stable && nt === "passing") attention.push([1, lnk(t.id, tHref(t.id)), html`${dotText("ok", "ready to launch")} · v${st.newest} passed its tests`, btn(`Launch v${st.newest}`, { cls: "sm", icon: "rocket", href: tHref(t.id, { launch: "1" }), perm: "template_admin" })]);
      else if (st.status === "draft") attention.push([2, lnk(t.id, tHref(t.id)), html`${dotText("warn", "draft")} · never launched${nt === "untested" ? ", tests not run" : !nt || nt === "no_tests" ? ", no tests" : ""}`, st.has_tests ? btn("Run tests", { cls: "sm", icon: "flask", perm: "template_admin", data: { act: "test", v: st.newest, id: t.id } }) : lnk("open", tHref(t.id))]);
      else if (st.status === "deprecated") attention.push([3, lnk(t.id, tHref(t.id)), html`${dotText("muted", "deprecated")}${st.deprecation && st.deprecation.reason ? ` · ${st.deprecation.reason}` : ""}${C.triggers ? (triggersFor(t.id).length ? ` · still used by ${plural(triggersFor(t.id).length, "trigger")}` : " · no trigger uses it") : ""}`, C.triggers && triggersFor(t.id).length ? lnk("show triggers", href.triggers()) : lnk("open", tHref(t.id))]);
    }
    attention.sort((a, b) => a[0] - b[0]);
    let recent;
    if (!C.regDetails) {
      loadRegistryDetails().then(() => alive && !P.id && renderDetail());
      recent = html`<div class="pad">${skelRows(3)}</div>`;
    } else {
      const ls = [];
      for (const [id, d] of C.regDetails) for (const l of d.launches || []) ls.push({ id, ...l });
      ls.sort((a, b) => Date.parse(b.launched_at) - Date.parse(a.launched_at));
      recent = ls.length ? html`<div class="log">${ls.slice(0, 6).map((l) => html`<div>${ago(l.launched_at, " ago")}&nbsp; <a class="lnk" href="${tHref(l.id)}">${l.id}</a> v${l.version} launched by <b>${l.launched_by || "cli"}</b></div>`)}</div>` : html`<div class="empty-line">Nothing launched yet.</div>`;
    }
    return html`${card({ icon: "layers", title: "Lifecycle", aside: "where every template stands", body: html`<div class="tpl-funnel">${funnel.map(([k, n, sub]) => html`<div><small>${k}</small><b>${n}</b>${meter((n / total) * 100)}<span class="muted" style="font-size:12px">${sub}</span></div>`)}</div>` })}
      ${card({ icon: "alert", title: "Needs attention", right: fmtInt(attention.length), body: attention.length ? tbl(["Template", "What", ""], attention.slice(0, 10).map((r) => r.slice(1))) : html`<div class="empty-line">Every template is launched and passing its tests.</div>` })}
      ${card({ title: "Recent launches", body: recent })}`;
  }

  function matrixView() {
    if (!C.matrix) {
      if (!C.matrixErr) loadMatrix().then(() => alive && renderDetail());
      return C.matrixErr ? errorAlert("the compatibility matrix", C.matrixErr) : skelCards(2);
    }
    const M = C.matrix;
    const so = Q("sowner"), stp = Q("stype"), ko = Q("kowner"), kt = Q("ktype");
    const owner = (x) => x.owner || (x.id.includes("/") ? x.id.split("/")[0] : "");
    const allS = M.sources || [], allK = M.sinks || [];
    if (!allS.length || !allK.length) return card({ icon: "matrix", title: "Source × sink compatibility", body: empty("No pairings to show", "The matrix appears once the registry holds at least one source template and one sink template.", { icon: "matrix" }) });
    const S = allS.filter((x) => (!so || owner(x) === so) && (!stp || x.source_type === stp));
    const K = allK.filter((x) => (!ko || owner(x) === ko) && (!kt || x.sink_type === kt));
    const uniq = (xs) => [...new Set(xs.filter(Boolean))].sort();
    const facets = html`${select([["", "Source owner · any"], ...uniq(allS.map(owner)).map((o) => [o, `Source owner · ${o}`])], so, { name: "sowner" })}${select([["", "Source type · any"], ...uniq(allS.map((x) => x.source_type)).map((o) => [o, `Source type · ${o}`])], stp, { name: "stype" })}${select([["", "Sink owner · any"], ...uniq(allK.map(owner)).map((o) => [o, `Sink owner · ${o}`])], ko, { name: "kowner" })}${select([["", "Sink type · any"], ...uniq(allK.map((x) => x.sink_type)).map((o) => [o, `Sink type · ${o}`])], kt, { name: "ktype" })}`;
    const fchips = [so && ["sowner", `source owner: ${so}`], stp && ["stype", `source type: ${stp}`], ko && ["kowner", `sink owner: ${ko}`], kt && ["ktype", `sink type: ${kt}`]].filter(Boolean);
    const [selS, selK] = (Q("cell") || "").split("→");
    const cellHref = (a, b) => regHref({ tab: "matrix", sowner: so, stype: stp, kowner: ko, ktype: kt, cell: `${a}→${b}` });
    const rows = S.map((x) => [html`${lnk(x.id, tHref(x.id))} <span class="muted" style="font-size:12px">${x.source_type || "source"} · ${(x.streams || []).length}</span>`, ...K.map((k) => {
      const c = cellOf(x.id, k.id);
      if (!c) return html`<span class="cell no">—</span>`;
      const f = fitOf(c);
      const on = selS === x.id && selK === k.id;
      const label = f.n === 0 ? "✗" : f.n === f.m ? `✓ ${f.n} / ${f.m}` : `${f.n} / ${f.m}`;
      const tip = [...(c.streams || []).map((y) => `${y.stream}: ${y.write_mode}${y.satisfies && y.satisfies !== y.write_mode ? ` (for ${y.satisfies})` : ""}`), ...(c.incompatible || []).map((y) => `${y.stream}: ${y.reason}`)].join("\n");
      return html`<a class="cell ${on ? "sel" : f.n === 0 ? "no" : f.n < f.m ? "warn" : ""}" href="${cellHref(x.id, k.id)}" title="${tip}">${label}</a>`;
    })]);
    const matrix = card({
      icon: "matrix", title: "Source × sink compatibility", aside: "streams a sink can write", right: `${S.length} of ${allS.length} sources × ${K.length} of ${allK.length} sinks`,
      body: html`<div class="tpl-facets">${facets}${fchips.map(([k, l]) => chip(l, "x", { "data-clear": k, role: "button", tabindex: "0", title: "Remove filter" }))}${fchips.length ? html`<span class="r"><a class="lnk" href="${regHref({ tab: "matrix" })}">Clear filters</a></span>` : ""}</div><div class="mx tpl-mxwrap">${S.length && K.length ? tbl(["Source ↓ · sink →", ...K.map((k) => html`<a class="lnk" href="${tHref(k.id)}">${k.id}</a>`)], rows) : html`<div class="empty-line">No template matches these filters.</div>`}</div>`,
    });
    let detail = "";
    const c = selS && cellOf(selS, selK);
    if (c) {
      const f = fitOf(c);
      const src = allS.find((x) => x.id === selS) || {};
      const req = (name) => { const y = (src.streams || []).find((z) => z.name === name); return y && y.write ? y.write.join(" | ") : "—"; };
      detail = card({ title: `${selS} → ${selK}`, tag: `${f.n} of ${plural(f.m, "stream")}`, right: f.n ? btn(f.n === f.m ? "Run" : `Run ${plural(f.n, "stream")}`, { cls: "sm", icon: "play", href: tHref(selS, { tab: "run", sink: selK }), perm: "run_write" }) : "", body: tbl(["Stream", "Requested", "Resolved", ""], [...(c.streams || []).map((y) => [html`<span class="mono">${y.stream}</span>`, req(y.stream), y.satisfies && y.satisfies !== y.write_mode ? `${y.write_mode} ← ${y.satisfies}` : y.write_mode, dotText("ok", "ok")]), ...(c.incompatible || []).map((y) => [html`<span class="mono">${y.stream}</span>`, req(y.stream), "—", dotText("bad", y.reason)])]), note: c.command ? html`CLI <code>${c.command}</code><button type="button" class="cpi" data-copy="${c.command}" title="Copy" aria-label="Copy">${ic("copy")}</button>` : "" });
    }
    return html`${matrix}${detail}`;
  }

  const reg = { id: "", desc: "", text: "", fmt: "yaml", launch: false, err: null, busy: false };
  function registerView() {
    const ph = "kind: source-template        # or sink-template, deployment, or pipeline\nname: billing\nowner: acme\nparams:\n  api_token: { type: string, required: true, secret: true }\nsource:\n  type: rest\n  config: { base_url: https://api.example.com/v1 }\nstreams:\n  - { name: invoices, source: { config: { path: /invoices } }, primary_keys: [id], write: [upsert, append] }";
    return card({
      icon: "plus", title: "Register a template", aside: "source-template, sink-template, deployment or pipeline",
      right: html`<span class="seg" role="group" aria-label="Format">${["yaml", "json"].map((f) => html`<span role="button" tabindex="0" class="${reg.fmt === f ? "on" : ""}" data-act="fmt" data-fmt="${f}">${f.toUpperCase()}</span>`)}</span>`,
      body: html`<div class="form">${field("Id", input(reg.id, { name: "reg-id", ph: "acme/orders-v2" }), { help: html`optional · defaults to <code>owner/name</code> from the document` })}${field("Description", input(reg.desc, { name: "reg-desc", ph: "What it moves, in one line" }))}
        <div class="field wide tpl-ed"><label>Document</label><textarea class="area mono" name="reg-body" spellcheck="false" rows="16" placeholder="${ph}">${reg.text}</textarea><div class="help">A <code>kind:</code> line says what the document is. A version with a <code>tests:</code> block is tested as it registers; registering never moves <code>stable</code>.</div></div></div>
        ${reg.err ? html`<div style="padding:0 16px 14px">${registerError(reg.err)}</div>` : ""}`,
      note: html`<div class="tpl-next">${toggle("Launch it if the tests pass", reg.launch, { name: "reg-launch" })}${btn(reg.busy ? "Registering…" : "Register", { kind: "primary", icon: "plus", perm: "template_admin", data: { act: "register" }, disabled: reg.busy })}</div>`,
    });
  }

  const syncS = { origin: "", result: null, busy: false, err: null };
  function syncView() {
    if (!C.sync.length) return card({ icon: "sync", title: "Origins", body: empty("No sync origins configured", html`Pull templates from a GitHub repo or a bucket: start the server with <code>faucet serve --templates-sync sync.yaml</code>.`, { icon: "sync" }) });
    const origins = card({ icon: "sync", title: "Origins", aside: "where templates are pulled from", right: html`${select([["", "All origins"], ...C.sync.map((o) => [o.name, o.name])], syncS.origin, { name: "sync-origin" })}${btn("Dry run", { cls: "sm", perm: "template_admin", data: { act: "sync-dry" }, disabled: syncS.busy })}${btn("Pull now", { cls: "sm", icon: "sync", perm: "template_admin", data: { act: "sync-pull" }, disabled: syncS.busy })}`, body: tbl(["Origin", "Kind", "Prefix", "Launch", "Prune", "Every"], C.sync.map((o) => [o.name, o.kind, html`<span class="mono">${o.prefix || "(all)"}</span>`, o.launch, o.prune, o.interval_secs ? `${o.interval_secs}s` : "on demand"])) });
    let result = "";
    if (syncS.err) result = alert("The pull failed.", syncS.err.message, "bad");
    else if (syncS.result) {
      const res = syncS.result;
      const MUT = ["register", "launch", "revive", "deprecate"];
      const blocks = (res.reports || []).map((r) => {
        const o = r.outcome;
        const mut = o ? o.registered.length + o.launched.length + o.revived.length + o.deprecated.length : (r.plan || []).filter((a) => MUT.includes(a.action)).length;
        const unchanged = (r.plan || []).filter((a) => a.action === "unchanged");
        const rows = (r.plan || []).filter((a) => a.action !== "unchanged").map((a) => {
          const act = { register: html`<span class="ok">register</span>`, launch: html`<span class="info">launch</span>`, revive: html`<span class="info">revive</span>`, deprecate: html`<span class="warn">deprecate</span>` }[a.action] || a.action;
          const det = { register: `${a.replaces != null ? `after v${a.replaces}` : "new template"}${a.launch ? ", then launch" : ""}`, launch: `v${a.version}`, revive: "back at the origin", deprecate: "removed from the origin", orphaned: "gone upstream; kept", skipped: html`<span class="bad">${a.reason}</span>` }[a.action] || "";
          return [act, a.id ? lnk(a.id, tHref(a.id)) : html`<span class="mono">${a.name}</span>`, det];
        });
        if (unchanged.length) rows.push(["unchanged", html`<span class="muted">${plural(unchanged.length, "template")}</span>`, ""]);
        for (const w of r.warnings || []) rows.push([html`<span class="warn">warning</span>`, "", w]);
        for (const f of (o && o.failed) || []) rows.push([html`<span class="bad">failed</span>`, lnk(f.id, tHref(f.id)), f.error]);
        return card({ title: `${res.dry_run ? "Dry run" : "Pull"} · ${r.origin}`, tag: res.dry_run ? "nothing changed" : plural(mut, "change"), right: "just now", body: rows.length ? tbl(["Action", "Template", "Detail"], rows) : html`<div class="empty-line">Nothing at this origin.</div>`, note: res.dry_run ? html`<div class="tpl-next"><span>Pulling would make <b>${plural(mut, "change")}</b>.</span>${btn("Pull now", { kind: "primary", icon: "sync", perm: "template_admin", data: { act: "sync-pull" } })}</div>` : "" });
      });
      result = html`${blocks}${(res.origin_errors || []).map((e) => alert(`Origin ${e.origin} failed.`, e.error, "bad"))}`;
    }
    return html`${origins}${result}`;
  }

  // ── detail render ──────────────────────────────────────────────────────────
  let renderSeq = 0;
  async function renderDetail() {
    if (!alive) return;
    const seq = ++renderSeq;
    L.select(!!P.id);
    if (!P.id) { patch(L.detail, registryView()); return; }
    const id = P.id;
    let d = C.details.get(id);
    if (!d) {
      setCrumbs(["Build", { label: "Templates", href: "#/templates" }, id]);
      setDelta("");
      setContext("");
      const err = C.detailErr.get(id);
      if (err) {
        patch(L.detail, html`${dhead("", "", backLink())}${head(id, [pill("Unknown")])}${err.status === 404 && !unavailable(err) ? empty("Template not found", html`No template is registered as <b>${id}</b>. It may have been deleted.`, { icon: "tpl", btns: btn("All templates", { href: "#/templates" }) }) : alert("Couldn’t load this template.", html`The server answered ${err.status ? html`<code>${err.status}</code>` : ""}: ${err.message}. The list on the left is from the last good load.`, "bad", btn("Retry", { icon: "refresh", data: { act: "retry-detail" } }))}`);
        return;
      }
      patch(L.detail, html`${dhead(html`<span class="skel" style="width:280px;height:26px"></span>`)}<div><span class="skel t"></span><div style="height:10px"></div><span class="skel w30"></span></div>${skelCards(3)}`);
      d = await loadDetail(id);
      if (seq !== renderSeq || P.id !== id) return;
      if (!C.runs.has(id)) loadTemplateRuns(id).then(() => alive && P.id === id && renderDetail());
      renderDetail();
      return;
    }
    setTop(d);
    const tabsList = tabsFor(d);
    let tab = Q("tab") || "overview";
    if (!tabsList.some((t) => t[2] === tab)) tab = "overview";
    const body = { overview: overviewTab, versions: versionsTab, tests: testsTab, run: runTab, pairings: pairingsTab, activity: activityTab }[tab](d);
    const newver = tab === "versions" && Q("newver") === "1";
    const right = tab === "run" ? runActions(d) : newver ? html`${btn("Cancel", { href: tHref(d.id, { tab: "versions" }) })}${btn(nv.busy ? "Registering…" : `Register v${nextVersion(stateOf(d))}`, { kind: "primary", icon: "plus", perm: "template_admin", data: { act: "newver-submit" }, disabled: nv.busy })}` : headerActions(d);
    const dh = dhead(dtabs(tabsList, tab, { hrefFor: (x) => tHref(d.id, { tab: x === "overview" ? "" : x }) }), right, backLink());
    patch(L.detail, html`${dh}${newver ? "" : templateHead(d)}${body}`);
    if (Q("launch") === "1") { setQuery({ launch: "" }, { replace: true }); const g = gateInfo(d); if (g.can) doLaunch(d, g.cand); }
    if (Q("rollback") === "1") { setQuery({ rollback: "" }, { replace: true }); doRollback(d); }
  }

  // ── actions ────────────────────────────────────────────────────────────────
  async function refreshAfter(id) {
    C.regDetails = null;
    bodies.clear();
    paramCache.clear();
    healthCache.delete(id);
    await Promise.all([loadList(), id ? loadDetail(id, true) : null, loadMatrix(true)]);
    if (id) { runForms.delete(id); loadTemplateRuns(id); }
    renderList();
    renderDetail();
  }

  async function doLaunch(d, n) {
    const st = stateOf(d);
    const ts = testStatus(st, n);
    const tv = (d.tests && (d.tests.versions || []).find((x) => x.version === n)) || null;
    const r = tv && tv.results && tv.results[0];
    await loadTriggers();
    const trig = triggersFor(d.id);
    const g = grid(2, [
      kv("Stable", st.stable != null ? `v${st.stable} → v${n}` : `none → v${n}`),
      kv("Tests", ts === "passing" && r ? html`<span class="ok">${r.cases.filter((c) => c.passed).length} / ${r.cases.length} passed</span>` : ts === "untested" ? html`<span class="warn">not run · they run first</span>` : TEST_WORD[ts] || "—"),
      kv("Follows stable", C.triggers ? plural(trig.length, "trigger") : "unpinned runs", C.triggers ? "and every unpinned run" : ""),
      kv("Undo", st.stable != null ? "Roll back" : "—", st.stable != null ? `returns stable to v${st.stable}` : ""),
    ]);
    const res0 = await dialog(`Launch v${n} of ${d.id}?`, html`<div class="tpl-dlg-grid">${g}</div><span class="muted">Runs already started keep their version. New runs resolve v${n} from their next start.</span>`, html`${btn("Cancel", { kind: "quiet", data: { act: "no" } })}${btn(`Launch v${n}`, { kind: "primary", icon: "rocket", data: { act: "yes" } })}`, { icon: "rocket" });
    if (!res0 || res0.act !== "yes") return;
    try {
      const res = await api(`/v1/templates/${enc(d.id)}/launch`, { method: "POST", body: { version: n } });
      if (res.status === "pending_approval") toast("Launch requested: it waits for an approver", "info", res.change_id ? { href: href.change(res.change_id), label: "open" } : null);
      else if (res.already_launched) toast(`v${res.version} was already stable`, "info");
      else toast(html`<b>v${res.version} launched.</b> Stable is now v${res.version}${C.triggers ? ` for ${plural(trig.length, "trigger")}` : ""}.`, "ok", res.replaced != null ? { href: tHref(d.id, { rollback: "1" }), label: "Undo" } : null);
    } catch (e) {
      toast(e.message, "bad");
    }
    refreshAfter(d.id);
  }

  async function doRollback(d) {
    const st = stateOf(d);
    if (st.previous == null) { toast("There is no earlier launch to roll back to", "bad"); return; }
    const r = await dialog(`Roll back ${d.id} to v${st.previous}?`, html`<span>Stable moves from <b>v${st.stable}</b> to the previous launch, <b>v${st.previous}</b>. The launch log keeps both entries, and runs already started keep their version.</span>`, html`${btn("Cancel", { kind: "quiet", data: { act: "no" } })}${btn(`Roll back to v${st.previous}`, { kind: "danger", icon: "undo", data: { act: "yes" } })}`, { icon: "undo" });
    if (!r || r.act !== "yes") return;
    try {
      const res = await api(`/v1/templates/${enc(d.id)}/rollback`, { method: "POST", body: {} });
      if (res.status === "pending_approval") toast("Roll back requested: it waits for an approver", "info");
      else toast(html`Rolled back: stable is <b>v${res.version}</b>.`, "ok");
    } catch (e) {
      toast(e.message, "bad");
    }
    refreshAfter(d.id);
  }

  async function doDeprecate(d, ver = null, undo = false) {
    const what = ver != null ? `v${ver} of ${d.id}` : d.id;
    const body = { undo };
    if (!undo) {
      const r = await dialog(`Deprecate ${what}?`, html`<span>${ver != null ? "Pinned runs of this version still work but warn; newest skips it and launch refuses it." : "Unpinned runs still resolve stable and warn. Nothing stops until the template is deleted."}</span>${field("Reason", input("", { name: "reason", ph: "e.g. replaced by acme/orders-v2" }), { help: "shown on the template and in the audit log" })}`, html`${btn("Cancel", { kind: "quiet", data: { act: "no" } })}${btn("Deprecate", { kind: "danger", icon: "pause", data: { act: "yes" } })}`, { icon: "pause" });
      if (!r || r.act !== "yes") return;
      if ((r.values.reason || "").trim()) body.reason = r.values.reason.trim();
    } else if (!(await confirmAction(`Revive ${what}?`, ver != null ? `v${ver} becomes usable again.` : "Triggers stop warning; stable stays where it is.", { label: "Revive", icon: "undo" }))) return;
    try {
      await api(ver != null ? `/v1/templates/${enc(d.id)}/versions/${ver}/deprecate` : `/v1/templates/${enc(d.id)}/deprecate`, { method: "POST", body });
      toast(`${what} ${undo ? "revived" : "deprecated"}`, "ok");
    } catch (e) {
      toast(e.message, "bad");
    }
    refreshAfter(d.id);
  }

  async function doDelete(d, ver = null) {
    const st = stateOf(d);
    const whole = ver == null || st.versions.length === 1;
    if (!(await confirmAction(whole ? `Delete ${d.id}?` : `Delete v${ver} of ${d.id}?`, whole ? "Every version, channel and launch record goes. Runs that already started are not affected; unpinned triggers start failing." : "Channels pointing at it are dropped too. This cannot be undone.", { label: whole ? "Delete template" : `Delete v${ver}`, danger: true, icon: "trash" }))) return;
    try {
      await api(`/v1/templates/${enc(d.id)}${whole ? "" : `?version=${ver}`}`, { method: "DELETE" });
      toast(whole ? `${d.id} deleted` : `v${ver} deleted`, "ok");
      if (whole) {
        C.details.delete(d.id);
        navigate(regHref());
        await loadList();
        renderList();
        return;
      }
    } catch (e) {
      toast(e.message, "bad");
    }
    refreshAfter(d.id);
  }

  async function doTest(id, n, button) {
    if (button) button.disabled = true;
    toast(`Running v${n}’s tests…`, "info");
    try {
      const r = await api(`/v1/templates/${enc(id)}/versions/${n}/test`, { method: "POST", body: {} });
      const failed = r.cases.filter((c) => !c.passed).length;
      if (r.passed) toast(`v${n}: ${plural(r.cases.length, "case")} passed`, "ok");
      else toast(`v${n}: ${failed} of ${r.cases.length} cases failed`, "bad", { href: tHref(id, { tab: "tests" }), label: "see failures" });
    } catch (e) {
      toast(e.message, "bad");
    }
    refreshAfter(id);
  }

  async function doAssign(d, tg, n) {
    try {
      await api(`/v1/templates/${enc(d.id)}/tags`, { method: "POST", body: { tag: tg, version: n } });
      toast(html`<b>${tg}</b> → v${n}`, "ok");
    } catch (e) {
      toast(e.message, "bad");
    }
    refreshAfter(d.id);
  }

  async function doRegister(body, newVersionOf = null) {
    try {
      const resp = await api("/v1/templates", { method: "POST", body });
      if (resp.status === "pending_approval") {
        toast("Registration requested: it waits for an approver", "info", resp.change_id ? { href: href.change(resp.change_id), label: "open" } : null);
        return null;
      }
      const id = resp.id || (resp.summary && resp.summary.id);
      const ver = resp.version ?? (resp.summary && resp.summary.version);
      const inherited = resp.tests_inherited_from != null ? ` (tests inherited from v${resp.tests_inherited_from})` : "";
      if (resp.tests && !resp.tests.passed) toast(`${id} v${ver} registered${inherited}; its tests fail, so it can take no label`, "warn", { href: tHref(id, { tab: "tests" }), label: "see failures" });
      else toast(html`<b>${id} v${ver}</b> registered${inherited}${resp.tests ? " · tests passing" : ""}`, "ok");
      if (newVersionOf) { nv.seeded = false; navigate(tHref(id, { tab: "versions" })); }
      else { Object.assign(reg, { text: "", id: "", desc: "", err: null }); navigate(tHref(id)); }
      await refreshAfter(id);
      return null;
    } catch (e) {
      return e;
    }
  }

  async function doSync(dryRun) {
    syncS.busy = true;
    syncS.err = null;
    if (P.id || Q("tab") !== "sync") navigate(regHref({ tab: "sync" }));
    renderDetail();
    try {
      syncS.result = await api("/v1/templates/sync", { method: "POST", body: { origin: syncS.origin || undefined, dry_run: dryRun } });
      if (!dryRun) await refreshAfter("");
    } catch (e) {
      syncS.err = e;
    } finally {
      syncS.busy = false;
      renderDetail();
    }
  }

  L.detail.addEventListener("click", async (e) => {
    // Test-run rows expand their failing cases; filter chips clear.
    const trow = e.target.closest(".tpl-trtbl tr[data-id]");
    if (trow) { setQuery({ tr: Q("tr") === trow.dataset.id ? "" : trow.dataset.id }, { replace: true }); return; }
    const clr = e.target.closest("[data-clear]");
    if (clr) { setQuery({ [clr.dataset.clear]: "", cell: "" }); return; }
    const b = e.target.closest("[data-act]");
    if (!b || b.disabled) return;
    const act = b.dataset.act;
    const d = P.id ? C.details.get(P.id) : null;
    const n = b.dataset.v != null ? Number(b.dataset.v) : null;
    if (act === "retry-detail") { C.detailErr.delete(P.id); renderDetail(); return; }
    if (act === "launch" && d) return doLaunch(d, n);
    if (act === "rollback" && d) return doRollback(d);
    if (act === "deprecate" && d) return doDeprecate(d);
    if (act === "revive" && d) return doDeprecate(d, null, true);
    if (act === "vdeprecate" && d) return doDeprecate(d, n);
    if (act === "vrevive" && d) return doDeprecate(d, n, true);
    if (act === "vdelete" && d) return doDelete(d, n);
    if (act === "delete-all" && d) return doDelete(d, null);
    if (act === "test") return doTest(b.dataset.id || P.id, n, b);
    if (act === "assign" && d) { b.closest("details")?.removeAttribute("open"); return doAssign(d, b.dataset.tag, n); }
    if (act === "fmt") { reg.fmt = b.dataset.fmt; renderDetail(); return; }
    if (act === "sync-dry") return doSync(true);
    if (act === "sync-pull") return doSync(false);
    if (act === "register") {
      reg.busy = true;
      renderDetail();
      reg.err = await doRegister({ config: reg.text, config_format: reg.fmt, ...(reg.id.trim() ? { id: reg.id.trim() } : {}), ...(reg.desc.trim() ? { description: reg.desc.trim() } : {}), ...(reg.launch ? { launch: true } : {}) });
      reg.busy = false;
      renderDetail();
      return;
    }
    if (act === "newver-submit" && d) {
      nv.busy = true;
      renderDetail();
      const fmt = (bodyOf(d.id, stateOf(d).newest, false).done || {}).format || "yaml";
      nv.err = await doRegister({ id: d.id, config: nv.text, config_format: fmt, ...(nv.desc.trim() ? { description: nv.desc.trim() } : {}) }, d.id);
      nv.busy = false;
      renderDetail();
      return;
    }
    if (act === "pick" && d) {
      const R = runState(d);
      const ok = (r) => !(r.write && r.write.supported === false);
      R.picked = new Set((R.rows || []).filter((r) => ok(r) && (b.dataset.pick === "all" || (b.dataset.pick === "default" && r.default_selected))).map((r) => r.id));
      renderDetail();
      refreshPreview(d, R);
      return;
    }
    if (act === "copy-api" && d) {
      const body = runBody(d, runState(d), { mask: true });
      const cmd = `curl -X POST ${location.origin}/v1/templates/${enc(d.id)}/runs \\\n  -H "Authorization: Bearer $FAUCET_TOKEN" -H 'Content-Type: application/json' \\\n  -d '${JSON.stringify(body).replace(/'/g, "'\\''")}'`;
      try { await copyText(cmd); toast("API call copied (secret values masked)", "ok"); } catch { toast("Couldn’t copy to the clipboard", "bad"); }
      return;
    }
    if (act === "run-submit" && d) submitRun(d);
  });
  L.detail.addEventListener("keydown", (e) => {
    if ((e.key === "Enter" || e.key === " ") && e.target.matches('.seg [data-act="fmt"]')) { e.preventDefault(); e.target.click(); }
  });
  L.detail.addEventListener("input", (e) => {
    const t = e.target;
    if (t.name === "reg-body") { reg.text = t.value; return; }
    if (t.name === "reg-id") { reg.id = t.value; return; }
    if (t.name === "reg-desc") { reg.desc = t.value; return; }
    if (t.name === "nv-body") { nv.text = t.value; return; }
    if (t.name === "nv-desc") { nv.desc = t.value; return; }
    const d = P.id ? C.details.get(P.id) : null;
    if (!d) return;
    const R = runState(d);
    if (t.name === "r-name") { R.name = t.value; renderDetail(); return; }
    if (t.name && t.name.startsWith("p:") && t.tagName === "INPUT" && t.type !== "checkbox") { R.params[t.name.slice(2)] = t.value; renderDetail(); }
  });
  L.detail.addEventListener("focusout", (e) => {
    const t = e.target;
    const d = P.id ? C.details.get(P.id) : null;
    if (d && t.name && t.name.startsWith("p:")) {
      const R = runState(d);
      if (!R.touched.has(t.name.slice(2))) { R.touched.add(t.name.slice(2)); renderDetail(); }
    }
  });
  L.detail.addEventListener("change", (e) => {
    const t = e.target;
    if (t.name === "reg-launch") { reg.launch = t.checked; renderDetail(); return; }
    if (t.name === "sync-origin") { syncS.origin = t.value; return; }
    if (["sowner", "stype", "kowner", "ktype"].includes(t.name)) { setQuery({ [t.name]: t.value, cell: "" }); return; }
    if (t.name === "cmp-a" || t.name === "cmp-b") {
      const [a, b] = Q("cmp").split("..");
      setQuery({ cmp: `${t.name === "cmp-a" ? t.value : a}..${t.name === "cmp-b" ? t.value : b}` }, { replace: true });
      return;
    }
    if (t.name === "cmp-clean") { setQuery({ clean: t.checked ? "" : "0" }, { replace: true }); return; }
    if (t.name === "cfg-clean") { setQuery({ clean: t.checked ? "1" : "" }, { replace: true }); return; }
    const d = P.id ? C.details.get(P.id) : null;
    if (!d) return;
    const R = runState(d);
    if (t.name === "r-version") { R.version = t.value; R.allow = false; }
    else if (t.name === "r-sink") { R.sink = t.value; const s = findT(t.value); R.sinkVersion = s && stateOf(s).stable != null ? "stable" : "newest"; R.picked = null; }
    else if (t.name === "r-sinkv") R.sinkVersion = t.value;
    else if (t.name === "r-overlay") { R.overlay = t.value; const o = findT(t.value); R.overlayVersion = o ? (stateOf(o).stable != null ? "stable" : "newest") : ""; }
    else if (t.name === "r-parents") { R.parents = t.value; renderDetail(); refreshPreview(d, R); return; }
    else if (t.name === "r-allow") R.allow = t.checked;
    else if (t.name === "row") {
      if (!R.picked) R.picked = new Set();
      if (t.checked) R.picked.add(t.value); else R.picked.delete(t.value);
      renderDetail();
      refreshPreview(d, R);
      return;
    } else if (t.name && t.name.startsWith("p:")) { R.params[t.name.slice(2)] = t.type === "checkbox" ? t.checked : t.value; R.touched.add(t.name.slice(2)); }
    else return;
    renderDetail();
  });

  // ── boot ────────────────────────────────────────────────────────────────────
  if (Q("register") === "1" && !P.id) setQuery({ register: "", tab: "register" }, { replace: true });
  renderList();
  renderDetail();
  await Promise.all([loadList(), loadMatrix(), loadTriggers(), loadChanges()]);
  if (!alive) return undefined;
  if (P.id && !C.runs.has(P.id)) loadTemplateRuns(P.id).then(() => alive && renderDetail());
  renderList();
  renderDetail();
  const stop = poll(async () => {
    await loadList();
    renderList();
    if (!P.id && (Q("tab") || "overview") === "overview") renderDetail();
  }, 30000);

  return {
    teardown() { alive = false; stop(); },
    update(p) {
      P = { id: p.id || "", q: p.query };
      if (Q("register") === "1" && !P.id) { setQuery({ register: "", tab: "register" }, { replace: true }); return; }
      if (P.id && !C.runs.has(P.id) && C.details.has(P.id)) loadTemplateRuns(P.id).then(() => alive && renderDetail());
      if (Q("newver") !== "1") nv.seeded = false;
      renderList();
      renderDetail();
    },
  };
}

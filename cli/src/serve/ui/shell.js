// The console shell (#998): grouped sidebar with "Go to…", top bar (crumbs,
// delta, page chips, tenant, timezone, theme, link, expand) and the status line.
// Pages render into `#view` (the body); see page.js.
import { api } from "./api.js";
import { html, raw, ic, v, join } from "./components.js";
import { fmtInt, copyText } from "./utils.js";
import { tenantList, withTenant } from "./tenant.js";
import { whoami, can } from "./access.js";
import { openPalette, configurePalette } from "./palette.js";
import { toast } from "./page.js";

// [group, [[key, icon, label, hash, perm?, soon?]]]
export const NAV = [
  ["Operate", [
    ["overview", "home", "Overview", "#/overview"],
    ["runs", "runs", "Runs", "#/runs"],
    ["changes", "changes", "Changes", "#/changes"],
    ["triggers", "trig", "Triggers", "#/triggers"],
    ["schedules", "sched", "Schedules", "#/schedules", "", true],
  ]],
  ["Data", [
    ["datasets", "db", "Datasets", "#/catalog"],
    ["lineage", "lineage", "Lineage", "#/lineage"],
  ]],
  ["Build", [
    ["templates", "tpl", "Templates", "#/templates"],
    ["schemas", "schema", "Schemas", "#/schemas"],
    ["submit", "submit", "Submit", "#/submit", "config_submit"],
  ]],
  ["Govern", [
    ["policy", "policy", "Policy", "#/policy", "", true],
    ["usage", "usage", "Usage", "#/usage"],
    ["tenants", "tenants", "Tenants", "#/tenants"],
  ]],
  ["Admin", [
    ["audit", "audit", "Audit log", "#/audit", "audit_read"],
    ["settings", "settings", "Settings", "#/settings"],
  ]],
];
// Pages a tenant-scoped principal cannot use (the server refuses them).
const TENANT_HIDDEN = new Set(["tenants", "audit", "submit", "schemas"]);
const COLLAPSE_KEY = "faucet.nav.collapsed";
const EXPAND_KEY = "faucet.shell.expanded";

function readJson(key, fallback) {
  try { return JSON.parse(localStorage.getItem(key)) ?? fallback; } catch { return fallback; }
}
function writeJson(key, x) {
  try { localStorage.setItem(key, JSON.stringify(x)); } catch { /* this page only */ }
}

const LOGO = raw('<svg class="logo" viewBox="6 6 116 116" width="30" height="30" aria-hidden="true"><rect class="tile" x="6" y="6" width="116" height="116" rx="28"/><g class="mk" fill="none" stroke-width="11" stroke-linecap="round" stroke-linejoin="round" transform="translate(-2,2)"><path d="M55 40 V26"/><path d="M44 26 H66"/><path d="M34 40 H76 a14 14 0 0 1 14 14 V64"/></g><g class="dr" transform="translate(-2,2)"><circle cx="90" cy="79" r="5.5"/><circle cx="90" cy="95" r="4.6" opacity="0.85"/><circle cx="90" cy="108" r="3.6" opacity="0.6"/></g></svg>');

export function initials(name) {
  const parts = String(name || "?").split(/[^A-Za-z0-9]+/).filter(Boolean);
  return (parts.length > 1 ? parts[0][0] + parts[1][0] : (parts[0] || "?").slice(0, 2)).toUpperCase();
}

function renderSide() {
  const me = whoami();
  const scoped = !!(me && me.tenant);
  const collapsed = new Set(readJson(COLLAPSE_KEY, []));
  const hasTenants = tenantList() !== null;
  const groups = NAV.map(([g, items]) => {
    const shown = items.filter(([key, , , , perm]) => !(scoped && TENANT_HIDDEN.has(key)) && !(key === "tenants" && !hasTenants) && (!perm || can(perm)));
    if (!shown.length) return "";
    return html`<div class="grp${collapsed.has(g) ? " closed" : ""}" data-group="${g}"><h6 role="button" tabindex="0" aria-expanded="${String(!collapsed.has(g))}">${g} ${ic("chev")}</h6>${shown.map(([key, i, label, hash, , soon]) =>
      html`<a class="nav${soon ? " soon" : ""}" id="nav-${key}" href="${hash}" data-route="${hash}" title="${label}">${ic(i)}<span class="lbl">${label}</span>${soon ? html`<span class="tag">soon</span>` : html`<span class="n" hidden></span>`}</a>`)}</div>`;
  });
  const role = me ? (me.tenant ? `tenant ${me.tenant}` : me.role === "viewer" ? "read-only" : me.role) : "";
  const side = document.getElementById("side");
  side.innerHTML = v(html`
    <a class="ws" href="#/overview" title="faucet">${LOGO}<div><b>faucet</b><span id="server-id">${scoped ? `${me.tenant} · tenant console` : location.host}</span></div></a>
    <button type="button" class="find" id="find">${ic("search")}<span>Go to…</span><kbd>⌘K</kbd></button>
    ${groups}
    <div class="foot">${me ? html`<div class="who" title="${me.principal} · ${role}"><span class="avatar">${initials(me.principal)}</span><span class="nm">${me.principal}</span></div><span class="tag">${role}</span>` : ""}</div>`);
  side.querySelector("#find").onclick = () => openPalette();
  side.querySelectorAll(".grp h6").forEach((h) => {
    const toggle = () => {
      const grp = h.parentElement;
      const now = !grp.classList.contains("closed");
      grp.classList.toggle("closed", now);
      h.setAttribute("aria-expanded", String(!now));
      const set = new Set(readJson(COLLAPSE_KEY, []));
      if (now) set.add(grp.dataset.group); else set.delete(grp.dataset.group);
      writeJson(COLLAPSE_KEY, [...set]);
    };
    h.onclick = toggle;
    h.onkeydown = (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggle(); } };
  });
}

function entryFor(path) {
  const top = "#/" + (path.split("/")[1] || "overview");
  for (const [group, items] of NAV) for (const [key, , label, hash] of items) if (hash === top) return { group, key, label, hash };
  return null;
}
function currentPath() {
  const h = location.hash || "#/overview";
  const q = h.indexOf("?");
  return q === -1 ? h : h.slice(0, q);
}

/** Follow the route: active nav item, default crumbs, title; clear page chips. */
export function syncShell() {
  const path = currentPath();
  const entry = entryFor(path);
  document.querySelectorAll(".nav[data-route]").forEach((a) => {
    const on = !!entry && a.dataset.route === entry.hash;
    a.classList.toggle("on", on);
    if (on) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current");
  });
  const rest = path.split("/").slice(2).map((s) => { try { return decodeURIComponent(s); } catch { return s; } });
  setCrumbs(entry ? [entry.group, rest.length ? { label: entry.label, href: entry.hash } : entry.label, ...(rest.length ? [rest.join(" / ")] : [])] : ["Not found"]);
  setDelta("");
  setContext("");
  refreshBadges();
}

/** The top bar's crumbs: strings or `{label, href}`; the last one is bold. */
export function setCrumbs(parts) {
  const el = document.getElementById("crumb");
  const n = parts.length;
  el.innerHTML = v(join(parts.map((p, i) => {
    const label = typeof p === "string" ? p : p.label;
    if (i === n - 1) return html`<b title="${label}">${label}</b>`;
    return p.href ? html`<a href="${p.href}">${label}</a><span class="faint">/</span>` : html`${label}<span class="faint">/</span>`;
  }), " "));
  const last = parts[n - 1];
  document.title = `${typeof last === "string" ? last : last.label} · faucet console`;
}
/** Replace only the last crumb (an item's own name instead of its id). */
export function setCrumbTitle(text) {
  const last = document.querySelector("#crumb > b:last-child");
  if (!last || !text) return;
  last.textContent = text;
  last.title = text;
  document.title = `${text} · faucet console`;
}
/** The delta beside the crumbs (markup; "" clears). */
export function setDelta(markup) {
  document.getElementById("delta").innerHTML = v(markup || "");
}
/** Page chips at the right of the top bar (markup; "" clears). */
export function setContext(markup) {
  document.getElementById("ctx").innerHTML = v(markup || "");
}

function setBadge(key, n, { hot = false } = {}) {
  const el = document.querySelector(`#nav-${key} .n`);
  if (!el) return;
  if (!n) { el.hidden = true; return; }
  el.hidden = false;
  el.textContent = fmtInt(n);
  el.classList.toggle("hot", hot);
}

let badgeTimer = null;
let badgeAt = 0;
/** Sidebar counts: active runs, pending changes, datasets, templates, tenants.
 *  One cheap request each; a failure hides its badge. */
export function refreshBadges(force = false) {
  if (!force && Date.now() - badgeAt < 5000) return;
  badgeAt = Date.now();
  clearTimeout(badgeTimer);
  badgeTimer = setTimeout(() => refreshBadges(true), 30000);
  const scoped = !!(whoami() && whoami().tenant);
  api(`/v1/runs?${withTenant(new URLSearchParams({ status: "running,sharded", limit: "1" }))}`)
    .then((r) => setBadge("runs", r && r.counts ? (r.counts.running || 0) + (r.counts.sharded || 0) : (r?.runs || []).length))
    .catch(() => setBadge("runs", 0));
  api(`/v1/changes/counts?${withTenant(new URLSearchParams())}`)
    .then((c) => setBadge("changes", c && c.counts ? c.counts.pending || 0 : 0, { hot: true }))
    .catch(() => setBadge("changes", 0));
  if (scoped) return;
  api("/v1/catalog/datasets?limit=1000")
    .then((d) => setBadge("datasets", d && d.datasets ? d.datasets.length : 0))
    .catch(() => setBadge("datasets", 0));
  api("/v1/templates")
    .then((t) => { const l = (t && (t.templates || t)) || []; setBadge("templates", Array.isArray(l) ? l.length : 0); })
    .catch(() => setBadge("templates", 0));
  const t = tenantList();
  setBadge("tenants", t ? t.length : 0);
}

let version = null;
let ready = null;
async function loadVersion() {
  try {
    const resp = await fetch("/metrics");
    if (!resp.ok) return;
    const m = (await resp.text()).match(/faucet_build_info\{[^}]*version="([^"]+)"/);
    if (m) version = m[1];
  } catch { /* no metrics endpoint: the version is left out */ }
}

/** The server's readiness as last read (`/readyz`) and its version. */
export const readiness = () => ready;
export const serverVersion = () => version;

async function refreshStatus() {
  try { ready = await (await fetch("/readyz")).json(); } catch { ready = null; }
  const parts = [];
  if (ready) {
    const backend = ready.history_backend ? `history ${ready.history_backend}` : "history";
    parts.push(ready.history_ok ? html`<span><span class="dot"></span>${backend} · healthy</span>` : html`<span><span class="dot bad"></span>${backend} · degraded</span>`);
    if (ready.queue_ok === false) parts.push(html`<span><span class="dot warn"></span>run queue full</span>`);
    if (ready.draining) parts.push(html`<span><span class="dot warn"></span>draining</span>`);
    const cl = ready.cluster || {};
    if (cl.enabled) parts.push(html`<span>cluster · ${fmtInt(cl.instances || 0)} live</span>`);
    const me = whoami();
    const ident = document.getElementById("server-id");
    if (ident && !(me && me.tenant)) ident.textContent = cl.enabled ? `${location.host} · ${fmtInt(cl.instances || 0)} replicas` : location.host;
  } else {
    parts.push(html`<span><span class="dot bad"></span>server unreachable</span>`);
  }
  if (version) parts.push(html`<span>faucet serve ${version}</span>`);
  document.getElementById("status").innerHTML = v(join(parts));
}

function wireTop() {
  document.getElementById("theme-toggle").innerHTML = v(ic("theme"));
  const copy = document.getElementById("copy-link");
  copy.innerHTML = v(ic("link"));
  copy.onclick = async () => {
    try { await copyText(location.href); toast("Link to this page copied", "ok"); } catch { toast("Couldn’t copy the link", "bad"); }
  };
  const app = document.getElementById("app");
  const expand = document.getElementById("expand-btn");
  const apply = (on) => {
    app.dataset.expanded = String(on);
    expand.setAttribute("aria-pressed", String(on));
    expand.title = on ? "Show the sidebar" : "Hide the sidebar";
    expand.innerHTML = v(ic(on ? "collapse" : "expand"));
  };
  apply(readJson(EXPAND_KEY, false) === true);
  expand.onclick = () => { const on = app.dataset.expanded !== "true"; writeJson(EXPAND_KEY, on); apply(on); };
}

function paletteEntries() {
  const pages = [];
  for (const [group, items] of NAV) for (const [key, i, label, hash, perm] of items) pages.push({ group: "Pages", icon: i, title: label, meta: group, keys: key, go: hash, perm });
  const actions = [
    { group: "Actions", icon: "submit", title: "Submit a run", meta: "opens Submit", go: "#/submit", perm: "config_submit" },
    { group: "Actions", icon: "tpl", title: "Register a template", meta: "opens Templates", go: "#/templates?register=1", perm: "template_admin" },
    { group: "Actions", icon: "theme", title: "Switch light / dark", meta: "theme", run: () => document.getElementById("theme-toggle").click() },
    { group: "Actions", icon: "key", title: "Access token", meta: "opens Settings", go: "#/settings?token=1" },
    { group: "Actions", icon: "link", title: "Copy a link to this page", meta: "clipboard", run: () => document.getElementById("copy-link").click() },
  ];
  configurePalette({ pages, actions });
}

/** Build the shell once at boot, after identity and tenants are known. */
export async function startShell() {
  renderSide();
  wireTop();
  paletteEntries();
  window.addEventListener("hashchange", syncShell);
  syncShell();
  await loadVersion();
  refreshStatus();
  setInterval(refreshStatus, 30000);
}

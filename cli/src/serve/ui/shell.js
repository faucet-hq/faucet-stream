// The console shell (#998): grouped sidebar, breadcrumb top bar and status line.
// Views render into `#view` unchanged; the shell follows the hash route.
import { api, toast } from "./api.js";
import { icon, chip } from "./components.js";
import { escapeHtml, fmtInt } from "./utils.js";
import { tenantList, withTenant } from "./tenant.js";

// [group, [[key, label, hash, perm?]]]. An entry without a hash is a view this
// console does not have yet: listed (disabled) so the layout is stable.
const NAV = [
  ["Operate", [
    ["runs", "Runs", "#/runs"],
    ["changes", "Changes", "#/changes"],
    ["triggers", "Triggers"],
    ["schedules", "Schedules"],
  ]],
  ["Data", [
    ["datasets", "Datasets", "#/catalog"],
    ["lineage", "Lineage", "#/lineage"],
  ]],
  ["Build", [
    ["templates", "Templates", "#/templates"],
    ["schemas", "Schemas", "#/schemas"],
    ["submit", "Submit", "#/submit", "config_submit"],
  ]],
  ["Govern", [
    ["policy", "Policy"],
    ["usage", "Usage", "#/usage"],
    ["tenants", "Tenants", "#/tenants"],
  ]],
  ["Admin", [
    ["audit", "Audit log", null, "audit_read"],
    ["settings", "Settings"],
  ]],
];

const COLLAPSE_KEY = "faucet.nav.collapsed";
const EXPAND_KEY = "faucet.shell.expanded";

function readJson(key, fallback) {
  try {
    return JSON.parse(localStorage.getItem(key)) ?? fallback;
  } catch {
    return fallback;
  }
}
function writeJson(key, v) {
  try {
    localStorage.setItem(key, JSON.stringify(v));
  } catch {
    /* storage unavailable: the choice lasts for this page only */
  }
}

function renderNav() {
  const host = document.getElementById("side-nav");
  const collapsed = new Set(readJson(COLLAPSE_KEY, []));
  host.innerHTML = NAV.map(([group, items]) => {
    const rows = items
      .map(([key, label, hash, perm]) => {
        const p = perm ? ` data-perm="${perm}"` : "";
        const tip = ` title="${escapeHtml(label)}"`;
        if (!hash) {
          return `<span class="sh-nav is-disabled" id="nav-${key}" aria-disabled="true"${p} title="${escapeHtml(label)} — not in this console yet">${icon(key)}<span class="sh-nav-label">${escapeHtml(label)}</span><span class="sh-nav-soon">soon</span></span>`;
        }
        return `<a class="sh-nav" id="nav-${key}" href="${hash}" data-route="${hash}"${p}${tip}>${icon(key)}<span class="sh-nav-label">${escapeHtml(label)}</span><span class="sh-nav-n" hidden></span></a>`;
      })
      .join("");
    return `<div class="sh-grp" data-group="${group}" data-collapsed="${collapsed.has(group)}"><button type="button" class="sh-grp-h" aria-expanded="${!collapsed.has(group)}">${escapeHtml(group)}${icon("chev")}</button>${rows}</div>`;
  }).join("");
  host.querySelectorAll(".sh-grp-h").forEach((btn) => {
    btn.onclick = () => {
      const grp = btn.parentElement;
      const now = grp.dataset.collapsed !== "true";
      grp.dataset.collapsed = String(now);
      btn.setAttribute("aria-expanded", String(!now));
      const set = new Set(readJson(COLLAPSE_KEY, []));
      if (now) set.add(grp.dataset.group);
      else set.delete(grp.dataset.group);
      writeJson(COLLAPSE_KEY, [...set]);
    };
  });
  const tenants = document.getElementById("nav-tenants");
  if (tenants) tenants.hidden = tenantList() === null;
}

function entryFor(path) {
  const top = "#/" + (path.split("/")[1] || "runs");
  for (const [group, items] of NAV) {
    for (const [key, label, hash] of items) {
      if (hash === top) return { group, key, label, hash };
    }
  }
  return null;
}

function currentPath() {
  const hash = location.hash || "#/runs";
  const q = hash.indexOf("?");
  return q === -1 ? hash : hash.slice(0, q);
}

/** Follow the route: active nav item, breadcrumb, context chip. */
export function syncShell() {
  const path = currentPath();
  const entry = entryFor(path);
  document.querySelectorAll(".sh-nav[data-route]").forEach((a) => {
    const on = !!entry && a.dataset.route === entry.hash;
    a.classList.toggle("on", on);
    if (on) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });
  const crumb = document.getElementById("crumb");
  const rest = path.split("/").slice(2).map((s) => {
    try {
      return decodeURIComponent(s);
    } catch {
      return s;
    }
  });
  const sep = `<span class="sep">/</span>`;
  if (!entry) {
    crumb.innerHTML = `<b>Not found</b>`;
  } else if (!rest.length) {
    crumb.innerHTML = `<span>${escapeHtml(entry.group)}</span>${sep}<b>${escapeHtml(entry.label)}</b>`;
  } else {
    crumb.innerHTML = `<span>${escapeHtml(entry.group)}</span>${sep}<a href="${entry.hash}">${escapeHtml(entry.label)}</a>${sep}<b title="${escapeHtml(rest.join(" / "))}">${escapeHtml(rest.join(" / "))}</b>`;
  }
  document.title = entry ? `${rest.length ? rest[rest.length - 1] + " · " : ""}${entry.label} — faucet stream` : "faucet stream — console";
  setContext(rest.length && entry && entry.key === "runs" ? chip(`run ${shortId(rest[0])}`, "mono", rest[0]) : "");
  refreshBadges();
}

function shortId(id) {
  return id.length > 14 ? `${id.slice(0, 6)}…${id.slice(-4)}` : id;
}

/** Show a view's own name for the item the route selects (a run's name,
 *  not its id) as the last breadcrumb. */
export function setCrumbTitle(text) {
  const last = document.querySelector("#crumb > b:last-child");
  if (!last || !text) return;
  last.textContent = text;
  last.title = text;
  const entry = entryFor(currentPath());
  if (entry) document.title = `${text} · ${entry.label} — faucet stream`;
}

/** Fill the top bar's context chip slot (trusted markup; "" clears it). */
export function setContext(html) {
  const el = document.getElementById("ctx-chip");
  if (el) el.innerHTML = html || "";
}

function setBadge(key, n, { hot = false, more = false } = {}) {
  const el = document.querySelector(`#nav-${key} .sh-nav-n`);
  if (!el) return;
  if (!n) {
    el.hidden = true;
    return;
  }
  el.hidden = false;
  el.textContent = more ? `${fmtInt(n)}+` : fmtInt(n);
  el.classList.toggle("hot", hot);
}

let badgeTimer = null;
let badgeAt = 0;
/** Counts that are one cheap request each: running runs, pending changes and
 *  tenants. A failed request hides its badge. */
export function refreshBadges(force = false) {
  if (!force && Date.now() - badgeAt < 5000) return;
  badgeAt = Date.now();
  clearTimeout(badgeTimer);
  badgeTimer = setTimeout(() => refreshBadges(true), 30000);
  const runs = withTenant(new URLSearchParams({ status: "running", limit: "100" }));
  api(`/v1/runs?${runs}`)
    .then((r) => setBadge("runs", (r && r.runs ? r.runs.length : 0), { more: !!(r && r.next_cursor) }))
    .catch(() => setBadge("runs", 0));
  const changes = withTenant(new URLSearchParams({ status: "pending", limit: "100" }));
  api(`/v1/changes?${changes}`)
    .then((c) => setBadge("changes", Array.isArray(c) ? c.length : 0, { hot: true }))
    .catch(() => setBadge("changes", 0));
  const t = tenantList();
  setBadge("tenants", t ? t.length : 0);
}

let version = null;
async function loadVersion() {
  try {
    const resp = await fetch("/metrics");
    if (!resp.ok) return;
    const m = (await resp.text()).match(/faucet_build_info\{[^}]*version="([^"]+)"/);
    if (m) version = m[1];
  } catch {
    /* no metrics endpoint: the version is left out */
  }
}

async function refreshStatus() {
  const line = document.getElementById("status-line");
  const ident = document.getElementById("server-id");
  let ready = null;
  try {
    const resp = await fetch("/readyz");
    ready = await resp.json();
  } catch {
    ready = null;
  }
  const parts = [];
  if (ready) {
    const hist = ready.history_ok
      ? `<span><span class="sh-dot"></span>history · healthy</span>`
      : `<span><span class="sh-dot bad"></span>history · degraded</span>`;
    parts.push(hist);
    if (!ready.queue_ok) parts.push(`<span><span class="sh-dot warn"></span>run queue full</span>`);
    if (ready.draining) parts.push(`<span><span class="sh-dot warn"></span>draining</span>`);
    const cl = ready.cluster || {};
    if (cl.enabled) parts.push(`<span>cluster · ${fmtInt(cl.instances || 0)} live</span>`);
    if (ident) ident.textContent = cl.enabled ? `${location.host} · ${fmtInt(cl.instances || 0)} replicas` : location.host;
  } else if (ident) {
    ident.textContent = location.host;
  }
  if (version) parts.push(`<span>faucet serve ${escapeHtml(version)}</span>`);
  if (line) line.innerHTML = parts.join("");
}

// Clipboard API is unavailable on plain-http origins other than localhost.
function copyFallback(text) {
  const ta = document.createElement("textarea");
  ta.value = text;
  ta.setAttribute("readonly", "");
  ta.style.position = "fixed";
  ta.style.opacity = "0";
  document.body.appendChild(ta);
  ta.select();
  let ok = false;
  try {
    ok = document.execCommand("copy");
  } catch {
    ok = false;
  }
  ta.remove();
  return ok;
}

function wireTopButtons() {
  const theme = document.getElementById("theme-toggle");
  if (theme) theme.innerHTML = icon("theme");
  const tokenBtn = document.getElementById("token-btn");
  if (tokenBtn) tokenBtn.innerHTML = icon("key");
  const copy = document.getElementById("copy-link");
  copy.innerHTML = icon("link");
  copy.onclick = async () => {
    try {
      await navigator.clipboard.writeText(location.href);
      toast("Link copied");
    } catch {
      const ok = copyFallback(location.href);
      toast(ok ? "Link copied" : "Could not copy the link", ok ? "info" : "error");
    }
  };
  const app = document.getElementById("app");
  const expand = document.getElementById("expand-btn");
  const apply = (on) => {
    app.dataset.expanded = String(on);
    expand.setAttribute("aria-pressed", String(on));
    expand.title = on ? "Show the sidebar" : "Hide the sidebar";
    expand.innerHTML = icon(on ? "collapse" : "expand");
  };
  apply(readJson(EXPAND_KEY, false) === true);
  expand.onclick = () => {
    const on = app.dataset.expanded !== "true";
    writeJson(EXPAND_KEY, on);
    apply(on);
  };
}

/** Build the shell once at boot, after identity and tenants are known. */
export async function startShell() {
  renderNav();
  wireTopButtons();
  window.addEventListener("hashchange", syncShell);
  syncShell();
  await loadVersion();
  refreshStatus();
  setInterval(refreshStatus, 30000);
}

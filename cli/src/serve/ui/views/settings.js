// Settings (#998): canvas — who you are on this server, how the server is
// doing, and how this console behaves for you (saved in this browser).
import { api, token } from "../api.js";
import { html, v, ic, head, card, grid, kv, tbl, chip, field, select, btn, input, lnk } from "../components.js";
import { layout, dialog, confirmAction, toast, setQuery } from "../page.js";
import { setCrumbs, initials, serverVersion } from "../shell.js";
import { whoami, can } from "../access.js";
import { getTz, setTz, TZ_OPTIONS } from "../tz.js";
import { tenantList, currentTenant, setTenant, withTenant } from "../tenant.js";
import { refresh } from "../router.js";
import { href } from "../links.js";
import { fmtInt } from "../utils.js";

const THEME_KEY = "faucet.theme";
const theme = () => document.documentElement.dataset.theme || "auto";
function applyTheme(t) {
  document.documentElement.dataset.theme = t;
  try { localStorage.setItem(THEME_KEY, t); } catch { /* this page only */ }
}

/** The access-token dialog (also opened from the palette with ?token=1). */
async function askToken() {
  const r = await dialog(
    "Access token",
    html`<div>The bearer token sent on every API call. It is kept in this browser only.</div>${input("", { type: "password", ph: "Paste a bearer token", id: "token-input" })}`,
    html`${btn("Cancel", { kind: "quiet", data: { act: "cancel" } })}${btn("Save", { kind: "primary", data: { act: "save" } })}`,
    { icon: "key", onOpen: (el) => el.querySelector("#token-input").addEventListener("keydown", (e) => { if (e.key === "Enter") el.querySelector('[data-act="save"]').click(); }) },
  );
  if (r && r.act === "save") {
    const val = (r.values["token-input"] || "").trim();
    if (!val) return;
    token.set(val);
    location.reload();
  }
}

const KEYS = [
  [html`<kbd>⌘</kbd> <kbd>K</kbd>`, "Go to any page, run, template, dataset or tenant"],
  [html`<kbd>j</kbd> <kbd>k</kbd>`, "Move down and up a list"],
  [html`<kbd>↵</kbd>`, "Open the highlighted item"],
  [html`<kbd>esc</kbd>`, "Back to the list, or close a menu"],
  [html`<kbd>/</kbd>`, "Search the current list"],
];

export async function renderSettings(host, params) {
  const { detail } = layout(host, "canvas");
  setCrumbs(["Admin", "Settings"]);
  let ready = null;
  let running = null;
  let reload = null; // { path, at, by } after a reload in this session

  const me = whoami();
  const identity = () => {
    if (!me) return card({ icon: "user", title: "You", body: html`<div class="empty-line">This server did not say who you are (no <code>/v1/whoami</code>). Every control stays visible; the server still refuses what you may not do.</div>` });
    const viewer = me.role === "viewer";
    const role = me.tenant ? `tenant ${me.tenant}` : viewer ? "viewer · read-only" : me.role;
    const perms = (me.permissions || []).filter((p) => p !== "identity");
    return card({ icon: "user", title: "You", right: can("audit_read") ? lnk("Audit log for you", href.audit({ principal: me.principal })) : "",
      body: html`<div class="set-who"><span class="avatar">${initials(me.principal)}</span><div><b>${me.principal}</b><div class="muted">${role} · ${token.get() ? "signed in with a bearer token" : "no token: the server allows anonymous access"}</div></div></div>
        <div class="set-perms">${perms.map((p) => chip(p, "mono"))}</div>`,
      note: viewer ? "Controls you cannot use are hidden across the console. An admin can change your role in the auth config." : html`${perms.length} permissions · every route still checks its own.` });
  };

  const prefs = () => {
    const t = theme();
    const seg = html`<span class="seg" role="radiogroup" aria-label="Theme">${[["light", "Light"], ["dark", "Dark"], ["auto", "Auto"]].map(([val, l]) => html`<span role="radio" tabindex="0" aria-checked="${String(t === val)}" class="${t === val ? "on" : ""}" data-theme-set="${val}">${l}</span>`)}</span>`;
    const tenants = tenantList();
    const scoped = me && me.tenant;
    const sidebarOn = document.getElementById("app").dataset.expanded !== "true";
    return card({ icon: "theme", title: "This console", aside: "saved in this browser",
      body: html`<div class="form">${field("Theme", seg)}${field("Times shown in", select(TZ_OPTIONS.map((o) => [o.value, o.value === "local" ? `Local${Intl.DateTimeFormat().resolvedOptions().timeZone ? ` (${Intl.DateTimeFormat().resolvedOptions().timeZone})` : ""}` : o.value === "UTC" ? "UTC" : o.value]), getTz(), { name: "tz" }), { help: "Every time and the date picker follow this" })}
        ${tenants !== null && !scoped ? field("Tenant scope", select([["", "All tenants"], ...tenants.map((x) => [x.id, x.name || x.id])], currentTenant(), { name: "tenant" }), { help: "Scopes Runs, Usage and Changes" }) : scoped ? field("Tenant scope", input(me.tenant, { a: { disabled: true }, cls: "dis" }), { help: "Your token is scoped to this tenant" }) : ""}
        ${field("Sidebar", html`<label class="toggle${sidebarOn ? " on" : ""}"><input type="checkbox" name="sidebar"${sidebarOn ? " checked" : ""}><i></i>Show the sidebar</label>`)}</div>` });
  };

  const tokenCard = () => card({ icon: "key", title: "Access token", aside: "sent as a bearer token on every API call",
    body: html`<div class="form one">${field("Token", html`<div style="display:flex;gap:8px"><label class="input" style="flex:1"><input type="password" value="${token.get() ? "•".repeat(24) : ""}" placeholder="No token: anonymous" readonly aria-label="Stored token"></label>${btn(token.get() ? "Replace" : "Add a token", { data: { act: "token" } })}${token.get() ? btn("Clear", { kind: "quiet", data: { act: "clear-token" } }) : ""}</div>`, { help: "Kept in this browser only. Clearing it signs you out." })}</div>` });

  const server = () => {
    const r = ready;
    const cl = (r && r.cluster) || {};
    const members = cl.members || [];
    const facts = r
      ? [
          kv("History", r.history_ok ? html`<span class="ok">healthy</span>` : html`<span class="bad">degraded</span>`, r.history_backend || ""),
          kv("Run queue", r.queue_ok === false ? html`<span class="warn">full</span>` : html`<span class="ok">accepting</span>`, running != null ? `${fmtInt(running)} running` : ""),
          kv("Draining", r.draining ? html`<span class="warn">yes</span>` : "no"),
          kv("Cluster", cl.enabled ? `${fmtInt(cl.instances || members.length)} replicas` : "single server", (cl.pools || []).length ? `pools ${(cl.pools || []).join(", ")}` : ""),
        ]
      : [kv("Server", html`<span class="bad">unreachable</span>`, "/readyz did not answer")];
    return card({ icon: "server", title: "Server", right: serverVersion() ? `faucet serve ${serverVersion()}` : "",
      body: html`${grid(4, facts)}${members.length ? tbl(["Replica", "Listens on", "Pools", ["Running", "num"], ["Max", "num"]], members.map((m) => [html`<span class="mono" title="${m.instance_id}">${String(m.instance_id || "").slice(0, 12)}</span>`, html`<span class="mono">${m.listen || "—"}</span>`, (m.pools || ["default"]).join(", "), fmtInt(m.in_flight ?? 0), fmtInt(m.max_concurrent ?? 0)])) : ""}` });
  };

  const admin = () => card({ icon: "refresh", title: "Default config", aside: "the merge base every submitted config starts from", right: btn("Reload", { icon: "refresh", perm: "reload", data: { act: "reload" } }),
    body: grid(3, [
      kv("File", reload && reload.path ? html`<span class="mono">${reload.path}</span>` : reload && reload.none ? "none" : html`<span class="muted">—</span>`, reload && reload.none ? "this server has no --default-config" : reload ? "" : "shown after a reload"),
      kv("Loaded", reload && reload.none ? "—" : reload ? "just now" : "at startup", reload && !reload.none ? `by ${me ? me.principal : "you"}` : ""),
      kv("Runs in flight", running != null ? fmtInt(running) : "—", "keep the config they started with"),
    ]),
    note: reload && !reload.none ? html`<span class="ok">Reloaded.</span> The new defaults apply to runs submitted from now on.` : reload && reload.none ? "Start the server with --default-config to give every submitted config a shared base." : "Reload re-reads and validates the file; an invalid file is refused and the current one stays." });

  const keys = () => card({ icon: "key", title: "Keyboard", body: html`<table class="set-kbd"><tbody>${KEYS.map(([k, d]) => html`<tr><td>${k}</td><td>${d}</td></tr>`)}</tbody></table>` });

  function paint() {
    detail.innerHTML = v(html`
      ${head("Settings", [], "Who you are on this server, how the server is doing, and how this console behaves for you.")}
      <div class="set-grid">
        <div class="stack">${identity()}${prefs()}${tokenCard()}</div>
        <div class="stack">${server()}${can("reload") && !(me && me.tenant) ? admin() : ""}${keys()}</div>
      </div>`);
  }

  detail.addEventListener("click", async (e) => {
    const t = e.target.closest("[data-theme-set]");
    if (t) { applyTheme(t.dataset.themeSet); paint(); return; }
    const a = e.target.closest("[data-act]");
    if (!a) return;
    if (a.dataset.act === "token") askToken();
    else if (a.dataset.act === "clear-token") {
      if (await confirmAction("Clear the access token?", "The console signs out and asks for a token on the next call that needs one.", { label: "Clear token", danger: true })) { token.clear(); location.reload(); }
    } else if (a.dataset.act === "reload") {
      a.disabled = true;
      try {
        const res = await api("/v1/reload", { method: "POST" });
        if (res && res.reloaded) {
          reload = { path: res.path, at: new Date().toISOString() };
          toast(html`Reloaded <span class="mono">${res.path}</span>`, "ok");
        } else {
          reload = { none: true };
          toast((res && res.reason) || "Nothing to reload", "info");
        }
      } catch (err) {
        toast(`Reload refused: ${err.message}`, "bad");
      }
      paint();
    }
  });
  detail.addEventListener("keydown", (e) => {
    const t = e.target.closest("[data-theme-set]");
    if (t && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); t.click(); }
  });
  detail.addEventListener("change", (e) => {
    const n = e.target.name;
    if (n === "tz") {
      setTz(e.target.value);
      const top = document.getElementById("tz-select");
      if (top) top.value = e.target.value;
    } else if (n === "tenant") {
      setTenant(e.target.value);
    } else if (n === "sidebar") {
      document.getElementById("expand-btn").click();
      e.target.closest(".toggle").classList.toggle("on", e.target.checked);
    }
  });

  paint();
  const [r, runs] = await Promise.all([
    fetch("/readyz").then((x) => x.json()).catch(() => null),
    api(`/v1/runs?${withTenant(new URLSearchParams({ status: "running,sharded", limit: "100", counts: "true" }))}`).catch(() => null),
  ]);
  ready = r;
  running = runs ? (runs.counts ? (runs.counts.running || 0) + (runs.counts.sharded || 0) : (runs.runs || []).length) : null;
  paint();
  if (params.query.get("token") === "1") {
    setQuery({ token: "" }, { replace: true });
    askToken();
  }
  return { update(p) { if (p.query.get("token") === "1") { setQuery({ token: "" }, { replace: true }); askToken(); } } };
}

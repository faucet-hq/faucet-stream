// The page pattern (#998): page shape, URL state, loading, polling, overlays and
// the behaviours every page gets for free (copy, row links, menus, keyboard,
// collapsible sections). See docs/contributing/console-design.md.
import { navigate } from "./router.js";
import { html, raw, v, ic, alert, btn, esc } from "./components.js";
import { copyText, fmtAge } from "./utils.js";
import { formatTs } from "./tz.js";

// ---- shape ----------------------------------------------------------------------
// The body is one of two shapes: `split` (list + detail) or `canvas` (one
// column, optionally with a side panel). A page calls `layout()` once and
// renders into the parts it gets back.

/** Lay out the page body. Returns `{ list, detail, panel, select(on) }`. */
export function layout(host, shape = "split") {
  const body = document.getElementById("view");
  body.className = "body";
  if (shape !== "split") body.classList.add("canvas");
  if (shape === "canvas-panel") body.classList.add("side-panel");
  host.innerHTML = `${shape === "split" ? '<div class="list"></div>' : ""}<div class="detail"></div>${shape === "canvas-panel" ? '<div class="panel"></div>' : ""}`;
  return {
    list: host.querySelector(".list"),
    detail: host.querySelector(".detail"),
    panel: host.querySelector(".panel"),
    /** On a narrow window a selected item shows its detail instead of the list. */
    select(on) { body.classList.toggle("has-sel", !!on); },
  };
}

// ---- URL state ------------------------------------------------------------------
// Filters, tabs and selection live in the hash (`#/path?k=v`) so every view is
// linkable and the back button works. A view that returns `update(params)`
// keeps its DOM when only the query (or the selected item) changes.

export function currentHash() {
  const h = location.hash || "#/overview";
  const q = h.indexOf("?");
  return { path: q === -1 ? h : h.slice(0, q), query: new URLSearchParams(q === -1 ? "" : h.slice(q + 1)) };
}

/** A hash for `path` with `query` (URLSearchParams or object; empty values dropped). */
export function hashFor(path, query = {}) {
  const q = new URLSearchParams();
  const entries = query instanceof URLSearchParams ? [...query.entries()] : Object.entries(query);
  for (const [k, x] of entries) {
    if (Array.isArray(x)) x.forEach((y) => y !== "" && y != null && q.append(k, y));
    else if (x !== "" && x != null && x !== false) q.set(k, String(x));
  }
  const s = q.toString();
  return s ? `${path}?${s}` : path;
}

/** Merge `patch` into the current query (null / "" removes a key). `replace`
 *  rewrites the URL without a history entry (typing in a search box). */
export function setQuery(patchObj, { replace = false, path = null } = {}) {
  const cur = currentHash();
  const q = new URLSearchParams(cur.query);
  for (const [k, x] of Object.entries(patchObj)) {
    q.delete(k);
    if (Array.isArray(x)) x.forEach((y) => y !== "" && y != null && q.append(k, y));
    else if (x !== "" && x != null && x !== false) q.set(k, String(x));
  }
  const next = hashFor(path ?? cur.path, q);
  if (next === (location.hash || "")) return;
  if (replace) {
    history.replaceState(null, "", next);
    window.dispatchEvent(new HashChangeEvent("hashchange"));
  } else navigate(next);
}

/** `path` with the current query kept (moving between items of one list). */
export function withQuery(path, drop = ["tab"]) {
  const q = new URLSearchParams(currentHash().query);
  drop.forEach((k) => q.delete(k));
  const s = q.toString();
  return s ? `${path}?${s}` : path;
}

// ---- data ----------------------------------------------------------------------------

/** Render `skel` into `host`, await `fn()`; on failure render a quiet alert with
 *  Retry. Resolves to the value, or `undefined` after a failure. */
export async function load(host, fn, { skel = "", what = "this page", onRetry = null } = {}) {
  if (host && skel) host.innerHTML = v(skel);
  try {
    return await fn();
  } catch (e) {
    if (host) {
      host.innerHTML = v(errorAlert(what, e));
      host.querySelector("[data-retry]").onclick = () => (onRetry ? onRetry() : load(host, fn, { skel, what }));
    }
    return undefined;
  }
}

/** The standard load-failure alert. */
export const errorAlert = (what, e) =>
  alert(`Couldn’t load ${what}.`, e && e.status ? `The server answered ${e.status}: ${e.message}.` : (e && e.message) || String(e || ""), "bad", btn("Retry", { icon: "refresh", cls: "sm", data: { retry: "1" } }));

/** Call `fn` every `ms` while the tab is visible; returns `stop()`. A failing
 *  call keeps the last good state and is tried again on the next tick. */
export function poll(fn, ms) {
  let timer = null;
  let stopped = false;
  const tick = async () => {
    if (stopped) return;
    if (document.visibilityState === "visible") {
      try { await fn(); } catch { /* keep the last good state */ }
    }
    if (!stopped) timer = setTimeout(tick, typeof ms === "function" ? ms() : ms);
  };
  timer = setTimeout(tick, typeof ms === "function" ? ms() : ms);
  const onVis = () => { if (document.visibilityState === "visible" && !stopped) { clearTimeout(timer); tick(); } };
  document.addEventListener("visibilitychange", onVis);
  return () => { stopped = true; clearTimeout(timer); document.removeEventListener("visibilitychange", onVis); };
}

/** Replace `el`'s markup only when it changed, keeping scroll and the focused
 *  control (by id or name). Returns whether anything changed. */
export function patch(el, markup) {
  const s = v(markup);
  if (!el || el.__html === s) return false;
  const top = el.scrollTop;
  const f = document.activeElement && el.contains(document.activeElement) ? document.activeElement : null;
  const key = f && (f.id ? `#${CSS.escape(f.id)}` : f.name ? `[name="${CSS.escape(f.name)}"]` : "");
  el.innerHTML = s;
  el.__html = s;
  el.scrollTop = top;
  if (key) el.querySelector(key)?.focus();
  return true;
}

// ---- time ------------------------------------------------------------------------------

/** Relative age ("14m") with the absolute time (display timezone) on hover. */
export const ago = (iso, suffix = "") =>
  iso ? html`<time datetime="${iso}" title="${formatTs(iso)}">${fmtAge(iso)}${suffix}</time>` : raw("");
/** Absolute time in the display timezone. */
export const at = (iso) => (iso ? html`<time datetime="${iso}">${formatTs(iso)}</time>` : raw("—"));

// ---- overlays --------------------------------------------------------------------------

const overlayHost = () => document.querySelector(".main");

/** A modal dialog. `body` and `actions` are markup. Resolves with
 *  `{ act, values }` — the clicked control's `data-act` and the dialog's named
 *  fields (by `name`, else `id`) — or `null` on Esc / scrim click. `onOpen(el)`
 *  wires fields. A control with `data-keep` handles its own click. */
export function dialog(title, body, actions, { icon: i = "", wide = false, onOpen = null } = {}) {
  return new Promise((resolve) => {
    const wrap = document.createElement("div");
    wrap.className = "scrim";
    wrap.innerHTML = v(html`<div class="dialog${wide ? " wide" : ""}" role="dialog" aria-modal="true" aria-label="${title}"><div class="dh">${i ? ic(i) : ""}${title}</div><div class="db">${body}</div><div class="actions">${actions}</div></div>`);
    const values = () => Object.fromEntries([...wrap.querySelectorAll("input, select, textarea")].filter((el) => el.name || el.id).map((el) => [el.name || el.id, el.type === "checkbox" ? el.checked : el.value]));
    const done = (val) => { const out = val == null ? null : { act: val, values: values() }; wrap.remove(); document.removeEventListener("keydown", onKey, true); resolve(out); };
    const onKey = (e) => { if (e.key === "Escape") { e.stopPropagation(); done(null); } };
    wrap.addEventListener("click", (e) => {
      if (e.target === wrap) return done(null);
      const b = e.target.closest("[data-act]");
      if (b && !b.disabled && !b.hasAttribute("data-keep")) done(b.dataset.act);
    });
    document.addEventListener("keydown", onKey, true);
    overlayHost().appendChild(wrap);
    if (onOpen) onOpen(wrap.querySelector(".dialog"));
    (wrap.querySelector("input, textarea, select") || wrap.querySelector(".actions .btn:last-child"))?.focus();
  });
}

/** Ask to confirm a destructive or important action. Resolves true/false. */
export async function confirmAction(title, text, { label = "Confirm", danger = false, icon: i = "alert" } = {}) {
  const r = await dialog(title, html`<div>${text}</div>`, html`${btn("Cancel", { kind: "quiet", data: { act: "no" } })}${btn(label, { kind: danger ? "danger" : "primary", data: { act: "yes" } })}`, { icon: i });
  return !!r && r.act === "yes";
}

/** A toast: tone ok | bad | info | warn; `link` = {href, label}. */
export function toast(text, tone = "ok", link = null) {
  let host = document.getElementById("toasts");
  if (!host) {
    host = document.createElement("div");
    host.id = "toasts";
    host.className = "toasts";
    overlayHost().appendChild(host);
  }
  const el = document.createElement("div");
  el.className = `toast ${tone}`;
  el.setAttribute("role", tone === "bad" ? "alert" : "status");
  el.innerHTML = v(html`${ic(tone === "ok" ? "check" : tone === "bad" ? "x" : "info")}<span>${text}${link ? html` · <a class="lnk" href="${link.href}">${link.label}</a>` : ""}</span>`);
  host.appendChild(el);
  setTimeout(() => el.remove(), tone === "bad" ? 8000 : 5000);
}

// ---- collapsible sections ------------------------------------------------------------

/** Remembered open/closed state for `sect()` headers under one storage key. */
export function sections(storageKey) {
  let closed;
  try { closed = new Set(JSON.parse(localStorage.getItem(storageKey)) || []); } catch { closed = new Set(); }
  return {
    isClosed: (sid) => closed.has(sid),
    toggle(sid) {
      if (closed.has(sid)) closed.delete(sid); else closed.add(sid);
      try { localStorage.setItem(storageKey, JSON.stringify([...closed])); } catch { /* this page only */ }
      return closed.has(sid);
    },
  };
}

// ---- global behaviours ---------------------------------------------------------------

let installed = false;
/** Delegated behaviours, wired once: copy, table-row links, menus, section
 *  toggles, list keyboard (j/k/Enter/Esc, `/`). */
export function installGlobalBehaviours() {
  if (installed) return;
  installed = true;
  document.addEventListener("click", async (e) => {
    const c = e.target.closest("[data-copy]");
    if (c) {
      e.preventDefault();
      e.stopPropagation();
      try {
        await copyText(c.dataset.copy);
        c.classList.add("done");
        const old = c.innerHTML;
        c.innerHTML = v(ic("check"));
        setTimeout(() => { c.classList.remove("done"); c.innerHTML = old; }, 1200);
      } catch {
        toast("Couldn’t copy to the clipboard", "bad");
      }
      return;
    }
    const tr = e.target.closest("tr[data-href]");
    if (tr && !e.target.closest("a, button, input, select, textarea, summary")) {
      if (e.metaKey || e.ctrlKey) window.open(tr.dataset.href, "_blank");
      else navigate(tr.dataset.href);
    }
    const s = e.target.closest(".sect[data-sect]");
    if (s) {
      const next = s.nextElementSibling;
      const closed = !s.classList.contains("closed");
      s.classList.toggle("closed", closed);
      s.setAttribute("aria-expanded", String(!closed));
      if (next && next.classList.contains("sect-rows")) next.classList.toggle("closed", closed);
      s.dispatchEvent(new CustomEvent("sect-toggle", { bubbles: true, detail: { id: s.dataset.sect, closed } }));
    }
    const g = e.target.closest(".grp h6");
    if (g) g.parentElement.dispatchEvent(new CustomEvent("grp-toggle", { bubbles: true }));
    // Menus close on an outside click or after an item is chosen.
    document.querySelectorAll("details.menu[open]").forEach((d) => {
      if (!d.contains(e.target) || e.target.closest(".mi")) d.open = false;
    });
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") document.querySelectorAll("details.menu[open]").forEach((d) => (d.open = false));
    listKeys(e);
  });
}

const typing = (el) => el && (el.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName));

function listKeys(e) {
  if (e.metaKey || e.ctrlKey || e.altKey || document.querySelector(".scrim")) return;
  if (typing(document.activeElement)) {
    if (e.key === "Escape") document.activeElement.blur();
    return;
  }
  const list = document.querySelector("#view .list");
  if (e.key === "/" ) {
    const s = document.querySelector("#view .list input[type=search], #view .list input[name=q], #view .detail input[type=search]");
    if (s) { e.preventDefault(); s.focus(); s.select(); }
    return;
  }
  if (!list) return;
  const rows = [...list.querySelectorAll(".row")].filter((r) => r.offsetParent);
  const cur = list.querySelector(".row.kb") || list.querySelector(".row.on");
  const i = rows.indexOf(cur);
  const go = (n) => {
    const r = rows[Math.max(0, Math.min(rows.length - 1, n))];
    if (!r) return;
    rows.forEach((x) => x.classList.remove("kb"));
    r.classList.add("kb");
    r.scrollIntoView({ block: "nearest" });
  };
  if (e.key === "j") { e.preventDefault(); go(i + 1); }
  else if (e.key === "k") { e.preventDefault(); go(i < 0 ? 0 : i - 1); }
  else if (e.key === "Enter" && cur && cur.classList.contains("kb")) { e.preventDefault(); cur.click(); }
  else if (e.key === "Escape") {
    const back = document.querySelector("#view .detail .back");
    if (back && document.getElementById("view").classList.contains("has-sel") && back.offsetParent) navigate(back.getAttribute("href"));
  }
}

export { esc };

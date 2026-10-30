// Command palette (⌘K / Ctrl+K, or "Go to…" in the sidebar): jump to a page, run,
// dataset, template, change or tenant, or run a console action. Data comes from
// the list endpoints the pages already use, cached for 30 s.
import { api } from "./api.js";
import { html, ic, v, esc, shortId } from "./components.js";
import { fmtAge } from "./utils.js";
import { navigate } from "./router.js";
import { href, datasetName, datasetScheme } from "./links.js";
import { withTenant, tenantList } from "./tenant.js";
import { can } from "./access.js";

let pages = [];
let actions = [];
let cache = null;
let cacheAt = 0;
let scrim = null;
let items = [];
let sel = 0;

/** Pages and actions come from the shell (one source of truth for the nav). */
export function configurePalette({ pages: p, actions: a }) {
  pages = p;
  actions = a;
}

async function fetchAll() {
  if (cache && Date.now() - cacheAt < 30000) return cache;
  const safe = (p) => p.catch(() => null);
  const [runs, templates, datasets, changes] = await Promise.all([
    safe(api(`/v1/runs?${withTenant(new URLSearchParams({ limit: "200" }))}`)),
    safe(api("/v1/templates")),
    safe(api("/v1/catalog/datasets?limit=500")),
    safe(api(`/v1/changes?${withTenant(new URLSearchParams({ limit: "100" }))}`)),
  ]);
  const out = [];
  for (const r of (runs && runs.runs) || []) {
    out.push({ group: "Runs", icon: "runs", title: r.name || r.run_id, meta: `${r.status} · ${shortId(r.run_id)} · ${fmtAge(r.submitted_at)} ago`, keys: `${r.run_id} ${Object.values(r.labels || {}).join(" ")}`, go: href.run(r.run_id) });
  }
  for (const d of (datasets && datasets.datasets) || []) {
    out.push({ group: "Datasets", icon: "db", title: datasetName(d.uri), meta: `${datasetScheme(d.uri) || d.kind} · ${d.role || d.kind || ""}`, keys: `${d.uri} ${d.kind} ${d.id}`, go: href.dataset(d.id) });
  }
  const tpls = (templates && (templates.templates || templates)) || [];
  for (const t of Array.isArray(tpls) ? tpls : []) {
    const st = t.stable != null ? `stable v${t.stable}` : t.status || "";
    out.push({ group: "Templates", icon: "tpl", title: t.id, meta: `${(t.kind || "pipeline").replace("-template", "")} · ${st}`, keys: `${t.name || ""} ${t.description || ""}`, go: href.template(t.id) });
  }
  for (const c of Array.isArray(changes) ? changes : (changes && changes.changes) || []) {
    out.push({ group: "Changes", icon: "changes", title: c.reason || `${c.kind} ${shortId(c.id)}`, meta: `${c.status} · ${c.requester || ""}`, keys: `${c.id} ${c.kind}`, go: href.change(c.id) });
  }
  for (const t of tenantList() || []) {
    out.push({ group: "Tenants", icon: "tenants", title: t.name || t.id, meta: t.id, keys: t.id, go: href.tenant(t.id) });
  }
  cache = out;
  cacheAt = Date.now();
  return out;
}

function score(item, terms) {
  const hay = `${item.title} ${item.keys || ""} ${item.meta || ""}`.toLowerCase();
  let s = 0;
  for (const t of terms) {
    const i = hay.indexOf(t);
    if (i < 0) return -1;
    s += i === 0 ? 3 : item.title.toLowerCase().includes(t) ? 2 : 1;
  }
  return s;
}

const ORDER = ["Pages", "Runs", "Datasets", "Templates", "Changes", "Tenants", "Actions"];
function filter(all, q) {
  const terms = q.toLowerCase().split(/\s+/).filter(Boolean);
  const base = [...pages.filter((p) => !p.perm || can(p.perm)), ...actions.filter((a) => !a.perm || can(a.perm))];
  let list;
  if (!terms.length) list = [...all.filter((i) => i.group === "Runs").slice(0, 5), ...base];
  else {
    const scored = [...base, ...all].map((i) => [score(i, terms), i]).filter(([s]) => s >= 0);
    scored.sort((a, b) => b[0] - a[0]);
    const per = {};
    list = scored.map(([, i]) => i).filter((i) => (per[i.group] = (per[i.group] || 0) + 1) <= 6);
  }
  return list.sort((a, b) => ORDER.indexOf(a.group) - ORDER.indexOf(b.group));
}

function mark(title, terms) {
  let out = esc(title);
  for (const t of terms) {
    if (!t) continue;
    out = out.replace(new RegExp(t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "ig"), (m) => `<mark>${m}</mark>`);
  }
  return out;
}

function render(list, el, q) {
  items = list;
  if (sel >= items.length) sel = Math.max(0, items.length - 1);
  if (!items.length) {
    el.innerHTML = v(html`<div class="empty"><b>Nothing matches.</b><span>Try a run name, a template id or a dataset path.</span></div>`);
    return;
  }
  const terms = q.toLowerCase().split(/\s+/).filter(Boolean).map((t) => esc(t));
  let last = "";
  el.innerHTML = items.map((it, i) => {
    const g = it.group !== last ? `<div class="pg">${esc(it.group)}</div>` : "";
    last = it.group;
    return `${g}<div class="pi${i === sel ? " on" : ""}" role="option" aria-selected="${i === sel}" data-i="${i}">${v(ic(it.icon))}<span>${mark(it.title, terms)}</span>${it.meta ? `<span class="m">${esc(it.meta)}</span>` : ""}</div>`;
  }).join("");
  el.querySelector(".pi.on")?.scrollIntoView({ block: "nearest" });
}

function choose(it, newTab = false) {
  closePalette();
  if (!it) return;
  if (it.run) it.run();
  else if (it.go) (newTab ? window.open(it.go, "_blank") : navigate(it.go));
}

export function closePalette() {
  if (scrim) scrim.remove();
  scrim = null;
}

export async function openPalette() {
  if (scrim) return;
  scrim = document.createElement("div");
  scrim.className = "scrim";
  scrim.innerHTML = v(html`<div class="pal" role="dialog" aria-label="Go to"><div class="pin">${ic("search")}<input type="text" placeholder="Go to a run, dataset, template, change, tenant or page…" aria-label="Search the console" autocomplete="off" spellcheck="false"></div><div class="pl" role="listbox"></div><div class="pf"><span><kbd>↑</kbd> <kbd>↓</kbd> move</span><span><kbd>↵</kbd> open</span><span><kbd>⌘↵</kbd> new tab</span><span><kbd>esc</kbd> close</span></div></div>`);
  document.querySelector(".main").appendChild(scrim);
  scrim.addEventListener("mousedown", (e) => { if (e.target === scrim) closePalette(); });
  const input = scrim.querySelector("input");
  const list = scrim.querySelector(".pl");
  sel = 0;
  input.focus();
  let all = [];
  render(filter(all, ""), list, "");
  fetchAll().then((x) => { all = x; if (scrim) render(filter(all, input.value), list, input.value); });
  input.oninput = () => { sel = 0; render(filter(all, input.value), list, input.value); };
  input.onkeydown = (e) => {
    if (e.key === "ArrowDown") { e.preventDefault(); sel = Math.min(items.length - 1, sel + 1); render(items, list, input.value); }
    else if (e.key === "ArrowUp") { e.preventDefault(); sel = Math.max(0, sel - 1); render(items, list, input.value); }
    else if (e.key === "Enter") { e.preventDefault(); choose(items[sel], e.metaKey || e.ctrlKey); }
    else if (e.key === "Escape") { e.preventDefault(); closePalette(); }
  };
  list.onclick = (e) => { const r = e.target.closest(".pi"); if (r) choose(items[Number(r.dataset.i)], e.metaKey || e.ctrlKey); };
  list.onmousemove = (e) => {
    const r = e.target.closest(".pi");
    if (r && Number(r.dataset.i) !== sel) { sel = Number(r.dataset.i); list.querySelectorAll(".pi").forEach((x) => x.classList.toggle("on", x === r)); }
  };
}

/** ⌘K / Ctrl+K anywhere opens (or closes) the palette. */
export function installPalette() {
  document.addEventListener("keydown", (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
      e.preventDefault();
      if (scrim) closePalette();
      else openPalette();
    }
  });
}

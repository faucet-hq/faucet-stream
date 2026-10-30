// Shared console components (#998): small functions that return markup, so a
// view composes them instead of writing its own. Every text argument is escaped
// here; arguments named `html` (and nested component output) are trusted markup.
import { escapeHtml, fmtInt } from "./utils.js";

const PATHS = {
  overview: '<rect x="3" y="3" width="8" height="10" rx="1.5"/><rect x="13" y="3" width="8" height="6" rx="1.5"/><rect x="13" y="11" width="8" height="10" rx="1.5"/><rect x="3" y="15" width="8" height="6" rx="1.5"/>',
  runs: '<path d="M5 4l14 8-14 8z"/>',
  changes: '<path d="M6 3v12"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="6" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/>',
  triggers: '<path d="M13 2L4 14h7l-1 8 9-12h-7z"/>',
  schedules: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  datasets: '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
  lineage: '<circle cx="5" cy="6" r="2.5"/><circle cx="5" cy="18" r="2.5"/><circle cx="19" cy="12" r="2.5"/><path d="M7.5 6h3a4 4 0 0 1 4 4v0M7.5 18h3a4 4 0 0 0 4-4"/>',
  templates: '<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><path d="M17.5 14v7M14 17.5h7"/>',
  schemas: '<path d="M8 3H6a2 2 0 0 0-2 2v4l-2 3 2 3v4a2 2 0 0 0 2 2h2M16 3h2a2 2 0 0 1 2 2v4l2 3-2 3v4a2 2 0 0 1-2 2h-2"/>',
  submit: '<path d="M12 19V5M5 12l7-7 7 7"/>',
  policy: '<path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/>',
  usage: '<path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/>',
  tenants: '<circle cx="9" cy="8" r="3.5"/><path d="M2.5 20a6.5 6.5 0 0 1 13 0"/><path d="M16 4.5a3.5 3.5 0 0 1 0 7M21.5 20a6.5 6.5 0 0 0-4-6"/>',
  audit: '<path d="M9 4h10v16H5V8z"/><path d="M9 4v4H5M9 13h6M9 17h6"/>',
  settings: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 0 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 0 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 0 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 0 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z"/>',
  link: '<path d="M10 14a4 4 0 0 0 5.7 0l3-3a4 4 0 0 0-5.7-5.7l-1 1M14 10a4 4 0 0 0-5.7 0l-3 3a4 4 0 0 0 5.7 5.7l1-1"/>',
  expand: '<path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7"/>',
  collapse: '<path d="M4 14h6v6M20 10h-6V4M14 10l7-7M3 21l7-7"/>',
  chev: '<path d="M6 9l6 6 6-6"/>',
  logs: '<path d="M4 6h16M4 12h16M4 18h10"/>',
  key: '<circle cx="7.5" cy="15.5" r="4.5"/><path d="M10.7 12.3L21 2M17 6l3 3M14 9l2 2"/>',
  theme: '<circle cx="12" cy="12" r="9"/><path d="M12 3a9 9 0 0 0 0 18z" fill="currentColor"/>',
  check: '<path d="M20 6L9 17l-5-5"/>',
  x: '<path d="M18 6L6 18M6 6l12 12"/>',
  play: '<path d="M6 4l14 8-14 8z"/>',
  filter: '<path d="M3 5h18M6 12h12M10 19h4"/>',
  refresh: '<path d="M20 11a8 8 0 1 0-2.3 5.7M20 4v7h-7"/>',
  back: '<path d="M15 6l-6 6 6 6"/>',
  undo: '<path d="M9 14L4 9l5-5"/><path d="M4 9h10a6 6 0 0 1 0 12h-3"/>',
  trash: '<path d="M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  matrix: '<rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18M3 15h18M9 3v18M15 3v18"/>',
  sync: '<path d="M4 12a8 8 0 0 1 13.7-5.7M20 12a8 8 0 0 1-13.7 5.7"/><path d="M18 3v4h-4M6 21v-4h4"/>',
  graph: '<circle cx="5" cy="6" r="2.5"/><circle cx="5" cy="18" r="2.5"/><circle cx="19" cy="12" r="2.5"/><path d="M7.5 6h3a4 4 0 0 1 4 4v0M7.5 18h3a4 4 0 0 0 4-4"/>',
  db: '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
  user: '<circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0 1 16 0"/>',
};

/** An inline stroke icon by name (see `PATHS`); unknown names render nothing. */
export function icon(name, cls = "i") {
  const p = PATHS[name];
  return p ? `<svg class="${cls}" viewBox="0 0 24 24" aria-hidden="true">${p}</svg>` : "";
}

const esc = escapeHtml;
const cls = (...xs) => xs.filter(Boolean).join(" ");
const num = (v) => (typeof v === "number" ? fmtInt(v) : esc(v));

/** Four (or any number of) headline numbers: `[{ label, value, tone?, unit? }]`. */
export function statStrip(items) {
  return `<div class="c-stats" style="--c-cols:${items.length}">${items
    .map(
      (s) =>
        `<div><small>${esc(s.label)}</small><b class="${esc(s.tone || "")}">${num(s.value)}${
          s.unit ? ` <em>${esc(s.unit)}</em>` : ""
        }</b></div>`,
    )
    .join("")}</div>`;
}

/** A collapsible section header for a list: label + count. */
export function sectionHeader(label, count, { collapsed = false } = {}) {
  return `<button type="button" class="c-sect" aria-expanded="${!collapsed}">${icon("chev")}${esc(label)}${
    count == null ? "" : `<span class="c-sect-n">${num(count)}</span>`
  }</button>`;
}

/** The status ring a list row leads with. `tone` is ok|warn|bad|info|muted;
 *  `shape` is full|half|hollow. */
export function statusRing(tone = "muted", shape = "full") {
  return `<span class="c-ring ${esc(tone)} ${shape === "full" ? "" : esc(shape)}" aria-hidden="true"></span>`;
}

/** One list item: ring, title, age, and a muted meta line whose `metaLead`
 *  is shown in bold. `href` makes the row a link. */
export function listRow({ tone, shape, title, age, metaLead, meta, href, selected, tooltip, id, ageTitle }) {
  const tag = href ? "a" : "div";
  const attrs = (href ? ` href="${esc(href)}"` : "") + (id ? ` data-id="${esc(id)}"` : "");
  const lead = metaLead ? `<b>${esc(metaLead)}</b>` : "";
  const rest = meta ? `${lead ? " · " : ""}${esc(meta)}` : "";
  return `<${tag} class="${cls("c-row", selected && "on")}"${attrs}${selected ? ' aria-current="true"' : ""}>${statusRing(tone, shape)}<span class="c-row-t" title="${esc(tooltip || title)}">${esc(title)}</span><span class="c-row-age"${ageTitle ? ` title="${esc(ageTitle)}"` : ""}>${esc(age || "")}</span><span class="c-row-meta" title="${esc([metaLead, meta].filter(Boolean).join(" · "))}">${lead}${rest}</span></${tag}>`;
}

/** A row of tabs: `[{ id, label, count? }]`; `on` is the active id. */
export function tabs(items, on, { cls: extra = "" } = {}) {
  return `<div class="${cls("c-tabs", extra)}" role="tablist">${items
    .map(
      (t) =>
        `<button type="button" role="tab" class="${cls("c-tab", t.id === on && "on")}" data-tab="${esc(t.id)}" aria-selected="${t.id === on}"${
          t.hidden ? " hidden" : ""
        }${t.perm ? ` data-perm="${esc(t.perm)}"` : ""}>${esc(t.label)}${t.count == null ? "" : `<span class="c-tab-n">${num(t.count)}</span>`}</button>`,
    )
    .join("")}</div>`;
}

/** A rounded status pill: tone ok|warn|bad|info or none. `iconName` is optional. */
export function pill(text, tone = "", iconName = "") {
  return `<span class="${cls("c-pill", tone)}">${iconName ? icon(iconName) : ""}${esc(text)}</span>`;
}

/** A small square-cornered chip (a context value, a channel). `kind` adds a
 *  modifier class: `stable` (the only teal chip), `mono`. */
export function chip(text, kind = "", title = "") {
  const mods = String(kind || "").split(/\s+/).filter(Boolean).map((k) => `c-chip-${k}`);
  return `<span class="${cls("c-chip", ...mods)}"${title ? ` title="${esc(title)}"` : ""}>${esc(text)}</span>`;
}

/** A key/value grid: `[{ k, v, s? }]` (value, optional sub-line). `cols` 2–4. */
export function kvGrid(items, cols = 4) {
  return `<div class="c-kvgrid" style="--c-cols:${cols}">${items
    .map(
      (i) =>
        `<div class="c-kv"><small>${esc(i.k)}</small><b>${num(i.v)}</b>${i.s ? `<div class="c-kv-s">${esc(i.s)}</div>` : ""}</div>`,
    )
    .join("")}</div>`;
}

/** A card: a header line (title, optional tag and right-hand note), a body
 *  (trusted markup, e.g. `kvGrid(...)`) and an optional footnote verdict. */
export function card({ title, iconName, tag, aside, right, html = "", footnote = "", footTone = "" }) {
  const head = title
    ? `<div class="c-card-h">${iconName ? icon(iconName) : ""}<b>${esc(title)}</b>${tag ? `<span class="c-tag">${esc(tag)}</span>` : ""}${
        aside ? `<span class="c-card-aside">${esc(aside)}</span>` : ""
      }${right ? `<span class="c-card-r">${esc(right)}</span>` : ""}</div>`
    : "";
  const foot = footnote ? `<div class="${cls("c-card-note", footTone)}">${esc(footnote)}</div>` : "";
  return `<section class="c-card">${head}${html}${foot}</section>`;
}

/** An inline SVG trend line. `mark` highlights one point (index) in amber. */
export function sparkline(points, { width = 180, height = 34, color = "var(--accent)", mark = null, min, max } = {}) {
  const pts = (points || []).map(Number).filter(Number.isFinite);
  if (pts.length < 2) return `<svg class="c-spark" width="${width}" height="${height}" aria-hidden="true"></svg>`;
  const hi = max ?? Math.max(...pts);
  const lo = min ?? Math.min(...pts);
  const xy = pts.map((p, i) => [
    +(i * (width / (pts.length - 1))).toFixed(1),
    +(height - 4 - ((p - lo) / (hi - lo || 1)) * (height - 8)).toFixed(1),
  ]);
  const dot =
    mark != null && xy[mark] ? `<circle cx="${xy[mark][0]}" cy="${xy[mark][1]}" r="3" fill="var(--warn)"/>` : "";
  return `<svg class="c-spark" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" aria-hidden="true"><polyline fill="none" stroke="${esc(color)}" stroke-width="1.6" points="${xy
    .map((p) => p.join(","))
    .join(" ")}"/>${dot}</svg>`;
}

/** A plan / schema diff: `[{ op: "+"|"-"|" ", text }]`. */
export function diffBlock(lines) {
  const kind = { "+": "a", "-": "d" };
  return `<div class="c-diff">${(lines || [])
    .map((l) => `<div class="${kind[l.op] || "k"}">${esc(l.op || " ")} ${esc(l.text)}</div>`)
    .join("")}</div>`;
}

/** Log lines: `[{ ts, level, msg }]`. */
export function logBlock(lines) {
  return `<div class="c-log">${(lines || [])
    .map((l) => {
      const lv = String(l.level || "").toUpperCase();
      const tone = { WARN: "warn", ERROR: "bad", INFO: "info" }[lv] || "";
      return `<div><span class="c-log-ts">${esc(l.ts || "")}</span> <span class="c-log-lv ${tone}">${esc(lv)}</span> ${esc(l.msg || "")}</div>`;
    })
    .join("")}</div>`;
}

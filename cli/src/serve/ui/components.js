// The console's components (#998): one function per design-system component,
// named and shaped like the approved mockups' helpers (console-redesign/mock.js),
// so a page reads like its mock. See docs/contributing/console-design.md.
//
// Safety: every argument is text and is escaped, unless it is markup built by
// `html\`…\`` or another component (a `Raw`). Write markup with `html`, never by
// concatenating strings with data in them.

/** Trusted markup. */
export class Raw {
  constructor(s) { this.s = String(s); }
  toString() { return this.s; }
}
export const raw = (s) => (s instanceof Raw ? s : new Raw(s ?? ""));

const ESC = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
/** Escape text for markup. */
export const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ESC[c]);
/** A value as markup: `Raw` passes through, arrays join, anything else is escaped. */
export const v = (x) => (x instanceof Raw ? x.s : Array.isArray(x) ? x.map(v).join("") : x == null || x === false ? "" : esc(x));

/** Tagged template: `html\`<b>${name}</b>\`` escapes `name` unless it is markup. */
export function html(strings, ...vals) {
  let out = strings[0];
  for (let i = 0; i < vals.length; i++) out += v(vals[i]) + strings[i + 1];
  return new Raw(out);
}
/** Join items (text or markup) with a separator (text or markup). */
export const join = (items, sep = "") => new Raw(items.filter((x) => x !== "" && x != null && x !== false).map(v).join(v(sep)));
/** Attributes from an object; `true` renders a bare attribute, nullish/false is left out. */
export function attrs(o = {}) {
  return Object.entries(o)
    .filter(([, x]) => x !== undefined && x !== null && x !== false)
    .map(([k, x]) => (x === true ? ` ${k}` : ` ${k}="${esc(x)}"`))
    .join("");
}
const cls = (...xs) => xs.filter(Boolean).join(" ");
const dataAttrs = (d = {}) => Object.fromEntries(Object.entries(d).map(([k, x]) => [`data-${k}`, x]));

const P = {
  home: '<path d="M3 11l9-7 9 7v9a1 1 0 0 1-1 1h-5v-6H9v6H4a1 1 0 0 1-1-1z"/>',
  runs: '<path d="M5 4l14 8-14 8z"/>',
  changes: '<path d="M6 3v12"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="6" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/>',
  trig: '<path d="M13 2L4 14h7l-1 8 9-12h-7z"/>',
  sched: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  db: '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
  lineage: '<circle cx="5" cy="6" r="2.5"/><circle cx="5" cy="18" r="2.5"/><circle cx="19" cy="12" r="2.5"/><path d="M7.5 6h3a4 4 0 0 1 4 4v0M7.5 18h3a4 4 0 0 0 4-4"/>',
  tpl: '<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><path d="M17.5 14v7M14 17.5h7"/>',
  schema: '<path d="M8 3H6a2 2 0 0 0-2 2v4l-2 3 2 3v4a2 2 0 0 0 2 2h2M16 3h2a2 2 0 0 1 2 2v4l2 3-2 3v4a2 2 0 0 1-2 2h-2"/>',
  submit: '<path d="M12 19V5M5 12l7-7 7 7"/>',
  policy: '<path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/>',
  usage: '<path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/>',
  tenants: '<circle cx="9" cy="8" r="3.5"/><path d="M2.5 20a6.5 6.5 0 0 1 13 0"/><path d="M16 4.5a3.5 3.5 0 0 1 0 7M21.5 20a6.5 6.5 0 0 0-4-6"/>',
  audit: '<path d="M9 4h10v16H5V8z"/><path d="M9 4v4H5M9 13h6M9 17h6"/>',
  settings: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 0 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 0 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 0 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 0 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z"/>',
  search: '<circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/>',
  filter: '<path d="M3 5h18M6 12h12M10 19h4"/>',
  sliders: '<path d="M4 6h10M18 6h2M4 18h4M12 18h8M14 4v4M8 16v4"/>',
  link: '<path d="M10 14a4 4 0 0 0 5.7 0l3-3a4 4 0 0 0-5.7-5.7l-1 1M14 10a4 4 0 0 0-5.7 0l-3 3a4 4 0 0 0 5.7 5.7l1-1"/>',
  expand: '<path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7"/>',
  collapse: '<path d="M4 14h6v6M20 10h-6V4M14 10l7-7M3 21l7-7"/>',
  chev: '<path d="M6 9l6 6 6-6"/>',
  right: '<path d="M9 6l6 6-6 6"/>',
  back: '<path d="M15 6l-6 6 6 6"/>',
  updown: '<path d="M8 9l4-4 4 4M8 15l4 4 4-4"/>',
  bell: '<path d="M6 8a6 6 0 1 1 12 0c0 7 3 9 3 9H3s3-2 3-9M10.3 21a1.9 1.9 0 0 0 3.4 0"/>',
  logs: '<path d="M4 6h16M4 12h16M4 18h10"/>',
  check: '<path d="M20 6L9 17l-5-5"/>',
  x: '<path d="M18 6L6 18M6 6l12 12"/>',
  play: '<path d="M6 4l14 8-14 8z"/>',
  pause: '<path d="M8 5v14M16 5v14"/>',
  undo: '<path d="M9 14L4 9l5-5"/><path d="M4 9h11a5 5 0 0 1 0 10h-3"/>',
  flask: '<path d="M9 3h6M10 3v6L4 19a1.5 1.5 0 0 0 1.3 2h13.4A1.5 1.5 0 0 0 20 19l-6-10V3"/>',
  rocket: '<path d="M12 15l-3-3a22 22 0 0 1 2-4A12.9 12.9 0 0 1 22 2c0 2.7-.8 7.5-6 11a22.4 22.4 0 0 1-4 2z"/><path d="M9 12H5s.6-3 2-4c1.6-1.1 4 0 4 0"/><path d="M12 15v4s3-.6 4-2c1.1-1.6 0-4 0-4"/><circle cx="15.5" cy="8.5" r="1.5"/><path d="M7 17l-3 3M5 14.5l-1.5 1.5M9.5 19.5L8 21"/>',
  copy: '<rect x="8" y="8" width="13" height="13" rx="2"/><path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/>',
  refresh: '<path d="M20 11a8 8 0 1 0-2.3 5.7M20 4v7h-7"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  trash: '<path d="M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13"/>',
  more: '<circle cx="5" cy="12" r="1.2"/><circle cx="12" cy="12" r="1.2"/><circle cx="19" cy="12" r="1.2"/>',
  matrix: '<rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18M3 15h18M9 3v18M15 3v18"/>',
  sync: '<path d="M4 12a8 8 0 0 1 13.7-5.7M20 12a8 8 0 0 1-13.7 5.7"/><path d="M18 3v4h-4M6 21v-4h4"/>',
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7.5v.5"/>',
  alert: '<path d="M12 3l10 18H2z"/><path d="M12 10v5M12 18v.5"/>',
  diff: '<rect x="3" y="3" width="18" height="18" rx="2"/><path d="M12 7v6M9 10h6M9 17h6"/>',
  code: '<path d="M8 6l-6 6 6 6M16 6l6 6-6 6"/>',
  zoomin: '<circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4M8 11h6M11 8v6"/>',
  zoomout: '<circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4M8 11h6"/>',
  fit: '<path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/>',
  focus: '<circle cx="12" cy="12" r="3"/><path d="M12 2v4M12 18v4M2 12h4M18 12h4"/>',
  columns: '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M9 4v16M15 4v16"/>',
  download: '<path d="M12 4v11M7 10l5 5 5-5M5 20h14"/>',
  edit: '<path d="M4 20h4L19 9l-4-4L4 16z"/>',
  key: '<circle cx="7.5" cy="15.5" r="4.5"/><path d="M10.7 12.3L21 2M17 6l3 3M14 9l2 2"/>',
  theme: '<circle cx="12" cy="12" r="9"/><path d="M12 3a9 9 0 0 0 0 18z" fill="currentColor"/>',
  server: '<rect x="3" y="4" width="18" height="7" rx="1.5"/><rect x="3" y="13" width="18" height="7" rx="1.5"/><path d="M7 7.5h.01M7 16.5h.01"/>',
  shield: '<path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/><path d="M9 12l2 2 4-4"/>',
  user: '<circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0 1 16 0"/>',
  layers: '<path d="M12 3l9 5-9 5-9-5z"/><path d="M3 13l9 5 9-5"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
};

/** An inline stroke icon by name; unknown names render nothing. */
export const ic = (n, c = "i") => new Raw(P[n] ? `<svg class="${c}" viewBox="0 0 24 24" aria-hidden="true">${P[n]}</svg>` : "");
export const icon = ic;

/** Status ring: tone ok|warn|bad|info|"" and shape ""|half|hollow. */
export const ring = (tone = "", shape = "") => html`<span class="${cls("ring", shape, tone)}"></span>`;

/** A list row: lead (a ring) · title · age · meta. `href` makes it a link;
 *  `id` is what selection matches. */
export function row(lead, title, age, metaHtml, { on = false, href = "", id: rid = "", mono = false, data = {}, tip = "" } = {}) {
  const tagName = href ? "a" : "div";
  const a = attrs({ href: href || undefined, "data-id": rid || undefined, "aria-current": on ? "true" : undefined, title: tip || undefined, ...dataAttrs(data) });
  return html`<${raw(tagName)} class="${cls("row", on && "on")}"${raw(a)}>${lead}<span class="${cls("t", mono && "mono")}">${title}</span><span class="age">${age}</span><span class="meta">${metaHtml}</span></${raw(tagName)}>`;
}

/** Collapsible list-section header; `id` keys its remembered state. */
export const sect = (label, count, { closed = false, id: sid = "" } = {}) =>
  html`<button type="button" class="${cls("sect", closed && "closed")}"${raw(attrs({ "data-sect": sid || label, "aria-expanded": String(!closed) }))}>${ic("chev")}${label}<span class="c">${count}</span></button>`;

/** Stat strip: `[[label, value, tone?, filterValue?]]`. A cell with a filter value is clickable (`data-q`). */
export function stats(items, on = -1) {
  return html`<div class="stats" style="--n:${raw(items.length)}">${items.map(([k, val, tone, q], i) => html`<div class="${on === i ? "on" : ""}"${raw(attrs({ "data-q": q ?? undefined, role: q != null ? "button" : undefined, tabindex: q != null ? "0" : undefined }))}><small>${k}</small><b class="${tone || ""}">${val}</b></div>`)}</div>`;
}

/** Tabs: items are labels or `[label, count?, value?]`; `on` is the active value.
 *  Each tab is a link from `hrefFor(value)`, or a button carrying `data-tab`. */
export function tabItems(items, on, { hrefFor = null } = {}) {
  return join(items.filter(Boolean).map((it) => {
    const [l, n, val] = Array.isArray(it) ? it : [it];
    const value = val ?? l;
    const isOn = value === on;
    const count = n != null && n !== "" ? html`<span class="n">${n}</span>` : "";
    if (hrefFor) return html`<a class="${cls("tab", isOn && "on")}" role="tab" aria-selected="${String(isOn)}" href="${hrefFor(value)}">${l}${count}</a>`;
    return html`<button type="button" class="${cls("tab", isOn && "on")}" role="tab" aria-selected="${String(isOn)}" data-tab="${value}">${l}${count}</button>`;
  }));
}
export const tabs = (items, on, tools = "", opts = {}) =>
  html`<div class="tabs" role="tablist">${tabItems(items, on, opts)}${tools ? html`<span class="tools">${tools}</span>` : ""}</div>`;
export const dtabs = tabItems;
/** The detail header: tabs left, actions right; `back` = {href, label} for narrow screens. */
export const dhead = (tabsHtml, right = "", back = null) =>
  html`<div class="dhead" role="tablist">${back ? html`<a class="back" href="${back.href}">${ic("back")}${back.label}</a>` : ""}${tabsHtml}${right ? html`<span class="r">${right}</span>` : ""}</div>`;

export const kv = (k, val, s = "") => html`<div class="kv"><small>${k}</small><b>${val}</b>${s !== "" && s != null && s !== false ? html`<div class="s">${s}</div>` : ""}</div>`;
export const grid = (n, items) => html`<div class="grid${raw(n)}">${items}</div>`;

/** A card: header (icon · bold title · tag · muted aside · right), body, footnote. */
export function card({ icon: i, title, tag: t, aside, right, body = "", note, cls: c = "", id: cid = "" } = {}) {
  const h = title || i ? html`<div class="h">${i ? ic(i) : ""}${title ? html`<b>${title}</b>` : ""}${t ? html`<span class="tag">${t}</span>` : ""}${aside ? html`<span class="muted">${aside}</span>` : ""}${right ? html`<span class="r">${right}</span>` : ""}</div>` : "";
  return html`<div class="${cls("card", c)}"${raw(attrs({ id: cid || undefined }))}>${h}${body}${note ? html`<div class="note">${note}</div>` : ""}</div>`;
}

/** A table: `cols` = labels or `[label, "num"]`; rows = cell arrays,
 *  `{cells, cls, href, id}`, or `{span, cls}` (one cell spanning all but the first column). */
export function tbl(cols, rows, foot = null) {
  const cc = (i) => (Array.isArray(cols[i]) ? cols[i][1] : "");
  return html`<table><thead><tr>${cols.map((c) => (Array.isArray(c) ? html`<th class="${c[1] || ""}">${c[0]}</th>` : html`<th>${c}</th>`))}</tr></thead><tbody>${rows.map((r) => {
    if (r.span != null) return html`<tr${raw(attrs({ class: r.cls || undefined }))}><td></td><td colspan="${cols.length - 1}">${r.span}</td></tr>`;
    const cells = r.cells || r;
    return html`<tr${raw(attrs({ class: r.cls || undefined, "data-href": r.href || undefined, "data-id": r.id || undefined }))}>${cells.map((c, i) => html`<td class="${cc(i)}">${c}</td>`)}</tr>`;
  })}</tbody>${foot ? html`<tfoot><tr>${foot.map((c, i) => html`<td class="${cc(i)}">${c}</td>`)}</tr></tfoot>` : ""}</table>`;
}

export const pill = (text, tone = "", i = "") => html`<span class="${cls("pill", tone)}">${i ? ic(i) : ""}${text}</span>`;
/** A chip; pass `{ "data-remove": value }` with class `x` for a removable filter chip. */
export const chip = (text, c = "", a = {}) => html`<span class="${cls("chip", c)}"${raw(attrs(a))}>${text}</span>`;
export const ch = (text, stable = false) => html`<span class="${cls("ch", stable && "stable")}">${text}</span>`;
export const tag = (text, tone = "") => html`<span class="${cls("tag", tone)}">${text}</span>`;

/** A button; `href` makes it a link; `perm` gates it with data-perm. */
export function btn(label, { kind = "", icon: i = "", cls: c = "", href = "", perm = "", data = {}, title = "", disabled = false, type = "button", id: bid = "" } = {}) {
  const a = { id: bid || undefined, "data-perm": perm || undefined, title: title || undefined, ...dataAttrs(data) };
  if (href) return html`<a class="${cls("btn", kind, c)}" href="${href}"${raw(attrs(a))}>${i ? ic(i) : ""}${label}</a>`;
  return html`<button type="${type}" class="${cls("btn", kind, c, disabled && "dis")}"${raw(attrs({ ...a, disabled }))}>${i ? ic(i) : ""}${label}</button>`;
}
export function ibtn(i, { on = false, dot = false, title = "", href = "", perm = "", data = {}, id: bid = "" } = {}) {
  const a = { id: bid || undefined, title: title || undefined, "aria-label": title || undefined, "aria-pressed": on ? "true" : undefined, "data-perm": perm || undefined, ...dataAttrs(data) };
  const inner = html`${ic(i)}${dot ? html`<span class="bd"></span>` : ""}`;
  return href ? html`<a class="${cls("ibtn", on && "on")}" href="${href}"${raw(attrs(a))}>${inner}</a>` : html`<button type="button" class="${cls("ibtn", on && "on")}"${raw(attrs(a))}>${inner}</button>`;
}

/** A text input inside the input frame. */
export const input = (value = "", { ph = "", icon: i = "", cls: c = "", mono = false, name = "", type = "text", id: iid = "", a = {} } = {}) =>
  html`<label class="${cls("input", mono && "mono", c)}">${i ? ic(i) : ""}<input${raw(attrs({ type, name: name || undefined, id: iid || undefined, placeholder: ph || undefined, value: value ?? "", spellcheck: "false", autocomplete: "off", ...a }))}></label>`;
/** A select: `options` = `[[value, label]]` or values. */
export const select = (options, value = "", { name = "", cls: c = "", id: sid = "", a = {} } = {}) =>
  html`<span class="${cls("select", c)}"><select${raw(attrs({ name: name || undefined, id: sid || undefined, ...a }))}>${options.map((o) => {
    const [val, l] = Array.isArray(o) ? o : [o, o];
    return html`<option value="${val}"${raw(String(val) === String(value) ? " selected" : "")}>${l}</option>`;
  })}</select></span>`;
export const check = (label, on = false, { name = "", a = {} } = {}) =>
  html`<label class="${cls("check", on && "on")}"><input type="checkbox"${raw(attrs({ name: name || undefined, checked: on, ...a }))}><i></i>${label}</label>`;
export const toggle = (label, on = false, { name = "", a = {} } = {}) =>
  html`<label class="${cls("toggle", on && "on")}"><input type="checkbox"${raw(attrs({ name: name || undefined, checked: on, ...a }))}><i></i>${label}</label>`;
export const field = (label, control, { help = "", req = false, code: isCode = false, cls: c = "", err = "" } = {}) =>
  html`<div class="${cls("field", c)}"><label>${isCode ? html`<code>${label}</code>` : label}${req ? html`<span class="req" title="required"></span>` : ""}</label>${control}${err ? html`<div class="help bad">${err}</div>` : help ? html`<div class="help">${help}</div>` : ""}</div>`;

export const alert = (title, text = "", tone = "", btns = "") =>
  html`<div class="${cls("alert", tone)}" role="${tone === "bad" ? "alert" : "status"}">${ic(tone === "ok" ? "check" : tone ? "alert" : "info")}<div><b>${title}</b>${text ? html` ${text}` : ""}${btns ? html`<div class="btns">${btns}</div>` : ""}</div></div>`;
export const empty = (title, text = "", { icon: i = "", btns = "" } = {}) =>
  html`<div class="empty">${i ? ic(i) : ""}<b>${title}</b>${text ? html`<span>${text}</span>` : ""}${btns ? html`<div class="btns">${btns}</div>` : ""}</div>`;
export const skelRows = (n = 6) => raw('<div class="skel-row"><span class="skel"></span><span class="skel w80"></span><span class="skel"></span><span class="skel"></span></div>'.repeat(n));
export const skelCards = (n = 2) => raw('<div class="skel-card"><span class="skel w30"></span><span class="skel w80"></span><span class="skel w60"></span></div>'.repeat(n));
/** The detail-pane skeleton: title line, sub line, then cards. */
export const skelDetail = (n = 2) => html`<span class="skel t"></span><span class="skel w60"></span>${skelCards(n)}`;

/** Copy buttons: on a code block (top-right) / inline after an id. */
export const cp = (text) => html`<button type="button" class="cp" data-copy="${text}" title="Copy" aria-label="Copy">${ic("copy")}</button>`;
export const cpi = (text) => html`<button type="button" class="cpi" data-copy="${text}" title="Copy" aria-label="Copy">${ic("copy")}</button>`;
/** A link to a page, or the plain text when there is no page to go to. */
export const lnk = (text, href = "", mono = false, title = "") =>
  href ? html`<a class="${cls("lnk", mono && "mono")}" href="${href}"${raw(attrs({ title: title || undefined }))}>${text}</a>` : html`<span class="${mono ? "mono" : ""}"${raw(attrs({ title: title || undefined }))}>${text}</span>`;
export function shortId(x) {
  const s = String(x || "");
  return s.length > 14 ? `${s.slice(0, 4)}…${s.slice(-4)}` : s;
}
/** An id in mono with a copy button; `short` shows a shortened form (full on hover). */
export const id = (text, { short = false, copy = true, href = "" } = {}) => {
  const s = short ? shortId(text) : text;
  return html`${href ? lnk(s, href, true, text) : html`<span class="mono" title="${text}">${s}</span>`}${copy ? cpi(text) : ""}`;
};

function yaml(text) {
  return String(text).split("\n").map((line) => {
    if (/^\s*#/.test(line)) return `<span class="c">${esc(line)}</span>`;
    const m = line.match(/^(\s*-?\s*)([A-Za-z0-9_.$/-]+)(:)(.*)$/);
    if (!m) return esc(line);
    const tv = m[4].trim();
    const c = /^-?\d+(\.\d+)?$|^(true|false|null)$/.test(tv) ? "n" : /^["']/.test(tv) ? "s" : "";
    const cm = m[4].match(/^(.*?)(\s+#.*)$/);
    const val = cm ? cm[1] : m[4];
    return `${esc(m[1])}<span class="k">${esc(m[2])}</span>:${c ? `<span class="${c}">${esc(val)}</span>` : esc(val)}${cm ? `<span class="c">${esc(cm[2])}</span>` : ""}`;
  }).join("\n");
}
function json(text) {
  return esc(text).replace(/(&quot;(?:[^&]|&(?!quot;))*?&quot;)(\s*:)?|\b(-?\d+(?:\.\d+)?(?:e[+-]?\d+)?|true|false|null)\b/g, (m, s, c, lit) => (s ? (c ? `<span class="k">${s}</span>${c}` : `<span class="s">${s}</span>`) : `<span class="n">${lit}</span>`));
}
/** A code block (`yaml` | `json` | `text`) with a copy button. */
export const code = (text, { box = true, copy = true, lang = "yaml" } = {}) =>
  html`<div class="${cls("code", box && "box")}">${raw(lang === "yaml" ? yaml(text) : lang === "json" ? json(text) : esc(text))}${copy ? cp(text) : ""}</div>`;
/** A line diff: `[[op, text]]`, op `+` `-` `@` or "". */
export const diff = (lines) =>
  html`<div class="diff">${lines.map(([op, t]) => html`<span class="${{ "+": "a", "-": "d", "@": "h" }[op] || "k"}">${op === "@" ? "" : `${op || " "} `}${t}</span>`)}</div>`;
export const logl = (ts, lv, msg) => html`<div><span class="muted">${ts}</span> <span class="${cls("lv", { INFO: "info", WARN: "warn", ERROR: "bad" }[lv])}">${lv}</span> ${msg}</div>`;

/** A sparkline; `mark` puts a warn dot on one point. */
export function spark(pts, color = "var(--text-2)", w = 150, h = 30, mark = null, lo = null, hi = null) {
  if (!pts || pts.length < 2) return raw(`<svg class="spark" width="${w}" height="${h}"></svg>`);
  const max = hi ?? Math.max(...pts), min = lo ?? Math.min(...pts);
  const xy = pts.map((p, i) => [+(i * (w / (pts.length - 1))).toFixed(1), +(h - 4 - ((p - min) / (max - min || 1)) * (h - 8)).toFixed(1)]);
  return raw(`<svg class="spark" width="${w}" height="${h}" aria-hidden="true"><polyline fill="none" stroke="${esc(color)}" stroke-width="1.6" points="${xy.map((p) => p.join(",")).join(" ")}"/>${mark != null && xy[mark] ? `<circle cx="${xy[mark][0]}" cy="${xy[mark][1]}" r="3" fill="var(--warn)"/>` : ""}</svg>`);
}
/** A stacked bar: `[[percent, cssColour, title?]]`. Colours must be tokens (`var(--ok)`). */
export const bar = (parts) => html`<div class="bar">${parts.filter(([w]) => w > 0).map(([w, c, t]) => html`<i style="width:${raw(Number(w).toFixed(2))}%;background:${raw(esc(c))}"${raw(attrs({ title: t || undefined }))}></i>`)}</div>`;
export const legend = (parts) => html`<div class="legend">${parts.map(([l, c]) => html`<span style="--c:${raw(esc(c))}">${l}</span>`)}</div>`;
export const meter = (pct) => html`<div class="meter"><i style="width:${raw(Math.max(0, Math.min(100, Number(pct) || 0)).toFixed(1))}%"></i></div>`;

/** Page title, a sub line of facts (dot separated) and an optional lede. */
export const head = (title, sub = [], lede = "") =>
  html`<div><h1${raw(attrs({ title: typeof title === "string" ? title : undefined }))}>${title}</h1>${sub.filter((x) => x !== "" && x != null && x !== false).length ? html`<div class="sub">${join(sub, html`<span class="faint">·</span>`)}</div>` : ""}${lede ? html`<p class="lede">${lede}</p>` : ""}</div>`;

/** A menu: a summary button opening a popover.
 *  items: "-" separator, `{heading}`, or `{label, icon, hint, cls, href, action, perm, data}`. */
export function menu(summary, items, { left = false, title = "More actions", perm = "" } = {}) {
  const body = items.filter(Boolean).map((it) => {
    if (it === "-") return html`<div class="msep"></div>`;
    if (it.heading != null) return html`<div class="mlab">${it.heading}</div>`;
    const inner = html`${it.icon ? ic(it.icon) : ""}${it.label}${it.hint ? html`<span class="muted">${it.hint}</span>` : ""}`;
    const a = { "data-perm": it.perm || undefined, "data-action": it.action || undefined, ...dataAttrs(it.data) };
    if (it.href) return html`<a class="${cls("mi", it.cls)}" href="${it.href}"${raw(attrs(a))}>${inner}</a>`;
    return html`<button type="button" class="${cls("mi", it.cls)}"${raw(attrs({ ...a, disabled: it.cls === "dis" }))}>${inner}</button>`;
  });
  const sum = summary === "more" ? html`<summary class="btn" title="${title}" aria-label="${title}">${ic("more")}</summary>` : html`<summary class="btn">${summary}</summary>`;
  return html`<details class="${cls("menu", left && "left")}"${raw(attrs({ "data-perm": perm || undefined }))}>${sum}<div class="pop" role="menu">${body}</div></details>`;
}

export const steps = (items) =>
  html`<div class="steps">${items.map(([label, st, n], i) => html`${i ? html`<span class="${cls("step-line", (st === "done" || st === "cur") && "done")}"></span>` : ""}<span class="${cls("step", st)}"><span class="sn">${st === "done" ? ic("check") : st === "bad" ? ic("x") : n ?? i + 1}</span>${label}</span>`)}</div>`;

/** Row meta: facts separated by dots, the first one bold. */
export const meta = (...parts) => {
  const xs = parts.flat().filter((x) => x !== "" && x != null && x !== false);
  return xs.length ? html`<b>${xs[0]}</b>${xs.length > 1 ? html` · ${join(xs.slice(1), " · ")}` : ""}` : raw("");
};
/** A status dot + text, for table cells ("● running"). */
export const dotText = (tone, text) => html`<span class="${tone}"><span class="${cls("dot", tone)}"></span>${text}</span>`;

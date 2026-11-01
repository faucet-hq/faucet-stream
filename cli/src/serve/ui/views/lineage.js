// Lineage (#998): a graph tool over the Data Movement Catalog — a full-width
// canvas (drag to pan, wheel or buttons to zoom, fit, minimap, search, kind
// filter) with focus mode (#/lineage/<id>?depth=N&dir=both|up|down), a side
// panel for the selected dataset (?sel=) or edge (?edge=src~dst), and column
// tracing (?col=) through every recorded hop. Hand-written SVG, no deps.
import { api } from "../api.js";
import { html, raw, v, ic, ring, dhead, dtabs, ibtn, input, select, toggle, chip, empty, head, card, grid, kv, tbl, pill, btn, lnk, join, shortId } from "../components.js";
import { layout, setQuery, hashFor, errorAlert } from "../page.js";
import { setCrumbs } from "../shell.js";
import { fmtInt, fmtCompact, fmtAge } from "../utils.js";
import { href, datasetName } from "../links.js";
import { navigate } from "../router.js";
import { catalogUnavailable } from "./datasets.js";

const W = 210, H = 46, CW = 470, RG = 34, PAD = 40;
const BW = 210, ROW = 20, MAXCOLS = 10;
const DAY = 86400000;

const tone = (d) => (!d || !d.last_success ? "muted" : Date.now() - Date.parse(d.last_success) > DAY ? "warn" : "ok");
const trunc = (s, n) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);
const how = (into, from) => (!from || !from.length ? "no source" : from.length > 1 ? "combined" : from[0] === into ? "identity" : "rename");
const curve = (x1, y1, x2, y2) => { const m = (x1 + x2) / 2; return `M${x1} ${y1} C${m} ${y1} ${m} ${y2} ${x2} ${y2}`; };

/** Longest-path layers + barycentre ordering. Returns positions by id. */
function layoutGraph(ids, edges, heightOf = () => H) {
  const preds = new Map(ids.map((i) => [i, []]));
  const succs = new Map(ids.map((i) => [i, []]));
  for (const e of edges) { if (preds.has(e.dst_id) && succs.has(e.src_id)) { preds.get(e.dst_id).push(e.src_id); succs.get(e.src_id).push(e.dst_id); } }
  const layer = new Map(ids.map((i) => [i, 0]));
  for (let pass = 0; pass < ids.length; pass++) {
    let moved = false;
    for (const e of edges) {
      if (!layer.has(e.src_id) || !layer.has(e.dst_id)) continue;
      const want = layer.get(e.src_id) + 1;
      if (want > layer.get(e.dst_id) && want < ids.length) { layer.set(e.dst_id, want); moved = true; }
    }
    if (!moved) break;
  }
  const cols = [];
  for (const i of ids) { const l = layer.get(i); (cols[l] = cols[l] || []).push(i); }
  const order = new Map();
  cols.forEach((c) => c.sort().forEach((i, k) => order.set(i, k)));
  for (let it = 0; it < 3; it++) {
    for (let l = 1; l < cols.length; l++) {
      const bc = (i) => { const p = preds.get(i).filter((x) => layer.get(x) < l); return p.length ? p.reduce((a, x) => a + order.get(x), 0) / p.length : order.get(i); };
      cols[l].sort((a, b) => bc(a) - bc(b) || a.localeCompare(b)).forEach((i, k) => order.set(i, k));
    }
  }
  const pos = new Map();
  const colH = cols.map((c) => c.reduce((a, i) => a + heightOf(i) + RG, 0));
  const tallest = Math.max(0, ...colH);
  let maxY = 0;
  cols.forEach((c, l) => {
    let y = PAD + (tallest - colH[l]) / 2;
    for (const i of c) { const h = heightOf(i); pos.set(i, { x: PAD + l * CW, y, h, layer: l }); y += h + RG; }
    maxY = Math.max(maxY, y);
  });
  return { pos, layers: cols.length, bounds: { x: 0, y: 0, w: PAD * 2 + Math.max(0, cols.length - 1) * CW + W, h: Math.max(maxY, PAD * 2 + H) } };
}

/** Datasets within `depth` hops of `root` (up, down or both). */
function neighbourhood(edges, root, depth, dir) {
  const keep = new Set([root]);
  const walk = (from, next) => {
    let frontier = [from];
    for (let d = 0; d < depth && frontier.length; d++) {
      const nf = [];
      for (const n of frontier) for (const x of next(n)) if (!keep.has(x)) { keep.add(x); nf.push(x); }
      frontier = nf;
    }
  };
  if (dir !== "down") walk(root, (n) => edges.filter((e) => e.dst_id === n).map((e) => e.src_id));
  if (dir !== "up") walk(root, (n) => edges.filter((e) => e.src_id === n).map((e) => e.dst_id));
  return keep;
}

/** Trace one column through the recorded column lineage, up and down. */
function traceColumn(edges, node, col) {
  const cells = new Set([`${node}\u0000${col}`]);
  const links = [];
  const up = [[node, col]];
  while (up.length) {
    const [n, c] = up.pop();
    for (const e of edges.filter((x) => x.dst_id === n)) {
      const from = (e.column_lineage && e.column_lineage.fields && e.column_lineage.fields[c]) || [];
      for (const s of from) {
        links.push({ e, sc: s, dc: c });
        const k = `${e.src_id}\u0000${s}`;
        if (!cells.has(k)) { cells.add(k); up.push([e.src_id, s]); }
      }
    }
  }
  const down = [[node, col]];
  while (down.length) {
    const [n, c] = down.pop();
    for (const e of edges.filter((x) => x.src_id === n)) {
      const f = (e.column_lineage && e.column_lineage.fields) || {};
      for (const [d, from] of Object.entries(f)) {
        if (!from.includes(c)) continue;
        links.push({ e, sc: c, dc: d });
        const k = `${e.dst_id}\u0000${d}`;
        if (!cells.has(k)) { cells.add(k); down.push([e.dst_id, d]); }
      }
    }
  }
  return { cells, links };
}

export async function renderLineage(host, params) {
  const L = layout(host, "canvas-panel");
  const body = document.getElementById("view");
  let edges = null;
  let meta = new Map();
  let err = null;
  let q = read(params);
  let vb = null; // current viewBox {x,y,w,h}
  let fitVb = null;
  let graph = null; // { pos, bounds, ids, shown edges, trace }
  let canvasEl = null;

  function read(p) {
    const s = p.query;
    return {
      root: p.root || "", depth: Math.max(1, Math.min(8, Number(s.get("depth")) || 2)), dir: s.get("dir") || "both",
      sel: s.get("sel") || "", edge: s.get("edge") || "", col: s.get("col") || "", ptab: s.get("ptab") || "dataset",
      q: s.get("q") || "", kind: s.get("kind") || "", src: s.get("src") !== "0",
    };
  }
  const base = () => (q.root ? `#/lineage/${encodeURIComponent(q.root)}` : "#/lineage");
  const qs = (extra = {}) => hashFor(base(), { depth: q.root && q.depth !== 2 ? q.depth : "", dir: q.dir !== "both" ? q.dir : "", q: q.q, kind: q.kind, src: q.src ? "" : "0", ...extra });
  const name = (id) => { const d = meta.get(id); if (d) return datasetName(d.uri); const e = (edges || []).find((x) => x.src_id === id || x.dst_id === id); return e ? datasetName(e.src_id === id ? e.src_uri : e.dst_uri) : shortId(id); };
  const uriOf = (id) => { const d = meta.get(id); if (d) return d.uri; const e = (edges || []).find((x) => x.src_id === id || x.dst_id === id); return e ? (e.src_id === id ? e.src_uri : e.dst_uri) : id; };
  const colsOf = (id) => Object.keys((meta.get(id) && meta.get(id).current_schema && meta.get(id).current_schema.properties) || {});

  // ── graph model for the current URL state ──
  function model() {
    let shown = edges.slice();
    if (q.root) { const keep = neighbourhood(edges, q.root, q.depth, q.dir); shown = shown.filter((e) => keep.has(e.src_id) && keep.has(e.dst_id)); }
    if (q.kind) shown = shown.filter((e) => (meta.get(e.src_id) || {}).kind === q.kind || (meta.get(e.dst_id) || {}).kind === q.kind);
    if (!q.src) {
      const dsts = new Set(edges.map((e) => e.dst_id));
      shown = shown.filter((e) => dsts.has(e.src_id));
    }
    let ids = [...new Set(shown.flatMap((e) => [e.src_id, e.dst_id]))];
    if (q.root && !ids.includes(q.root)) ids.push(q.root);
    const traceNode = q.col ? q.sel || q.root : "";
    let trace = null;
    if (traceNode) {
      trace = traceColumn(edges, traceNode, q.col);
      const tn = new Set([traceNode, ...[...trace.cells].map((k) => k.split("\u0000")[0])]);
      ids = [...tn];
      shown = edges.filter((e) => tn.has(e.src_id) && tn.has(e.dst_id));
    }
    const colList = (id) => {
      const all = colsOf(id);
      const hl = trace ? [...trace.cells].filter((k) => k.startsWith(`${id}\u0000`)).map((k) => k.split("\u0000")[1]) : [];
      const list = [...new Set([...hl, ...all])].slice(0, MAXCOLS);
      return all.length ? list.sort((a, b) => all.indexOf(a) - all.indexOf(b)) : list;
    };
    const heightOf = trace ? (id) => 46 + colList(id).length * ROW + 6 : () => H;
    const lay = layoutGraph(ids, shown, heightOf);
    return { ...lay, ids, shown, trace, traceNode, colList };
  }

  // ── svg ──
  function svgMarkup() {
    const g = graph;
    const sel = q.sel || q.root;
    const nb = new Set();
    if (q.sel) for (const e of g.shown) { if (e.src_id === q.sel) nb.add(e.dst_id); if (e.dst_id === q.sel) nb.add(e.src_id); }
    const s = q.q.toLowerCase();
    const match = (id) => !s || `${uriOf(id)} ${name(id)}`.toLowerCase().includes(s);
    const parts = [];
    // layer headings
    if (!g.trace) for (let l = 0; l < g.layers; l++) parts.push(html`<g class="gn"><text class="gs" x="${PAD + l * CW}" y="24">${l === 0 ? "Sources" : `${l} hop${l === 1 ? "" : "s"} downstream`}</text></g>`);
    // edges
    if (g.trace) {
      const yc = (id, col) => { const p = g.pos.get(id); const i = g.colList(id).indexOf(col); return p.y + (i < 0 ? H / 2 : 57 + i * ROW); };
      for (const lk of g.trace.links) {
        const a = g.pos.get(lk.e.src_id), b = g.pos.get(lk.e.dst_id);
        if (!a || !b) continue;
        parts.push(html`<path class="ge on" d="${curve(a.x + BW, yc(lk.e.src_id, lk.sc), b.x, yc(lk.e.dst_id, lk.dc))}"/>`);
      }
      for (const e of g.shown) {
        if (g.trace.links.some((l) => l.e === e)) continue;
        const a = g.pos.get(e.src_id), b = g.pos.get(e.dst_id);
        parts.push(html`<path class="ge dim" d="${curve(a.x + BW, a.y + H / 2, b.x, b.y + H / 2)}"/>`);
      }
    } else {
      for (const e of g.shown) {
        const a = g.pos.get(e.src_id), b = g.pos.get(e.dst_id);
        if (!a || !b) continue;
        const key = `${e.src_id}~${e.dst_id}`;
        const on = q.edge === key ? "sel" : q.sel && (e.src_id === q.sel || e.dst_id === q.sel) ? "on" : s && !(match(e.src_id) || match(e.dst_id)) ? "dim" : "";
        const d = curve(a.x + W, a.y + H / 2, b.x, b.y + H / 2);
        const sw = (1.4 + Math.min(1.6, Math.log2(1 + (e.runs || 1)) / 4)).toFixed(2);
        parts.push(html`<g class="ge-g" data-edge="${key}"><path class="ge ${on}" d="${d}" style="stroke-width:${sw}"/><path d="${d}" stroke="transparent" stroke-width="12" fill="none" style="cursor:pointer"><title>${e.pipeline} · ${fmtInt(e.runs || 0)} runs · ${fmtCompact(e.last_records || 0)} rows last run</title></path></g>`);
      }
    }
    // nodes
    for (const id of g.ids) {
      const p = g.pos.get(id);
      if (!p) continue;
      const d = meta.get(id);
      const cls = id === sel ? "sel" : nb.has(id) ? "nb" : s && !match(id) ? "dim" : "";
      const sub = d ? `${d.kind} · ${fmtCompact(d.last_records ?? 0)} rows${d.last_success ? ` · ${fmtAge(d.last_success)}` : " · never"}` : "dataset";
      if (g.trace) {
        const cl = g.colList(id);
        const hl = new Set([...g.trace.cells].filter((k) => k.startsWith(`${id}\u0000`)).map((k) => k.split("\u0000")[1]));
        parts.push(html`<g class="gn ${hl.size ? "" : "dim"} ${id === g.traceNode ? "sel" : ""}" data-node="${id}" style="cursor:pointer"><title>${uriOf(id)}</title><rect x="${p.x}" y="${p.y}" width="${BW}" height="${p.h}" rx="8"/><circle class="gdot ${tone(d)}" cx="${p.x + 14}" cy="${p.y + 16}" r="4"/><text class="gt" x="${p.x + 26}" y="${p.y + 20}">${trunc(name(id), 25)}</text><text class="gs" x="${p.x + 26}" y="${p.y + 35}">${d ? d.kind : "dataset"}</text><line class="sep" x1="${p.x}" x2="${p.x + BW}" y1="${p.y + 44}" y2="${p.y + 44}"/>${cl.map((c, i) => html`${hl.has(c) ? html`<rect class="colhl" x="${p.x + 4}" y="${p.y + 48 + i * ROW}" width="${BW - 8}" height="18" rx="4"/>` : ""}<text class="gcol ${hl.has(c) ? "hl" : ""}" x="${p.x + 14}" y="${p.y + 61 + i * ROW}" data-col="${c}">${trunc(c, 26)}</text>`)}</g>`);
      } else {
        parts.push(html`<g class="gn ${cls}" data-node="${id}" style="cursor:pointer"><title>${uriOf(id)}${d && d.last_success ? `\nlast success ${fmtAge(d.last_success)} ago` : ""}</title><rect x="${p.x}" y="${p.y}" width="${W}" height="${H}" rx="8"/><circle class="gdot ${tone(d)}" cx="${p.x + 14}" cy="${p.y + 16}" r="4"/><text class="gt" x="${p.x + 26}" y="${p.y + 20}">${trunc(name(id), 25)}</text><text class="gs" x="${p.x + 26}" y="${p.y + 35}">${trunc(sub, 32)}</text></g>`);
      }
    }
    // selected edge label
    if (q.edge && !g.trace) {
      const e = g.shown.find((x) => `${x.src_id}~${x.dst_id}` === q.edge);
      if (e) {
        const a = g.pos.get(e.src_id), b = g.pos.get(e.dst_id);
        const mx = (a.x + W + b.x) / 2, my = (a.y + b.y) / 2 + H / 2 - (Math.abs(a.y - b.y) < H ? H / 2 + 6 : 0);
        const t = trunc(e.pipeline || "pipeline", 26);
        const w = t.length * 6.4 + 28;
        parts.push(html`<g><rect class="ge-lbl-bg" x="${mx - w / 2}" y="${my - 10}" width="${w}" height="20" rx="10"/><text class="ge-lbl" x="${mx - w / 2 + 14}" y="${my + 4}">${t}</text></g>`);
      }
    }
    return join(parts);
  }

  function minimapMarkup() {
    const b = graph.bounds;
    return html`<svg viewBox="${b.x} ${b.y} ${b.w} ${b.h}" preserveAspectRatio="xMidYMid meet">${graph.ids.map((id) => { const p = graph.pos.get(id); return p ? html`<rect class="mm-n ${id === (q.sel || q.root) ? "sel" : ""}" x="${p.x}" y="${p.y}" width="${W}" height="${Math.max(p.h * 0.6, 14)}" rx="6"/>` : ""; })}<rect class="mm-vp" rx="6"/></svg>`;
  }

  // ── viewport ──
  const box = () => { const svg = canvasEl && canvasEl.querySelector("svg.g-main"); return { cw: (svg && svg.clientWidth) || 900, ch: (svg && svg.clientHeight) || 500 }; };
  /** The whole graph in view (zoomed out as far as needed). */
  function fitTo(b = graph.bounds) {
    const { cw, ch } = box();
    const pad = 20;
    const w0 = b.w + pad * 2, h0 = b.h + pad * 2;
    const scale = Math.max(w0 / cw, h0 / ch, 0.8);
    const w = cw * scale, h = ch * scale;
    return { x: b.x - pad - (w - w0) / 2, y: b.y - pad - (h - h0) / 2, w, h };
  }
  /** The opening view: the whole graph when it fits at a readable size,
   *  else actual size from the top, centred horizontally when it fits across. */
  function openingView() {
    const f = fitTo();
    const { cw, ch } = box();
    if (f.w <= cw * 1.15) return f;
    const b = graph.bounds;
    const x = b.w + 40 <= cw ? b.x - (cw - b.w) / 2 : b.x - 20;
    return { x, y: b.y - 10, w: cw, h: ch };
  }
  function applyVb() {
    if (!canvasEl || !vb) return;
    const svg = canvasEl.querySelector("svg.g-main");
    svg.setAttribute("viewBox", `${vb.x} ${vb.y} ${vb.w} ${vb.h}`);
    const z = canvasEl.querySelector(".lg-zoom");
    if (z) z.textContent = `${Math.round((box().cw / vb.w) * 100)}%`;
    const r = canvasEl.querySelector(".mm-vp");
    if (r) { r.setAttribute("x", vb.x); r.setAttribute("y", vb.y); r.setAttribute("width", vb.w); r.setAttribute("height", vb.h); }
  }
  function zoom(f, cx, cy) {
    const nw = Math.max(box().cw / 4, Math.min(Math.max(fitVb.w, box().cw) * 1.5, vb.w * f));
    const k = nw / vb.w;
    const px = cx ?? vb.x + vb.w / 2, py = cy ?? vb.y + vb.h / 2;
    vb = { x: px - (px - vb.x) * k, y: py - (py - vb.y) * k, w: nw, h: vb.h * k };
    applyVb();
  }
  function centreOn(id) {
    const p = graph.pos.get(id);
    if (!p) return;
    vb = { ...vb, x: p.x + W / 2 - vb.w / 2, y: p.y + (p.h || H) / 2 - vb.h / 2 };
    applyVb();
  }

  // ── chrome ──
  function bar() {
    const kinds = [...new Set([...meta.values()].map((d) => d.kind))].sort();
    const search = html`<label class="input" style="width:${q.root ? 180 : 220}px">${ic("search")}<input type="search" name="q" value="${q.q}" placeholder="Find a dataset" autocomplete="off" spellcheck="false" aria-label="Find a dataset"></label>`;
    const total = edges ? new Set(edges.flatMap((e) => [e.src_id, e.dst_id])).size : 0;
    if (q.root) {
      const depth = html`<span class="lg-depth" role="group" aria-label="Hops"><button type="button" data-depth="-1" title="Fewer hops" aria-label="Fewer hops">−</button><b>${q.depth} hop${q.depth === 1 ? "" : "s"}</b><button type="button" data-depth="1" title="More hops" aria-label="More hops">+</button></span>`;
      return html`<div class="lg-bar">${search}${chip(`focus: ${name(q.root)}`, "x", { "data-unfocus": "1", role: "button", title: "Leave focus mode" })}${depth}${select([["both", "Both ways"], ["up", "Upstream"], ["down", "Downstream"]], q.dir, { name: "dir" })}<span class="r">${fmtInt(graph ? graph.ids.length : 0)} of ${fmtInt(total)}</span></div>`;
    }
    return html`<div class="lg-bar">${search}${select([["", "Every kind"], ...kinds.map((k) => [k, k])], q.kind, { name: "kind" })}${toggle("Show sources", q.src, { name: "src" })}<span class="r">${fmtInt(graph ? graph.ids.length : 0)} datasets · ${fmtInt(graph ? graph.shown.length : 0)} edges${btn("Datasets", { cls: "sm", icon: "db", href: href.datasets() })}</span></div>`;
  }
  const legend = html`<div class="lg-legend"><span><span class="dot"></span>fresh</span><span><span class="dot warn"></span>stale</span><span><span class="dot muted"></span>never succeeded</span></div>`;
  const tools = html`<div class="gtools">${ibtn("zoomin", { title: "Zoom in (+)", data: { z: "in" } })}${ibtn("zoomout", { title: "Zoom out (−)", data: { z: "out" } })}${ibtn("fit", { title: "Fit (0)", data: { z: "fit" } })}${ibtn("focus", { title: "Focus on the selected dataset", data: { z: "focus" } })}</div>`;

  function headLine() {
    const sub = [];
    if (q.col) sub.push(html`<span>tracing column ${chip(`${name(graph.traceNode)}.${q.col}`, "mono")}</span>`, html`<span>${fmtInt(graph.ids.length)} datasets · ${fmtInt(graph.trace.cells.size)} columns</span>`);
    else if (q.root) sub.push(html`<span>focused on ${lnk(name(q.root), href.dataset(q.root))}</span>`, html`<span>${q.depth} hop${q.depth === 1 ? "" : "s"} ${q.dir === "both" ? "up and down" : q.dir === "up" ? "upstream" : "downstream"}</span>`);
    else sub.push(html`<span>${fmtInt(graph.ids.length)} datasets</span>`, html`<span>${fmtInt(graph.shown.length)} edges from recorded runs</span>`, html`<span>drag to pan · scroll to zoom · click a dataset to select, double-click to open it</span>`);
    return html`${dhead("", btn("Copy link", { icon: "link", kind: "quiet", data: { copy: location.href } }))}${head("Lineage", sub)}`;
  }

  // ── side panel ──
  function nodePanel(id) {
    const d = meta.get(id) || {};
    const up = edges.filter((e) => e.dst_id === id), down = edges.filter((e) => e.src_id === id);
    const close = ibtn("x", { title: "Close (Esc)", data: { close: "1" } });
    const tabsHtml = dtabs([["Dataset", null, "dataset"], ["Columns", null, "columns"]], q.col || q.ptab === "columns" ? "columns" : "dataset", { hrefFor: (t) => qs({ sel: id, ptab: t === "dataset" ? "" : t, col: t === "dataset" ? "" : q.col }) });
    const f = tone(d);
    const pl = f === "ok" ? pill(`Fresh · ${fmtAge(d.last_success)}`, "ok", "check") : f === "warn" ? pill(`Stale · ${fmtAge(d.last_success)}`, "warn", "clock") : pill("Never succeeded", "", "clock");
    const listOf = (es, end) => html`<div class="lg-list">${es.length ? es.map((e) => { const o = e[`${end}_id`]; return html`<div>${ring(tone(meta.get(o)) === "muted" ? "" : tone(meta.get(o)))}<a class="lnk" href="${qs({ sel: o })}" title="${uriOf(o)}">${trunc(name(o), 30)}</a><span class="r">${e.pipeline}</span></div>`; }) : html`<div class="muted">none recorded</div>`}</div>`;
    if (q.col || q.ptab === "columns") {
      const cols = colsOf(id);
      if (!q.col) {
        return html`${dhead(tabsHtml, close)}${head(name(id), [html`<span>${d.kind || "dataset"}</span>`, html`<span>${cols.length} columns</span>`])}${card({ title: "Trace a column", aside: "through every recorded hop", body: cols.length ? html`<div class="pad" style="display:flex;flex-wrap:wrap;gap:6px">${cols.map((c) => html`<a class="chip mono" href="${qs({ sel: id, col: c, ptab: "columns" })}">${c}</a>`)}</div>` : html`<div class="empty-line">No schema recorded for this dataset.</div>` })}`;
      }
      const tr = graph.trace;
      const from = tr.links.filter((l) => l.e.dst_id === id && l.dc === q.col);
      const into = tr.links.filter((l) => l.e.src_id === id && l.sc === q.col);
      const further = tr.links.filter((l) => !from.includes(l) && !into.includes(l));
      const rowsOf = (ls, side) => (ls.length ? html`<div class="lg-list">${ls.map((l) => html`<div><span class="mono" title="${uriOf(l.e[`${side}_id`])}">${trunc(name(l.e[`${side}_id`]), 24)}.${side === "src" ? l.sc : l.dc}</span><span class="r">${how(l.dc, [l.sc])}</span></div>`)}</div>` : html`<div class="empty-line">${side === "src" ? "No recorded source column." : "No recorded downstream column."}</div>`);
      return html`${dhead(tabsHtml, close)}${head(q.col, [html`<span>${name(id)}</span>`, html`<span>${(d.current_schema && d.current_schema.properties && d.current_schema.properties[q.col] && d.current_schema.properties[q.col].type) || ""}</span>`])}
        ${card({ title: "Comes from", right: fmtInt(from.length), body: rowsOf(from, "src") })}
        ${card({ title: "Flows into", right: fmtInt(into.length), body: rowsOf(into, "dst"), note: further.length ? html`Further hops: ${fmtInt(further.length)} more column links across ${fmtInt(graph.ids.length)} datasets.` : "" })}
        ${card({ title: "Other columns", body: html`<div class="pad" style="display:flex;flex-wrap:wrap;gap:6px">${colsOf(id).filter((c) => c !== q.col).map((c) => html`<a class="chip mono" href="${qs({ sel: id, col: c, ptab: "columns" })}">${c}</a>`)}</div>` })}
        <div style="display:flex;gap:8px;flex-wrap:wrap">${btn("Stop tracing", { icon: "x", href: qs({ sel: id }) })}</div>`;
    }
    return html`${dhead(tabsHtml, close)}
      ${head(name(id), [pl, html`<span>${d.kind || "dataset"}</span>`])}
      ${card({ body: grid(2, [kv("Rows last run", fmtCompact(d.last_records ?? 0)), kv("Schema", d.schema_versions ? `v${d.schema_versions}` : "—", d.first_seen ? `first seen ${fmtAge(d.first_seen)} ago` : ""), kv("Last success", d.last_success ? `${fmtAge(d.last_success)} ago` : "never", d.last_run_id ? html`run ${lnk(shortId(d.last_run_id), href.run(d.last_run_id), true, d.last_run_id)}` : ""), kv("Owners", (d.owners || []).length ? d.owners.join(", ") : "none declared")]) })}
      ${card({ title: "Upstream", right: fmtInt(up.length), body: listOf(up, "src") })}
      ${card({ title: "Downstream", right: fmtInt(down.length), body: listOf(down, "dst") })}
      <div style="display:flex;gap:8px;flex-wrap:wrap">${btn("Open dataset", { icon: "db", href: href.dataset(id) })}${d.last_run_id ? btn("Last run", { icon: "runs", href: href.run(d.last_run_id) }) : ""}${btn("Trace a column", { icon: "columns", href: qs({ sel: id, ptab: "columns" }) })}${q.root === id ? "" : btn("Focus here", { icon: "focus", href: hashFor(`#/lineage/${encodeURIComponent(id)}`, { depth: q.depth !== 2 ? q.depth : "" }) })}</div>`;
  }

  function edgePanel(key) {
    const [s, t] = key.split("~");
    const e = edges.find((x) => x.src_id === s && x.dst_id === t);
    const close = ibtn("x", { title: "Close (Esc)", data: { close: "1" } });
    if (!e) return html`${dhead(dtabs([["Edge", null, "edge"]], "edge"), close)}${empty("This edge is not in the graph", "It may have aged out of the catalogue.")}`;
    const f = (e.column_lineage && e.column_lineage.fields) || null;
    return html`${dhead(dtabs([["Edge", null, "edge"]], "edge"), close)}
      ${head(`${trunc(name(s), 22)} → ${trunc(name(t), 22)}`, [html`<span>written by a pipeline</span>`])}
      ${card({ body: grid(2, [kv("Pipeline", lnk(e.pipeline, href.runs({ label: `pipeline:${e.pipeline}` }))), kv("Matrix row", html`<span class="mono">${e.row || "—"}</span>`), kv("Runs", fmtInt(e.runs || 0), e.first_seen ? `first ${fmtAge(e.first_seen)} ago` : ""), kv("Last rows", fmtInt(e.last_records || 0), e.last_run_id ? html`run ${lnk(shortId(e.last_run_id), href.run(e.last_run_id), true, e.last_run_id)}${e.last_seen ? ` · ${fmtAge(e.last_seen)}` : ""}` : "")]) })}
      ${card({ icon: "columns", title: "Column lineage", right: f ? `${fmtInt(Object.keys(f).length)} columns` : "", body: f ? tbl(["Into", "From", "How"], Object.entries(f).map(([into, from]) => [html`<a class="lnk mono" href="${qs({ sel: t, col: into, ptab: "columns" })}">${into}</a>`, from.length ? html`<span class="mono">${from.join(", ")}</span>` : html`<span class="faint">—</span>`, how(into, from)])) : html`<div class="empty-line">No column lineage recorded: an opaque transform hides it.</div>`, note: "Recorded from the pipeline’s transforms; opaque steps are marked, never guessed." })}
      <div style="display:flex;gap:8px;flex-wrap:wrap">${btn("Open pipeline runs", { icon: "runs", href: href.runs({ label: `pipeline:${e.pipeline}` }) })}${e.last_run_id ? btn("Last run", { icon: "play", href: href.run(e.last_run_id) }) : ""}</div>`;
  }

  // ── render ──
  function renderAll(refit = false) {
    const crumbs = ["Data", q.root || q.sel ? { label: "Lineage", href: "#/lineage" } : "Lineage"];
    if (q.root || q.sel) crumbs.push(name(q.sel || q.root));
    if (err) {
      setCrumbs(["Data", "Lineage"]);
      showPanel(false);
      L.detail.innerHTML = v(html`${dhead("", "")}${head("Lineage")}${catalogUnavailable(err) ? empty("Lineage is not available", "The Data Movement Catalog is not built into this server (faucet was built without the catalog feature).", { icon: "lineage" }) : errorAlert("the lineage graph", err)}`);
      L.detail.querySelector("[data-retry]")?.addEventListener("click", loadAll);
      return;
    }
    if (!edges) {
      setCrumbs(["Data", "Lineage"]);
      showPanel(false);
      const sk = [[40, 60], [40, 160], [40, 260], [330, 110], [330, 210], [620, 80], [620, 200], [910, 150]].map(([x, y]) => `<rect x="${x}" y="${y}" width="${W}" height="${H}" rx="8" style="fill:var(--pill)"/>`).join("");
      L.detail.innerHTML = v(html`${dhead("", "")}<div><h1>Lineage</h1><div class="sub"><span class="skel" style="width:180px"></span></div></div><div class="card lg"><div class="lg-bar">${input("", { icon: "search", ph: "Find a dataset" })}</div><div class="graph"><svg viewBox="0 0 1160 360" preserveAspectRatio="xMidYMid meet">${raw(sk)}</svg></div></div>`);
      return;
    }
    setCrumbs(crumbs);
    if (!edges.length) {
      showPanel(false);
      L.detail.innerHTML = v(html`${dhead("", btn("Copy link", { icon: "link", kind: "quiet", data: { copy: location.href } }))}${head("Lineage", [html`<span>no edges yet</span>`])}<div class="card lg"><div class="lg-bar">${input("", { icon: "search", ph: "Find a dataset" })}</div><div class="graph" style="display:grid;place-items:center">${empty("No lineage recorded yet", html`An edge appears when a run reads one dataset and writes another. Run a pipeline, or turn on the catalogue with a <code>catalog:</code> block on a CLI run.`, { icon: "lineage", btns: html`${btn("Submit a run", { kind: "primary", icon: "submit", href: href.submit(), perm: "config_submit" })}${btn("Datasets", { icon: "db", href: href.datasets() })}` })}</div></div>`);
      return;
    }
    const prevBounds = graph && graph.bounds;
    graph = model();
    const sameShape = prevBounds && prevBounds.w === graph.bounds.w && prevBounds.h === graph.bounds.h;
    // Re-render the chrome only when it changed; keep the search box focused.
    const focusQ = document.activeElement && document.activeElement.name === "q";
    const caret = focusQ ? document.activeElement.selectionStart : 0;
    L.detail.innerHTML = v(html`${headLine()}<div class="card lg">${bar()}<div class="graph"><svg class="g-main" role="img" aria-label="Lineage graph" preserveAspectRatio="xMidYMid meet">${svgMarkup()}</svg>${tools}<span class="lg-zoom">100%</span><div class="minimap" title="Click or drag to move the view">${minimapMarkup()}</div>${legend}</div></div>`);
    canvasEl = L.detail.querySelector(".graph");
    if (focusQ) { const i = L.detail.querySelector("input[name=q]"); i.focus(); i.setSelectionRange(caret, caret); }
    const open = !!(q.sel || q.edge || (q.col && q.root));
    const wasOpen = !L.panel.hidden;
    showPanel(open);
    if (open) L.panel.innerHTML = v(q.edge ? edgePanel(q.edge) : nodePanel(q.sel || q.root));
    // Measure after the panel takes its column, so the view fits the canvas it gets.
    fitVb = fitTo();
    if (!vb || refit || !sameShape || wasOpen !== open) vb = openingView();
    applyVb();
    wireCanvas();
  }
  function showPanel(on) {
    body.classList.toggle("side-panel", on);
    L.panel.hidden = !on;
    L.select(on);
  }

  // ── interaction ──
  let drag = null;
  function svgPoint(ev) {
    const svg = canvasEl.querySelector("svg.g-main");
    const r = svg.getBoundingClientRect();
    // preserveAspectRatio meet: same scale both ways (the viewBox follows the box aspect after fit)
    const s = Math.max(vb.w / r.width, vb.h / r.height);
    const ox = (r.width * s - vb.w) / 2, oy = (r.height * s - vb.h) / 2;
    return { x: vb.x - ox + (ev.clientX - r.left) * s, y: vb.y - oy + (ev.clientY - r.top) * s, s };
  }
  function wireCanvas() {
    const svg = canvasEl.querySelector("svg.g-main");
    svg.addEventListener("wheel", (ev) => { ev.preventDefault(); const p = svgPoint(ev); zoom(ev.deltaY > 0 ? 1.15 : 1 / 1.15, p.x, p.y); }, { passive: false });
    svg.addEventListener("pointerdown", (ev) => {
      if (ev.button !== 0) return;
      drag = { x: ev.clientX, y: ev.clientY, vb: { ...vb }, moved: false, s: svgPoint(ev).s };
      try { svg.setPointerCapture(ev.pointerId); } catch { /* synthetic or lost pointer */ }
    });
    svg.addEventListener("pointermove", (ev) => {
      if (!drag) return;
      const dx = ev.clientX - drag.x, dy = ev.clientY - drag.y;
      if (Math.abs(dx) + Math.abs(dy) > 3) drag.moved = true;
      if (!drag.moved) return;
      vb = { ...drag.vb, x: drag.vb.x - dx * drag.s, y: drag.vb.y - dy * drag.s };
      svg.classList.add("panning");
      applyVb();
    });
    svg.addEventListener("pointerup", (ev) => {
      const d = drag;
      drag = null;
      svg.classList.remove("panning");
      if (!d || d.moved) return;
      const t = document.elementFromPoint(ev.clientX, ev.clientY);
      const col = t && t.closest("[data-col]");
      const node = t && t.closest("[data-node]");
      const edge = t && t.closest("[data-edge]");
      if (col && node) return setQuery({ sel: node.dataset.node, col: col.dataset.col, ptab: "columns", edge: "" });
      if (node) return setQuery({ sel: q.sel === node.dataset.node && !q.col ? "" : node.dataset.node, edge: "", col: "", ptab: "" });
      if (edge) return setQuery({ edge: q.edge === edge.dataset.edge ? "" : edge.dataset.edge, sel: "", col: "", ptab: "" });
      if (q.sel || q.edge) setQuery({ sel: "", edge: "", col: "", ptab: "" });
    });
    svg.addEventListener("dblclick", (ev) => { const n = ev.target.closest("[data-node]"); if (n) navigate(href.dataset(n.dataset.node)); });
    const mm = canvasEl.querySelector(".minimap svg");
    const mmMove = (ev) => {
      const r = mm.getBoundingClientRect();
      const b = graph.bounds;
      const s = Math.max(b.w / r.width, b.h / r.height);
      const ox = (r.width * s - b.w) / 2, oy = (r.height * s - b.h) / 2;
      const x = b.x - ox + (ev.clientX - r.left) * s, y = b.y - oy + (ev.clientY - r.top) * s;
      vb = { ...vb, x: x - vb.w / 2, y: y - vb.h / 2 };
      applyVb();
    };
    let mmDrag = false;
    mm.addEventListener("pointerdown", (ev) => { mmDrag = true; try { mm.setPointerCapture(ev.pointerId); } catch { /* synthetic or lost pointer */ } mmMove(ev); });
    mm.addEventListener("pointermove", (ev) => { if (mmDrag) mmMove(ev); });
    mm.addEventListener("pointerup", () => { mmDrag = false; });
  }

  L.detail.addEventListener("click", (e) => {
    const z = e.target.closest("[data-z]");
    if (z) {
      const k = z.dataset.z;
      if (k === "in") zoom(1 / 1.3);
      else if (k === "out") zoom(1.3);
      else if (k === "fit") { vb = { ...fitVb }; applyVb(); }
      else if (k === "focus") { const id = q.sel || q.root; if (id) navigate(hashFor(`#/lineage/${encodeURIComponent(id)}`, { sel: id })); }
      return;
    }
    const dep = e.target.closest("[data-depth]");
    if (dep) return setQuery({ depth: Math.max(1, Math.min(8, q.depth + Number(dep.dataset.depth))) });
    if (e.target.closest("[data-unfocus]")) return navigate(hashFor("#/lineage", { sel: q.root }));
    if (e.target.closest("[data-retry]")) return loadAll();
  });
  L.panel.addEventListener("click", (e) => { if (e.target.closest("[data-close]")) setQuery({ sel: "", edge: "", col: "", ptab: "" }); });
  L.detail.addEventListener("input", (e) => { if (e.target.name === "q") setQuery({ q: e.target.value }, { replace: true }); });
  L.detail.addEventListener("keydown", (e) => {
    if (e.target.name === "q" && e.key === "Enter") {
      const s = e.target.value.toLowerCase();
      const hit = graph && graph.ids.find((id) => `${uriOf(id)} ${name(id)}`.toLowerCase().includes(s));
      if (hit) { centreOn(hit); setQuery({ sel: hit, edge: "" }); }
    }
  });
  L.detail.addEventListener("change", (e) => {
    if (e.target.name === "kind") return setQuery({ kind: e.target.value });
    if (e.target.name === "dir") return setQuery({ dir: e.target.value === "both" ? "" : e.target.value });
    if (e.target.name === "src") return setQuery({ src: e.target.checked ? "" : "0" });
  });
  const onKey = (e) => {
    if (document.querySelector(".scrim") || /^(INPUT|TEXTAREA|SELECT)$/.test((document.activeElement || {}).tagName || "")) return;
    if (!graph || e.metaKey || e.ctrlKey || e.altKey) return;
    if (e.key === "+" || e.key === "=") zoom(1 / 1.3);
    else if (e.key === "-" || e.key === "_") zoom(1.3);
    else if (e.key === "0") { vb = { ...fitVb }; applyVb(); }
    else if (e.key === "Escape" && (q.sel || q.edge || q.col)) setQuery({ sel: "", edge: "", col: "", ptab: "" });
  };
  document.addEventListener("keydown", onKey);
  let rt = null;
  const onResize = () => { clearTimeout(rt); rt = setTimeout(() => { if (graph && canvasEl) { fitVb = fitTo(); vb = openingView(); applyVb(); } }, 150); };
  window.addEventListener("resize", onResize);

  async function loadAll() {
    err = null;
    edges = null;
    renderAll();
    try {
      const [lin, ds] = await Promise.all([api("/v1/catalog/lineage"), api("/v1/catalog/datasets?limit=1000").catch(() => ({ datasets: [] }))]);
      edges = lin.edges || [];
      meta = new Map((ds.datasets || []).map((d) => [d.id, d]));
    } catch (e) {
      err = e;
    }
    renderAll(true);
    if (q.sel && graph && graph.pos.has(q.sel) && !q.root) centreOn(q.sel);
  }
  await loadAll();

  return {
    teardown() { document.removeEventListener("keydown", onKey); window.removeEventListener("resize", onResize); },
    update(p) {
      const prev = q;
      q = read(p);
      const reshape = prev.root !== q.root || prev.depth !== q.depth || prev.dir !== q.dir || prev.kind !== q.kind || prev.src !== q.src || prev.col !== q.col || (q.col && prev.sel !== q.sel);
      renderAll(reshape);
    },
  };
}

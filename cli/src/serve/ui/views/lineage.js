// Data Movement Catalog (#279): the Lineage graph — datasets as nodes laid
// out in topological layers (sources left, sinks right), edges as curves.
// Pure SVG, no dependencies. Clicking a node opens its dataset detail.
import { api } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml } from "../utils.js";
import { catalogUnavailable } from "./datasets.js";

export async function renderLineage(container, params = {}) {
  container.innerHTML = `<div class="empty">loading…</div>`;
  let data;
  try {
    const p = new URLSearchParams();
    if (params.root) {
      p.set("root", params.root);
      p.set("depth", "8");
    }
    data = await api(`/v1/catalog/lineage?${p}`);
  } catch (e) {
    container.innerHTML = `<div class="empty">${
      catalogUnavailable(e)
        ? "The Data Movement Catalog endpoints are not available on this server " +
          "(faucet was built without the `catalog` feature)."
        : escapeHtml(e.message)
    }</div>`;
    return;
  }

  container.innerHTML = `
    <div class="page page-wide">
      <div class="page-head">
        <h1>Lineage${params.root ? " (rooted)" : ""}</h1>
        <button class="btn-ghost" id="l-datasets">← Datasets</button>
        ${params.root ? `<button class="btn-ghost" id="l-all">Whole graph</button>` : ""}
      </div>
      <p class="lineage-hint">Click a dataset to highlight its connections · double-click to open it · scroll inside a box to read the full name.</p>
      <div id="graph" class="lineage-graph"></div>
    </div>`;
  container.querySelector("#l-datasets").onclick = () => navigate("#/catalog");
  const all = container.querySelector("#l-all");
  if (all) all.onclick = () => navigate("#/lineage");

  const graph = container.querySelector("#graph");
  if (!data.edges.length) {
    graph.innerHTML = `<div class="empty">No lineage edges recorded yet — run a pipeline first.</div>`;
    return;
  }
  // Lay out once; repaint on resize so nodes always fill the available width
  // (they size to the container, so a wide screen doesn't leave dead space).
  const laid = layout(data.edges);
  const paint = () => {
    graph.innerHTML = "";
    graph.appendChild(buildSvg(laid, params.root, graph.clientWidth));
  };
  paint();
  let rt;
  const onResize = () => { clearTimeout(rt); rt = setTimeout(paint, 150); };
  window.addEventListener("resize", onResize);
  return () => { clearTimeout(rt); window.removeEventListener("resize", onResize); };
}

/** Compute topological layers: a node's layer is 1 + max layer of its inputs
 * (cycle-safe via bounded iteration). Returns { nodes, edges }. */
export function layout(edges) {
  const nodes = new Map(); // id → { id, uri, layer }
  for (const e of edges) {
    if (!nodes.has(e.src_id)) nodes.set(e.src_id, { id: e.src_id, uri: e.src_uri, layer: 0 });
    if (!nodes.has(e.dst_id)) nodes.set(e.dst_id, { id: e.dst_id, uri: e.dst_uri, layer: 0 });
  }
  // Relax layers |V| times max (cycles just stop improving).
  for (let i = 0; i < nodes.size; i++) {
    let changed = false;
    for (const e of edges) {
      const s = nodes.get(e.src_id);
      const d = nodes.get(e.dst_id);
      if (d.layer < s.layer + 1 && s.layer + 1 <= nodes.size) {
        d.layer = s.layer + 1;
        changed = true;
      }
    }
    if (!changed) break;
  }
  // Row order within each layer: minimize edge crossings with the median
  // heuristic (the core of Sugiyama layout). Seed alphabetically for
  // determinism, then sweep forward (order each layer by the median row of its
  // predecessors) and backward (by successors) a few times.
  const byLayer = new Map();
  for (const n of [...nodes.values()].sort((a, b) => a.uri.localeCompare(b.uri))) {
    const rows = byLayer.get(n.layer) || [];
    n.row = rows.length;
    rows.push(n);
    byLayer.set(n.layer, rows);
  }
  const preds = new Map();
  const succs = new Map();
  const push = (m, k, v) => {
    const a = m.get(k) || [];
    a.push(v);
    m.set(k, a);
  };
  for (const e of edges) {
    push(succs, e.src_id, e.dst_id);
    push(preds, e.dst_id, e.src_id);
  }
  const maxLayer = Math.max(0, ...[...nodes.values()].map((n) => n.layer));
  const median = (arr) => {
    if (!arr.length) return -1; // no fixed neighbours → keep current position
    const s = arr.slice().sort((a, b) => a - b);
    const m = Math.floor(s.length / 2);
    return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
  };
  const reorder = (layer, neighbours) => {
    const rows = byLayer.get(layer);
    if (!rows || rows.length < 2) return;
    const key = new Map();
    rows.forEach((n, i) => {
      const nb = (neighbours.get(n.id) || []).map((id) => nodes.get(id).row).filter((r) => r >= 0);
      const md = median(nb);
      key.set(n.id, md < 0 ? i : md);
    });
    rows.sort((a, b) => key.get(a.id) - key.get(b.id) || a.uri.localeCompare(b.uri));
    rows.forEach((n, i) => (n.row = i));
  };
  for (let sweep = 0; sweep < 4; sweep++) {
    for (let L = 1; L <= maxLayer; L++) reorder(L, preds);
    for (let L = maxLayer - 1; L >= 0; L--) reorder(L, succs);
  }
  return { nodes, edges };
}

const NODE_H = 32;
const GAP_X = 120;
const GAP_Y = 14;
const FONT_PX = 11;
const CHAR_W = 6.7; // ≈ px per char at 11px mono
const LABEL_PAD = 24; // text inset left+right
const MIN_W = 240;
const MAX_W = 620; // cap so one huge path can't blow the layout out — the rest is in the tooltip

/** Build the interactive lineage SVG. Nodes are as wide as needed to show the
 *  full dataset name (capped at MAX_W, then the graph scrolls horizontally);
 *  clicking a node selects it and highlights every directly-connected node and
 *  edge, dimming the rest. A double-click opens the dataset. */
function buildSvg({ nodes, edges }, rootId, containerWidth = 1000) {
  const ns = "http://www.w3.org/2000/svg";
  const layers = Math.max(...[...nodes.values()].map((n) => n.layer)) + 1;
  const rows = Math.max(...[...nodes.values()].map((n) => n.row)) + 1;

  // Node width is responsive: the columns fill the card's width (no dead space
  // on the right), capped so boxes don't get absurdly wide on an ultra-wide
  // screen — any remainder past the cap is centred (see .lineage-svg margin).
  // The full dataset name lives *inside* each box and scrolls left/right within
  // it (the foreignObject label below), mirroring the datasets-table URI cells.
  const avail = Math.max(560, containerWidth - 16); // minus .lineage-graph padding
  let NODE_W = Math.floor((avail - 32 - (layers - 1) * GAP_X) / layers);
  NODE_W = Math.max(320, Math.min(NODE_W, 720));

  const width = layers * NODE_W + (layers - 1) * GAP_X + 32;
  const height = rows * (NODE_H + GAP_Y) + 32;
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("width", width);
  svg.setAttribute("height", height);
  svg.setAttribute("class", "lineage-svg");

  const x = (n) => 16 + n.layer * (NODE_W + GAP_X);
  const y = (n) => 16 + n.row * (NODE_H + GAP_Y);

  // Adjacency, so a click can light up a node's whole neighbourhood.
  const neighbours = new Map(); // id → Set(neighbour id)
  const link = (a, b) => { (neighbours.get(a) || neighbours.set(a, new Set()).get(a)).add(b); };
  const edgeEls = []; // { el, src, dst }
  const nodeEls = new Map(); // id → <g>

  for (const e of edges) {
    link(e.src_id, e.dst_id);
    link(e.dst_id, e.src_id);
    const s = nodes.get(e.src_id);
    const d = nodes.get(e.dst_id);
    const x1 = x(s) + NODE_W;
    const y1 = y(s) + NODE_H / 2;
    const x2 = x(d);
    const y2 = y(d) + NODE_H / 2;
    const mid = (x1 + x2) / 2;
    const path = document.createElementNS(ns, "path");
    path.setAttribute("d", `M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`);
    path.setAttribute("class", "lineage-edge");
    const title = document.createElementNS(ns, "title");
    title.textContent = `${e.pipeline} (row ${e.row}) — ${e.runs} run(s), ${e.last_records} row(s) last`;
    path.appendChild(title);
    svg.appendChild(path);
    edgeEls.push({ el: path, src: e.src_id, dst: e.dst_id });
  }

  for (const n of nodes.values()) {
    const g = document.createElementNS(ns, "g");
    g.setAttribute("class", `lineage-node${n.id === rootId ? " lineage-root" : ""}`);
    g.setAttribute("transform", `translate(${x(n)}, ${y(n)})`);
    g.dataset.id = n.id;
    g.style.cursor = "pointer";
    const rect = document.createElementNS(ns, "rect");
    rect.setAttribute("width", NODE_W);
    rect.setAttribute("height", NODE_H);
    rect.setAttribute("rx", 8);
    // The label is an HTML div inside a foreignObject so it can scroll
    // horizontally *within the fixed-width box* — the full path is always
    // reachable by scrolling the box, never truncated.
    const fo = document.createElementNS(ns, "foreignObject");
    fo.setAttribute("x", 1);
    fo.setAttribute("y", 1);
    fo.setAttribute("width", NODE_W - 2);
    fo.setAttribute("height", NODE_H - 2);
    const div = document.createElementNS("http://www.w3.org/1999/xhtml", "div");
    div.setAttribute("class", "lnode-label");
    div.setAttribute("title", n.uri);
    div.textContent = n.uri;
    fo.appendChild(div);
    const title = document.createElementNS(ns, "title");
    title.textContent = `${n.uri}\n(click to highlight connections · double-click to open · scroll the box to read the full name)`;
    g.appendChild(rect);
    g.appendChild(fo);
    g.appendChild(title);
    svg.appendChild(g);
    nodeEls.set(n.id, g);
  }

  // Selection: single click highlights the node + its neighbours + their edges
  // and dims everything else; clicking it again (or the background) clears.
  // Double-click opens the dataset.
  let selected = null;
  const clear = () => {
    selected = null;
    svg.classList.remove("has-selection");
    for (const g of nodeEls.values()) g.classList.remove("is-selected", "is-neighbour");
    for (const { el } of edgeEls) el.classList.remove("is-active");
  };
  const select = (id) => {
    if (selected === id) { clear(); return; }
    clear();
    selected = id;
    svg.classList.add("has-selection");
    nodeEls.get(id)?.classList.add("is-selected");
    for (const nb of neighbours.get(id) || []) nodeEls.get(nb)?.classList.add("is-neighbour");
    for (const e of edgeEls) if (e.src === id || e.dst === id) el_active(e.el);
  };
  const el_active = (el) => el.classList.add("is-active");
  for (const [id, g] of nodeEls) {
    g.addEventListener("click", (ev) => { ev.stopPropagation(); select(id); });
    g.addEventListener("dblclick", (ev) => { ev.stopPropagation(); navigate(`#/catalog/${id}`); });
  }
  svg.addEventListener("click", clear); // click empty space to clear
  return svg;
}

function shorten(s, max) {
  return s.length <= max ? s : `…${s.slice(s.length - max + 1)}`;
}

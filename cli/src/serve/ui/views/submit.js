// Submit (#998): canvas — build a pipeline config from the connector schemas,
// check it (doctor), plan it and run it (or request approval), with a live YAML
// preview beside the form. Guided steps: Source · Sink · Transforms · Options ·
// Review; Editor mode takes a whole config. URL: ?step= ?mode=editor
// ?source= ?sink= ?run=<id> (prefill from a past run's stored config).
import { api } from "../api.js";
import { html, raw, v, ic, esc, head, card, field, input, select, check, toggle, btn, ibtn, alert, code, tbl, kv, grid, steps, dhead, lnk, menu, diff, skelCards, empty, chip, cpi, shortId } from "../components.js";
import { layout, setQuery, toast } from "../page.js";
import { setCrumbs, setContext } from "../shell.js";
import { navigate } from "../router.js";
import { href } from "../links.js";
import { can } from "../access.js";
import { mdInline } from "../utils.js";
import { isSecretField } from "../secret-field.js";

const STEPS = ["Source", "Sink", "Transforms", "Options", "Review"];
const RELIABILITY = [["state", "bookmarks survive restarts"], ["dlq", "failed rows land here"], ["delivery", "exactly-once needs a CDC source"], ["resilience", "retries and a circuit breaker"], ["sla", "freshness and volume alerts"]];
const GOVERNANCE = [["quality", "quality"], ["contract", "contract"], ["masking", "masking"], ["schema", "schema drift"]];

// ---- small DOM helpers -------------------------------------------------------------
const node = (markup) => {
  const t = document.createElement("template");
  t.innerHTML = v(markup).trim();
  return t.content.firstElementChild;
};
const firstSentence = (s) => {
  if (!s) return "";
  const flat = String(s).replace(/\s+/g, " ").replace(/\[([^\]]+)\]\((?!https?:)[^)]*\)/g, "$1");
  const m = flat.split(/(?<!\b(?:e\.g|i\.e|etc|vs))\.\s/)[0];
  return (m.length > 140 ? `${m.slice(0, 137)}…` : m).replace(/\.$/, "");
};
/** A short phrase from a description, for checkbox labels and sub-panel hints. */
const brief = (s0, max = 52) => {
  let t = firstSentence(s0).split(/: | \(|; | — | - /)[0].replace(/`/g, "");
  if (t.length < 16) t = firstSentence(s0).replace(/`/g, "");
  return t.length > max ? `${t.slice(0, max - 1).replace(/\s+\S*$/, "")}…` : t;
};
const helpOf = (d) => (d ? raw(mdInline(firstSentence(d))) : "");

// ---- YAML out (no YAML library in the console) -------------------------------------
export function toYaml(obj) {
  const plain = /^[A-Za-z_$][A-Za-z0-9_\-./{}:$ ]*$/;
  const reserved = /^(true|false|yes|no|on|off|y|n|null|~)$/i;
  const scalar = (x) => {
    if (x === null || x === undefined) return "null";
    if (typeof x === "number") return Number.isFinite(x) ? String(x) : JSON.stringify(String(x));
    if (typeof x === "boolean") return String(x);
    const t = String(x);
    return plain.test(t) && !reserved.test(t) && !/: |:$| #|^ | $/.test(t) && !/^\d/.test(t) ? t : JSON.stringify(t);
  };
  const isColl = (x) => x !== null && typeof x === "object" && Object.keys(x).length > 0;
  const inline = (x) => (Array.isArray(x) ? "[]" : x !== null && typeof x === "object" ? "{}" : scalar(x));
  const lines = (x, ind) => {
    const pad = "  ".repeat(ind);
    if (Array.isArray(x)) {
      return x.flatMap((item) => {
        if (!isColl(item)) return [`${pad}- ${inline(item)}`];
        const inner = lines(item, ind + 1);
        return [`${pad}- ${inner[0].slice(pad.length + 2)}`, ...inner.slice(1)];
      });
    }
    return Object.entries(x).flatMap(([k, val]) => (isColl(val) ? [`${pad}${scalar(k)}:`, ...lines(val, ind + 1)] : [`${pad}${scalar(k)}: ${inline(val)}`]));
  };
  return lines(obj, 0).join("\n");
}

/** Line diff (LCS) for "changes from the original run". */
function lineDiff(a, b) {
  const A = a.split("\n"), B = b.split("\n");
  const n = A.length, m = B.length;
  if (n * m > 400000) return [["@", "too large to compare"]];
  const L = Array.from({ length: n + 1 }, () => new Int32Array(m + 1));
  for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) L[i][j] = A[i] === B[j] ? L[i + 1][j + 1] + 1 : Math.max(L[i + 1][j], L[i][j + 1]);
  const out = [];
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (A[i] === B[j]) { out.push(["", A[i]]); i++; j++; }
    else if (L[i + 1][j] >= L[i][j + 1]) out.push(["-", A[i++]]);
    else out.push(["+", B[j++]]);
  }
  while (i < n) out.push(["-", A[i++]]);
  while (j < m) out.push(["+", B[j++]]);
  // Keep one line of context around each change.
  const keep = out.map((x, k) => x[0] !== "" || (out[k - 1] && out[k - 1][0] !== "") || (out[k + 1] && out[k + 1][0] !== ""));
  const res = [];
  out.forEach((x, k) => { if (keep[k]) res.push(x); else if (res.length && res[res.length - 1][0] !== "@") res.push(["@", "⋯"]); });
  return res;
}

// ---- the schema form, drawn with the design system ---------------------------------
function deref(schema, root) {
  let s = schema;
  for (let i = 0; s && s.$ref && i < 8; i++) {
    const tgt = s.$ref.replace(/^#\//, "").split("/").reduce((n, k) => (n ? n[k] : undefined), root);
    const { $ref, ...rest } = s;
    s = { ...(tgt || {}), ...rest };
  }
  return s || {};
}
const typeOf = (s) => (typeof s.type === "string" ? s.type : Array.isArray(s.type) ? s.type.find((t) => t !== "null") : s.properties ? "object" : s.items ? "array" : s.enum ? typeof s.enum[0] : "string");
const tagOf = (s) => {
  const p = s && s.properties && s.properties.type;
  return p ? (p.const !== undefined ? p.const : Array.isArray(p.enum) && p.enum.length === 1 ? p.enum[0] : null) : null;
};
const emptyish = (x) => x === undefined || x === "" || (x !== null && typeof x === "object" && !Array.isArray(x) && !Object.keys(x).length);

/** Build a control for one schema node. Returns `{ el, read, validate, block }`:
 *  `block` controls render below the field grid in a sub-panel. */
function build(schema0, root, key, value, required, onChange) {
  const desc = schema0.description;
  let s = deref(schema0, root);
  const variants = s.oneOf || s.anyOf;
  if (Array.isArray(variants)) {
    const vs = variants.map((x) => deref(x, root));
    const real = vs.filter((x) => x.type !== "null");
    // Option<T>
    if (real.length === 1 && real.length < vs.length) {
      const inner = { ...real[0], description: desc || real[0].description, default: s.default };
      if (typeOf(inner) === "object" && inner.properties) return optionalObject(inner, root, key, value, onChange);
      return build(inner, root, key, value, false, onChange);
    }
    if (vs.length && vs.every((x) => x.const !== undefined || Array.isArray(x.enum))) return enumField({ ...s, enum: vs.flatMap((x) => (x.const !== undefined ? [x.const] : x.enum)) }, key, value, required, desc, onChange);
    if (vs.every((x) => tagOf(x) !== null)) return union(vs, root, key, value, desc, onChange);
    return rawField(key, value, desc, onChange);
  }
  if (Array.isArray(s.enum)) return enumField(s, key, value, required, desc, onChange);
  const t = typeOf(s);
  if (t === "object" && s.properties) return objectBlock(s, root, key, value, required, desc, onChange);
  if (t === "array") {
    const it = deref(s.items || {}, root);
    if (["string", "integer", "number"].includes(typeOf(it)) && !it.properties) return arrayField(s, it, key, value, required, desc, onChange);
    return rawField(key, value, desc, onChange);
  }
  if (t === "boolean") return boolField(s, key, value, desc, onChange);
  if (t === "integer" || t === "number") return numberField(s, t, key, value, required, desc, onChange);
  if (t === "string") return stringField(s, key, value, required, desc, onChange);
  return rawField(key, value, desc, onChange);
}

function labelled(key, control, { req = false, help = "", cls = "" } = {}) {
  return node(field(key, control, { code: true, req, help, cls }));
}
function setErr(el, msg) {
  const ctl = el.querySelector(".input, .area, .select");
  if (ctl) ctl.classList.toggle("err", !!msg);
  let h = el.querySelector(":scope > .help");
  if (!h && msg) { h = document.createElement("div"); h.className = "help"; el.appendChild(h); }
  if (h) {
    if (!h.dataset.orig && !msg) return;
    if (h.dataset.orig === undefined) h.dataset.orig = h.innerHTML;
    h.classList.toggle("bad", !!msg);
    h.innerHTML = msg ? esc(msg) : h.dataset.orig;
  }
}

function stringField(s, key, value, required, desc, onChange) {
  const sql = s["x-faucet-sql"] || key === "query";
  const secret = isSecretField(key, s.title);
  const def = s.default != null ? String(s.default) : "";
  const init = value != null ? String(value) : def;
  const el = sql
    ? labelled(key, html`<textarea class="area mono" rows="3" name="${key}" spellcheck="false" style="height:58px">${init}</textarea>`, { req: required, help: helpOf(desc), cls: "wide" })
    : labelled(key, input(init, { name: key, mono: secret || /path|url|uri|column|table|key|name|_id$/.test(key), type: secret && !/^\$\{/.test(init) ? "password" : "text", ph: secret ? "${env:NAME} or ${secret:…}" : "" }), { req: required, help: secret ? html`${helpOf(desc)}${desc ? ". " : ""}Use a secret reference, never a literal secret.` : helpOf(desc) });
  const ctl = el.querySelector("input, textarea");
  ctl.addEventListener("input", onChange);
  return {
    el,
    read: () => { const x = ctl.value; return x === "" || (def !== "" && x === def) ? undefined : x; },
    validate() {
      const msg = required && !ctl.value.trim() ? "Required." : s.pattern && ctl.value && !new RegExp(s.pattern).test(ctl.value) ? `Must match ${s.pattern}.` : "";
      setErr(el, msg);
      return msg ? [{ key, msg, el: ctl }] : [];
    },
  };
}
function numberField(s, t, key, value, required, desc, onChange) {
  const def = s.default != null ? String(s.default) : "";
  const el = labelled(key, input(value != null ? String(value) : def, { name: key, a: { inputmode: "numeric" } }), { req: required, help: helpOf(desc) });
  const ctl = el.querySelector("input");
  ctl.addEventListener("input", onChange);
  const parse = () => (ctl.value.trim() === "" ? undefined : Number(ctl.value.replace(/[,_]/g, "")));
  return {
    el,
    read: () => { const n = parse(); return n === undefined || Number.isNaN(n) ? (ctl.value.trim() ? ctl.value : undefined) : def !== "" && String(n) === def ? undefined : n; },
    validate() {
      const n = parse();
      let msg = "";
      if (n === undefined) msg = required ? "Required." : "";
      else if (Number.isNaN(n)) msg = "Must be a number.";
      else if (t === "integer" && !Number.isInteger(n)) msg = `Must be a whole number${s.minimum != null ? ` of at least ${s.minimum}` : ""}${s.maximum != null ? ` and at most ${s.maximum.toLocaleString("en-US")}` : ""}.`;
      else if (s.minimum != null && n < s.minimum) msg = `Must be at least ${s.minimum}.`;
      else if (s.maximum != null && n > s.maximum) msg = `Must be at most ${s.maximum.toLocaleString("en-US")}.`;
      setErr(el, msg);
      return msg ? [{ key, msg, el: ctl }] : [];
    },
  };
}
function boolField(s, key, value, desc, onChange) {
  const on = value != null ? !!value : !!s.default;
  const el = labelled(key, check(brief(desc) || key, on, { name: key }), { help: firstSentence(desc).length > brief(desc).length + 2 ? helpOf(desc) : "" });
  const cb = el.querySelector("input");
  cb.addEventListener("change", () => { cb.closest(".check").classList.toggle("on", cb.checked); onChange(); });
  return { el, read: () => (cb.checked === !!s.default ? undefined : cb.checked), validate: () => [] };
}
function enumField(s, key, value, required, desc, onChange) {
  const def = s.default;
  const opts = s.enum.map((x) => [String(x), String(x)]);
  if (!required && def === undefined) opts.unshift(["", "—"]);
  const init = value != null ? String(value) : def != null ? String(def) : "";
  const el = labelled(key, select(opts, init, { name: key }), { req: required, help: helpOf(desc) });
  const sel = el.querySelector("select");
  sel.addEventListener("change", onChange);
  const back = (x) => s.enum.find((e) => String(e) === x);
  return { el, read: () => (sel.value === "" || (def != null && sel.value === String(def)) ? undefined : back(sel.value)), validate: () => [] };
}
function arrayField(s, it, key, value, required, desc, onChange) {
  const el = labelled(key, html`<div class="sb-arr"><div class="add">${btn("Add", { icon: "plus", cls: "sm", data: { add: "1" } })}</div></div>`, { req: required, help: helpOf(desc), cls: "wide" });
  const list = el.querySelector(".sb-arr");
  const addRow = (x = "") => {
    const r = node(html`<div>${input(x, { mono: true })}${ibtn("x", { title: "Remove" })}</div>`);
    r.querySelector("input").addEventListener("input", onChange);
    r.querySelector("button").onclick = () => { r.remove(); onChange(); };
    list.insertBefore(r, list.querySelector(".add"));
    return r;
  };
  (Array.isArray(value) ? value : []).forEach((x) => addRow(String(x)));
  if (!list.querySelector(":scope > div:not(.add)")) addRow();
  list.querySelector("[data-add]").onclick = () => { addRow().querySelector("input").focus(); };
  const num = ["integer", "number"].includes(typeOf(it));
  const vals = () => [...list.querySelectorAll(":scope > div:not(.add) input")].map((i) => i.value.trim()).filter(Boolean).map((x) => (num && !Number.isNaN(Number(x)) ? Number(x) : x));
  return {
    el,
    read: () => (vals().length ? vals() : undefined),
    validate() { const msg = required && !vals().length ? "Add at least one value." : ""; setErr(el, msg); return msg ? [{ key, msg, el: list.querySelector("input") }] : []; },
  };
}
function rawField(key, value, desc, onChange) {
  const el = labelled(key, html`<textarea class="area mono" rows="3" spellcheck="false" placeholder="JSON" style="height:70px">${value !== undefined ? JSON.stringify(value, null, 2) : ""}</textarea>`, { help: html`${helpOf(desc)}${desc ? ". " : ""}Written as JSON.`, cls: "wide" });
  const ta = el.querySelector("textarea");
  ta.addEventListener("input", onChange);
  return {
    el,
    read() { if (!ta.value.trim()) return undefined; try { return JSON.parse(ta.value); } catch { return ta.value; } },
    validate() {
      let msg = "";
      if (ta.value.trim()) { try { JSON.parse(ta.value); } catch (e) { msg = `Not valid JSON: ${e.message}`; } }
      setErr(el, msg);
      return msg ? [{ key, msg, el: ta }] : [];
    },
  };
}
/** An object's fields: scalars in a two-column grid, nested blocks below. */
function objectFields(s, root, value, onChange, { prefix = "" } = {}) {
  const req = new Set(s.required || []);
  const grid = node(html`<div class="form"></div>`);
  const blocks = [];
  const readers = {};
  const validators = [];
  for (const [k, sub] of Object.entries(s.properties || {})) {
    if (k === "type" && tagOf(s)) continue;
    const c = build(sub, root, k, value ? value[k] : undefined, req.has(k), onChange);
    (c.block ? blocks : [grid]).length;
    if (c.block) blocks.push(c.el); else grid.appendChild(c.el);
    readers[k] = c.read;
    validators.push(() => c.validate().map((e) => ({ ...e, key: `${prefix}${e.key}` })));
  }
  const frag = document.createDocumentFragment();
  if (grid.children.length) frag.appendChild(grid);
  blocks.forEach((b) => frag.appendChild(b));
  return {
    frag,
    read() {
      const out = {};
      for (const [k, r] of Object.entries(readers)) { const x = r(); if (!emptyish(x)) out[k] = x; }
      return out;
    },
    validate: () => validators.flatMap((f) => f()),
  };
}
function sub(title, right = "") {
  return node(html`<div class="sb-sub"><div class="h"><b>${title}</b>${right ? html`<span class="r">${right}</span>` : ""}</div></div>`);
}
function objectBlock(s, root, key, value, required, desc, onChange) {
  const el = sub(html`<code>${key}</code>`, desc ? brief(desc, 44) : "");
  const f = objectFields(s, root, value || {}, onChange, { prefix: `${key}.` });
  el.appendChild(f.frag);
  return { el, block: true, read: f.read, validate: f.validate };
}
function optionalObject(s, root, key, value, onChange) {
  const on = value !== undefined && value !== null;
  const el = sub(html`<label class="toggle${on ? " on" : ""}"><input type="checkbox"${raw(on ? " checked" : "")}><i></i><code>${key}</code></label>`, s.description ? `optional · ${brief(s.description, 44).replace(/^Optional\s+/i, "")}` : "optional");
  const body = document.createElement("div");
  const f = objectFields(s, root, value || {}, onChange, { prefix: `${key}.` });
  body.appendChild(f.frag);
  body.hidden = !on;
  el.appendChild(body);
  const cb = el.querySelector(".h input");
  const sync = () => { cb.closest(".toggle").classList.toggle("on", cb.checked); body.hidden = !cb.checked; el.querySelector(".h").style.borderBottomColor = cb.checked ? "" : "transparent"; };
  cb.addEventListener("change", () => { sync(); onChange(); });
  sync();
  return { el, block: true, read: () => (cb.checked ? f.read() : undefined), validate: () => (cb.checked ? f.validate() : []) };
}
/** `{type, config}` and other tagged unions: a segmented picker, then the variant's fields. */
function unionBody(vs, root, key, value, onChange) {
  const tags = vs.map(tagOf);
  let cur = value && tags.includes(value.type) ? value.type : tags[0];
  const wrap = document.createElement("div");
  const seg = node(html`<div class="form one"><div class="wide"><span class="seg" role="radiogroup">${tags.map((t) => html`<span role="radio" tabindex="0" data-v="${t}" class="${t === cur ? "on" : ""}">${t}</span>`)}</span></div></div>`);
  wrap.appendChild(seg);
  const host = document.createElement("div");
  wrap.appendChild(host);
  let f = null;
  let configShaped = false;
  const mount = (val) => {
    host.innerHTML = "";
    const vnt = vs[tags.indexOf(cur)];
    const props = { ...(vnt.properties || {}) };
    delete props.type;
    const keys = Object.keys(props);
    configShaped = keys.length === 1 && keys[0] === "config";
    const inner = configShaped ? deref(props.config, root) : { type: "object", properties: props, required: (vnt.required || []).filter((r) => r !== "type") };
    const d = vnt.description || inner.description;
    if (d) host.appendChild(node(html`<div class="form one" style="padding-top:0"><div class="help">${helpOf(d)}</div></div>`));
    f = objectFields(inner, root, configShaped ? (val && val.config) || {} : val || {}, onChange, { prefix: `${key}.` });
    host.appendChild(f.frag);
  };
  mount(value);
  seg.addEventListener("click", (e) => {
    const b = e.target.closest("[data-v]");
    if (!b || b.dataset.v === cur) return;
    cur = b.dataset.v;
    seg.querySelectorAll("[data-v]").forEach((x) => x.classList.toggle("on", x === b));
    mount(undefined);
    onChange();
  });
  seg.addEventListener("keydown", (e) => { if ((e.key === "Enter" || e.key === " ") && e.target.dataset.v) { e.preventDefault(); e.target.click(); } });
  return {
    wrap,
    read: () => { const inner = f.read(); return configShaped ? { type: cur, config: inner } : { type: cur, ...inner }; },
    validate: () => f.validate(),
  };
}
function union(vs, root, key, value, desc, onChange) {
  const el = sub(html`<code>${key}</code>`, `one of ${vs.length}`);
  const u = unionBody(vs, root, key, value, onChange);
  el.appendChild(u.wrap);
  return { el, block: true, read: u.read, validate: u.validate };
}
/** A whole connector/block form in `host`. */
function mountForm(host, schema, value, onChange) {
  host.innerHTML = "";
  const root = schema;
  const s = deref(schema, root);
  const vs = (s.oneOf || s.anyOf || []).map((x) => deref(x, root));
  if (vs.length && vs.every((x) => tagOf(x) !== null)) {
    const u = unionBody(vs, root, "", value, onChange);
    host.appendChild(u.wrap);
    return { read: u.read, validate: u.validate };
  }
  const f = objectFields(s, root, value || {}, onChange);
  host.appendChild(f.frag);
  return { read: f.read, validate: f.validate };
}

// ---- duration text ("2h", "90m", "45s", "3600") → seconds ------------------------
function secs(text) {
  const t = String(text || "").trim();
  if (!t) return undefined;
  const m = t.match(/^(\d+(?:\.\d+)?)\s*(s|m|h|d)?$/i);
  if (!m) return NaN;
  return Math.round(Number(m[1]) * { s: 1, m: 60, h: 3600, d: 86400 }[(m[2] || "s").toLowerCase()]);
}

export async function renderSubmit(host, params) {
  const { detail } = layout(host, "canvas");
  setCrumbs(["Build", "Submit"]);
  detail.innerHTML = v(html`${head("Submit a run")}${skelCards(3)}`);
  let catalog;
  try {
    catalog = await api("/v1/schemas");
  } catch (e) {
    detail.innerHTML = v(alert("Couldn’t load the connector catalogue.", e.message, "bad", btn("Retry", { icon: "refresh", cls: "sm", data: { retry: "1" } })));
    detail.querySelector("[data-retry]").onclick = () => renderSubmit(host, params);
    return;
  }
  if (!catalog.sources.length || !catalog.sinks.length) {
    detail.innerHTML = v(html`${head("Submit a run")}${empty("This server has no sources or sinks compiled in", "Rebuild faucet with connector features to submit runs from the console.", { icon: "submit" })}`);
    return;
  }
  const blockMeta = new Map((catalog.blocks || []).map((b) => [b.name, b]));
  const canRun = can("config_submit");
  const q = params.query;
  const st = {
    step: STEPS.includes(cap(q.get("step"))) ? cap(q.get("step")) : "Source",
    mode: q.get("mode") === "editor" ? "editor" : "guided",
    src: { type: pick(catalog.sources, q.get("source")), form: null },
    snk: { type: pick(catalog.sinks, q.get("sink")), form: null },
    tx: [],
    blocks: new Map(),
    doctor: null, // { ok, invocations, error, at, ms }
    plan: null,   // plan response or { error }
    errors: [],
    editorSeeded: "",
    fromRun: null, // { id, name, body }
  };
  function cap(s) { return s ? s[0].toUpperCase() + s.slice(1) : ""; }
  function pick(list, name) { return (list.find((x) => x.name === name) || list.find((x) => x.name === "file") || list[0]).name; }

  // ---- page skeleton (built once; steps toggle) ----
  const modeSeg = (m) => html`<span class="seg" role="tablist"><span role="tab" data-mode="guided" class="${m === "guided" ? "on" : ""}">Guided</span><span role="tab" data-mode="editor" class="${m === "editor" ? "on" : ""}">Editor</span></span>`;
  const actions = html`${btn("Validate", { icon: "check", perm: "doctor", data: { act: "validate" } })}${btn("Plan", { icon: "diff", perm: "plan", data: { act: "plan" } })}${btn("Submit run", { icon: "submit", kind: "primary", perm: "config_submit", data: { act: "submit" } })}`;
  detail.innerHTML = v(html`
    <div class="dhead"><span class="sb-mode"></span><span class="r">${actions}</span></div>
    ${head("Submit a run", [], "Pick a source and a sink; the form comes from each connector’s schema. Validate probes every connection without moving data.")}
    <div class="sb-banner stack"></div>
    <div class="sb-grid" data-pane="guided">
      <div class="stack">
        <div class="card sb-steps"></div>
        <div class="stack" data-step="Source"></div>
        <div class="stack" data-step="Sink"></div>
        <div class="stack" data-step="Transforms"></div>
        <div class="stack" data-step="Options"></div>
        <div class="stack" data-step="Review"></div>
      </div>
      <div class="stack sb-side"></div>
    </div>
    <div class="sb-grid" data-pane="editor" hidden>
      <div class="stack sb-edcol"></div>
      <div class="stack sb-edside"></div>
    </div>`);
  const $ = (sel) => detail.querySelector(sel);
  const stepHost = (name) => detail.querySelector(`[data-step="${name}"]`);

  // ---- live preview ----
  const preview = node(card({ icon: "code", title: "Config", aside: "live preview", right: btn("Copy", { icon: "copy", cls: "sm", data: { copy: "" } }), body: html`<div class="sb-prev"></div>`, note: " ", cls: "sb-preview" }));
  $(".sb-side").appendChild(preview);
  let pending = 0;
  const changed = () => { cancelAnimationFrame(pending); pending = requestAnimationFrame(refreshPreview); };

  function buildConfig() {
    const cfg = { version: 1 };
    const name = runOpt("name");
    if (name) cfg.name = slug(name);
    cfg.pipeline = {};
    if (st.src.form) cfg.pipeline.source = { type: st.src.type, config: st.src.form.read() };
    if (st.snk.form) cfg.pipeline.sink = { type: st.snk.type, config: st.snk.form.read() };
    const tx = st.tx.filter((t) => t.form).map((t) => ({ type: t.type, config: t.form.read() }));
    if (tx.length) cfg.pipeline.transforms = tx;
    for (const [nm, b] of st.blocks) {
      if (!b.on || !b.read) continue;
      const val = b.read();
      if (emptyish(val)) continue;
      if (b.placement === "pipeline") cfg.pipeline[nm] = val; else cfg[nm] = val;
    }
    return cfg;
  }
  function slug(s) { return s.toLowerCase().replace(/[^a-z0-9_-]+/g, "_").replace(/^_+|_+$/g, "") || undefined; }

  function refreshPreview() {
    const yaml = toYaml(buildConfig());
    const lines = yaml.split("\n").length;
    preview.querySelector(".sb-prev").innerHTML = v(code(yaml, { box: false, copy: false }));
    preview.querySelector("[data-copy]").dataset.copy = yaml;
    const errs = st.errors.length;
    const appr = st.blocks.size >= 0 && runOptChecked("approval") ? html` · approval required` : "";
    preview.querySelector(".note").innerHTML = v(errs ? html`<span class="bad">${errs} error${errs > 1 ? "s" : ""}</span> · fix ${errs > 1 ? "them" : "it"} in the form` : html`<span class="ok">valid YAML</span> · ${lines} lines · secrets stay as references${appr}`);
  }

  // ---- stepper ----
  function paintSteps() {
    const at = STEPS.indexOf(st.step);
    const s = $(".sb-steps");
    s.innerHTML = v(steps(STEPS.map((l, i) => [l, i < at ? "done" : i === at ? "cur" : ""])));
    s.querySelectorAll(".step").forEach((el, i) => { el.dataset.go = STEPS[i]; el.setAttribute("role", "button"); el.tabIndex = 0; });
    detail.querySelectorAll("[data-step]").forEach((el) => (el.hidden = el.dataset.step !== st.step));
  }

  // ---- connector cards ----
  async function connectorCard(kind, slot, hostEl, value) {
    const list = kind === "source" ? catalog.sources : catalog.sinks;
    const title = kind === "source" ? "Source" : "Sink";
    const c = node(card({ icon: "db", title, right: html`${select(list.map((x) => [x.name, x.name]), slot.type, { cls: "sb-kind", a: { "aria-label": `${title} kind` } })}${lnk("schema", href.schema(kind, slot.type), false)}`, body: html`<div class="sb-form"><div class="pad">${skelCards(1)}</div></div>` }));
    hostEl.innerHTML = "";
    hostEl.appendChild(c);
    const sel = c.querySelector(".sb-kind select");
    const formHost = c.querySelector(".sb-form");
    const load = async (val) => {
      c.querySelector(".h a.lnk").setAttribute("href", href.schema(kind, slot.type));
      formHost.innerHTML = v(html`<div class="pad">${skelCards(1)}</div>`);
      try {
        const schema = await api(`/v1/schemas/${kind}/${encodeURIComponent(slot.type)}`);
        slot.form = mountForm(formHost, schema, val, changed);
      } catch (e) {
        slot.form = null;
        formHost.innerHTML = v(html`<div class="pad">${alert(`Couldn’t load the ${slot.type} schema.`, e.message, "bad")}</div>`);
      }
      changed();
    };
    sel.addEventListener("change", () => { slot.type = sel.value; setQuery({ [kind]: sel.value }, { replace: true }); load(); });
    await load(value);
  }

  // ---- transforms ----
  function paintTransforms() {
    const h = stepHost("Transforms");
    h.querySelectorAll(":scope > .sb-tx").forEach((el) => el.remove());
    st.tx.forEach((t, i) => {
      if (!t.el) {
        t.el = node(card({ title: html`<span class="sb-txn"></span> · <span class="mono" style="font-weight:400">${t.type}</span>`, right: html`${ibtn("updown", { title: "Move up", data: { move: "1" } })}${ibtn("trash", { title: "Remove", data: { remove: "1" } })}`, body: html`<div class="sb-form"><div class="pad">${skelCards(1)}</div></div>`, cls: "sb-tx" }));
        api(`/v1/schemas/transform/${encodeURIComponent(t.type)}`).then((schema) => {
          t.form = mountForm(t.el.querySelector(".sb-form"), schema, t.value, changed);
          t.el.querySelectorAll(".sb-form > .form").forEach((f) => { if (Object.keys(schema.properties || {}).length >= 3) f.classList.add("three"); });
          changed();
        }).catch((e) => { t.el.querySelector(".sb-form").innerHTML = v(html`<div class="pad">${alert(`Couldn’t load ${t.type}.`, e.message, "bad")}</div>`); });
        t.el.querySelector("[data-move]").onclick = () => { const k = st.tx.indexOf(t); if (k > 0) { st.tx.splice(k, 1); st.tx.splice(k - 1, 0, t); } else if (st.tx.length > 1) { st.tx.splice(k, 1); st.tx.push(t); } paintTransforms(); changed(); };
        t.el.querySelector("[data-remove]").onclick = () => { st.tx.splice(st.tx.indexOf(t), 1); paintTransforms(); changed(); };
      }
      t.el.querySelector(".sb-txn").textContent = String(i + 1);
      h.insertBefore(t.el, h.querySelector(".sb-txadd"));
    });
    const empt = h.querySelector(".sb-txempty");
    if (empt) empt.hidden = st.tx.length > 0;
  }
  function setupTransforms() {
    const h = stepHost("Transforms");
    h.appendChild(node(html`<div class="sb-txempty">${card({ body: html`<div class="empty-line">No transforms: records go to the sink as the source reads them. Add one to rename, cast, select, mask or reshape fields.</div>` })}</div>`));
    h.appendChild(node(html`<div class="sb-txadd">${menu(html`${ic("plus")}Add transform`, [{ heading: "Transforms built into this server" }, ...catalog.transforms.map((t) => ({ label: html`<span class="mono">${t.name}</span>`, hint: brief(t.description, 30), data: { addtx: t.name } }))], { left: true })}</div>`));
    h.addEventListener("click", (e) => {
      const a = e.target.closest("[data-addtx]");
      if (!a) return;
      st.tx.push({ type: a.dataset.addtx, form: null, el: null });
      paintTransforms();
      changed();
    });
  }

  // ---- options ----
  function blockSub(name, hint, initial) {
    const meta = blockMeta.get(name);
    const b = { on: initial !== undefined, placement: meta.placement, read: null, value: initial };
    st.blocks.set(name, b);
    const el = sub(html`<label class="toggle${b.on ? " on" : ""}"><input type="checkbox"${raw(b.on ? " checked" : "")}><i></i><code>${name}</code></label>`, hint || firstSentence(meta.description));
    const body = document.createElement("div");
    el.appendChild(body);
    let loaded = false;
    const cb = el.querySelector(".h input");
    const sync = async () => {
      b.on = cb.checked;
      cb.closest(".toggle").classList.toggle("on", b.on);
      body.hidden = !b.on;
      el.querySelector(".h").style.borderBottomColor = b.on ? "" : "transparent";
      if (b.on && !loaded) {
        loaded = true;
        body.innerHTML = v(html`<div class="pad">${skelCards(1)}</div>`);
        try {
          const schema = await api(`/v1/schemas/block/${encodeURIComponent(name)}`);
          b.read = name === "dlq" ? dlqForm(body, schema, b.value) : mountForm(body, schema, b.value, changed).read;
          const f = name === "dlq" ? null : null;
          b.validate = f;
        } catch (e) {
          body.innerHTML = v(html`<div class="pad">${alert(`Couldn’t load the ${name} block.`, e.message, "bad")}</div>`);
        }
      }
      changed();
    };
    cb.addEventListener("change", sync);
    if (b.on) sync(); else { body.hidden = true; el.querySelector(".h").style.borderBottomColor = "transparent"; }
    return el;
  }
  // The DLQ's `sink` is a whole connector: pick its kind, then that sink's form.
  function dlqForm(host, schema, value) {
    host.innerHTML = "";
    const { sink: _s, ...rest } = schema.properties || {};
    const top = node(html`<div class="form">${field("sink", select(catalog.sinks.map((x) => [x.name, x.name]), (value && value.sink && value.sink.type) || pick(catalog.sinks, "file"), { name: "dlq-sink" }), { code: true, req: true, help: "Where failed and quarantined rows are written" })}</div>`);
    host.appendChild(top);
    const sinkHost = document.createElement("div");
    host.appendChild(sinkHost);
    const restHost = document.createElement("div");
    host.appendChild(restHost);
    const sel = top.querySelector("select");
    let sinkForm = null;
    const load = async (val) => {
      try { sinkForm = mountForm(sinkHost, await api(`/v1/schemas/sink/${encodeURIComponent(sel.value)}`), val, changed); } catch { sinkForm = null; }
      changed();
    };
    sel.addEventListener("change", () => load());
    load(value && value.sink && value.sink.config);
    const restForm = mountForm(restHost, { ...schema, properties: rest, required: (schema.required || []).filter((r) => r !== "sink") }, value, changed);
    return () => ({ sink: { type: sel.value, config: sinkForm ? sinkForm.read() : {} }, ...restForm.read() });
  }
  function runOpt(n) { const el = detail.querySelector(`[data-ro="${n}"]`); return el ? el.value.trim() : ""; }
  function runOptChecked(n) { const el = detail.querySelector(`[data-ro="${n}"]`); return !!(el && el.checked); }
  function setupOptions() {
    const h = stepHost("Options");
    const rel = node(card({ icon: "shield", title: "Reliability", aside: "what happens on a crash, a bad row, a slow source", body: html`<div class="sb-blocks" style="padding-top:12px"></div>` }));
    const relHost = rel.querySelector(".sb-blocks");
    for (const [nm, hint] of RELIABILITY) if (blockMeta.has(nm)) relHost.appendChild(blockSub(nm, hint));
    const gov = node(card({ icon: "policy", title: "Data governance", aside: "checks and policies applied to every page before it is written", body: html`<div class="sb-govblocks"></div><div class="sb-adders" style="border-top:0">${GOVERNANCE.filter(([nm]) => blockMeta.has(nm)).map(([nm, l]) => btn(l, { icon: "plus", cls: "sm", data: { addblock: nm }, title: blockMeta.get(nm).description }))}</div>` }));
    gov.addEventListener("click", (e) => {
      const a = e.target.closest("[data-addblock]");
      if (!a) return;
      const el = blockSub(a.dataset.addblock, "", {});
      gov.querySelector(".sb-govblocks").appendChild(el);
      if (!gov.querySelector(".sb-govblocks").style.paddingTop) gov.querySelector(".sb-govblocks").style.paddingTop = "12px";
      a.remove();
    });
    const pools = (window.__faucetPools || ["default"]);
    const ro = node(card({ icon: "runs", title: "Run options", body: html`<div class="form three">
      ${field("Run name", input("", { a: { "data-ro": "name" }, ph: "Defaults to the config’s name" }))}
      ${field("Pool", select(pools.map((p) => [p, p]), "default", { a: { "data-ro": "pool" } }))}
      ${field("Timeout", input("", { ph: "e.g. 2h", a: { "data-ro": "timeout" } }), { help: "Cancel the run after this long" })}
      ${field("Labels", html`<div class="sb-labels stack" style="gap:6px"></div><div>${btn("Add label", { icon: "plus", cls: "sm", data: { addlabel: "1" } })}</div>`, { cls: "wide", help: "Searchable on the Runs page" })}
      ${field("Max records", input("", { ph: "No limit", a: { "data-ro": "max_records", inputmode: "numeric" } }), { help: "Budget: the page that would cross is refused" })}
      ${field("Max duration", input("", { ph: "No limit", a: { "data-ro": "max_duration" } }))}
      ${field("Idempotency key", input("", { ph: "Optional", a: { "data-ro": "idem" } }))}
      <div class="wide" style="display:flex;gap:18px;flex-wrap:wrap">${check("Probe connections first (doctor)", false, { a: { "data-ro": "doctor" } })}<span data-perm="change_request">${check("Request approval before it runs", false, { a: { "data-ro": "approval" } })}</span></div>
      <div class="wide sb-reason" hidden>${field("Reason for approvers", html`<textarea class="area sans" data-ro="reason" rows="2" style="height:46px;white-space:normal" placeholder="Why this run is needed; approvers see it"></textarea>`)}</div>
    </div>` }));
    ro.addEventListener("change", (e) => {
      const c = e.target.closest(".check");
      if (c) c.classList.toggle("on", e.target.checked);
      if (e.target.dataset.ro === "approval") ro.querySelector(".sb-reason").hidden = !e.target.checked;
      changed();
    });
    ro.addEventListener("input", changed);
    ro.addEventListener("click", (e) => {
      if (!e.target.closest("[data-addlabel]")) return;
      const r = node(html`<div class="sb-kv">${input("", { mono: true, ph: "key", a: { "data-lk": "1" } })}${input("", { mono: true, ph: "value", a: { "data-lv": "1" } })}${ibtn("x", { title: "Remove" })}</div>`);
      r.querySelector("button").onclick = () => r.remove();
      ro.querySelector(".sb-labels").appendChild(r);
      r.querySelector("input").focus();
    });
    h.append(rel, gov, ro);
  }

  // ---- review ----
  function paintReview() {
    const h = stepHost("Review");
    const d = st.doctor;
    let doc;
    if (!d) {
      doc = alert("Not checked yet.", html`Validate parses the config and probes each source, sink and state store. ${btn("Validate", { icon: "check", cls: "sm", perm: "doctor", data: { act: "validate" } })}`, "");
    } else if (d.error && !d.invocations) {
      doc = alert("Validation failed.", d.error, "bad");
    } else {
      const probes = (d.invocations || []).flatMap((inv) => (inv.probes || []).map((p) => ({ ...p, row: inv.id })));
      const failed = probes.filter((p) => p.status === "fail");
      const tone = (s) => (s === "pass" ? "ok" : s === "fail" ? "bad" : "faint");
      const rows = probes.map((p) => [html`${p.role} <span class="mono">${p.connector || ""}</span> · ${p.name}`, html`<span class="${tone(p.status)}">● ${p.status}</span>`, failed.length ? (p.reason || "") : (p.elapsed_ms != null ? `${p.elapsed_ms} ms` : "")]);
      doc = card({ icon: failed.length ? "x" : "check", title: failed.length ? "Validation failed" : "Validated", tag: "doctor", aside: failed.length ? `${failed.length} of ${probes.length} probes failed` : `${probes.length} probes passed${d.ms != null ? ` in ${(d.ms / 1000).toFixed(1)}s` : ""}`, right: "just now", body: tbl(["Probe", "Result", failed.length ? "Detail" : ["Time", "num"]], rows) });
    }
    let plan;
    if (!st.plan) plan = card({ icon: "diff", title: "Plan", aside: "what this run will do", body: html`<div class="empty-line">Press Plan to see the write mode, delivery guarantee and policy verdict before anything runs.</div>` });
    else if (st.plan.error) plan = alert("Couldn’t plan this config.", st.plan.error, "bad");
    else {
      const p = st.plan;
      const pol = p.policy;
      const viol = pol ? (pol.violations || []).length : 0;
      const key = p.key || (p.sink_config && p.sink_config.key);
      plan = card({ icon: "diff", title: "Plan", aside: "what this run will do", body: grid(4, [
        kv("Row", p.row || "—", (p.transforms || []).length ? `${p.transforms.length} transforms` : "no transforms"),
        kv("Write", p.write_mode || "append", key ? `key ${[].concat(key).join(", ")}` : `${p.source} → ${p.sink}`),
        kv("Delivery", String(p.delivery_guarantee || "").replace(/([a-z])([A-Z])/g, "$1 $2").toLowerCase() || "—"),
        kv("Policy", pol ? (viol ? html`<span class="bad">${viol} violation${viol > 1 ? "s" : ""}</span>` : html`<span class="ok">allowed</span>`) : "no policy", [p.quality && "quality", p.contract && "contract", p.masking && "masking"].filter(Boolean).join(" · ")),
      ]), note: p.impact ? html`Schema impact · ${p.impact.severity || "none"}` : "" });
    }
    h.innerHTML = "";
    h.append(node(html`<div>${doc}</div>`).firstElementChild, node(html`<div>${plan}</div>`).firstElementChild);
  }

  // ---- editor ----
  function setupEditor() {
    const col = $(".sb-edcol");
    col.innerHTML = v(html`${card({ icon: "code", title: html`<span class="sb-edname">pipeline.yaml</span>`, right: select([["yaml", "YAML"], ["json", "JSON"]], "yaml", { cls: "sb-fmt", a: { "aria-label": "Format" } }), body: html`<div class="sb-ed"><pre class="sb-gutter" aria-hidden="true"></pre><textarea class="sb-text" spellcheck="false" aria-label="Config"></textarea></div>`, note: html`<span class="sb-ednote">Write a whole config, or switch from Guided to start from its form.</span>` })}`);
    const ta = col.querySelector("textarea");
    const gut = col.querySelector(".sb-gutter");
    const lines = () => { const n = ta.value.split("\n").length; gut.textContent = Array.from({ length: n }, (_, i) => String(i + 1)).join("\n"); };
    ta.addEventListener("input", () => {
      lines(); st.editorErr = null; paintEdNote(); paintFromRun();
      const m = ta.value.match(/^name:\s*["']?([^"'\n]+)/m);
      col.querySelector(".sb-edname").textContent = `${m ? m[1].trim() : "pipeline"}.${$(".sb-fmt select") ? $(".sb-fmt select").value : "yaml"}`;
    });
    ta.addEventListener("scroll", () => { gut.scrollTop = ta.scrollTop; });
    lines();
    $(".sb-edside").innerHTML = v(html`${card({ title: "Run options", body: html`<div class="form one">${field("Run name", input("", { a: { "data-eo": "name" }, ph: "Defaults to the config’s name" }))}${field("Timeout", input("", { ph: "e.g. 6h", a: { "data-eo": "timeout" } }), { help: "Cancel the run after this long" })}${check("Probe connections before running (doctor first)", false, { a: { "data-eo": "doctor" } })}<span data-perm="change_request">${check("Request approval before it runs", false, { a: { "data-eo": "approval" } })}</span></div>` })}<div class="sb-edstatus"></div><div class="sb-fromrun"></div>`);
    $(".sb-edside").addEventListener("change", (e) => { const c = e.target.closest(".check"); if (c) c.classList.toggle("on", e.target.checked); });
  }
  const edText = () => $(".sb-text").value;
  function setEditor(text) { $(".sb-text").value = text; $(".sb-text").dispatchEvent(new Event("input")); }
  function paintEdNote() {
    const n = $(".sb-ednote");
    const e = st.editorErr;
    if (!e) { n.innerHTML = "Write a whole config, or switch from Guided to start from its form."; n.className = "sb-ednote"; return; }
    const m = String(e).match(/line (\d+)/i);
    n.innerHTML = v(m ? html`<span class="bad">Line ${m[1]}</span> · ${e}` : html`<span class="bad">${e}</span>`);
  }
  function paintEdStatus() {
    const d = st.doctor;
    const h = $(".sb-edstatus");
    if (!d) { h.innerHTML = v(alert("Not checked yet.", "Validate parses the config and probes each source, sink and state store.", "")); return; }
    const probes = (d.invocations || []).flatMap((inv) => inv.probes || []);
    const failed = probes.filter((p) => p.status === "fail");
    h.innerHTML = v(d.error && !d.invocations ? alert("Validation failed.", d.error, "bad") : failed.length ? alert(`${failed.length} of ${probes.length} probes failed.`, failed.map((p) => `${p.role} ${p.connector}: ${p.reason || p.name}`).join(" · "), "bad") : alert("Validated.", `${probes.length} probes passed.`, "ok"));
  }
  function paintFromRun() {
    const h = $(".sb-fromrun");
    if (!st.fromRun || !h) return;
    const d = lineDiff(st.fromRun.body.trimEnd(), edText().trimEnd());
    const n = d.filter(([op]) => op === "+" || op === "-").length;
    h.innerHTML = v(card({ icon: "diff", title: "Changes from the original run", right: n ? `${n} line${n > 1 ? "s" : ""}` : "none", body: n ? diff(d) : html`<div class="empty-line">Unchanged: this resubmits the run as it was.</div>` }));
  }

  // ---- mode ----
  function paintMode() {
    $(".sb-mode").innerHTML = v(modeSeg(st.mode));
    detail.querySelector('[data-pane="guided"]').hidden = st.mode !== "guided";
    detail.querySelector('[data-pane="editor"]').hidden = st.mode !== "editor";
    detail.querySelector(".lede").textContent = st.mode === "editor" ? "Paste or write a full config. Guided keeps its own form; switching to Editor starts from what the form builds." : "Pick a source and a sink; the form comes from each connector’s schema. Validate probes every connection without moving data.";
    if (st.mode === "editor") {
      const fromForm = toYaml(buildConfig());
      if (!edText().trim() || edText() === st.editorSeeded) { setEditor(fromForm); st.editorSeeded = fromForm; }
      paintEdStatus();
    }
  }

  // ---- banner (errors, read-only, prefill) ----
  function paintBanner() {
    const parts = [];
    if (!canRun) parts.push(alert("Read-only access.", "Your role can browse schemas and validate a config, but not submit runs. Ask an operator, or open a change request from an operator account.", ""));
    if (st.fromRun) parts.push(alert(`Prefilled from run ${st.fromRun.name}.`, html`The config stored with ${lnk(shortId(st.fromRun.id), href.run(st.fromRun.id), true)}${st.fromRun.redacted ? " (secret values were redacted)" : ""}. Secrets stay as references. It opens in Editor; the changes you make are shown beside it.`, ""));
    if (st.fromRunMissing) parts.push(alert("That run has no stored config.", html`The server keeps a config body only for some runs. Start from ${lnk("the run", href.run(st.fromRunMissing), false)}’s source and sink by hand.`, "warn"));
    if (st.errors.length) {
      parts.push(alert(`${st.errors.length} field${st.errors.length > 1 ? "s need" : " needs"} attention before this can run.`, join2(st.errors.map((e, i) => html`<a class="lnk" href="#" data-err="${i}">${e.key}</a> ${e.msg.replace(/\.$/, "").toLowerCase()}`)), "bad"));
    }
    if (st.submitErr) parts.push(alert("The server refused this run.", st.submitErr, "bad"));
    $(".sb-banner").innerHTML = v(parts);
    $(".sb-banner").hidden = !parts.length;
  }
  const join2 = (xs) => raw(xs.map((x) => v(x)).join(" · "));

  function collectErrors() {
    const errs = [];
    const tag = (step, list) => list.forEach((e) => errs.push({ ...e, key: step === "Source" || step === "Sink" ? `${step.toLowerCase()}.${e.key}` : e.key, step }));
    if (st.src.form) tag("Source", st.src.form.validate());
    if (st.snk.form) tag("Sink", st.snk.form.validate());
    st.tx.forEach((t) => t.form && tag("Transforms", t.form.validate()));
    const t = runOpt("timeout");
    if (t && Number.isNaN(secs(t))) errs.push({ step: "Options", key: "timeout", msg: "Must be a duration like 90s, 30m or 2h.", el: detail.querySelector('[data-ro="timeout"]') });
    const md = runOpt("max_duration");
    if (md && Number.isNaN(secs(md))) errs.push({ step: "Options", key: "max_duration", msg: "Must be a duration like 30m or 3h.", el: detail.querySelector('[data-ro="max_duration"]') });
    const mr = runOpt("max_records");
    if (mr && !/^\d[\d,_]*$/.test(mr)) errs.push({ step: "Options", key: "max_records", msg: "Must be a whole number.", el: detail.querySelector('[data-ro="max_records"]') });
    return errs;
  }

  function requestBody() {
    const editor = st.mode === "editor";
    const fmt = editor ? $(".sb-fmt select").value : "yaml";
    const body = { config: editor ? edText() : toYaml(buildConfig()), config_format: fmt };
    const o = (n) => (editor ? (detail.querySelector(`[data-eo="${n}"]`) || {}) : (detail.querySelector(`[data-ro="${n}"]`) || {}));
    const name = (o("name").value || "").trim();
    if (name) body.name = name;
    const t = secs(o("timeout").value);
    if (t) body.timeout_secs = t;
    if (o("doctor").checked) body.doctor_first = true;
    if (o("approval").checked) {
      body.require_approval = true;
      const r = runOpt("reason");
      if (r) body.reason = r;
    }
    if (!editor) {
      const pool = runOpt("pool");
      if (pool && pool !== "default") body.pool = pool;
      const idem = runOpt("idem");
      if (idem) body.idempotency_key = idem;
      const labels = {};
      detail.querySelectorAll(".sb-labels .sb-kv").forEach((r) => { const k = r.querySelector("[data-lk]").value.trim(); if (k) labels[k] = r.querySelector("[data-lv]").value.trim(); });
      if (Object.keys(labels).length) body.labels = labels;
      const budget = {};
      const mr = runOpt("max_records");
      if (mr) budget.max_records = Number(mr.replace(/[,_]/g, ""));
      const md = secs(runOpt("max_duration"));
      if (md) budget.max_duration_secs = md;
      if (Object.keys(budget).length) body.budget = budget;
    }
    return body;
  }

  async function runAction(act, b) {
    if (st.mode === "guided") {
      st.errors = collectErrors();
      paintBanner();
      refreshPreview();
      if (st.errors.length) { goStep(st.errors[0].step); st.errors[0].el?.focus(); return; }
    }
    const rb = requestBody();
    b && (b.disabled = true);
    try {
      if (act === "validate") {
        const t0 = performance.now();
        try {
          const rep = await api("/v1/doctor", { method: "POST", body: { config: rb.config, config_format: rb.config_format } });
          st.doctor = { invocations: (rep && rep.invocations) || [], ms: performance.now() - t0 };
        } catch (e) {
          st.doctor = e.details && e.details.invocations ? { invocations: e.details.invocations, ms: performance.now() - t0 } : { error: e.message };
          st.editorErr = e.details && e.details.invocations ? null : e.message;
        }
        if (st.mode === "guided") { paintReview(); goStep("Review"); } else { paintEdStatus(); paintEdNote(); }
      } else if (act === "plan") {
        try { st.plan = await api("/v1/plan", { method: "POST", body: { config: rb.config, config_format: rb.config_format } }); } catch (e) { st.plan = { error: e.message }; st.editorErr = e.message; }
        if (st.mode === "guided") { paintReview(); goStep("Review"); } else { paintEdNote(); toast(st.plan.error ? "Plan failed" : `Plan: ${st.plan.write_mode || "append"} · ${String(st.plan.delivery_guarantee || "").replace(/([a-z])([A-Z])/g, "$1 $2").toLowerCase()}`, st.plan.error ? "bad" : "info"); }
      } else if (act === "submit") {
        st.submitErr = null;
        try {
          const resp = await api("/v1/runs", { method: "POST", body: rb });
          if (resp.status === "pending_approval") {
            toast(html`Change request opened · waits for approval`, "ok", { href: href.change(resp.change_id), label: "open request" });
            navigate(href.change(resp.change_id));
            return;
          }
          toast(html`Run <b>${rb.name || buildConfig().name || shortId(resp.run_id)}</b> ${resp.status || "queued"}`, "ok", { href: href.run(resp.run_id), label: "open run" });
          navigate(href.run(resp.run_id));
        } catch (e) {
          st.submitErr = `${e.message}${e.retryAfter ? ` Retry in ${e.retryAfter}s.` : ""}`;
          if (st.mode === "editor") { st.editorErr = e.message; paintEdNote(); }
          paintBanner();
          detail.scrollTop = 0;
        }
      }
    } finally {
      b && (b.disabled = false);
    }
  }

  function goStep(s) {
    if (s === st.step) return;
    st.step = s;
    paintSteps();
    setQuery({ step: s === "Source" ? "" : s.toLowerCase() }, { replace: false });
  }

  // ---- events ----
  detail.addEventListener("click", (e) => {
    const go = e.target.closest("[data-go]");
    if (go) { goStep(go.dataset.go); return; }
    const m = e.target.closest("[data-mode]");
    if (m && m.dataset.mode !== st.mode) { st.mode = m.dataset.mode; paintMode(); setQuery({ mode: st.mode === "editor" ? "editor" : "" }); return; }
    const a = e.target.closest("[data-act]");
    if (a && ["validate", "plan", "submit"].includes(a.dataset.act)) { e.preventDefault(); runAction(a.dataset.act, a); return; }
    const er = e.target.closest("[data-err]");
    if (er) { e.preventDefault(); const x = st.errors[Number(er.dataset.err)]; if (x) { goStep(x.step); setTimeout(() => x.el?.focus(), 30); } }
  });
  detail.addEventListener("keydown", (e) => {
    const go = e.target.closest("[data-go], [data-mode]");
    if (go && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); go.click(); }
  });
  detail.addEventListener("input", (e) => { if (e.target.dataset.ro === "name") { const n = $(".sb-edname"); if (n) n.textContent = `${slug(e.target.value) || "pipeline"}.yaml`; } });

  // ---- boot ----
  fetch("/readyz").then((r) => r.json()).then((r) => { window.__faucetPools = (r.cluster && r.cluster.pools) || ["default"]; }).catch(() => {});
  setupEditor();
  setupTransforms();
  setupOptions();
  paintMode();
  paintSteps();
  paintReview();
  paintBanner();
  refreshPreview();
  await Promise.all([connectorCard("source", st.src, stepHost("Source")), connectorCard("sink", st.snk, stepHost("Sink"))]);
  paintMode();

  const fromId = q.get("run") || q.get("from");
  if (fromId) {
    try {
      const c = await api(`/v1/runs/${encodeURIComponent(fromId)}/config`);
      let name = fromId;
      try { const r = await api(`/v1/runs/${encodeURIComponent(fromId)}`); name = r.name || r.labels?.pipeline || fromId; } catch { /* name stays the id */ }
      st.fromRun = { id: fromId, name, body: c.body || "", redacted: !!c.redacted };
      st.mode = "editor";
      setEditor(st.fromRun.body);
      st.editorSeeded = "\u0000";
      const fmt = $(".sb-fmt select");
      if (fmt && (c.format === "json" || c.format === "yaml")) fmt.value = c.format;
      setContext(chip(html`from run ${shortId(fromId)}${cpi(fromId)}`, "mono"));
      paintMode();
      paintFromRun();
    } catch (e) {
      st.fromRunMissing = fromId;
    }
    paintBanner();
  }

  return {
    update(p) {
      const s = cap(p.query.get("step"));
      const want = STEPS.includes(s) ? s : "Source";
      if (want !== st.step) { st.step = want; paintSteps(); }
      const mode = p.query.get("mode") === "editor" ? "editor" : "guided";
      if (mode !== st.mode) { st.mode = mode; paintMode(); }
    },
  };
}

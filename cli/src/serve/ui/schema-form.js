// Render a form from a (schemars) JSON Schema and read a value back out.
// Supports: object properties + required, primitives, enum→select, nested
// objects→fieldsets, arrays→add/remove, $ref→$defs, oneOf/anyOf with a const
// "type" discriminator (the {type,config} auth pattern). Anything else falls
// back to a raw-JSON textarea so no config is ever unreachable.

import { escapeHtml, mdInline } from "./utils.js";

function resolveRef(schema, root) {
  if (schema && schema.$ref) {
    const path = schema.$ref.replace(/^#\//, "").split("/");
    let node = root;
    for (const seg of path) node = node?.[seg];
    return node ? { ...node, ...schema, $ref: undefined } : schema;
  }
  return schema;
}

// Build a control for `schema`; returns { el, read() }.
function build(schema, root, value, required) {
  schema = resolveRef(schema, root) || {};
  const desc = schema.description ? `<small class="help">${mdInline(firstSentence(schema.description))}</small>` : "";

  // Discriminated union: oneOf/anyOf whose branches each fix a `type` const.
  const variants = schema.oneOf || schema.anyOf;
  // `Option<T>`: one real branch plus `null`. A nested object is opt-in behind a
  // toggle, so leaving it alone never switches the feature on with its defaults.
  if (Array.isArray(variants)) {
    const real = variants.filter((v) => resolveRef(v, root)?.type !== "null");
    if (real.length === 1 && real.length < variants.length) {
      const inner = { ...resolveRef(real[0], root), description: schema.description };
      const innerType = jsonType(inner, value);
      if (innerType === "object" && inner.properties) return buildOptional(inner, root, value);
      return build(inner, root, value, required);
    }
  }
  // A unit enum with documented variants (schemars emits one `const` per branch).
  if (Array.isArray(variants) && variants.length && variants.every((v) => resolveRef(v, root)?.const !== undefined)) {
    const consts = variants.map((v) => resolveRef(v, root));
    return buildEnum({ ...schema, enum: consts.map((v) => v.const), enumHelp: consts.map((v) => v.description) }, value, desc);
  }
  if (Array.isArray(variants) && variants.every((v) => discriminatorConst(resolveRef(v, root)))) {
    return buildDiscriminated(variants, root, value, desc);
  }

  if (Array.isArray(schema.enum)) return buildEnum(schema, value, desc);

  // An object/array default (e.g. an auth block) cannot go in a text box.
  if (value === undefined && schema.default !== null && typeof schema.default === "object") value = schema.default;
  const type = jsonType(schema, value);
  if (type === "object" && schema.properties) return buildObject(schema, root, value || {});
  if (type === "array") return buildArray(schema, root, value || []);
  if (type === "boolean") return buildBool(schema, value, desc);
  if (type === "integer" || type === "number") return buildNumber(schema, value, type, desc);
  if (type === "string") return buildString(schema, value, desc);
  return buildRaw(value); // fallback
}

function buildObject(schema, root, value) {
  const wrap = document.createElement("fieldset");
  wrap.className = "sf-object";
  const reqset = new Set(schema.required || []);
  const readers = {};
  for (const [key, sub] of Object.entries(schema.properties)) {
    const row = document.createElement("div");
    row.className = "sf-field";
    const label = document.createElement("label");
    label.innerHTML = escapeHtml(key).replace(/_/g, "_<wbr>") + (reqset.has(key) ? " *" : "");
    const ctrl = build(sub, root, value[key], reqset.has(key));
    row.appendChild(label);
    row.appendChild(ctrl.el);
    wrap.appendChild(row);
    readers[key] = ctrl.read;
  }
  return {
    el: wrap,
    read() {
      const out = {};
      for (const [k, r] of Object.entries(readers)) {
        const v = r();
        if (v !== undefined && v !== "" && !(typeof v === "object" && v !== null && Object.keys(v).length === 0))
          out[k] = v;
      }
      return out;
    },
  };
}

function buildOptional(schema, root, value) {
  const wrap = document.createElement("div");
  wrap.className = "sf-optional";
  const toggle = document.createElement("label");
  toggle.className = "sf-optional-toggle";
  const cb = document.createElement("input");
  cb.type = "checkbox";
  cb.checked = value !== undefined && value !== null;
  toggle.append(cb, document.createTextNode(schema.description ? firstSentence(schema.description) : "Enable"));
  const inner = buildObject(schema, root, value || {});
  inner.el.hidden = !cb.checked;
  cb.onchange = () => { inner.el.hidden = !cb.checked; };
  wrap.append(toggle, inner.el);
  return { el: wrap, read: () => (cb.checked ? inner.read() : undefined) };
}

function buildArray(schema, root, value) {
  const wrap = document.createElement("div");
  wrap.className = "sf-array";
  const list = document.createElement("div");
  const readers = [];
  const addItem = (v) => {
    const item = document.createElement("div");
    item.className = "sf-array-item";
    const ctrl = build(schema.items || {}, root, v, false);
    const del = document.createElement("button");
    del.type = "button";
    del.className = "btn-ghost";
    del.textContent = "✕";
    del.onclick = () => {
      const i = readers.indexOf(ctrl.read);
      if (i >= 0) readers.splice(i, 1);
      item.remove();
    };
    item.appendChild(ctrl.el);
    item.appendChild(del);
    list.appendChild(item);
    readers.push(ctrl.read);
  };
  (Array.isArray(value) ? value : []).forEach(addItem);
  const add = document.createElement("button");
  add.type = "button";
  add.className = "btn-ghost";
  add.textContent = "+ add";
  add.onclick = () => addItem(undefined);
  wrap.appendChild(list);
  wrap.appendChild(add);
  return { el: wrap, read: () => readers.map((r) => r()).filter((v) => v !== undefined && v !== "") };
}

function buildDiscriminated(variants, root, value, desc) {
  const wrap = document.createElement("div");
  wrap.className = "sf-union";
  const select = document.createElement("select");
  const bodies = {};
  let current = null;
  const render = (tag) => {
    if (current) current.el.remove();
    const variant = variants.map((v) => resolveRef(v, root)).find((v) => discriminatorConst(v) === tag);
    // strip the const `type` prop; render the rest (usually a `config` object).
    const sub = stripConst(variant);
    const own = value && value.type === tag ? value : {};
    // The `{type, config}` shape: show the config's fields directly rather than
    // nesting them under a lone "config" label.
    const keys = Object.keys(sub.properties);
    const cfg = keys.length === 1 && keys[0] === "config" ? resolveRef(sub.properties.config, root) : null;
    if (cfg && jsonType(cfg) === "object") {
      const inner = cfg.properties && Object.keys(cfg.properties).length ? buildObject(cfg, root, own.config || {}) : null;
      current = inner
        ? { el: inner.el, read: () => { const c = inner.read(); return Object.keys(c).length ? { config: c } : {}; } }
        : { el: document.createElement("span"), read: () => ({}) };
    } else {
      current = build(sub, root, own, false);
    }
    bodies[tag] = current;
    wrap.appendChild(current.el);
  };
  for (const v of variants) {
    const tag = discriminatorConst(resolveRef(v, root));
    const opt = document.createElement("option");
    opt.value = tag;
    opt.textContent = tag;
    select.appendChild(opt);
  }
  select.value = (value && value.type) || select.options[0]?.value;
  select.onchange = () => render(select.value);
  wrap.appendChild(select);
  if (desc) wrap.insertAdjacentHTML("beforeend", desc);
  render(select.value);
  return {
    el: wrap,
    read() {
      const tag = select.value;
      return { type: tag, ...current.read() };
    },
  };
}

function buildEnum(schema, value, desc) {
  const sel = document.createElement("select");
  for (const v of schema.enum) {
    const o = document.createElement("option");
    o.value = String(v);
    o.textContent = String(v);
    const help = schema.enumHelp?.[schema.enum.indexOf(v)];
    if (help) o.title = firstSentence(help);
    sel.appendChild(o);
  }
  if (value !== undefined) sel.value = String(value);
  else if (schema.default !== undefined) sel.value = String(schema.default);
  return withHelp(sel, desc, () => sel.value);
}

function buildBool(schema, value, desc) {
  const cb = document.createElement("input");
  cb.type = "checkbox";
  cb.checked = value !== undefined ? !!value : !!schema.default;
  return withHelp(cb, desc, () => cb.checked);
}

function buildNumber(schema, value, type, desc) {
  const inp = document.createElement("input");
  inp.type = "number";
  if (type === "integer") inp.step = "1";
  if (value !== undefined) inp.value = value;
  else if (schema.default !== undefined) inp.value = schema.default;
  if (schema.description) inp.placeholder = plain(firstSentence(schema.description));
  return withHelp(inp, desc, () => (inp.value === "" ? undefined : Number(inp.value)));
}

function buildString(schema, value, desc) {
  const inp = document.createElement("input");
  inp.type = /password|secret|token|key/i.test(schema.title || "") ? "password" : "text";
  if (value !== undefined) inp.value = value;
  else if (schema.default !== undefined) inp.value = schema.default;
  if (schema.description) inp.placeholder = plain(firstSentence(schema.description));
  return withHelp(inp, desc, () => (inp.value === "" ? undefined : inp.value));
}

function buildRaw(value) {
  const ta = document.createElement("textarea");
  ta.className = "sf-raw";
  ta.placeholder = "raw JSON";
  if (value !== undefined) ta.value = JSON.stringify(value, null, 2);
  return {
    el: ta,
    read() {
      if (!ta.value.trim()) return undefined;
      try { return JSON.parse(ta.value); } catch { return ta.value; }
    },
  };
}

function withHelp(el, desc, read) {
  if (!desc) return { el, read };
  const wrap = document.createElement("div");
  wrap.appendChild(el);
  wrap.insertAdjacentHTML("beforeend", desc);
  return { el: wrap, read };
}

// --- schema introspection helpers ---
function jsonType(schema, value) {
  if (typeof schema.type === "string") return schema.type;
  if (Array.isArray(schema.type)) return schema.type.find((t) => t !== "null") || "string";
  if (schema.properties) return "object";
  if (schema.items) return "array";
  if (schema.enum) return typeof schema.enum[0];
  if (value !== undefined) return Array.isArray(value) ? "array" : typeof value;
  return "string";
}
function discriminatorConst(variant) {
  const p = variant && variant.properties && variant.properties.type;
  if (!p) return null;
  if (p.const !== undefined) return p.const;
  if (Array.isArray(p.enum) && p.enum.length === 1) return p.enum[0];
  return null;
}
function stripConst(variant) {
  const props = { ...(variant.properties || {}) };
  delete props.type;
  return { type: "object", properties: props, required: (variant.required || []).filter((r) => r !== "type") };
}
/** Markdown emphasis/code markers stripped, for attributes that render text as-is. */
function plain(s) {
  return s.replace(/\*\*?|`/g, "");
}
function firstSentence(s) {
  const flat = s
    .replace(/\s+/g, " ")
    .replace(/\[([^\]]+)\]\((?!https?:)[^)]*\)/g, "$1");
  const m = flat.split(/(?<!\b(?:e\.g|i\.e|etc|vs))\.\s/)[0];
  return (m.length > 120 ? m.slice(0, 117) + "…" : m).replace(/\.$/, "");
}
// Public: render a form for `schema`; returns { el, read() } where read() yields
// the config object.
export function renderSchemaForm(schema, value) {
  return build(schema, schema, value, true);
}

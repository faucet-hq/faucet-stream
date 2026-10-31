// Schemas (#998): split pane — every compiled source, sink, transform and state
// store, and the config schema of the one selected (#/schemas/<kind>/<name>).
// This is the reference split-pane page: list (tabs · search · sections · rows)
// beside a detail (dhead · head · stats · cards); `update()` keeps the list.
import { api } from "../api.js";
import { html, raw, v, ic, row, sect, tabs, ibtn, input, empty, skelRows, skelCards, head, stats, card, tbl, pill, code, kv, btn, dhead, lnk, join } from "../components.js";
import { layout, setQuery, patch, errorAlert, hashFor } from "../page.js";
import { setCrumbs } from "../shell.js";
import { fmtInt, mdInline } from "../utils.js";
import { href } from "../links.js";
import { withTenant } from "../tenant.js";

const KINDS = [["source", "Sources", "db"], ["sink", "Sinks", "db"], ["transform", "Transforms", "code"], ["state", "State stores", "key"]];
const TAB_KIND = { Sources: "source", Sinks: "sink", Transforms: "transform", State: "state" };
const STATE_DESC = {
  memory: "In-process only: nothing survives a restart",
  file: "One JSON file per key, fsync and atomic rename",
  redis: "Keys under a namespace",
  postgres: "faucet_state table, upserted per commit",
  sqlite: "One local SQLite file",
};

/** Resolve `$ref`s against the root's `$defs`. */
function deref(root, p) {
  let n = p;
  for (let i = 0; n && n.$ref && i < 8; i++) n = (root.$defs || root.definitions || {})[n.$ref.split("/").pop()] || {};
  return n || {};
}
function typeOf(root, p0) {
  const p = deref(root, p0);
  if (p.enum) return "enum";
  if (p.oneOf || p.anyOf) {
    const alts = (p.oneOf || p.anyOf).map((a) => deref(root, a)).filter((a) => a.type !== "null");
    if (alts.length === 1) return typeOf(root, alts[0]);
    if (alts.every((a) => a.const != null || a.enum)) return "enum";
    return "one of";
  }
  const t = Array.isArray(p.type) ? p.type.filter((x) => x !== "null")[0] : p.type;
  return t || (p.properties ? "object" : "any");
}
const firstSentence = (s) => (s ? String(s).split(/\.\s|\n\n/)[0].replace(/\.$/, "") + "." : "");
const isSecret = (name) => /password|secret|token|connection_url|credentials|api_key|private_key/i.test(name);

function fieldsOf(schema) {
  const props = schema.properties || {};
  const req = new Set(schema.required || []);
  return Object.entries(props).map(([name, p0]) => {
    const p = deref(schema, p0);
    const alts = (p.oneOf || p.anyOf || []).map((a) => deref(schema, a));
    const enumVals = p.enum || alts.flatMap((a) => (a.const != null ? [a.const] : a.enum || []));
    return { name, p, required: req.has(name), type: typeOf(schema, p0), def: p0.default !== undefined ? p0.default : p.default, desc: p0.description || p.description || "", enumVals, secret: isSecret(name) };
  });
}

/** An example config like `faucet init`: required fields, then defaults. */
function exampleYaml(kind, name, fields) {
  const top = kind === "transform" ? `transforms:\n  - type: ${name}\n    config:` : `${kind}:\n  type: ${name}\n  config:`;
  const ind = kind === "transform" ? "      " : "    ";
  const lines = [];
  for (const f of fields.filter((x) => x.required)) {
    const val = f.secret ? `\${env:${f.name.toUpperCase()}}` : f.enumVals.length ? String(f.enumVals[0]) : f.type === "integer" || f.type === "number" ? "0" : f.type === "boolean" ? "false" : f.type === "array" ? "[]" : f.type === "object" || f.type === "one of" ? "{}" : '"…"';
    lines.push(`${ind}${f.name}: ${val}${" ".repeat(Math.max(1, 26 - f.name.length - String(val).length))}# required${f.secret ? ", secret" : ""}`);
  }
  for (const f of fields.filter((x) => !x.required && x.def !== undefined && x.def !== null && typeof x.def !== "object")) lines.push(`${ind}${f.name}: ${f.def}`);
  for (const f of fields.filter((x) => !x.required && (x.def === undefined || x.def === null)).slice(0, 4)) lines.push(`${ind}# ${f.name}:`);
  return `${top}\n${lines.join("\n")}`;
}

export async function renderSchemas(host, params) {
  const L = layout(host, "split");
  let catalog = null;
  let usageCounts = null;
  let state = { kind: params.kind || "", name: params.name || "", tab: params.query.get("tab") || "All", q: params.query.get("q") || "", field: params.query.get("field") || "", ff: "" };
  let schema = null;
  let schemaKey = "";

  const items = () => (catalog ? [
    ...catalog.sources.map((s) => ({ kind: "source", ...s })),
    ...catalog.sinks.map((s) => ({ kind: "sink", ...s })),
    ...catalog.transforms.map((s) => ({ kind: "transform", ...s })),
    ...(catalog.state || []).map((s) => ({ kind: "state", name: s, description: STATE_DESC[s] || "" })),
  ] : []);
  const listHref = () => hashFor(href.schemas(), { tab: state.tab !== "All" ? state.tab : "", q: state.q });

  function renderList() {
    if (!catalog) {
      L.list.innerHTML = v(html`${tabs([["All"], "Sources", "Sinks", "Transforms", "State"], "All")}${input("", { ph: "Search connectors and fields", icon: "search", type: "search", name: "q" })}${skelRows(9)}`);
      return;
    }
    const all = items();
    const q = state.q.toLowerCase();
    const shown = all.filter((i) => (state.tab === "All" || i.kind === TAB_KIND[state.tab]) && (!q || `${i.name} ${i.description}`.toLowerCase().includes(q)));
    const sel = `${state.kind}:${state.name}`;
    const body = shown.length
      ? join(KINDS.map(([kind, label, i]) => {
          const rows = shown.filter((x) => x.kind === kind);
          if (!rows.length) return "";
          return html`${sect(label, rows.length, { id: kind })}<div class="sect-rows rows">${rows.map((x) =>
            row(html`<span class="ri">${ic(i)}</span>`, x.name, "", firstSentence(x.description).replace(/\.$/, ""), {
              mono: true, on: sel === `${kind}:${x.name}`, id: `${kind}:${x.name}`, tip: x.description,
              href: kind === "state" ? "" : hashFor(href.schema(kind, x.name), { tab: state.tab !== "All" ? state.tab : "", q: state.q }),
            }))}</div>`;
        }))
      : empty(`No connector matches “${state.q}”`, html`Check the spelling, or the connector may not be compiled into this server. <a class="lnk" href="${href.schemas()}">Clear the search</a>`, { icon: "search" });
    const top = tabs([["All", all.length], "Sources", "Sinks", "Transforms", "State"], state.tab, ibtn("refresh", { title: "Reload the catalogue", data: { reload: "1" } }));
    if (!L.list.querySelector(".list-body")) {
      L.list.innerHTML = v(html`<div class="list-top" style="display:contents"></div>${input(state.q, { ph: "Search connectors and fields", icon: "search", type: "search", name: "q" })}<div class="list-body" style="display:flex;flex-direction:column;gap:10px"></div>`);
      const qi = L.list.querySelector("input[name=q]");
      qi.addEventListener("input", () => setQuery({ q: qi.value }, { replace: true }));
    }
    patch(L.list.querySelector(".list-top"), top);
    patch(L.list.querySelector(".list-body"), body);
  }
  L.list.addEventListener("click", (e) => {
    const t = e.target.closest("[data-tab]");
    if (t) setQuery({ tab: t.dataset.tab === "All" ? "" : t.dataset.tab });
    if (e.target.closest("[data-reload]")) { catalog = null; load(); }
  });

  function catalogueDetail() {
    const c = catalog;
    const used = usageCounts && usageCounts.length
      ? card({ title: "Most used here", aside: "by recent runs on this server", body: tbl(["Connector", "Kind", ["Runs", "num"]], usageCounts.slice(0, 6).map(([k, n]) => { const [kind, name] = k.split(":"); return [lnk(name, href.schema(kind, name), true), kind, fmtInt(n)]; })) })
      : card({ title: "Most used here", aside: "by recent runs on this server", body: html`<div class="empty-line">No runs yet to count.</div>` });
    return html`${head("Connector schemas", [], "Every source, sink, transform and state store compiled into this server, with the config each one accepts. Pick one to see its fields, an example config and the raw JSON Schema.")}
      ${stats([["Sources", c.sources.length], ["Sinks", c.sinks.length], ["Transforms", c.transforms.length], ["State stores", (c.state || []).length]])}
      <div class="two">${used}${card({ title: "Configuration blocks", aside: "pipeline-level", body: html`<div class="pad" style="display:flex;flex-wrap:wrap;gap:6px">${(c.blocks || []).map((b) => html`<span class="chip mono" title="${b.description}">${b.name}</span>`)}</div>` })}</div>
      <div class="note" style="border:0;padding:0 2px">Showing the <b>${c.sources.length} sources</b> and <b>${c.sinks.length} sinks</b> built into this server. Rebuild with more Cargo features (<code>--features full</code>) to enable the rest.</div>`;
  }

  function connectorDetail() {
    const fields = fieldsOf(schema);
    const ff = state.ff.toLowerCase();
    const shownFields = fields.filter((f) => !ff || `${f.name} ${f.desc}`.toLowerCase().includes(ff));
    const rows = [];
    for (const f of shownFields) {
      const open = state.field === f.name;
      rows.push({ cls: open ? "sel" : "", id: f.name, cells: [html`<span class="sc-name"><span class="mono">${f.name}</span> ${f.required ? html`<span class="req" title="required"></span>` : ""}</span>`, pill(f.type), f.def !== undefined && f.def !== null ? html`<code>${typeof f.def === "object" ? JSON.stringify(f.def) : String(f.def)}</code>` : html`<span class="faint">—</span>`, raw(mdInline(firstSentence(f.desc)))] });
      if (open) {
        const p = f.p;
        const cons = [p.minimum != null && `≥ ${p.minimum}`, p.maximum != null && `≤ ${p.maximum}`, p.minLength != null && `min length ${p.minLength}`, p.pattern && `pattern ${p.pattern}`].filter(Boolean);
        rows.push({ cls: "sc-open sel", span: html`<div class="lede" style="margin:2px 0 8px">${raw(mdInline(f.desc || "No description."))}</div><div class="grid4" style="padding:2px 0 6px">${kv("Required", f.required ? "yes" : "no", f.def !== undefined ? `default ${JSON.stringify(f.def)}` : "no default")}${kv("Type", f.type, f.enumVals.length ? `${f.enumVals.length} values` : "")}${f.enumVals.length ? kv("Values", html`<span class="mono">${f.enumVals.join(", ")}</span>`) : ""}${cons.length ? kv("Constraints", cons.join(" · ")) : ""}${p["x-faucet-sql"] ? kv("Marked as SQL", html`<span class="mono">x-faucet-sql</span>`, "serve triggers refuse a whole sender-chosen value here") : ""}${p["x-faucet-resource"] ? kv("Names a resource", p["x-faucet-resource"], "confined by the server’s reference policy") : ""}</div>` });
      }
    }
    const req = fields.filter((f) => f.required).length;
    const defs = fields.filter((f) => f.def !== undefined).length;
    const secrets = fields.filter((f) => f.secret).length;
    const json = JSON.stringify(schema, null, 2);
    const filter = html`<label class="input" style="height:26px;width:190px">${ic("search")}<input type="search" name="ff" placeholder="Filter fields" value="${state.ff}" autocomplete="off"></label>`;
    const submitHref = state.kind === "source" || state.kind === "sink" ? href.submit({ [state.kind]: state.name }) : href.submit();
    return html`${dhead("", html`${btn("Copy link", { icon: "link", kind: "quiet", data: { copy: location.href } })}${btn("Use in Submit", { icon: "submit", kind: "primary", href: submitHref, perm: "config_submit" })}`, { href: listHref(), label: "Schemas" })}
      ${head(`${state.name} · ${state.kind}`, [pill("Built in", "ok", "check"), schema.title ? html`<span class="mono">${schema.title}</span>` : "", html`<span>${fields.length} fields · ${req} required</span>`], raw(mdInline(firstSentence(schema.description || ""))))}
      ${stats([["Fields", fields.length], ["Required", req], ["With defaults", defs], ["Secret", secrets]])}
      ${card({ title: "Fields", aside: "click a field for its full description", right: filter, body: shownFields.length ? html`<div class="tbl-wrap sc-field">${tbl(["Field", "Type", "Default", "Description"], rows)}</div>` : html`<div class="empty-line">No field matches “${state.ff}”.</div>` })}
      ${card({ icon: "code", title: "Example config", aside: "required fields and defaults, like faucet init", body: code(exampleYaml(state.kind, state.name, fields), { box: false }) })}
      <details class="card fold"><summary class="h">${ic("chev")}<b>Raw JSON Schema</b><span class="muted">draft 2020-12 · ${(json.length / 1024).toFixed(1)} KB</span><span class="r">${btn("Copy", { icon: "copy", cls: "sm", data: { copy: json } })}</span></summary>${code(json, { box: false, lang: "json", copy: false })}</details>`;
  }

  async function renderDetail() {
    L.select(!!state.name);
    if (!catalog) { L.detail.__html = ""; L.detail.innerHTML = v(html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`); return; }
    if (!state.name) {
      setCrumbs(["Build", "Schemas"]);
      patch(L.detail, catalogueDetail());
      return;
    }
    setCrumbs(["Build", { label: "Schemas", href: href.schemas() }, `${state.name} · ${state.kind}`]);
    const key = `${state.kind}/${state.name}`;
    if (schemaKey !== key) {
      schemaKey = key;
      schema = null;
      L.detail.__html = "";
      L.detail.innerHTML = v(html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`);
      try {
        schema = await api(`/v1/schemas/${encodeURIComponent(state.kind)}/${encodeURIComponent(state.name)}`);
      } catch (e) {
        if (schemaKey !== key) return;
        L.detail.innerHTML = v(e.status === 404 ? empty(`No ${state.kind} named “${state.name}”`, "It is not compiled into this server.", { icon: "schema", btns: btn("All schemas", { href: href.schemas() }) }) : errorAlert(`the ${state.name} schema`, e));
        L.detail.querySelector("[data-retry]")?.addEventListener("click", () => { schemaKey = ""; renderDetail(); });
        return;
      }
      if (schemaKey !== key) return;
    }
    patch(L.detail, connectorDetail());
  }

  L.detail.addEventListener("input", (e) => {
    if (e.target.name === "ff") { state.ff = e.target.value; patch(L.detail, connectorDetail()); }
  });
  L.detail.addEventListener("click", (e) => {
    const tr = e.target.closest("tr[data-id]");
    if (tr && !e.target.closest("a, button, input")) setQuery({ field: state.field === tr.dataset.id ? "" : tr.dataset.id }, { replace: true });
  });

  async function load() {
    renderList();
    renderDetail();
    try {
      catalog = await api("/v1/schemas");
    } catch (e) {
      L.list.innerHTML = v(errorAlert("the connector catalogue", e));
      L.list.querySelector("[data-retry]").onclick = load;
      L.detail.innerHTML = "";
      return;
    }
    renderList();
    renderDetail();
    // "Most used here": connector kinds of recent runs (best effort).
    api(`/v1/runs?${withTenant(new URLSearchParams({ limit: "200" }))}`).then((r) => {
      const n = {};
      for (const run of (r && r.runs) || []) {
        const seen = new Set();
        for (const inv of run.invocations || []) {
          const u = inv.usage || {};
          if (u.source_kind) seen.add(`source:${u.source_kind}`);
          if (u.sink_kind) seen.add(`sink:${u.sink_kind}`);
        }
        seen.forEach((k) => (n[k] = (n[k] || 0) + 1));
      }
      usageCounts = Object.entries(n).sort((a, b) => b[1] - a[1]);
      if (!state.name) patch(L.detail, catalogueDetail());
    }).catch(() => {});
  }
  await load();

  return {
    update(p) {
      const next = { kind: p.kind || "", name: p.name || "", tab: p.query.get("tab") || "All", q: p.query.get("q") || "", field: p.query.get("field") || "" };
      const listChanged = next.tab !== state.tab || next.q !== state.q || next.kind !== state.kind || next.name !== state.name;
      const fieldChanged = next.field !== state.field;
      if (next.name !== state.name || next.kind !== state.kind) state.ff = "";
      state = { ...state, ...next };
      if (listChanged) renderList();
      if (listChanged || fieldChanged) renderDetail();
    },
  };
}

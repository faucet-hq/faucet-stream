import { api, toast } from "../api.js";
import { renderSchemaForm } from "../schema-form.js";
import { escapeHtml } from "../utils.js";
import { navigate } from "../router.js";
import { can } from "../access.js";

// Optional blocks, grouped the way people think about them. Blocks this build
// did not compile (the server omits them from /v1/schemas) are skipped.
const BLOCK_GROUPS = [
  { id: "reliability", title: "Reliability", hint: "Optional. Where to resume from, where failed records go, the delivery guarantee, retries, and freshness checks.",
    blocks: ["state", "dlq", "delivery", "resilience", "sla"] },
  { id: "governance", title: "Data governance", hint: "Optional. Checks and policies applied to every page before it is written.",
    blocks: ["quality", "contract", "masking", "schema"] },
];
const BLOCK_LABELS = {
  state: "State", dlq: "Dead-letter queue", delivery: "Delivery", resilience: "Resilience", sla: "SLA",
  quality: "Quality checks", contract: "Contract", masking: "PII masking", schema: "Schema drift",
};

export async function renderSubmit(container) {
  let catalog = { sources: [], sinks: [], transforms: [], state: [], blocks: [] };
  try { catalog = await api("/v1/schemas"); } catch (e) { toast(e.message, "error"); }

  container.innerHTML = `
    <div class="page">
      <div class="page-head"><h1>Submit a run</h1>
        <div class="mode-toggle">
          <button id="mode-guided" class="btn-ghost active">Guided</button>
          <button id="mode-editor" class="btn-ghost">Editor</button>
        </div>
      </div>
      ${can("run_write") ? "" : `<div class="tpl-notice">Your role is read-only: you can build and inspect a config here, but submitting a run needs an operator or admin token.</div>`}
      <div id="guided" class="submit-mode"></div>
      <div id="editor" class="submit-mode" hidden>
        <textarea id="cfg" class="code" spellcheck="false" placeholder="version: 1
pipeline:
  source: { type: rest, config: { ... } }
  sink: { type: jsonl, config: { ... } }"></textarea>
      </div>
      <fieldset class="submit-opts">
        <label>name <input id="o-name" /></label>
        <label>format
          <select id="o-format"><option value="yaml">yaml</option><option value="json">json</option></select>
        </label>
        <label>timeout (s) <input id="o-timeout" type="number" /></label>
        <label><input id="o-doctor" type="checkbox" /> doctor first</label>
        <label>idempotency key <input id="o-idem" /></label>
        <label data-perm="change_request"><input id="o-approval" type="checkbox" /> request approval</label>
        <label data-perm="change_request">reason <input id="o-reason" placeholder="why — shown to approvers" /></label>
      </fieldset>
      <div class="submit-actions">
        <button id="btn-check" class="btn-ghost" data-perm="doctor">Check (doctor)</button>
        <button id="btn-run" class="btn-primary" data-perm="run_write">Run</button>
      </div>
      <pre id="submit-out" class="submit-out" hidden></pre>
    </div>`;

  const guided = container.querySelector("#guided");
  const editor = container.querySelector("#editor");
  const cfgEl = container.querySelector("#cfg");
  const out = container.querySelector("#submit-out");

  // --- guided wizard ---
  let srcForm = null, sinkForm = null;
  const txForms = [];

  function selector(label, options) {
    return `<label>${label}<select>${options.map((o) => `<option value="${escapeHtml(o.name)}">${escapeHtml(o.name)}</option>`).join("")}</select></label>`;
  }

  guided.innerHTML = `
    <div class="wizard-step"><h3>Source</h3>${selector("kind", catalog.sources)}<div class="sf-host" id="src-form"></div></div>
    <div class="wizard-step"><h3>Sink</h3>${selector("kind", catalog.sinks)}<div class="sf-host" id="sink-form"></div></div>
    <div class="wizard-step"><h3>Transforms</h3><div id="tx-list"></div><button class="btn-ghost" id="tx-add">+ add transform</button></div>
    ${BLOCK_GROUPS.map((g) => `<div class="wizard-step" data-group="${g.id}" hidden>
      <h3>${g.title}</h3><p class="wizard-hint">${g.hint}</p>
      <div class="wizard-blocks"></div><div class="wizard-adders"></div></div>`).join("")}`;

  const srcSel = guided.querySelector("#guided .wizard-step:nth-child(1) select") || guided.querySelectorAll("select")[0];
  const sinkSel = guided.querySelectorAll("select")[1];

  async function loadForm(kind, name, host, set) {
    host.innerHTML = `<div class="empty">loading schema…</div>`;
    try {
      const schema = await api(`/v1/schemas/${kind}/${encodeURIComponent(name)}`);
      host.innerHTML = "";
      const form = renderSchemaForm(schema);
      host.appendChild(form.el);
      set(form);
    } catch (e) {
      host.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
    }
  }

  if (catalog.sources.length) {
    srcSel.onchange = () => loadForm("source", srcSel.value, guided.querySelector("#src-form"), (f) => (srcForm = f));
    await loadForm("source", srcSel.value, guided.querySelector("#src-form"), (f) => (srcForm = f));
  }
  if (catalog.sinks.length) {
    sinkSel.onchange = () => loadForm("sink", sinkSel.value, guided.querySelector("#sink-form"), (f) => (sinkForm = f));
    await loadForm("sink", sinkSel.value, guided.querySelector("#sink-form"), (f) => (sinkForm = f));
  }

  guided.querySelector("#tx-add").onclick = async () => {
    const wrap = document.createElement("div");
    wrap.className = "wizard-tx";
    wrap.innerHTML = selector("transform", catalog.transforms);
    const host = document.createElement("div");
    host.className = "sf-host";
    wrap.appendChild(host);
    guided.querySelector("#tx-list").appendChild(wrap);
    const sel = wrap.querySelector("select");
    const entry = { kind: () => sel.value, form: null };
    const reload = () => loadForm("transform", sel.value, host, (f) => (entry.form = f));
    sel.onchange = reload;
    await reload();
    txForms.push(entry);
  };

  // --- optional pipeline blocks (state, DLQ, delivery, quality, …) ---
  const blocks = new Map(); // name → { placement, read }
  const byName = new Map((catalog.blocks || []).map((b) => [b.name, b]));
  for (const g of BLOCK_GROUPS) {
    const step = guided.querySelector(`[data-group="${g.id}"]`);
    const names = g.blocks.filter((n) => byName.has(n));
    if (!names.length) continue;
    step.hidden = false;
    const adders = step.querySelector(".wizard-adders");
    for (const name of names) {
      const meta = byName.get(name);
      const add = document.createElement("button");
      add.type = "button";
      add.className = "btn-ghost";
      add.textContent = `+ ${BLOCK_LABELS[name] || name}`;
      add.title = meta.description;
      add.onclick = () => addBlock(step, add, meta);
      adders.appendChild(add);
    }
  }

  async function addBlock(step, adder, meta) {
    adder.hidden = true;
    const panel = document.createElement("div");
    panel.className = "wizard-block";
    panel.innerHTML = `<div class="wizard-block-head"><div><b>${escapeHtml(BLOCK_LABELS[meta.name] || meta.name)}</b>
      <small>${escapeHtml(meta.description)}</small></div>
      <button type="button" class="btn-ghost wizard-block-remove" title="Remove this block">✕</button></div><div class="sf-host"></div>`;
    step.querySelector(".wizard-blocks").appendChild(panel);
    const host = panel.querySelector(".sf-host");
    panel.querySelector(".wizard-block-remove").onclick = () => {
      blocks.delete(meta.name);
      panel.remove();
      adder.hidden = false;
    };
    host.innerHTML = `<div class="empty">loading schema…</div>`;
    let schema;
    try {
      schema = await api(`/v1/schemas/block/${encodeURIComponent(meta.name)}`);
    } catch (e) {
      host.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
      return;
    }
    host.innerHTML = "";
    const read = meta.name === "dlq" ? dlqForm(host, schema) : mount(host, schema);
    blocks.set(meta.name, { placement: meta.placement, read });
  }

  function mount(host, schema) {
    const form = renderSchemaForm(schema);
    host.appendChild(form.el);
    return form.read;
  }

  // The DLQ's `sink` is a whole connector: pick its kind from the compiled
  // sinks and render that sink's own form, then the DLQ's other settings.
  function dlqForm(host, schema) {
    const { sink: _sink, ...rest } = schema.properties || {};
    const sinkWrap = document.createElement("div");
    sinkWrap.className = "wizard-tx";
    sinkWrap.innerHTML = selector("DLQ sink", catalog.sinks);
    const sinkHost = document.createElement("div");
    sinkHost.className = "sf-host";
    sinkWrap.appendChild(sinkHost);
    host.appendChild(sinkWrap);
    const sel = sinkWrap.querySelector("select");
    const preferred = catalog.sinks.find((k) => k.name === "jsonl");
    if (preferred) sel.value = preferred.name;
    let sinkForm = null;
    const load = () => loadForm("sink", sel.value, sinkHost, (f) => (sinkForm = f));
    sel.onchange = load;
    load();
    const restRead = mount(host, { ...schema, properties: rest, required: (schema.required || []).filter((r) => r !== "sink") });
    return () => ({ sink: { type: sel.value, config: sinkForm ? sinkForm.read() : {} }, ...restRead() });
  }

  // Assemble the canonical config object from the wizard.
  function buildConfig() {
    const cfg = { version: 1, pipeline: {} };
    if (srcForm) cfg.pipeline.source = { type: srcSel.value, config: srcForm.read() };
    if (sinkForm) cfg.pipeline.sink = { type: sinkSel.value, config: sinkForm.read() };
    const tx = txForms
      .filter((t) => t.form)
      .map((t) => ({ type: t.kind(), config: t.form.read() }));
    if (tx.length) cfg.pipeline.transforms = tx;
    for (const [name, b] of blocks) {
      const v = b.read();
      if (v === undefined || v === "" || (typeof v === "object" && v !== null && !Object.keys(v).length)) continue;
      if (b.placement === "pipeline") cfg.pipeline[name] = v;
      else cfg[name] = v;
    }
    const name = container.querySelector("#o-name").value.trim();
    if (name) cfg.name = name;
    return cfg;
  }

  function toYaml(obj) {
    // Minimal, dependency-free YAML emitter for the preview/editor hand-off.
    const dump = (v, ind) => {
      const pad = "  ".repeat(ind);
      if (v === null || v === undefined) return "null";
      if (Array.isArray(v))
        return v.length ? "\n" + v.map((i) => `${pad}- ${dump(i, ind + 1).replace(/^\n/, "")}`).join("\n") : "[]";
      if (typeof v === "object" && !Object.keys(v).length) return "{}";
      if (typeof v === "object")
        return "\n" + Object.entries(v).map(([k, val]) => `${pad}${k}: ${dump(val, ind + 1)}`).join("\n");
      if (typeof v === "string") return /[:#\n]/.test(v) ? JSON.stringify(v) : v;
      return String(v);
    };
    return Object.entries(obj).map(([k, v]) => `${k}: ${dump(v, 1)}`).join("\n").replace(/: \n/g, ":\n") + "\n";
  }

  // --- mode toggle ---
  const guidedBtn = container.querySelector("#mode-guided");
  const editorBtn = container.querySelector("#mode-editor");
  guidedBtn.onclick = () => { guided.hidden = false; editor.hidden = true; guidedBtn.classList.add("active"); editorBtn.classList.remove("active"); };
  editorBtn.onclick = () => {
    // Hand the assembled config to the raw editor for tweaks.
    if (!cfgEl.value.trim()) {
      cfgEl.value = toYaml(buildConfig());
      container.querySelector("#o-format").value = "yaml";
    }
    guided.hidden = true; editor.hidden = false; editorBtn.classList.add("active"); guidedBtn.classList.remove("active");
  };

  // --- request building ---
  function requestBody() {
    const isEditor = !editor.hidden;
    const format = container.querySelector("#o-format").value;
    const config = isEditor ? cfgEl.value : (format === "json" ? JSON.stringify(buildConfig()) : toYaml(buildConfig()));
    const body = { config, config_format: format };
    const name = container.querySelector("#o-name").value.trim();
    const timeout = container.querySelector("#o-timeout").value;
    const idem = container.querySelector("#o-idem").value.trim();
    if (name) body.name = name;
    if (timeout) body.timeout_secs = Number(timeout);
    if (container.querySelector("#o-doctor").checked) body.doctor_first = true;
    if (idem) body.idempotency_key = idem;
    const approval = container.querySelector("#o-approval");
    if (approval && approval.checked) body.require_approval = true;
    const reason = container.querySelector("#o-reason");
    if (reason && reason.value.trim()) body.reason = reason.value.trim();
    return body;
  }

  container.querySelector("#btn-check").onclick = async () => {
    out.hidden = false;
    out.textContent = "running doctor…";
    const b = requestBody();
    try {
      const rep = await api("/v1/doctor", { method: "POST", body: { config: b.config, config_format: b.config_format } });
      out.textContent = "✓ all probes passed\n\n" + JSON.stringify(rep, null, 2);
    } catch (e) {
      out.textContent = `✗ ${e.message}\n\n` + (e.details ? JSON.stringify(e.details, null, 2) : "");
    }
  };

  container.querySelector("#btn-run").onclick = async () => {
    try {
      const resp = await api("/v1/runs", { method: "POST", body: requestBody() });
      // Approval first (#703): the server (or the option) turned the run into
      // a change request — go to it instead of a run.
      if (resp.status === "pending_approval") {
        toast(`change request ${resp.change_id} awaits approval`);
        navigate(`#/changes/${resp.change_id}`);
        return;
      }
      toast(`run ${resp.run_id} ${resp.status}`);
      navigate(`#/runs/${resp.run_id}`);
    } catch (e) {
      out.hidden = false;
      const extra = e.retryAfter ? ` (retry in ${e.retryAfter}s)` : "";
      out.textContent = `✗ ${e.message}${extra}\n\n` + (e.details ? JSON.stringify(e.details, null, 2) : "");
      toast(e.message, "error");
    }
  };
}

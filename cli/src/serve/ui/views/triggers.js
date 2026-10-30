// Triggers (#998): split pane — the event triggers this server watches
// (webhooks, object arrival, queue depth, schedules), their health and the runs
// they started. Reads GET /v1/triggers; an older server reports only each
// trigger's name and health in /readyz, and the page says so.
import { api } from "../api.js";
import { html, v, ic, row, ring, sect, tabs, ibtn, stats, empty, skelRows, skelCards, head, card, grid, kv, tbl, pill, code, btn, dhead, dtabs, lnk, alert, field, input, join } from "../components.js";
import { layout, setQuery, patch, poll, errorAlert, hashFor, ago, dialog, toast, sections } from "../page.js";
import { setCrumbs } from "../shell.js";
import { fmtInt } from "../utils.js";
import { href } from "../links.js";
import { can } from "../access.js";

const TYPES = [["webhook", "Webhooks", "webhook"], ["object_arrival", "Object arrival", "object arrival"], ["queue_depth", "Queue depth", "queue depth"], ["schedule", "Schedules", "schedule"], ["", "Other", "trigger"]];
const typeLabel = (t) => (TYPES.find(([k]) => k === (t || "")) || TYPES[4])[2];
const RUN_LABEL = "faucet.trigger.name";
const TERMINAL_TONE = { completed: "ok", failed: "bad", cancelled: "", running: "info", sharded: "info", queued: "", pending: "" };

/** One trigger, normalised from GET /v1/triggers (or /readyz on older servers). */
function norm(t) {
  return {
    name: t.name,
    type: t.type || t.kind || "",
    enabled: t.enabled !== false,
    healthy: t.healthy !== false,
    methods: t.methods || [],
    cron: t.cron || "",
    timezone: t.timezone || "",
    last_fire: t.last_fire || t.last_fired_at || null,
    fired: t.fired_total ?? null,
    errors: t.errors_total ?? null,
    coalesced: t.coalesced_total ?? null,
    template: t.template || null,
    config: t.config || null,
    tenants: t.tenants || null,
    dedupe: t.dedupe_header || "",
    debounce: t.debounce_secs ?? null,
    error: t.error || t.last_error || (t.consecutive_failures ? `${t.consecutive_failures} failed polls in a row` : ""),
    detail: t,
  };
}
function what(t, legacy = false) {
  const d = t.detail || {};
  if (t.type === "webhook") return [t.methods.length ? t.methods.join(", ") : legacy ? "" : "POST, PUT", t.fired != null ? `${fmtInt(t.fired)} fired` : ""];
  if (t.type === "schedule") return [[t.cron, t.timezone].filter(Boolean).join(" "), Array.isArray(t.tenants) ? `${t.tenants.length} tenants` : t.tenants === "all" ? "all tenants" : ""];
  if (t.type === "object_arrival") return [[d.store_kind || d.store || "", d.prefix || d.bucket || ""].filter(Boolean).join(" "), d.mode ? `${d.mode} mode` : ""];
  if (t.type === "queue_depth") return [[d.queue_kind || "", d.queue || d.key || d.topic || ""].filter(Boolean).join(" "), !t.healthy ? "error backoff" : d.threshold != null ? `threshold ${fmtInt(d.threshold)}` : ""];
  return [t.healthy ? "healthy" : "unhealthy"];
}
const tplParts = (x) => (typeof x === "string" ? x.split("@") : [x.id, x.version]);
const runsTarget = (t) => (t.template ? html`template ${lnk(tplParts(t.template)[0], href.template(tplParts(t.template)[0]))}${tplParts(t.template)[1] ? html` <span class="mono">${tplParts(t.template)[1]}</span>` : ""}` : t.config ? html`config <span class="mono">${typeof t.config === "string" ? t.config : "inline"}</span>` : "");

export async function renderTriggers(host, params) {
  const L = layout(host, "split");
  let triggers = null;      // [] when none, null while loading
  let legacy = false;       // only names + health known (no GET /v1/triggers)
  let loadErr = null;
  let state = { name: params.id || "", tab: params.query.get("tab") || "All", dtab: params.query.get("dtab") || "Overview" };
  let runs = { name: "", list: null, total: null };
  const secs = sections("faucet.triggers.sections");

  async function fetchTriggers() {
    try {
      const r = await api("/v1/triggers");
      legacy = false;
      return (Array.isArray(r) ? r : (r && r.triggers) || []).map(norm);
    } catch (e) {
      if (e.status !== 404) throw e;
      const ready = await fetch("/readyz").then((x) => x.json());
      legacy = true;
      return (ready.triggers || []).map((t) => norm({ name: t.name, type: t.type, healthy: t.healthy, last_fire: t.last_fire, error: t.consecutive_failures ? `${t.consecutive_failures} failed polls in a row` : "" }));
    }
  }

  function listHtml() {
    if (loadErr) return errorAlert("the triggers", loadErr);
    if (!triggers) return html`${tabs([["All"], ["Unhealthy"]], "All")}${skelRows(8)}`;
    const top = tabs([["All", triggers.length], ["Unhealthy", triggers.filter((t) => !t.healthy).length]], state.tab, ibtn("refresh", { title: "Refresh", data: { reload: "1" } }));
    if (!triggers.length) return html`${top}${empty("No triggers on this server", "Triggers start runs when a webhook is called, a file lands, a queue backs up or a schedule ticks.", { icon: "trig" })}`;
    const shown = triggers.filter((t) => state.tab !== "Unhealthy" || !t.healthy);
    const fired = triggers.reduce((n, t) => n + (t.fired || 0), 0);
    const body = shown.length ? join(TYPES.map(([k, label]) => {
      const rows = shown.filter((t) => (TYPES.some(([x]) => x === t.type) ? t.type : "") === k);
      if (!rows.length) return "";
      const closed = secs.isClosed(k || "other");
      return html`${sect(label, rows.length, { id: k || "other", closed })}<div class="sect-rows rows${closed ? " closed" : ""}">${rows.map((t) => {
        const [first, ...rest] = what(t, legacy).filter(Boolean);
        return row(ring(!t.enabled ? "" : t.healthy ? "ok" : "bad", t.enabled ? "" : "hollow"), t.name, t.last_fire ? ago(t.last_fire) : "", html`<b>${typeLabel(t.type)}</b>${first ? html` · ${first}` : ""}${rest.length ? html` · ${rest.join(" · ")}` : ""}${!t.enabled ? " · disabled" : ""}`, { on: t.name === state.name, id: t.name, href: hashFor(href.trigger(t.name), { tab: state.tab !== "All" ? state.tab : "" }) });
      })}</div>`;
    })) : empty("Every trigger is healthy", "", { icon: "check" });
    return html`${top}${stats([["Triggers", triggers.length], ["Healthy", triggers.filter((t) => t.healthy).length, "ok"], ["Unhealthy", triggers.filter((t) => !t.healthy).length, triggers.some((t) => !t.healthy) ? "bad" : ""], ["Fired", legacy ? "—" : fmtInt(fired)]])}${body}`;
  }

  const curl = (t) => {
    const hdr = t.dedupe ? `\n  -H "${t.dedupe}: <unique id>" \\` : "";
    return `curl -X ${t.methods[0] || "POST"} ${location.origin}/v1/triggers/${encodeURIComponent(t.name)} \\\n  -H "Authorization: Bearer $FAUCET_TOKEN" \\${hdr}\n  -H "Content-Type: application/json" \\\n  -d '{}'`;
  };

  function runsTable(list, limit) {
    if (!list) return html`<div class="pad">${skelRows(3)}</div>`;
    if (!list.length) return html`<div class="empty-line">No runs from this trigger yet.</div>`;
    return tbl(["Run", "Status", ["Rows", "num"], "Started"], list.slice(0, limit).map((r) => ({ href: href.run(r.run_id), cells: [lnk(r.name || r.run_id, href.run(r.run_id)), html`<span class="${TERMINAL_TONE[r.status] ?? ""}">● ${r.status}</span>`, r.records_written != null ? fmtInt(r.records_written) : "—", ago(r.submitted_at, " ago")] })));
  }

  function emptyDetail() {
    return html`${head("Start runs from events", [], html`This server was started without a triggers file. Add one and restart <code>faucet serve</code> with <code>--triggers</code>.`)}
      ${card({ icon: "code", title: "triggers.yaml", aside: "one webhook, one schedule", body: code(`version: 1
triggers:
  - name: orders-ingest
    type: webhook
    methods: [POST]
    dedupe_header: X-Delivery-Id
    config: pipelines/orders.yaml
  - name: nightly-rollup
    type: schedule
    cron: "0 2 * * *"
    timezone: Europe/Berlin
    template: { id: acme/billing-api }`, { box: false }) })}
      ${card({ title: "Then", body: code("faucet serve --triggers triggers.yaml", { box: false, lang: "text" }), note: html`Read the <a class="lnk" href="https://faucet-hq.github.io/faucet-stream/reference/triggers.html" target="_blank" rel="noopener">triggers reference</a> for object arrival and queue depth.` })}`;
  }

  function detailHtml() {
    if (loadErr) return "";
    if (!triggers) return html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`;
    if (!state.name) {
      if (!triggers.length) return emptyDetail();
      return html`${head("Triggers", [], "Pick a trigger to see what starts it, its health and the runs it started.")}${legacy ? alert("This server reports only each trigger’s name, type and health.", "Upgrade the server for methods, schedules and fire counts.", "") : ""}`;
    }
    const t = triggers.find((x) => x.name === state.name);
    const back = { href: hashFor(href.triggers(), { tab: state.tab !== "All" ? state.tab : "" }), label: "Triggers" };
    if (!t) return html`${dhead("", "", back)}${empty(`No trigger named “${state.name}”`, "It is not in this server’s triggers file.", { icon: "trig", btns: btn("All triggers", { href: href.triggers() }) })}`;
    const webhook = t.type === "webhook";
    const actions = webhook
      ? html`${btn("Copy curl", { icon: "copy", data: { copy: curl(t) } })}${btn("Fire", { icon: "play", kind: "primary", perm: "trigger_fire", data: { act: "fire" } })}`
      : btn("Copy name", { icon: "copy", data: { copy: t.name } });
    const tabsHtml = dtabs(["Overview", ["Runs", runs.name === t.name && runs.total != null ? runs.total : ""]], state.dtab, { hrefFor: (val) => hashFor(href.trigger(t.name), { tab: state.tab !== "All" ? state.tab : "", dtab: val === "Overview" ? "" : val }) });
    const sub = [
      t.healthy ? pill("Healthy", "ok", "check") : pill("Unhealthy", "bad", "alert"),
      !t.enabled ? pill("Disabled") : "",
      html`<span>${typeLabel(t.type)}</span>`,
      runsTarget(t) ? html`<span>runs ${runsTarget(t)}</span>` : "",
      t.last_fire ? html`<span>last fired ${ago(t.last_fire, " ago")}</span>` : legacy ? "" : html`<span>never fired</span>`,
    ];
    const allRuns = lnk(`all${runs.total != null ? ` ${fmtInt(runs.total)}` : ""} in Runs`, href.runs({ label: `${RUN_LABEL}:${t.name}` }));
    if (state.dtab === "Runs") {
      return html`${dhead(tabsHtml, actions, back)}${head(t.name, sub)}${card({ icon: "runs", title: "Runs started by this trigger", right: allRuns, body: runsTable(runs.name === t.name ? runs.list : null, 50) })}`;
    }
    const d = t.detail;
    const n = (x) => (x == null ? "—" : fmtInt(x));
    let facts;
    let title;
    let aside;
    let note = "";
    if (webhook) {
      title = "Webhook"; aside = legacy ? "fires when its URL is called" : `fires on ${t.methods.length ? t.methods.join(" or ") : "POST or PUT"} to its URL`;
      const nr = "not reported by this server";
      facts = legacy ? [kv("Methods", "—", nr), kv("Dedupe header", "—", nr), kv("Debounce", "—", nr),
      ] : [kv("Methods", t.methods.length ? t.methods.join(", ") : "POST, PUT"), ...(t.dedupe ? [kv("Dedupe header", html`<span class="mono">${t.dedupe}</span>`, "same value → same run")] : []), kv("Debounce", t.debounce ? `${t.debounce}s` : "none", t.debounce ? "leading edge" : "")];
      if (!legacy) facts.push(kv("Errors", fmtInt(t.errors ?? 0), "since the server started"));
      facts.push( kv("Fired", n(t.fired), legacy ? "" : "since the server started"));
      note = html`Runs are labelled <code>${RUN_LABEL}=${t.name}</code> and audited as <b>trigger:${t.name}</b>. The request body is available to the config as <code>\${trigger.body}</code>.`;
    } else if (t.type === "schedule") {
      title = "Schedule"; aside = "ticks on a cron in the server";
      facts = [kv("Cron", html`<span class="mono">${t.cron || "—"}</span>`), kv("Timezone", t.timezone || "UTC"), kv("Tenants", t.tenants === "all" ? "all" : Array.isArray(t.tenants) ? fmtInt(t.tenants.length) : "—"), kv("Fired", n(t.fired), t.errors != null ? `${fmtInt(t.errors)} errors` : "")];
    } else if (t.type === "object_arrival") {
      title = "Object arrival"; aside = "fires when new objects land under a prefix";
      facts = [kv("Polls every", d.poll_interval_secs ? `${d.poll_interval_secs}s` : "—"), kv("Errors", n(t.errors)), kv("Health", t.healthy ? "watching" : "in backoff"), kv("Fired", n(t.fired), t.errors != null ? `${fmtInt(t.errors)} errors` : "")];
    } else if (t.type === "queue_depth") {
      title = "Queue depth"; aside = "fires once each time the depth rises past the threshold";
      facts = [kv("Polls every", d.poll_interval_secs ? `${d.poll_interval_secs}s` : "—"), kv("Threshold", d.threshold != null ? `${fmtInt(d.threshold)} entries` : "—"), kv("Fired", n(t.fired)), kv("Errors", n(t.errors), "")];
    } else {
      title = "Trigger"; aside = legacy ? "this server reports name, type and health only" : "";
      facts = [kv("Health", t.healthy ? html`<span class="ok">healthy</span>` : html`<span class="bad">unhealthy</span>`), kv("Fired", n(t.fired))];
    }
    if (t.error) note = html`<span class="mono">${t.error}</span>`;
    return html`${dhead(tabsHtml, actions, back)}${head(t.name, sub)}
      ${!t.healthy ? alert("The watcher is in error backoff.", t.error ? html`Its last error is below. It keeps retrying; runs already queued are unaffected.` : "It keeps retrying; runs already queued are unaffected.", "bad") : ""}
      ${card({ icon: "trig", title, aside, right: legacy ? "" : "from the triggers file", body: grid(4, facts), note })}
      ${webhook ? card({ icon: "code", title: "Fire it from anywhere", aside: "needs a token with trigger_fire", body: code(curl(t), { box: false, lang: "text" }) }) : ""}
      ${card({ icon: "runs", title: "Runs started by this trigger", right: allRuns, body: runsTable(runs.name === t.name ? runs.list : null, 5) })}`;
  }

  async function loadRuns() {
    if (!state.name) return;
    const name = state.name;
    try {
      const r = await api(`/v1/runs?${new URLSearchParams({ label: `${RUN_LABEL}:${name}`, limit: "50", counts: "true" })}`);
      if (state.name !== name) return;
      const total = r.counts ? Object.values(r.counts).reduce((a, b) => a + b, 0) : (r.runs || []).length;
      runs = { name, list: r.runs || [], total };
    } catch {
      runs = { name, list: [], total: null };
    }
    paint();
  }

  function paint() {
    L.select(!!state.name);
    patch(L.list, listHtml());
    patch(L.detail, detailHtml());
    setCrumbs(state.name ? ["Operate", { label: "Triggers", href: href.triggers() }, state.name] : ["Operate", "Triggers"]);
  }

  async function load() {
    try {
      triggers = await fetchTriggers();
      loadErr = null;
    } catch (e) {
      loadErr = e;
    }
    paint();
  }

  async function fire(t) {
    const r = await dialog(`Fire ${t.name}`,
      html`<span>Starts one run${runsTarget(t) ? html` of ${runsTarget(t)}` : ""} now, exactly as a caller’s ${t.methods[0] || "POST"} would.</span>
        ${field("Request body", html`<textarea class="area mono" name="body" rows="5" spellcheck="false" placeholder="{}" style="height:110px"></textarea>`, { help: html`Optional JSON, available to the config as <code>\${trigger.body}</code>` })}
        ${t.dedupe ? field("Dedupe key", input(`console-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}`, { mono: true, name: "dedupe" }), { help: html`Sent as ${t.dedupe}; firing again with the same key does nothing` }) : ""}
        <div class="help bad" data-err hidden></div>`,
      html`${btn("Cancel", { kind: "quiet", data: { act: "cancel" } })}${btn("Fire", { icon: "play", kind: "primary", data: { act: "fire", keep: "1" } })}`,
      { icon: "trig", onOpen: (el) => {
        el.querySelector('[data-act="fire"]').addEventListener("click", async (e) => {
          const b = e.currentTarget;
          const err = el.querySelector("[data-err]");
          const raw = el.querySelector("[name=body]").value.trim();
          let body;
          try { body = raw ? JSON.parse(raw) : {}; } catch (x) { err.hidden = false; err.textContent = `The body is not valid JSON: ${x.message}`; return; }
          const headers = {};
          const dk = el.querySelector("[name=dedupe]");
          if (t.dedupe && dk && dk.value.trim()) headers[t.dedupe] = dk.value.trim();
          b.disabled = true;
          try {
            const res = await api(`/v1/triggers/${encodeURIComponent(t.name)}`, { method: t.methods.includes("POST") || !t.methods.length ? "POST" : t.methods[0], body, headers });
            el.querySelector('[data-act="cancel"]').click();
            if (res && res.run_id) toast(html`Fired <b>${t.name}</b> · run queued`, "ok", { href: href.run(res.run_id), label: "open run" });
            else toast(html`Fired <b>${t.name}</b> · coalesced with a run already started for this key`, "info");
            loadRuns();
            load();
          } catch (x) {
            b.disabled = false;
            err.hidden = false;
            err.textContent = x.message;
          }
        });
      } });
    return r;
  }

  L.list.addEventListener("click", (e) => {
    const tb = e.target.closest("[data-tab]");
    if (tb) setQuery({ tab: tb.dataset.tab === "All" ? "" : tb.dataset.tab });
    if (e.target.closest("[data-reload]")) load();
  });
  L.list.addEventListener("sect-toggle", (e) => secs.toggle(e.detail.id));
  L.detail.addEventListener("click", (e) => {
    const f = e.target.closest('[data-act="fire"]');
    if (f) { const t = triggers && triggers.find((x) => x.name === state.name); if (t && can("trigger_fire")) fire(t); }
  });

  paint();
  await load();
  loadRuns();
  const stop = poll(async () => { triggers = await fetchTriggers(); paint(); }, 15000);
  return {
    teardown: stop,
    update(p) {
      const next = { name: p.id || "", tab: p.query.get("tab") || "All", dtab: p.query.get("dtab") || "Overview" };
      const changed = next.name !== state.name;
      state = next;
      paint();
      if (changed) { runs = { name: "", list: null, total: null }; loadRuns(); }
    },
  };
}

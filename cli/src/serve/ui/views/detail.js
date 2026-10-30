// One run's detail pane (#998), mounted beside the run list: overview (output,
// row accounting, invocations, provenance, live log tail), the full
// invocations table, the log stream over SSE, and the DLQ tools.
import { api, toast } from "../api.js";
import { streamLogs } from "../sse.js";
import { formatTs, formatTsSplit } from "../tz.js";
import { escapeHtml, fmtInt, fmtAge, labelChip, sortedLabels, TERMINAL_RUN_STATUSES as TERMINAL } from "../utils.js";
import { icon, tabs, pill, card, kvGrid } from "../components.js";
import { runTitle, runKinds } from "./runs.js";
import { setCrumbTitle } from "../shell.js";

/** Human duration between two RFC3339 timestamps; "—" if either is missing. */
function fmtDur(fromISO, toISO) {
  if (!fromISO || !toISO) return "—";
  const ms = Date.parse(toISO) - Date.parse(fromISO);
  if (!(ms >= 0)) return "—";
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

/** Human duration from a millisecond count (per-row invocation timing, #645). */
function fmtMs(ms) {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms} ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)} s`;
  const m = Math.floor(s / 60);
  return `${m}m ${Math.round(s % 60)}s`;
}

const STATUS_PILL = {
  running: ["info", "play", "Running"],
  sharded: ["info", "play", "Sharded"],
  queued: ["warn", "", "Queued"],
  pending: ["warn", "", "Pending"],
  completed: ["ok", "check", "Completed"],
  failed: ["bad", "x", "Failed"],
  cancelled: ["", "", "Cancelled"],
};

const LOG_EXPORT_TONE = { exported: "ok", pending: "warn", failed: "bad", partially_dropped: "warn" };

/** The log-export card (#806): did the run's logs reach the log service? */
function logExportCard(le) {
  if (!le || (le.status === "not_configured" && !le.link)) return "";
  const items = [];
  if (le.status !== "not_configured") {
    items.push(`<div class="c-kv"><small>Pending</small><b>${fmtInt(le.pending_lines ?? 0)}</b><div class="c-kv-s">lines buffered locally</div></div>`);
    if (le.dropped_lines) items.push(`<div class="c-kv"><small>Dropped</small><b class="le-bad">${fmtInt(le.dropped_lines)}</b><div class="c-kv-s">before delivery</div></div>`);
    if (le.last_error) items.push(`<div class="c-kv c-kv-wide"><small>Last error</small><b class="le-error" title="${escapeHtml(le.last_error)}">${escapeHtml(le.last_error)}</b></div>`);
  }
  if (le.link) items.push(`<div class="c-kv"><small>Log service</small><b><a class="le-link" href="${escapeHtml(le.link)}" target="_blank" rel="noopener noreferrer">View logs ↗</a></b></div>`);
  const head = `<div class="c-card-h">${icon("logs")}<b>Log export</b><span class="le-status">${pill(le.status.replace("_", " "), LOG_EXPORT_TONE[le.status] || "")}</span></div>`;
  return `<section class="c-card">${head}${items.length ? `<div class="c-kvgrid" style="--c-cols:4">${items.join("")}</div>` : ""}</section>`;
}

/** Link to the template a run was triggered from, with the numeric version it
 *  resolved to (both carried in the run's labels). Empty for other runs. */
function templateLink(rec) {
  const l = rec.labels || {};
  if (!l.template) return "";
  const ver = l.template_version ? ` <span class="mono">v${escapeHtml(String(l.template_version))}</span>` : "";
  return `<a href="#/templates/${encodeURIComponent(l.template)}">${escapeHtml(l.template)}</a>${ver}`;
}

/** Render a rollback report: what changed (or would), and why it was blocked. */
function renderRollback(el, r) {
  const verb = r.dry_run ? "would " : "";
  const lines = [];
  if (r.mode === "append") lines.push(`${verb}delete <b>${fmtInt(r.deleted)}</b> appended row(s)`);
  if (r.mode === "upsert") lines.push(`${verb}delete <b>${fmtInt(r.deleted)}</b> created key(s), ${verb}restore <b>${fmtInt(r.restored)}</b> before-image(s)`);
  if (r.mode === "overwrite") lines.push(`${verb}swap back the kept previous table (<b>${fmtInt(r.restored)}</b> row(s))`);
  if (r.conflicts > 0) lines.push(`<b>${fmtInt(r.conflicts)}</b> key(s) were changed by a later run${r.applied ? " (restored anyway: force)" : ""}`);
  if (r.note) lines.push(escapeHtml(r.note));
  if (r.applied && !r.dry_run) lines.push(`bookmark ${r.bookmark_rewound ? "rewound" : "unchanged"}; exactly-once watermark ${r.token_rewound ? "rewound" : "not applicable"}`);
  const status = r.dry_run ? "dry-run" : r.applied ? "rolled back" : "blocked";
  const tone = r.dry_run ? "warn" : r.applied ? "ok" : "bad";
  el.innerHTML = `<div class="rollback-result">${pill(status, tone)} run <span class="mono">${escapeHtml(r.run_id)}</span> on <b>${escapeHtml(r.row)}</b> (${escapeHtml(r.sink_kind)} ${escapeHtml(r.dataset)}, ${escapeHtml(r.mode)})<ul>${lines.map((l) => `<li>${l}</li>`).join("")}</ul></div>`;
}

// Location-driven DLQ panel: inspect / replay / discard envelopes at a
// server-local path. The DLQ is not run-scoped, so the location is entered
// explicitly. inspect → DlqRead (viewer); replay/discard → DlqManage (operator).
function wireDlqPanel(container) {
  const $ = (id) => container.querySelector(id);
  const resultEl = $("#dlq-result");
  const loc = () => $("#dlq-location").value.trim();
  const reason = () => $("#dlq-reason").value || undefined;

  function requireLocation() {
    if (loc()) return true;
    toast("enter a DLQ location", "error");
    return false;
  }

  $("#dlq-inspect").onclick = async () => {
    if (!requireLocation()) return;
    try {
      const s = await api("/v1/dlq/inspect", { method: "POST", body: { location: loc(), reason: reason(), limit: 5 } });
      renderInspect(resultEl, s);
    } catch (e) {
      resultEl.innerHTML = `<div class="error-box">${escapeHtml(e.message)}</div>`;
    }
  };

  $("#dlq-discard").onclick = async () => {
    if (!requireLocation()) return;
    const del = $("#dlq-delete").checked;
    if (!confirm(`${del ? "Delete" : "Archive"} matching envelopes at ${loc()}?`)) return;
    try {
      const o = await api("/v1/dlq/discard", { method: "POST", body: { location: loc(), reason: reason(), delete: del } });
      toast(`discarded ${o.discarded} envelope(s) across ${o.files_rewritten} file(s)`);
      resultEl.innerHTML = `<pre class="dlq-json">${escapeHtml(JSON.stringify(o, null, 2))}</pre>`;
    } catch (e) {
      toast(e.message, "error");
    }
  };

  $("#dlq-replay").onclick = async () => {
    if (!requireLocation()) return;
    const config = $("#dlq-config").value.trim();
    if (!config) { toast("paste a config to replay through", "error"); return; }
    const dry = $("#dlq-dryrun").checked;
    try {
      const o = await api("/v1/dlq/replay", {
        method: "POST",
        body: { config, config_format: "yaml", from: loc(), reason: reason(), dry_run: dry },
      });
      toast(dry ? `dry-run: ${o.candidates} candidate(s)` : `replayed ${o.records_written} record(s)`);
      resultEl.innerHTML = `<pre class="dlq-json">${escapeHtml(JSON.stringify(o, null, 2))}</pre>`;
    } catch (e) {
      toast(e.message, "error");
    }
  };
}

function renderInspect(el, s) {
  const rows = (obj) =>
    Object.entries(obj || {})
      .map(([k, v]) => `<tr><td>${escapeHtml(k)}</td><td>${escapeHtml(String(v))}</td></tr>`)
      .join("") || `<tr><td colspan="2">—</td></tr>`;
  const sample = (s.sample || [])
    .map(
      (e) =>
        `<li><span class="pill">${escapeHtml(e.reason || "?")}</span> ${escapeHtml(e.error_kind || "")}: ${escapeHtml(e.error_message || "")}<pre class="dlq-json">${escapeHtml(JSON.stringify(e.payload))}</pre></li>`,
    )
    .join("");
  el.innerHTML = `
    <div class="dlq-summary">
      <div>${fmtInt(s.total_envelopes)} envelope(s) · ${fmtInt(s.files_read)} file(s) · ${fmtInt(s.malformed)} malformed · ${fmtInt(s.non_envelope)} non-envelope</div>
      <div class="dlq-tables">
        <table class="tbl"><thead><tr><th>reason</th><th>count</th></tr></thead><tbody>${rows(s.by_reason)}</tbody></table>
        <table class="tbl"><thead><tr><th>error kind</th><th>count</th></tr></thead><tbody>${rows(s.by_error_kind)}</tbody></table>
      </div>
      <ul class="dlq-sample">${sample}</ul>
    </div>`;
}

const TAB_LIST = [
  { id: "overview", label: "Overview" },
  { id: "invocations", label: "Invocations" },
  { id: "logs", label: "Logs" },
  { id: "dlq", label: "Dead-letter queue" },
];

// One remembered tab across runs, so stepping through the list stays on Logs.
let lastTab = "overview";

/** Mount run `id` into `el`. Hooks: `backHref` (narrow back link),
 *  `onLabel(filter)`, `onChange()` (the run changed: refresh the list),
 *  `onDeleted()`. Returns a stop function. */
export function mountRunDetail(el, id, hooks = {}) {
  let pollTimer = null;
  let logCtrl = null;
  let stopped = false;
  let rec = null;
  const logLines = [];
  let logState = "connecting";

  el.innerHTML = `
    <div class="c-dhead">
      <a class="sp-back" href="${escapeHtml(hooks.backHref || "#/runs")}">${icon("back")}Runs</a>
      ${tabs(TAB_LIST, lastTab)}
      <span class="c-dhead-r">
        <button class="c-btn danger" id="cancel" data-perm="run_write" hidden>${icon("x")}Cancel run</button>
        <button class="c-btn" id="rollback-toggle" data-perm="rollback" hidden>${icon("undo")}Roll back…</button>
        <button class="c-btn danger" id="delete" data-perm="run_write" hidden>${icon("trash")}Delete</button>
      </span>
    </div>
    <div id="d-title"><div class="sp-loading">loading…</div></div>
    <div class="d-pane" data-pane="overview">
      <div id="d-alert"></div>
      <section class="c-card rollback-card" id="rollback-panel" data-perm="rollback" hidden>
        <div class="c-card-h">${icon("undo")}<b>Roll back this run</b><span class="c-tag">rollback:</span></div>
        <div class="rollback-body">
          <p class="dlq-hint">
            Undo what one invocation wrote — delete the rows it appended, restore the
            before-images of the keys it upserted, or swap back the table it overwrote —
            and rewind the row's bookmark so the next run re-reads it. The run must have
            been made with a <code>rollback:</code> block. A key a later run changed since
            is a conflict and blocks the rollback unless <em>force</em> is set.
          </p>
          <div class="dlq-row">
            <label class="dlq-check">invocation
              <select id="rollback-invocation"></select>
            </label>
            <label class="dlq-check"><input type="checkbox" id="rollback-dryrun" checked /> dry-run</label>
            <label class="dlq-check"><input type="checkbox" id="rollback-force" /> force (restore keys a later run changed)</label>
            <button class="btn-danger" id="rollback-go">Roll back</button>
          </div>
          <details class="dlq-replay" id="rollback-config-details">
            <summary>Config (only needed when the server did not store this run's config)</summary>
            <textarea id="rollback-config" rows="6" placeholder="paste the pipeline config (YAML) the run was made with"></textarea>
          </details>
          <div id="rollback-result"></div>
        </div>
      </section>
      <div id="d-over" class="d-stack"></div>
      <section class="c-card" id="d-tail">
        <div class="c-card-h">${icon("logs")}<b id="d-tail-title">Live log</b><span class="c-card-r" id="d-tail-state"></span></div>
        <div class="c-log d-tail-lines" id="d-tail-lines"></div>
      </section>
    </div>
    <div class="d-pane" data-pane="invocations" hidden><div id="invocations"></div></div>
    <div class="d-pane" data-pane="logs" hidden>
      <div class="logs-head"><span class="c-card-r" id="d-log-state"></span><a id="logs-link" class="le-link" target="_blank" rel="noopener noreferrer" hidden>View in the log service ↗</a></div>
      <pre id="logs" class="logs"></pre>
    </div>
    <div class="d-pane" data-pane="dlq" hidden>
      <section class="c-card">
        <div class="c-card-h"><b>Dead-letter queue</b><span class="c-card-aside">not tied to this run</span></div>
        <div class="dlq-panel">
          <p class="dlq-hint">
            Inspect, replay, or discard DLQ envelopes at a server-local location
            (a <code>.jsonl</code> file, a directory of <code>*.jsonl</code>, or a glob).
          </p>
          <div class="dlq-row">
            <input id="dlq-location" type="text" placeholder="./dlq/dead-letters.jsonl" class="dlq-input" />
            <select id="dlq-reason">
              <option value="">any reason</option>
              <option value="partial">partial</option>
              <option value="dlq_all">dlq_all</option>
              <option value="quality">quality</option>
              <option value="schema_drift">schema_drift</option>
              <option value="contract">contract</option>
            </select>
            <button class="btn-ghost" id="dlq-inspect">Inspect</button>
            <button class="btn-warn" id="dlq-discard" data-perm="dlq_manage">Discard</button>
            <label class="dlq-check" data-perm="dlq_manage"><input type="checkbox" id="dlq-delete" /> delete (no archive)</label>
          </div>
          <div id="dlq-result"></div>
          <details class="dlq-replay" data-perm="dlq_manage">
            <summary>Replay through a config</summary>
            <textarea id="dlq-config" rows="6" placeholder="paste the pipeline config (YAML) whose sink/transforms/quality/contract to replay through"></textarea>
            <div class="dlq-row">
              <label class="dlq-check"><input type="checkbox" id="dlq-dryrun" checked /> dry-run</label>
              <button class="btn-ghost" id="dlq-replay">Replay</button>
            </div>
          </details>
        </div>
      </section>
    </div>`;
  el.scrollTop = 0;
  const $ = (sel) => el.querySelector(sel);
  wireDlqPanel(el);

  function showTab(t) {
    lastTab = t;
    el.querySelectorAll(".c-dhead .c-tab").forEach((b) => {
      const on = b.dataset.tab === t;
      b.classList.toggle("on", on);
      b.setAttribute("aria-selected", String(on));
    });
    el.querySelectorAll(".d-pane").forEach((p) => (p.hidden = p.dataset.pane !== t));
    if (t === "logs") {
      const logsEl = $("#logs");
      logsEl.scrollTop = logsEl.scrollHeight;
    }
  }
  el.querySelectorAll(".c-dhead .c-tab").forEach((b) => (b.onclick = () => showTab(b.dataset.tab)));
  showTab(lastTab);

  el.addEventListener("click", (e) => {
    const chip = e.target.closest(".label-chip");
    if (chip && el.contains(chip)) hooks.onLabel?.(chip.dataset.label);
    const go = e.target.closest("[data-goto]");
    if (go && el.contains(go)) {
      e.preventDefault();
      showTab(go.dataset.goto);
    }
  });

  const logsEl = $("#logs");
  function appendLog(text, cls = "") {
    const atBottom = logsEl.scrollHeight - logsEl.scrollTop - logsEl.clientHeight < 40;
    const span = document.createElement("span");
    if (cls) span.className = cls;
    span.textContent = text + "\n";
    logsEl.appendChild(span);
    if (atBottom) logsEl.scrollTop = logsEl.scrollHeight;
    logLines.push({ text, cls });
    if (logLines.length > 200) logLines.shift();
    paintTail();
  }
  function setLogState(s) {
    logState = s;
    $("#d-tail-state").textContent = s;
    $("#d-log-state").textContent = s;
  }
  function paintTail() {
    const last = logLines.slice(-10);
    $("#d-tail-lines").innerHTML = last.length
      ? last.map((l) => tailLine(l)).join("")
      : `<div class="c-log-empty">${logState === "connecting" ? "waiting for log lines…" : "no log lines"}</div>`;
  }

  async function load() {
    try {
      rec = await api(`/v1/runs/${encodeURIComponent(id)}`);
    } catch (e) {
      if (stopped) return;
      $("#d-title").innerHTML = `<div class="sp-none"><b>${e.status === 404 ? "Run not found" : "Could not load this run"}</b><span>${escapeHtml(e.message)}</span></div>`;
      // Keep polling through a transient (network / 5xx) failure; stop on 4xx (e.g. a deleted run).
      if (e.status === undefined || e.status >= 500) {
        clearTimeout(pollTimer);
        pollTimer = setTimeout(load, 3000);
      }
      return;
    }
    if (stopped) return;
    const before = el.dataset.status;
    render(rec);
    el.dataset.status = rec.status;
    $("#d-tail-title").textContent = TERMINAL.includes(rec.status) ? "Log tail" : "Live log";
    if (before && before !== rec.status) hooks.onChange?.();
    const live = !TERMINAL.includes(rec.status);
    $("#cancel").hidden = !live;
    $("#delete").hidden = !TERMINAL.includes(rec.status);
    // Roll back (#706): only a finished run with at least one invocation id can be undone.
    const wanted = (rec.invocations || []).filter((i) => i.run_id);
    const undoable = TERMINAL.includes(rec.status) && wanted.length > 0;
    $("#rollback-toggle").hidden = !undoable;
    if (!undoable) $("#rollback-panel").hidden = true;
    const sel = $("#rollback-invocation");
    if (sel.options.length !== wanted.length) {
      sel.innerHTML = wanted
        .map((i) => `<option value="${escapeHtml(i.run_id)}">${escapeHtml(i.row_id)} — ${escapeHtml(i.run_id)}</option>`)
        .join("");
    }
    clearTimeout(pollTimer);
    if (live) pollTimer = setTimeout(load, 3000);
  }

  function render(r) {
    const [tone, ic, label] = STATUS_PILL[r.status] || ["", "", r.status];
    const name = runTitle(r);
    const started = r.started_at ? `started ${fmtAge(r.started_at)} ago` : `submitted ${fmtAge(r.submitted_at)} ago`;
    const sub = [
      pill(label, tone, ic),
      runKinds(r) ? `<span>${escapeHtml(runKinds(r))}</span>` : "",
      `<span class="mono" title="run id">${escapeHtml(r.run_id)}</span>`,
      `<span title="${escapeHtml(formatTs(r.started_at || r.submitted_at))}">${escapeHtml(started)}</span>`,
      r.attempt ? `<span>attempt ${fmtInt(r.attempt + 1)}</span>` : "",
    ].filter(Boolean);
    setCrumbTitle(name);
    $("#d-title").innerHTML = `<h1 class="c-title" title="${escapeHtml(name)}">${escapeHtml(name)}</h1><div class="c-sub">${sub.join('<span class="c-sub-sep">·</span>')}</div>`;

    const invErrors = (r.invocations || []).filter((i) => i.error && !i.skipped);
    const generic = invErrors.length && /^\d+ (pipeline )?invocation\(s\) failed\b/.test(String(r.error || "").trim());
    const errText = [generic ? "" : r.error, ...invErrors.map((i) => `${i.row_id}${i.parent_record_key ? ` (${i.parent_record_key})` : ""}: ${i.error}`)]
      .filter(Boolean)
      .join("\n");
    $("#d-alert").innerHTML = errText
      ? `<div class="c-alert bad"><b>${r.status === "failed" ? "The run failed" : "Error"}</b><pre>${escapeHtml(errText)}</pre></div>`
      : "";

    const logsLink = $("#logs-link");
    if (r.log_export?.link) {
      logsLink.href = r.log_export.link;
      logsLink.hidden = false;
    }

    const invs = r.invocations || [];
    const live = !TERMINAL.includes(r.status);
    const stamp = (iso, empty) => {
      const t = formatTsSplit(iso);
      return t ? { v: t.time, s: t.date } : { v: empty, s: "" };
    };
    const sub_ = stamp(r.submitted_at, "—");
    const st = stamp(r.started_at, r.status === "queued" || r.status === "pending" ? "waiting" : "—");
    const fin = stamp(r.finished_at, live ? "running" : "—");
    const total = fmtDur(r.submitted_at, r.finished_at);
    const queuedMs = r.started_at ? Date.parse(r.started_at) - Date.parse(r.submitted_at) : 0;
    const queued = queuedMs >= 1000 ? `${fmtDur(r.submitted_at, r.started_at)} queued` : "";
    const output = card({
      title: `${fmtInt(r.records_written ?? 0)} records written${live ? " so far" : ""}`,
      iconName: "runs",
      aside: `${fmtInt(invs.length)} invocation${invs.length === 1 ? "" : "s"}`,
      right: live ? "counted as each invocation ends" : "",
      html:
        kvGrid([
          { k: "Submitted", v: sub_.v, s: sub_.s },
          { k: "Started", v: st.v, s: st.s },
          { k: "Finished", v: fin.v, s: fin.s },
          { k: "Duration", v: live && r.started_at ? fmtAge(r.started_at) : total, s: live ? "so far" : queued },
        ]) + rowAccounting(invs),
    });

    const shown = invs.slice().sort((a, b) => (b.duration_ms || 0) - (a.duration_ms || 0));
    const top = shown.slice(0, 6);
    const invCard = invs.length
      ? `<section class="c-card"><div class="c-card-h"><b>Invocations</b><span class="c-card-r">${
          invs.length > top.length ? `<a href="#" data-goto="invocations">all ${fmtInt(invs.length)} rows →</a>` : `${fmtInt(invs.length)} row${invs.length === 1 ? "" : "s"}`
        }</span></div><table class="c-tbl d-inv-sum"><colgroup><col /><col style="width:118px" /><col style="width:96px" /><col style="width:88px" /></colgroup><thead><tr><th>Row</th><th>Status</th><th class="num">Records</th><th class="num">Duration</th></tr></thead><tbody>${top
          .map((i) => {
            const [t, s] = invStatus(i, live);
            return `<tr><td class="mono c-ell" title="${escapeHtml(i.row_id)}${i.parent_record_key ? ` · ${escapeHtml(i.parent_record_key)}` : ""}">${escapeHtml(i.row_id)}${i.parent_record_key ? `<span class="c-muted"> · ${escapeHtml(i.parent_record_key)}</span>` : ""}</td><td class="c-nowrap"><span class="c-dot ${t}"></span>${escapeHtml(s)}</td><td class="num">${fmtInt(i.records_written ?? 0)}</td><td class="num">${fmtMs(i.duration_ms)}</td></tr>`;
          })
          .join("")}</tbody></table></section>`
      : `<section class="c-card"><div class="c-card-h"><b>Invocations</b></div><div class="c-card-empty">${live ? "No invocation has finished yet — rows appear as each one ends." : "This run recorded no invocations."}</div></section>`;

    const prov = provenance(r);
    const provCard = prov.length
      ? `<section class="c-card"><div class="c-card-h"><b>Provenance</b></div><div class="c-kvgrid" style="--c-cols:2">${prov.join("")}</div></section>`
      : "";
    const labels = sortedLabels(r.labels);
    const labelCard = labels.length
      ? `<section class="c-card"><div class="c-card-h"><b>Labels</b><span class="c-card-r">click one to filter the list</span></div><div class="rs-labels-chips d-labels">${labels.map(([k, v, p]) => labelChip(k, v, p)).join("")}</div></section>`
      : "";

    $("#d-over").innerHTML = `${output}<div class="${provCard ? "c-two" : ""}">${invCard}${provCard}</div>${labelCard}${logExportCard(r.log_export)}`;

    // Per-row timing (#645): slowest first, with a proportional bar.
    const maxMs = Math.max(1, ...shown.map((i) => i.duration_ms || 0));
    $("#invocations").innerHTML = invs.length
      ? `<section class="c-card"><div class="c-card-h"><b>Invocations</b><span class="c-card-aside">slowest first</span><span class="c-card-r">${fmtInt(invs.length)} row${invs.length === 1 ? "" : "s"}</span></div><div class="d-scroll-x"><table class="tbl inv-tbl"><thead><tr><th title="the matrix row this invocation ran">matrix row</th><th>parent key</th><th title="the invocation's own run id — what faucet rollback --run undoes">run id</th><th>records</th><th title="where every offered row went: written / merged by a keyed write / declined by the sink / failed to the DLQ / quarantined / dropped / unaccounted">rows</th><th title="sink writes: committed / partly to the DLQ / whole to the DLQ (dlq_all) / failed">batches</th><th title="how far the source was behind its head when the invocation ended">lag</th><th>duration</th><th>error</th></tr></thead><tbody>${shown
          .map((i) => {
            const ms = i.duration_ms || 0;
            const pct = Math.round((ms / maxMs) * 100);
            return `<tr><td>${escapeHtml(i.row_id)}</td><td>${escapeHtml(i.parent_record_key || "—")}</td><td class="mono" title="${escapeHtml(i.run_id || "")}">${escapeHtml(i.run_id ? i.run_id.slice(0, 8) + "…" : "—")}</td><td>${fmtInt(i.records_written ?? 0)}</td><td>${rowsCell(i.rows)}</td><td>${batchCell(i.batches)}</td><td class="mono">${escapeHtml(lagText(i.source_lag))}</td><td><div style="white-space:nowrap">${fmtMs(ms)}</div><div class="inv-bar"><i style="width:${pct}%"></i></div></td><td title="${escapeHtml(i.error || "")}">${escapeHtml(i.error || "")}</td></tr>`;
          })
          .join("")}</tbody></table></div></section>`
      : `<div class="sp-none"><b>No invocations yet</b><span>${live ? "Each matrix row's invocation is listed here when it ends." : "This run recorded no invocations."}</span></div>`;
  }

  $("#cancel").onclick = async () => {
    try { await api(`/v1/runs/${encodeURIComponent(id)}/cancel`, { method: "POST" }); toast("cancel requested"); load(); hooks.onChange?.(); }
    catch (e) { toast(e.message, "error"); }
  };
  $("#delete").onclick = async () => {
    if (!confirm(`Delete run ${rec ? runTitle(rec) : id} from the history?`)) return;
    try { await api(`/v1/runs/${encodeURIComponent(id)}`, { method: "DELETE" }); toast("run deleted"); hooks.onDeleted?.(); }
    catch (e) { toast(e.message, "error"); }
  };
  $("#rollback-toggle").onclick = () => {
    const panel = $("#rollback-panel");
    panel.hidden = !panel.hidden;
    if (!panel.hidden) showTab("overview");
  };
  $("#rollback-go").onclick = async () => {
    const resultEl = $("#rollback-result");
    const body = {
      invocation_id: $("#rollback-invocation").value || undefined,
      dry_run: $("#rollback-dryrun").checked,
      force: $("#rollback-force").checked,
    };
    const cfg = $("#rollback-config").value.trim();
    if (cfg) body.config = cfg;
    resultEl.textContent = "rolling back…";
    try {
      const r = await api(`/v1/runs/${encodeURIComponent(id)}/rollback`, { method: "POST", body });
      renderRollback(resultEl, r);
    } catch (e) {
      resultEl.innerHTML = `<div class="error-box">${escapeHtml(e.message)}</div>`;
      if (e.status === 422) $("#rollback-config-details").open = true;
    }
  };

  setLogState("connecting");
  paintTail();
  logCtrl = streamLogs(id, {
    onLog: (l) => {
      if (logState !== "streaming") setLogState("streaming");
      appendLog(l);
    },
    onTruncated: (m) => appendLog(`— ${m} —`, "log-truncated"),
    onEnd: () => {
      appendLog("— end of logs —", "log-end");
      setLogState("ended");
    },
    onExpired: () => {
      appendLog("— logs expired —", "log-end");
      setLogState("expired");
    },
    onLink: (url) => {
      appendLog("— the local copy of these logs has aged out; open them in the log service ↗ —", "log-end");
      const a = $("#logs-link");
      a.href = url;
      a.hidden = false;
      setLogState("in the log service");
    },
    onError: (e) => {
      appendLog(`— log stream error: ${e.message} —`, "log-truncated");
      setLogState("stream error");
    },
  });

  load();
  return () => {
    stopped = true;
    clearTimeout(pollTimer);
    logCtrl?.abort();
  };
}

const LEVEL_RE = /\b(TRACE|DEBUG|INFO|WARN|ERROR)\b/;

function tailLine({ text, cls }) {
  if (cls) return `<div class="c-log-note ${cls}">${escapeHtml(text)}</div>`;
  const m = text.match(LEVEL_RE);
  if (!m) return `<div>${escapeHtml(text)}</div>`;
  const tone = { WARN: "warn", ERROR: "bad", INFO: "info" }[m[1]] || "";
  const at = m.index;
  return `<div>${escapeHtml(text.slice(0, at))}<span class="c-log-lv ${tone}">${m[1]}</span>${escapeHtml(text.slice(at + m[1].length))}</div>`;
}

function invStatus(i, live) {
  const s = i.status || (i.skipped ? "skipped" : i.error ? "failed" : live ? "running" : "succeeded");
  return [{ succeeded: "ok", failed: "bad", skipped: "muted", running: "info" }[s] || "muted", s];
}

function provenance(r) {
  const kv = (k, v, title = "") => `<div class="c-kv"><small${title ? ` title="${escapeHtml(title)}"` : ""}>${escapeHtml(k)}</small><b>${v}</b></div>`;
  const out = [];
  if (r.submitted_by) out.push(kv("Submitted by", `<span class="mono">${escapeHtml(r.submitted_by)}</span>`, "the principal that submitted the run"));
  if (templateLink(r)) out.push(kv("Template", templateLink(r)));
  if (r.pool) out.push(kv("Pool", `<span class="mono">${escapeHtml(r.pool)}</span>`, "execution pool"));
  if (r.tenant) out.push(kv("Tenant", `<a href="#/tenants/${encodeURIComponent(r.tenant)}">${escapeHtml(r.tenant)}</a>`));
  const deps = Object.entries(r.depends_on || {}).filter(([, after]) => (after || []).length);
  if (deps.length) {
    out.push(kv("Depends on", deps.map(([row, after]) => `<div class="mono" title="${escapeHtml(row)} starts after ${escapeHtml(after.join(", "))}">${escapeHtml(row)} ← ${after.map(escapeHtml).join(", ")}</div>`).join(""), "completion order the run request added"));
  }
  if (r.replay_of) out.push(kv("Replayed from", `<span class="mono">${escapeHtml(r.replay_of)}</span>`, "the DLQ location this run replayed"));
  if (r.idempotency_key) out.push(kv("Idempotency key", `<span class="mono">${escapeHtml(r.idempotency_key)}</span>`));
  if (r.clock) out.push(kv("Clock", `<span class="mono">${escapeHtml(r.clock)}</span>`, "the run's ${now.*} clock"));
  if (r.timeout_secs) out.push(kv("Timeout", `${fmtInt(r.timeout_secs)}s`));
  return out;
}

const ACCOUNT = [
  ["accepted", "written", "var(--ok)"],
  ["coalesced", "merged", "var(--info)"],
  ["skipped", "skipped", "var(--muted)"],
  ["failed", "DLQ", "var(--warn)"],
  ["quarantined", "quarantined", "var(--warn)"],
  ["dropped", "dropped", "var(--bad)"],
  ["unaccounted", "unaccounted", "var(--bad)"],
];

/** Every invocation's row accounting (#879) summed into one bar. */
function rowAccounting(invs) {
  const sum = { offered: 0 };
  let any = false;
  for (const i of invs) {
    const r = i.rows;
    if (!r) continue;
    any = true;
    sum.offered += r.offered || 0;
    for (const [k] of ACCOUNT) sum[k] = (sum[k] || 0) + (r[k] || 0);
  }
  if (!any || !sum.offered) return "";
  const total = ACCOUNT.reduce((n, [k]) => n + (sum[k] || 0), 0) || 1;
  const bar = ACCOUNT.filter(([k]) => sum[k])
    .map(([k, , c]) => `<i style="width:${((sum[k] / total) * 100).toFixed(2)}%;background:${c}"></i>`)
    .join("");
  const legend = ACCOUNT.filter(([k]) => sum[k])
    .map(([k, label, c]) => `<span style="--c:${c}">${label} ${fmtInt(sum[k])}</span>`)
    .join("");
  const ok = !sum.unaccounted;
  return `<div class="c-acct"><div class="c-acct-h"><span>Row accounting · ${fmtInt(sum.offered)} offered</span><span class="${ok ? "ok" : "bad"}">${ok ? "balanced" : `${fmtInt(sum.unaccounted)} unaccounted`}</span></div><div class="c-bar">${bar}</div><div class="c-legend">${legend}</div></div>`;
}

/** `98 ok · 2 dlq` — where one invocation's offered rows went (#879). */
function rowsCell(r) {
  if (!r || (!r.offered && !r.unaccounted)) return `<span class="run-meta">—</span>`;
  const parts = [`<span>${fmtInt(r.accepted || 0)} ok</span>`];
  const filtered = Math.max(0, (r.transform_in || 0) - (r.transform_out || 0));
  if (r.coalesced) parts.push(`<span>${fmtInt(r.coalesced)} merged</span>`);
  if (r.skipped) parts.push(`<span>${fmtInt(r.skipped)} skipped</span>`);
  if (r.failed) parts.push(`<span class="batch-warn">${fmtInt(r.failed)} dlq</span>`);
  if (r.quarantined) parts.push(`<span class="batch-warn">${fmtInt(r.quarantined)} quarantined</span>`);
  if (r.dropped) parts.push(`<span class="batch-warn">${fmtInt(r.dropped)} dropped</span>`);
  if (r.unaccounted) parts.push(`<span class="batch-bad">${fmtInt(r.unaccounted)} unaccounted</span>`);
  const title = [
    `${fmtInt(r.offered || 0)} offered`,
    filtered ? `${fmtInt(filtered)} filtered by transforms` : null,
    r.non_finite ? `${fmtInt(r.non_finite)} NaN / infinite values written as null` : null,
  ].filter(Boolean).join(", ");
  return `<span class="batch-cell" title="${title}">${parts.join(" · ")}</span>`;
}

/** `12 ok` or `10 ok · 1 partial · 1 dlq_all` for one invocation's sink writes (#737). */
function batchCell(b) {
  if (!b || !b.attempted) return `<span class="run-meta">—</span>`;
  const parts = [`<span>${fmtInt(b.committed)} ok</span>`];
  if (b.dlq_partial) parts.push(`<span class="batch-warn">${fmtInt(b.dlq_partial)} partial</span>`);
  if (b.dlq_all) parts.push(`<span class="batch-warn">${fmtInt(b.dlq_all)} dlq_all</span>`);
  if (b.failed) parts.push(`<span class="batch-bad">${fmtInt(b.failed)} failed</span>`);
  return `<span class="batch-cell" title="${fmtInt(b.attempted)} sink write(s)">${parts.join(" · ")}</span>`;
}

/** `412 MiB · 3m 20s` from a source-lag reading (#733). */
function lagText(l) {
  if (!l) return "—";
  const out = [];
  if (l.bytes != null) {
    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let v = l.bytes;
    let i = 0;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
    out.push(i === 0 ? `${l.bytes} B` : `${Math.round(v)} ${units[i]}`);
  }
  if (l.events != null) out.push(`${fmtInt(l.events)} event${l.events === 1 ? "" : "s"}`);
  if (l.seconds != null) {
    const sec = Math.max(0, Math.floor(l.seconds));
    out.push(sec < 60 ? `${sec}s` : sec < 3600 ? `${Math.floor(sec / 60)}m ${sec % 60}s` : `${Math.floor(sec / 3600)}h ${Math.floor((sec % 3600) / 60)}m`);
  }
  return out.join(" · ") || "—";
}

import { api, toast } from "../api.js";
import { streamLogs } from "../sse.js";
import { navigate } from "../router.js";
import { formatTsSplit } from "../tz.js";
import { escapeHtml, fmtInt, fmtCompact } from "../utils.js";

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

const TERMINAL = ["completed", "failed", "cancelled"];

/** Link to the template a run was triggered from, with the numeric version it
 *  resolved to (both carried in the run's labels). Empty for other runs. */
function templateLink(rec) {
  const l = rec.labels || {};
  if (!l.template) return "";
  const ver = l.template_version ? ` <b>v${escapeHtml(String(l.template_version))}</b>` : "";
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
  const cls = r.dry_run ? "pill-queued" : r.applied ? "pill-completed" : "pill-failed";
  el.innerHTML = `<div class="rollback-result"><span class="pill ${cls}">${status}</span> run <span class="mono">${escapeHtml(r.run_id)}</span> on <b>${escapeHtml(r.row)}</b> (${escapeHtml(r.sink_kind)} ${escapeHtml(r.dataset)}, ${escapeHtml(r.mode)})<ul>${lines.map((l) => `<li>${l}</li>`).join("")}</ul></div>`;
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
      .map(([k, v]) => `<tr><td>${escapeHtml(k)}</td><td>${v}</td></tr>`)
      .join("") || `<tr><td colspan="2">—</td></tr>`;
  const sample = (s.sample || [])
    .map(
      (e) =>
        `<li><span class="pill">${escapeHtml(e.reason || "?")}</span> ${escapeHtml(e.error_kind || "")}: ${escapeHtml(e.error_message || "")}<pre class="dlq-json">${escapeHtml(JSON.stringify(e.payload))}</pre></li>`,
    )
    .join("");
  el.innerHTML = `
    <div class="dlq-summary">
      <div>${s.total_envelopes} envelope(s) · ${s.files_read} file(s) · ${s.malformed} malformed · ${s.non_envelope} non-envelope</div>
      <div class="dlq-tables">
        <table class="tbl"><thead><tr><th>reason</th><th>count</th></tr></thead><tbody>${rows(s.by_reason)}</tbody></table>
        <table class="tbl"><thead><tr><th>error kind</th><th>count</th></tr></thead><tbody>${rows(s.by_error_kind)}</tbody></table>
      </div>
      <ul class="dlq-sample">${sample}</ul>
    </div>`;
}

export async function renderDetail(container, { id }) {
  let pollTimer = null;
  let logCtrl = null;

  container.innerHTML = `
    <div class="page">
      <div class="page-head">
        <button class="btn-ghost" id="back">← Runs</button>
        <div class="detail-actions">
          <button class="btn-warn" id="cancel" data-perm="run_write" hidden>Cancel</button>
          <button class="btn-warn" id="rollback-toggle" data-perm="rollback" hidden>Roll back…</button>
          <button class="btn-danger" id="delete" data-perm="run_write" hidden>Delete</button>
        </div>
      </div>
      <div id="detail-head"></div>
      <section class="dlq-panel rollback-panel" id="rollback-panel" data-perm="rollback" hidden>
        <h2>Roll back this run</h2>
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
      </section>
      <h2>Invocations</h2>
      <div id="invocations"></div>
      <h2>Logs</h2>
      <pre id="logs" class="logs"></pre>
      <h2>Dead-letter queue</h2>
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
    </div>`;

  container.querySelector("#back").onclick = () => navigate("#/runs");
  wireDlqPanel(container);

  const logsEl = container.querySelector("#logs");
  function appendLog(text, cls = "") {
    const atBottom = logsEl.scrollHeight - logsEl.scrollTop - logsEl.clientHeight < 40;
    const span = document.createElement("span");
    if (cls) span.className = cls;
    span.textContent = text + "\n";
    logsEl.appendChild(span);
    if (atBottom) logsEl.scrollTop = logsEl.scrollHeight;
  }

  async function load() {
    let rec;
    try {
      rec = await api(`/v1/runs/${encodeURIComponent(id)}`);
    } catch (e) {
      container.querySelector("#detail-head").innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
      // Keep polling through a transient (network / 5xx) failure; stop on 4xx (e.g. a deleted run).
      if (e.status === undefined || e.status >= 500) {
        clearTimeout(pollTimer);
        pollTimer = setTimeout(load, 3000);
      }
      return;
    }
    renderHead(rec);
    const live = !TERMINAL.includes(rec.status);
    container.querySelector("#cancel").hidden = !(rec.status === "running" || rec.status === "queued");
    container.querySelector("#delete").hidden = !TERMINAL.includes(rec.status);
    // Roll back (#706): only a finished run with at least one invocation id can
    // be undone. The toggle keeps its own hidden state once revealed.
    const undoable = TERMINAL.includes(rec.status) && (rec.invocations || []).some((i) => i.run_id);
    const toggle = container.querySelector("#rollback-toggle");
    if (undoable && toggle.hidden && !toggle.dataset.shown) toggle.hidden = false;
    toggle.dataset.shown = "1";
    if (!undoable) { toggle.hidden = true; container.querySelector("#rollback-panel").hidden = true; }
    const sel = container.querySelector("#rollback-invocation");
    const wanted = (rec.invocations || []).filter((i) => i.run_id);
    if (sel.options.length !== wanted.length) {
      sel.innerHTML = wanted
        .map((i) => `<option value="${escapeHtml(i.run_id)}">${escapeHtml(i.row_id)} — ${escapeHtml(i.run_id)}</option>`)
        .join("");
    }
    if (live) {
      clearTimeout(pollTimer);
      pollTimer = setTimeout(load, 3000);
    }
  }

  function renderHead(rec) {
    const errors = rec.error ? `<div class="error-box">${escapeHtml(rec.error)}</div>` : "";
    const invCount = (rec.invocations || []).length;
    const stamp = (iso) => {
      const t = formatTsSplit(iso);
      return t ? `<dd>${t.time}<small>${t.date}</small></dd>` : `<dd class="rs-empty">—</dd>`;
    };
    const provenance = [
      templateLink(rec) && `<dt>Template</dt><dd>${templateLink(rec)}</dd>`,
      rec.idempotency_key && `<dt>Idempotency key</dt><dd class="mono">${escapeHtml(rec.idempotency_key)}</dd>`,
    ].filter(Boolean).join("");
    container.querySelector("#detail-head").innerHTML = `
      <section class="run-summary">
        <header class="rs-head">
          <span class="pill pill-${rec.status}">${rec.status}</span>
          <h2 class="rs-name">${escapeHtml(rec.name || rec.run_id)}</h2>
          ${rec.name ? `<span class="rs-id mono" title="run id">${escapeHtml(rec.run_id)}</span>` : ""}
        </header>
        <div class="rs-segments">
          <div class="rs-seg">
            <h3>Timeline</h3>
            <dl><dt>Submitted</dt>${stamp(rec.submitted_at)}<dt>Started</dt>${stamp(rec.started_at)}<dt>Finished</dt>${stamp(rec.finished_at)}</dl>
          </div>
          <div class="rs-seg">
            <h3>Duration</h3>
            <dl>
              <dt title="submitted → finished">Total</dt><dd>${fmtDur(rec.submitted_at, rec.finished_at)}<small>including time queued</small></dd>
              <dt title="started → finished">Running</dt><dd>${fmtDur(rec.started_at, rec.finished_at)}</dd>
            </dl>
          </div>
          <div class="rs-seg">
            <h3>Output</h3>
            <dl>
              <dt>Records written</dt><dd title="${fmtInt(rec.records_written ?? 0)}">${fmtCompact(rec.records_written ?? 0)}</dd>
              <dt>Invocations</dt><dd>${fmtInt(invCount)}</dd>
            </dl>
          </div>
          ${provenance ? `<div class="rs-seg"><h3>Provenance</h3><dl>${provenance}</dl></div>` : ""}
        </div>
      </section>${errors}`;
    const inv = container.querySelector("#invocations");
    // Per-row timing (#645): sort slowest-first so the object dominating the
    // run's makespan is obvious, and draw a proportional bar next to each.
    const invs = (rec.invocations || [])
      .slice()
      .sort((a, b) => (b.duration_ms || 0) - (a.duration_ms || 0));
    const maxMs = Math.max(1, ...invs.map((i) => i.duration_ms || 0));
    inv.innerHTML =
      `<table class="tbl"><thead><tr><th title="the matrix row this invocation ran">matrix row</th><th>parent key</th><th title="the invocation's own run id — what faucet rollback --run undoes">run id</th><th>records</th><th style="min-width:160px">duration</th><th>error</th></tr></thead><tbody>` +
      invs
        .map((i) => {
          const ms = i.duration_ms || 0;
          const pct = Math.round((ms / maxMs) * 100);
          // A recessed neutral track (groove) with a glossy 3D pill fill
          // proportional to the time — the slowest row fills it completely. The
          // fill layers a white top-sheen over a light→dark teal gradient, with a
          // drop shadow + inner highlight for depth. Lives inside the duration
          // cell so it reads as a duration visual, not the error column. Track
          // uses a translucent neutral so it works on light+dark.
          const fill =
            `<div style="height:100%;width:${pct}%;min-width:3px;border-radius:4px;` +
            `background:linear-gradient(to bottom, var(--brand-strong), var(--brand));` +
            `box-shadow:inset 0 1px 0 rgba(255,255,255,0.25), 0 1px 1px rgba(0,0,0,0.12)"></div>`;
          const bar =
            `<div style="height:9px;width:100%;max-width:180px;border-radius:4px;background:rgba(120,120,120,0.14);` +
            `box-shadow:inset 0 1px 1px rgba(0,0,0,0.12);margin-top:5px">${fill}</div>`;
          return `<tr><td>${escapeHtml(i.row_id)}</td><td>${escapeHtml(i.parent_record_key || "—")}</td><td class="mono">${escapeHtml(i.run_id || "—")}</td><td>${fmtInt(i.records_written ?? 0)}</td><td><div style="white-space:nowrap">${fmtMs(ms)}</div>${bar}</td><td>${escapeHtml(i.error || "")}</td></tr>`;
        })
        .join("") +
      `</tbody></table>`;
  }

  container.querySelector("#cancel").onclick = async () => {
    try { await api(`/v1/runs/${encodeURIComponent(id)}/cancel`, { method: "POST" }); toast("cancel requested"); load(); }
    catch (e) { toast(e.message, "error"); }
  };
  container.querySelector("#delete").onclick = async () => {
    try { await api(`/v1/runs/${encodeURIComponent(id)}`, { method: "DELETE" }); navigate("#/runs"); }
    catch (e) { toast(e.message, "error"); }
  };
  container.querySelector("#rollback-toggle").onclick = () => {
    const panel = container.querySelector("#rollback-panel");
    panel.hidden = !panel.hidden;
  };
  container.querySelector("#rollback-go").onclick = async () => {
    const resultEl = container.querySelector("#rollback-result");
    const body = {
      invocation_id: container.querySelector("#rollback-invocation").value || undefined,
      dry_run: container.querySelector("#rollback-dryrun").checked,
      force: container.querySelector("#rollback-force").checked,
    };
    const cfg = container.querySelector("#rollback-config").value.trim();
    if (cfg) body.config = cfg;
    resultEl.textContent = "rolling back…";
    try {
      const r = await api(`/v1/runs/${encodeURIComponent(id)}/rollback`, { method: "POST", body });
      renderRollback(resultEl, r);
      // A 422 for a missing config is surfaced below; on success reveal the
      // config editor only when it was needed.
    } catch (e) {
      resultEl.innerHTML = `<div class="error-box">${escapeHtml(e.message)}</div>`;
      if (e.status === 422) container.querySelector("#rollback-config-details").open = true;
    }
  };

  logCtrl = streamLogs(id, {
    onLog: (l) => appendLog(l),
    onTruncated: (m) => appendLog(`— ${m} —`, "log-truncated"),
    onEnd: () => appendLog("— end of logs —", "log-end"),
    onExpired: () => appendLog("— logs expired —", "log-end"),
    onError: (e) => appendLog(`— log stream error: ${e.message} —`, "log-truncated"),
  });

  await load();
  return () => {
    clearTimeout(pollTimer);
    logCtrl?.abort();
  };
}

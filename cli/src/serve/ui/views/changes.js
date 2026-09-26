// Change requests (#703): plan → approve → run. The list of proposals with
// their status, and a detail pane that renders the stored plan (one row per
// pipeline row: source → sink, write mode, delivery guarantee, transforms,
// policy verdict, impact severity), the budget, the approvals so far, and the
// approve / reject controls — hidden without `change_approve`; the server's
// approval policy still has the final say (a 403 names the rule).
import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml, fmtInt } from "../utils.js";
import { fmtTime } from "./runs.js";

const STATUSES = ["pending", "approved", "executed", "rejected", "invalidated", "expired", "failed"];

function statusPill(s) {
  const cls = {
    pending: "pill-queued",
    approved: "pill-queued",
    executed: "pill-completed",
    rejected: "pill-cancelled",
    invalidated: "pill-failed",
    expired: "pill-cancelled",
    failed: "pill-failed",
  }[s] || "";
  return `<span class="pill ${cls}">${escapeHtml(s)}</span>`;
}

function summaryLine(c) {
  const s = (c.plan && c.plan.summary) || {};
  if (c.kind === "run") {
    const sinks = Array.isArray(s.sinks) ? s.sinks.join(", ") : "";
    return `${escapeHtml(s.pipeline || "(unnamed)")} · ${fmtInt(s.rows || 0)} row${s.rows === 1 ? "" : "s"}${sinks ? ` → ${escapeHtml(sinks)}` : ""}`;
  }
  if (c.kind === "template_register") {
    return `register ${escapeHtml(s.template || "?")} (${escapeHtml(s.template_kind || "template")})${s.previous_version != null ? ` after v${s.previous_version}` : ", first version"}${s.launch ? " and launch" : ""}`;
  }
  if (c.kind === "template_launch") {
    return `launch ${escapeHtml(s.template || "?")} v${s.target_version ?? "?"}${s.stable_before != null ? ` (stable is v${s.stable_before})` : " (nothing launched yet)"}`;
  }
  return "";
}

export async function renderChanges(container, params = {}) {
  container.innerHTML = `
    <div class="page page-wide">
      <div class="page-head">
        <h1>Changes</h1>
        <button class="btn-ghost" id="c-refresh" title="refresh">↻</button>
      </div>
      <div class="filters filters-1line">
        <span class="filter-chips" id="c-status">
          <button class="chip chip-on" data-status="pending">pending</button>
          <button class="chip" data-status="">all</button>
          ${STATUSES.filter((s) => s !== "pending").map((s) => `<button class="chip" data-status="${s}">${s}</button>`).join("")}
        </span>
      </div>
      <table class="chg-table">
        <thead><tr><th>status</th><th>kind</th><th>proposal</th><th>requester</th><th>approvals</th><th>requested</th></tr></thead>
        <tbody id="c-list"></tbody>
      </table>
      <div id="c-detail" class="chg-detail" hidden></div>
    </div>`;

  const list = container.querySelector("#c-list");
  const detail = container.querySelector("#c-detail");
  let status = "pending";
  let selected = params.id || null;

  async function load() {
    list.innerHTML = `<tr><td colspan="6" class="empty">loading…</td></tr>`;
    const p = new URLSearchParams();
    if (status) p.set("status", status);
    let rows;
    try {
      rows = await api(`/v1/changes?${p}`);
    } catch (e) {
      list.innerHTML = `<tr><td colspan="6" class="empty">${escapeHtml(e.message)}</td></tr>`;
      return;
    }
    if (!rows.length) {
      list.innerHTML = `<tr><td colspan="6" class="empty">No ${status || ""} change requests. Propose one with <code>POST /v1/changes</code>, the Submit page's “request approval” option, or an agent's <code>propose_run</code>.</td></tr>`;
    } else {
      list.innerHTML = rows
        .map(
          (c) => `<tr class="chg-tr ${c.id === selected ? "chg-selected" : ""}" data-id="${escapeHtml(c.id)}">
            <td>${statusPill(c.status)}</td>
            <td><code>${escapeHtml(c.kind)}</code></td>
            <td class="chg-summary"><div>${summaryLine(c)}</div>${c.reason ? `<div class="chg-reason">${escapeHtml(c.reason)}</div>` : ""}</td>
            <td>${escapeHtml(c.requester)}</td>
            <td>${(c.approvals || []).length} / ${c.required_approvals}</td>
            <td title="${escapeHtml(c.created_at)}">${fmtTime(c.created_at)}</td>
          </tr>`,
        )
        .join("");
      list.querySelectorAll(".chg-tr").forEach((tr) => {
        tr.onclick = () => {
          selected = tr.dataset.id;
          navigate(`#/changes/${encodeURIComponent(selected)}`);
        };
      });
    }
    if (selected) await showDetail(selected);
    else detail.hidden = true;
  }

  async function showDetail(id) {
    let c;
    try {
      c = await api(`/v1/changes/${encodeURIComponent(id)}`);
    } catch (e) {
      detail.hidden = false;
      detail.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
      return;
    }
    const plan = c.plan || { rows: [], summary: {} };
    const rowsHtml = plan.rows.length
      ? `<table class="chg-plan"><thead><tr><th>row</th><th>source → sink</th><th>write</th><th>delivery</th><th>transforms</th><th>policy</th><th>impact</th></tr></thead><tbody>${plan.rows
          .map(
            (r) => `<tr>
              <td><code>${escapeHtml(r.row || "")}</code></td>
              <td>${escapeHtml(r.source || "")} → <b>${escapeHtml(r.sink || "")}</b></td>
              <td>${escapeHtml(r.write_mode || "")}</td>
              <td>${escapeHtml(r.delivery_guarantee || "")}</td>
              <td>${(r.transforms || []).map((t) => `<code>${escapeHtml(t)}</code>`).join(" ") || "—"}</td>
              <td>${r.policy ? (r.policy.violations && r.policy.violations.length ? `<span class="pill pill-failed">${r.policy.violations.length} violation(s)</span>` : `<span class="pill pill-completed">ok</span>`) : "—"}</td>
              <td>${r.impact ? `<span class="pill ${r.impact.severity === "breaking" ? "pill-failed" : r.impact.severity === "additive" ? "pill-completed" : ""}">${escapeHtml(r.impact.severity)}</span>` : "—"}</td>
            </tr>`,
          )
          .join("")}</tbody></table>`
      : `<p class="chg-muted">No row plan for this kind.</p>`;
    const budget = c.budget
      ? Object.entries(c.budget)
          .filter(([, v]) => v != null && !(Array.isArray(v) && !v.length))
          .map(([k, v]) => `<span class="chg-kv"><b>${escapeHtml(k)}</b> ${escapeHtml(Array.isArray(v) ? v.join(", ") : String(v))}</span>`)
          .join("")
      : `<span class="chg-muted">none</span>`;
    const approvals = (c.approvals || []).length
      ? `<ul class="chg-approvals">${c.approvals
          .map((a) => `<li><b>${escapeHtml(a.principal)}</b> (${escapeHtml(a.role)}) · ${fmtTime(a.at)}${a.comment ? ` — ${escapeHtml(a.comment)}` : ""}</li>`)
          .join("")}</ul>`
      : `<span class="chg-muted">none yet</span>`;
    const outcome = c.run_id
      ? `<p>Run <a href="#/runs/${encodeURIComponent(c.run_id)}"><code>${escapeHtml(c.run_id)}</code></a></p>`
      : c.template
        ? `<p>Template <a href="#/templates/${encodeURIComponent(c.template.id)}"><code>${escapeHtml(c.template.id)}</code></a> v${c.template.version}</p>`
        : "";
    const pending = c.status === "pending";
    detail.hidden = false;
    detail.innerHTML = `
      <div class="chg-detail-head">
        <h2>${statusPill(c.status)} <code>${escapeHtml(c.kind)}</code> · ${summaryLine(c)}</h2>
        <button class="btn-ghost" id="cd-close">✕</button>
      </div>
      <p class="chg-meta">Requested by <b>${escapeHtml(c.requester)}</b> (${escapeHtml(c.requester_role)}) · ${fmtTime(c.created_at)} · ${pending ? `expires ${fmtTime(c.expires_at)}` : `updated ${fmtTime(c.updated_at)}`} · id <code>${escapeHtml(c.id)}</code></p>
      ${c.reason ? `<p class="chg-reason-big">${escapeHtml(c.reason)}</p>` : ""}
      ${c.error ? `<p class="chg-error">${escapeHtml(c.error)}</p>` : ""}
      ${c.rejection ? `<p class="chg-error">Rejected by <b>${escapeHtml(c.rejection.principal)}</b>: ${escapeHtml(c.rejection.reason)}</p>` : ""}
      ${outcome}
      <h3>Plan</h3>
      ${rowsHtml}
      <div class="chg-cols">
        <div><h3>Budget</h3><div class="chg-kvs">${budget}</div></div>
        <div><h3>Approvals (${(c.approvals || []).length} / ${c.required_approvals})</h3>${approvals}</div>
      </div>
      ${
        pending
          ? `<div class="chg-actions" data-perm="change_approve">
              <input id="cd-comment" placeholder="comment (optional)" />
              <button class="btn-primary" id="cd-approve">Approve</button>
              <input id="cd-reason" placeholder="reason for rejecting" />
              <button class="btn-warn" id="cd-reject">Reject</button>
            </div>`
          : ""
      }`;
    detail.querySelector("#cd-close").onclick = () => {
      selected = null;
      navigate("#/changes");
    };
    const approve = detail.querySelector("#cd-approve");
    if (approve) {
      approve.onclick = async () => {
        const comment = detail.querySelector("#cd-comment").value.trim();
        approve.disabled = true;
        try {
          const r = await api(`/v1/changes/${encodeURIComponent(id)}/approve`, {
            method: "POST",
            body: comment ? { comment } : {},
          });
          toast(r.status === "pending" ? `approved — ${r.approvals.length} / ${r.required_approvals}` : `change ${r.status}`, r.status === "failed" || r.status === "invalidated" ? "error" : "info");
          await load();
        } catch (e) {
          toast(e.message, "error");
          approve.disabled = false;
        }
      };
      detail.querySelector("#cd-reject").onclick = async () => {
        const reason = detail.querySelector("#cd-reason").value.trim();
        if (!reason) {
          toast("a rejection needs a reason", "error");
          return;
        }
        try {
          await api(`/v1/changes/${encodeURIComponent(id)}/reject`, { method: "POST", body: { reason } });
          toast("change rejected");
          await load();
        } catch (e) {
          toast(e.message, "error");
        }
      };
    }
  }

  container.querySelectorAll("#c-status .chip").forEach((chip) => {
    chip.onclick = () => {
      container.querySelectorAll("#c-status .chip").forEach((c) => c.classList.remove("chip-on"));
      chip.classList.add("chip-on");
      status = chip.dataset.status;
      load();
    };
  });
  container.querySelector("#c-refresh").onclick = load;
  await load();
}

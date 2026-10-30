// Change requests (#703): plan → approve → run. The proposals beside the
// selected one's detail: the stored plan as a diff (one block per pipeline
// row: source → sink, write mode, delivery guarantee, transforms), the impact
// and policy verdicts, the budget, the approvals so far and the approve /
// reject controls — hidden without `change_approve`; the server's approval
// policy still has the final say (a 403 names the rule).
import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml, fmtInt, fmtAge } from "../utils.js";
import { fmtTime } from "./runs.js";
import { withTenant } from "../tenant.js";
import { whoami, can } from "../access.js";
import { refreshBadges, setCrumbTitle } from "../shell.js";
import { icon, statStrip, listRow, tabs, pill, card, kvGrid, diffBlock } from "../components.js";
import { splitPane, sectionList } from "./split.js";

const LIMIT = 500;
const DECIDED = ["executed", "rejected", "failed", "invalidated", "expired"];

const RING = {
  pending: ["warn", "hollow"],
  approved: ["info", "half"],
  executed: ["ok", "full"],
  rejected: ["bad", "full"],
  failed: ["bad", "full"],
  invalidated: ["bad", "hollow"],
  expired: ["muted", "hollow"],
};
const TONE = { pending: "warn", approved: "info", executed: "ok", rejected: "bad", failed: "bad", invalidated: "bad", expired: "" };
const KIND = {
  run: "run",
  template_register: "template register",
  template_launch: "template launch",
  template_delete: "template delete",
  template_deprecate: "template deprecate",
};

/** One plain-text line saying what a change proposes. */
export function summaryText(c) {
  const s = (c.plan && c.plan.summary) || {};
  if (c.kind === "run") {
    const sinks = Array.isArray(s.sinks) ? s.sinks.join(", ") : "";
    return `Run ${s.pipeline || "(unnamed)"} · ${fmtInt(s.rows || 0)} row${s.rows === 1 ? "" : "s"}${sinks ? ` → ${sinks}` : ""}`;
  }
  if (c.kind === "template_register") {
    return `Register ${s.template || "?"} (${s.template_kind || "template"})${s.previous_version != null ? ` after v${s.previous_version}` : ", first version"}${s.launch ? " and launch" : ""}`;
  }
  if (c.kind === "template_launch") {
    return `Launch ${s.template || "?"} v${s.target_version ?? "?"}`;
  }
  if (c.kind === "template_delete") {
    const what = s.delete === "all" ? "every version of" : `v${s.delete ?? "?"} of`;
    return `Delete ${what} ${s.template || "?"}`;
  }
  if (c.kind === "template_deprecate") {
    return `${s.action === "revive" ? "Revive" : "Retire"} ${s.template || "?"} v${s.target_version ?? "?"}`;
  }
  return c.kind;
}

/** Whether the signed-in caller is the one to act on `c`. */
function needsMe(c, me) {
  if (c.status !== "pending") return false;
  if (!me) return true;
  if (!can("change_approve")) return false;
  return c.requester !== me.principal && !(c.approvals || []).some((a) => a.principal === me.principal);
}

export async function renderChanges(container, params = {}) {
  const me = whoami();
  const approver = can("change_approve");
  let tab = approver ? "mine" : "all";
  let changes = [];
  let truncated = false;
  let counts = null;
  let selected = params.id || null;
  let alive = true;
  let pollTimer = null;
  let detailGen = 0;

  const pane = splitPane(container, {
    name: "Changes",
    listHead: `
      <div class="sp-tabs">
        ${tabs([{ id: "mine", label: "Needs me", hidden: !approver }, { id: "all", label: "All" }], tab)}
        <span class="sp-tools">
          <button type="button" class="icon-btn" id="c-refresh" title="Refresh" aria-label="Refresh">${icon("refresh")}</button>
        </span>
      </div>
      <div id="c-stats"></div>`,
  });
  const list = sectionList(pane.body, "faucet.changes.sections");

  pane.head.querySelectorAll(".c-tab").forEach((b) => {
    b.onclick = () => {
      tab = b.dataset.tab;
      pane.head.querySelectorAll(".c-tab").forEach((x) => {
        x.classList.toggle("on", x === b);
        x.setAttribute("aria-selected", String(x === b));
      });
      paint();
    };
  });
  pane.head.querySelector("#c-refresh").onclick = () => {
    load();
    if (selected) showDetail(selected, { keep: true });
  };

  let seq = 0;
  async function load() {
    const mine = ++seq;
    const p = new URLSearchParams({ limit: String(LIMIT) });
    withTenant(p);
    const cp = new URLSearchParams();
    withTenant(cp);
    try {
      // Counts are exact over every request; an older server without the
      // endpoint falls back to counting the loaded rows.
      const [rows, totals] = await Promise.all([
        api(`/v1/changes?${p}`),
        api(`/v1/changes/counts?${cp}`).catch(() => null),
      ]);
      if (!alive || mine !== seq) return;
      changes = rows;
      counts = totals ? totals.counts : null;
      truncated = rows.length >= LIMIT;
      paint();
    } catch (e) {
      if (!alive || mine !== seq) return;
      pane.body.innerHTML = `<div class="sp-empty"><b>Could not load change requests.</b><span>${escapeHtml(e.message)}</span></div>`;
      list.invalidate();
    } finally {
      clearTimeout(pollTimer);
      if (alive) pollTimer = setTimeout(load, 15000);
    }
  }

  function paint() {
    const more = counts ? "" : truncated ? "+" : "";
    const n = (sts) =>
      counts ? sts.reduce((t, s) => t + (counts[s] || 0), 0) : changes.filter((c) => sts.includes(c.status)).length;
    pane.head.querySelector("#c-stats").innerHTML = statStrip([
      { label: "Pending", value: `${fmtInt(n(["pending"]))}${more}`, tone: n(["pending"]) ? "warn" : "" },
      { label: "Executed", value: `${fmtInt(n(["executed"]))}${more}`, tone: "ok" },
      { label: "Rejected", value: `${fmtInt(n(["rejected"]))}${more}` },
      { label: "Failed", value: `${fmtInt(n(["failed", "invalidated"]))}${more}`, tone: n(["failed", "invalidated"]) ? "bad" : "" },
    ]);
    const shown = tab === "mine" ? changes.filter((c) => needsMe(c, me)) : changes;
    const now = Date.now();
    const rows = (sts) => shown.filter((c) => sts.includes(c.status)).map((c) => changeRow(c, now));
    const empty =
      tab === "mine"
        ? `<div class="sp-empty"><b>Nothing needs your approval.</b><span>Requests you can approve appear here. <a href="#" data-all>Show all change requests</a>.</span></div>`
        : `<div class="sp-empty"><b>No change requests yet.</b><span>Propose one with the Submit page's “request approval” option, <code>POST /v1/changes</code>, or an agent's <code>propose_run</code>. With <code>--require-approval run</code>, every submitted run lands here first.</span></div>`;
    list.render(
      [
        { id: "pending", label: "Pending", rows: rows(["pending"]) },
        { id: "approved", label: "Approved, executing", rows: rows(["approved"]) },
        { id: "decided", label: "Decided", rows: rows(DECIDED) },
      ],
      { empty },
    );
    if (!selected) showNone();
  }

  pane.body.addEventListener("click", (e) => {
    if (e.target.closest("[data-all]")) {
      e.preventDefault();
      pane.head.querySelector('.c-tab[data-tab="all"]').click();
    }
  });

  function changeRow(c, now) {
    const [tone, shape] = RING[c.status] || ["muted", "full"];
    const approvals = (c.approvals || []).length;
    let lead;
    const meta = [];
    if (c.status === "pending") {
      lead = KIND[c.kind] || c.kind;
      meta.push(`${fmtInt(approvals)} of ${fmtInt(c.required_approvals)} approvals`, `by ${c.requester}`);
    } else {
      lead = c.status;
      if (c.status === "rejected" && c.rejection) meta.push(`by ${c.rejection.principal}: ${c.rejection.reason}`);
      else if (c.error) meta.push(c.error);
      else if (approvals) meta.push(`approved by ${c.approvals.map((a) => a.principal).join(", ")}`);
      meta.push(KIND[c.kind] || c.kind);
    }
    const title = summaryText(c);
    return listRow({
      id: c.id,
      href: `#/changes/${encodeURIComponent(c.id)}`,
      tone,
      shape,
      title,
      tooltip: `${title}${c.reason ? `\n${c.reason}` : ""}\n${c.id}`,
      age: fmtAge(c.created_at, now),
      ageTitle: `requested ${fmtTime(c.created_at)}`,
      metaLead: lead,
      meta: meta.join(" · "),
      selected: c.id === selected,
    });
  }

  function showNone() {
    pane.setSelected(false);
    pane.detail.innerHTML = changes.length
      ? `<div class="sp-none"><b>Select a change request</b><span>Pick one on the left to review its plan, impact and approvals.</span></div>`
      : `<div class="sp-none"><b>Nothing to review</b><span>Change requests appear here when a run or a template change needs approval.</span></div>`;
  }

  let detailTab = "plan";
  async function showDetail(id, { keep = false } = {}) {
    selected = id;
    list.select(id);
    if (!id) {
      showNone();
      return;
    }
    pane.setSelected(true);
    const gen = ++detailGen;
    if (!keep) {
      detailTab = "plan";
      pane.detail.innerHTML = `<div class="sp-loading">loading…</div>`;
    }
    let c;
    try {
      c = await api(`/v1/changes/${encodeURIComponent(id)}`);
    } catch (e) {
      if (gen !== detailGen || !alive) return;
      pane.detail.innerHTML = `<div class="c-dhead"><a class="sp-back" href="#/changes">${icon("back")}Changes</a></div><div class="sp-none"><b>${e.status === 404 ? "Change request not found" : "Could not load this change request"}</b><span>${escapeHtml(e.message)}</span></div>`;
      return;
    }
    if (gen !== detailGen || !alive) return;
    renderDetail(c);
  }

  function renderDetail(c) {
    const plan = c.plan || { rows: [], summary: {} };
    const pending = c.status === "pending";
    const approvals = c.approvals || [];
    const impactRows = (plan.rows || []).filter((r) => r.impact || r.policy);
    const title = summaryText(c);
    setCrumbTitle(title);
    const status = pending
      ? `Pending · ${fmtInt(approvals.length)} of ${fmtInt(c.required_approvals)} approvals`
      : c.status[0].toUpperCase() + c.status.slice(1);
    const when = pending
      ? `<span title="${escapeHtml(fmtTime(c.expires_at))}">expires in ${escapeHtml(fmtIn(c.expires_at))}</span>`
      : `<span title="${escapeHtml(fmtTime(c.updated_at))}">decided ${escapeHtml(fmtAge(c.updated_at))} ago</span>`;
    const sub = [
      pill(status, TONE[c.status] || ""),
      `<span>requested by <b>${escapeHtml(c.requester)}</b> · ${escapeHtml(c.requester_role)}</span>`,
      `<span title="${escapeHtml(fmtTime(c.created_at))}">${escapeHtml(fmtAge(c.created_at))} ago</span>`,
      when,
      c.tenant ? `<span>tenant <b>${escapeHtml(c.tenant)}</b></span>` : "",
    ].filter(Boolean);

    const outcome = c.run_id
      ? `<div class="c-alert ok">Started run <a href="#/runs/${encodeURIComponent(c.run_id)}"><code>${escapeHtml(c.run_id)}</code></a></div>`
      : c.template
        ? `<div class="c-alert ok">Template <a href="#/templates/${encodeURIComponent(c.template.id)}"><code>${escapeHtml(c.template.id)}</code></a> v${escapeHtml(String(c.template.version))}</div>`
        : "";
    const alerts = [
      c.error ? `<div class="c-alert bad"><b>${c.status === "invalidated" ? "Approval invalidated" : "Execution failed"}</b><pre>${escapeHtml(c.error)}</pre></div>` : "",
      c.rejection ? `<div class="c-alert bad"><b>Rejected by ${escapeHtml(c.rejection.principal)}</b> ${escapeHtml(fmtAge(c.rejection.at))} ago — ${escapeHtml(c.rejection.reason)}</div>` : "",
      outcome,
    ].join("");

    const { lines, verdict, verdictTone } = planDiff(c);
    const planCard = card({
      title: c.kind === "run" ? "What changes if this runs" : "What this changes",
      iconName: "changes",
      tag: pending ? "re-planned on approve" : "",
      right: plan.material ? `material ${plan.material.slice(0, 8)}…` : "",
      html: lines.length ? diffBlock(lines) : `<div class="c-card-empty">No row plan for this kind.</div>`,
      footnote: verdict,
      footTone: verdictTone,
    });

    const votes = approvals
      .map(
        (a) =>
          `<div class="c-vote"><span class="sh-avatar">${escapeHtml(initials(a.principal))}</span><span class="c-ell" title="${escapeHtml(a.principal)}">${escapeHtml(a.principal)}</span><span class="c-muted">${escapeHtml(a.role)}</span><span class="ok">${icon("check")}</span><span class="c-vote-r" title="${escapeHtml(fmtTime(a.at))}${a.comment ? ` — ${escapeHtml(a.comment)}` : ""}">${escapeHtml(fmtAge(a.at))} ago${a.comment ? ` · “${escapeHtml(a.comment)}”` : ""}</span></div>`,
      )
      .join("");
    const waiting = pending
      ? Array.from({ length: Math.max(0, c.required_approvals - approvals.length) }, () => `<div class="c-vote"><span class="sh-avatar c-avatar-empty"></span><span class="c-muted">waiting for an approver</span></div>`).join("")
      : "";
    const approvalsCard = `<section class="c-card"><div class="c-card-h"><b>Approvals</b><span class="c-card-r">${fmtInt(approvals.length)} of ${fmtInt(c.required_approvals)} needed</span></div>${votes || waiting ? votes + waiting : `<div class="c-card-empty">No approvals.</div>`}</section>`;
    const budget = Object.entries(c.budget || {}).filter(([, v]) => v != null && !(Array.isArray(v) && !v.length));
    const budgetCard = `<section class="c-card"><div class="c-card-h"><b>Budget</b><span class="c-card-r">ceilings the run must honour</span></div>${
      budget.length
        ? kvGrid(budget.map(([k, v]) => ({ k: k.replace(/_/g, " "), v: Array.isArray(v) ? v.join(", ") : typeof v === "number" ? v : String(v) })), 2)
        : `<div class="c-card-empty">No budget beyond the config's own.</div>`
    }</section>`;
    const decide = pending
      ? `<section class="c-card" data-perm="change_approve"><div class="c-card-h"><b>Your decision</b><span class="c-card-aside">the approval policy decides who may approve</span></div><div class="chg-decide"><label><span>Comment <span class="c-muted">· sent with an approval</span></span><input id="cd-comment" placeholder="optional" /></label><label><span>Reason <span class="c-muted">· required to reject</span></span><input id="cd-reason" placeholder="why this should not run" /></label></div></section>`
      : "";
    const reason = c.reason ? `<div class="c-quote">${escapeHtml(c.reason)}</div>` : "";

    pane.detail.innerHTML = `
      <div class="c-dhead">
        <a class="sp-back" href="#/changes">${icon("back")}Changes</a>
        ${tabs([{ id: "plan", label: "Plan" }, { id: "impact", label: "Impact", count: impactRows.length || null }, { id: "history", label: "History" }], detailTab)}
        <span class="c-dhead-r">${
          pending
            ? `<button class="c-btn danger" id="cd-reject" data-perm="change_approve">${icon("x")}Reject</button><button class="c-btn primary" id="cd-approve" data-perm="change_approve">${icon("check")}${c.required_approvals - approvals.length <= 1 ? (c.kind === "run" ? "Approve and run" : "Approve and apply") : "Approve"}</button>`
            : ""
        }</span>
      </div>
      <div><h1 class="c-title" title="${escapeHtml(title)}">${escapeHtml(title)}</h1><div class="c-sub">${sub.join('<span class="c-sub-sep">·</span>')}</div></div>
      <div class="d-pane" data-pane="plan">${alerts}${reason}${planCard}<div class="c-two">${approvalsCard}${budgetCard}</div>${decide}</div>
      <div class="d-pane" data-pane="impact" hidden>${impactPane(impactRows)}</div>
      <div class="d-pane" data-pane="history" hidden>${historyPane(c)}</div>`;

    const showTab = (t) => {
      detailTab = t;
      pane.detail.querySelectorAll(".c-dhead .c-tab").forEach((b) => {
        b.classList.toggle("on", b.dataset.tab === t);
        b.setAttribute("aria-selected", String(b.dataset.tab === t));
      });
      pane.detail.querySelectorAll(".d-pane").forEach((p) => (p.hidden = p.dataset.pane !== t));
    };
    pane.detail.querySelectorAll(".c-dhead .c-tab").forEach((b) => (b.onclick = () => showTab(b.dataset.tab)));
    showTab(detailTab);

    const approve = pane.detail.querySelector("#cd-approve");
    if (approve) {
      approve.onclick = async () => {
        const comment = pane.detail.querySelector("#cd-comment")?.value.trim();
        approve.disabled = true;
        try {
          const r = await api(`/v1/changes/${encodeURIComponent(c.id)}/approve`, {
            method: "POST",
            body: comment ? { comment } : {},
          });
          toast(r.status === "pending" ? `approved — ${r.approvals.length} / ${r.required_approvals}` : `change ${r.status}`, r.status === "failed" || r.status === "invalidated" ? "error" : "info");
          afterDecision(c.id);
        } catch (e) {
          toast(e.message, "error");
          approve.disabled = false;
        }
      };
      pane.detail.querySelector("#cd-reject").onclick = async () => {
        const input = pane.detail.querySelector("#cd-reason");
        const why = input?.value.trim();
        if (!why) {
          toast("a rejection needs a reason", "error");
          showTab("plan");
          input?.focus();
          return;
        }
        try {
          await api(`/v1/changes/${encodeURIComponent(c.id)}/reject`, { method: "POST", body: { reason: why } });
          toast("change rejected");
          afterDecision(c.id);
        } catch (e) {
          toast(e.message, "error");
        }
      };
    }
  }

  async function afterDecision(id) {
    showDetail(id, { keep: true });
    await load();
    revealSelected();
    refreshBadges(true);
  }

  // A linked request the "Needs me" tab does not list opens on "All".
  function revealSelected() {
    if (tab !== "mine" || !selected) return;
    const c = changes.find((x) => x.id === selected);
    if (c && !needsMe(c, me)) pane.head.querySelector('.c-tab[data-tab="all"]').click();
  }

  await load();
  if (!alive) return;
  revealSelected();
  if (selected) showDetail(selected);

  return {
    teardown() {
      alive = false;
      clearTimeout(pollTimer);
    },
    update(p) {
      const id = p.id || null;
      if (id === selected) return;
      selected = id;
      revealSelected();
      showDetail(id);
    },
  };
}

/** The plan as diff lines, plus a one-line verdict for the card's footnote. */
function planDiff(c) {
  const plan = c.plan || { rows: [], summary: {} };
  const s = plan.summary || {};
  const lines = [];
  if (c.kind === "template_launch") {
    lines.push({ op: " ", text: `template ${s.template || "?"}` });
    if (s.stable_before != null) lines.push({ op: "-", text: `stable: v${s.stable_before}` });
    lines.push({ op: "+", text: `stable: v${s.target_version ?? "?"}` });
    if (s.stable_before == null) lines.push({ op: " ", text: "  nothing launched yet: unpinned triggers start resolving this version" });
  } else if (c.kind === "template_delete") {
    lines.push({ op: " ", text: `template ${s.template || "?"}` });
    lines.push({ op: "-", text: s.delete === "all" ? "every version" : `v${s.delete ?? "?"}` });
  } else if (c.kind === "template_deprecate") {
    lines.push({ op: " ", text: `template ${s.template || "?"} v${s.target_version ?? "?"}` });
    lines.push(s.action === "revive" ? { op: "-", text: "deprecated" } : { op: "+", text: "deprecated" });
  } else if (c.kind === "template_register") {
    lines.push({ op: " ", text: `template ${s.template || "?"} (${s.template_kind || "template"})` });
    lines.push({ op: "+", text: `v${s.previous_version != null ? s.previous_version + 1 : 1}${s.launch ? " · launched as stable" : ""}` });
  }
  for (const r of plan.rows || []) {
    lines.push({ op: " ", text: `row ${r.row || "?"} · ${r.source || "?"} → ${r.sink || "?"}` });
    if (r.write_mode) lines.push({ op: "+", text: `write_mode: ${r.write_mode}` });
    if (r.delivery_guarantee) lines.push({ op: "+", text: `delivery: ${r.delivery_guarantee}` });
    if ((r.transforms || []).length) lines.push({ op: "+", text: `transforms: ${r.transforms.join(", ")}` });
    if (r.schema_drift) lines.push({ op: "+", text: `schema drift: ${r.schema_drift}` });
    const gov = [r.quality && "quality", r.contract && "contract", r.masking && "masking"].filter(Boolean);
    lines.push({ op: " ", text: `  ${gov.length ? gov.join(" · ") : "no quality / contract / masking passes"}` });
  }
  for (const [k, v] of Object.entries(c.budget || {})) {
    if (v == null || (Array.isArray(v) && !v.length)) continue;
    lines.push({ op: "+", text: `budget.${k}: ${Array.isArray(v) ? v.join(", ") : typeof v === "number" ? fmtInt(v) : v}` });
  }
  const rows = plan.rows || [];
  const breaking = rows.flatMap((r) => (r.impact && r.impact.severity === "breaking" ? r.impact.affected || [] : []));
  const violations = rows.reduce((n, r) => n + ((r.policy && r.policy.violations) || []).length, 0);
  let verdict = "";
  let verdictTone = "";
  if (violations) {
    verdict = `${fmtInt(violations)} policy violation${violations === 1 ? "" : "s"} · see Impact`;
    verdictTone = "bad";
  } else if (breaking.length) {
    const ids = [...new Set(breaking.filter((a) => a.depth > 0).map((a) => a.uri || a.id))];
    verdict = ids.length
      ? `Breaking for ${fmtInt(ids.length)} downstream dataset${ids.length === 1 ? "" : "s"}: ${ids.slice(0, 3).join(", ")}${ids.length > 3 ? "…" : ""}`
      : "Breaking schema change at the row's own sink";
    verdictTone = "warn";
  } else if (rows.some((r) => r.impact && r.impact.severity === "unknown")) {
    verdict = "Downstream impact unknown: a dataset on the path records no column lineage";
  } else if (rows.some((r) => r.impact && r.impact.severity === "additive")) {
    verdict = "Additive schema change only: nothing downstream breaks";
    verdictTone = "ok";
  } else if (rows.some((r) => r.impact)) {
    verdict = "No schema change reaches a downstream dataset";
    verdictTone = "ok";
  }
  return { lines, verdict, verdictTone };
}

function impactPane(rows) {
  if (!rows.length) {
    return `<div class="sp-none"><b>No impact or policy analysis</b><span>Impact analysis needs a <code>catalog:</code> block and a recorded run of the row; a policy verdict needs a <code>policy:</code> in force.</span></div>`;
  }
  return rows
    .map((r) => {
      const parts = [];
      if (r.impact) {
        const i = r.impact;
        const delta = i.delta || {};
        const items = [
          { k: "Severity", v: i.severity },
          { k: "Planned from", v: i.planned_from },
          { k: "Columns added / removed", v: `${fmtInt((delta.added || []).length)} / ${fmtInt((delta.removed || []).length)}` },
          { k: "Retyped / renamed", v: `${fmtInt((delta.retyped || []).length)} / ${fmtInt((delta.renamed || []).length)}` },
        ];
        const affected = (i.affected || [])
          .map(
            (a) =>
              `<tr><td class="c-ell" title="${escapeHtml(a.uri || a.id)}">${escapeHtml(a.uri || a.id)}</td><td class="num">${fmtInt(a.depth)}</td><td>${pill(a.severity, a.severity === "breaking" ? "bad" : a.severity === "additive" ? "ok" : "")}${a.opaque ? ' <span class="c-muted">opaque</span>' : ""}</td><td class="c-ell">${escapeHtml((a.owners || []).join(", ") || "—")}</td></tr>`,
          )
          .join("");
        parts.push(
          kvGrid(items) +
            (affected
              ? `<table class="c-tbl"><colgroup><col /><col style="width:72px" /><col style="width:130px" /><col style="width:150px" /></colgroup><thead><tr><th>Affected dataset</th><th class="num">Depth</th><th>Severity</th><th>Owners</th></tr></thead><tbody>${affected}</tbody></table>`
              : "") +
            ((i.notes || []).length ? `<div class="c-card-note">${(i.notes || []).map(escapeHtml).join(" · ")}</div>` : ""),
        );
      }
      if (r.policy) {
        const v = r.policy.violations || [];
        parts.push(
          v.length
            ? `<table class="c-tbl"><thead><tr><th>Policy rule</th><th>Column</th><th>Label</th><th>Sink</th></tr></thead><tbody>${v
                .map((x) => `<tr><td>${escapeHtml(x.rule)}</td><td class="mono">${escapeHtml(x.column)}</td><td>${escapeHtml(x.label)}</td><td>${escapeHtml(x.sink)} (${escapeHtml(x.sink_kind)})</td></tr>`)
                .join("")}</tbody></table>`
            : `<div class="c-card-note ok">Policy: no violations at this row's sink</div>`,
        );
      }
      const sev = r.impact ? r.impact.severity : "";
      return `<section class="c-card"><div class="c-card-h"><b>row ${escapeHtml(r.row || "?")}</b><span class="c-card-aside">${escapeHtml(r.source || "")} → ${escapeHtml(r.sink || "")}</span>${
        sev ? `<span class="c-card-r">${pill(sev, sev === "breaking" ? "bad" : sev === "additive" ? "ok" : "")}</span>` : ""
      }</div>${parts.join("")}</section>`;
    })
    .join("");
}

function historyPane(c) {
  const ev = [{ at: c.created_at, text: `requested by <b>${escapeHtml(c.requester)}</b> (${escapeHtml(c.requester_role)})` }];
  for (const a of c.approvals || []) {
    ev.push({ at: a.at, text: `approved by <b>${escapeHtml(a.principal)}</b> (${escapeHtml(a.role)})${a.comment ? ` — “${escapeHtml(a.comment)}”` : ""}` });
  }
  if (c.rejection) ev.push({ at: c.rejection.at, text: `rejected by <b>${escapeHtml(c.rejection.principal)}</b> — ${escapeHtml(c.rejection.reason)}` });
  if (c.status !== "pending" && c.status !== "rejected") ev.push({ at: c.updated_at, text: `<b>${escapeHtml(c.status)}</b>${c.run_id ? ` · run <a href="#/runs/${encodeURIComponent(c.run_id)}"><code>${escapeHtml(c.run_id)}</code></a>` : ""}` });
  if (c.status === "pending") ev.push({ at: c.expires_at, text: "lapses if not decided", future: true });
  ev.sort((a, b) => Date.parse(a.at) - Date.parse(b.at));
  return `<section class="c-card"><div class="c-card-h"><b>History</b><span class="c-card-r">id <span class="mono">${escapeHtml(c.id)}</span></span></div><div class="c-log c-timeline">${ev
    .map((e) => `<div${e.future ? ' class="c-muted"' : ""}><span class="c-log-ts" title="${escapeHtml(fmtTime(e.at))}">${escapeHtml(fmtTime(e.at))}</span>&nbsp; ${e.text}</div>`)
    .join("")}</div></section>`;
}

function fmtIn(iso) {
  const ms = Date.parse(iso) - Date.now();
  if (!(ms > 0)) return "0s";
  return fmtAge(new Date(Date.now() - ms).toISOString());
}

function initials(name) {
  const parts = String(name || "?").split(/[^A-Za-z0-9]+/).filter(Boolean);
  const two = parts.length > 1 ? parts[0][0] + parts[1][0] : (parts[0] || "?").slice(0, 2);
  return two.toUpperCase();
}

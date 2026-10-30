// Changes (#998, #703): split pane — change requests waiting for approval
// (plan → approve → run) beside the selected one's plan, impact and history.
// Approve / reject are gated on `change_approve`; the server's approval policy
// still decides (a 403 names the rule).
import { api } from "../api.js";
import { html, raw, v, ic, row, ring, sect, tabs, ibtn, empty, skelRows, skelCards, head, stats, card, tbl, pill, chip, kv, grid, btn, dhead, dtabs, lnk, join, diff, alert, cpi, field, select, input, meta, dotText } from "../components.js";
import { layout, setQuery, patch, errorAlert, hashFor, poll, ago, dialog, toast, sections } from "../page.js";
import { setCrumbs, setDelta, setContext, refreshBadges } from "../shell.js";
import { fmtInt, fmtAge } from "../utils.js";
import { formatTs, getTz } from "../tz.js";
import { href } from "../links.js";
import { withTenant } from "../tenant.js";
import { whoami, can } from "../access.js";

const LIMIT = 500;
const DECIDED = ["executed", "rejected", "failed", "invalidated", "expired"];
const RING = { pending: ["warn", "hollow"], approved: ["info", "half"], executed: ["ok", ""], rejected: ["bad", ""], failed: ["bad", ""], invalidated: ["bad", ""], expired: ["", "hollow"] };
const TONE = { pending: "warn", approved: "info", executed: "ok", rejected: "bad", failed: "bad", invalidated: "bad", expired: "" };
const PILL_ICON = { executed: "check", rejected: "x", failed: "x", invalidated: "x", approved: "play" };
const KIND = { run: "run", template_register: "template register", template_launch: "template launch", template_delete: "template delete", template_deprecate: "template deprecate" };
/** "today 07:12", "tomorrow 07:12" or "28 Oct 07:12" in the display timezone. */
function shortTime(iso) {
  const d = new Date(iso);
  if (isNaN(d)) return "";
  const tz = getTz() === "local" ? {} : { timeZone: getTz() };
  const day = (x) => new Intl.DateTimeFormat("en-CA", { ...tz, year: "numeric", month: "2-digit", day: "2-digit" }).format(x);
  const hm = new Intl.DateTimeFormat("en-GB", { ...tz, hour: "2-digit", minute: "2-digit" }).format(d);
  const diffDays = Math.round((Date.parse(day(d)) - Date.parse(day(new Date()))) / 86400000);
  const lead = diffDays === 0 ? "today" : diffDays === -1 ? "yesterday" : diffDays === 1 ? "tomorrow" : new Intl.DateTimeFormat("en-GB", { ...tz, day: "numeric", month: "short" }).format(d);
  return `${lead} ${hm}`;
}
const cap = (s) => (s ? s[0].toUpperCase() + s.slice(1) : s);

/** One line saying what a change proposes. */
export function summaryText(c) {
  const s = (c.plan && c.plan.summary) || {};
  if (c.kind === "run") {
    const sinks = Array.isArray(s.sinks) ? s.sinks.join(", ") : "";
    return `Run ${s.pipeline || "(unnamed)"}${sinks ? ` → ${sinks}` : ""}`;
  }
  if (c.kind === "template_register") return `Register ${s.template || "?"} v${s.previous_version != null ? s.previous_version + 1 : 1}${s.launch ? " and launch" : ""}`;
  if (c.kind === "template_launch") return `Launch ${s.template || "?"} v${s.target_version ?? "?"}`;
  if (c.kind === "template_delete") return `Delete ${s.delete === "all" ? "every version of" : `v${s.delete ?? "?"} of`} ${s.template || "?"}`;
  if (c.kind === "template_deprecate") return `${s.action === "revive" ? "Revive" : "Deprecate"} ${s.template || "?"} v${s.target_version ?? "?"}`;
  return c.kind;
}

/** Whether the signed-in caller is the one to act on `c`. */
function needsMe(c, me) {
  if (c.status !== "pending" || !can("change_approve")) return false;
  if (!me) return true;
  return c.requester !== me.principal && !(c.approvals || []).some((a) => a.principal === me.principal);
}
function fmtIn(iso) {
  const ms = Date.parse(iso) - Date.now();
  if (!(ms > 0)) return "now";
  return fmtAge(new Date(Date.now() - ms).toISOString());
}
/** A principal: a link to its audit entries for whoever may read them. */
const who = (p) => (can("audit_read") ? lnk(p, href.audit({ principal: p })) : html`<b>${p}</b>`);
const datasetLink = (a) => lnk(a.uri || a.id, a.id ? href.dataset(a.id) : "");
function initials(name) {
  const parts = String(name || "?").split(/[^A-Za-z0-9]+/).filter(Boolean);
  return (parts.length > 1 ? parts[0][0] + parts[1][0] : (parts[0] || "?").slice(0, 2)).toUpperCase();
}
const quorumLabel = (c) => {
  const left = (c.required_approvals || 1) - (c.approvals || []).length;
  return left <= 1 ? (c.kind === "run" ? "Approve and run" : "Approve and apply") : "Approve";
};

/** The plan as diff lines, plus a verdict for the card's footnote. */
function planDiff(c) {
  const plan = c.plan || { rows: [], summary: {} };
  const s = plan.summary || {};
  const lines = [];
  if (c.kind === "template_launch") {
    lines.push([" ", `template ${s.template || "?"}`]);
    if (s.stable_before != null) lines.push(["-", `stable: v${s.stable_before}`]);
    lines.push(["+", `stable: v${s.target_version ?? "?"}`]);
    if (s.stable_before == null) lines.push([" ", "  nothing launched yet: unpinned triggers start resolving this version"]);
  } else if (c.kind === "template_delete") {
    lines.push([" ", `template ${s.template || "?"}`]);
    lines.push(["-", s.delete === "all" ? "every version" : `v${s.delete ?? "?"}`]);
  } else if (c.kind === "template_deprecate") {
    lines.push([" ", `template ${s.template || "?"} v${s.target_version ?? "?"}`]);
    lines.push(s.action === "revive" ? ["-", "deprecated"] : ["+", "deprecated"]);
  } else if (c.kind === "template_register") {
    lines.push([" ", `template ${s.template || "?"} (${s.template_kind || "template"})`]);
    lines.push(["+", `v${s.previous_version != null ? s.previous_version + 1 : 1}${s.launch ? " · launched as stable" : ""}`]);
  }
  for (const r of plan.rows || []) {
    lines.push([" ", `row ${r.row || "?"} · ${r.source || "?"} → ${r.sink || "?"}`]);
    if (r.write_mode) lines.push(["+", `write_mode: ${r.write_mode}`]);
    if (r.delivery_guarantee) lines.push(["+", `delivery: ${r.delivery_guarantee}`]);
    if ((r.transforms || []).length) lines.push(["+", `transforms: ${r.transforms.join(", ")}`]);
    if (r.schema_drift) lines.push(["+", `schema drift: ${r.schema_drift}`]);
    const gov = [r.quality && "quality", r.contract && "contract", r.masking && "masking"].filter(Boolean);
    lines.push([" ", gov.length ? gov.join(" · ") : "no quality, contract or masking passes"]);
  }
  for (const [k, x] of Object.entries(c.budget || {})) {
    if (x == null || (Array.isArray(x) && !x.length)) continue;
    lines.push(["+", `budget.${k}: ${Array.isArray(x) ? x.join(", ") : x}`]);
  }
  const rows = plan.rows || [];
  const breaking = rows.flatMap((r) => (r.impact && r.impact.severity === "breaking" ? (r.impact.affected || []).filter((a) => a.depth > 0) : []));
  const violations = rows.reduce((n, r) => n + ((r.policy && r.policy.violations) || []).length, 0);
  let note = "";
  if (violations) note = html`<span class="bad">${fmtInt(violations)} policy violation${violations === 1 ? "" : "s"}</span> · approval is blocked until the plan changes. <a class="lnk" href="#" data-dtab="Impact">See impact</a>`;
  else if (breaking.length) {
    const uniq = [...new Map(breaking.map((a) => [a.id || a.uri, a])).values()];
    note = html`<span class="warn">Breaking for ${fmtInt(uniq.length)} downstream dataset${uniq.length === 1 ? "" : "s"}</span> · ${join(uniq.slice(0, 3).map(datasetLink), " and ")}${uniq.length > 3 ? "…" : ""} read columns this change alters. <a class="lnk" href="#" data-dtab="Impact">See impact</a>`;
  } else if (rows.some((r) => r.impact && r.impact.planned_from === "unknown")) note = html`<span class="warn">Downstream impact unknown</span> · this row has no recorded run in the catalog yet. <a class="lnk" href="#" data-dtab="Impact">See impact</a>`;
  else if (rows.some((r) => r.impact && r.impact.severity === "unknown")) note = html`<span class="warn">Downstream impact unknown</span> · a dataset on the path records no column lineage.`;
  else if (rows.some((r) => r.impact && r.impact.severity === "additive")) note = html`<span class="ok">Additive only</span> · nothing downstream breaks.`;
  else if (rows.some((r) => r.impact)) note = html`<span class="ok">No schema change</span> reaches a downstream dataset.`;
  return { lines, note };
}

export async function renderChanges(host, params) {
  const L = layout(host, "split");
  const me = whoami();
  const approver = can("change_approve");
  const defView = approver ? "mine" : "all";
  const secs = sections("faucet.changes.sections");
  let changes = null;
  let counts = null;
  let truncated = false;
  let loadErr = null;
  const read = (p) => ({
    id: p.id || "",
    view: p.query.get("view") || defView,
    dtab: p.query.get("tab") || "Plan",
    filters: p.query.get("filters") === "1",
    kind: p.query.get("kind") || "",
    requester: p.query.get("requester") || "",
  });
  let state = read(params);
  let current = null;
  let currentErr = null;
  let detailGen = 0;

  const listQuery = () => ({ view: state.view !== defView ? state.view : "", kind: state.kind, requester: state.requester, filters: state.filters ? "1" : "" });
  const listHref = () => hashFor(href.changes(), listQuery());
  const rowHref = (cid) => hashFor(href.change(cid), listQuery());
  const filtered = () => (changes || []).filter((c) => (!state.kind || c.kind === state.kind) && (!state.requester || c.requester === state.requester));
  const shown = () => (state.view === "mine" ? filtered().filter((c) => needsMe(c, me)) : filtered());

  function changeRow(c) {
    const [tone, shape] = RING[c.status] || ["", ""];
    const n = (c.approvals || []).length;
    const s = (c.plan && c.plan.summary) || {};
    let facts;
    if (c.status === "pending") {
      const wm = c.kind === "run" ? [...new Set(((c.plan && c.plan.rows) || []).map((r) => r.write_mode).filter(Boolean))] : [];
      facts = [KIND[c.kind] || c.kind, wm.length ? `write_mode ${wm.join(", ")}` : "", c.kind === "run" && s.rows > 1 ? `${fmtInt(s.rows)} rows` : "", `${n} of ${c.required_approvals}${c.required_approvals === 1 ? "" : " approvals"}`, `by ${c.requester}`];
    } else {
      const why = c.status === "rejected" && c.rejection ? c.rejection.reason : c.error ? c.error.split("\n")[0] : n ? `approved by ${c.approvals.map((a) => a.principal).join(", ")}` : c.status === "expired" ? "nobody approved in time" : "";
      facts = [c.status === "approved" ? "executing" : c.status, why];
    }
    const title = summaryText(c);
    return row(ring(tone, shape), title, html`<time datetime="${c.created_at}" title="requested ${formatTs(c.created_at)}">${fmtAge(c.created_at)}</time>`, meta(facts), { on: c.id === state.id, href: rowHref(c.id), id: c.id, tip: `${title}${c.reason ? `\n${c.reason}` : ""}` });
  }

  function renderList() {
    if (!changes) {
      L.list.innerHTML = v(loadErr ? html`${tabs([["All"]], "All")}${errorAlert("change requests", loadErr)}` : html`${tabs([["Needs me"], "All"], "Needs me")}${stats([["Pending", "–"], ["Executed", "–"], ["Rejected", "–"], ["Failed", "–"]])}${skelRows(7)}`);
      L.list.querySelector("[data-retry]")?.addEventListener("click", load);
      return;
    }
    const more = counts ? "" : truncated ? "+" : "";
    const n = (sts) => (counts ? sts.reduce((t, s) => t + (counts[s] || 0), 0) : changes.filter((c) => sts.includes(c.status)).length);
    const mine = filtered().filter((c) => needsMe(c, me));
    const tabList = [approver ? ["Needs me", mine.length, "mine"] : null, ["All", null, "all"]].filter(Boolean);
    const anyFilter = !!(state.kind || state.requester);
    const tools = html`${ibtn("filter", { title: "Filter by kind or requester", on: state.filters, dot: anyFilter, data: { filters: "1" } })}${ibtn("refresh", { title: "Refresh", data: { reload: "1" } })}`;
    const list = shown();
    const group = (sid, label, sts) => {
      const rows = list.filter((c) => sts.includes(c.status));
      if (!rows.length) return "";
      const closed = secs.isClosed(sid);
      return html`${sect(label, rows.length, { id: sid, closed })}<div class="rows sect-rows${closed ? " closed" : ""}">${rows.map(changeRow)}</div>`;
    };
    const kinds = [...new Set(changes.map((c) => c.kind))];
    const requesters = [...new Set(changes.map((c) => c.requester).filter(Boolean))].sort();
    const filterBox = state.filters ? html`<div class="filters"><div class="r2">${select([["", "Any kind"], ...kinds.map((k) => [k, KIND[k] || k])], state.kind, { name: "kind" })}${select([["", "Anyone"], ...requesters.map((r) => [r, r])], state.requester, { name: "requester" })}</div></div>` : "";
    const active = anyFilter ? html`<div class="active-f">${state.kind ? chip(`kind: ${KIND[state.kind] || state.kind}`, "x", { "data-clear": "kind", title: "Remove this filter" }) : ""}${state.requester ? chip(`requester: ${state.requester}`, "x", { "data-clear": "requester", title: "Remove this filter" }) : ""}<a class="lnk" href="#" data-clear="all" style="font-size:12px;margin-left:4px">clear</a></div>` : "";
    let body;
    if (!list.length && state.view === "mine") {
      const decided = filtered().filter((c) => DECIDED.includes(c.status)).slice(0, 3);
      body = html`${sect("Pending", 0, { id: "pending" })}${empty("Nothing needs you", html`Every pending change already has your vote, or you requested it yourself. <a class="lnk" href="${hashFor(href.changes(), { view: "all" })}">Show all changes</a>`, { icon: "check" })}${decided.length ? html`${sect("Recently decided", n(DECIDED), { id: "decided" })}<div class="rows sect-rows">${decided.map(changeRow)}</div>` : ""}`;
    } else if (!list.length) {
      body = anyFilter ? empty("No change request matches", html`<a class="lnk" href="#" data-clear="all">Clear the filters</a>`, { icon: "filter" }) : empty("No change requests yet", html`Propose one from Submit with “Request approval”. With <code>--require-approval run</code>, every submitted run lands here first.`, { icon: "changes" });
    } else {
      body = html`${group("pending", "Pending", ["pending"])}${group("approved", "Approved · executing", ["approved"])}${group("decided", "Recently decided", DECIDED)}`;
    }
    const top = html`${tabs(tabList, state.view, tools)}${stats([["Pending", `${fmtInt(n(["pending"]))}${more}`, n(["pending"]) ? "warn" : ""], ["Executed", `${fmtInt(n(["executed"]))}${more}`, "ok"], ["Rejected", `${fmtInt(n(["rejected"]))}${more}`], ["Failed", `${fmtInt(n(["failed", "invalidated"]))}${more}`, n(["failed", "invalidated"]) ? "bad" : ""]])}${filterBox}${active}`;
    if (!L.list.querySelector(".list-body")) L.list.innerHTML = '<div class="list-top" style="display:contents"></div><div class="list-body rows"></div>';
    patch(L.list.querySelector(".list-top"), top);
    patch(L.list.querySelector(".list-body"), body);
  }

  L.list.addEventListener("click", (e) => {
    const t = e.target.closest("[data-tab]");
    if (t) return setQuery({ view: t.dataset.tab === defView ? "" : t.dataset.tab });
    if (e.target.closest("[data-reload]")) { load(); if (state.id) loadDetail(true); return; }
    if (e.target.closest("[data-filters]")) return setQuery({ filters: state.filters ? "" : "1" }, { replace: true });
    const clr = e.target.closest("[data-clear]");
    if (clr) {
      e.preventDefault();
      setQuery(clr.dataset.clear === "all" ? { kind: "", requester: "" } : { [clr.dataset.clear]: "" }, { replace: true });
    }
  });
  L.list.addEventListener("change", (e) => {
    if (e.target.name === "kind" || e.target.name === "requester") setQuery({ [e.target.name]: e.target.value }, { replace: true });
  });
  L.list.addEventListener("sect-toggle", (e) => secs.toggle(e.detail.id));

  // ---- detail ----
  function noneDetail() {
    setCrumbs(["Operate", "Changes"]);
    setContext("");
    if (!changes) { setDelta(""); return html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(2)}`; }
    const mine = filtered().filter((c) => needsMe(c, me)).sort((a, b) => Date.parse(a.created_at) - Date.parse(b.created_at));
    setDelta(mine.length ? html`<span class="warn">${mine.length} wait${mine.length === 1 ? "s" : ""} for you</span>` : "");
    if (!mine.length) {
      return empty("No change requests wait for you", html`When someone proposes a run or a template launch under <code>--require-approval</code>, it appears here for a vote. Propose one from Submit with “Request approval”.`, { icon: "changes", btns: html`${btn("Open Submit", { icon: "submit", href: href.submit(), perm: "config_submit" })}${state.view === "mine" ? btn("Show all changes", { href: hashFor(href.changes(), { view: "all" }) }) : ""}` });
    }
    return html`${head(`${mine.length} change${mine.length === 1 ? "" : "s"} wait${mine.length === 1 ? "s" : ""} for your vote`, [html`<span>oldest ${fmtAge(mine[0].created_at)} ago</span>`, html`<span>approval rules from the auth config</span>`])}
      ${card({ title: "Waiting for you", right: "oldest first", body: tbl(["Change", "Kind", "Approvals", "Requested", ["Expires", "num"]], mine.map((c) => ({ href: rowHref(c.id), cells: [lnk(summaryText(c), rowHref(c.id)), KIND[c.kind] || c.kind, `${(c.approvals || []).length} of ${c.required_approvals}`, html`${who(c.requester)} · ${ago(c.created_at, " ago")}`, html`<span title="${formatTs(c.expires_at)}">${fmtIn(c.expires_at)}</span>`] }))) })}
      <div class="muted" style="font-size:12px">Pick a change to see its plan. <kbd>j</kbd> <kbd>k</kbd> move · <kbd>↵</kbd> open</div>`;
  }

  function approvalsCard(c) {
    const votes = (c.approvals || []).map((a, i) => html`<div class="vote"${raw(i === 0 ? ' style="border-top:0"' : "")}><span class="avatar">${initials(a.principal)}</span>${who(a.principal)}<span class="muted">${a.role}</span><span class="ok">${ic("check")}</span><span class="r" title="${formatTs(a.at)}">${fmtAge(a.at)} ago${a.comment ? ` · “${a.comment}”` : ""}</span></div>`);
    const waitN = c.status === "pending" ? Math.max(0, (c.required_approvals || 1) - (c.approvals || []).length) : 0;
    const waiting = Array.from({ length: waitN }, (_, i) => html`<div class="vote"${raw(!votes.length && i === 0 ? ' style="border-top:0"' : "")}><span class="avatar vacant"></span><span class="muted">waiting for ${votes.length || i ? "another" : "an"} approver</span></div>`);
    const body = votes.length || waiting.length ? html`${votes}${waiting}` : html`<div class="empty-line">No approvals.</div>`;
    return card({ title: "Approvals", right: `${(c.approvals || []).length} of ${c.required_approvals} needed`, body });
  }
  function budgetCard(c) {
    const b = c.budget || {};
    const has = (k) => b[k] != null && !(Array.isArray(b[k]) && !b[k].length);
    const dur = (s) => (s >= 3600 ? `${Math.round(s / 360) / 10}h` : s >= 60 ? `${Math.round(s / 60)}m` : `${s}s`);
    const any = Object.keys(b).some(has);
    return card({ title: "Budget attached", right: "stricter wins", body: any ? grid(2, [kv("Max records", has("max_records") ? fmtInt(b.max_records) : "—"), kv("Max duration", has("max_duration_secs") ? dur(b.max_duration_secs) : "—"), kv("Allowed sinks", has("allowed_sinks") ? html`<span class="mono">${b.allowed_sinks.join(", ")}</span>` : "—"), kv("Max bytes", has("max_bytes") ? fmtInt(b.max_bytes) : "—")]) : html`<div class="empty-line">No budget beyond the config’s own.</div>` });
  }

  function planPane(c) {
    const pending = c.status === "pending";
    const plan = c.plan || {};
    const { lines, note } = planDiff(c);
    const alerts = [];
    const pipe = c.plan && c.plan.summary && c.plan.summary.pipeline;
    if (c.run_id) alerts.push(alert("Started run", html`${lnk(pipe || c.run_id, href.run(c.run_id), true, c.run_id)}${cpi(c.run_id)}${(c.plan && c.plan.summary && c.plan.summary.sinks || []).length ? html` · it wrote to ${c.plan.summary.sinks.join(", ")}` : ""}.`, "ok"));
    else if (c.template) alerts.push(alert("Applied to template", html`${lnk(c.template.id, href.template(c.template.id))} v${c.template.version}.`, "ok"));
    if (c.status === "invalidated") alerts.push(alert("Approval invalidated.", html`The plan was re-built on approve and its material no longer matched what was reviewed${c.error ? html`: ${c.error}` : ""}. Nothing ran; propose it again to review the new plan.`, "bad", c.kind === "run" ? btn("Propose again", { icon: "refresh", cls: "sm", href: href.submit(), perm: "change_request" }) : ""));
    else if (c.error) alerts.push(alert("Execution failed.", c.error, "bad"));
    if (c.rejection) alerts.push(alert(`Rejected by ${c.rejection.principal}`, html`${fmtAge(c.rejection.at)} ago · “${c.rejection.reason}”`, "bad"));
    const planCard = card({
      icon: "changes",
      title: c.status === "executed" ? (c.kind === "run" ? "What it ran" : "What it changed") : c.kind === "run" ? "What changes if this runs" : "What this changes",
      tag: pending ? "re-planned on approve" : c.status === "executed" ? "material matched on approve" : "",
      right: plan.material ? html`material fingerprint <span class="mono">${plan.material.slice(0, 4)}…</span>${cpi(plan.material)}` : "",
      body: lines.length ? diff(lines) : html`<div class="empty-line">No row plan for this kind.</div>`,
      note: note || "",
    });
    const quote = c.reason ? html`<div class="card"><div class="pad" style="color:var(--text-2)"><small class="muted" style="display:block;margin-bottom:4px">Reason from the requester</small>${c.reason}</div></div>` : "";
    const hasBudget = Object.values(c.budget || {}).some((x) => x != null && !(Array.isArray(x) && !x.length));
    return html`${alerts}${planCard}${quote}${pending || hasBudget ? html`<div class="two">${approvalsCard(c)}${budgetCard(c)}</div>` : approvalsCard(c)}`;
  }

  function impactPane(c) {
    const rows = ((c.plan && c.plan.rows) || []).filter((r) => r.impact || r.policy);
    if (!rows.length) return card({ body: empty("No impact or policy analysis", html`Impact analysis needs a <code>catalog:</code> block and a recorded run of the row; a policy verdict needs a <code>policy:</code> in force.`, { icon: "lineage" }) });
    const sevTone = (s) => (s === "breaking" ? "bad" : s === "additive" ? "ok" : "muted");
    return join(rows.map((r) => {
      const out = [];
      if (r.impact) {
        const i = r.impact;
        const aff = (i.affected || []).filter((a) => a.depth > 0);
        const d = i.delta || {};
        out.push(card({
          icon: "lineage",
          title: aff.length ? `${aff.length} downstream dataset${aff.length === 1 ? "" : "s"} affected` : i.planned_from === "unknown" ? `Impact unknown for ${r.row || "this row"}` : `${r.row || "This row"} · no downstream dataset affected`,
          tag: i.planned_from === "unknown" && !aff.length ? "unknown" : i.severity || "",
          right: `planned from ${i.planned_from || "?"}`,
          body: aff.length ? tbl(["Dataset", "Depth", "Severity", "Columns read", "Owners"], aff.map((a) => [datasetLink(a), String(a.depth), dotText(sevTone(a.severity), a.severity || "unknown"), (a.columns || []).length ? join(a.columns.map((x) => html`<span class="mono">${x.column}</span>`), " · ") : html`<span class="muted">${a.opaque ? "no column lineage recorded" : "—"}</span>`, (a.owners || []).length ? a.owners.join(", ") : html`<span class="muted">—</span>`])) : "",
          note: html`Column delta: ${fmtInt((d.added || []).length)} added · ${fmtInt((d.removed || []).length)} removed · ${fmtInt((d.retyped || []).length)} retyped · ${fmtInt((d.renamed || []).length)} renamed.${(i.notes || []).length ? html` ${i.notes.join(" · ")}` : ""}`,
        }));
      }
      if (r.policy) {
        const vs = r.policy.violations || [];
        out.push(card({
          icon: "policy", title: "Policy check", right: `row ${r.row || "?"}${r.sink ? ` · ${r.sink}` : ""}`,
          body: vs.length ? tbl(["Rule", "Column", "Label", "Sink", "Result"], vs.map((x) => [x.rule, html`<span class="mono">${x.column}</span>`, x.label, `${x.sink} · ${x.sink_kind}`, dotText("bad", "denied")])) : "",
          note: vs.length ? html`<span class="bad">${vs.length} violation${vs.length === 1 ? "" : "s"}.</span> A violation blocks approval until the plan changes.` : html`<span class="ok">No violations.</span> A violation here would block approval until the plan changes.`,
        }));
      }
      return join(out);
    }));
  }

  function historyPane(c) {
    const ev = [{ at: c.created_at, tone: "muted", text: html`Requested by ${who(c.requester)} · ${KIND[c.kind] || c.kind}${c.plan && c.plan.material ? html` · plan fingerprint <span class="mono">${c.plan.material.slice(0, 4)}…</span>` : ""}` }];
    for (const a of c.approvals || []) ev.push({ at: a.at, tone: "ok", text: html`Approved by ${who(a.principal)}${a.comment ? ` · “${a.comment}”` : ""}` });
    if (c.rejection) ev.push({ at: c.rejection.at, tone: "bad", text: html`Rejected by ${who(c.rejection.principal)} · “${c.rejection.reason}”` });
    if (["executed", "failed", "invalidated", "expired"].includes(c.status)) ev.push({ at: c.updated_at, tone: c.status === "executed" ? "ok" : c.status === "expired" ? "muted" : "bad", text: html`${cap(c.status)}${c.run_id ? html` · run ${lnk(c.run_id, href.run(c.run_id), true)}` : ""}${c.error ? html` · ${c.error.split("\n")[0]}` : ""}` });
    if (c.status === "pending") {
      const left = (c.required_approvals || 1) - (c.approvals || []).length;
      ev.push({ at: null, tone: "warn", label: "pending", text: `Waiting for ${left} more approval${left === 1 ? "" : "s"}` });
      ev.push({ at: c.expires_at, tone: "muted", text: html`<span class="muted">Expires if not approved</span>` });
    }
    return card({
      title: "Timeline", right: html`change <span class="mono">${c.id}</span>${cpi(c.id)}`,
      body: html`<div class="log chg-timeline">${ev.map((e) => html`<div><span class="muted ts"${raw(e.at ? ` title="${formatTs(e.at)}"` : "")}>${e.label || shortTime(e.at)}</span><span class="dot ${e.tone}"></span>${e.text}</div>`)}</div>`,
    });
  }

  function changeDetail(c) {
    const pending = c.status === "pending";
    const n = (c.approvals || []).length;
    const impactN = ((c.plan && c.plan.rows) || []).filter((r) => r.impact || r.policy).length;
    const title = summaryText(c);
    setCrumbs(["Operate", { label: "Changes", href: listHref() }, title]);
    const left = (c.required_approvals || 1) - n;
    setDelta(pending ? html`<span class="warn">awaiting ${left} approval${left === 1 ? "" : "s"}</span>` : "");
    setContext(pending ? chip(`expires in ${fmtIn(c.expires_at)}`) : "");
    const status = pending ? `Pending · ${n} of ${c.required_approvals} approval${c.required_approvals === 1 ? "" : "s"}` : c.status === "approved" ? "Approved · executing" : cap(c.status);
    const sub = [
      pill(status, TONE[c.status] || "", PILL_ICON[c.status] || ""),
      html`<span>requested by ${who(c.requester)}${c.requester_role ? ` · ${c.requester_role}` : ""}</span>`,
      html`<span>${ago(c.created_at, " ago")}</span>`,
      pending ? html`<span title="${formatTs(c.expires_at)}">expires in ${fmtIn(c.expires_at)}</span>` : html`<span title="${formatTs(c.updated_at)}">decided ${fmtAge(c.updated_at)} ago</span>`,
      c.tenant ? html`<span>tenant ${lnk(c.tenant, href.tenant(c.tenant))}</span>` : "",
    ];
    const actions = pending ? html`${btn("Reject", { kind: "danger", icon: "x", perm: "change_approve", data: { decide: "reject" } })}${btn(quorumLabel(c), { kind: "primary", icon: "check", perm: "change_approve", data: { decide: "approve" } })}` : "";
    const tabsHtml = dtabs([["Plan", null, "Plan"], ["Impact", impactN || null, "Impact"], ["History", null, "History"]], state.dtab);
    const body = state.dtab === "Impact" ? impactPane(c) : state.dtab === "History" ? historyPane(c) : planPane(c);
    return html`${dhead(tabsHtml, actions, { href: listHref(), label: "Changes" })}${head(title, sub)}${body}`;
  }

  function renderDetail() {
    L.select(!!state.id);
    if (!state.id) { patch(L.detail, noneDetail()); return; }
    if (currentErr) {
      setCrumbs(["Operate", { label: "Changes", href: listHref() }, state.id]);
      patch(L.detail, html`${dhead("", "", { href: listHref(), label: "Changes" })}${currentErr.status === 404 ? empty("Change request not found", "It may have been purged, or the id is wrong.", { icon: "changes", btns: btn("All changes", { href: href.changes() }) }) : errorAlert("this change request", currentErr)}`);
      L.detail.querySelector("[data-retry]")?.addEventListener("click", () => loadDetail());
      return;
    }
    if (!current || current.id !== state.id) { L.detail.__html = ""; L.detail.innerHTML = v(html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`); return; }
    patch(L.detail, changeDetail(current));
  }

  async function loadDetail(keep = false) {
    const cid = state.id;
    if (!cid) { current = null; currentErr = null; renderDetail(); return; }
    const gen = ++detailGen;
    if (!keep) { current = null; currentErr = null; renderDetail(); }
    try {
      const c = await api(`/v1/changes/${encodeURIComponent(cid)}`);
      if (gen !== detailGen) return;
      current = c;
      currentErr = null;
    } catch (e) {
      if (gen !== detailGen) return;
      if (!keep || !current) { current = null; currentErr = e; }
    }
    renderDetail();
  }

  L.detail.addEventListener("click", (e) => {
    const t = e.target.closest("[data-tab]");
    if (t) return setQuery({ tab: t.dataset.tab === "Plan" ? "" : t.dataset.tab }, { replace: true });
    const dt = e.target.closest("[data-dtab]");
    if (dt) { e.preventDefault(); return setQuery({ tab: dt.dataset.dtab }, { replace: true }); }
    const d = e.target.closest("[data-decide]");
    if (d && current) decide(current, d.dataset.decide);
  });

  async function decide(c, what) {
    if (what === "approve") {
      const left = (c.required_approvals || 1) - (c.approvals || []).length;
      const text = left <= 1
        ? html`Yours completes the approvals, so ${c.kind === "run" ? "the run starts" : "the change applies"} right away after a fresh re-plan. If the material differs from what you reviewed, it stops instead.`
        : html`${left - 1} more approval${left - 1 === 1 ? " is" : "s are"} needed after yours before anything runs.`;
      const r = await dialog("Approve this change?", html`<span>${text}</span>${field("Comment", html`<textarea class="area sans" name="comment" rows="3" placeholder="Optional"></textarea>`, { help: "Optional · shown in the history and the audit log" })}`, html`${btn("Cancel", { data: { act: "cancel" } })}${btn(quorumLabel(c), { kind: "primary", icon: "check", data: { act: "approve" } })}`, { icon: "check" });
      if (!r || r.act !== "approve") return;
      const comment = (r.values.comment || "").trim();
      try {
        const out = await api(`/v1/changes/${encodeURIComponent(c.id)}/approve`, { method: "POST", body: comment ? { comment } : {} });
        if (out.status === "pending") toast(`Approved · ${(out.approvals || []).length} of ${out.required_approvals}`, "ok");
        else if (out.run_id) toast(html`Approved · run <span class="mono">${(out.plan && out.plan.summary && out.plan.summary.pipeline) || out.run_id}</span> started`, "ok", { href: href.run(out.run_id), label: "open run" });
        else toast(`Change ${out.status}`, out.status === "failed" || out.status === "invalidated" ? "bad" : "ok");
      } catch (e) {
        toast(e.message, "bad");
      }
      afterDecision();
      return;
    }
    const r = await dialog("Reject this change?", html`<span>The requester sees your reason. They can propose it again.</span>${field("Reason", input("", { name: "reason", ph: "Why this should not run" }), { req: true, help: "Required" })}`, html`${btn("Cancel", { data: { act: "cancel" } })}${btn("Reject", { kind: "danger", icon: "x", data: { act: "reject", keep: "1" } })}<button type="button" hidden data-act="reject-ok"></button>`, {
      icon: "x",
      onOpen: (el) => {
        const inp = el.querySelector("input[name=reason]");
        const submit = () => {
          if (!inp.value.trim()) {
            inp.parentElement.classList.add("err");
            const h = el.querySelector(".field .help");
            h.textContent = "A reason is required to reject.";
            h.classList.add("bad");
            inp.focus();
            return;
          }
          el.querySelector('[data-act="reject-ok"]').click();
        };
        el.querySelector('[data-act="reject"]').addEventListener("click", submit);
        inp.addEventListener("keydown", (ev) => { if (ev.key === "Enter") submit(); });
      },
    });
    if (!r || r.act !== "reject-ok") return;
    try {
      await api(`/v1/changes/${encodeURIComponent(c.id)}/reject`, { method: "POST", body: { reason: r.values.reason.trim() } });
      toast("Change rejected", "ok");
    } catch (e) {
      toast(e.message, "bad");
    }
    afterDecision();
  }

  async function afterDecision() {
    await Promise.all([load(), loadDetail(true)]);
    refreshBadges(true);
    if (state.view === "mine" && current && !needsMe(current, me)) setQuery({ view: "all" }, { replace: true });
  }

  async function load() {
    const p = withTenant(new URLSearchParams({ limit: String(LIMIT) }));
    const cp = withTenant(new URLSearchParams());
    try {
      const [rows, totals] = await Promise.all([api(`/v1/changes?${p}`), api(`/v1/changes/counts?${cp}`).catch(() => null)]);
      changes = Array.isArray(rows) ? rows : (rows && rows.changes) || [];
      counts = totals ? totals.counts : null;
      truncated = changes.length >= LIMIT;
      loadErr = null;
    } catch (e) {
      if (!changes) loadErr = e;
    }
    renderList();
    if (!state.id) renderDetail();
  }

  renderList();
  renderDetail();
  await load();
  if (state.id) loadDetail();
  // A linked request that "Needs me" does not list opens on "All".
  if (state.id && state.view === "mine" && changes && !params.query.get("view")) {
    const c = changes.find((x) => x.id === state.id);
    if (c && !needsMe(c, me)) setQuery({ view: "all" }, { replace: true });
  }
  const stop = poll(async () => {
    await load();
    if (state.id && current && ["pending", "approved"].includes(current.status)) await loadDetail(true);
  }, 15000);

  return {
    teardown: stop,
    update(p) {
      const next = read(p);
      const idChanged = next.id !== state.id;
      state = next;
      renderList();
      if (idChanged) loadDetail();
      else renderDetail();
    },
  };
}

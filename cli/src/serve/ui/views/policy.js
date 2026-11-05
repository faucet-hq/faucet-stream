// Policy (#1043): the data-flow policy in force — classifications, rules,
// refusals, a dry run and the version history — and, for admins, an editor
// whose changes become change requests another admin approves. A `--policy`
// file is shown read-only. Each tenant can add rules on top of the server's
// (add-only); a tenant principal sees the server rules that apply to it and
// edits its own, an operator sees both with the tenant switcher set.
import { api } from "../api.js";
import { html, head, card, tbl, pill, kv, grid, btn, dhead, dtabs, lnk, alert, skelCards, stats, diff, field, select, check, tag, input, code } from "../components.js";
import { layout, setQuery, errorAlert, ago, dialog, toast, patch } from "../page.js";
import { setCrumbs, setDelta, setContext, refreshBadges } from "../shell.js";
import { fmtInt, fmtAge } from "../utils.js";
import { formatTs } from "../tz.js";
import { href } from "../links.js";
import { whoami, can } from "../access.js";
import { currentTenant, tenantList } from "../tenant.js";

const nw = (t) => html`<span class="mono" style="white-space:nowrap">${t}</span>`;
const mono = (t) => html`<span class="mono">${t}</span>`;
const who = (p) => (p ? (can("audit_read") ? lnk(p, href.audit({ principal: p })) : html`<b>${p}</b>`) : html`<span class="muted">—</span>`);
const enc = encodeURIComponent;
const tenantBase = (t) => `/v1/tenants/${enc(t)}/policy`;

const STARTER = `version: 1
description: Personal data stays in the EU
classifications:
  - label: pii.email
    fields: [email]
    value_detector: email
rules:
  - name: pii-to-eu-only
    when: { label: pii.email }
    require: { residency: [eu] }
    mask: [hash]
`;
const tenantStarter = (t) => `version: 1
description: ${t}'s own data rules
classifications:
  - label: ${t}.internal
    fields: [internal_notes]
rules:
  - name: ${t}-internal-only
    when: { label: ${t}.internal }
    require: { tier: [internal] }
`;

/** "today 09:12", "yesterday 17:03" or "28 Oct 07:12". */
function when(iso) {
  const d = new Date(iso);
  if (isNaN(d)) return "";
  const days = Math.floor((Date.now() - d.getTime()) / 86400000);
  const hm = formatTs(iso).slice(-8, -3);
  return html`<time datetime="${iso}" title="${formatTs(iso)}">${days <= 0 ? "today" : days === 1 ? "yesterday" : fmtAge(iso) + " ago"} ${days <= 1 ? hm : ""}</time>`;
}

/** What a rule's `when` covers. */
function whenText(r) {
  const parts = [mono(r.when.label)];
  parts.push((r.when.sink_kind || []).length ? html`sink kind ${r.when.sink_kind.join(", ")}` : "any sink");
  for (const [k, vals] of Object.entries(r.when.sink || {})) parts.push(html`sink ${mono(k)} ∈ ${vals.join(", ")}`);
  return html`${parts.reduce((a, b, i) => (i ? html`${a} · ${b}` : b), "")}`;
}
/** What satisfies a rule. */
function allowedText(r) {
  if (r.deny) return (r.mask || []).length ? html`only masked by ${r.mask.join(" · ")}` : html`<span class="bad">never (deny)</span>`;
  const parts = Object.entries(r.require || {}).map(([k, vals]) => html`sink ${mono(k)} ∈ ${vals.join(", ")}`);
  if ((r.mask || []).length) parts.push(html`masked by ${r.mask.join(" · ")}`);
  return parts.reduce((a, b, i) => (i ? html`${a}, or ${b}` : b), "");
}
function matchesText(c) {
  const parts = [];
  if ((c.fields || []).length) parts.push(html`fields ${c.fields.map(mono).reduce((a, b) => html`${a}, ${b}`)}`);
  if (c.field_pattern) parts.push(html`pattern ${mono(c.field_pattern)}`);
  return parts.length ? parts.reduce((a, b) => html`${a}, ${b}`) : html`<span class="muted">by value only</span>`;
}

/** A violation's "to fix" hint. */
function fixText(x) {
  const k = x.kind || "";
  const mask = (x.satisfied_by_mask || []).join(" or ");
  if (k === "denied") return mask ? `mask it with ${mask}` : "drop the column or use another sink";
  if (k === "missing_attribute" || k === "attribute_not_allowed") {
    const allowed = (x.allowed || []).join(", ");
    return `${mask ? `${mask} it, or ` : ""}use a sink with ${x.attribute}${allowed ? ` in [${allowed}]` : ""}`;
  }
  if (k === "unmasked") return `mask it with ${(x.allowed || []).join(" or ")}`;
  return "";
}
function whyText(x) {
  const k = x.kind || "";
  if (k === "denied") return `may not reach ${x.sink_kind}`;
  if (k === "missing_attribute") return `${x.sink} declares no ${x.attribute}`;
  if (k === "attribute_not_allowed") return `${x.attribute} is ${x.value}`;
  if (k === "unmasked") return "reaches the sink unmasked";
  return k;
}

export function renderPolicy(host, params) {
  const L = layout(host, "canvas");
  const me = whoami();
  const scoped = me && me.tenant ? me.tenant : "";
  let st = read(params);
  let sp = null; // GET /v1/policy
  let tp = null; // GET /v1/tenants/{t}/policy
  let versions = null;
  let refusals = null;
  let err = null;
  let gen = 0;
  // Dry run and editor state survive tab switches (not page reloads).
  const dry = { mode: "paste", text: "", template: "", row: "", sample: false, sampleText: "", result: null, error: null, busy: false, templates: null };
  const ed = { text: null, problems: [], preview: null, checking: false, step: "edit", reason: "", busy: false, error: null, timer: null, seq: 0 };

  function read(p) {
    const tenant = scoped || currentTenant() || "";
    return { tab: p.query.get("tab") || "Rules", edit: p.query.get("edit") === "1", tenant };
  }
  const layer = () => (st.tenant ? "tenant" : "server");

  async function load() {
    const g = ++gen;
    err = null;
    try {
      const [s, t] = await Promise.all([
        scoped ? Promise.resolve(null) : api("/v1/policy"),
        st.tenant ? api(tenantBase(st.tenant)).catch((e) => (e.status === 404 && !scoped ? null : Promise.reject(e))) : Promise.resolve(null),
      ]);
      if (g !== gen) return;
      sp = s;
      tp = t;
      if (!t && !scoped) st.tenant = "";
      versions = !st.tenant && sp && sp.source === "server" ? await api("/v1/policy/versions").catch(() => []) : null;
      if (g !== gen) return;
      refusals = null;
    } catch (e) {
      if (g !== gen) return;
      err = e;
    }
    render();
    if (st.tab === "Refusals" || st.tab.startsWith("Refusals")) loadRefusals();
  }

  async function loadRefusals() {
    const q = new URLSearchParams({ since: new Date(Date.now() - 7 * 86400000).toISOString() });
    if (st.tenant) q.set("tenant", st.tenant);
    if (st.refTenant) q.set("tenant", st.refTenant);
    try {
      refusals = await api(`/v1/policy/refusals?${q}`);
    } catch (e) {
      refusals = { error: e };
    }
    render();
  }

  // ---- header pieces ----------------------------------------------------------------------
  function serverSub() {
    if (!sp || !sp.loaded) return [pill("None loaded", "")];
    const desc = (sp.policy && sp.policy.description) || "server policy";
    if (sp.source === "file") {
      const f = sp.file || {};
      return [pill("Enforced", "ok", "shield"), html`<span>${(f.path || "").split("/").pop()}</span>`, html`<span>loaded from <span class="mono">--policy</span> file · sha <span class="mono">${(f.sha256 || "").slice(0, 4)}…${(f.sha256 || "").slice(-2)}</span></span>`, html`<span>since server start ${f.loaded_at ? `${fmtAge(f.loaded_at)} ago` : ""}</span>`];
    }
    const ver = sp.version || {};
    return [pill("Enforced", "ok", "shield"), html`<span>${desc} · version ${ver.version}</span>`, ver.approved_by ? html`<span>approved by ${who(ver.approved_by)} ${fmtAge(ver.created_at)} ago</span>` : "", html`<span>managed by the server</span>`];
  }
  function pendingBanner(pending, what, inForce) {
    if (!pending) return "";
    const mine = me && me.principal === pending.requester;
    const link = lnk(`change ${String(pending.id).slice(0, 8)}`, href.change(pending.id));
    return alert(mine ? `Your change to ${what} v${pending.next_version} waits for approval.` : `A change to ${what} v${pending.next_version} by ${pending.requester} waits for approval.`, html`It takes effect on every replica once ${layer() === "tenant" ? `another ${st.tenant} admin or a server admin` : "another admin"} approves ${link}. ${inForce} stays enforced until then.`, "warn", btn("Open in Changes", { icon: "changes", href: href.change(pending.id) }));
  }

  // ---- server rules ---------------------------------------------------------------------
  function classCard(p, seen) {
    const cs = p.classifications || [];
    return card({
      icon: "layers", title: "Classifications", aside: "label a column by name, pattern or value", right: `${fmtInt(new Set(cs.map((c) => c.label)).size)} labels`,
      body: cs.length ? tbl(["Label", "Matches", "Value detector", ["Columns seen (30d)", "num"]], cs.map((c) => {
        const s = (seen || {})[c.label];
        return [mono(c.label), matchesText(c), c.value_detector || "—", s ? `${fmtInt(s.columns)} in ${fmtInt(s.datasets)} dataset${s.datasets === 1 ? "" : "s"}` : "—"];
      })) : html`<div class="empty-line">No classifications.</div>`,
    });
  }
  function rulesCard(p) {
    const rs = p.rules || [];
    return card({
      icon: "shield", title: "Rules", aside: "which sinks a label may reach", right: `${fmtInt(rs.length)} rule${rs.length === 1 ? "" : "s"}`,
      body: rs.length ? tbl(["Rule", "When", "Allowed if", "At run time"], rs.map((r) => [nw(r.name), whenText(r), allowedText(r), r.on_runtime || "fail"])) : html`<div class="empty-line">No rules.</div>`,
      note: "Refused at submit when a plan shows a violation; checked again on every page at run time.",
    });
  }
  function factsCard() {
    const s = sp.summary || {};
    const r = sp.refusals_7d || {};
    const sa = s.sinks_with_attributes || {};
    return card({ title: "In effect", body: grid(4, [kv("Rules", fmtInt(s.rules || 0)), kv("Labels", fmtInt(s.labels || 0)), kv("Sinks with attributes", fmtInt(sa.sinks || 0), (sa.attributes || []).join(", ")), kv("Refusals (7d)", fmtInt(r.total || 0), `${fmtInt(r.submit || 0)} at submit · ${fmtInt(r.runtime || 0)} at run time`)]) });
  }

  // ---- refusals -------------------------------------------------------------------------
  function refusalsPane({ tenantView = false, layerCol = false } = {}) {
    if (!refusals) return skelCards(2);
    if (refusals.error) return errorAlert("refusals", refusals.error);
    const list = refusals;
    const submit = list.filter((r) => r.phase === "submit").length;
    const rules = new Set(list.map((r) => r.rule).filter(Boolean)).size;
    const runCell = (r) => (r.run_id ? lnk(`${r.target ? `${r.target.replace(/^pipeline:/, "")} · ` : ""}${String(r.run_id).slice(0, 4)}`, href.run(r.run_id)) : html`${(r.target || "submission").replace(/^pipeline:/, "")}`);
    const sinkCell = (r) => (r.sink ? `${r.sink_kind ? `${r.sink_kind} · ` : ""}${r.sink}` : "—");
    const where = (r) => html`${r.phase === "runtime" ? "run time" : "submit"} · <span class="${r.outcome === "refused" ? "" : "bad"}">${r.outcome}</span>`;
    const cols = ["When", "Run or change", ...(tenantView ? [] : ["Tenant"]), "Rule", ...(layerCol ? ["Layer"] : []), "Column", "Sink", "Where"];
    const rows = list.map((r) => [when(r.at), runCell(r), ...(tenantView ? [] : [r.tenant ? lnk(r.tenant, href.tenant(r.tenant)) : html`<span class="muted">—</span>`]), r.rule ? nw(r.rule) : "—", ...(layerCol ? [r.layer ? tag(r.layer) : "—"] : []), r.column ? mono(r.column) : "—", sinkCell(r), where(r)]);
    const ts = tenantList();
    const tenantSel = !tenantView && !scoped && ts && ts.length ? select([["", "All tenants"], ...ts.map((t) => [t.id, t.name || t.id])], st.refTenant || "", { name: "ref-tenant" }) : "";
    const statItems = layerCol
      ? [["Refusals (7d)", fmtInt(list.length)], ["By server rules", fmtInt(list.filter((r) => r.layer === "server").length)], [`By ${st.tenant} rules`, fmtInt(list.filter((r) => r.layer === "tenant").length)]]
      : [["Refusals (7d)", fmtInt(list.length)], ["At submit", fmtInt(submit)], ["At run time", fmtInt(list.length - submit), list.length - submit ? "bad" : ""], ...(tenantView ? [] : [["Rules involved", fmtInt(rules)]])];
    return html`${stats(statItems)}${card({
      icon: "alert", title: tenantView ? (scoped ? "Your refused runs" : `${st.tenant}’s refused runs`) : "Refusals", right: tenantSel,
      body: list.length ? tbl(cols, rows) : html`<div class="empty-line">No refusals in the last 7 days.</div>`,
      note: tenantView ? "Server rules are set by the server operator; ask them if one blocks something you need." : html`From the audit log (<code>policy.denied</code>): submissions the policy refused and runs it stopped. ${lnk("Open in Audit log", href.audit({ action: "policy.denied" }))}`,
    })}`;
  }

  // ---- dry run --------------------------------------------------------------------------
  function dryPane() {
    const seg = html`<span class="seg"><span class="${dry.mode === "paste" ? "on" : ""}" data-dry-mode="paste">Paste</span><span class="${dry.mode === "template" ? "on" : ""}" data-dry-mode="template">Template</span></span>`;
    const tpls = dry.templates || [];
    const input1 = dry.mode === "paste"
      ? html`<textarea class="area mono" name="dry-config" rows="9" placeholder="pipeline:&#10;  source: …&#10;  sink: …">${dry.text}</textarea>`
      : html`${select([["", dry.templates ? "Pick a pipeline template" : "Loading templates…"], ...tpls.map((t) => [t.id, t.id])], dry.template, { name: "dry-template" })}`;
    const sampleBox = dry.sample && dry.mode === "paste" ? html`<textarea class="area mono" name="dry-sample" rows="4" placeholder='[{"email": "a@example.com"}]'>${dry.sampleText}</textarea>` : "";
    const left = card({
      icon: "code", title: "Config to check", right: seg,
      body: html`<div class="pad stack" style="gap:10px">${input1}${sampleBox}<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">${input(dry.row, { ph: "row: all rows", name: "dry-row", mono: true })}${dry.mode === "paste" ? check("Use a sample of rows (plan --sample)", dry.sample, { name: "dry-sample-on" }) : ""}<span style="margin-left:auto">${btn(dry.busy ? "Checking…" : "Check", { icon: "shield", kind: "primary", data: { "dry-run": "1" }, disabled: dry.busy })}</span></div></div>`,
      note: "Nothing runs. The config is checked the way a submit would be.",
    });
    let right = card({ icon: "shield", title: "Result", body: html`<div class="empty-line">Paste a config, or pick a template, and check it.</div>` });
    if (dry.error) right = card({ icon: "alert", title: "Could not check", body: html`<div class="pad">${alert("The config did not load.", dry.error.message, "bad")}</div>` });
    else if (dry.result) {
      const r = dry.result;
      const vs = (r.rows || []).flatMap((row) => row.violations || []);
      if (r.loaded === false) right = card({ icon: "shield", title: "No policy applies", body: html`<div class="pad">${alert("Nothing to check against.", "No policy is in force for this config.", "")}</div>` });
      else if (!vs.length) {
        const known = (r.rows || []).reduce((n, row) => n + (row.known_columns || []).length, 0);
        right = card({ icon: "check", title: "No violations", body: html`<div class="pad">${alert("This config passes every rule.", known ? `${fmtInt(known)} known column${known === 1 ? "" : "s"} checked against ${fmtInt(r.rules || 0)} rule${r.rules === 1 ? "" : "s"}.` : "No column is known before a run (no contract or sample): the run-time check still applies.", "ok")}</div>` });
      } else right = card({
        icon: "alert", title: `${fmtInt(vs.length)} violation${vs.length === 1 ? "" : "s"}`, tag: "would be refused",
        body: tbl(["Rule", "Column", "Label", "Why", "To fix"], vs.map((x) => [nw(x.rule), mono(x.column), x.label, whyText(x), fixText(x)])),
        note: dry.sample ? "Labels came from the sample. Columns the sample did not show are checked again at run time." : "Columns come from the config’s contract. Columns it does not declare are checked again at run time.",
      });
    }
    return html`<div class="pair">${left}${right}</div>`;
  }

  async function runDry() {
    const root = L.detail;
    dry.text = root.querySelector('[name="dry-config"]')?.value ?? dry.text;
    dry.template = root.querySelector('[name="dry-template"]')?.value ?? dry.template;
    dry.row = root.querySelector('[name="dry-row"]')?.value ?? dry.row;
    dry.sampleText = root.querySelector('[name="dry-sample"]')?.value ?? dry.sampleText;
    const body = {};
    if (dry.mode === "template") {
      if (!dry.template) return toast("Pick a template to check", "bad");
      body.template = dry.template;
    } else {
      if (!dry.text.trim()) return toast("Paste a config to check", "bad");
      body.config = dry.text;
      if (dry.sample) {
        try {
          body.sample = JSON.parse(dry.sampleText || "[]");
        } catch {
          return toast("The sample must be a JSON array of records", "bad");
        }
      }
    }
    if (dry.row.trim()) body.row = dry.row.trim();
    dry.busy = true;
    dry.error = null;
    render();
    try {
      dry.result = await api("/v1/policy/check", { method: "POST", body });
    } catch (e) {
      dry.result = null;
      dry.error = e;
    }
    dry.busy = false;
    render();
  }

  // ---- history --------------------------------------------------------------------------
  function historyCard(list, { title, editable, tenantOf = "" }) {
    const rows = (list || []).map((x, i) => [
      html`${i === 0 ? html`<b>v${x.version}</b> ${tag("active")}` : `v${x.version}`}${x.restores != null ? html` <span class="muted">(restores v${x.restores})</span>` : ""}`,
      who(x.created_by), who(x.approved_by), x.reason || html`<span class="muted">—</span>`, ago(x.created_at, " ago"),
      i === 0 || !editable ? "" : btn(`Roll back to v${x.version}`, { icon: "undo", cls: "sm", perm: "policy_admin", data: { rollback: String(x.version), "rb-tenant": tenantOf } }),
    ]);
    return card({
      icon: "clock", title, right: tenantOf ? "approved by another tenant admin or a server admin" : "every change was approved by a second admin",
      body: rows.length ? tbl(["Version", "Changed by", "Approved by", "Reason", "When", ""], rows) : html`<div class="empty-line">No versions yet.</div>`,
      note: "A rollback is a change request too: it takes effect when another admin approves it.",
    });
  }

  async function rollback(version, tenantOf) {
    const list = tenantOf ? (tp && tp.versions) || [] : versions || [];
    const target = list.find((x) => String(x.version) === String(version));
    const current = list[0];
    if (!target) return;
    const base = tenantOf ? tenantBase(tenantOf) : "/v1/policy";
    let preview = null;
    try {
      preview = await api(`${base}/preview`, { method: "POST", body: { policy: target.yaml } });
    } catch (e) {
      return toast(e.message, "bad");
    }
    const lines = (preview.diff || []).filter((l) => l.op !== " ").map((l) => [l.op, l.text]);
    const r = await dialog(`Roll back to version ${version}?`, html`<span>This creates a change request. Version ${current ? current.version : "?"} stays enforced until another admin approves it.</span>${lines.length ? diff(lines) : html`<div class="muted">No difference from the version in force.</div>`}${preview.valid ? "" : alert("This version no longer validates.", (preview.problems || []).map((p) => p.message).join("; "), "bad")}${field("Reason", html`<input class="input" name="reason" required placeholder="Why roll back">`, { req: true })}`, html`${btn("Cancel", { data: { act: "cancel" } })}${btn("Request rollback", { kind: "primary", icon: "undo", data: { act: "go" } })}`, { icon: "undo" });
    if (!r || r.act !== "go") return;
    if (!(r.values.reason || "").trim()) return toast("A rollback needs a reason", "bad");
    try {
      const c = await api(`${base}/rollback`, { method: "POST", body: { version: Number(version), reason: r.values.reason.trim() } });
      toast(`Rollback to v${version} requested`, "ok", { href: href.change(c.id), label: "open change" });
      refreshBadges(true);
      load();
    } catch (e) {
      toast(e.message, "bad");
    }
  }

  // ---- tenant views ---------------------------------------------------------------------
  function serverForTenant(rules) {
    return card({
      icon: "shield", title: `Server rules that apply to ${st.tenant}`, tag: "server", aside: "read only", right: `${fmtInt(rules.length)} rule${rules.length === 1 ? "" : "s"}`,
      body: rules.length ? tbl(["Rule", "Requires", "Layer"], rules.map((r) => [nw(r.name), html`${mono(r.label)} ${r.requires}`, tag("server")])) : html`<div class="empty-line">The server policy has no rules.</div>`,
      note: `${st.tenant}’s rules can add to these, never remove or loosen them.`,
    });
  }
  function tenantRulesCard(edit) {
    const p = tp.policy || {};
    const ver = tp.version ? `v${tp.version.version}` : "";
    return card({
      icon: "layers", title: `${st.tenant}’s rules`, tag: "tenant", aside: `${ver} · ${st.tenant}’s runs only`, right: edit ? btn(`Edit ${st.tenant} rules`, { icon: "edit", kind: "primary", perm: "policy_admin", data: { edit: "1" }, disabled: !!tp.pending_change }) : "",
      body: html`${tbl(["Classification", "Matches", "Layer"], (p.classifications || []).map((c) => [nw(c.label), matchesText(c), tag("tenant")]))}${tbl(["Rule", "Requires", "At run time", "Layer"], (p.rules || []).map((r) => [nw(r.name), html`${mono(r.when.label)} ${allowedText(r)}`, r.on_runtime || "fail", tag("tenant")]))}`,
    });
  }
  function tenantHead(opView) {
    const sv = tp.server || {};
    const server = sv.loaded ? `server ${sv.source === "file" ? "file policy" : `v${sv.version}`}` : "no server policy";
    const mine = tp.loaded ? `${st.tenant} rules v${tp.version.version}` : `no ${st.tenant} rules`;
    const sub = [pill("Enforced", "ok", "shield"), html`<span>${server} + ${mine}</span>`, opView ? html`<span>tenant switcher set to ${st.tenant}</span>` : tp.version && tp.version.approved_by ? html`<span>${st.tenant} rules approved by ${who(tp.version.approved_by)} ${fmtAge(tp.version.created_at)} ago</span>` : ""];
    return head(opView ? `Policy · ${st.tenant}` : "Policy", sub, opView ? `What ${st.tenant}’s runs are checked against: the server policy plus ${st.tenant}’s add-only rules. Pick “All tenants” above for the server policy alone.` : `${st.tenant}’s runs are checked against the server’s rules and ${st.tenant}’s own. ${st.tenant}’s rules can only add: they never remove or loosen a server rule.`);
  }

  function tenantView() {
    const opView = !scoped;
    setCrumbs(["Govern", "Policy", ...(opView ? [st.tenant] : [])]);
    const editable = tp.editable && can("policy_admin");
    const refN = (tp.refusals_7d || {}).total || 0;
    const tabNames = opView ? ["Rules", [`Refusals · ${st.tenant}`, refN, "Refusals"], [`${st.tenant} history`, (tp.versions || []).length, "History"]] : ["Rules", ["Refusals", refN, "Refusals"], ["History", (tp.versions || []).length, "History"]];
    const tab = ["Refusals", "History"].includes(st.tab) ? st.tab : "Rules";
    const banner = pendingBanner(tp.pending_change, `${st.tenant} rules`, tp.version ? `${st.tenant} v${tp.version.version}` : "The server policy");
    if (!tp.loaded && tab === "Rules") {
      return html`${tenantHead(opView)}${banner}${dhead(dtabs(tabNames, tab))}${alert(`Only the server policy applies to ${st.tenant}.`, `${st.tenant} can add its own classifications and rules on top. They can only tighten what the server allows, and the first version needs another ${st.tenant} admin or a server admin to approve it.`, "", editable && !tp.pending_change ? btn("Add rules", { icon: "plus", kind: "primary", perm: "policy_admin", data: { edit: "1" } }) : "")}${serverForTenant((tp.server || {}).rules || [])}`;
    }
    const right = opView && editable ? btn(`Edit ${st.tenant} rules`, { icon: "edit", kind: "primary", perm: "policy_admin", data: { edit: "1" }, disabled: !!tp.pending_change }) : "";
    let body;
    if (tab === "Refusals") body = refusalsPane({ tenantView: true, layerCol: true });
    else if (tab === "History") body = historyCard(tp.versions, { title: `${st.tenant} policy versions`, editable, tenantOf: st.tenant });
    else if (opView) body = html`<div class="pair">${sp && sp.policy ? rulesCard(sp.policy) : serverForTenant((tp.server || {}).rules || [])}${tenantRulesCard(false)}</div>${historyCard(tp.versions, { title: `${st.tenant} policy versions`, editable, tenantOf: st.tenant })}`;
    else body = html`<div class="pair">${serverForTenant((tp.server || {}).rules || [])}${tenantRulesCard(editable)}</div>${editable ? "" : html`<div class="muted" style="font-size:12px">Only ${st.tenant} admins change ${st.tenant}’s rules.</div>`}`;
    return html`${tenantHead(opView)}${banner}${dhead(dtabs(tabNames, tab), right)}${body}`;
  }

  // ---- server view ------------------------------------------------------------------------
  function serverView() {
    setCrumbs(["Govern", "Policy"]);
    if (!sp.loaded) {
      const canWrite = sp.source === "server" && sp.editable && can("policy_admin");
      return html`${head("Policy", [pill("None loaded", "")], "No data-flow policy is enforced: any column may reach any sink.")}${pendingBanner(sp.pending_change, "the policy", "No policy")}
        <div class="two">${card({ icon: "edit", title: "Write one here", body: html`<div class="pad stack" style="gap:10px"><span>Start from a template. Like every policy change, the first version needs a second admin’s approval before it is enforced.</span><div>${canWrite && !sp.pending_change ? btn("Write a policy", { icon: "plus", kind: "primary", perm: "policy_admin", data: { edit: "1" } }) : html`<span class="muted">${sp.pending_change ? "A first version waits for approval." : "Admins can write one."}</span>`}</div></div>` })}
        ${card({ icon: "code", title: "Or load a file at start", body: code("faucet serve --policy policies/eu-pii.yaml", { box: false, lang: "text" }), note: "A file-loaded policy is shown here read-only." })}</div>`;
    }
    const file = sp.source === "file";
    const refN = (sp.refusals_7d || {}).total || 0;
    const tabNames = ["Rules", ["Refusals", refN, "Refusals"], can("plan") ? "Dry run" : null, file ? null : ["History", (versions || []).length, "History"]].filter(Boolean);
    const tab = tabNames.some((t) => (Array.isArray(t) ? t[2] : t) === st.tab) ? st.tab : "Rules";
    const banner = file
      ? alert("Managed by the --policy file.", html`This server loaded <code>${(sp.file || {}).path || "the policy file"}</code> at start. Change the file and restart to update it; the console shows it read-only.`, "")
      : pendingBanner(sp.pending_change, "the policy", `Version ${(sp.version || {}).version}`);
    const editBtn = !file && sp.editable && can("policy_admin") ? btn("Edit policy", { icon: "edit", kind: "primary", perm: "policy_admin", data: { edit: "1" }, disabled: !!sp.pending_change }) : "";
    let body;
    if (tab === "Refusals") body = refusalsPane();
    else if (tab === "Dry run") body = dryPane();
    else if (tab === "History") body = historyCard(versions, { title: "Versions", editable: sp.editable });
    else body = html`${factsCard()}<div class="pair">${classCard(sp.policy || {}, (sp.summary || {}).columns_seen)}${rulesCard(sp.policy || {})}</div>`;
    return html`${head("Policy", serverSub())}${banner}${dhead(dtabs(tabNames, tab), editBtn)}${body}`;
  }

  // ---- editor -----------------------------------------------------------------------------
  function editorView() {
    const tenantEdit = layer() === "tenant";
    const t = st.tenant;
    const what = tenantEdit ? `${t}’s rules` : (sp && sp.policy && sp.policy.description) || "the policy";
    const curVer = tenantEdit ? (tp && tp.version ? tp.version.version : null) : sp && sp.version ? sp.version.version : null;
    const nextVer = (curVer || 0) + 1;
    setCrumbs(["Govern", { label: "Policy", href: href.policy() }, tenantEdit ? `Edit ${t} rules` : "Edit"]);
    const approver = tenantEdit ? `needs another ${t} admin or a server admin` : "needs a second admin’s approval";
    const subBase = [html`<span>draft of ${tenantEdit ? `${t} ` : ""}v${nextVer}${curVer ? ` · based on v${curVer}` : " · the first version"}</span>`, html`<span>${approver}</span>`];
    const review = ed.step === "review" && ed.preview && ed.preview.valid;
    const right = html`${btn("Discard", { kind: "quiet", data: { discard: "1" } })}${review ? btn("Back to editor", { icon: "edit", data: { back: "1" } }) : btn("Preview impact", { icon: "diff", kind: "primary", data: { review: "1" }, disabled: !(ed.preview && ed.preview.valid) || ed.checking })}`;
    const top = html`${dhead("", right, { href: href.policy(), label: "Policy" })}${head(review ? `Review your change to ${what}` : `Edit ${what}`, subBase)}`;
    if (!review) {
      const n = ed.problems.length;
      const problems = n
        ? card({ icon: "alert", title: "Problems", body: tbl(["Line", "Problem"], ed.problems.map((p) => [p.line ? String(p.line) : "—", html`<span class="bad">${p.message}</span>`])), note: "Checked as you type, with the same validation the server runs on submit." })
        : card({ icon: ed.checking ? "refresh" : "check", title: ed.checking ? "Checking…" : ed.preview && ed.preview.valid ? "No problems" : "Problems", body: html`<div class="empty-line">${ed.checking ? "Validating the draft." : ed.preview && ed.preview.valid ? "The draft validates. Preview its impact next." : "Edit the draft to check it."}</div>`, note: tenantEdit ? `Tenant rules can only add: they may not reuse a server rule’s name.` : "Checked as you type, with the same validation the server runs on submit." });
      return html`${top}${n ? alert(`${n} problem${n === 1 ? "" : "s"} in the draft.`, tenantEdit ? "Tenant rules can only add. Fix them to preview the impact." : "Fix them to preview the impact.", "bad") : ""}
        <div class="pair">${card({ icon: "code", title: tenantEdit ? `${t}-policy.yaml` : "policy.yaml", aside: "draft", body: html`<div class="pad"><textarea class="area mono${n ? " err" : ""}" name="draft" rows="22" spellcheck="false">${ed.text}</textarea></div>` })}${problems}</div>`;
    }
    const im = ed.preview.impact || {};
    const refused = im.newly_refused || [];
    const allowed = im.newly_allowed || [];
    const lines = (ed.preview.diff || []);
    const changed = lines.filter((l) => l.op !== " ");
    const impactRows = refused.slice(0, 8).map((x) => [x.kind === "template" ? html`${lnk(x.id, href.template(x.id))}${x.version != null ? html` <span class="mono">v${x.version}</span>` : ""}` : lnk(`run ${String(x.id).slice(0, 8)}`, href.run(x.id)), x.rule ? nw(x.rule) : "—", x.why]);
    const runsRefused = refused.filter((x) => x.kind === "run").length;
    const tplRefused = refused.filter((x) => x.kind === "template").length;
    return html`${top}
      <div class="pair">${card({ icon: "diff", title: "What changes", aside: `${curVer ? `v${curVer}` : "nothing"} → draft`, body: changed.length ? diff(lines.filter((l, i) => l.op !== " " || lines.slice(Math.max(0, i - 1), i + 2).some((x) => x.op !== " ")).map((l) => [l.op === " " ? " " : l.op, l.text])) : html`<div class="empty-line">No change from the version in force.</div>` })}
      ${card({ icon: "lineage", title: tenantEdit ? `Impact on ${t}` : "Impact", aside: tenantEdit ? `${t}’s templates and its last 7 days of runs` : "every registered template and the last 7 days of runs", body: html`${grid(3, [kv("Newly refused", html`<span class="${tplRefused ? "bad" : ""}">${fmtInt(tplRefused)} template${tplRefused === 1 ? "" : "s"}</span>`, `${fmtInt(runsRefused)} recent run${runsRefused === 1 ? "" : "s"}`), kv("Newly allowed", fmtInt(allowed.length), tenantEdit ? "tenant rules only add" : ""), kv("Unchanged", `${fmtInt(im.unchanged || 0)}`, `${fmtInt(im.templates_checked || 0)} templates · ${fmtInt(im.runs_checked || 0)} runs checked`)])}${impactRows.length ? html`<div class="pad" style="padding-top:0">${tbl(["Would be refused", "Rule", "Why"], impactRows)}</div>` : ""}`, note: im.templates_runtime_only ? `${fmtInt(im.templates_runtime_only)} template${im.templates_runtime_only === 1 ? "" : "s"} declare no contract: their columns are only known at run time, where the same rules apply.` : "" })}</div>
      ${card({ title: "Request the change", body: html`<div class="form one">${field("Reason", html`<textarea class="area sans" name="reason" rows="2" placeholder="Why this change">${ed.reason}</textarea>`, { req: true, help: "Shown to the approver and kept with the version" })}</div><div class="actions"><span class="muted">${tenantEdit ? `Another ${t} admin or a server admin must approve.` : "Policy changes always need another admin’s approval."} Runs already admitted keep the version they were checked against.</span>${btn(ed.busy ? "Requesting…" : "Request change", { icon: "changes", kind: "primary", data: { submit: "1" }, disabled: ed.busy })}</div>` })}`;
  }

  async function validateDraft() {
    const seq = ++ed.seq;
    ed.checking = true;
    render();
    const base = layer() === "tenant" ? tenantBase(st.tenant) : "/v1/policy";
    try {
      const p = await api(`${base}/preview`, { method: "POST", body: { policy: ed.text } });
      if (seq !== ed.seq) return;
      ed.preview = p;
      ed.problems = p.problems || [];
    } catch (e) {
      if (seq !== ed.seq) return;
      ed.preview = null;
      ed.problems = [{ message: e.message }];
    }
    ed.checking = false;
    render();
  }

  async function submitDraft() {
    ed.reason = L.detail.querySelector('[name="reason"]')?.value ?? ed.reason;
    if (!ed.reason.trim()) return toast("A policy change needs a reason", "bad");
    const tenantEdit = layer() === "tenant";
    const base = tenantEdit ? tenantBase(st.tenant) : "/v1/policy";
    const curVer = tenantEdit ? (tp && tp.version ? tp.version.version : null) : sp && sp.version ? sp.version.version : null;
    ed.busy = true;
    render();
    try {
      const c = await api(base, { method: "POST", body: { policy: ed.text, reason: ed.reason.trim(), base_version: curVer } });
      toast("Change requested", "ok", { href: href.change(c.id), label: "open change" });
      ed.text = null;
      ed.preview = null;
      ed.step = "edit";
      ed.reason = "";
      refreshBadges(true);
      ed.busy = false;
      setQuery({ edit: "", tab: "" });
      load();
    } catch (e) {
      ed.busy = false;
      toast(e.message, "bad");
      render();
    }
  }

  // ---- render + events -----------------------------------------------------------------
  function render() {
    setDelta("");
    setContext("");
    if (err) {
      setCrumbs(["Govern", "Policy"]);
      patch(L.detail, html`${head("Policy", [])}${alert("Could not load the policy.", html`The server answered ${err.status || "with an error"}. Enforcement is unaffected; only this page failed.`, "bad", btn("Retry", { icon: "refresh", data: { retry: "1" } }))}`);
      return;
    }
    if (!sp && !tp) {
      setCrumbs(["Govern", "Policy"]);
      patch(L.detail, html`${head("Policy", [])}${skelCards(3)}`);
      return;
    }
    if (st.edit) {
      if (ed.text == null) {
        const tenantEdit = layer() === "tenant";
        ed.text = tenantEdit ? (tp && tp.yaml) || tenantStarter(st.tenant) : (sp && sp.yaml) || STARTER;
        validateDraft();
      }
      patch(L.detail, editorView());
      return;
    }
    patch(L.detail, st.tenant && tp ? tenantView() : serverView());
  }

  L.detail.addEventListener("click", (e) => {
    const t = e.target.closest("[data-tab]");
    if (t) return setQuery({ tab: t.dataset.tab === "Rules" ? "" : t.dataset.tab }, { replace: true });
    if (e.target.closest("[data-retry]")) return load();
    if (e.target.closest("[data-edit]")) return setQuery({ edit: "1" });
    if (e.target.closest("[data-discard]")) { ed.text = null; ed.preview = null; ed.problems = []; ed.step = "edit"; return setQuery({ edit: "" }); }
    if (e.target.closest("[data-review]") && ed.preview && ed.preview.valid) { ed.step = "review"; return render(); }
    if (e.target.closest("[data-back]")) { ed.step = "edit"; return render(); }
    if (e.target.closest("[data-submit]")) return submitDraft();
    const m = e.target.closest("[data-dry-mode]");
    if (m) {
      dry.mode = m.dataset.dryMode;
      if (dry.mode === "template" && !dry.templates) {
        api("/v1/templates?kind=pipeline").then((r) => { dry.templates = (r.templates || r || []).filter((x) => !x.kind || x.kind === "pipeline"); render(); }).catch(() => { dry.templates = []; render(); });
      }
      return render();
    }
    if (e.target.closest("[data-dry-run]")) return runDry();
    const rb = e.target.closest("[data-rollback]");
    if (rb) return rollback(rb.dataset.rollback, rb.dataset.rbTenant || "");
  });
  L.detail.addEventListener("input", (e) => {
    if (e.target.name === "draft") {
      ed.text = e.target.value;
      clearTimeout(ed.timer);
      ed.timer = setTimeout(validateDraft, 450);
    }
    if (e.target.name === "dry-config") dry.text = e.target.value;
    if (e.target.name === "dry-sample") dry.sampleText = e.target.value;
    if (e.target.name === "dry-row") dry.row = e.target.value;
    if (e.target.name === "reason") ed.reason = e.target.value;
  });
  L.detail.addEventListener("change", (e) => {
    if (e.target.name === "dry-sample-on") { dry.sample = e.target.checked; render(); }
    if (e.target.name === "dry-template") dry.template = e.target.value;
    if (e.target.name === "ref-tenant") { st.refTenant = e.target.value; refusals = null; render(); loadRefusals(); }
  });

  load();
  return {
    update(p) {
      const next = read(p);
      const reload = next.tenant !== st.tenant;
      const wasEdit = st.edit;
      st = { ...st, ...next };
      if (wasEdit && !st.edit) { ed.text = null; ed.preview = null; ed.problems = []; ed.step = "edit"; }
      if (reload) { sp = null; tp = null; load(); return; }
      render();
      if (st.tab === "Refusals" && !refusals) loadRefusals();
    },
    teardown() { clearTimeout(ed.timer); },
  };
}

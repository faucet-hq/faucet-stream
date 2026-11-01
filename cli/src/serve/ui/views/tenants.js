// Tenants (#998, #709): split pane — customers the server runs pipelines for,
// beside the selected one's usage, runs, limits, labels and sealed connections.
// A tenant-scoped principal sees only its own tenant (canvas, read-only limits).
import { api } from "../api.js";
import { html, raw, v, ic, row, ring, sect, tabs, ibtn, empty, skelRows, skelCards, head, stats, card, tbl, pill, chip, kv, grid, btn, dhead, dtabs, lnk, join, alert, cpi, field, select, input, meta, dotText, spark, shortId } from "../components.js";
import { layout, setQuery, patch, errorAlert, hashFor, poll, ago, dialog, toast, sections, confirmAction } from "../page.js";
import { setCrumbs, setContext, refreshBadges } from "../shell.js";
import { fmtInt, fmtAge, fmtCompact } from "../utils.js";
import { formatTs } from "../tz.js";
import { href } from "../links.js";
import { whoami, can } from "../access.js";
import { setTenant, currentTenant } from "../tenant.js";
import { navigate } from "../router.js";

const LIMITS = [
  ["max_concurrent_runs", "Concurrent runs", (x) => `${fmtInt(x)} concurrent`],
  ["max_records_per_run", "Records per run", (x) => `${fmtCompact(x)} rows/run`],
  ["max_bytes_per_run", "Bytes per run", (x) => `${fmtBytes(x)}/run`],
  ["max_duration_secs", "Seconds per run", (x) => `${fmtInt(x)}s/run`],
];
const CONN_TYPES = ["static", "oauth2", "oauth2_refresh", "token_endpoint", "flow"];
const RUN_TONE = { running: "info", sharded: "info", queued: "warn", pending: "warn", completed: "ok", failed: "bad", cancelled: "muted" };
const CHG_RING = { pending: ["warn", "hollow"], approved: ["info", "half"], executed: ["ok", ""], rejected: ["bad", ""], failed: ["bad", ""], invalidated: ["bad", ""], expired: ["", "hollow"] };

export function fmtBytes(b) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let x = Number(b) || 0;
  let i = 0;
  while (x >= 1024 && i < units.length - 1) { x /= 1024; i += 1; }
  return i === 0 ? `${fmtInt(x)} B` : `${x.toFixed(1)} ${units[i]}`;
}
const money = (x, cur = "USD") => {
  const n = Number(x) || 0;
  const s = n !== 0 && n < 0.01 ? n.toFixed(4) : n.toFixed(2);
  return cur === "USD" ? `$${Number(s).toLocaleString("en-US", { minimumFractionDigits: s.split(".")[1].length })}` : `${cur} ${s}`;
};
const reauth = (t) => (t.connections || []).filter((c) => c.status === "needs_reauth").length;
const limitsText = (l) => {
  const parts = LIMITS.filter(([k]) => l && l[k] != null).map(([k, , f]) => f(l[k]));
  return parts.length ? parts.join(" · ") : "no limits";
};
const who = (p) => (can("audit_read") ? lnk(p, href.audit({ principal: p })) : html`<b>${p}</b>`);
const daysAgo = (n) => new Date(Date.now() - n * 86400000).toISOString().slice(0, 10);

export async function renderTenants(host, params) {
  const me = whoami();
  const scoped = !!(me && me.tenant);
  const L = layout(host, scoped ? "canvas" : "split");
  const secs = sections("faucet.tenants.sections");
  let tenants = null;
  let listErr = null;
  const read = (p) => ({ id: scoped ? me.tenant : p.id || "", tab: p.query.get("tab") || "Overview", create: p.query.get("new") === "1", q: p.query.get("q") || "", search: p.query.get("search") === "1" });
  let state = read(params);
  let t = null;
  let tErr = null;
  let extra = {};
  let providers = null;
  let detailGen = 0;
  let storeErr = "";
  const listHref = () => hashFor(href.tenants(), { q: state.q, search: state.search ? "1" : "" });
  const tHref = (tid) => hashFor(href.tenant(tid), { q: state.q, search: state.search ? "1" : "" });

  function tenantRow(x) {
    const n = reauth(x);
    const [tone, shape] = x.suspended ? ["", "hollow"] : n ? ["warn", "half"] : ["ok", ""];
    const lead = x.suspended ? "suspended" : n ? `${n} needs re-auth` : x.active_runs ? `${fmtInt(x.active_runs)} running` : "idle";
    const conns = (x.connections || []).length;
    return row(ring(tone, shape), x.name || x.id, html`<time datetime="${x.updated_at}" title="updated ${formatTs(x.updated_at)}">${fmtAge(x.updated_at)}</time>`, meta(lead, `${conns} connection${conns === 1 ? "" : "s"}`, x.suspended ? "runs refused" : limitsText(x.limits)), { on: x.id === state.id, href: tHref(x.id), id: x.id, tip: `${x.name || x.id} · ${x.id}` });
  }

  function renderList() {
    if (scoped) return;
    if (!tenants) {
      L.list.innerHTML = v(listErr ? errorAlert("tenants", listErr) : html`${tabs(["Tenants"], "Tenants")}${stats([["Tenants", "–"], ["Running", "–"], ["Need re-auth", "–"]])}${skelRows(5)}`);
      L.list.querySelector("[data-retry]")?.addEventListener("click", load);
      return;
    }
    const q = state.q.toLowerCase();
    const shown = tenants.filter((x) => !q || `${x.id} ${x.name || ""} ${Object.entries(x.labels || {}).map(([k, val]) => `${k}=${val}`).join(" ")}`.toLowerCase().includes(q));
    const tools = html`${ibtn("filter", { title: "Search tenants", on: state.search, dot: !!state.q, data: { search: "1" } })}${ibtn("refresh", { title: "Refresh", data: { reload: "1" } })}${ibtn("plus", { title: "Create a tenant", perm: "tenant_admin", href: hashFor(href.tenants(), { new: "1" }) })}`;
    const running = tenants.reduce((s, x) => s + (x.active_runs || 0), 0);
    const need = tenants.reduce((s, x) => s + reauth(x), 0);
    const group = (sid, label, rows) => {
      if (!rows.length) return "";
      const closed = secs.isClosed(sid);
      return html`${sect(label, rows.length, { id: sid, closed })}<div class="sect-rows${closed ? " closed" : ""}">${rows.map(tenantRow)}</div>`;
    };
    const body = !tenants.length
      ? empty("No tenants yet", "Create one to scope runs, limits and connections to a customer.", { btns: btn("Create tenant", { kind: "primary", icon: "plus", perm: "tenant_admin", href: hashFor(href.tenants(), { new: "1" }) }) })
      : !shown.length ? empty(`No tenant matches “${state.q}”`, html`<a class="lnk" href="${hashFor(href.tenants(), { search: "1" })}">Clear the search</a>`, { icon: "search" })
      : html`${group("active", "Active", shown.filter((x) => !x.suspended))}${group("suspended", "Suspended", shown.filter((x) => x.suspended))}`;
    const top = html`${tabs(["Tenants"], "Tenants", tools)}${stats([["Tenants", fmtInt(tenants.length)], ["Running", fmtInt(running), running ? "info" : ""], ["Need re-auth", fmtInt(need), need ? "bad" : ""]])}`;
    if (!L.list.querySelector(".list-body")) {
      L.list.innerHTML = v(html`<div class="list-top" style="display:contents"></div><div class="list-search" style="display:contents"></div><div class="list-body rows"></div>`);
    }
    patch(L.list.querySelector(".list-top"), top);
    const searchSlot = L.list.querySelector(".list-search");
    if (state.search && !searchSlot.querySelector("input")) {
      searchSlot.innerHTML = v(input(state.q, { ph: "Search by id, name or label", icon: "search", type: "search", name: "q" }));
      const qi = searchSlot.querySelector("input");
      qi.addEventListener("input", () => setQuery({ q: qi.value }, { replace: true }));
      qi.focus();
    } else if (!state.search) searchSlot.innerHTML = "";
    patch(L.list.querySelector(".list-body"), body);
  }
  if (!scoped) {
    L.list.addEventListener("click", (e) => {
      if (e.target.closest("[data-reload]")) { load(); if (state.id) loadDetail(true); }
      if (e.target.closest("[data-search]")) setQuery({ search: state.search ? "" : "1", q: state.search ? "" : state.q }, { replace: true });
    });
    L.list.addEventListener("sect-toggle", (e) => secs.toggle(e.detail.id));
  }

  // ---- detail ----
  function noneDetail() {
    setCrumbs(["Govern", "Tenants"]);
    setContext("");
    if (!tenants) return html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(2)}`;
    if (!tenants.length) return empty("Run pipelines for your customers", "A tenant owns sealed connections, per-run limits and its own state namespace. Template runs and triggers can fan out to every tenant.", { icon: "tenants" });
    return empty("Pick a tenant", "Its usage, recent runs, limits, labels and connections appear here.", { icon: "tenants" });
  }

  function createDetail() {
    setCrumbs(["Govern", { label: "Tenants", href: href.tenants() }, "New tenant"]);
    setContext("");
    return html`${dhead("", "", { href: listHref(), label: "Tenants" })}${head("New tenant", [html`<span>an id is permanent; the name can change later</span>`])}
      <form id="tn-create" class="stack">
        ${card({ title: "Tenant", body: html`<div class="form">${field("Id", input("", { mono: true, name: "id", ph: "initech-eu", a: { required: true, pattern: "[a-z0-9][a-z0-9-]*", autofocus: true } }), { req: true, help: "Lowercase letters, digits and dashes · used in state keys and URLs" })}${field("Display name", input("", { name: "name", ph: "Initech Europe" }))}</div>`, note: "Limits, labels and connections can be added after it exists." })}
        <div style="display:flex;gap:8px;justify-content:flex-end">${btn("Cancel", { href: listHref() })}${btn("Create tenant", { kind: "primary", icon: "plus", type: "submit" })}</div>
      </form>`;
  }

  function usageCard() {
    const u = extra.usage;
    if (u === undefined) return card({ icon: "usage", title: "Usage", aside: "last 30 days", body: html`<div class="pad">${skelRows(1)}</div>` });
    if (u === null) return "";
    const tot = u.total || {};
    const pts = (u.rows || []).slice().sort((a, b) => String(a.key).localeCompare(String(b.key))).map((r) => r.records_written || 0);
    return card({
      icon: "usage", title: "Usage", aside: "last 30 days", right: lnk("open in Usage", hashFor(href.usage(), { tenant: t.id, since: daysAgo(30) })),
      body: grid(4, [kv("Runs", fmtInt(tot.runs || 0), tot.failed_runs ? `${fmtInt(tot.failed_runs)} failed` : "none failed"), kv("Rows written", fmtCompact(tot.records_written || 0), pts.length > 1 ? spark(pts, "var(--text-2)", 130, 26) : ""), kv("Bytes written", fmtBytes(tot.bytes_written || 0)), kv("Estimated cost", money(tot.cost, u.currency), `hosted equivalent ${money(tot.hosted_equivalent, u.currency)}`)]),
    });
  }
  function runsCard() {
    const rs = extra.runs;
    const body = rs === undefined ? html`<div class="pad">${skelRows(3)}</div>` : !rs || !rs.length ? html`<div class="empty-line">No runs for this tenant yet.</div>` : tbl(["Run", "Status", ["Rows", "num"], ["When", "num"]], rs.map((r) => ({ href: href.run(r.run_id), cells: [lnk(r.name || (r.labels && r.labels.pipeline) || shortId(r.run_id), href.run(r.run_id), !(r.name || (r.labels && r.labels.pipeline)), r.run_id), html`${dotText(RUN_TONE[r.status] || "muted", r.status)}${r.status === "failed" && r.error ? html`<div class="muted" style="font-size:12px">${String(r.error).split("\n")[0].slice(0, 60)}</div>` : ""}`, fmtInt(r.records_written || 0), ago(r.submitted_at, " ago")] })));
    return card({ icon: "runs", title: "Recent runs", right: lnk(`all runs for ${t.id}`, href.runs({ tenant: t.id })), body });
  }
  function changesCard() {
    const cs = (extra.changes || []).filter((c) => c.status === "pending");
    if (!can("change_read") || extra.changes === null) return "";
    return card({ icon: "changes", title: "Pending changes", right: String(cs.length), body: cs.length ? html`<div class="tn-chg">${cs.map((c) => { const [tone, shape] = CHG_RING[c.status] || ["", ""]; return row(ring(tone, shape), c.reason || c.kind, fmtAge(c.created_at), meta(c.kind.replace(/_/g, " "), `${(c.approvals || []).length} of ${c.required_approvals} approvals`), { href: href.change(c.id) }); })}</div>` : html`<div class="empty-line">Nothing waits for approval.</div>` });
  }
  function auditCard(n = 3) {
    if (!can("audit_read") || extra.audit === null) return "";
    const es = (extra.audit || []).slice(0, n);
    return card({ icon: "audit", title: "Recent audit", right: lnk("open in Audit log", href.audit({ tenant: t.id })), body: extra.audit === undefined ? html`<div class="pad">${skelRows(2)}</div>` : es.length ? tbl(["Who and what", ["When", "num"]], es.map((e) => ({ href: href.auditEntry(e.id), cells: [html`${who(e.principal)}<div class="muted mono">${e.action}</div>`, ago(e.timestamp, " ago")] }))) : html`<div class="empty-line">No audit entries for this tenant.</div>` });
  }
  function labelsCard() {
    const ls = Object.entries(t.labels || {});
    return card({ title: "Labels", right: "set through the API", body: ls.length ? html`<div class="pad" style="display:flex;gap:6px;flex-wrap:wrap">${ls.map(([k, val]) => chip(`${k}=${val}`, "mono"))}</div>` : html`<div class="empty-line">No labels.</div>` });
  }
  function limitsCard() {
    const l = t.limits || {};
    if (scoped || !can("tenant_admin")) {
      return card({ title: "Limits", aside: "set by the operator", body: grid(4, LIMITS.map(([k, label]) => kv(label, l[k] != null ? fmtInt(l[k]) : "none", k === "max_concurrent_runs" && l[k] != null ? `${fmtInt(t.active_runs || 0)} in use` : ""))) });
    }
    return card({
      title: "Limits", aside: "ceilings every run of this tenant honours",
      body: html`<form id="tn-limits"><div class="form" style="grid-template-columns:repeat(4,minmax(0,1fr))">${LIMITS.map(([k, label]) => field(label, input(l[k] != null ? String(l[k]) : "", { name: k, ph: "none", type: "number", a: { min: "1" } }), { help: k === "max_concurrent_runs" ? `${fmtInt(t.active_runs || 0)} in use` : "" }))}</div><div class="actions"><span class="muted">Blank means no limit. Changes apply to the next run.</span>${btn("Save limits", { kind: "primary", type: "submit" })}</div></form>`,
    });
  }
  function deleteCard() {
    if (scoped) return "";
    return html`<div data-perm="tenant_admin">${card({ title: "Delete tenant", aside: "removes its runs, usage, change requests, state and connections", body: html`<div class="form one" style="grid-template-columns:1fr auto;align-items:end">${field(html`Type <code>${t.id}</code> to confirm`, input("", { name: "confirm", ph: `type ${t.id} to delete`, mono: true }))}${btn("Delete tenant", { kind: "danger", icon: "trash", data: { del: "1" }, disabled: true })}</div>` })}</div>`;
  }

  function overviewPane() {
    return html`${usageCard()}<div class="two"><div class="stack">${runsCard()}${scoped ? "" : changesCard()}</div><div class="stack">${labelsCard()}${scoped ? "" : auditCard()}</div></div>${limitsCard()}${deleteCard()}`;
  }

  function connectionsPane() {
    const conns = t.connections || [];
    const mng = can("connection_manage");
    const rows = conns.map((c) => [
      html`<span class="mono">${c.name}</span>`,
      html`${c.provider_type}${c.connect_provider ? html` · via provider <span class="mono">${c.connect_provider}</span>` : ""}`,
      html`${c.status === "needs_reauth" ? dotText("warn", "needs re-auth") : dotText("ok", "active")}${c.reauth_reason ? html`<div class="muted" style="font-size:12px">${c.reauth_reason}</div>` : ""}`,
      html`${who(c.updated_by || "?")} · ${ago(c.updated_at, " ago")}`,
      mng ? html`<span class="tn-acts">${c.connect_provider ? btn("Reconnect", { cls: "sm", icon: "refresh", data: { reconnect: c.name, provider: c.connect_provider } }) : ""}${btn("", { cls: "sm quiet", icon: "trash", title: `Delete connection ${c.name}`, data: { delconn: c.name } })}</span>` : "",
    ]);
    const ps = providers || [];
    const back = location.href;
    const forms = mng && !scoped ? html`<div class="pair">
      ${card({ title: "Connect through a provider", aside: "opens the provider’s sign-in", body: ps.length ? html`<form id="tn-connect"><div class="form one">${field("Provider", select(ps.map((p) => [p.name, p.name]), ps[0].name, { name: "provider" }))}${field("Connection name", input("", { mono: true, name: "connection", ph: "crm", a: { required: true } }), { req: true })}${field("Return to", input(back, { name: "redirect" }), { help: "Where the provider sends the browser back" })}</div><div class="actions">${btn("Connect", { kind: "primary", icon: "link", type: "submit" })}</div></form>` : html`<div class="empty-line">No providers configured. Start the server with <code>--connect-providers</code>.</div>` })}
      ${card({ title: "Store credentials", aside: "sealed at rest", body: html`<form id="tn-store"><div class="form one">${field("Name", input("", { mono: true, name: "sname", ph: "erp", a: { required: true } }), { req: true })}${field("Type", select(CONN_TYPES, "static", { name: "stype" }))}${field("Config", html`<textarea class="area${storeErr ? " err" : ""}" name="sconfig" rows="5" placeholder='{ "token": "\${secret:ERP_TOKEN}" }' spellcheck="false"></textarea>`, storeErr ? { err: storeErr } : { help: raw("JSON · use <code>${secret:…}</code> references, never raw secrets") })}</div><div class="actions">${btn("Save", { kind: "primary", type: "submit" })}</div></form>` })}
    </div>` : "";
    return html`${card({ icon: "key", title: "Connections", aside: "credentials are sealed; only names, types and status are shown", right: String(conns.length), body: conns.length ? tbl(["Name", "Type", "Status", "Updated", ""], rows) : html`<div class="empty-line">No connections yet.${mng ? " Add one below." : ""}</div>` })}${forms}`;
  }

  function activityPane() {
    const cs = extra.changes || [];
    return html`${can("audit_read") && extra.audit !== null ? card({ icon: "audit", title: "Audit", aside: "newest first", right: lnk("open in Audit log", href.audit({ tenant: t.id })), body: (extra.audit || []).length ? tbl(["Who", "Action", "Result", ["When", "num"]], extra.audit.map((e) => ({ href: href.auditEntry(e.id), cells: [who(e.principal), html`<span class="mono">${e.action}</span>`, dotText(e.result === "ok" ? "ok" : "bad", e.result), ago(e.timestamp, " ago")] }))) : html`<div class="empty-line">No audit entries for this tenant.</div>` }) : ""}
      ${can("change_read") ? card({ icon: "changes", title: "Change requests", right: String(cs.length), body: cs.length ? html`<div class="tn-chg">${cs.map((c) => { const [tone, shape] = CHG_RING[c.status] || ["", ""]; return row(ring(tone, shape), c.reason || c.kind, fmtAge(c.created_at), meta(c.status, c.kind.replace(/_/g, " "), `by ${c.requester}`), { href: href.change(c.id) }); })}</div>` : html`<div class="empty-line">No change requests for this tenant.</div>` }) : ""}`;
  }

  function tenantDetail() {
    const name = t.name || t.id;
    setCrumbs(scoped ? ["Govern", "Tenants", name] : ["Govern", { label: "Tenants", href: listHref() }, name]);
    setContext(t.active_runs ? chip(`${fmtInt(t.active_runs)} running`) : "");
    const n = reauth(t);
    const status = t.suspended ? pill("Suspended", "", "pause") : n ? pill(`${n} need${n === 1 ? "s" : ""} re-auth`, "warn") : pill("Active", "ok", "check");
    const conns = (t.connections || []).length;
    const tabItems = scoped ? [["Overview", null, "Overview"], ["Connections", conns, "Connections"]] : [["Overview", null, "Overview"], ["Connections", conns, "Connections"], ["Activity", null, "Activity"]];
    const right = scoped
      ? btn("Runs", { icon: "runs", href: href.runs({ tenant: t.id }) })
      : html`${btn("Runs for this tenant", { icon: "runs", href: href.runs({ tenant: t.id }) })}${t.suspended ? btn("Resume", { kind: "primary", icon: "play", perm: "tenant_admin", data: { suspend: "0" } }) : btn("Suspend", { kind: "danger", icon: "pause", perm: "tenant_admin", data: { suspend: "1" } })}`;
    const sub = scoped
      ? [status, html`<span>you are signed in as <b>${me.principal}</b> · tenant ${t.id}</span>`]
      : [status, html`<span>id <span class="mono">${t.id}</span>${cpi(t.id)}</span>`, html`<span>created by ${who(t.created_by || "?")} · ${ago(t.created_at, " ago")}</span>`, html`<span>${fmtInt(t.active_runs || 0)} active run${t.active_runs === 1 ? "" : "s"}</span>`];
    const body = state.tab === "Connections" ? connectionsPane() : state.tab === "Activity" && !scoped ? activityPane() : overviewPane();
    return html`${dhead(dtabs(tabItems, state.tab), right, scoped ? null : { href: listHref(), label: "Tenants" })}${head(name, sub)}${scoped ? alert("You see only this tenant.", "Limits are set by the operator; other tenants, audit and server settings are not shown.") : ""}${t.suspended && !scoped ? alert("Suspended.", "New runs, triggers and fan-outs for this tenant are refused until it is resumed.", "warn") : ""}${body}`;
  }

  function renderDetail() {
    if (!scoped) L.select(!!state.id || state.create);
    if (state.create && !scoped) { patch(L.detail, createDetail()); return; }
    if (!state.id) { patch(L.detail, noneDetail()); return; }
    if (tErr) {
      setCrumbs(["Govern", { label: "Tenants", href: listHref() }, state.id]);
      patch(L.detail, html`${scoped ? "" : dhead("", "", { href: listHref(), label: "Tenants" })}${tErr.status === 404 ? empty("Tenant not found", "It may have been deleted, or the id is wrong.", { icon: "tenants", btns: scoped ? "" : btn("All tenants", { href: href.tenants() }) }) : errorAlert("this tenant", tErr)}`);
      L.detail.querySelector("[data-retry]")?.addEventListener("click", () => loadDetail());
      return;
    }
    if (!t || t.id !== state.id) { L.detail.__html = ""; L.detail.innerHTML = v(html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`); return; }
    // Keep what the user typed in a form across a background refresh.
    const typed = [...L.detail.querySelectorAll("form input, form textarea, form select, input[name=confirm]")].map((el) => [el.name, el.value]);
    const changed = patch(L.detail, tenantDetail());
    if (changed) {
      for (const [n, val] of typed) {
        const el = L.detail.querySelector(`[name="${CSS.escape(n)}"]`);
        if (el && val) el.value = val;
      }
      const cf = L.detail.querySelector("input[name=confirm]");
      if (cf) L.detail.querySelector("[data-del]").disabled = cf.value.trim() !== t.id;
    }
  }

  async function loadExtras(tid, gen) {
    const safe = (p) => p.catch(() => null);
    const [usage, runs, changes, audit] = await Promise.all([
      safe(api(`/v1/usage?${new URLSearchParams({ by: "day", tenant: tid, since: daysAgo(30) })}`).then((r) => r.report)),
      safe(api(`/v1/runs?${new URLSearchParams({ tenant: tid, limit: "5" })}`).then((r) => r.runs || [])),
      can("change_read") ? safe(api(`/v1/changes?${new URLSearchParams({ tenant: tid, limit: "50" })}`).then((r) => (Array.isArray(r) ? r : r.changes || []))) : Promise.resolve(null),
      can("audit_read") && !scoped ? safe(api(`/v1/audit?${new URLSearchParams({ tenant: tid, limit: "50" })}`).then((r) => r.entries || [])) : Promise.resolve(null),
    ]);
    if (gen !== detailGen) return;
    extra = { usage, runs, changes, audit };
    renderDetail();
  }

  async function loadDetail(keep = false) {
    const tid = state.id;
    if (!tid || state.create) { renderDetail(); return; }
    const gen = ++detailGen;
    if (!keep) { t = null; tErr = null; extra = {}; renderDetail(); }
    try {
      const [x, ps] = await Promise.all([api(`/v1/tenants/${encodeURIComponent(tid)}`), providers === null ? api("/v1/connect/providers").catch(() => []) : Promise.resolve(providers)]);
      if (gen !== detailGen) return;
      t = x;
      providers = ps || [];
      tErr = null;
    } catch (e) {
      if (gen !== detailGen) return;
      if (!keep || !t) { t = null; tErr = e; }
    }
    renderDetail();
    if (t) loadExtras(tid, gen);
  }

  const refreshAll = async () => { await load(); await loadDetail(true); refreshBadges(true); };

  L.detail.addEventListener("click", async (e) => {
    const tb = e.target.closest("[data-tab]");
    if (tb) return setQuery({ tab: tb.dataset.tab === "Overview" ? "" : tb.dataset.tab }, { replace: true });
    const sp = e.target.closest("[data-suspend]");
    if (sp && t) {
      const suspend = sp.dataset.suspend === "1";
      if (suspend) {
        const ok = await confirmAction(`Suspend ${t.name || t.id}?`, html`New runs, triggers and fan-outs for <b>${t.id}</b> are refused until you resume it.${t.active_runs ? html` The <b>${fmtInt(t.active_runs)} running</b> run${t.active_runs === 1 ? "" : "s"} finish; nothing is deleted.` : " Nothing is deleted."}`, { label: "Suspend tenant", danger: true, icon: "pause" });
        if (!ok) return;
      }
      try {
        await api(`/v1/tenants/${encodeURIComponent(t.id)}`, { method: "PATCH", body: { suspended: suspend } });
        toast(suspend ? `Tenant ${t.id} suspended` : `Tenant ${t.id} resumed`, "ok");
      } catch (err) { toast(err.message, "bad"); }
      return refreshAll();
    }
    if (e.target.closest("[data-del]") && t) {
      const tid = t.id;
      const conns = (t.connections || []).length;
      const ok = await confirmAction(`Delete ${t.name || t.id} permanently?`, html`This removes its runs, their usage records, change requests, state keys and <b>${conns} connection${conns === 1 ? "" : "s"}</b>. It cannot be undone.`, { label: "Delete tenant", danger: true, icon: "trash" });
      if (!ok) return;
      try {
        const res = await api(`/v1/tenants/${encodeURIComponent(tid)}`, { method: "DELETE" });
        toast(`Deleted ${tid}: ${fmtInt(res && res.runs || 0)} run(s), ${fmtInt(res && res.state_keys_deleted || 0)} state key(s)`, "ok");
        if (currentTenant() === tid) setTenant("");
        await load();
        navigate(listHref());
        refreshBadges(true);
      } catch (err) { toast(err.message, "bad"); }
      return;
    }
    const dc = e.target.closest("[data-delconn]");
    if (dc && t) {
      const name = dc.dataset.delconn;
      const ok = await confirmAction(`Delete connection ${name}?`, html`Runs of <b>${t.id}</b> that reference <code>auth: { ref: ${name} }</code> fail until it is stored again.`, { label: "Delete connection", danger: true, icon: "trash" });
      if (!ok) return;
      try {
        await api(`/v1/tenants/${encodeURIComponent(t.id)}/connections/${encodeURIComponent(name)}`, { method: "DELETE" });
        toast(`Connection ${name} deleted`, "ok");
      } catch (err) { toast(err.message, "bad"); }
      return refreshAll();
    }
    const rc = e.target.closest("[data-reconnect]");
    if (rc && t) startConnect(rc.dataset.provider, rc.dataset.reconnect, location.href);
  });
  L.detail.addEventListener("input", (e) => {
    if (e.target.name === "confirm" && t) L.detail.querySelector("[data-del]").disabled = e.target.value.trim() !== t.id;
    if (e.target.name === "sconfig") {
      const val = e.target.value.trim();
      let err = "";
      if (val) { try { JSON.parse(val); } catch (x) { err = `Not valid JSON: ${x.message}`; } }
      const help = e.target.closest(".field").querySelector(".help");
      e.target.classList.toggle("err", !!err);
      help.classList.toggle("bad", !!err);
      help.innerHTML = err ? v(err) : v(html`JSON · use <code>\${secret:…}</code> references, never raw secrets`);
      e.target.closest("form").querySelector("[type=submit]").classList.toggle("dis", !!err);
    }
  });

  async function startConnect(provider, connection, redirect) {
    try {
      const r = await api(`/v1/tenants/${encodeURIComponent(t.id)}/connect/${encodeURIComponent(provider)}`, { method: "POST", body: { connection, redirect } });
      window.open(r.authorize_url, "_blank", "noopener");
      toast("The provider’s sign-in opened in a new tab", "info");
    } catch (err) { toast(err.message, "bad"); }
  }

  host.addEventListener("submit", async (e) => {
    e.preventDefault();
    const f = e.target;
    if (f.id === "tn-create") {
      const tid = f.elements.id.value.trim();
      const name = f.elements.name.value.trim();
      try {
        await api("/v1/tenants", { method: "POST", body: name ? { id: tid, name } : { id: tid } });
        toast(`Tenant ${tid} created`, "ok");
        await load();
        navigate(href.tenant(tid));
        refreshBadges(true);
      } catch (err) { toast(err.message, "bad"); }
    } else if (f.id === "tn-limits") {
      const next = {};
      for (const [k] of LIMITS) { const val = f.elements[k].value.trim(); if (val) next[k] = Number(val); }
      try {
        await api(`/v1/tenants/${encodeURIComponent(t.id)}`, { method: "PATCH", body: { limits: next } });
        toast("Limits saved", "ok");
      } catch (err) { toast(err.message, "bad"); }
      refreshAll();
    } else if (f.id === "tn-connect") {
      startConnect(f.elements.provider.value, f.elements.connection.value.trim(), f.elements.redirect.value.trim());
    } else if (f.id === "tn-store") {
      let config;
      try { config = JSON.parse(f.elements.sconfig.value); } catch (x) {
        storeErr = `Not valid JSON: ${x.message}`;
        renderDetail();
        storeErr = "";
        return;
      }
      const name = f.elements.sname.value.trim();
      try {
        await api(`/v1/tenants/${encodeURIComponent(t.id)}/connections/${encodeURIComponent(name)}`, { method: "PUT", body: { provider: { type: f.elements.stype.value, config } } });
        toast(`Connection ${name} saved`, "ok");
        f.reset();
      } catch (err) { toast(err.message, "bad"); }
      refreshAll();
    }
  });

  async function load() {
    if (scoped) return;
    try {
      const x = await api("/v1/tenants");
      tenants = (x || []).slice().sort((a, b) => String(a.name || a.id).localeCompare(String(b.name || b.id)));
      listErr = null;
    } catch (e) {
      if (e.status === 404) return off();
      if (!tenants) listErr = e;
    }
    renderList();
    if (!state.id && !state.create) renderDetail();
  }

  let isOff = false;
  function off() {
    isOff = true;
    layout(host, "canvas");
    const d = host.querySelector(".detail");
    setCrumbs(["Govern", "Tenants"]);
    d.innerHTML = v(empty("This server was built without tenants", html`Multi-tenant integrations need the <code>tenants</code> feature and a vault key (<code>--vault-key</code>). The rest of the console works without it.`, { icon: "tenants" }));
  }

  renderList();
  renderDetail();
  await load();
  if (isOff) return null;
  if (state.id) loadDetail();
  const stop = poll(async () => { await load(); if (state.id && !state.create) await loadDetail(true); }, 15000);

  return {
    teardown: stop,
    update(p) {
      if (isOff) return;
      const next = read(p);
      const idChanged = next.id !== state.id || next.create !== state.create;
      state = next;
      renderList();
      if (idChanged) loadDetail();
      else renderDetail();
    },
  };
}

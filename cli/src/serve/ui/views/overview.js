// Overview (#998): the home page (canvas) — what needs an operator now. Trends
// come from `GET /v1/overview` (#1006); running runs, change requests,
// templates and server health from their own list endpoints. `?range=7d`
// switches Today ↔ This week.
import { api } from "../api.js";
import {
  html, v, ic, row, ring, dhead, dtabs, btn, head, card, tbl, kv, grid, lnk, spark, steps, empty, meta, chip, skelCards, join,
} from "../components.js";
import { layout, poll, patch, errorAlert, hashFor } from "../page.js";
import { setContext, readiness } from "../shell.js";
import { fmtInt, fmtCompact, fmtAge } from "../utils.js";
import { formatTs, getTz } from "../tz.js";
import { href, datasetName } from "../links.js";
import { withTenant, tenantList } from "../tenant.js";
import { whoami } from "../access.js";
import { runTitle, runKinds, errorCode, money, fmtSecs } from "./runs.js";

const RANGES = { "24h": ["Today", "Last 24 hours"], "7d": ["This week", "Last 7 days"] };
const ACTIVE = "running,sharded,queued,pending";

const clip = (s, n) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);
const since = (iso) => html`<time datetime="${iso}" title="${formatTs(iso)}">${fmtAge(iso)}</time>`;

function greeting() {
  let h = new Date().getHours();
  try {
    const tz = getTz();
    if (tz !== "local") h = Number(new Intl.DateTimeFormat("en-US", { hour: "numeric", hour12: false, timeZone: tz }).format(new Date())) % 24;
  } catch { /* the browser's own hour */ }
  return h < 5 ? "Good evening" : h < 12 ? "Good morning" : h < 18 ? "Good afternoon" : "Good evening";
}

export async function renderOverview(host, params) {
  const L = layout(host, "canvas");
  let range = RANGES[params.query.get("range")] ? params.query.get("range") : "24h";
  let data = null;
  let err = null;
  let alive = true;

  const safe = (p) => p.catch(() => null);
  async function load() {
    const t = withTenant(new URLSearchParams());
    const other = range === "24h" ? "7d" : "24h";
    const runSince = new Date(Date.now() - (range === "24h" ? 1 : 7) * 86400000).toISOString();
    const [ov, ovOther, active, failed, any, changes, templates] = await Promise.all([
      api(`/v1/overview?${new URLSearchParams({ ...Object.fromEntries(t), since: range, bucket: range === "24h" ? "hour" : "day" })}`).catch((e) => (e.status === 404 ? null : Promise.reject(e))),
      safe(api(`/v1/overview?${new URLSearchParams({ ...Object.fromEntries(t), since: other })}`)),
      safe(api(`/v1/runs?${withTenant(new URLSearchParams({ status: ACTIVE, limit: "6" }))}`)),
      safe(api(`/v1/runs?${withTenant(new URLSearchParams({ status: "failed", since: runSince, limit: "5" }))}`)),
      safe(api(`/v1/runs?${withTenant(new URLSearchParams({ limit: "1", counts: "true" }))}`)),
      safe(api(`/v1/changes?${withTenant(new URLSearchParams({ limit: "3" }))}`)),
      safe(api("/v1/templates")),
    ]);
    let fallback = null;
    if (!ov) fallback = await safe(api(`/v1/runs?${withTenant(new URLSearchParams({ since: runSince, limit: "500" }))}`));
    return { ov, ovOther, active, failed, any, changes, templates, fallback, at: new Date().toISOString() };
  }
  async function refresh() {
    try { data = await load(); err = null; } catch (e) { err = e; }
    if (alive) paint();
  }

  // ---- cards ----
  function activity() {
    const ov = data.ov;
    let total, ok, fin, recs, cost, runSeries, recSeries;
    if (ov) {
      total = ov.runs.total;
      ok = ov.runs.by_status.completed || 0;
      fin = ok + (ov.runs.by_status.failed || 0);
      recs = ov.records.total;
      cost = ov.cost;
      runSeries = ov.runs.series.map((b) => b.succeeded + b.failed + b.cancelled);
      recSeries = ov.records.series.map((b) => b.written);
    } else {
      const rs = (data.fallback && data.fallback.runs) || [];
      total = rs.length;
      ok = rs.filter((r) => r.status === "completed").length;
      fin = ok + rs.filter((r) => r.status === "failed").length;
      recs = rs.reduce((n, r) => n + (r.records_written || 0), 0);
    }
    const other = data.ovOther && data.ovOther.cost;
    const costSub = other ? `${range === "24h" ? "this week" : "today"} ${money(other.estimated, other.currency)}` : cost ? `${fmtInt(cost.invocations)} invocations priced` : "no usage recorded";
    const sp = (pts) => (pts && pts.some(Boolean) ? spark(pts, "var(--text-2)", 140, 26, null, 0) : "");
    return card({ icon: "usage", title: RANGES[range][1], right: lnk("Usage", href.usage()), body: grid(4, [
      kv("Runs", fmtInt(total), sp(runSeries) || "no runs in this range"),
      kv("Success rate", fin ? `${((ok / fin) * 100).toFixed(1)}%` : "—", `${fmtInt(ok)} of ${fmtInt(fin)} finished`),
      kv("Records written", fmtCompact(recs), sp(recSeries) || "nothing written"),
      kv("Estimated cost", cost ? money(cost.estimated, cost.currency) : "—", costSub),
    ]) });
  }

  function attentionItems() {
    const out = [];
    const ov = data.ov;
    for (const r of (data.failed && data.failed.runs) || []) {
      out.push({ tone: "bad", what: html`${lnk(runTitle(r), href.run(r.run_id))} failed · ${clip(errorCode(r), 40)}`, where: "run", at: r.finished_at || r.submitted_at, go: btn("Open", { cls: "sm", href: href.run(r.run_id) }) });
    }
    const a = (ov && ov.attention) || {};
    for (const t of (a.failing_templates && a.failing_templates.items) || []) {
      out.push({ tone: "bad", what: html`${lnk(t.id, href.template(t.id, { version: t.version }))} v${t.version} · ${fmtInt(t.failed_cases)} test case${t.failed_cases === 1 ? "" : "s"} failing`, where: "template", at: t.recorded_at, go: btn("Open", { cls: "sm", href: href.template(t.id) }) });
    }
    for (const d of (ov && ov.freshness && ov.freshness.stale) || []) {
      out.push({ tone: "warn", what: html`${lnk(datasetName(d.uri), href.dataset(d.id), false, d.uri)} stale · no success since ${fmtAge(d.last_success)}`, where: "dataset", at: d.last_success, go: btn("Open", { cls: "sm", href: href.dataset(d.id) }) });
    }
    for (const c of (a.pending_changes && a.pending_changes.items) || []) {
      out.push({ tone: "warn", what: html`${lnk(`${c.kind.replace("_", " ")} by ${c.requester}`, href.change(c.id))} awaits approval`, where: "change", at: c.created_at, go: btn("Review", { cls: "sm", href: href.change(c.id) }) });
    }
    for (const t of (a.unhealthy_triggers && a.unhealthy_triggers.items) || []) {
      out.push({ tone: "warn", what: html`${lnk(t.name, href.trigger(t.name))} ${t.kind} trigger failing · ${fmtInt(t.consecutive_failures)}×`, where: "trigger", at: null, go: btn("Open", { cls: "sm", href: href.trigger(t.name) }) });
    }
    for (const c of (a.connections_needing_reauth && a.connections_needing_reauth.items) || []) {
      const name = (tenantList() || []).find((t) => t.id === c.tenant)?.name || c.tenant;
      out.push({ tone: "warn", what: html`${lnk(name, href.tenant(c.tenant, "connections"))} connection <span class="mono">${c.name}</span> needs re-auth`, where: "tenant", at: null, go: btn("Open", { cls: "sm", href: href.tenant(c.tenant, "connections") }) });
    }
    const rank = { bad: 0, warn: 1 };
    return out.sort((x, y) => rank[x.tone] - rank[y.tone] || Date.parse(y.at || 0) - Date.parse(x.at || 0));
  }
  function attention(items) {
    const shown = items.slice(0, 6);
    return card({ icon: "alert", title: "Needs attention", right: fmtInt(items.length), cls: "ov-att",
      body: shown.length
        ? tbl(["", "What", "Where", ["Since", "num"], ""], shown.map((i) => [html`<span class="${i.tone}">●</span>`, i.what, i.where, i.at ? since(i.at) : html`<span class="faint">now</span>`, i.go]))
        : html`<div class="empty-line">Nothing needs attention. Failed runs, stale datasets, pending changes and failing templates show up here.</div>` });
  }

  function running() {
    const rs = (data.active && data.active.runs) || [];
    const body = rs.length
      ? html`<div class="ov-rows">${rs.slice(0, 4).map((r) => {
          const live = r.status === "running" || r.status === "sharded";
          const kinds = runKinds(r);
          return row(ring(live ? "info" : "", live ? "half" : "hollow"), runTitle(r), since(r.submitted_at), live ? meta(kinds || "running", `${fmtCompact(r.records_written || 0)} rows${kinds ? "" : " so far"}`, r.pool && r.pool !== "default" ? `pool ${r.pool}` : "") : meta(r.status, `pool ${r.pool || "default"}`), { href: href.run(r.run_id), id: r.run_id });
        })}</div>`
      : html`<div class="empty-line">Nothing is running or waiting right now.</div>`;
    return card({ icon: "runs", title: "Running now", right: lnk("All runs", href.runs({ status: ACTIVE })), body });
  }

  function freshness() {
    const fr = data.ov && data.ov.freshness;
    if (!fr) return card({ icon: "db", title: "Data freshness", right: lnk("Datasets", href.datasets()), body: html`<div class="empty-line">${data.ov ? "No dataset has been catalogued yet." : "This server reports no catalog freshness."}</div>` });
    const links = (xs) => join(xs.slice(0, 3).map((d) => lnk(datasetName(d.uri), href.dataset(d.id), false, d.uri)), " · ");
    const changed = fr.schema_changed || [];
    return card({ icon: "db", title: "Data freshness", right: lnk("Datasets", href.datasets()), body: grid(3, [
      kv("Fresh", html`<span class="ok">${fmtInt(Math.max(0, fr.datasets - fr.stale_count))}</span>`, `last success within ${fmtAge(fr.stale_before)}`),
      kv("Stale", html`<span class="${fr.stale_count ? "warn" : ""}">${fmtInt(fr.stale_count)}</span>`, fr.stale_count ? links(fr.stale) : "every dataset is current"),
      kv("Schema changed", html`<span class="${fr.schema_changed_count ? "bad" : ""}">${fmtInt(fr.schema_changed_count)}</span>`, fr.schema_changed_count ? links(changed) : "no new schema versions"),
    ]), note: changed.length ? html`Schema changes ${range === "24h" ? "today" : "this week"}: ${join(changed.slice(0, 3).map((c) => html`<b>${datasetName(c.uri)}</b> (${fmtInt(c.changes)} new version${c.changes === 1 ? "" : "s"})`), ", ")}.` : "" });
  }

  function changes() {
    const raw0 = data.changes;
    const cs = Array.isArray(raw0) ? raw0 : (raw0 && raw0.changes) || [];
    const tone = { pending: ["warn", "hollow"], approved: ["info", "half"], executed: ["ok", ""], rejected: ["bad", ""], failed: ["bad", ""], invalidated: ["bad", "hollow"], expired: ["", "hollow"] };
    const body = cs.length
      ? html`<div class="ov-rows">${cs.slice(0, 3).map((c) => {
          const [t, s] = tone[c.status] || ["", ""];
          const votes = c.required_approvals ? `${fmtInt((c.approvals || []).length)} of ${fmtInt(c.required_approvals)} approvals` : "";
          return row(ring(t, s), c.reason || `${c.kind.replace("_", " ")} ${c.id.slice(0, 8)}`, since(c.created_at), meta(c.status === "pending" ? "needs approval" : c.status, c.status === "pending" ? votes : c.kind.replace("_", " "), c.requester ? `by ${c.requester}` : ""), { href: href.change(c.id), id: c.id });
        })}</div>`
      : html`<div class="empty-line">No change requests. Runs and template launches that need approval wait here.</div>`;
    return card({ icon: "changes", title: "Change requests", right: lnk("Changes", href.changes()), body });
  }

  function health() {
    const rd = readiness();
    if (!rd) return card({ icon: "server", title: "Server health", right: lnk("Settings", href.settings()), body: html`<div class="empty-line">The server did not answer <span class="mono">/readyz</span>.</div>` });
    const st = (tone, text) => html`<span class="${tone}">● ${text}</span>`;
    const by = (data.ov && data.ov.runs) || {};
    const rows = [
      ["History", rd.history_ok ? st("ok", "healthy") : st("bad", "degraded"), rd.history_ok ? "run history writable" : "falling back to memory"],
      ["Run queue", rd.queue_ok === false ? st("warn", "full") : rd.draining ? st("warn", "draining") : st("ok", "accepting"), `${fmtInt(by.running || 0)} running · ${fmtInt(by.queued || 0)} queued`],
    ];
    const cl = rd.cluster || {};
    rows.push(["Cluster", cl.enabled ? st("ok", `${fmtInt(cl.instances || 0)} live`) : st("ok", "single server"), (cl.pools || []).length ? `pools ${(cl.pools || []).join(" · ")}` : ""]);
    const trig = rd.triggers || [];
    if (trig.length) {
      const bad = trig.filter((t) => !t.healthy);
      rows.push(["Triggers", bad.length ? st("warn", `${bad.length} of ${trig.length} degraded`) : st("ok", `${trig.length} healthy`), bad.length ? join(bad.slice(0, 2).map((t) => lnk(t.name, href.trigger(t.name))), " · ") : ""]);
    }
    return card({ icon: "server", title: "Server health", right: lnk("Settings", href.settings()), body: tbl(["Part", "State", "Detail"], rows) });
  }

  function templates() {
    const raw0 = data.templates;
    const ts = (raw0 && (raw0.templates || raw0)) || [];
    if (!Array.isArray(ts)) return "";
    const st = (t) => t.state || {};
    const launched = ts.filter((t) => st(t).status === "launched").length;
    const draft = ts.filter((t) => st(t).status === "draft").length;
    const failing = ts.filter((t) => (st(t).test_status || {})[st(t).newest] === "failing").length;
    const ready = ts.filter((t) => st(t).status !== "deprecated" && st(t).newest && st(t).newest !== st(t).stable && (st(t).test_status || {})[st(t).newest] === "passing");
    return card({ icon: "tpl", title: "Templates", right: lnk("Templates", href.templates()), body: ts.length ? grid(2, [
      kv("Launched", fmtInt(launched), [draft && `${fmtInt(draft)} draft`, failing && `${fmtInt(failing)} failing tests`].filter(Boolean).join(" · ") || "none waiting"),
      kv("Ready to launch", fmtInt(ready.length), ready.length ? join(ready.slice(0, 2).map((t) => html`${lnk(t.id, href.template(t.id))} v${st(t).newest}`), " · ") : "newest versions are live"),
    ]) : html`<div class="empty-line">No templates registered.</div>` });
  }

  function firstRun() {
    return html`${head("Welcome to faucet", [html`<span>this server has not run anything yet</span>`])}
      ${card({ icon: "rocket", title: "Get your first pipeline running", right: "0 of 4 done", body: steps([["Pick a source and sink", "cur"], ["Validate the config", ""], ["Run it", ""], ["Watch it in Runs", ""]]),
        note: html`<div class="ov-checklist">${btn("Submit a run", { kind: "primary", icon: "submit", href: href.submit(), perm: "config_submit" })}${btn("Start from a template", { icon: "tpl", href: href.templates() })}${btn("Browse connectors", { icon: "schema", href: href.schemas() })}</div>` })}
      <div class="pair">
        ${card({ icon: "tpl", title: "Templates", body: empty("No templates registered", html`Register one, or point <code>faucet serve --templates-sync</code> at a repo.`, { btns: btn("Register a template", { href: href.templates({ register: 1 }), perm: "template_admin" }) }) })}
        ${card({ icon: "db", title: "Datasets", body: empty("Nothing catalogued yet", "Every dataset a run reads or writes shows up here with its schema, freshness and lineage.") })}
      </div>`;
  }

  function paint() {
    const cl = (readiness() || {}).cluster || {};
    setContext(cl.enabled ? chip(html`<span class="dot"></span>cluster · ${fmtInt(cl.instances || 0)} live`) : "");
    if (err && !data) {
      patch(L.detail, errorAlert("the overview", err));
      L.detail.querySelector("[data-retry]")?.addEventListener("click", refresh);
      return;
    }
    if (!data) {
      patch(L.detail, html`<span class="skel t"></span><span class="skel w30"></span><div class="skel-card"><span class="skel w30"></span><div class="grid4" style="padding:0">${v('<div><span class="skel w60"></span><span class="skel" style="margin-top:8px;height:16px;width:50%"></span></div>'.repeat(4))}</div></div><div class="two">${skelCards(1)}${skelCards(1)}</div><div class="two">${skelCards(1)}${skelCards(1)}</div>`);
      return;
    }
    const everRan = data.any && ((data.any.runs || []).length || (data.any.counts && Object.values(data.any.counts).some(Boolean)));
    const ts = data.templates && (data.templates.templates || data.templates);
    const hasTemplates = Array.isArray(ts) && ts.length;
    if (!everRan && !hasTemplates && !(data.ov && data.ov.freshness && data.ov.freshness.datasets)) {
      patch(L.detail, firstRun());
      return;
    }
    const items = attentionItems();
    const me = whoami();
    const name = me && me.principal && me.principal !== "anonymous" ? `, ${me.principal}` : "";
    const ident = me && me.tenant ? `${me.tenant} · tenant console` : cl.enabled ? `${location.host} · ${fmtInt(cl.instances || 0)} replicas` : location.host;
    patch(L.detail, html`${dhead(dtabs([["Today", "", "24h"], ["This week", "", "7d"]], range, { hrefFor: (r) => hashFor("#/overview", { range: r === "24h" ? "" : r }) }), btn("Submit a run", { icon: "submit", href: href.submit(), perm: "config_submit" }))}
      ${head(`${greeting()}${name}`, [html`<span>${ident}</span>`, html`<span>${items.length ? `${fmtInt(items.length)} thing${items.length === 1 ? "" : "s"} need${items.length === 1 ? "s" : ""} attention` : "nothing needs attention"}</span>`])}
      ${err ? errorAlert("the latest figures", err) : ""}
      ${activity()}
      <div class="two">${attention(items)}${running()}</div>
      <div class="two">${freshness()}${changes()}</div>
      <div class="two">${health()}${templates()}</div>`);
    L.detail.querySelector("[data-retry]")?.addEventListener("click", refresh);
  }

  paint();
  await refresh();
  const stop = poll(refresh, 30000);
  return {
    teardown() { alive = false; stop(); },
  };
}

export { fmtSecs };

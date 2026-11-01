// Audit log (#998, #205): split pane — every mutating or denied control-plane
// action, newest first, by day, beside one entry's fields and what it touched.
// Reading it needs `audit_read` (admin). Filters live in the URL; any value in
// an entry (principal, action, tenant) is a link that filters by it.
import { api } from "../api.js";
import { html, raw, v, ic, row, ring, sect, tabs, ibtn, empty, skelRows, skelCards, head, stats, card, tbl, pill, chip, kv, grid, btn, dhead, lnk, alert, cpi, field, input, dotText } from "../components.js";
import { layout, setQuery, patch, errorAlert, hashFor, poll, ago, sections } from "../page.js";
import { setCrumbs } from "../shell.js";
import { fmtInt, fmtAge, fmtCompact } from "../utils.js";
import { formatTs, getTz } from "../tz.js";
import { href } from "../links.js";
import { can } from "../access.js";

const PAGE = 100;
const FILTERS = ["principal", "action", "tenant", "since", "until"];
const READ_ONLY = /\.(list|get|rows|lineage|status|tests)$|^plan$|^verify$|^usage\./;
const denied = (e) => e.result === "denied";
const tone = (e) => (denied(e) ? "bad" : /needs_reauth|failed|invalidated|expired/.test(e.action) ? "warn" : e.result === "ok" ? "ok" : "info");
const zone = () => (getTz() === "local" ? {} : { timeZone: getTz() });
const dayKey = (iso) => new Intl.DateTimeFormat("en-CA", { ...zone(), year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(iso));
function dayLabel(iso) {
  const k = dayKey(iso);
  const diff = Math.round((Date.parse(dayKey(new Date().toISOString())) - Date.parse(k)) / 86400000);
  if (diff === 0) return "Today";
  if (diff === 1) return "Yesterday";
  if (diff < 7) return new Intl.DateTimeFormat("en-GB", { ...zone(), weekday: "long" }).format(new Date(iso));
  return new Intl.DateTimeFormat("en-GB", { ...zone(), day: "numeric", month: "short", year: "numeric" }).format(new Date(iso));
}
/** Row age: "2m" today, else the weekday ("Wed") or a short date. */
function shortAge(iso) {
  const diff = Date.now() - Date.parse(iso);
  if (diff < 86400000 && dayKey(iso) === dayKey(new Date().toISOString())) return fmtAge(iso);
  if (diff < 7 * 86400000) return new Intl.DateTimeFormat("en-GB", { ...zone(), weekday: "short" }).format(new Date(iso));
  return new Intl.DateTimeFormat("en-GB", { ...zone(), day: "numeric", month: "short" }).format(new Date(iso));
}
/** A target ("template:a/b@3", "connection:t/name", "run:id", "change:id", …) as a link. */
function targetLink(target) {
  if (!target) return html`<span class="muted">—</span>`;
  const m = String(target).match(/^([a-z_]+):(.*)$/);
  if (!m) return html`<span class="mono">${target}</span>`;
  const [, kind, rest] = m;
  if (kind === "template") {
    const [tid, ver] = rest.split("@");
    return html`${lnk(tid, href.template(tid, ver ? { version: ver } : {}))}${ver ? html` <span class="mono">v${ver}</span>` : ""}`;
  }
  if (kind === "connection") {
    const [tn, name] = rest.split("/");
    return html`${lnk(tn, href.tenant(tn, "Connections"))} · <span class="mono">${name || ""}</span>`;
  }
  if (kind === "run") return lnk(rest, href.run(rest), true);
  if (kind === "change") return lnk(rest, href.change(rest), true);
  if (kind === "tenant") return lnk(rest, href.tenant(rest));
  if (kind === "trigger") return lnk(rest, href.trigger(rest));
  return html`<span class="mono">${target}</span>`;
}
const targetKind = (target) => {
  const k = String(target || "").split(":")[0];
  return { template: "template version", connection: "tenant connection", run: "run", change: "change request", tenant: "tenant", trigger: "trigger" }[k] || "";
};
/** The facts after the principal in a list row. */
function rowFacts(e) {
  const t = e.target ? String(e.target).replace(/^[a-z_]+:/, "") : "";
  return [e.tenant ? `tenant ${e.tenant}` : "", t, e.run_id && !t ? `run ${e.run_id.slice(0, 4)}…${e.run_id.slice(-4)}` : "", denied(e) ? `denied${e.detail ? ` · ${e.detail}` : ""}` : e.result !== "ok" ? e.result : e.detail || "ok"].filter(Boolean);
}

export async function renderAudit(host, params) {
  if (!can("audit_read")) {
    const { detail } = layout(host, "canvas");
    setCrumbs(["Admin", "Audit log"]);
    detail.innerHTML = v(empty("The audit log is for admins", html`Reading it needs the <code>audit_read</code> permission, which only the admin role has. Ask an admin, or use the Runs and Changes pages for what you started.`, { icon: "shield", btns: btn("Open Runs", { icon: "runs", href: href.runs() }) }));
    return null;
  }
  const L = layout(host, "split");
  const secs = sections("faucet.audit.sections");
  let entries = null;
  let more = false;
  let nextBefore = null;
  let serverResult = true; // the server filters `result=` itself (#998); older ones ignore it
  let loadErr = null;
  let loadingMore = false;
  const read = (p) => {
    const s = { id: p.id || "", tab: p.query.get("result") === "denied" ? "Denials" : "All", filters: p.query.get("filters") === "1" };
    for (const k of FILTERS) s[k] = p.query.get(k) || "";
    return s;
  };
  let state = read(params);
  let sel = null;
  let selErr = null;
  let selGen = 0;
  let extra = {};

  const listQuery = (patchQ = {}) => {
    const q = { result: state.tab === "Denials" ? "denied" : "", filters: state.filters ? "1" : "" };
    for (const k of FILTERS) q[k] = state[k];
    return { ...q, ...patchQ };
  };
  const listHref = (p = {}) => hashFor(href.audit(), listQuery(p));
  const entryHref = (eid) => hashFor(href.auditEntry(eid), listQuery());
  const filterKey = () => [state.tab, ...FILTERS.map((k) => state[k])].join("|");
  let loadedKey = "";

  function query(before = "") {
    const p = new URLSearchParams({ limit: String(PAGE) });
    for (const k of FILTERS) {
      if (!state[k]) continue;
      // Dates in the URL are days (YYYY-MM-DD); the API wants instants.
      if (k === "since") p.set(k, new Date(`${state.since}T00:00:00`).toISOString());
      else if (k === "until") p.set(k, new Date(`${state.until}T23:59:59.999`).toISOString());
      else p.set(k, state[k]);
    }
    if (state.tab === "Denials") p.set("result", "denied");
    if (before) p.set("before", before);
    return p;
  }
  const shown = () => (entries || []).filter((e) => state.tab !== "Denials" || denied(e));

  async function load() {
    const key = filterKey();
    try {
      const r = await api(`/v1/audit?${query()}`);
      if (key !== filterKey()) return;
      const fresh = r.entries || [];
      // A background refresh keeps pages already loaded below the first.
      if (entries && loadedKey === key && entries.length > PAGE) {
        const seen = new Set(fresh.map((e) => e.id));
        entries = [...fresh, ...entries.filter((e) => !seen.has(e.id))];
      } else {
        entries = fresh;
        nextBefore = r.next_before || (fresh.length >= PAGE ? fresh[fresh.length - 1].id : null);
        more = fresh.length >= PAGE;
      }
      if (state.tab === "Denials" && fresh.some((e) => !denied(e))) serverResult = false;
      loadedKey = key;
      loadErr = null;
    } catch (e) {
      if (!entries) loadErr = e;
    }
    renderList();
    if (!state.id) renderDetail();
  }

  async function loadOlder() {
    if (!entries || !entries.length || loadingMore) return;
    loadingMore = true;
    renderList();
    const last = entries[entries.length - 1];
    let r;
    try {
      r = await api(`/v1/audit?${query(nextBefore || last.id)}`);
    } catch (e) {
      // A server without the cursor pages by time instead.
      const p = query();
      p.set("until", last.timestamp);
      try { r = await api(`/v1/audit?${p}`); } catch { r = null; }
    }
    loadingMore = false;
    if (r) {
      const seen = new Set(entries.map((e) => e.id));
      const add = (r.entries || []).filter((e) => !seen.has(e.id));
      entries = [...entries, ...add];
      more = (r.entries || []).length >= PAGE && add.length > 0;
      nextBefore = r.next_before || (add.length ? add[add.length - 1].id : null);
    }
    renderList();
  }

  function renderList() {
    if (!entries) {
      L.list.innerHTML = v(loadErr ? errorAlert("the audit log", loadErr) : html`${tabs(["All", "Denials"], state.tab)}${stats([["Entries", "–"], ["Principals", "–"], ["Denied", "–"], ["Mutations", "–"]])}${skelRows(8)}`);
      L.list.querySelector("[data-retry]")?.addEventListener("click", load);
      return;
    }
    const list = shown();
    const all = entries;
    const active = FILTERS.filter((k) => state[k]);
    const tools = html`${ibtn("filter", { title: "Filter by principal, action, tenant or date", on: state.filters, dot: active.length > 0, data: { filters: "1" } })}${ibtn("refresh", { title: "Refresh", data: { reload: "1" } })}`;
    const plus = more ? "+" : "";
    const st = stats([["Entries", `${fmtCompact(list.length)}${plus}`], ["Principals", fmtInt(new Set(all.map((e) => e.principal)).size)], ["Denied", `${fmtInt(all.filter(denied).length)}${all.some(denied) ? plus : ""}`, all.some(denied) ? "bad" : "", "denied"], ["Mutations", fmtInt(all.filter((e) => !READ_ONLY.test(e.action)).length)]], state.tab === "Denials" ? 2 : -1);
    const filterBox = state.filters ? html`<form class="filters" id="au-filters"><div class="r2">${input(state.principal, { name: "principal", ph: "Principal" })}${input(state.action, { name: "action", ph: "Action, e.g. run.submit" })}</div><div class="r2">${input(state.since, { name: "since", type: "date", a: { title: "Since (inclusive)" } })}${input(state.until, { name: "until", type: "date", a: { title: "Until (inclusive)" } })}</div><div class="r2">${input(state.tenant, { name: "tenant", ph: "Tenant" })}${btn("Apply", { kind: "primary", type: "submit" })}</div></form>` : "";
    const chips = active.length ? html`<div class="active-f">${active.map((k) => chip(`${k}: ${state[k]}`, "x", { "data-clear": k, title: "Remove this filter", role: "button" }))}<a class="lnk" href="#" data-clear="all" style="font-size:12px;margin-left:4px">clear</a></div>` : "";
    let body;
    if (!list.length) {
      body = empty(active.length || state.tab === "Denials" ? "Nothing in this window" : "No audit entries yet", active.length || state.tab === "Denials" ? "No entry matches. Clear a filter, or widen the dates; entries older than the run-retention window are purged." : "Mutating and denied control-plane actions appear here as they happen.", { icon: "audit" });
    } else {
      const groups = [];
      for (const e of list) {
        const label = dayLabel(e.timestamp);
        if (!groups.length || groups[groups.length - 1][0] !== label) groups.push([label, []]);
        groups[groups.length - 1][1].push(e);
      }
      body = html`${groups.map(([label, es]) => {
        const closed = secs.isClosed(label);
        return html`${sect(label, es.length, { id: label, closed })}<div class="sect-rows${closed ? " closed" : ""}">${es.map((e) => row(ring(tone(e)), e.action, html`<time datetime="${e.timestamp}" title="${formatTs(e.timestamp)}">${shortAge(e.timestamp)}</time>`, html`<b>${e.principal}</b> · ${rowFacts(e).join(" · ")}`, { mono: true, on: e.id === state.id, href: entryHref(e.id), id: e.id }))}</div>`;
      })}${more ? html`<button type="button" class="btn quiet more" data-older="1"${raw(loadingMore ? " disabled" : "")}>${loadingMore ? "Loading…" : "Load older"}</button>` : ""}`;
    }
    const top = html`${tabs(["All", "Denials"], state.tab, tools)}${st}${filterBox}${chips}`;
    if (!L.list.querySelector(".list-body")) L.list.innerHTML = '<div class="list-top" style="display:contents"></div><div class="list-body rows"></div>';
    patch(L.list.querySelector(".list-top"), top);
    patch(L.list.querySelector(".list-body"), body);
  }

  L.list.addEventListener("click", (e) => {
    const t = e.target.closest("[data-tab]");
    if (t) return setQuery({ result: t.dataset.tab === "Denials" ? "denied" : "" });
    const q = e.target.closest(".stats [data-q]");
    if (q) return setQuery({ result: state.tab === "Denials" ? "" : "denied" });
    if (e.target.closest("[data-reload]")) { load(); return; }
    if (e.target.closest("[data-filters]")) return setQuery({ filters: state.filters ? "" : "1" }, { replace: true });
    if (e.target.closest("[data-older]")) return loadOlder();
    const c = e.target.closest("[data-clear]");
    if (c) {
      e.preventDefault();
      setQuery(c.dataset.clear === "all" ? Object.fromEntries(FILTERS.map((k) => [k, ""])) : { [c.dataset.clear]: "" });
    }
  });
  L.list.addEventListener("submit", (e) => {
    e.preventDefault();
    const f = e.target;
    setQuery(Object.fromEntries(FILTERS.map((k) => [k, f.elements[k].value.trim()])));
  });
  L.list.addEventListener("sect-toggle", (e) => secs.toggle(e.detail.id));

  // ---- detail ----
  function noneDetail() {
    setCrumbs(["Admin", "Audit log"]);
    if (!entries) return html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(2)}`;
    return empty("Pick an entry", "Each entry is one mutating or denied control-plane action, attributed to a principal.", { icon: "audit" });
  }

  function entryDetail(e) {
    setCrumbs(["Admin", { label: "Audit log", href: listHref() }, e.action]);
    const res = denied(e) ? pill("Denied", "bad", "x") : e.result === "ok" ? pill("ok", "ok", "check") : pill(e.result, "info");
    const sub = [res, html`<span>by ${lnk(e.principal, listHref({ principal: e.principal }))} · ${e.role}</span>`, html`<span>${ago(e.timestamp, " ago")} · ${formatTs(e.timestamp)}</span>`];
    const fp = e.config_fingerprint;
    const entry = card({
      title: "Entry", right: html`<span class="mono">${e.id.slice(0, 13)}…${e.id.slice(-5)}</span>${cpi(e.id)}`,
      body: grid(3, [
        kv("Principal", lnk(e.principal, listHref({ principal: e.principal })), e.role ? `role ${e.role}` : ""),
        kv("Action", lnk(e.action, listHref({ action: e.action }), true)),
        kv("Result", html`<span class="${denied(e) ? "bad" : e.result === "ok" ? "ok" : ""}">${e.result}</span>`),
        kv("Target", targetLink(e.target), targetKind(e.target)),
        kv("Tenant", e.tenant ? lnk(e.tenant, href.tenant(e.tenant)) : html`<span class="muted">—</span>`, e.tenant ? html`<a class="lnk" href="${listHref({ tenant: e.tenant })}">filter by it</a>` : ""),
        kv("Run", e.run_id ? lnk(`${e.run_id.slice(0, 4)}…${e.run_id.slice(-4)}`, href.run(e.run_id), true, e.run_id) : html`<span class="muted">—</span>`),
        kv("Source IP", e.source_ip ? html`<span class="mono">${e.source_ip}</span>` : html`<span class="muted">—</span>`),
        kv("Config fingerprint", fp ? html`<span class="mono">${fp.length > 18 ? `${fp.slice(0, 11)}…${fp.slice(-4)}` : fp}</span>${cpi(fp)}` : html`<span class="muted">—</span>`, fp ? "" : "set on run submits"),
        kv("Detail", e.detail ? e.detail : html`<span class="muted">—</span>`),
      ]),
    });
    const why = denied(e) ? alert("Refused before anything changed.", e.detail || "The principal lacked the permission this route needs, or a policy refused it.", "bad") : "";
    const run = extra.run;
    const runCard = e.run_id ? card({
      icon: "runs", title: e.action === "run.submit" ? "The run it started" : "The run", right: lnk("open run", href.run(e.run_id)),
      body: run === undefined ? html`<div class="pad">${skelRows(1)}</div>` : !run ? html`<div class="empty-line">The run is no longer in the history (purged by retention).</div>` : grid(4, [
        kv("Status", html`<span class="${{ running: "info", completed: "ok", failed: "bad" }[run.status] || ""}">${run.status}</span>`),
        kv("Rows", fmtCompact(run.records_written || 0)),
        kv("Pipeline", runKinds(run) || run.name || "—"),
        kv("Started", run.started_at ? ago(run.started_at, " ago") : "—"),
      ]),
    }) : "";
    const others = (extra.others || []).filter((x) => x.id !== e.id).slice(0, 5);
    const moreCard = extra.others === undefined ? card({ icon: "user", title: `More from ${e.principal}`, body: html`<div class="pad">${skelRows(2)}</div>` }) : others.length ? card({
      icon: "user", title: `More from ${e.principal}`, right: lnk("filter the list", listHref({ principal: e.principal })),
      body: tbl(["Action", "Target", "Result", ["When", "num"]], others.map((x) => ({ href: entryHref(x.id), cells: [html`<span class="mono">${x.action}</span>`, x.target || !x.run_id ? targetLink(x.target) : lnk(`run ${x.run_id.slice(0, 4)}…${x.run_id.slice(-4)}`, href.run(x.run_id), true, x.run_id), dotText(denied(x) ? "bad" : "ok", x.result), ago(x.timestamp, " ago")] }))),
    }) : "";
    return html`${dhead("", btn("Copy link", { icon: "link", data: { copy: location.href } }), { href: listHref(), label: "Audit log" })}${head(e.action, sub)}${why}${entry}${runCard}${moreCard}`;
  }

  function runKinds(r) {
    const u = ((r.invocations || [])[0] || {}).usage || {};
    return u.source_kind && u.sink_kind ? `${u.source_kind} → ${u.sink_kind}` : "";
  }

  function renderDetail() {
    L.select(!!state.id);
    if (!state.id) { patch(L.detail, noneDetail()); return; }
    if (selErr) {
      setCrumbs(["Admin", { label: "Audit log", href: listHref() }, "Entry"]);
      patch(L.detail, html`${dhead("", "", { href: listHref(), label: "Audit log" })}${selErr.status === 404 ? empty("Entry not found", "It is older than the history keeps, or the id is wrong.", { icon: "audit", btns: btn("Back to the log", { href: listHref() }) }) : errorAlert("this entry", selErr)}`);
      L.detail.querySelector("[data-retry]")?.addEventListener("click", selectEntry);
      return;
    }
    if (!sel || sel.id !== state.id) { L.detail.__html = ""; L.detail.innerHTML = v(html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(2)}`); return; }
    patch(L.detail, entryDetail(sel));
  }

  async function selectEntry() {
    const eid = state.id;
    const gen = ++selGen;
    selErr = null;
    extra = {};
    sel = (entries || []).find((e) => e.id === eid) || null;
    renderDetail();
    if (!eid) return;
    if (!sel) {
      try {
        sel = await api(`/v1/audit/${encodeURIComponent(eid)}`);
      } catch (e) {
        if (gen !== selGen) return;
        selErr = e.status === 405 ? Object.assign(new Error("not found"), { status: 404 }) : e;
        renderDetail();
        return;
      }
      if (gen !== selGen) return;
      renderDetail();
    }
    const e = sel;
    const [others, run] = await Promise.all([
      api(`/v1/audit?${new URLSearchParams({ principal: e.principal, limit: "6" })}`).then((r) => r.entries || []).catch(() => []),
      e.run_id ? api(`/v1/runs/${encodeURIComponent(e.run_id)}`).catch(() => null) : Promise.resolve(undefined),
    ]);
    if (gen !== selGen) return;
    extra = { others, run };
    renderDetail();
  }

  renderList();
  renderDetail();
  await load();
  if (state.id) selectEntry();
  const stop = poll(load, 30000);

  return {
    teardown: stop,
    update(p) {
      const prevKey = filterKey();
      const prevId = state.id;
      state = read(p);
      if (filterKey() !== prevKey) { entries = null; more = false; nextBefore = null; renderList(); load(); }
      else renderList();
      if (state.id !== prevId) selectEntry();
      else renderDetail();
    },
  };
}

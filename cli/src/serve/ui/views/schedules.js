// Schedules (#1044): split pane — registered templates run on a cron, separate
// from event triggers. Console-made schedules live in the run history (every
// replica sees them); `--schedules <file>` ones are read-only here. A tenant
// principal manages its own tenant's schedules within its `max_schedules` and
// the server's minimum tick interval; the operator's fan-out schedules that
// include its tenant show read-only. Reads GET /v1/schedules[/{name}].
import { api } from "../api.js";
import { html, raw, ic, row, ring, sect, tabs, ibtn, stats, empty, skelRows, skelCards, head, card, grid, kv, tbl, pill, chip, tag, code, btn, dhead, dtabs, lnk, alert, field, input, select, check, diff, join, menu, dotText } from "../components.js";
import { layout, setQuery, patch, poll, errorAlert, hashFor, ago, dialog, toast } from "../page.js";
import { setCrumbs, refreshBadges } from "../shell.js";
import { fmtInt } from "../utils.js";
import { shortId } from "../components.js";
import { href } from "../links.js";
import { can, whoami } from "../access.js";
import { currentTenant, tenantList } from "../tenant.js";
import { navigate } from "../router.js";

const enc = encodeURIComponent;
const SEALED = "$sealed";
const TZS = ["UTC", "Europe/London", "Europe/Berlin", "Europe/Paris", "America/New_York", "America/Chicago", "America/Los_Angeles", "Asia/Kolkata", "Asia/Singapore", "Asia/Tokyo", "Australia/Sydney"];
const RUN_TONE = { completed: "ok", failed: "bad", cancelled: "", running: "info", sharded: "info", queued: "", pending: "" };
const plural = (n, w, ws = `${w}s`) => `${fmtInt(n)} ${n === 1 ? w : ws}`;
const andList = (xs) => (xs.length < 2 ? xs.join("") : `${xs.slice(0, -1).join(", ")} and ${xs[xs.length - 1]}`);
const NUM_WORDS = ["", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"];
const allOf = (n) => (n === 2 ? "both" : `all ${NUM_WORDS[n] || fmtInt(n)}`);

// ── Time ─────────────────────────────────────────────────────────────────────
const fmt = (iso, tz, opts) => new Intl.DateTimeFormat("en-GB", { timeZone: tz || "UTC", ...opts }).format(new Date(iso));
const hm = (iso, tz) => fmt(iso, tz, { hour: "2-digit", minute: "2-digit", hourCycle: "h23" });
const dayKey = (d, tz) => fmt(d.toISOString(), tz, { year: "numeric", month: "2-digit", day: "2-digit" });
/** `today` / `tomorrow` / `yesterday` / `Fri 7 Nov` in `tz`. */
function dayWord(iso, tz) {
  const now = new Date();
  const k = dayKey(new Date(iso), tz);
  if (k === dayKey(now, tz)) return "today";
  if (k === dayKey(new Date(now.getTime() + 864e5), tz)) return "tomorrow";
  if (k === dayKey(new Date(now.getTime() - 864e5), tz)) return "yesterday";
  return fmt(iso, tz, { weekday: "short", day: "numeric", month: "short" });
}
const when = (iso, tz) => (iso ? `${dayWord(iso, tz)} ${hm(iso, tz)}` : "—");
/** `in 14m` / `in 6h 12m` / `in 4d`; `—` when unknown. */
function until(iso) {
  if (!iso) return "—";
  const s = Math.round((new Date(iso).getTime() - Date.now()) / 1000);
  if (s <= 0) return "now";
  if (s < 3600) return `in ${Math.max(1, Math.round(s / 60))}m`;
  if (s < 86400) { const h = Math.floor(s / 3600); const m = Math.round((s % 3600) / 60); return m ? `in ${h}h ${m}m` : `in ${h}h`; }
  return `in ${Math.round(s / 86400)}d`;
}
const took = (r) => (r.elapsed_secs == null ? "—" : r.elapsed_secs < 60 ? `${Math.round(r.elapsed_secs)}s` : r.elapsed_secs < 3600 ? `${Math.round(r.elapsed_secs / 60)}m` : `${(r.elapsed_secs / 3600).toFixed(1)}h`);

const DOW = { 0: "Sundays", 1: "Mondays", 2: "Tuesdays", 3: "Wednesdays", 4: "Thursdays", 5: "Fridays", 6: "Saturdays", 7: "Sundays", SUN: "Sundays", MON: "Mondays", TUE: "Tuesdays", WED: "Wednesdays", THU: "Thursdays", FRI: "Fridays", SAT: "Saturdays", "MON-FRI": "weekdays", "1-5": "weekdays" };
const pad2 = (x) => String(x).padStart(2, "0");
/** The common cron shapes in words; anything else stays the expression. */
function cronWords(cron) {
  const f = String(cron || "").trim().split(/\s+/);
  if (f.length !== 5) return "";
  const [mi, h, dom, mon, dow] = f;
  const num = (x) => /^\d+$/.test(x);
  if (mon !== "*") return "";
  if (dom === "*" && dow === "*") {
    if (mi === "*" && h === "*") return "every minute";
    const em = /^\*\/(\d+)$/.exec(mi);
    if (em && h === "*") return `every ${em[1]} minutes`;
    if (num(mi) && h === "*") return `every hour at :${pad2(mi)}`;
    const eh = /^\*\/(\d+)$/.exec(h);
    if (num(mi) && eh) return `every ${eh[1]} hours${mi !== "0" ? ` at :${pad2(mi)}` : ""}`;
    if (num(mi) && num(h)) return `every day at ${pad2(h)}:${pad2(mi)}`;
  }
  if (dom === "*" && DOW[String(dow).toUpperCase()] && num(mi) && num(h)) return `${DOW[String(dow).toUpperCase()]} at ${pad2(h)}:${pad2(mi)}`;
  if (num(dom) && dow === "*" && num(mi) && num(h)) return `on day ${dom} of every month at ${pad2(h)}:${pad2(mi)}`;
  return "";
}
const cronLine = (s) => cronWords(s.cron) || s.cron;

// ── Facts about one schedule ────────────────────────────────────────────────
const tenantCount = (s, all) => (s.tenants === "all" ? (all ? all.length : null) : Array.isArray(s.tenants) ? s.tenants.length : null);
function runsFor(s) {
  if (s.source === "file" && !s.tenants) return "from schedules file";
  if (s.tenant) return `tenant ${s.tenant}`;
  if (s.tenants === "all") return "all tenants";
  if (Array.isArray(s.tenants)) return plural(s.tenants.length, "tenant");
  return "this server";
}
const version = (s) => s.template.version || "stable";
const verLabel = (v) => (/^\d+$/.test(String(v)) ? `pin v${v}` : ["stable", "newest", "previous"].includes(v) ? v : `channel ${v}`);
const tplLink = (s) => html`${lnk(s.template.id, href.template(s.template.id))} <span class="mono">${verLabel(version(s))}</span>`;
const unhealthy = (s) => s.status === "failing" || s.status === "retrying";

// ── The page ────────────────────────────────────────────────────────────────
export async function renderSchedules(host, params) {
  const L = layout(host, "split");
  let data = null;          // GET /v1/schedules
  let loadErr = null;
  let detail = null;        // GET /v1/schedules/{name}
  let detailErr = null;
  let templates = null;     // GET /v1/templates (for the form)
  let state = { name: params.id || "", tab: params.query.get("tab") || "All", dtab: params.query.get("dtab") || "Overview", mode: params.query.get("mode") || "" };
  let form = null;          // the create / edit form's state
  let requested = null;     // the change request a gated create made
  let alive = true;

  const me = () => whoami() || {};
  const scope = () => currentTenant();
  const tenantPrincipal = () => !!me().tenant;
  const list = () => (data && data.schedules) || [];
  const find = (n) => list().find((s) => s.name === n) || null;
  const back = () => ({ href: hashFor(href.schedules(), { tab: state.tab !== "All" ? state.tab : "" }), label: "Schedules" });

  async function load() {
    try {
      const q = new URLSearchParams();
      if (scope()) q.set("tenant", scope());
      data = await api(`/v1/schedules?${q}`);
      loadErr = null;
    } catch (e) {
      loadErr = e;
    }
    paint();
  }

  async function loadDetail() {
    const name = state.name;
    if (!name || state.mode) { detail = null; return; }
    try {
      const q = new URLSearchParams();
      if (scope()) q.set("tenant", scope());
      const d = await api(`/v1/schedules/${enc(name)}?${q}`);
      if (state.name !== name) return;
      detail = d;
      detailErr = null;
    } catch (e) {
      detail = null;
      detailErr = e;
    }
    paint();
  }

  // ── List pane ─────────────────────────────────────────────────────────────
  function listRow(s) {
    const shared = s.shared && (scope() || tenantPrincipal());
    let metaHtml;
    if (s.paused) metaHtml = html`<b>paused</b> · ${s.cron}${s.paused_by ? html` · by ${s.paused_by}${s.paused_at ? html` ${ago(s.paused_at, " ago")}` : ""}` : ""}`;
    else if (unhealthy(s)) metaHtml = html`<b>${s.status === "retrying" ? "retrying" : "failing"}</b> · ${s.template.id}@${version(s)}${s.consecutive_failures ? ` · ${plural(s.consecutive_failures, "failed tick")}` : ""}`;
    else if (shared && tenantPrincipal()) metaHtml = html`<b>shared</b> · by the operator · read only`;
    else if (shared) metaHtml = html`<b>shared</b> · runs for ${plural(tenantCount(s) ?? 0, "tenant")} · ${s.cron}`;
    else metaHtml = html`<b>${s.cron}</b> · ${s.timezone} · ${tenantPrincipal() && s.created_by ? `by ${s.created_by}` : runsFor(s)}`;
    const tone = s.paused ? "" : unhealthy(s) ? "bad" : "ok";
    return row(ring(tone, s.paused ? "hollow" : ""), s.name, s.paused ? "—" : until(s.next_tick).replace(/^in /, "in "), metaHtml, { on: s.name === state.name && !state.mode, id: s.name, href: hashFor(href.schedule(s.name), { tab: state.tab !== "All" ? state.tab : "" }) });
  }

  function listHtml() {
    const tools = (n) => html`${ibtn("refresh", { title: "Refresh", data: { reload: "1" } })}${newButton(n)}`;
    if (loadErr) return html`${tabs([["All", "—"]], "All", ibtn("refresh", { title: "Refresh", data: { reload: "1" } }))}${empty("Could not load schedules", `The server answered ${loadErr.status || "an error"}.`, { icon: "alert", btns: btn("Retry", { icon: "refresh", data: { reload: "1" } }) })}`;
    if (!data) return html`${tabs([["All", "…"]], "All", ibtn("refresh"))}${skelRows(8)}`;
    const all = list();
    if (data.available === false) return html`${tabs([["All", 0]], "All", ibtn("refresh", { title: "Refresh", data: { reload: "1" } }))}${empty("Schedules are not built in", "This faucet binary was built without the schedule feature.", { icon: "sched" })}`;
    const paused = all.filter((s) => s.paused);
    const top = tabs([["All", all.length], ["Paused", paused.length]], state.tab, tools(all));
    if (!all.length) return html`${top}${empty(scope() && !tenantPrincipal() ? `No schedules run for ${scope()}` : "No schedules yet", "A schedule runs a registered template on a cron.", { icon: "sched" })}`;
    const byNext = (a, b) => (a.next_tick || "~").localeCompare(b.next_tick || "~") || a.name.localeCompare(b.name);
    const shown = (state.tab === "Paused" ? paused : all).slice().sort(byNext);
    const next = all.map((s) => s.next_tick).filter(Boolean).sort()[0];
    const missed24 = all.reduce((n, s) => n + (s.missed_24h || 0), 0);
    const today = all.reduce((n, s) => n + (s.ticks_today || 0), 0);
    const t = scope();
    let body;
    let statsHtml;
    if (tenantPrincipal()) {
      const own = shown.filter((s) => s.tenant === t);
      const sharedRows = shown.filter((s) => s.tenant !== t);
      const max = data.max_schedules;
      statsHtml = stats([["Schedules", max != null ? `${data.owned ?? own.length} of ${max}` : fmtInt(own.length), max != null && (data.owned ?? 0) >= max ? "warn" : ""], ["Ticks today", fmtInt(today)], ["Missed", fmtInt(missed24), missed24 ? "bad" : ""], ["Next tick", until(next)]]);
      const act = own.filter((s) => !s.paused);
      const pz = own.filter((s) => s.paused);
      body = html`${act.length ? html`${sect("Active", act.length)}${act.map(listRow)}` : ""}${pz.length ? html`${sect("Paused", pz.length)}${pz.map(listRow)}` : ""}
        ${sharedRows.length ? html`${sect("From the operator", sharedRows.length)}${sharedRows.map(listRow)}<div class="muted" style="font-size:12px;padding:10px 14px">Shared schedules are managed by the server operator and do not count toward ${t}’s limit.</div>` : ""}`;
    } else if (t) {
      const own = shown.filter((s) => s.tenant === t);
      const sharedRows = shown.filter((s) => s.tenant !== t);
      statsHtml = stats([[`For ${t}`, fmtInt(all.length)], ["Ticks today", fmtInt(today)], ["Missed", fmtInt(missed24), missed24 ? "bad" : ""], ["Next tick", until(next)]]);
      body = html`${own.length ? html`${sect(`${t}’s own`, own.length)}${own.map(listRow)}` : ""}${sharedRows.length ? html`${sect(`Shared · include ${t}`, sharedRows.length)}${sharedRows.map(listRow)}` : ""}
        <div class="muted" style="font-size:12px;padding:10px 14px">Showing schedules that run for ${t}. Pick “All tenants” above to see ${data.total != null ? `all ${fmtInt(data.total)}` : "every schedule"}.</div>`;
    } else {
      statsHtml = stats([["Schedules", fmtInt(all.length)], ["Ticks today", fmtInt(today)], ["Missed", fmtInt(missed24), missed24 ? "bad" : ""], ["Next tick", until(next)]]);
      const bad = shown.filter((s) => !s.paused && unhealthy(s));
      const act = shown.filter((s) => !s.paused && !unhealthy(s));
      const pz = shown.filter((s) => s.paused);
      body = html`${bad.length ? html`${sect("Needs attention", bad.length)}${bad.map(listRow)}` : ""}${act.length ? html`${sect("Active", act.length)}${act.map(listRow)}` : ""}${pz.length ? html`${sect("Paused", pz.length)}${pz.map(listRow)}` : ""}`;
    }
    if (!shown.length) body = empty("Nothing paused", "", { icon: "check" });
    return html`${top}${statsHtml}${body}`;
  }

  function atLimit() {
    return tenantPrincipal() && data && data.max_schedules != null && (data.owned ?? 0) >= data.max_schedules;
  }
  function newButton() {
    if (!data || data.available === false) return "";
    return btn("New", { icon: "plus", kind: "primary", perm: "schedule_write", disabled: atLimit(), title: atLimit() ? "This tenant has reached its schedule limit" : "", data: { act: "new" } });
  }

  // ── Detail: nothing selected ──────────────────────────────────────────────
  function comingUp(rows, withOwner) {
    const items = rows.flatMap((s) => (s.upcoming || []).map((t) => ({ s, t }))).sort((a, b) => a.t.localeCompare(b.t)).slice(0, 8);
    if (!items.length) return html`<div class="empty-line">Nothing in the next 12 hours.</div>`;
    return tbl(["When", "Schedule", withOwner ? "Owner" : "Runs", "Template"], items.map(({ s, t }) => ({ href: href.schedule(s.name), cells: [`${until(t)} · ${hm(t, "UTC")}`, lnk(s.name, href.schedule(s.name)), withOwner ? (s.shared ? html`shared ${tag(plural(tenantCount(s) ?? 0, "tenant"))}` : s.tenant || "server") : runsFor(s).replace("this server", "1").replace(/^tenant /, "1 · "), s.template.id] })));
  }

  function emptyDetail() {
    if (data.available === false) {
      return html`${head("Schedules need the schedule feature", [], html`This server was built without it, so it cannot tick schedules. The release binaries, the Homebrew formula and the <code>full</code> container image include it.`)}${card({ icon: "code", title: "Build it in", body: code('cargo install faucet-cli --features "serve,schedule,templates"', { box: false, lang: "text" }) })}`;
    }
    return html`${head("Run templates on a cron", [], html`Create a schedule here, or define schedules in a file and start the server with <code>--schedules</code>. Schedules from a file are shown read-only.`)}
      <div class="two">${card({ icon: "plus", title: "In the console", body: html`<div class="pad stack" style="gap:10px"><span>Pick a template, set its params and a cron. Every replica picks it up without a restart.</span><div>${btn("New schedule", { icon: "plus", kind: "primary", perm: "schedule_write", data: { act: "new" } })}</div></div>` })}
      ${card({ icon: "code", title: "schedules.yaml", body: code(`version: 1
schedules:
  - name: nightly-rollup
    cron: "0 2 * * *"
    timezone: Europe/Berlin
    template:
      id: acme/billing-api
      version: stable
      params: { region: eu }`, { box: false }), note: html`Then start the server with <code>faucet serve --schedules schedules.yaml</code>.` })}</div>`;
  }

  function overviewDetail() {
    const all = list();
    if (!all.length) return emptyDetail();
    const t = scope();
    const next = all.map((s) => s.next_tick).filter(Boolean).sort()[0];
    if (tenantPrincipal()) {
      const max = data.max_schedules;
      const limit = atLimit() ? alert(`${t} has reached its limit of ${max} schedules.`, html`Delete one to create another, or ask an operator to raise <code>max_schedules</code>. Paused schedules count toward the limit.`, "warn") : "";
      return html`${head(`${t}’s schedules`, [max != null ? html`<span>${data.owned ?? 0} of ${max} in use</span>` : "", html`<span>ticks at most every ${Math.round((data.tenant_min_interval_secs || 300) / 60)} minutes</span>`])}${limit}${card({ title: "Coming up", right: "next 12 hours · UTC", body: comingUp(all, false) })}`;
    }
    if (t) {
      const own = all.filter((s) => s.tenant === t).length;
      return html`${head(`${plural(all.length, "schedule")} run for ${t}`, [html`<span>${own} of its own · ${all.length - own} shared</span>`, next ? html`<span>next tick ${until(next)}</span>` : ""])}${card({ title: `Coming up for ${t}`, right: "next 12 hours · UTC", body: comingUp(all, true) })}`;
    }
    const missed24 = all.reduce((n, s) => n + (s.missed_24h || 0), 0);
    const broken = all.filter((s) => !s.paused && unhealthy(s));
    return html`${head(plural(all.length, "schedule"), [next ? html`<span>next tick ${until(next)}</span>` : "", missed24 ? html`<span class="bad">${missed24} missed in the last 24h</span>` : ""])}
      ${broken.map((s) => alert(`${s.name} failed its last ${s.consecutive_failures > 1 ? `${s.consecutive_failures} ticks` : "tick"}.`, s.last_error || "", "bad", btn("Open", { icon: "right", href: href.schedule(s.name) })))}
      ${card({ title: "Coming up", right: "next 12 hours · UTC", body: comingUp(all, false) })}`;
  }

  // ── Detail: one schedule ──────────────────────────────────────────────────
  function actionsFor(s) {
    if (s.source === "file") return s.can_run ? btn("Run now", { icon: "play", perm: "schedule_write", data: { act: "run" } }) : "";
    const out = [];
    if (s.can_run) out.push(btn("Run now", { icon: "play", perm: "schedule_write", data: { act: "run" } }));
    if (s.can_edit) {
      out.push(s.paused ? btn("Resume", { icon: "play", kind: "primary", perm: "schedule_write", data: { act: "resume" } }) : btn("Pause", { icon: "pause", perm: "schedule_write", data: { act: "pause" } }));
      out.push(btn("Edit", { icon: "edit", perm: "schedule_write", data: { act: "edit" } }));
      out.push(menu("more", [{ label: "Delete schedule", icon: "trash", cls: "bad", perm: "schedule_write", data: { act: "delete" } }]));
    }
    return join(out);
  }

  function nextTable(d, s) {
    const ticks = d.next_ticks || [];
    if (s.paused) return empty("No upcoming ticks", "Resume to see the next ticks.", {});
    if (!ticks.length) return html`<div class="empty-line">No upcoming ticks.</div>`;
    if (s.timezone === "UTC") return tbl([`Next ${ticks.length} ticks`, "UTC"], ticks.map((t) => [dayWord(t, "UTC"), hm(t, "UTC")]));
    return tbl([`Next ${ticks.length} ticks`, s.timezone, ["UTC", "num"]], ticks.map((t) => [dayWord(t, s.timezone), hm(t, s.timezone), hm(t, "UTC")]));
  }

  function lastTickSub(d) {
    const last = (d.recent_ticks || []).find((t) => !t.manual);
    if (!last) return "";
    if (last.outcome === "failed") return html`<span class="bad">failed</span>`;
    if (last.outcome === "skipped") return "skipped";
    const n = last.runs.length;
    const done = last.runs.filter((r) => r.status === "completed").length;
    const whose = scope() ? ` for ${scope()}` : "";
    if (n === 1) return `1 run${whose} · ${done ? "completed" : last.runs[0].status}`;
    return `${plural(n, "run")}${whose} · ${done === n ? "all completed" : `${done} completed`}`;
  }

  function ticksCard(d, s) {
    const words = cronWords(s.cron);
    return card({ icon: "clock", title: "When it runs", aside: html`<span class="mono">${s.cron}</span>${words ? ` · ${words}` : ""}`,
      body: html`${grid(2, [
        kv("Next tick", s.paused ? "—" : when(s.next_tick, s.timezone), s.paused ? "paused" : s.next_tick ? `${until(s.next_tick)} · ${s.timezone}` : ""),
        kv("Last tick", s.last_tick ? when(s.last_tick, s.timezone) : "—", lastTickSub(d)),
        kv(scope() && s.shared ? `Missed (30d) · ${scope()}` : "Missed (30d)", fmtInt(s.missed_30d || 0), s.missed_30d ? "not run later" : "none"),
        kv("Overlap", s.overlap, { skip: "while the last tick still runs", queue: "held until the last run ends", forbid: "fails the tick" }[s.overlap] || ""),
      ])}<div class="pad" style="padding-top:0">${nextTable(d, s)}</div>` });
  }

  function paramValue(v) {
    if (v && typeof v === "object" && v[SEALED] === true) return html`<span class="faint">sealed · secret param</span>`;
    return typeof v === "string" ? v : JSON.stringify(v);
  }

  function targetCard(s, d) {
    const params = Object.entries(s.template.params || {});
    const kindWord = { pipeline: "pipeline", "source-template": "source template" }[d.template_kind] || d.template_kind || "";
    const pinned = verLabel(version(s)).startsWith("pin");
    const verSub = d.resolved_version == null ? "does not resolve now" : pinned ? "never moves" : `resolves to v${d.resolved_version} now`;
    const tplSub = [kindWord, s.template.sink ? `into ${s.template.sink}` : ""].filter(Boolean).join(" · ");
    const facts = [kv("Template", lnk(s.template.id, href.template(s.template.id)), tplSub), kv("Version", html`<span class="mono">${version(s)}</span>`, verSub), kv("Pool", (s.run || {}).pool || "default"), kv("Timeout", (s.run || {}).timeout_secs ? `${Math.round((s.run || {}).timeout_secs / 60) || 1}m` : "none")];
    if (s.template.overlay) facts.push(kv("Deployment", s.template.overlay, s.template.overlay_version || "stable"));
    return card({ icon: "tpl", title: "What it runs", right: lnk("open template", href.template(s.template.id)),
      body: html`${grid(2, facts)}${params.length ? html`<div class="pad" style="padding-top:0">${tbl(["Param", "Value"], params.map(([k, v]) => [html`<span class="mono">${k}</span>`, paramValue(v)]))}</div>` : ""}` });
  }

  function tenantsCard(d, s) {
    if (s.tenant) return card({ icon: "tenants", title: "Runs for", body: html`<div class="pad">${chip(s.tenant)}</div>`, note: "A tenant’s schedules run only for that tenant, within its limits." });
    if (!s.shared) return "";
    const names = d.applies_to || (Array.isArray(s.tenants) ? s.tenants : []);
    const focus = scope();
    return card({ icon: "tenants", title: "Tenants", tag: "fan-out", aside: "operators only",
      body: html`<div class="pad" style="display:flex;gap:6px;flex-wrap:wrap">${names.length ? names.map((t) => (t === focus ? chip(html`${ic("check")}${t}`, "on") : chip(t))) : html`<span class="muted">${s.tenants === "all" ? "every tenant that is not suspended" : "none"}</span>`}</div>`,
      note: focus ? `${focus} is the tenant you are looking at.` : "One run per tenant each tick, admitted against that tenant’s own limits. One tenant at its limit does not stop the others." });
  }

  function tickOutcome(t) {
    if (t.outcome === "failed") return dotText("bad", t.errors.length > 1 ? `${t.errors.length} refused` : "failed");
    if (t.outcome === "skipped") return dotText("muted", "skipped");
    const n = t.runs.length;
    const done = t.runs.filter((r) => r.status === "completed").length;
    const failed = t.runs.filter((r) => r.status === "failed").length;
    const going = t.runs.filter((r) => !["completed", "failed", "cancelled"].includes(r.status)).length;
    if (failed) return dotText("bad", `${failed} of ${n} failed`);
    if (going) return dotText("info", `${going} of ${n} running`);
    return dotText("ok", `${done} ${n > 1 || scope() ? `of ${n} ` : ""}completed`);
  }

  function ticksTable(d, title = "Recent ticks") {
    const rows = (d.recent_ticks || []).slice(0, 6);
    const tz = d.schedule.timezone;
    const days = (d.next_ticks || []).map((t) => dayWord(t, tz));
    const subDaily = new Set(days).size < days.length;
    const right = lnk(scope() ? `all ${fmtInt(d.runs_total)} in Runs` : `all ${plural(d.runs_total, "run")}`, href.runs({ label: `schedule:${d.schedule.name}` }));
    if (!rows.length) return card({ icon: "runs", title, right, body: html`<div class="empty-line">No ticks yet.</div>` });
    return card({ icon: "runs", title, right, body: tbl(["Tick", "Outcome", ["Took", "num"]], rows.map((t) => [html`${dayWord(t.tick, tz)}${subDaily || t.manual ? ` ${hm(t.tick, tz)}` : ""}${t.manual ? html` ${tag("run now")}` : ""}`, tickOutcome(t), t.runs.length ? took(t.runs.reduce((a, b) => ((a.elapsed_secs || 0) > (b.elapsed_secs || 0) ? a : b))) : "—"])) });
  }

  function failedTicksTable(d) {
    return card({ icon: "runs", title: "Recent ticks", body: tbl(["Tick", "Outcome", "Reason"], (d.recent_ticks || []).slice(0, 8).map((t) => [when(t.tick, d.schedule.timezone), tickOutcome(t), t.errors.length ? t.errors.join("; ") : t.reason || "—"])) });
  }

  function fileCard(s) {
    const lines = [`- name: ${s.name}`, `  cron: "${s.cron}"`, `  timezone: ${s.timezone}`, `  template: { id: ${s.template.id}${s.template.version ? `, version: ${s.template.version}` : ""} }`];
    return card({ icon: "code", title: "schedules.yaml", aside: "as loaded at start", body: code(lines.join("\n"), { box: false }) });
  }

  function historyCard(d) {
    const rows = d.changes || [];
    if (!rows.length) return card({ title: "Change history", body: html`<div class="empty-line">${d.schedule.source === "file" ? "Defined in the schedules file; it has no history here." : "No changes recorded."}</div>` });
    const word = { created: "Created", updated: "Edited", paused: "Paused", resumed: "Resumed", run_now: "Run now" };
    const tone = { paused: "warn", resumed: "ok" };
    return card({ title: "Change history", right: "newest first", body: html`<div class="log" style="font-family:var(--sans);font-size:12.5px;line-height:2">${rows.map((c) => html`<div><span class="muted" style="display:inline-block;width:130px">${when(c.at, d.schedule.timezone)}</span><span class="dot ${tone[c.action] || "muted"}"></span>${word[c.action] || c.action} by <b>${c.by}</b>${c.detail && c.detail.length ? html` · <span class="mono">${c.detail.join(" · ")}</span>` : ""}</div>`)}</div>` });
  }

  function missedCard(d) {
    const ms = d.missed || [];
    if (!ms.length) return empty("No missed ticks", "Every tick in the last 30 days fired on time.", { icon: "check" });
    const tz = d.schedule.timezone;
    return card({ icon: "alert", title: plural(ms.reduce((n, m) => n + m.count, 0), "missed tick"), aside: "last 30 days", body: tbl(["Ticks", "Why", "Caught up"], ms.map((m) => [m.count > 1 ? `${when(m.from, tz)} – ${when(m.to, tz)} (${m.count})` : when(m.to, tz), `no ${m.reason.replace(/^while /, "")}`.replace("no no instance was running", "no replica was running").replace("no the server was behind", "the server was behind"), m.caught_up ? "the latest ran once" : "not run"])),
      note: "Missed ticks are counted, not run later. Use Run now to catch up by hand." });
  }

  function runsCard(d) {
    const runs = (d.recent_ticks || []).flatMap((t) => t.runs).slice(0, 50);
    return card({ icon: "runs", title: "Runs started by this schedule", right: lnk("open in Runs", href.runs({ label: `schedule:${d.schedule.name}` })),
      body: runs.length ? tbl(["Run", "Tenant", "Status", ["Rows", "num"], "Started"], runs.map((r) => ({ href: href.run(r.run_id), cells: [lnk(r.name || r.run_id, href.run(r.run_id)), r.tenant || "—", dotText(RUN_TONE[r.status] ?? "", r.status), fmtInt(r.records_written || 0), when(r.submitted_at, d.schedule.timezone)] }))) : html`<div class="empty-line">No runs yet.</div>`,
      note: html`Runs carry the labels <code>schedule=${d.schedule.name}</code> and <code>schedule_tick</code>.` });
  }

  function scheduleDetail() {
    const s = find(state.name);
    if (!s && data) return html`${dhead("", "", back())}${empty(`No schedule named “${state.name}”`, "It may have been deleted, or it does not run for this tenant.", { icon: "sched", btns: btn("All schedules", { href: href.schedules() }) })}`;
    if (!detail || detail.schedule.name !== state.name) return detailErr ? html`${dhead("", "", back())}${errorAlert("the schedule", detailErr)}` : html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`;
    const d = detail;
    const sch = d.schedule;
    const t = scope();
    const sharedForTenant = tenantPrincipal() && sch.shared;
    const tabNames = sharedForTenant ? ["Overview", ["Your runs", d.runs_total]] : ["Overview", [t && sch.shared ? `Runs · ${t}` : "Runs", d.runs_total], [t && sch.shared ? `Missed · ${t}` : "Missed ticks", (d.missed || []).reduce((n, m) => n + m.count, 0)], "History"];
    const tabsHtml = dtabs(tabNames, state.dtab, { hrefFor: (val) => hashFor(href.schedule(sch.name), { tab: state.tab !== "All" ? state.tab : "", dtab: val === "Overview" ? "" : val }) });
    const status = sch.paused ? pill("Paused", "", "pause") : unhealthy(sch) ? pill("Unhealthy", "bad", "alert") : pill("Active", "ok", "check");
    const sub = [
      status,
      sch.source === "file" ? chip("from schedules file") : "",
      sharedForTenant ? chip("managed by the server operator") : sch.shared && t ? chip(`shared · runs for ${plural(tenantCount(sch, d.applies_to) ?? 0, "tenant")}`) : "",
      html`<span>${cronLine(sch)} ${sch.timezone}</span>`,
      html`<span>runs ${tplLink(sch)}</span>`,
      sch.tenant && !tenantPrincipal() ? html`<span>tenant ${sch.tenant}</span>` : "",
      sch.created_by && !sharedForTenant ? html`<span>created by <b>${sch.created_by}</b></span>` : "",
    ];
    const top = html`${dhead(tabsHtml, sharedForTenant ? "" : actionsFor(sch), back())}${head(sch.name, sub)}${sch.description ? html`<p class="lede">${sch.description}</p>` : ""}`;
    let banner = "";
    if (sch.source === "file") banner = alert("Defined in the server’s schedules file.", html`Change it in the file passed to <code>--schedules</code> and restart the server. The console can run it now but cannot edit, pause or delete it.`, "");
    else if (sharedForTenant) banner = alert("Managed by the server operator.", `It runs for ${me().tenant} and other tenants. You can see when it runs and ${me().tenant}’s runs from it; ask the operator to change or pause it.`, "");
    else if (sch.shared && t) banner = alert(`Runs for ${andList(d.applies_to || []) || "several tenants"}; changes apply to ${allOf((d.applies_to || []).length)}.`, `Runs, missed ticks and recent ticks below are ${t}’s only, because the switcher is set to ${t}. Edit, Pause and Delete change the schedule for every tenant it runs for.`, "warn");
    if (sch.paused) banner = html`${banner}${alert(html`Paused${sch.paused_by ? html` by <b>${sch.paused_by}</b>` : ""}${sch.paused_at ? html` ${ago(sch.paused_at, " ago")}` : ""}.`, html`${sch.pause_reason ? html`“${sch.pause_reason}” ` : ""}${sch.skipped_while_paused != null ? `${plural(sch.skipped_while_paused, "tick was", "ticks were")} skipped; they are not run later.` : ""}`, "")}`;
    else if (sch.status === "failing") banner = html`${banner}${alert("The last tick was refused.", html`${sch.last_error || ""} Each tick is recorded as failed with its reason; none are skipped silently.`, "bad", sch.can_edit ? html`${btn("Edit target", { icon: "edit", perm: "schedule_write", data: { act: "edit" } })}${btn("Pause", { icon: "pause", perm: "schedule_write", data: { act: "pause" } })}` : "")}`;
    else if (sch.status === "retrying") banner = html`${banner}${alert("The last tick has not started yet.", `${sch.last_error || ""} It is retried until it starts.`, "warn")}`;
    if (state.dtab === "Runs" || state.dtab.startsWith("Runs ·") || state.dtab === "Your runs") return html`${top}${banner}${runsCard(d)}`;
    if (state.dtab === "History") return html`${top}${historyCard(d)}`;
    if (state.dtab === "Missed ticks" || state.dtab.startsWith("Missed ·")) return html`${top}${missedCard(d)}`;
    if (sch.status === "failing") return html`${top}${banner}${failedTicksTable(d)}`;
    const right = sch.source === "file" ? fileCard(sch) : html`${sharedForTenant ? "" : tenantsCard(d, sch)}${ticksTable(d, sharedForTenant ? `${me().tenant}’s runs` : t && sch.shared ? `Recent ticks · ${t}` : "Recent ticks")}`;
    const left = sch.source === "file" || sharedForTenant || (sch.shared && t) ? ticksCard(d, sch) : html`${ticksCard(d, sch)}${targetCard(sch, d)}`;
    return html`${top}${banner}<div class="two"><div class="stack">${left}</div><div class="stack">${right}</div></div>`;
  }

  // ── The create / edit form ────────────────────────────────────────────────
  function freshForm(base) {
    const s = base || null;
    const tz = s ? s.timezone : "UTC";
    const ver = s ? version(s) : "stable";
    const mode = /^\d+$/.test(ver) ? "pin" : ["stable", "newest"].includes(ver) ? ver : "channel";
    return {
      edit: !!s,
      orig: s,
      name: s ? s.name : "",
      description: s ? s.description || "" : "",
      template: s ? s.template.id : "",
      vmode: mode,
      vtext: mode === "pin" || mode === "channel" ? String(ver) : "",
      sink: s ? s.template.sink || "" : "",
      sinkVersion: s ? s.template.sink_version || "stable" : "stable",
      overlay: s ? s.template.overlay || "" : "",
      params: s ? { ...s.template.params } : {},
      declared: null,
      declaredErr: null,
      streams: null,
      picked: s && (s.run || {}).selection && (s.run || {}).selection.select ? new Set((s.run || {}).selection.select) : null,
      cron: s ? s.cron : "0 2 * * *",
      timezone: tz,
      overlap: s ? s.overlap : "skip",
      runFor: s ? (s.tenants === "all" ? "all tenants" : Array.isArray(s.tenants) ? "selected" : "this server") : "this server",
      tenants: s && Array.isArray(s.tenants) ? [...s.tenants] : [],
      pool: s ? (s.run || {}).pool || "" : "",
      timeout: s && (s.run || {}).timeout_secs ? `${Math.round((s.run || {}).timeout_secs / 60)}m` : "",
      labels: s ? Object.entries((s.run || {}).labels || {}).map(([k, v]) => `${k}=${v}`).join(", ") : "",
      preview: null,
      tried: false,
      busy: false,
      err: null,
    };
  }

  const tplOf = (id) => (templates || []).find((t) => t.id === id) || null;
  const kindOf = (t) => (t && t.kind) || "pipeline";

  async function loadTemplates() {
    if (templates) return;
    try {
      const r = await api("/v1/templates");
      templates = (r.templates || r || []).filter((t) => ["pipeline", "source-template"].includes(kindOf(t)) || ["sink-template", "deployment"].includes(kindOf(t)));
    } catch {
      templates = [];
    }
    paint();
  }

  function versionSel() {
    if (form.vmode === "pin") return form.vtext.trim();
    if (form.vmode === "channel") return form.vtext.trim();
    return form.vmode;
  }

  async function loadDeclared() {
    const id = form.template;
    if (!id) { form.declared = {}; return; }
    const v = versionSel() || "stable";
    try {
      const r = await api(`/v1/templates/${enc(id)}?version=${enc(v)}`);
      if (form.template !== id) return;
      const ps = { ...(r.params || {}) };
      for (const [extraId, extraV] of [[form.sink, form.sinkVersion], [form.overlay, "stable"]]) {
        if (!extraId) continue;
        try {
          const x = await api(`/v1/templates/${enc(extraId)}?version=${enc(extraV || "stable")}`);
          for (const [k, p] of Object.entries(x.params || {})) if (!(k in ps)) ps[k] = p;
        } catch { /* the form still works without them */ }
      }
      form.declared = ps;
      form.resolved = r.version ?? null;
      form.declaredErr = null;
    } catch (e) {
      form.declared = null;
      form.declaredErr = e;
    }
    if (kindOf(tplOf(id)) === "source-template" && form.sink) {
      try {
        const rows = await api(`/v1/templates/${enc(id)}/rows?${new URLSearchParams({ sink: form.sink })}`);
        form.streams = (rows.rows || []).map((r) => r.id);
        if (!form.picked) form.picked = new Set((rows.rows || []).filter((r) => r.default_selected !== false).map((r) => r.id));
      } catch {
        form.streams = null;
      }
    } else form.streams = null;
    paint();
  }

  let previewTimer = null;
  function schedulePreview() {
    clearTimeout(previewTimer);
    previewTimer = setTimeout(async () => {
      const cron = form.cron;
      const tz = form.timezone;
      try {
        const p = await api("/v1/schedules/preview", { method: "POST", body: { cron, timezone: tz } });
        if (form && form.cron === cron && form.timezone === tz) { form.preview = p; paint(); }
      } catch (e) {
        if (form) { form.preview = { valid: false, error: e.message, next_ticks: [] }; paint(); }
      }
    }, 250);
  }

  function paramError(p, v) {
    const val = v == null ? "" : String(v);
    if (val === "") return p.required && p.default == null ? "Required: this param has no default." : "";
    if (p.type === "int" && !/^\s*-?\d+\s*$/.test(val)) return "Must be a whole number.";
    if (p.type === "float" && !Number.isFinite(Number(val))) return "Must be a number.";
    if (p.values && !p.values.map(String).includes(val)) return `Must be one of ${p.values.join(", ")}.`;
    return "";
  }
  const isSealed = (v) => v && typeof v === "object" && v[SEALED] === true;

  function cronError() {
    const p = form.preview;
    if (p && !p.valid) {
      const why = String(p.error || "").split(": ").pop().replace(/\.$/, "");
      return `Not a valid cron: ${why ? why[0].toLowerCase() + why.slice(1) : "it does not parse"}.`;
    }
    const min = data && data.tenant_min_interval_secs;
    const owner = form.edit ? form.orig.tenant : tenantPrincipal() ? me().tenant : "";
    if (owner && min && p && p.shortest_gap_secs != null && p.shortest_gap_secs < min) {
      const human = (s) => (s % 3600 === 0 && s >= 3600 ? plural(s / 3600, "hour") : s % 60 === 0 && s >= 60 ? plural(s / 60, "minute") : plural(s, "second"));
      return `Ticks every ${human(p.shortest_gap_secs)}; ${owner}’s schedules may tick at most every ${human(min)}.`;
    }
    return "";
  }

  function specFromForm() {
    const spec = { name: form.name.trim(), cron: form.cron.trim(), timezone: form.timezone, overlap: form.overlap, template: { id: form.template } };
    if (form.description.trim()) spec.description = form.description.trim();
    const v = versionSel();
    if (v && v !== "stable") spec.template.version = v;
    if (kindOf(tplOf(form.template)) === "source-template") {
      if (form.sink) spec.template.sink = form.sink;
      if (form.sinkVersion && form.sinkVersion !== "stable") spec.template.sink_version = form.sinkVersion;
    }
    if (form.overlay) spec.template.overlay = form.overlay;
    const params = {};
    for (const [k, p] of Object.entries(form.declared || {})) {
      const val = form.params[k];
      if (val === undefined || val === "") continue;
      if (isSealed(val)) { params[k] = { [SEALED]: true }; continue; }
      params[k] = p.type === "int" ? Number.parseInt(val, 10) : p.type === "float" ? Number(val) : p.type === "bool" ? val === true || val === "true" : val;
    }
    if (Object.keys(params).length) spec.template.params = params;
    if (!tenantPrincipal() && !(form.edit && form.orig.tenant)) {
      if (form.runFor === "all tenants") spec.tenants = "all";
      else if (form.runFor === "selected" && form.tenants.length) spec.tenants = form.tenants;
    }
    const run = {};
    if (form.pool.trim()) run.pool = form.pool.trim();
    const m = /^\s*(\d+)\s*([smh]?)\s*$/.exec(form.timeout);
    if (m) run.timeout_secs = Number(m[1]) * ({ s: 1, m: 60, h: 3600, "": 60 }[m[2]]);
    const labels = {};
    for (const pair of form.labels.split(",").map((x) => x.trim()).filter(Boolean)) {
      const [k, ...rest] = pair.split("=");
      if (k && rest.length) labels[k.trim()] = rest.join("=").trim();
    }
    if (Object.keys(labels).length) run.labels = labels;
    if (form.streams && form.picked) run.selection = { select: [...form.picked] };
    if (Object.keys(run).length) spec.run = run;
    return spec;
  }

  function formErrors() {
    const errs = {};
    if (!form.edit && !/^[A-Za-z0-9_-]{1,64}$/.test(form.name.trim())) errs.name = "Letters, digits, dashes and underscores; at most 64.";
    if (!form.template) errs.template = "Pick a registered template.";
    if (kindOf(tplOf(form.template)) === "source-template" && !form.sink) errs.sink = "A source template runs paired with a sink.";
    if ((form.vmode === "pin" && !/^\d+$/.test(form.vtext.trim())) || (form.vmode === "channel" && !form.vtext.trim())) errs.version = form.vmode === "pin" ? "A version number." : "A channel name.";
    const ce = cronError();
    if (ce) errs.cron = ce;
    for (const [k, p] of Object.entries(form.declared || {})) {
      const v = form.params[k];
      if (isSealed(v)) continue;
      const e = paramError(p, v);
      if (e) errs[`p:${k}`] = e;
    }
    if (form.streams && form.picked && !form.picked.size) errs.streams = "Choose at least one stream.";
    return errs;
  }

  function formHtml() {
    if (!templates) return html`<span class="skel t"></span>${skelCards(3)}`;
    const errs = formErrors();
    const show = (k) => (form.tried ? errs[k] || "" : k === "cron" ? errs.cron || "" : "");
    const runnable = templates.filter((t) => ["pipeline", "source-template"].includes(kindOf(t)));
    const sinks = templates.filter((t) => kindOf(t) === "sink-template");
    const overlays = templates.filter((t) => kindOf(t) === "deployment");
    const tpl = tplOf(form.template);
    const source = kindOf(tpl) === "source-template";
    const seg = (name, items, on) => html`<span class="seg" data-seg="${name}">${items.map((x) => html`<span class="${x === on ? "on" : ""}" data-v="${x}">${x}</span>`)}</span>`;
    const verField = field("Version", html`${seg("vmode", ["stable", "newest", "channel", "pin"], form.vmode)}${form.vmode === "channel" || form.vmode === "pin" ? html`<div style="margin-top:6px">${input(form.vtext, { name: "f-vtext", mono: true, ph: form.vmode === "pin" ? "3" : "prod" })}</div>` : ""}`, { help: form.vmode === "pin" ? "pin never moves" : form.resolved != null ? `${versionSel() || form.vmode} · resolves to v${form.resolved} now` : "stable follows launches · resolved at each tick", err: show("version"), cls: source ? "wide" : "" });
    const tplField = field("Template", select([["", "choose a template…"], ...runnable.map((t) => [t.id, `${t.id} · ${kindOf(t) === "source-template" ? "source template" : "pipeline template"}`])], form.template, { name: "f-template" }), { req: true, help: tenantPrincipal() ? `Only templates ${me().tenant} may run` : "Registered templates only", err: show("template"), cls: source ? "wide" : "" });
    const what = source
      ? html`<div class="form">${tplField}${verField}${field("Sink template", select([["", "choose a sink…"], ...sinks.map((t) => [t.id, t.id])], form.sink, { name: "f-sink" }), { req: true, help: "A source template runs paired with a sink", err: show("sink") })}${field("Sink version", select([["stable", "stable"], ["newest", "newest"]], form.sinkVersion, { name: "f-sinkv" }))}
          ${field("Deployment overlay", select([["", "none"], ...overlays.map((t) => [t.id, t.id])], form.overlay, { name: "f-overlay" }), { help: "Optional · state, DLQ, notifications", cls: "wide" })}
          ${form.streams ? field("Streams", html`<div style="display:flex;gap:14px;flex-wrap:wrap;min-height:30px;align-items:center">${form.streams.map((x) => check(x, !!(form.picked && form.picked.has(x)), { name: `s:${x}` }))}</div>`, { help: `${form.picked ? form.picked.size : 0} of ${form.streams.length} streams`, err: show("streams"), cls: "wide" }) : ""}</div>`
      : html`<div class="form one">${tplField}${verField}</div>`;
    let paramsBody;
    if (!form.template) paramsBody = html`<div class="empty-line">Pick a template to see its params.</div>`;
    else if (form.declaredErr) paramsBody = html`<div class="pad">${errorAlert("the template’s params", form.declaredErr)}</div>`;
    else if (!form.declared) paramsBody = html`<div class="pad">${skelRows(2)}</div>`;
    else if (!Object.keys(form.declared).length) paramsBody = html`<div class="empty-line">The template declares no params.</div>`;
    else paramsBody = html`<div class="form">${Object.entries(form.declared).map(([k, p]) => {
      const v = form.params[k];
      const secret = !!(p.secret || (p.items && p.items.secret));
      const sealed = isSealed(v);
      let control;
      if (p.values) control = select([...(p.default == null ? [["", "choose…"]] : []), ...p.values.map((x) => [String(x), String(x)])], v == null ? (p.default == null ? "" : String(p.default)) : String(v), { name: `p:${k}` });
      else if (p.type === "bool") control = select([["", p.default != null ? `default · ${p.default}` : "choose…"], ["true", "true"], ["false", "false"]], v == null ? "" : String(v), { name: `p:${k}` });
      else control = input(sealed ? "" : v == null ? "" : String(v), { name: `p:${k}`, type: secret ? "password" : "text", ph: sealed ? "•••••••• stored · leave blank to keep" : p.default != null ? String(p.default) : p.type || "string", cls: show(`p:${k}`) ? "err" : "" });
      const help = secret ? "Secret · sealed with the server vault key, never shown again" : p.values ? `One of ${p.values.join(", ")}` : [p.type || "string", p.default != null ? `default ${p.default}` : "", p.description || ""].filter(Boolean).join(" · ");
      return field(k, control, { code: true, req: !!p.required && p.default == null, help, err: show(`p:${k}`), cls: secret ? "wide" : "" });
    })}</div>`;
    const prev = form.preview;
    const ce = cronError();
    const tz = form.timezone;
    const previewBody = ce || (prev && !prev.valid) ? empty("No preview", "Fix the cron to see the next ticks.", {}) : !prev ? html`<div class="pad">${skelRows(2)}</div>` : tz === "UTC" ? tbl([`Next ${prev.next_ticks.length} ticks`, "UTC"], prev.next_ticks.map((t) => [dayWord(t, "UTC"), hm(t, "UTC")])) : tbl([`Next ${prev.next_ticks.length} ticks`, tz, ["UTC", "num"]], prev.next_ticks.map((t) => [dayWord(t, tz), hm(t, tz), hm(t, "UTC")]));
    const tzs = TZS.includes(tz) ? TZS : [tz, ...TZS];
    const whenCard = card({ icon: "clock", title: "When", aside: "DST-correct in the timezone",
      body: html`<div class="form">${field("Name", input(form.name, { name: "f-name", mono: true, cls: form.edit ? "dis" : show("name") ? "err" : "", ph: "nightly-rollup" }), { req: true, help: form.edit ? "Names cannot change" : "Letters, digits, dashes", err: form.edit ? "" : show("name") })}${field("Timezone", select(tzs.map((z) => [z, z]), tz, { name: "f-tz" }))}
        ${field("Cron", input(form.cron, { name: "f-cron", mono: true, cls: ce ? "err" : "" }), ce ? { req: true, err: ce } : { req: true, help: cronWords(form.cron) ? `${cronWords(form.cron)[0].toUpperCase()}${cronWords(form.cron).slice(1)}` : "min hour day month weekday" })}${field("Overlap", seg("overlap", ["skip", "queue", "forbid"], form.overlap), { help: "While the last tick still runs" })}</div>
        <div class="pad" style="padding-top:0">${previewBody}</div>` });
    const allTenants = (data && data.tenants_all) || null;
    const ownerLocked = tenantPrincipal() || (form.edit && form.orig.tenant);
    const tenantsCard2 = ownerLocked ? "" : card({ icon: "tenants", title: "Tenants", aside: "operators only",
      body: html`<div class="form one">${field("Run for", seg("runfor", ["this server", "all tenants", "selected"], form.runFor))}${form.runFor === "selected" ? html`<div style="display:flex;gap:6px;flex-wrap:wrap;align-items:center">${form.tenants.map((t) => html`<button type="button" class="chip x" data-untenant="${t}" title="Remove ${t}">${t}</button>`)}${(() => { const rest = (tenantList() || []).map((t) => t.id).filter((id) => !form.tenants.includes(id)); return rest.length ? select([["", "+ add"], ...rest.map((id) => [id, id])], "", { name: "f-addtenant", cls: "chip-add" }) : ""; })()}</div>` : ""}</div>`,
      note: "One run per tenant each tick, each admitted against that tenant’s limits." });
    void allTenants;
    const opts = card({ title: "Run options", body: html`<div class="form three">${field("Pool", input(form.pool, { name: "f-pool", ph: "default" }))}${field("Timeout", input(form.timeout, { name: "f-timeout", ph: "45m" }))}${field("Labels", input(form.labels, { name: "f-labels", mono: true, ph: "team=data" }))}</div>` });
    const owner = tenantPrincipal() ? me().tenant : "";
    const limitAlert = owner && data ? alert(data.max_schedules != null ? `${owner} can have ${plural(data.max_schedules, "schedule")}; ${fmtInt(data.owned ?? 0)} ${(data.owned ?? 0) === 1 ? "is" : "are"} in use.` : `${owner}’s schedules run only for ${owner}.`, `Ticks fire at most every ${Math.round((data.tenant_min_interval_secs || 300) / 60)} minutes, and each run counts against ${owner}’s run limits.`, "") : "";
    let editNote = "";
    if (form.edit) {
      const before = form.orig;
      const after = specFromForm();
      const lines = [];
      const cmp = (label, a, b) => { if (JSON.stringify(a ?? null) !== JSON.stringify(b ?? null)) { lines.push(["-", `${label}: ${typeof a === "object" ? JSON.stringify(a) : a ?? "—"}`]); lines.push(["+", `${label}: ${typeof b === "object" ? JSON.stringify(b) : b ?? "—"}`]); } };
      cmp("cron", before.cron, after.cron);
      cmp("timezone", before.timezone, after.timezone);
      cmp("overlap", before.overlap, after.overlap);
      cmp("template", `${before.template.id}@${version(before)}`, `${after.template.id}@${after.template.version || "stable"}`);
      cmp("params", before.template.params || {}, after.template.params || {});
      cmp("tenants", before.tenants || null, after.tenants || null);
      if (!lines.length) lines.push([" ", "nothing changes yet"]);
      editNote = card({ icon: "diff", title: "What changes", body: diff(lines), note: "Takes effect from the next tick. A tick that already started keeps running." });
    }
    const gated = data && data.requires_approval;
    const nErr = Object.keys(errs).length;
    const submitLabel = form.busy ? "Saving…" : gated ? "Request approval" : form.edit ? "Save changes" : "Create schedule";
    const foot = html`<div class="actions" style="border:0;padding:4px 0 0">${gated ? html`<span class="muted">This server requires approval for schedule changes; this becomes a change request.</span>` : ""}${btn("Cancel", { data: { act: "cancel" } })}${btn(submitLabel, { kind: "primary", icon: gated ? "changes" : "check", perm: "schedule_write", disabled: form.busy, data: { act: "submit" } })}</div>`;
    const title = form.edit ? `Edit ${form.orig.name}` : "New schedule";
    const errAlert = form.tried && nErr ? alert(`${plural(nErr, "field needs", "fields need")} attention.`, "Nothing is saved until they are fixed.", "bad") : "";
    const serverErr = form.err ? alert("The server refused the schedule.", form.err.message, "bad") : "";
    return html`${dhead("", "", back())}${head(title, [], form.edit ? "" : "Runs a registered template on a cron. Every tick goes through the same admission, budgets and audit as any other run.")}
      ${limitAlert}${errAlert}${serverErr}
      <div class="pair"><div class="stack">${card({ icon: "tpl", title: "What it runs", body: what })}${card({ icon: "sliders", title: "Parameters", aside: "from the template’s params", body: paramsBody })}${opts}</div>
      <div class="stack">${whenCard}${tenantsCard2}${editNote}</div></div>${foot}`;
  }

  function requestedHtml() {
    const c = requested;
    const ch = c.change || {};
    const p = ch.payload || {};
    const spec = p.spec || {};
    const params = Object.entries((spec.template && spec.template.params) || {}).map(([k, v]) => `${k}=${v && typeof v === "object" ? "(sealed)" : v}`).join(", ");
    const lines = Object.entries({ name: spec.name, cron: spec.cron ? `${spec.cron} · ${spec.timezone || "UTC"}` : "", template: spec.template ? `${spec.template.id} @ ${spec.template.version || "stable"}` : "", params, tenants: spec.tenants ? (Array.isArray(spec.tenants) ? spec.tenants.join(", ") : spec.tenants) : "" }).filter(([, v]) => v).map(([k, v]) => ["+", `${k}: ${v}`]);
    const sid = shortId(c.change_id);
    const effect = { create: "The schedule starts ticking once an approver accepts it on the Changes page. Nothing runs until then.", update: "The change applies once an approver accepts it on the Changes page. The schedule keeps ticking as it is until then.", delete: "The schedule is deleted once an approver accepts it on the Changes page. It keeps ticking until then." }[p.action] || "Nothing changes until an approver accepts it on the Changes page.";
    const by = ch.requester || me().principal;
    return html`${dhead("", btn("Open in Changes", { icon: "changes", kind: "primary", href: href.change(c.change_id) }), back())}${head(p.name || "Schedule", [pill("Pending approval", "warn"), by ? html`<span>requested by <b>${by}</b> · just now</span>` : "", html`<span>change ${lnk(sid, href.change(c.change_id), true)}</span>`])}
      ${alert(`Change request ${sid} created.`, effect, "warn", btn("Open in Changes", { icon: "changes", href: href.change(c.change_id) }))}
      ${card({ icon: "diff", title: "What was requested", body: diff(lines.length ? lines : [[" ", p.action || ""]]) })}`;
  }

  function detailHtml() {
    if (loadErr) return alert("Schedules are unavailable right now.", "They are stored in the run-history database, which this server cannot reach. Ticks resume once it reconnects.", "bad", btn("Retry", { icon: "refresh", data: { reload: "1" } }));
    if (!data) return html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`;
    if (requested) return requestedHtml();
    if (form) return formHtml();
    if (!state.name) return overviewDetail();
    return scheduleDetail();
  }

  function paint() {
    if (!alive) return;
    L.select(!!state.name || !!form);
    patch(L.list, listHtml());
    patch(L.detail, detailHtml());
    const crumb = requested ? ((requested.change && requested.change.payload && requested.change.payload.name) || "Change request") : form ? (form.edit ? form.orig.name : "New schedule") : state.name;
    setCrumbs(crumb ? ["Operate", { label: "Schedules", href: href.schedules() }, crumb] : ["Operate", "Schedules"]);
  }

  // ── Actions ───────────────────────────────────────────────────────────────
  function openForm(base) {
    requested = null;
    form = freshForm(base);
    loadTemplates();
    if (form.template) loadDeclared();
    schedulePreview();
    paint();
  }

  async function submit() {
    form.tried = true;
    const errs = formErrors();
    if (Object.keys(errs).length) { paint(); toast(`${plural(Object.keys(errs).length, "field needs", "fields need")} attention`, "bad"); return; }
    const spec = specFromForm();
    form.busy = true;
    form.err = null;
    paint();
    try {
      const path = form.edit ? `/v1/schedules/${enc(form.orig.name)}` : "/v1/schedules";
      const r = await api(path, { method: form.edit ? "PUT" : "POST", body: spec });
      if (r && r.status === "pending_approval") {
        requested = r;
        form = null;
        toast("Approval requested: the change waits for an approver", "info", { href: href.change(r.change_id), label: "open" });
        paint();
        return;
      }
      toast(html`${form.edit ? "Saved" : "Created"} <b>${spec.name}</b>`, "ok");
      form = null;
      await load();
      refreshBadges(true);
      navigate(href.schedule(spec.name));
    } catch (e) {
      form.err = e;
      form.busy = false;
      toast(e.message, "bad");
      paint();
    }
  }

  async function runNow(s) {
    const d = detail && detail.schedule.name === s.name ? detail : null;
    const going = d && (d.recent_ticks || []).some((t) => t.runs.some((r) => ["queued", "running", "pending", "sharded"].includes(r.status)));
    const targets = s.tenant ? `for ${s.tenant}` : s.shared ? `for its ${plural(tenantCount(s, d && d.applies_to) ?? 0, "tenant")}` : "";
    const like = s.next_tick && !s.paused ? `the ${hm(s.next_tick, s.timezone)} tick` : "a scheduled tick";
    const lastDone = d ? (d.recent_ticks || []).flatMap((t) => t.runs).map((r) => r.finished_at).filter(Boolean).sort().pop() : null;
    const calm = lastDone ? `The last tick finished at ${hm(lastDone, s.timezone)}, so nothing overlaps.` : "Nothing from an earlier tick is still running.";
    const r = await dialog(`Run ${s.name} now?`, html`<span>Starts one tick now${targets ? html` <b>${targets}</b>` : ""}, exactly as ${like} would. The next scheduled tick is unchanged.</span>
      ${going ? alert("A run from an earlier tick is still going.", "This run starts anyway; the overlap rule applies only to scheduled ticks.", "warn") : alert(calm, "", "ok")}`,
      html`${btn("Cancel", { data: { act: "no" } })}${btn("Run now", { kind: "primary", icon: "play", data: { act: "yes" } })}`, { icon: "play" });
    if (!r || r.act !== "yes") return;
    try {
      const res = await api(`/v1/schedules/${enc(s.name)}/run`, { method: "POST" });
      const id = (res.runs || [])[0];
      toast(html`Started <b>${s.name}</b> · ${plural((res.runs || []).length, "run")}${(res.errors || []).length ? ` · ${res.errors.length} refused` : ""}`, (res.errors || []).length ? "warn" : "ok", id && !id.startsWith("change:") && !id.startsWith("(") ? { href: href.run(id), label: "open run" } : null);
      loadDetail();
    } catch (e) {
      toast(e.message, "bad");
    }
  }

  async function pause(s, paused) {
    const sharedNames = detail && detail.applies_to ? detail.applies_to : [];
    const scoped = paused && s.shared && scope();
    let reason = "";
    if (paused) {
      const r = await dialog(scoped ? `Pause ${s.name} for every tenant?` : `Pause ${s.name}?`,
        html`<span>${scoped ? html`This schedule runs for <b>${andList(sharedNames)}</b>. Pausing stops ticks for all of them, not only ${scope()}. ` : ""}No ticks fire while it is paused, and skipped ticks are not run later. Resume it at any time.</span>
        ${scoped ? alert(`To stop it for ${scope()} only, edit the schedule and remove ${scope()} from its tenants.`, "", "") : ""}
        ${field("Reason", input("", { name: "reason", ph: "Warehouse maintenance tonight" }), { help: "Optional · shown on the schedule and in the audit log" })}`,
        html`${btn("Cancel", { data: { act: "no" } })}${btn(scoped ? `Pause for all ${sharedNames.length} tenants` : "Pause", { kind: "primary", icon: "pause", data: { act: "yes" } })}`, { icon: "pause" });
      if (!r || r.act !== "yes") return;
      reason = (r.values.reason || "").trim();
    }
    try {
      await api(`/v1/schedules/${enc(s.name)}/${paused ? "pause" : "resume"}`, { method: "POST", body: reason ? { reason } : {} });
      toast(html`${paused ? "Paused" : "Resumed"} <b>${s.name}</b>`, "ok");
      await load();
      loadDetail();
    } catch (e) {
      toast(e.message, "bad");
    }
  }

  async function remove(s) {
    const r = await dialog(`Delete ${s.name}?`, html`<span>It stops ticking and leaves the list. Runs it already started stay in history.</span>${field(html`Type <code>${s.name}</code> to confirm`, input("", { name: "confirm", mono: true }))}<div class="help bad" data-err hidden></div>`,
      html`${btn("Cancel", { data: { act: "no" } })}${btn("Delete schedule", { kind: "danger", icon: "trash", data: { act: "yes", keep: "1" } })}`, { icon: "trash", onOpen: (el) => {
        el.querySelector('[data-act="yes"]').addEventListener("click", async () => {
          const err = el.querySelector("[data-err]");
          if (el.querySelector("[name=confirm]").value.trim() !== s.name) { err.hidden = false; err.textContent = "The name does not match."; return; }
          try {
            const res = await api(`/v1/schedules/${enc(s.name)}`, { method: "DELETE" });
            el.querySelector('[data-act="no"]').click();
            if (res && res.status === "pending_approval") { requested = res; paint(); toast("Approval requested: the delete waits for an approver", "info", { href: href.change(res.change_id), label: "open" }); return; }
            toast(html`Deleted <b>${s.name}</b>`, "ok");
            await load();
            refreshBadges(true);
            navigate(href.schedules());
          } catch (e) {
            err.hidden = false;
            err.textContent = e.message;
          }
        });
      } });
    return r;
  }

  // ── Events ────────────────────────────────────────────────────────────────
  L.list.addEventListener("click", (e) => {
    const tb = e.target.closest("[data-tab]");
    if (tb) setQuery({ tab: tb.dataset.tab === "All" ? "" : tb.dataset.tab });
    if (e.target.closest("[data-reload]")) load();
    if (e.target.closest('[data-act="new"]') && can("schedule_write") && !atLimit()) openForm(null);
  });
  L.detail.addEventListener("click", (e) => {
    if (e.target.closest("[data-reload]")) { load(); return; }
    const a = e.target.closest("[data-act]");
    if (a && !a.disabled) {
      const s = find(state.name) || (detail && detail.schedule);
      const act = a.dataset.act;
      if (act === "new" && can("schedule_write")) openForm(null);
      else if (act === "cancel") { form = null; requested = null; paint(); }
      else if (act === "submit") submit();
      else if (s && act === "run") runNow(s);
      else if (s && act === "pause") pause(s, true);
      else if (s && act === "resume") pause(s, false);
      else if (s && act === "edit") openForm(s);
      else if (s && act === "delete") remove(s);
      return;
    }
    const segItem = e.target.closest("[data-seg] [data-v]");
    if (segItem && form) {
      const which = segItem.closest("[data-seg]").dataset.seg;
      const val = segItem.dataset.v;
      if (which === "vmode") { form.vmode = val; form.vtext = ""; loadDeclared(); }
      if (which === "overlap") form.overlap = val;
      if (which === "runfor") form.runFor = val;
      paint();
      return;
    }
    const un = e.target.closest("[data-untenant]");
    if (un && form) { form.tenants = form.tenants.filter((t) => t !== un.dataset.untenant); paint(); }
  });
  L.detail.addEventListener("change", (e) => {
    if (!form) return;
    const t = e.target;
    const n = t.name || "";
    if (n === "f-template") { form.template = t.value; form.params = {}; form.declared = null; form.streams = null; form.picked = null; form.sink = ""; loadDeclared(); }
    else if (n === "f-sink") { form.sink = t.value; form.streams = null; form.picked = null; loadDeclared(); }
    else if (n === "f-sinkv") { form.sinkVersion = t.value; loadDeclared(); }
    else if (n === "f-overlay") { form.overlay = t.value; loadDeclared(); }
    else if (n === "f-tz") { form.timezone = t.value; schedulePreview(); }
    else if (n.startsWith("s:")) { if (!form.picked) form.picked = new Set(); if (t.checked) form.picked.add(n.slice(2)); else form.picked.delete(n.slice(2)); }
    else if (n.startsWith("p:") && t.tagName === "SELECT") form.params[n.slice(2)] = t.value;
    else if (n === "f-addtenant" && t.value.trim()) { const id = t.value.trim(); if (!form.tenants.includes(id)) form.tenants.push(id); }
    else if (n === "f-vtext") loadDeclared();
    else return;
    paint();
  });
  L.detail.addEventListener("input", (e) => {
    if (!form) return;
    const t = e.target;
    const n = t.name || "";
    if (n === "f-name") form.name = t.value;
    else if (n === "f-cron") { form.cron = t.value; schedulePreview(); }
    else if (n === "f-pool") form.pool = t.value;
    else if (n === "f-timeout") form.timeout = t.value;
    else if (n === "f-labels") form.labels = t.value;
    else if (n === "f-vtext") form.vtext = t.value;
    else if (n.startsWith("p:")) {
      const k = n.slice(2);
      // A blank secret field keeps the stored (sealed) value.
      if (t.type === "password" && !t.value && form.orig && isSealed((form.orig.template.params || {})[k])) form.params[k] = { [SEALED]: true };
      else form.params[k] = t.value;
    }
  });

  paint();
  await load();
  loadDetail();
  if (state.mode === "new" && can("schedule_write")) openForm(null);
  const stop = poll(async () => {
    if (form) return;
    await load();
    if (state.name) loadDetail();
  }, 15000);
  return {
    teardown() { alive = false; stop(); clearTimeout(previewTimer); },
    update(p) {
      const next = { name: p.id || "", tab: p.query.get("tab") || "All", dtab: p.query.get("dtab") || "Overview", mode: p.query.get("mode") || "" };
      const changed = next.name !== state.name;
      state = next;
      if (changed) { form = null; requested = null; detail = null; }
      paint();
      if (changed) loadDetail();
    },
  };
}

// Exported for tests that check the pure helpers.
export const _test = { cronWords, until, runsFor, verLabel, raw };

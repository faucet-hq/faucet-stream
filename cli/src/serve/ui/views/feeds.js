// Feeds (#1034): split pane over GET /v1/feeds[/{name}[/events|refusals]].
import { api } from "../api.js";
import { html, ic, row, ring, sect, tabs, ibtn, stats, empty, skelRows, skelCards, head, card, grid, kv, tbl, pill, chip, tag, code, btn, dhead, dtabs, lnk, alert, field, input, select, check, toggle, diff, join, menu, dotText, spark, bar, legend, meter, cpi, shortId } from "../components.js";
import { layout, setQuery, patch, poll, errorAlert, hashFor, ago, dialog, toast } from "../page.js";
import { setCrumbs, refreshBadges } from "../shell.js";
import { fmtInt, fmtAge } from "../utils.js";
import { formatTs } from "../tz.js";
import { href } from "../links.js";
import { can, whoami } from "../access.js";
import { currentTenant, tenantList } from "../tenant.js";
import { navigate } from "../router.js";

const enc = encodeURIComponent;
const SEALED = "$sealed";
const plural = (n, w, ws = `${w}s`) => `${fmtInt(n)} ${n === 1 ? w : ws}`;
const RUN_TONE = { completed: "ok", failed: "bad", cancelled: "muted", running: "info", sharded: "info", queued: "warn", pending: "warn" };
const OUTCOME = { delivered: ["ok", "completed"], failed: ["bad", "failed"], refused: ["bad", "refused"], cancelled: ["muted", "cancelled"], claim_lost: ["warn", "claim lost"] };
const REASON = { unauthorized: "Bad token or signature", rate_limited: "Rate limited", too_large: "Too large", bad_request: "Not valid", unsupported_media_type: "Content not accepted", content_encoding: "Content not accepted", duplicate: "Duplicate", full: "Feed full", held: "Feed held", store: "Store unreachable", timeout: "Timed out", busy: "Busy", draining: "Shutting down", definitions_stale: "Definitions unreadable" };
const REASON_TONE = ["var(--bad)", "var(--warn)", "var(--info)", "var(--text-2)", "var(--faint)"];
const ACCEPT = [["application/json", "JSON"], ["application/x-ndjson", "NDJSON"], ["application/x-www-form-urlencoded", "Form"], ["text/plain", "Text"]];
const DOCS = "https://faucet-hq.github.io/faucet-stream/cookbook/feeds/";

// ── Sizes and spans ─────────────────────────────────────────────────────────
const SIZE_UNITS = { B: 1, KIB: 2 ** 10, MIB: 2 ** 20, GIB: 2 ** 30, TIB: 2 ** 40, KB: 1e3, MB: 1e6, GB: 1e9, TB: 1e12 };
const SPAN_WORDS = { s: 1, sec: 1, secs: 1, second: 1, seconds: 1, m: 60, min: 60, mins: 60, minute: 60, minutes: 60, h: 3600, hr: 3600, hrs: 3600, hour: 3600, hours: 3600, d: 86400, day: 86400, days: 86400 };
const numUnit = (x) => /^\s*([\d,]+)\s*([A-Za-z]*)\s*$/.exec(String(x ?? ""));
/** Bytes of a size the API renders (`50GiB`, a number) or a person types (`50 GiB`). */
function sizeBytes(x) {
  if (typeof x === "number") return x;
  const m = numUnit(x);
  const unit = m && SIZE_UNITS[(m[2] || "B").toUpperCase()];
  return unit ? Number(m[1].replace(/,/g, "")) * unit : NaN;
}
/** Seconds of a span the API renders (`7d`, a number) or a person types (`7 days`). */
function spanSecs(x) {
  if (typeof x === "number") return x;
  const m = numUnit(x);
  const unit = m && SPAN_WORDS[(m[2] || "s").toLowerCase()];
  return unit ? Number(m[1].replace(/,/g, "")) * unit : NaN;
}
function bytes(b) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let x = Number(b) || 0;
  let i = 0;
  while (x >= 1024 && i < units.length - 1) { x /= 1024; i += 1; }
  return `${i === 0 || x >= 100 || Number.isInteger(x) ? fmtInt(Math.round(x)) : x.toFixed(1)} ${units[i]}`;
}
/** A span in words: `7 days`, `2 hours`, `5m`, `60s`. */
function span(secs) {
  const s = Number(secs) || 0;
  if (s && s % 86400 === 0) return plural(s / 86400, "day");
  if (s && s % 3600 === 0) return plural(s / 3600, "hour");
  if (s && s % 60 === 0 && s >= 120) return `${fmtInt(s / 60)}m`;
  return `${fmtInt(s)}s`;
}
/** How long ago `iso` was, in words: `38s`, `6 minutes`, `3 hours`, `2 days`. */
function since(iso) {
  const s = Math.max(0, Math.round((Date.now() - Date.parse(iso)) / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return plural(Math.floor(s / 60), "minute");
  if (s < 86400) return plural(Math.floor(s / 3600), "hour");
  return plural(Math.floor(s / 86400), "day");
}
/** What a person typed as a size, in the API's form (`64MiB`), or null. */
const sizeText = (x) => (sizeBytes(x) > 0 ? String(x).replace(/[\s,]/g, "") : null);
/** What a person typed as a span, in the API's form (`7d`), or null. */
function spanText(x) {
  const s = spanSecs(x);
  if (!(s > 0)) return null;
  for (const [u, f] of [["d", 86400], ["h", 3600], ["m", 60]]) if (s % f === 0) return `${s / f}${u}`;
  return `${s}s`;
}
const intText = (x) => (/^\s*[\d,]+\s*$/.test(String(x)) ? Number(String(x).replace(/[\s,]/g, "")) : NaN);
const seqNo = (n) => html`<span class="mono">#${fmtInt(n)}</span>`;
const dayMonth = (iso) => new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short", timeZone: "UTC" }).format(new Date(iso));
const until = (iso) => {
  const s = Math.round((Date.parse(iso) - Date.now()) / 1000);
  if (s <= 0) return "expired";
  return s < 3600 ? `in ${Math.max(1, Math.round(s / 60))}m` : s < 86400 ? `in ${Math.round(s / 3600)}h` : `in ${plural(Math.round(s / 86400), "day")}`;
};
const firstLine = (s) => String(s || "").split("\n")[0].replace(/^\d+ invocation\(s\) failed(; )?/, "");
/** A dedupe window in words; a day reads as 24h. */
const windowWords = (x) => (spanSecs(x) === 86400 ? "24h" : span(spanSecs(x)));

// ── Facts about one feed ────────────────────────────────────────────────────
const fkey = (f) => `${f.tenant || ""}/${f.name}`;
const flabel = (f) => (f.tenant ? `${f.tenant}/${f.name}` : f.name);
const lagOf = (f) => Math.max(0, ...f.deliveries.map((d) => d.lag || 0));
const storeDown = (f) => f.watermark == null;
const sickDelivery = (f) => f.deliveries.find((d) => ["stalled", "held", "failing", "needs_approval"].includes(d.status));
const needsAttention = (f) => storeDown(f) || ["stalled", "held", "full"].includes(f.status) || !!sickDelivery(f);
const version = (d) => (d.template && d.template.version) || "stable";
const verLabel = (v) => (/^\d+$/.test(String(v)) ? `pin v${v}` : ["stable", "newest", "previous"].includes(v) ? v : `channel ${v}`);
const signIn = (spec) => {
  const a = (spec.ingest || {}).auth || {};
  return a.tokens && a.signature ? "both" : a.signature ? "signature" : "token";
};
const signInWords = (spec) => ({ both: "token + signature", signature: "signature", token: "token" })[signIn(spec)];
const sealed = (f) => f.spec.seal ?? !!f.tenant;
const flushWords = (d) => `${fmtInt(d.flush.max_events)} events or ${span(spanSecs(d.flush.max_age))}`;
const acceptWord = (t) => (ACCEPT.find(([x]) => t && t.startsWith(x)) || [t, t || "—"])[1];
const oldestWaiting = (f) => f.deliveries.map((d) => d.oldest_undelivered_at).filter(Boolean).sort()[0] || null;
const undelivered = (spec) => `${bytes(sizeBytes(spec.retention.undelivered.max_bytes))} or ${span(spanSecs(spec.retention.undelivered.max_age))}`;

function behind(d) {
  if (d.lag == null) return "—";
  if (d.status === "paused") return `${fmtInt(d.lag)} · paused`;
  if (!d.lag) return "0 · up to date";
  const text = `${fmtInt(d.lag)}${d.oldest_undelivered_at ? ` · ${fmtAge(d.oldest_undelivered_at)}` : ""}`;
  return ["stalled", "failing", "held"].includes(d.status) ? html`<span class="${d.status === "failing" ? "warn" : "bad"}">${text}</span>` : text;
}
function lastRun(d) {
  const r = d.last_run;
  if (!r) return d.claim ? dotText("info", "running") : "—";
  const [tone, word] = OUTCOME[r.outcome] || ["muted", r.outcome];
  return html`${dotText(tone, word)} ${ago(r.at, " ago")}`;
}

// ── The page ────────────────────────────────────────────────────────────────
export async function renderFeeds(host, params) {
  const L = layout(host, "split");
  let data = null;          // GET /v1/feeds (merged over tenants for an operator)
  let loadErr = null;
  let off = false;          // the server has no feeds API
  let detail = null;        // GET /v1/feeds/{name}
  let detailErr = null;
  let events = null;        // { key, pages: [EventsPage], err }
  let opened = null;        // { key, seq, event, err, payload }
  let refusals = null;      // { key, list }
  let runs = null;          // { key, delivery, list, total }
  let templates = null;
  let form = null;          // feed create / edit form
  let dform = null;         // delivery add / edit form
  let requested = null;     // the change request a gated change made
  let created = null;       // { key, token, tokenName } after a create
  let alive = true;
  const read = (p) => ({ name: p.id || "", ft: p.query.get("t") || "", tab: p.query.get("tab") || "All", dtab: p.query.get("dtab") || "Overview", delivery: p.query.get("delivery") || "", event: p.query.get("event") || "", mode: p.query.get("mode") || "" });
  let state = read(params);

  const me = () => whoami() || {};
  const scope = () => currentTenant();
  const tenantPrincipal = () => !!me().tenant;
  const list = () => (data && data.feeds) || [];
  const selKey = () => `${state.ft || scope()}/${state.name}`;
  const current = () => (detail && fkey(detail) === selKey() ? detail : null);
  const q = (tenant, extra = {}) => {
    const p = new URLSearchParams(extra);
    if (tenant) p.set("tenant", tenant);
    const s = p.toString();
    return s ? `?${s}` : "";
  };
  const base = (f) => `/v1/feeds/${enc(f.name)}`;
  const listQuery = () => ({ tab: state.tab !== "All" ? state.tab : "" });
  const feedHref = (f, extra = {}) => hashFor(`#/feeds/${enc(f.name)}`, { ...listQuery(), t: f.tenant && !tenantPrincipal() && !scope() ? f.tenant : "", ...extra });
  const deliveryHref = (f, d) => feedHref(f, { dtab: "Deliveries", delivery: d });
  const back = () => ({ href: hashFor(href.feeds(), listQuery()), label: "Feeds" });
  const backToFeed = (f) => ({ href: feedHref(f), label: f.name });
  const ingestHere = () => !data || data.ingest_on_api !== false;
  const ingestUrl = (f) => (ingestHere() ? `${location.origin}${f.ingest_path}` : f.ingest_path);
  const gated = (withDelivery) => !!data && (data.approval === "always" || (data.approval === "deliveries" && withDelivery));
  const noStore = () => !!data && !off && !(data.stores || []).length && !list().length;

  async function load() {
    try {
      const t = scope();
      // An operator looking at every tenant asks for all of them, a page at a time.
      const everyone = !t && !tenantPrincipal();
      const page = (after) => api(`/v1/feeds${everyone ? `?tenant=*${after ? `&after=${enc(after)}` : ""}` : `${q(t)}${after ? `${t ? "&" : "?"}after=${enc(after)}` : ""}`}`);
      const first = await page();
      let feeds = first.feeds || [];
      for (let next = first.next; next; ) {
        const more = await page(next);
        feeds = feeds.concat(more.feeds || []);
        next = more.next;
      }
      data = { ...first, feeds };
      loadErr = null;
      off = false;
    } catch (e) {
      if (e.status === 404) { off = true; data = { feeds: [], stores: [], approval: "never" }; loadErr = null; } else loadErr = e;
    }
    paint();
  }

  async function loadDetail() {
    const key = selKey();
    if (!state.name || off) { detail = null; return; }
    try {
      const d = await api(`/v1/feeds/${enc(state.name)}${q(state.ft || scope())}`);
      if (selKey() !== key) return;
      detail = d;
      detailErr = null;
    } catch (e) {
      if (selKey() !== key) return;
      detail = null;
      detailErr = e;
    }
    paint();
    await loadTab();
  }

  const shownDelivery = (f) => f.deliveries.find((d) => d.name === state.delivery) || f.deliveries[0] || null;

  async function loadTab() {
    const f = current();
    if (!f) return;
    const key = fkey(f);
    if (state.dtab === "Events" && !storeDown(f)) {
      if (!events || events.key !== key) events = { key, pages: [], err: null };
      try {
        const page = await api(`${base(f)}/events${q(f.tenant, { limit: "25" })}`);
        if (!events || events.key !== key) return;
        events.pages = [page, ...events.pages.slice(1)];
        events.err = null;
      } catch (e) { if (events && events.key === key) events.err = e; }
      if (state.event) {
        const fresh = !opened || opened.seq !== Number(state.event);
        await openEvent(f, Number(state.event));
        paint();
        if (fresh) document.getElementById("fd-event")?.scrollIntoView({ block: "nearest" });
        return;
      }
      paint();
    } else if (state.dtab === "Refused") {
      try {
        const r = await api(`${base(f)}/refusals${q(f.tenant)}`);
        refusals = { key, list: r.refusals || [] };
      } catch { refusals = { key, list: [] }; }
      paint();
    } else if (state.dtab === "Deliveries") {
      const d = shownDelivery(f);
      if (!d) return;
      try {
        const p = new URLSearchParams({ limit: "20", counts: "true" });
        p.append("label", `feed:${flabel(f)}`);
        p.append("label", `feed_delivery:${d.name}`);
        if (f.tenant) p.set("tenant", f.tenant);
        const r = await api(`/v1/runs?${p}`);
        const total = Object.values(r.counts || {}).reduce((n, x) => n + (Number(x) || 0), 0);
        runs = { key, delivery: d.name, list: r.runs || [], total: Math.max(total, (r.runs || []).length) };
      } catch { runs = { key, delivery: d.name, list: [], total: 0 }; }
      paint();
    }
  }

  async function openEvent(f, seq) {
    const key = fkey(f);
    if (opened && opened.key === key && opened.seq === seq && (opened.event || opened.err)) return;
    const envelope = events && events.key === key ? events.pages.flatMap((p) => p.events).find((e) => e.seq === seq) : null;
    opened = { key, seq, event: envelope || null, err: null, payload: false };
    if (!can("feed_admin")) return;
    try {
      opened.event = await api(`${base(f)}/events/${seq}${q(f.tenant)}`);
      opened.payload = true;
    } catch (e) { opened.err = e; }
  }

  // ── List pane ─────────────────────────────────────────────────────────────
  function listRow(f) {
    const sick = sickDelivery(f);
    const lag = lagOf(f);
    const n = f.deliveries.length;
    let metaHtml;
    let tone = "ok";
    let shape = "";
    if (storeDown(f)) { metaHtml = html`<b>store unreachable</b> · ${plural(n, "delivery", "deliveries")} holding`; tone = "bad"; }
    else if (f.status === "held") { metaHtml = html`<b>held</b> · not receiving · needs recovery`; tone = "warn"; }
    else if (f.status === "stalled") { metaHtml = html`<b>delivery stalled</b> · ${sick ? sick.name : ""} · ${fmtInt(lag)} pending`; tone = "bad"; }
    else if (f.status === "full") { metaHtml = html`<b>refusing events</b> · undelivered limit reached`; tone = "bad"; }
    else if (sick && sick.status === "failing") { metaHtml = html`<b>delivery failing</b> · ${sick.name} · ${fmtInt(lag)} pending`; tone = "warn"; }
    else if (sick) { metaHtml = html`<b>needs approval</b> · ${sick.name} · ${fmtInt(lag)} pending`; tone = "warn"; }
    else if (f.status === "paused") { const p = f.deliveries.find((d) => d.paused_by); metaHtml = html`<b>delivery paused</b> · ${fmtInt(lag)} pending${p ? ` · by ${p.paused_by}` : ""}`; tone = ""; shape = "hollow"; }
    else {
      const last = f.source === "file" ? "from feeds file" : f.tenant && !scope() ? `tenant ${f.tenant}` : `${fmtInt(f.events_24h || 0)} today`;
      metaHtml = html`<b>${fmtInt(lag)} pending</b> · ${plural(n, "delivery", "deliveries")} · ${last}`;
    }
    return row(ring(tone, shape), f.name, f.last_event_at && f.status !== "full" ? fmtAge(f.last_event_at) : "—", metaHtml, { on: !!detail && fkey(f) === selKey() && !form, id: fkey(f), href: feedHref(f) });
  }

  const guaranteeWords = (g) => (g === "at-least-once" ? "at least once" : /atomic watermark/.test(g) ? "exactly once" : /keyed upsert/.test(g) ? "effectively once, by key" : "guarantee unknown");
  const atLimit = () => !!(data && data.quotas && data.quotas.max_feeds != null && data.quotas.feeds >= data.quotas.max_feeds);
  function newButton() {
    if (!data || off || noStore()) return "";
    return btn("New", { icon: "plus", kind: "primary", perm: "feed_write", disabled: atLimit(), title: atLimit() ? "This tenant has reached its feed limit" : "", data: { act: "new" } });
  }
  const refreshBtn = () => ibtn("refresh", { title: "Refresh", data: { reload: "1" } });

  function totals(all) {
    const today = all.reduce((n, f) => n + (f.events_24h || 0), 0);
    const pending = all.reduce((n, f) => n + lagOf(f), 0);
    const refused = all.reduce((n, f) => n + (f.refused_recent || 0), 0);
    const stored = all.reduce((n, f) => n + (f.retained_bytes || 0), 0);
    const pct = today + refused ? `${((refused / (today + refused)) * 100).toFixed(1)}%` : "0%";
    return { today, pending, refused, stored, pct };
  }

  function listHtml() {
    if (loadErr) return html`${tabs([["All", "—"]], "All", refreshBtn())}${empty("Could not load feeds", `The server answered ${loadErr.status || "an error"}.`, { icon: "alert", btns: btn("Retry", { icon: "refresh", data: { reload: "1" } }) })}`;
    if (!data) return html`${tabs([["All", "…"]], "All", ibtn("refresh"))}${skelRows(8)}`;
    const all = list();
    if (off || noStore()) return html`${tabs([["All", 0]], "All", refreshBtn())}${empty(off ? "Feeds are not built in" : "Feeds are off", off ? "This faucet binary was built without the feeds feature." : "This server keeps its history in memory.", { icon: "feed" })}`;
    const bad = all.filter(needsAttention);
    const top = tabs([["All", all.length], ["Needs attention", bad.length]], state.tab, html`${refreshBtn()}${newButton()}`);
    const t = scope();
    if (!all.length) return html`${top}${empty(t && !tenantPrincipal() ? `No feeds belong to ${t}` : "No feeds yet", "A feed gives senders a URL to push events to.", { icon: "feed" })}`;
    const shown = (state.tab === "Needs attention" ? bad : all).slice().sort((a, b) => a.name.localeCompare(b.name));
    const tt = totals(all);
    const paused = shown.filter((f) => !needsAttention(f) && f.status === "paused");
    const live = shown.filter((f) => !needsAttention(f) && f.status !== "paused");
    const attention = shown.filter(needsAttention);
    const groups = (liveLabel) => html`${attention.length ? html`${sect("Needs attention", attention.length)}${attention.map(listRow)}` : ""}${live.length ? html`${sect(liveLabel, live.length)}${live.map(listRow)}` : ""}${paused.length ? html`${sect("Delivery paused", paused.length)}${paused.map(listRow)}` : ""}`;
    let statsHtml;
    let body;
    if (tenantPrincipal()) {
      const max = data.quotas && data.quotas.max_feeds;
      statsHtml = stats([["Feeds", max != null ? `${fmtInt(all.length)} of ${fmtInt(max)}` : fmtInt(all.length), atLimit() ? "warn" : ""], ["Today", fmtInt(tt.today)], ["Pending", fmtInt(tt.pending)], ["Stored", bytes(tt.stored)]]);
      body = groups("Receiving");
    } else if (t) {
      statsHtml = stats([[`For ${t}`, fmtInt(all.length)], ["Today", fmtInt(tt.today)], ["Pending", fmtInt(tt.pending)], ["Stored", bytes(tt.stored)]]);
      body = html`${groups(`${t}’s feeds`)}<div class="muted" style="font-size:12px;padding:10px 14px">Showing feeds that belong to ${t}. Pick “All tenants” above to see every feed.</div>`;
    } else {
      statsHtml = stats([["Feeds", fmtInt(all.length)], ["Today", fmtInt(tt.today)], ["Pending", fmtInt(tt.pending), bad.length && tt.pending ? "warn" : ""], ["Refused", tt.pct]]);
      body = groups("Receiving");
    }
    if (!shown.length) body = empty("Nothing needs attention", "", { icon: "check" });
    return html`${top}${statsHtml}${body}`;
  }

  // ── Detail: nothing selected ──────────────────────────────────────────────
  function quotasCard(note = "", icon = "") {
    const qd = data.quotas;
    if (!qd) return "";
    const rate = list().filter((f) => f.tenant === qd.tenant).reduce((n, f) => n + ((f.spec.ingest.rate_limit || {}).events_per_sec || 0), 0);
    const of = (used, max, fmt = fmtInt) => (max != null ? `${fmt(used)} of ${fmt(max)}` : fmt(used));
    return card({ icon, title: `${qd.tenant}’s quotas`, body: grid(3, [
      kv("Feeds", of(qd.feeds, qd.max_feeds), atLimit() ? "at the limit" : ""),
      kv(icon ? "Rate limits set" : "Rate limits set on its feeds", html`${of(rate, qd.max_feed_events_per_sec)} <span class="muted">events/s</span>`, "the sum of its feeds’ configured limits, not live traffic"),
      kv(icon ? "Stored" : "Stored events", of(qd.stored_bytes, qd.max_feed_bytes, bytes)),
    ]), note });
  }

  function deliveriesOverview(all) {
    const rows = all.flatMap((f) => f.deliveries.map((d) => ({ f, d }))).sort((a, b) => (b.d.lag || 0) - (a.d.lag || 0));
    if (!rows.length) return "";
    return card({ title: "Deliveries", right: "most behind first", body: tbl(["Feed", "Delivery", "Behind", "Last run"], rows.slice(0, 50).map(({ f, d }) => ({ href: deliveryHref(f, d.name), cells: [lnk(f.name, feedHref(f)), lnk(d.name, deliveryHref(f, d.name)), behind(d), lastRun(d)] }))) });
  }

  function attentionAlert(f) {
    const sick = sickDelivery(f);
    const open = btn("Open", { icon: "right", href: sick ? deliveryHref(f, sick.name) : feedHref(f) });
    const openFeed = btn("Open", { icon: "right", href: feedHref(f) });
    if (storeDown(f)) return alert(`${f.name}’s store is not answering.`, "Senders get 503 with a retry time, and its deliveries wait.", "bad", openFeed);
    if (f.status === "held") return alert(`${f.name} is held.`, "Its store looks restored from a backup, so receiving and delivering are stopped until an admin recovers it.", "warn", openFeed);
    if (f.status === "stalled") return alert(`${f.name} has not delivered${sick && sick.oldest_undelivered_at ? ` for ${since(sick.oldest_undelivered_at)}` : ""}.`, `Its delivery ${sick ? sick.name : ""} is stalled on one event it cannot deliver. Events are still being stored.`, "bad", open);
    if (f.status === "full") return alert(`${f.name} is refusing new events.`, `It holds as many events that are not delivered yet as it may (${undelivered(f.spec)}), which is its limit.`, "bad", openFeed);
    if (sick && sick.status === "failing") return alert(`${f.name}’s delivery ${sick.name} is failing.`, "Its runs fail and are tried again with smaller batches. Nothing is skipped.", "warn", open);
    if (sick) return alert(`${f.name}’s delivery ${sick.name} waits for approval.`, firstLine(sick.last_error), "warn", open);
    return "";
  }

  function emptyDetail() {
    if (off) return html`${head("Feeds need the feeds feature", [], html`This server was built without it, so it cannot take pushed events. The release binaries, the Homebrew formula and the <code>full</code> container image include it.`)}${card({ icon: "code", title: "Build it in", body: code('cargo install faucet-cli --features "serve,feeds"', { box: false, lang: "text" }) })}`;
    if (noStore()) return html`${head("Feeds need a history that survives a restart", [], html`This server was started with <code>--history memory</code>. A feed’s definition would be lost at a restart while its events stayed, so feeds are off.`)}${card({ icon: "code", title: "Start it with a lasting history", body: code("faucet serve --history sqlite:./history.db", { box: false, lang: "text" }), note: html`Or leave <code>--history</code> out: the default is a SQLite file under <code>$FAUCET_DATA_DIR</code>.` })}`;
    return html`${head("Take pushed events without losing any", [], "Senders post to a feed’s URL. The server stores each event before it answers, then runs your template over the new events every few seconds or minutes.")}
      <div class="two">${card({ icon: "plus", title: "In the console", body: html`<div class="pad stack" style="gap:10px"><span>Name the feed, choose how senders sign in, and pick the template that delivers its events.</span><div>${btn("New feed", { icon: "plus", kind: "primary", perm: "feed_write", data: { act: "new" } })}</div></div>` })}
      ${card({ icon: "code", title: "feeds.yaml", body: code(`version: 1
feeds:
  - name: support-tickets
    ingest:
      auth: { tokens: managed }
    deliveries:
      - name: to-warehouse
        template: { id: acme/tickets-to-lake, version: 4 }
        flush: { max_events: 5000, max_age: 60s }`, { box: false }), note: html`Then start the server with <code>faucet serve --feeds feeds.yaml</code>.` })}</div>`;
  }

  function overviewDetail() {
    const all = list();
    const t = scope();
    if (!all.length) return html`${emptyDetail()}${t && !off ? quotasCard() : ""}`;
    const tt = totals(all);
    const alerts = all.filter(needsAttention).map(attentionAlert);
    if (tenantPrincipal()) {
      const qd = data.quotas || {};
      const limit = atLimit() ? alert(`${t} has reached its limit of ${plural(qd.max_feeds, "feed")}.`, html`Delete one to create another, or ask an operator to raise <code>max_feeds</code>.`, "warn") : "";
      return html`${head(`${t}’s feeds`, [qd.max_feeds != null ? html`<span>${fmtInt(qd.feeds)} of ${fmtInt(qd.max_feeds)} in use</span>` : "", html`<span>${fmtInt(tt.today)} events today</span>`])}${limit}${alerts}${quotasCard()}${deliveriesOverview(all)}`;
    }
    if (t) return html`${head(`${plural(all.length, "feed belongs", "feeds belong")} to ${t}`, [html`<span>${fmtInt(tt.today)} events today</span>`, html`<span>${fmtInt(tt.pending)} pending</span>`])}${alerts}${quotasCard()}${deliveriesOverview(all)}`;
    return html`${head(plural(all.length, "feed"), [html`<span>${fmtInt(tt.today)} events today</span>`, html`<span class="${tt.pending && alerts.length ? "warn" : ""}">${fmtInt(tt.pending)} pending</span>`, html`<span>${tt.pct} refused</span>`])}
      ${alerts}${deliveriesOverview(all)}`;
  }

  // ── Detail: one feed ──────────────────────────────────────────────────────
  const editable = (f) => f.source !== "file";
  function feedTabs(f) {
    const live = (f.tokens || []).filter((t) => !t.revoked_at).length;
    const items = ["Overview", ["Deliveries", f.deliveries.length], "Events", ["Refused", fmtInt(f.refused_recent || 0)], ["Tokens", live], "History"];
    return dtabs(items, state.dtab, { hrefFor: (val) => feedHref(f, { dtab: val === "Overview" ? "" : val }) });
  }
  function feedActions(f) {
    const test = btn("Send test event", { icon: "play", perm: "feed_write", data: { act: "test" } });
    if (f.status === "held") return btn("Recover…", { icon: "refresh", kind: "primary", perm: "feed_admin", data: { act: "recover" } });
    if (!editable(f)) return test;
    return html`${test}${btn("Edit", { icon: "edit", perm: "feed_write", data: { act: "edit" } })}${menu("more", [{ label: "Delete feed", icon: "trash", cls: "bad", perm: "feed_admin", data: { act: "delete" } }], { perm: "feed_admin" })}`;
  }
  function feedPill(f) {
    if (storeDown(f)) return pill("Store unreachable", "bad", "alert");
    if (f.status === "held") return pill("Held", "warn", "alert");
    if (f.status === "full") return pill("Refusing events", "bad", "alert");
    if (f.status === "stalled") return pill("Delivery stalled", "bad", "alert");
    if (f.status === "paused") return pill("Delivery paused", "", "pause");
    if (!f.last_event_at && !f.watermark) return pill("Waiting for the first event", "", "clock");
    return pill("Receiving", "ok", "check");
  }
  function feedHead(f) {
    const n = f.deliveries.length;
    return head(f.name, [
      feedPill(f),
      f.source === "file" ? chip("from feeds file") : "",
      f.status === "held" ? html`<span>not receiving</span>` : f.last_event_at ? html`<span>last ${f.status === "full" ? "stored " : ""}event ${ago(f.last_event_at, " ago")}</span>` : "",
      sealed(f) ? chip("payloads sealed") : "",
      html`<span>${plural(n, "delivery", "deliveries")}${f.status === "held" ? " waiting" : storeDown(f) ? " holding" : ""}</span>`,
      f.tenant && !tenantPrincipal() && !scope() ? html`<span>tenant ${f.tenant}</span>` : "",
      f.created_by ? html`<span>created by <b>${f.created_by}</b></span>` : "",
    ]);
  }
  const urlBox = (text) => html`<div class="input mono fd-url"><span class="fd-url-t" title="${text}">${text}</span>${cpi(text)}</div>`;

  function sendCard(f) {
    const a = f.spec.ingest.auth;
    const d = f.spec.ingest.dedupe;
    const url = ingestUrl(f);
    return card({ icon: "link", title: "Send events here", right: html`<a class="lnk" href="${DOCS}" target="_blank" rel="noopener">how senders connect</a>`,
      body: html`<div class="pad" style="padding-bottom:6px"><div class="input mono fd-url"><span class="faint">POST</span>&nbsp; <span class="fd-url-t" title="${url}">${url}</span>${cpi(url)}</div>${ingestHere() ? "" : html`<div class="help">Events are taken on this server’s <code>--feeds-listen</code> address, not this one: put that host in front of the path.</div>`}</div>
        ${grid(4, [
          kv("Sign-in", signInWords(f.spec), a.signature ? html`signature header <span class="mono">${a.signature.header}</span>` : "write-only feed token"),
          kv("Accepts", f.spec.ingest.accept.map(acceptWord).join(", "), `up to ${bytes(sizeBytes(f.spec.ingest.max_event_bytes))} per event`),
          kv("Duplicates", d ? html`<span class="mono">${d.header || d.body_path}</span>` : "not checked", d ? (d.on_duplicate === "conflict" ? `a repeat within ${windowWords(d.window)} is answered 409` : `same id within ${windowWords(d.window)} is stored once`) : "every event is stored"),
          kv("Answers", "202 once stored", "never before the event is durable"),
        ])}`,
      note: a.tokens ? "A feed token can only add events to this feed. It cannot read events, start runs or reach any other part of the server." : "Requests are checked by their signature only; the secret is sealed and never shown." });
  }

  function flowCard(f) {
    const delivered = f.deliveries.length ? Math.min(...f.deliveries.map((d) => d.delivered_24h ?? 0)) : 0;
    const pts = f.hourly_24h || [];
    const known = f.events_24h != null;
    return card({ icon: "usage", title: "Last 24 hours", right: pts.length > 1 && pts.some((x) => x) ? spark(pts, "var(--text-2)", 170, 30) : "",
      body: grid(4, [kv("Received", known ? fmtInt(f.events_24h) : "—", "stored and answered"), kv("Delivered", known ? fmtInt(delivered) : "—", "written by a run"), kv("Pending", html`<span class="${f.status === "full" ? "bad" : ""}">${fmtInt(lagOf(f))}</span>`, "waiting for the next flush"), kv("Refused", fmtInt(f.refused_recent || 0), "never stored")]) });
  }

  function storeCard(f) {
    const und = f.spec.retention.undelivered;
    const maxBytes = sizeBytes(und.max_bytes);
    const maxAge = spanSecs(und.max_age);
    const retained = f.retained_bytes || 0;
    const held = f.undelivered_bytes != null ? f.undelivered_bytes : lagOf(f) > 0 ? retained : 0;
    const kept = Math.max(0, retained - held);
    const oldest = oldestWaiting(f);
    const tone = f.status === "full" ? "bad" : "";
    const agePct = oldest ? ((Date.now() - Date.parse(oldest)) / 1000 / maxAge) * 100 : 0;
    const keep = span(spanSecs(f.spec.retention.max_age));
    return card({ icon: "db", title: "Storage and retention", aside: html`store <span class="mono">${f.store}</span>`,
      body: html`<div class="pad stack" style="gap:12px">
        <div><div class="fd-line"><span>Not delivered yet</span><b class="${tone}">${bytes(held)} of ${bytes(maxBytes)}</b></div>${meter(Math.max(held ? 1 : 0, (held / maxBytes) * 100))}</div>
        <div><div class="fd-line"><span>Oldest waiting</span><b class="${tone}">${oldest ? since(oldest) : "nothing"} <span class="muted">of ${span(maxAge)}</span></b></div>${meter(Math.max(oldest ? 1 : 0, agePct))}</div>
        <div class="fd-line"><span>Delivered, kept for replay</span><b>${bytes(kept)} <span class="muted">· ${keep}</span></b></div></div>`,
      note: `Events are removed only after ${keep} and once every delivery has them. At the limit the feed refuses new events; it never drops stored ones.` });
  }

  const tplLink = (d) => (d.template ? lnk(d.template.id, href.template(d.template.id)) : html`<span class="mono">${d.pipeline}</span>`);
  function delivCard(f) {
    const add = editable(f) && f.status !== "held" ? btn("Add delivery", { icon: "plus", perm: "feed_write", data: { act: "delivery-new" } }) : "";
    if (!f.deliveries.length) return card({ icon: "runs", title: "Deliveries", right: add, body: html`<div class="empty-line">No delivery yet: events are stored and wait until one is added.</div>` });
    return card({ icon: "runs", title: "Deliveries", right: add, body: tbl(["Delivery", "Runs", "Flushes at", "Behind", "Last run"], f.deliveries.map((d) => ({ href: deliveryHref(f, d.name), cells: [lnk(d.name, deliveryHref(f, d.name)), tplLink(d), flushWords(d), behind(d), lastRun(d)] }))) });
  }

  function fileCard(f) {
    const a = f.spec.ingest.auth;
    const lines = [`- name: ${f.name}`, "  ingest:", "    auth:"];
    if (a.tokens) lines.push("      tokens: managed");
    if (a.signature) lines.push(`      signature: { header: ${a.signature.header}, secret: (sealed) }`);
    if (f.deliveries.length) lines.push("  deliveries:");
    for (const d of f.deliveries) {
      lines.push(`    - name: ${d.name}`);
      lines.push(d.template ? `      template: { id: ${d.template.id}${d.template.version ? `, version: ${d.template.version}` : ""} }` : `      pipeline: ${d.pipeline}`);
      lines.push(`      flush: { max_events: ${d.flush.max_events}, max_age: ${d.flush.max_age} }`);
    }
    return card({ icon: "code", title: "feeds.yaml", aside: "as loaded at start", body: code(lines.join("\n"), { box: false }) });
  }

  function createdHtml(f) {
    const url = ingestUrl(f);
    const d0 = f.deliveries[0];
    const dd = f.spec.ingest.dedupe;
    const sig = f.spec.ingest.auth.signature;
    const curl = [`curl -X POST ${url} \\`, created.token ? `  -H "Authorization: Bearer ${created.token}" \\` : "", dd && dd.header ? `  -H "${dd.header}: evt-0001" \\` : "", '  -H "Content-Type: application/json" \\', `  -d '{"id": 1, "status": "open"}'`].filter(Boolean).join("\n");
    return html`${dhead(feedTabs(f), feedActions(f), back())}${head(f.name, [pill("Waiting for the first event", "", "clock"), html`<span>created just now</span>`])}
      ${created.token ? alert("Copy the token now. It is not shown again.", "", "warn") : alert("This feed checks a signature only.", "Senders sign each request with the secret you set; no token was made.", "")}
      <div class="two"><div class="stack">${created.token ? card({ icon: "key", title: `Token ${created.tokenName}`, body: html`<div class="pad">${urlBox(created.token)}</div>`, note: "Stored only as a hash. Lost it? Make a new token and revoke this one." }) : ""}
        ${card({ icon: "code", title: "Send the first event", body: code(curl, { box: false, lang: "text" }), note: sig ? html`This feed also checks a signature: add the <code>${sig.header}</code> header, or use <code>faucet feed send --signing-secret-env</code>.` : "" })}</div>
      <div class="stack">${card({ icon: "check", title: "What happens next", body: html`<div class="pad stack" style="gap:8px"><span>1. The sender posts to the URL and gets <b>202</b> once the event is stored.</span>${d0 ? html`<span>2. Within ${span(spanSecs(d0.flush.max_age))}, ${d0.name} runs <b>${d0.template ? d0.template.id : d0.pipeline}</b> over the new events.</span><span>3. The delivery moves forward only after that run has written them.</span>` : html`<span>2. Events are stored and wait. Add a delivery to run a template over them.</span>`}</div>` })}</div></div>
      <div class="actions" style="border:0;padding:4px 0 0">${btn("Done", { kind: "primary", icon: "check", data: { act: "created-done" } })}</div>`;
  }

  function overviewTab(f) {
    const top = html`${dhead(feedTabs(f), feedActions(f), back())}${feedHead(f)}`;
    const sick = sickDelivery(f);
    if (f.status === "held") {
      const d = f.deliveries.find((x) => x.status === "held") || f.deliveries[0];
      const lost = d && d.position != null && f.watermark != null && d.position - 1 > f.watermark;
      return html`${top}${alert("The feed store looks restored from a backup.", lost ? html`The delivery ${d.name} says it delivered up to ${seqNo(d.position - 1)}, but the store’s newest event is ${seqNo(f.watermark)}. Receiving and delivering are stopped so nothing is skipped or repeated by accident.` : "Receiving and delivering are stopped so nothing is skipped or repeated by accident.", "warn")}
        ${card({ icon: "info", title: "Recover", body: html`<div class="pad stack" style="gap:8px"><span><b>Continue from the store’s newest event.</b>${lost ? html` Events <span class="mono">#${fmtInt(f.watermark + 1)}–${fmtInt(d.position - 1)}</span> were lost with the restore and are recorded as a gap in History.` : " What was lost with the restore is recorded as a gap in History."}</span><span>Senders get 503 until an admin recovers the feed.</span></div>`, note: "Recovering is admin only and is written to the audit log." })}`;
    }
    if (storeDown(f)) return html`${top}${alert("The feed store is not answering.", html`Senders get <b>503</b> with a retry time, so no event is accepted without being stored. Deliveries wait and continue from the same place when the store is back.`, "bad", btn("Retry", { icon: "refresh", data: { reload: "1" } }))}
      <div class="two"><div class="stack">${sendCard(f)}${flowCard(f)}</div><div class="stack">${card({ icon: "db", title: "Store", body: grid(2, [kv("Name", html`<span class="mono">${f.store}</span>`), kv("State", "not answering", "deliveries hold")]) })}</div></div>`;
    let banner = "";
    if (f.source === "file") banner = alert("Defined in the server’s feeds file.", html`Change it in the file passed to <code>--feeds</code> and restart the server. The console can send a test event but cannot edit, pause or delete it.`, "");
    if (f.status === "full") {
      const slow = f.deliveries.find((d) => d.status === "paused") || sick || f.deliveries[0];
      banner = html`${banner}${alert("This feed holds as much as it is allowed to.", html`${slow ? html`Its delivery ${slow.name} ${slow.status === "paused" ? "is paused" : "is behind"}, so ` : ""}${bytes(f.undelivered_bytes != null ? f.undelivered_bytes : f.retained_bytes || 0)} of events are waiting, against a limit of ${undelivered(f.spec)}. Senders get <b>503</b> with a retry time and nothing already stored is dropped. Resume or fix the delivery, or raise the limit.`, "bad", html`${slow ? btn(`Open ${slow.name}`, { icon: "right", href: deliveryHref(f, slow.name) }) : ""}${editable(f) ? btn("Raise the limit", { icon: "edit", perm: "feed_write", data: { act: "edit" } }) : ""}`)}`;
    } else if (sick) banner = html`${banner}${attentionAlert(f)}`;
    for (const w of f.warnings || []) banner = html`${banner}${alert("Requests to this feed can be replayed.", w[0].toUpperCase() + w.slice(1) + ".", "warn")}`;
    if (f.source === "file") return html`${top}${banner}<div class="two"><div class="stack">${sendCard(f)}${flowCard(f)}</div><div class="stack">${fileCard(f)}${storeCard(f)}</div></div>${delivCard(f)}`;
    if (f.tenant && data.quotas && data.quotas.tenant === f.tenant) return html`${top}${banner}<div class="two"><div class="stack">${sendCard(f)}${flowCard(f)}</div><div class="stack">${quotasCard("Set by the server operator. Above the rate, senders get 429.", "tenants")}${storeCard(f)}</div></div>${delivCard(f)}`;
    return html`${top}${banner}${sendCard(f)}<div class="two"><div class="stack">${flowCard(f)}</div><div class="stack">${storeCard(f)}</div></div>${delivCard(f)}`;
  }

  // ── Detail: one delivery ──────────────────────────────────────────────────
  const runRows = (f, d) => (runs && runs.key === fkey(f) && runs.delivery === d.name ? runs.list : null);
  const runCount = (r) => (r.feed && r.feed.from != null ? r.feed.to - r.feed.from : null);
  const runRange = (r) => (runCount(r) == null ? "—" : html`<span class="mono">#${fmtInt(r.feed.from)}${runCount(r) > 1 ? `–${fmtInt(r.feed.to - 1)}` : ""}</span>`);
  const runName = (d, r) => `${d.name} · ${String(r.run_id).slice(-4)}`;

  function deliveryActions(f, d) {
    if (f.status === "held") return btn("Recover…", { icon: "refresh", kind: "primary", perm: "feed_admin", data: { act: "recover" } });
    const sick = ["stalled", "failing"].includes(d.status);
    const out = [];
    if (sick) out.push(btn("Try again now", { icon: "refresh", kind: "primary", perm: "feed_write", data: { act: "deliver-now" } }));
    else if (d.status !== "paused") out.push(btn("Deliver now", { icon: "play", perm: "feed_write", data: { act: "deliver-now" } }));
    out.push(d.status === "paused" ? btn("Resume", { icon: "play", kind: "primary", perm: "feed_write", data: { act: "resume" } }) : btn("Pause", { icon: "pause", perm: "feed_write", data: { act: "pause" } }));
    if (editable(f) && !sick) out.push(btn("Edit", { icon: "edit", perm: "feed_write", data: { act: "delivery-edit" } }));
    out.push(menu("more", [
      { label: "Replay…", icon: "undo", perm: "feed_admin", data: { act: "replay" } },
      { label: "Skip events…", icon: "right", perm: "feed_admin", data: { act: "skip" } },
      editable(f) ? "-" : null,
      editable(f) ? { label: "Delete delivery", icon: "trash", cls: "bad", perm: "feed_write", data: { act: "delivery-delete" } } : null,
    ], { perm: editable(f) ? "" : "feed_admin" }));
    return join(out);
  }

  function positionCard(f, d, extra = null) {
    const delivered = d.position != null ? d.position - 1 : null;
    const tone = d.status === "stalled" ? "bad" : d.status === "failing" ? "warn" : "";
    const lastOk = d.last_run && d.last_run.outcome === "delivered" ? d.last_run.at : null;
    return card({ icon: "feed", title: "Position", body: grid(4, [
      kv("Delivered to", delivered != null ? seqNo(delivered) : "—", extra ? (lastOk ? ago(lastOk, " ago") : "") : "the cursor"),
      kv("Newest event", f.watermark != null ? seqNo(f.watermark) : "—"),
      kv("Behind", html`<span class="${tone}">${plural(d.lag || 0, "event")}</span>`, d.oldest_undelivered_at && d.lag ? `oldest ${fmtAge(d.oldest_undelivered_at)}` : d.lag ? "" : "up to date"),
      extra || kv("Replay from", f.oldest != null ? seqNo(f.oldest) : "—", `kept ${span(spanSecs(f.spec.retention.max_age))}`),
    ]) });
  }

  function paramValue(val) {
    if (val && typeof val === "object" && val[SEALED] === true) return html`<span class="faint">sealed · secret param</span>`;
    return typeof val === "string" ? val : JSON.stringify(val);
  }

  function targetCard(d) {
    const failing = kv("If an event keeps failing", "stall and alert", `after ${plural(d.retries, "try", "tries")} at one event`);
    if (!d.template) return card({ icon: "tpl", title: "What it runs", body: grid(2, [kv("Pipeline file", html`<span class="mono">${d.pipeline}</span>`, "beside the feeds file"), kv("Pool", d.pool || "default"), failing]) });
    const ps = Object.entries(d.template.params || {});
    const pinned = /^\d+$/.test(String(version(d)));
    return card({ icon: "tpl", title: "What it runs", right: lnk("open template", href.template(d.template.id)),
      body: html`${grid(2, [kv("Template", lnk(d.template.id, href.template(d.template.id)), d.template.sink ? `into ${d.template.sink}` : ""), kv("Version", html`<span class="mono">${verLabel(version(d))}</span>`, pinned ? "never moves" : "resolved at each run"), kv("Pool", d.pool || "default"), failing])}
        ${ps.length ? html`<div class="pad" style="padding-top:0">${tbl(["Param", "Value"], ps.map(([k, val]) => [html`<span class="mono">${k}</span>`, paramValue(val)]))}</div>` : ""}` });
  }

  function flushCard(d) {
    return card({ icon: "clock", title: "When it flushes", body: grid(2, [kv("Events", fmtInt(d.flush.max_events), "or"), kv("Size", bytes(sizeBytes(d.flush.max_bytes)), "or"), kv("Oldest waiting", span(spanSecs(d.flush.max_age)), "whichever comes first"), kv("At most every", span(spanSecs(d.flush.min_interval)))]),
      note: `One run per flush, never one per event. A backlog is delivered in runs of up to ${fmtInt(d.max_batch.events)} events.` });
  }

  function runsCard(f, d) {
    const rows = runRows(f, d);
    const all = href.runs({ label: [`feed:${flabel(f)}`, `feed_delivery:${d.name}`] });
    const right = lnk(rows ? `all ${fmtInt(runs.total)} in Runs` : "open in Runs", all);
    const note = html`A run that fails is tried again with half as many events, until it gets through or one event is left. Runs carry the labels <code>feed=${flabel(f)}</code> and <code>feed_delivery=${d.name}</code>.`;
    if (!rows) return card({ icon: "runs", title: "Recent runs", right, body: html`<div class="pad">${skelRows(2)}</div>`, note });
    if (!rows.length) return card({ icon: "runs", title: "Recent runs", right, body: html`<div class="empty-line">No runs yet: the first one starts at the first flush.</div>`, note });
    return card({ icon: "runs", title: "Recent runs", right, body: tbl(["Run", "Events", "Status", ["Rows", "num"], "Started"], rows.slice(0, 8).map((r) => ({ href: href.run(r.run_id), cells: [lnk(runName(d, r), href.run(r.run_id)), runRange(r), dotText(RUN_TONE[r.status] ?? "", r.status), r.status === "completed" ? fmtInt(r.records_written || 0) : "—", ago(r.submitted_at, " ago")] }))), note });
  }

  function failedRunsCard(f, d) {
    const all = runRows(f, d);
    const rows = (all || []).filter((r) => r.status === "failed").slice(0, 6);
    const why = firstLine(d.last_error);
    if (!rows.length) return card({ icon: "runs", title: "Failed runs", body: html`<div class="empty-line">${all ? why || "No failed run is on record." : "Loading…"}</div>` });
    return card({ icon: "runs", title: "Failed runs", body: tbl(["Run", "Events", "Status", "Why"], rows.map((r) => ({ href: href.run(r.run_id), cells: [lnk(runName(d, r), href.run(r.run_id)), html`${runRange(r)}${runCount(r) != null ? ` · ${fmtInt(runCount(r))}` : ""}`, dotText("bad", "failed"), html`<span class="fd-wrap">${firstLine(r.error) || why || "—"}</span>`] }))) });
  }

  function deliverySwitch(f, d) {
    if (f.deliveries.length < 2) return "";
    return html`<div><span class="seg">${f.deliveries.map((x) => html`<a class="${x.name === d.name ? "on" : ""}" href="${deliveryHref(f, x.name)}">${x.name}</a>`)}</span></div>`;
  }

  function deliveryTab(f) {
    const d = shownDelivery(f);
    if (!d) return html`${dhead(feedTabs(f), feedActions(f), back())}${feedHead(f)}${delivCard(f)}`;
    const why = firstLine(d.last_error);
    const sick = ["stalled", "failing"].includes(d.status);
    const p = { paused: pill("Paused", "", "pause"), stalled: pill("Stalled", "bad", "alert"), failing: pill("Failing", "warn", "alert"), held: pill("Held", "warn", "alert"), needs_approval: pill("Needs approval", "warn", "clock"), setting_up: pill("Setting up", "", "clock") }[d.status] || pill("Delivering", "ok", "check");
    const top = html`${dhead(feedTabs(f), deliveryActions(f, d), backToFeed(f))}${deliverySwitch(f, d)}${head(d.name, [p, d.template ? html`<span>runs ${lnk(d.template.id, href.template(d.template.id))} <span class="mono">${verLabel(version(d))}</span></span>` : html`<span>runs <span class="mono">${d.pipeline}</span></span>`, d.guarantee ? html`<span>${guaranteeWords(d.guarantee)}</span>` : "", d.claim ? html`<span>a run is in progress</span>` : ""])}`;
    const lastRunBtn = d.last_run && d.last_run.run_id ? btn("Open the last run", { icon: "right", href: href.run(d.last_run.run_id) }) : "";
    if (sick) {
      const stalled = d.status === "stalled";
      const banner = stalled
        ? alert("One event cannot be delivered. Nothing was skipped.", html`Runs were cut down to a single event, and ${seqNo(d.position)} still fails${why ? html` with <code>${why}</code>` : ""}. New events are still stored. Fix the template or the destination and try again, or skip this event.`, "bad", html`${lastRunBtn}${btn("Open the event", { icon: "code", href: feedHref(f, { dtab: "Events", event: d.position }) })}${btn("Skip this event…", { icon: "right", perm: "feed_admin", data: { act: "skip" } })}`)
        : alert(`The last ${d.consecutive_failures > 1 ? `${fmtInt(d.consecutive_failures)} runs` : "run"} failed. Still trying, with smaller batches.`, html`${why ? html`Each run failed with <code>${why}</code>. ` : ""}The next run takes ${plural(d.batch_events, "event")}. Nothing is skipped, and new events are still stored.`, "warn", lastRunBtn);
      return html`${top}${banner}<div class="two"><div class="stack">${positionCard(f, d, kv("Next run takes", plural(d.batch_events, "event"), stalled ? "cannot go smaller" : "halved after each failure"))}</div><div class="stack">${storeCard(f)}</div></div>${failedRunsCard(f, d)}`;
    }
    if (d.status === "paused") {
      const ratio = f.undelivered_ratio;
      const banner = alert(html`Paused${d.paused_by ? html` by <b>${d.paused_by}</b>` : ""}${d.paused_at ? html` ${ago(d.paused_at, " ago")}` : ""}.`, html`${d.pause_reason ? html`“${d.pause_reason}” ` : ""}Events keep arriving and are stored; ${fmtInt(d.lag || 0)} ${d.lag === 1 ? "is" : "are"} waiting. They are delivered in order when it resumes.`, "");
      return html`${top}${banner}<div class="two"><div class="stack">${positionCard(f, d, kv("Feed limit", ratio != null ? `${(ratio * 100).toFixed(ratio < 0.1 ? 2 : 0)}% used` : "—", `of ${undelivered(f.spec)}`))}${targetCard(d)}</div><div class="stack">${storeCard(f)}</div></div>${runsCard(f, d)}`;
    }
    let banner = "";
    if (d.status === "held") banner = alert("Held with the feed.", "The feed’s store looks restored from a backup. Recover the feed to continue.", "warn");
    else if (d.status === "needs_approval") banner = alert("This delivery waits for approval.", why || "Its runs become change requests on this server.", "warn", btn("Open Changes", { icon: "changes", href: href.changes() }));
    return html`${top}${banner}<div class="two"><div class="stack">${positionCard(f, d)}${targetCard(d)}</div><div class="stack">${flushCard(d)}</div></div>${runsCard(f, d)}`;
  }

  // ── Detail: events, refusals, tokens, history ─────────────────────────────
  function eventsTab(f) {
    const top = html`${dhead(feedTabs(f), feedActions(f), back())}${feedHead(f)}`;
    const mine = events && events.key === fkey(f) ? events : null;
    const goto = html`<form data-goto class="fd-goto">${input("", { ph: "Go to event number", icon: "search", name: "goto", a: { inputmode: "numeric", "aria-label": "Go to event number" } })}</form>`;
    const note = "Event numbers only go up and are assigned when the event is stored. Events older than the replay window are gone.";
    if (storeDown(f)) return html`${top}${alert("The feed store is not answering.", "Its events cannot be listed right now.", "bad", btn("Retry", { icon: "refresh", data: { reload: "1" } }))}`;
    if (!mine || (!mine.pages.length && !mine.err)) return html`${top}${card({ icon: "logs", title: "Newest events", body: html`<div class="pad">${skelRows(4)}</div>` })}`;
    if (mine.err && !mine.pages.length) return html`${top}${errorAlert("the events", mine.err)}`;
    const rows = mine.pages.flatMap((p) => p.events);
    const last = mine.pages[mine.pages.length - 1];
    const sel = state.event ? Number(state.event) : null;
    const body = rows.length
      ? tbl(["Event", "Id", "Received", "Type", ["Size", "num"], "Delivered by"], rows.map((e) => ({ cls: e.seq === sel ? "on" : "", href: feedHref(f, { dtab: "Events", event: e.seq }), cells: [seqNo(e.seq), html`<span class="mono" title="${e.id}">${shortId(e.id)}</span>`, ago(e.received_at, " ago"), acceptWord(e.content_type), bytes(e.size), e.delivered_by.length ? e.delivered_by.join(" · ") : html`<span class="muted">pending</span>`] })))
      : html`<div class="empty-line">No events are stored yet.</div>`;
    const more = last && last.next_before != null ? html`<div class="pad" style="padding-top:8px;padding-bottom:8px">${btn("Older events", { icon: "chev", cls: "sm", data: { act: "older" } })}</div>` : "";
    return html`${top}${card({ icon: "logs", title: "Newest events", right: goto, body: html`${body}${more}`, note })}${eventCard(f)}`;
  }

  function eventCard(f) {
    if (!state.event) return "";
    const seq = Number(state.event);
    const o = opened && opened.key === fkey(f) && opened.seq === seq ? opened : null;
    if (!o) return card({ icon: "code", title: `Event #${fmtInt(seq)}`, id: "fd-event", body: html`<div class="pad">${skelRows(2)}</div>` });
    const e = o.event;
    if (!e) return card({ icon: "code", title: `Event #${fmtInt(seq)}`, id: "fd-event", body: html`<div class="pad">${o.err ? alert(o.err.status === 404 ? "This event is not stored." : "The event could not be read.", o.err.status === 404 ? "It is older than the replay window, or was never stored." : o.err.message, "bad") : alert("Not among the events listed.", "An admin can open any stored event by its number.", "")}</div>` });
    let payload;
    if (e.payload_withheld === "sealed" || (e.sealed && !o.payload)) payload = alert("This feed seals its events.", "Payloads are encrypted with the server’s vault key and are opened only inside a delivery run. The console shows the envelope.", "");
    else if (!o.payload) payload = alert("Payloads are for admins.", o.err ? o.err.message : "Your role sees an event’s envelope, not what it carries.", "");
    else if (e.payload_encoding === "base64") payload = html`${code(e.payload || "", { lang: "text" })}<div class="help">Not text: shown as base64.</div>`;
    else {
      let text = e.payload || "";
      let lang = "text";
      try { text = JSON.stringify(JSON.parse(text), null, 2); lang = "json"; } catch { /* shown as it was sent */ }
      payload = code(text, { lang });
    }
    const hs = Object.entries(e.headers || {});
    const open = o.payload && !e.sealed;
    return card({ icon: "code", title: `Event #${fmtInt(e.seq)}`, id: "fd-event", aside: html`received ${ago(e.received_at, " ago")} · ${bytes(e.size)} · ${acceptWord(e.content_type)}`, right: open ? tag("admins only") : "",
      body: html`${grid(3, [kv("Id", html`<span class="mono fd-wrap">${e.id}</span>`), kv("Duplicate key", e.dedupe_key ? html`<span class="mono fd-wrap">${e.dedupe_key}</span>` : "none"), kv("Stored", formatTs(e.stored_at), e.delivered_by.length ? `delivered by ${e.delivered_by.join(", ")}` : "not delivered yet")])}
        ${hs.length ? html`<div class="pad" style="padding-top:0">${tbl(["Header", "Value"], hs.map(([k, val]) => [html`<span class="mono">${k}</span>`, html`<span class="mono fd-wrap">${val}</span>`]))}</div>` : ""}
        <div class="pad" style="padding-top:0">${payload}</div>`,
      note: open ? "Opening a payload is recorded in the audit log." : "" });
  }

  function refusedTab(f) {
    const top = html`${dhead(feedTabs(f), feedActions(f), back())}${feedHead(f)}`;
    const mine = refusals && refusals.key === fkey(f) ? refusals.list : null;
    if (!mine) return html`${top}${card({ title: "Refused lately", body: html`<div class="pad">${skelRows(3)}</div>` })}`;
    if (!mine.length) return html`${top}${empty("Nothing was refused", "This server has refused no request for this feed since it started.", { icon: "check" })}`;
    const by = new Map();
    for (const r of mine) by.set(r.reason, (by.get(r.reason) || 0) + 1);
    const parts = [...by.entries()].sort((a, b) => b[1] - a[1]);
    const shown = parts.slice(0, 4);
    const rest = parts.slice(4).reduce((n, [, c]) => n + c, 0);
    if (rest) shown.push(["other", rest]);
    const total = mine.length;
    const reqs = (f.events_24h || 0) + total;
    return html`${top}${card({ title: "Refused lately", aside: `${fmtInt(total)} of ${fmtInt(reqs)} requests · ${((total / reqs) * 100).toFixed(1)}%`,
      body: html`<div class="pad">${bar(shown.map(([, c], i) => [(c / total) * 100, REASON_TONE[i]]))}${legend(shown.map(([r, c], i) => [`${REASON[r] || (r === "other" ? "Other" : r)} · ${fmtInt(c)}`, REASON_TONE[i]]))}</div>`,
      note: "A refused request stores nothing and the sender gets the status shown. Each server lists its own latest 200. A repeat of an event already stored is answered 202 and is not counted here." })}
      ${card({ icon: "alert", title: "Most recent", body: tbl(["When", "Why", ["Answer", "num"], "Sender", "Detail"], mine.slice(0, 50).map((r) => [ago(r.at, " ago"), REASON[r.reason] || r.reason, r.status, r.sender || "—", html`<span class="fd-wrap">${r.detail}</span>`])), note: "Payloads of refused requests are never kept." })}`;
  }

  function tokensTab(f) {
    const top = html`${dhead(feedTabs(f), feedActions(f), back())}${feedHead(f)}`;
    const a = f.spec.ingest.auth;
    const live = (f.tokens || []).filter((t) => !t.revoked_at);
    const tokensCard = !a.tokens
      ? card({ icon: "key", title: "Feed tokens", body: html`<div class="empty-line">This feed does not use tokens: requests are checked by their signature.</div>` })
      : card({ icon: "key", title: "Feed tokens", right: btn("New token", { icon: "plus", perm: "feed_write", data: { act: "token-new" } }),
        body: live.length ? tbl(["Name", "Starts with", "Created", "Last used", "Expires", ""], live.map((t) => [t.name, html`<span class="mono">${t.prefix}…</span>`, html`<span title="${formatTs(t.created_at)}">${dayMonth(t.created_at)} by <b>${t.created_by}</b></span>`, t.last_used_at ? ago(t.last_used_at, " ago") : "never", t.expires_at ? html`<span title="${formatTs(t.expires_at)}">${until(t.expires_at)}</span>` : "never", html`<button type="button" class="lnk fd-lnk" data-perm="feed_write" data-act="token-revoke" data-token="${t.id}">Revoke</button>`])) : html`<div class="empty-line">No token yet. Senders cannot sign in until one is made.</div>`,
        note: html`A token is shown once, when it is made. Senders put it in <code>Authorization: Bearer …</code>. Revoking takes effect within 5 seconds.` });
    const s = a.signature;
    const sigCard = s ? card({ icon: "shield", title: "Signature check", right: editable(f) ? btn("Change secret", { icon: "edit", perm: "feed_write", data: { act: "edit" } }) : "",
      body: grid(4, [kv("Header", html`<span class="mono">${s.header}</span>`), kv("Algorithm", `HMAC-${String(s.algorithm || "sha256").toUpperCase()}`, html`${s.encoding || "hex"}${s.prefix ? html`, prefix <span class="mono">${s.prefix}</span>` : ""}`), kv("Secret", html`<span class="faint">sealed</span>`, "never shown"), kv("Timestamp", s.timestamp_header ? html`<span class="mono">${s.timestamp_header}</span>` : "none", s.timestamp_header ? `within ${span(s.tolerance_secs)}` : "no replay window")]),
      note: a.tokens ? "With both set, a request needs a valid token and a valid signature." : "" }) : "";
    return html`${top}${tokensCard}${sigCard}`;
  }

  function historyTab(f) {
    const top = html`${dhead(feedTabs(f), feedActions(f), back())}${feedHead(f)}`;
    const tone = (a) => (/pause/.test(a) ? "warn" : /resume/.test(a) ? "ok" : /skip|delete|revoke/.test(a) ? "bad" : /recover|replay/.test(a) ? "warn" : "muted");
    const words = (a) => { const s = String(a).replace(/_/g, " "); return s[0].toUpperCase() + s.slice(1); };
    const items = (f.changes || []).map((c) => ({ at: c.at, tone: tone(c.action), text: html`${words(c.action)} by <b>${c.by}</b>${c.detail && c.detail.length ? html` · ${c.detail.join(" · ")}` : ""}` }));
    for (const g of f.gaps || []) items.push({ at: g.at, tone: "bad", text: html`${g.delivery || "Every delivery"} skipped <span class="mono">#${fmtInt(g.from)}${g.to - g.from > 1 ? `–${fmtInt(g.to - 1)}` : ""}</span> by <b>${g.by}</b> · ${plural(g.to - g.from, "event")} · “${g.reason}”` });
    for (const t of f.tokens || []) {
      items.push({ at: t.created_at, tone: "muted", text: html`Token ${t.name} made by <b>${t.created_by}</b>${t.expires_at ? ` · expires ${dayMonth(t.expires_at)}` : ""}` });
      if (t.revoked_at) items.push({ at: t.revoked_at, tone: "bad", text: html`Token ${t.name} revoked` });
    }
    items.sort((a, b) => b.at.localeCompare(a.at));
    if (!items.length) return html`${top}${card({ title: "Change history", body: html`<div class="empty-line">${f.source === "file" ? "Defined in the feeds file; it has no history here." : "No changes recorded."}</div>` })}`;
    return html`${top}${card({ title: "Change history", right: "newest first", body: html`<div class="log fd-log">${items.map((c) => html`<div><span class="muted fd-when" >${formatTs(c.at)}</span><span class="dot ${c.tone}"></span><span>${c.text}</span></div>`)}</div>`,
      note: "Skipped ranges stay on the feed for good. Every change is also in the audit log." })}`;
  }

  function feedDetail() {
    const f = current();
    if (!f) {
      if (detailErr && detailErr.status === 404) return html`${dhead("", "", back())}${empty(`No feed named “${state.name}”`, "It may have been deleted, or it belongs to another tenant.", { icon: "feed", btns: btn("All feeds", { href: href.feeds() }) })}`;
      if (detailErr) return html`${dhead("", "", back())}${errorAlert("the feed", detailErr)}`;
      return html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`;
    }
    if (created && created.key === fkey(f)) return createdHtml(f);
    if (state.dtab === "Deliveries") return deliveryTab(f);
    if (state.dtab === "Events") return eventsTab(f);
    if (state.dtab === "Refused") return refusedTab(f);
    if (state.dtab === "Tokens") return tokensTab(f);
    if (state.dtab === "History") return historyTab(f);
    return overviewTab(f);
  }

  // ── Forms ─────────────────────────────────────────────────────────────────
  const seg = (name, items, on) => html`<span class="seg" data-seg="${name}">${items.map((x) => html`<span class="${x === on ? "on" : ""}" data-v="${x}" role="button" tabindex="0">${x}</span>`)}</span>`;
  const kindOf = (t) => (t && t.kind) || "pipeline";
  const tplOf = (id) => (templates || []).find((t) => t.id === id) || null;
  const isSealed = (val) => !!val && typeof val === "object" && val[SEALED] === true;
  const NAME = /^[a-z0-9][a-z0-9-]{0,63}$/;

  function freshDelivery(d) {
    const ver = d ? version(d) : "stable";
    const vmode = /^\d+$/.test(ver) ? "pin" : ["stable", "newest"].includes(ver) ? ver : "channel";
    return {
      edit: !!d, orig: d || null,
      name: d ? d.name : "", start: "latest",
      template: d && d.template ? d.template.id : "", vmode, vtext: vmode === "pin" || vmode === "channel" ? String(ver) : "",
      sink: d && d.template ? d.template.sink || "" : "",
      params: d && d.template ? { ...d.template.params } : {},
      declared: null, declaredErr: null, resolved: null,
      events: d ? fmtInt(d.flush.max_events) : "5,000", size: d ? bytes(sizeBytes(d.flush.max_bytes)) : "64 MiB", age: d ? span(spanSecs(d.flush.max_age)) : "60s",
      tried: false, busy: false, err: null,
    };
  }
  function freshForm(f) {
    const s = f ? f.spec : null;
    const ing = s ? s.ingest : null;
    const tenant = f ? f.tenant || "" : scope();
    return {
      edit: !!f, orig: f || null, tenant,
      name: s ? s.name : "",
      signin: s ? signIn(s) : "token",
      sigHeader: ing && ing.auth.signature ? ing.auth.signature.header : "X-Signature-256",
      sigSecret: "",
      addresses: ing ? (ing.auth.allowed_addresses || []).join("\n") : "",
      accept: new Set(ing ? ing.accept : ["application/json", "application/x-ndjson"]),
      maxEvent: ing ? bytes(sizeBytes(ing.max_event_bytes)) : "1 MiB",
      rate: ing && ing.rate_limit ? fmtInt(ing.rate_limit.events_per_sec) : "",
      dedupe: ing && ing.dedupe ? ing.dedupe.header || ing.dedupe.body_path : "",
      onDup: ing && ing.dedupe && ing.dedupe.on_duplicate === "conflict" ? "409" : "202",
      keep: s ? span(spanSecs(s.retention.max_age)) : "7 days",
      refuse: s ? undelivered(s) : tenant ? "2 GiB or 30 days" : "50 GiB or 30 days",
      store: s ? s.store || "default" : "default",
      seal: f ? sealed(f) : !!tenant,
      d: freshDelivery(null),
      tried: false, busy: false, err: null,
    };
  }

  async function loadTemplates() {
    if (templates) return;
    try {
      const r = await api("/v1/templates");
      templates = r.templates || r || [];
    } catch { templates = []; }
    paint();
  }

  const dver = (d) => (d.vmode === "pin" || d.vmode === "channel" ? d.vtext.trim() : d.vmode);
  async function loadDeclared(d) {
    const id = d.template;
    if (!id) { d.declared = {}; paint(); return; }
    try {
      const r = await api(`/v1/templates/${enc(id)}?version=${enc(dver(d) || "stable")}`);
      if (d.template !== id) return;
      const ps = { ...(r.params || {}) };
      if (d.sink) {
        try {
          const x = await api(`/v1/templates/${enc(d.sink)}?version=stable`);
          for (const [k, p] of Object.entries(x.params || {})) if (!(k in ps)) ps[k] = p;
        } catch { /* the form still works without them */ }
      }
      d.declared = ps;
      d.resolved = r.version ?? null;
      d.declaredErr = null;
    } catch (e) {
      d.declared = null;
      d.declaredErr = e;
    }
    paint();
  }

  function paramError(p, val) {
    const s = val == null ? "" : String(val);
    if (s === "") return p.required && p.default == null ? "Required: this param has no default." : "";
    if (p.type === "int" && !/^\s*-?\d+\s*$/.test(s)) return "Must be a whole number.";
    if (p.type === "float" && !Number.isFinite(Number(s))) return "Must be a number.";
    if (p.values && !p.values.map(String).includes(s)) return `Must be one of ${p.values.join(", ")}.`;
    return "";
  }

  function deliveryErrors(d) {
    const errs = {};
    if (!d.edit && !NAME.test(d.name.trim())) errs["d-name"] = "Lowercase letters, digits and dashes only.";
    if (!d.template && !(d.edit && d.orig.pipeline)) errs["d-template"] = "Pick a registered template.";
    if (kindOf(tplOf(d.template)) === "source-template" && !d.sink) errs["d-sink"] = "A source template runs paired with a sink.";
    if (d.template && ((d.vmode === "pin" && !/^\d+$/.test(d.vtext.trim())) || (d.vmode === "channel" && !d.vtext.trim()))) errs["d-version"] = d.vmode === "pin" ? "A version number." : "A channel name.";
    else if (d.template && gated(true) && d.vmode !== "pin") errs["d-version"] = "Pin a version: this server approves what each delivery runs.";
    if (!(intText(d.events) > 0)) errs["d-events"] = "A whole number above 0.";
    if (!sizeText(d.size)) errs["d-size"] = "A size such as 64 MiB.";
    if (!spanText(d.age)) errs["d-age"] = "A time such as 60s or 5m.";
    for (const [k, p] of Object.entries(d.declared || {})) {
      const val = d.params[k];
      if (isSealed(val)) continue;
      const e = paramError(p, val);
      if (e) errs[`d-p:${k}`] = e;
    }
    return errs;
  }

  const VIEW_ONLY = ["uid", "status", "paused_by", "pause_reason", "paused_at", "position", "epoch", "lag", "oldest_undelivered_at", "batch_events", "claim", "last_run", "last_error", "consecutive_failures", "delivered_24h"];
  function deliverySpec(d) {
    const from = d.edit ? JSON.parse(JSON.stringify(d.orig)) : {};
    for (const k of VIEW_ONLY) delete from[k];
    const out = { ...from, name: d.name.trim() };
    if (!d.edit) out.start = d.start;
    if (d.template) {
      const tpl = { ...(from.template || {}), id: d.template };
      const ver = dver(d);
      if (ver && ver !== "stable") tpl.version = ver; else delete tpl.version;
      if (kindOf(tplOf(d.template)) === "source-template" && d.sink) tpl.sink = d.sink; else delete tpl.sink;
      const ps = {};
      for (const [k, p] of Object.entries(d.declared || {})) {
        const val = d.params[k];
        if (val === undefined || val === "") continue;
        if (isSealed(val)) { ps[k] = { [SEALED]: true }; continue; }
        ps[k] = p.type === "int" ? Number.parseInt(val, 10) : p.type === "float" ? Number(val) : p.type === "bool" ? val === true || val === "true" : val;
      }
      if (Object.keys(ps).length) tpl.params = ps; else delete tpl.params;
      out.template = tpl;
    }
    out.flush = { ...(from.flush || {}), max_events: intText(d.events), max_bytes: sizeText(d.size), max_age: spanText(d.age) };
    return out;
  }

  function deliveryFields(d, show, owner) {
    const runnable = (templates || []).filter((t) => ["pipeline", "source-template"].includes(kindOf(t)));
    const sinks = (templates || []).filter((t) => kindOf(t) === "sink-template");
    const source = kindOf(tplOf(d.template)) === "source-template";
    let paramsHtml = "";
    if (d.template && d.declaredErr) paramsHtml = html`<div class="wide">${errorAlert("the template’s params", d.declaredErr)}</div>`;
    else if (d.template && d.declared) paramsHtml = Object.entries(d.declared).map(([k, p]) => {
      const val = d.params[k];
      const secret = !!p.secret;
      const kept = isSealed(val);
      const name = `d-p:${k}`;
      let control;
      if (p.values) control = select([...(p.default == null ? [["", "choose…"]] : []), ...p.values.map((x) => [String(x), String(x)])], val == null ? (p.default == null ? "" : String(p.default)) : String(val), { name });
      else if (p.type === "bool") control = select([["", p.default != null ? `default · ${p.default}` : "choose…"], ["true", "true"], ["false", "false"]], val == null ? "" : String(val), { name });
      else control = input(kept ? "" : val == null ? "" : String(val), { name, type: secret ? "password" : "text", ph: kept ? "•••••••• stored · leave blank to keep" : p.default != null ? String(p.default) : "", cls: show(name) ? "err" : "" });
      const help = secret ? "Secret · sealed with the server vault key, never shown again" : p.values ? `One of ${p.values.join(", ")}` : [p.type || "string", p.default != null ? `default ${p.default}` : "", p.description || ""].filter(Boolean).join(" · ");
      return field(k, control, { code: true, req: !!p.required && p.default == null, help, err: show(name), cls: "wide" });
    });
    const tplField = d.edit && d.orig.pipeline && !d.template
      ? field("Pipeline file", input(d.orig.pipeline, { mono: true, cls: "dis", a: { disabled: true } }), { cls: "wide" })
      : field("Template", select([["", "choose a template…"], ...runnable.map((t) => [t.id, `${t.id} · ${kindOf(t) === "source-template" ? "source template" : "pipeline template"}`])], d.template, { name: "d-template" }), { req: true, help: owner ? `Only templates ${owner} may run` : html`Its source must be <code>type: feed</code>`, err: show("d-template"), cls: "wide" });
    const first = d.edit
      ? html`${field("Delivery name", input(d.name, { mono: true, cls: "dis", a: { disabled: true } }), { req: true, help: "Names cannot change" })}${field("Position", input(d.orig.position != null ? `#${fmtInt(d.orig.position)}` : "—", { mono: true, cls: "dis", a: { disabled: true } }), { help: "Use Replay or Skip to move it" })}`
      : html`${field("Delivery name", input(d.name, { name: "d-name", mono: true, ph: "to-warehouse", cls: show("d-name") ? "err" : "" }), { req: true, err: show("d-name") })}${field("Start from", select([["latest", "new events only"], ["earliest", "the oldest stored event"]], d.start, { name: "d-start" }), { help: d.start === "latest" ? "Or the oldest stored event" : "Delivers what is already stored first" })}`;
    return html`<div class="form">${first}${tplField}
      ${source ? field("Sink template", select([["", "choose a sink…"], ...sinks.map((t) => [t.id, t.id])], d.sink, { name: "d-sink" }), { req: true, help: "A source template runs paired with a sink", err: show("d-sink"), cls: "wide" }) : ""}
      ${d.template ? field("Version", html`${seg("d-vmode", ["stable", "newest", "channel", "pin"], d.vmode)}${d.vmode === "channel" || d.vmode === "pin" ? html`<div style="margin-top:6px">${input(d.vtext, { name: "d-vtext", mono: true, ph: d.vmode === "pin" ? "4" : "prod", cls: show("d-version") ? "err" : "" })}</div>` : ""}`, { help: d.vmode === "pin" ? `pin${d.vtext.trim() ? ` v${d.vtext.trim()}` : ""} · never moves` : d.resolved != null ? `${dver(d)} · resolves to v${d.resolved} now` : "resolved at each run", err: show("d-version"), cls: "wide" }) : ""}${paramsHtml}</div>
    <div class="form three">${field("Flush at events", input(d.events, { name: "d-events", cls: show("d-events") ? "err" : "" }), { err: show("d-events") })}${field("or size", input(d.size, { name: "d-size", cls: show("d-size") ? "err" : "" }), { err: show("d-size") })}${field("or oldest waiting", input(d.age, { name: "d-age", cls: show("d-age") ? "err" : "" }), { err: show("d-age") })}</div>`;
  }

  const hasFirstDelivery = () => !!(form && !form.edit && (form.d.name.trim() || form.d.template));
  function rateQuotaError() {
    const qd = data && data.quotas;
    if (!form.tenant || !qd || qd.tenant !== form.tenant || qd.max_feed_events_per_sec == null) return "";
    const mine = intText(form.rate);
    if (!(mine > 0)) return "";
    const others = list().filter((f) => f.tenant === form.tenant && !(form.edit && f.name === form.name)).reduce((n, f) => n + ((f.spec.ingest.rate_limit || {}).events_per_sec || 0), 0);
    return mine + others > qd.max_feed_events_per_sec ? `${form.tenant}’s feeds may take at most ${fmtInt(qd.max_feed_events_per_sec)} events per second in total; ${fmtInt(others)} are in use.` : "";
  }
  function refuseParts() {
    const m = /^(.*?)\s+or\s+(.*)$/i.exec(form.refuse.trim());
    return m ? { size: sizeText(m[1]), age: spanText(m[2]) } : { size: null, age: null };
  }
  const addressList = () => form.addresses.split(/[\n,]/).map((x) => x.trim()).filter(Boolean);
  function formErrors() {
    const errs = {};
    if (!form.edit && !NAME.test(form.name.trim())) errs.name = "Lowercase letters, digits and dashes only.";
    const sig = form.signin !== "token";
    const keeps = form.edit && form.orig.spec.ingest.auth.signature;
    if (sig && !form.sigHeader.trim()) errs.sigHeader = "The header that carries the signature.";
    if (sig && !form.sigSecret && !keeps) errs.sigSecret = "Required: the secret senders sign with.";
    const bad = addressList().find((x) => !/^[0-9a-fA-F:.]+(\/\d{1,3})?$/.test(x));
    if (bad) errs.addresses = `“${bad}” is not an address or a range such as 203.0.113.0/24.`;
    if (!form.accept.size) errs.accept = "Accept at least one kind of content.";
    if (!sizeText(form.maxEvent)) errs.maxEvent = "A size such as 1 MiB.";
    if (form.rate.trim() && !(intText(form.rate) > 0)) errs.rate = "A whole number above 0, or blank for no limit.";
    else if (rateQuotaError()) errs.rate = rateQuotaError();
    if (!spanText(form.keep)) errs.keep = "A time such as 7 days.";
    const rp = refuseParts();
    if (!rp.size || !rp.age) errs.refuse = "A size and an age, such as 50 GiB or 30 days.";
    if (hasFirstDelivery()) Object.assign(errs, deliveryErrors(form.d));
    return errs;
  }

  function specFromForm() {
    const s = form.edit ? JSON.parse(JSON.stringify(form.orig.spec)) : { name: form.name.trim() };
    const ing = s.ingest || {};
    const auth = {};
    if (form.signin !== "signature") auth.tokens = "managed";
    if (form.signin !== "token") auth.signature = { ...((ing.auth || {}).signature || {}), header: form.sigHeader.trim(), secret: form.sigSecret || SEALED };
    if (addressList().length) auth.allowed_addresses = addressList();
    ing.auth = auth;
    ing.accept = [...ACCEPT.map(([t]) => t).filter((t) => form.accept.has(t)), ...[...form.accept].filter((t) => !ACCEPT.some(([x]) => x === t))];
    ing.max_event_bytes = sizeText(form.maxEvent);
    if (form.rate.trim()) ing.rate_limit = { ...(ing.rate_limit || {}), events_per_sec: intText(form.rate) }; else delete ing.rate_limit;
    const key = form.dedupe.trim();
    if (key) {
      const d = { ...(ing.dedupe || {}), on_duplicate: form.onDup === "409" ? "conflict" : "accept" };
      delete d.header;
      delete d.body_path;
      if (key.startsWith("$") || key.startsWith("/")) d.body_path = key; else d.header = key;
      ing.dedupe = d;
    } else delete ing.dedupe;
    s.ingest = ing;
    const rp = refuseParts();
    s.retention = { ...(s.retention || {}), max_age: spanText(form.keep), undelivered: { max_bytes: rp.size, max_age: rp.age } };
    if (form.store && form.store !== "default") s.store = form.store; else delete s.store;
    s.seal = form.seal;
    if (form.tenant) s.tenant = form.tenant;
    if (hasFirstDelivery()) s.deliveries = [deliverySpec(form.d)];
    return s;
  }

  function editDiff() {
    const before = form.orig.spec;
    const after = specFromForm();
    const lines = [];
    const cmp = (label, a, b) => { if (JSON.stringify(a ?? null) !== JSON.stringify(b ?? null)) { lines.push(["-", `${label}: ${a ?? "none"}`]); lines.push(["+", `${label}: ${b ?? "none"}`]); } };
    const size = (x) => (x ? bytes(sizeBytes(x)) : null);
    const time = (x) => (x ? span(spanSecs(x)) : null);
    cmp("sign-in", signInWords(before), signInWords(after));
    if (form.sigSecret) lines.push(["+", "signing secret: replaced"]);
    cmp("signature header", (before.ingest.auth.signature || {}).header, (after.ingest.auth.signature || {}).header);
    cmp("only from addresses", (before.ingest.auth.allowed_addresses || []).join(", ") || null, (after.ingest.auth.allowed_addresses || []).join(", ") || null);
    cmp("accepts", before.ingest.accept.map(acceptWord).join(", "), after.ingest.accept.map(acceptWord).join(", "));
    cmp("largest event", size(before.ingest.max_event_bytes), size(after.ingest.max_event_bytes));
    cmp("events per second", (before.ingest.rate_limit || {}).events_per_sec, (after.ingest.rate_limit || {}).events_per_sec);
    const dk = (x) => (x ? `${x.header || x.body_path} · a repeat is answered ${x.on_duplicate === "conflict" ? 409 : 202}` : null);
    cmp("duplicate key", dk(before.ingest.dedupe), dk(after.ingest.dedupe));
    cmp("keep delivered events", time(before.retention.max_age), time(after.retention.max_age));
    const ref = (x) => (x && x.max_bytes && x.max_age ? `${size(x.max_bytes)} or ${time(x.max_age)}` : null);
    cmp("refuse new events above", ref(before.retention.undelivered), ref(after.retention.undelivered));
    cmp("seal payloads", sealed(form.orig) ? "on" : "off", after.seal ? "on" : "off");
    lines.push([" ", lines.length ? "deliveries — unchanged" : "nothing changes yet"]);
    return lines;
  }

  function formHtml() {
    const errs = formErrors();
    const show = (k) => (form.tried || (k === "rate" && rateQuotaError()) ? errs[k] || "" : "");
    const sig = form.signin !== "token";
    const keeps = form.edit && form.orig.spec.ingest.auth.signature;
    const nameField = form.edit
      ? field("Name", input(form.name, { mono: true, cls: "dis", a: { disabled: true } }), { req: true, help: "Names cannot change" })
      : field("Name", input(form.name, { name: "f-name", mono: true, ph: "support-tickets", cls: show("name") ? "err" : "" }), { req: true, help: "Becomes part of the URL", err: show("name") });
    const who = card({ icon: "key", title: "Who can send", body: html`<div class="form">${nameField}${field("Sign-in", seg("signin", ["token", "signature", "both"], form.signin), { help: "At least one is required", cls: "wide" })}
        ${sig ? html`${field("Signature header", input(form.sigHeader, { name: "f-sigheader", mono: true, cls: show("sigHeader") ? "err" : "" }), { err: show("sigHeader") })}${field("Signing secret", input(form.sigSecret, { name: "f-sigsecret", type: "password", ph: keeps ? "•••••••• stored · leave blank to keep" : "", cls: show("sigSecret") ? "err" : "" }), { help: "Sealed with the server vault key, never shown again", err: show("sigSecret") })}` : ""}
        ${field("Only from addresses", html`<textarea class="area sans fd-area${show("addresses") ? " err" : ""}" name="f-addresses" rows="2" placeholder="any address">${form.addresses}</textarea>`, { help: "Optional · one range per line", err: show("addresses"), cls: "wide" })}</div>`,
      note: form.edit ? "Tokens are managed on the Tokens tab." : form.signin === "signature" ? "No token is made: senders sign each request with the secret." : "A first token is made when you save and shown once." });
    const accepts = card({ icon: "filter", title: "What it accepts", body: html`<div class="form">${field("Content", html`<div class="fd-checks">${ACCEPT.map(([t, l]) => check(l, form.accept.has(t), { name: `a:${t}` }))}</div>`, { cls: "wide", err: show("accept") })}
        ${field("Largest event", input(form.maxEvent, { name: "f-maxevent", cls: show("maxEvent") ? "err" : "" }), { err: show("maxEvent") })}${field("Events per second", input(form.rate, { name: "f-rate", ph: "no limit", cls: show("rate") ? "err" : "" }), { help: "Above it senders get 429", err: show("rate") })}
        ${field("Duplicate key", input(form.dedupe, { name: "f-dedupe", mono: true, ph: "X-Event-Id" }), { help: "A header or a JSON path" })}${field("A repeat is answered", seg("ondup", ["202", "409"], form.onDup), { help: "Within 24 hours" })}</div>` });
    const stores = (data.stores || []).includes(form.store) ? data.stores : [form.store, ...(data.stores || [])];
    const keep = card({ icon: "db", title: "Keep and protect", body: html`<div class="form">${field("Keep delivered events", input(form.keep, { name: "f-keep", cls: show("keep") ? "err" : "" }), { help: "For replay", err: show("keep") })}${field("Refuse new events above", input(form.refuse, { name: "f-refuse", cls: show("refuse") ? "err" : "" }), { help: "Of events not delivered yet", err: show("refuse") })}
        ${field("Store", select(stores.map((x) => [x, x]), form.store, { name: "f-store", a: { disabled: form.edit } }), { help: form.edit ? "A feed stays in its store" : "Defined by the server" })}${field("Seal payloads", toggle(form.seal ? (form.tenant ? "On for tenant feeds" : "On") : "Off", form.seal, { name: "f-seal" }), { help: "Encrypted with the vault key" })}</div>` });
    const deliv = form.edit ? "" : card({ icon: "runs", title: "First delivery", aside: "more can be added later", body: templates ? deliveryFields(form.d, (k) => (form.tried ? errs[k] || "" : ""), form.tenant) : html`<div class="pad">${skelRows(2)}</div>`, note: "Leave it blank to only store events for now." });
    const editNote = form.edit ? card({ icon: "diff", title: "What changes", body: diff(editDiff()), note: "Takes effect within 5 seconds on every replica. Stored events are not touched." }) : "";
    const qd = data.quotas;
    let limitAlert = "";
    if (form.tenant && !form.edit && qd && qd.tenant === form.tenant) {
      const shares = [qd.max_feed_events_per_sec != null ? `${fmtInt(qd.max_feed_events_per_sec)} events per second` : "", qd.max_feed_bytes != null ? `${bytes(qd.max_feed_bytes)} of stored events` : ""].filter(Boolean).join(" and ");
      limitAlert = alert(qd.max_feeds != null ? `${form.tenant} can have ${plural(qd.max_feeds, "feed")}; ${fmtInt(qd.feeds)} ${qd.feeds === 1 ? "is" : "are"} in use.` : `This feed will belong to ${form.tenant}.`, shares ? `Its feeds share ${shares}.` : "", "");
    }
    const nErr = Object.keys(errs).length;
    const g = gated(hasFirstDelivery() || (form.edit && form.orig.deliveries.length > 0));
    const label = form.busy ? "Saving…" : g ? "Request approval" : form.edit ? "Save changes" : "Create feed";
    const blocked = (form.tried && nErr > 0) || !!rateQuotaError();
    const foot = html`<div class="actions" style="border:0;padding:4px 0 0">${g ? html`<span class="muted">${data.approval === "always" ? "This server requires approval for feed changes; this becomes a change request." : "This server requires approval for runs; a feed with a delivery becomes a change request."}</span>` : ""}${btn("Cancel", { kind: "quiet", data: { act: "cancel" } })}${btn(label, { kind: "primary", icon: g ? "changes" : "check", perm: "feed_write", disabled: form.busy || blocked, data: { act: "submit" } })}</div>`;
    return html`${dhead("", "", form.edit ? backToFeed(form.orig) : back())}${head(form.edit ? `Edit ${form.orig.name}` : "New feed", [], form.edit ? "" : "A feed gives senders a URL to push events to. Every event is stored before it is answered, and each delivery runs a template over the new events.")}
      ${limitAlert}${form.tried && nErr ? alert(`${plural(nErr, "field needs", "fields need")} attention.`, "Nothing is saved until they are fixed.", "bad") : ""}${form.err ? alert("The server refused the feed.", form.err.message, "bad") : ""}
      <div class="pair"><div class="stack">${who}${accepts}</div><div class="stack">${keep}${deliv}${editNote}</div></div>${foot}`;
  }

  function deliveryFormHtml() {
    const f = current();
    if (!f || !templates) return html`<span class="skel t"></span>${skelCards(2)}`;
    const errs = deliveryErrors(dform);
    const show = (k) => (dform.tried ? errs[k] || "" : "");
    const nErr = Object.keys(errs).length;
    const g = gated(true);
    const kept = f.oldest != null && f.watermark != null && f.watermark >= f.oldest ? f.watermark - f.oldest + 1 : 0;
    const label = dform.busy ? "Saving…" : g ? "Request approval" : dform.edit ? "Save changes" : "Add delivery";
    return html`${dhead("", "", backToFeed(f))}${head(dform.edit ? `Edit ${dform.orig.name}` : "Add a delivery", [], dform.edit ? "" : "Each delivery reads the same events at its own pace and runs its own template.")}
      ${dform.tried && nErr ? alert(`${plural(nErr, "field needs", "fields need")} attention.`, "Nothing is saved until they are fixed.", "bad") : ""}${dform.err ? alert("The server refused the delivery.", dform.err.message, "bad") : ""}
      <div class="pair"><div class="stack">${card({ icon: "runs", title: "Delivery", body: deliveryFields(dform, show, f.tenant) })}</div>
      <div class="stack">${card({ icon: "info", title: "Good to know", body: html`<div class="pad stack" style="gap:8px">${dform.edit ? html`<span>Changes apply from the next run. A run that already started finishes as it was.</span>` : html`<span>Starting from the oldest stored event delivers <b>${plural(kept, "event")}</b> from the last ${span(spanSecs(f.spec.retention.max_age))} first.</span>`}<span>A delivery that falls behind keeps its events from being removed, up to the feed’s limit.</span></div>` })}</div></div>
      <div class="actions" style="border:0;padding:4px 0 0">${g ? html`<span class="muted">This server requires approval; this becomes a change request.</span>` : ""}${btn("Cancel", { kind: "quiet", data: { act: "cancel" } })}${btn(label, { kind: "primary", icon: g ? "changes" : "check", perm: "feed_write", disabled: dform.busy || (dform.tried && nErr > 0), data: { act: "delivery-submit" } })}</div>`;
  }

  function requestedHtml() {
    const c = requested;
    const ch = c.change || {};
    const p = ch.payload || {};
    const s = p.spec || null;
    const lines = [];
    if (s) {
      const ing = s.ingest || {};
      lines.push(["+", `feed: ${s.name} · ${signInWords(s)}`]);
      lines.push(["+", `accepts: ${(ing.accept || []).map(acceptWord).join(", ")} · ${bytes(sizeBytes(ing.max_event_bytes))}${ing.rate_limit ? ` · ${fmtInt(ing.rate_limit.events_per_sec)} events/s` : ""}`]);
      if (s.retention) lines.push(["+", `retention: ${span(spanSecs(s.retention.max_age))} · refuse above ${undelivered(s)}`]);
      for (const d of s.deliveries || []) {
        lines.push(["+", `delivery ${d.name}: ${d.template ? `${d.template.id} @ ${verLabel(d.template.version || "stable")}` : d.pipeline}`]);
        if (d.flush) lines.push(["+", `flush: ${fmtInt(d.flush.max_events)} events or ${bytes(sizeBytes(d.flush.max_bytes))} or ${span(spanSecs(d.flush.max_age))}`]);
      }
    } else lines.push(["-", `feed: ${p.name || ""}`]);
    const sid = shortId(c.change_id);
    const effect = { create: "The feed and its URL exist once an approver accepts it on the Changes page. Nothing is received or run until then.", update: "The change applies once an approver accepts it on the Changes page. The feed keeps working as it is until then.", delete: "The feed is deleted once an approver accepts it on the Changes page. It keeps receiving until then." }[p.action] || "Nothing changes until an approver accepts it on the Changes page.";
    const by = ch.requester || me().principal;
    return html`${dhead("", btn("Open in Changes", { icon: "changes", kind: "primary", href: href.change(c.change_id) }), back())}${head(p.name || "Feed", [pill("Pending approval", "warn"), by ? html`<span>requested by <b>${by}</b> · just now</span>` : "", html`<span>change ${lnk(sid, href.change(c.change_id), true)}</span>`])}
      ${alert(`Change request ${sid} created.`, effect, "warn", btn("Open in Changes", { icon: "changes", href: href.change(c.change_id) }))}
      ${card({ icon: "diff", title: "What was requested", body: diff(lines) })}`;
  }

  function detailHtml() {
    if (loadErr) return alert("Feeds cannot be listed right now.", "Their settings are kept in the run-history database, which this server cannot reach. Senders are still answered for up to a minute, and stored events are safe.", "bad", btn("Retry", { icon: "refresh", data: { reload: "1" } }));
    if (!data) return html`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`;
    if (requested) return requestedHtml();
    if (form) return formHtml();
    if (dform) return deliveryFormHtml();
    if (!state.name || off) return overviewDetail();
    return feedDetail();
  }

  function paint() {
    if (!alive) return;
    L.select(!!state.name || !!form || !!requested);
    patch(L.list, listHtml());
    patch(L.detail, detailHtml());
    const f = current();
    const feedCrumb = f ? { label: f.name, href: feedHref(f) } : state.name;
    const d = f && state.dtab === "Deliveries" && !form && !dform ? shownDelivery(f) : null;
    let crumbs;
    if (requested) crumbs = [(requested.change && requested.change.payload && requested.change.payload.name) || "Change request"];
    else if (form) crumbs = [form.edit ? form.orig.name : "New feed"];
    else if (dform && f) crumbs = [feedCrumb, dform.edit ? dform.orig.name : "New delivery"];
    else if (d) crumbs = [feedCrumb, d.name];
    else crumbs = state.name ? [state.name] : [];
    setCrumbs(crumbs.length ? ["Operate", { label: "Feeds", href: href.feeds() }, ...crumbs] : ["Operate", "Feeds"]);
  }

  // ── Actions ───────────────────────────────────────────────────────────────
  function openForm(f) {
    requested = null; dform = null; created = null;
    form = freshForm(f);
    loadTemplates();
    paint();
  }
  function openDeliveryForm(d) {
    requested = null; form = null;
    dform = freshDelivery(d);
    loadTemplates();
    if (dform.template) loadDeclared(dform);
    paint();
  }
  const closeForms = () => { form = null; dform = null; requested = null; };
  const askedApproval = (r, what) => {
    requested = r; form = null; dform = null;
    toast(`Approval requested: the ${what} waits for an approver`, "info", { href: href.change(r.change_id), label: "open" });
    refreshBadges(true);
    paint();
  };

  async function submit() {
    form.tried = true;
    const errs = formErrors();
    if (Object.keys(errs).length) { paint(); toast(`${plural(Object.keys(errs).length, "field needs", "fields need")} attention`, "bad"); return; }
    const spec = specFromForm();
    const tenant = form.tenant;
    const wasEdit = form.edit;
    form.busy = true;
    form.err = null;
    paint();
    try {
      const r = await api(wasEdit ? `/v1/feeds/${enc(form.orig.name)}${q(tenant)}` : `/v1/feeds${q(tenant)}`, { method: wasEdit ? "PUT" : "POST", body: spec });
      if (r && r.status === "pending_approval") { askedApproval(r, "change"); return; }
      const target = { name: spec.name, tenant };
      if (!wasEdit) {
        created = { key: fkey(target), token: "", tokenName: "" };
        if (spec.ingest.auth.tokens) {
          try {
            const t = await api(`/v1/feeds/${enc(spec.name)}/tokens${q(tenant)}`, { method: "POST", body: { name: "first" } });
            created.token = t.token;
            created.tokenName = t.name || "first";
          } catch (e) { toast(`The feed was created, but its first token was not: ${e.message}`, "warn"); }
        }
      }
      toast(html`${wasEdit ? "Saved" : "Created"} <b>${spec.name}</b>`, "ok");
      form = null;
      detail = null;
      await load();
      refreshBadges(true);
      const to = feedHref(target);
      if (location.hash === to) loadDetail(); else navigate(to);
    } catch (e) {
      form.err = e;
      form.busy = false;
      toast(e.message, "bad");
      paint();
    }
  }

  async function submitDelivery() {
    const f = current();
    dform.tried = true;
    if (Object.keys(deliveryErrors(dform)).length) { paint(); return; }
    const spec = deliverySpec(dform);
    const wasEdit = dform.edit;
    dform.busy = true;
    dform.err = null;
    paint();
    try {
      const r = await api(wasEdit ? `${base(f)}/deliveries/${enc(spec.name)}${q(f.tenant)}` : `${base(f)}/deliveries${q(f.tenant)}`, { method: wasEdit ? "PUT" : "POST", body: spec });
      if (r && r.status === "pending_approval") { askedApproval(r, "change"); return; }
      toast(html`${wasEdit ? "Saved" : "Added"} <b>${spec.name}</b>`, "ok");
      dform = null;
      await load();
      const to = deliveryHref(f, spec.name);
      if (location.hash === to) loadDetail(); else navigate(to);
    } catch (e) {
      dform.err = e;
      dform.busy = false;
      toast(e.message, "bad");
      paint();
    }
  }

  const after = async () => { await load(); await loadDetail(); };
  const dpath = (f, d) => `${base(f)}/deliveries/${enc(d.name)}`;
  const setLabel = (b, text) => { b.lastChild.textContent = text; };
  const setHtml = (el, markup) => { el.innerHTML = String(markup); };

  async function deliverNow(f, d) {
    const lag = d.lag || 0;
    const take = Math.min(lag, d.batch_events || lag);
    const r = await dialog(`Deliver ${d.name} now?`, html`<span>Starts one run over the <b>${plural(take, "event")}</b> that ${take === 1 ? "is" : "are"} waiting${lag > take ? ` (of ${fmtInt(lag)})` : ""}, without waiting for the flush policy.</span>
      ${d.claim ? alert("A run of this delivery is in progress.", "The next run starts when it ends.", "warn") : !lag ? alert("Nothing is waiting.", "No run starts until an event arrives.", "") : alert("No run of this delivery is in progress.", "", "ok")}`,
      html`${btn("Cancel", { data: { act: "no" } })}${btn("Deliver now", { kind: "primary", icon: "play", data: { act: "yes" } })}`, { icon: "play" });
    if (!r || r.act !== "yes") return;
    try {
      await api(`${dpath(f, d)}/deliver-now${q(f.tenant)}`, { method: "POST" });
      toast(html`<b>${d.name}</b> delivers at its next step`, "ok");
      after();
    } catch (e) { toast(e.message, "bad"); }
  }

  async function pause(f, d, on) {
    let reason = "";
    if (on) {
      const r = await dialog(`Pause ${d.name}?`, html`<span>No runs start while it is paused. Events keep arriving and are stored, and they count toward the feed’s limit of ${undelivered(f.spec)}.</span>
        ${field("Reason", html`<textarea class="area sans fd-area" name="reason" rows="2" placeholder="Warehouse maintenance tonight"></textarea>`, { help: "Optional · shown on the delivery and in the audit log" })}`,
        html`${btn("Cancel", { data: { act: "no" } })}${btn("Pause", { kind: "primary", icon: "pause", data: { act: "yes" } })}`, { icon: "pause" });
      if (!r || r.act !== "yes") return;
      reason = (r.values.reason || "").trim();
    }
    try {
      await api(`${dpath(f, d)}/${on ? "pause" : "resume"}${q(f.tenant)}`, { method: "POST", body: on && reason ? { reason } : undefined });
      toast(html`${on ? "Paused" : "Resumed"} <b>${d.name}</b>`, "ok");
      after();
    } catch (e) { toast(e.message, "bad"); }
  }

  async function replay(f, d) {
    const oldest = f.oldest ?? 0;
    const end = (f.watermark || 0) + 1;
    const at = Math.min(d.position ?? end, end);
    await dialog(`Replay ${d.name}`, html`<span>Moves the delivery back so it delivers stored events again. Events from before the replay window are gone and cannot be replayed.</span>
      ${field("Replay from", html`<div class="fd-pairrow">${select([["seq", "event number"], ["time", "time (UTC)"]], "seq", { name: "by" })}${input(String(oldest), { name: "from", mono: true })}</div>`, { help: html`Earliest possible: event ${seqNo(oldest)}` })}
      <div data-count></div><div class="help bad" data-err hidden></div>`,
      html`${btn("Cancel", { data: { act: "no" } })}${btn("Replay", { kind: "primary", icon: "undo", data: { act: "yes", keep: "1" } })}`, { icon: "undo", onOpen: (el) => {
        const by = el.querySelector("[name=by]");
        const from = el.querySelector("[name=from]");
        const count = el.querySelector("[data-count]");
        const go = el.querySelector('[data-act="yes"]');
        const err = el.querySelector("[data-err]");
        let target = null;
        let timer = null;
        const say = (n) => {
          const k = Math.max(1, Math.ceil(n / (d.max_batch.events || n || 1)));
          setHtml(count, n > 0 ? alert(`${plural(n, "event")} will be delivered again, in ${plural(k, "run")}.`, "Rows already written by earlier runs are written again unless the template upserts by key.", "warn") : alert("Nothing to replay from there.", "The delivery has not delivered anything after that event.", ""));
          go.disabled = !(n > 0);
          go.classList.toggle("dis", !(n > 0));
          setLabel(go, n > 0 ? `Replay ${plural(n, "event")}` : "Replay");
        };
        const work = async () => {
          err.hidden = true;
          target = null;
          const val = from.value.trim();
          if (by.value === "seq") {
            const n = intText(val.replace(/^#/, ""));
            if (!(n >= 0)) { say(0); return; }
            const s = Math.max(n, oldest);
            target = { seq: s };
            say(Math.max(0, at - s));
            return;
          }
          const t = Date.parse(/[zZ]$|[+-]\d\d:?\d\d$/.test(val) ? val : `${val.replace(" ", "T")}Z`);
          if (!Number.isFinite(t)) { say(0); return; }
          const iso = new Date(t).toISOString();
          try {
            const page = await api(`${base(f)}/events${q(f.tenant, { at: iso, limit: "1" })}`);
            const first = page.events.length ? page.events[page.events.length - 1].seq : end;
            target = { time: iso };
            say(Math.max(0, at - first));
          } catch (e) { err.hidden = false; err.textContent = e.message; say(0); }
        };
        by.addEventListener("change", () => { from.value = by.value === "seq" ? String(oldest) : new Date(Date.now() - 3600e3).toISOString().slice(0, 16).replace("T", " "); work(); });
        from.addEventListener("input", () => { clearTimeout(timer); timer = setTimeout(work, 250); });
        work();
        go.addEventListener("click", async () => {
          if (!target) return;
          try {
            const res = await api(`${dpath(f, d)}/replay${q(f.tenant)}`, { method: "POST", body: target });
            el.querySelector('[data-act="no"]').click();
            toast(html`<b>${d.name}</b> replays from #${fmtInt(res.to)}`, "ok");
            after();
          } catch (e) { err.hidden = false; err.textContent = e.message; }
        });
      } });
  }

  async function skip(f, d) {
    const at = d.position ?? 0;
    const max = f.watermark ?? at;
    const stalled = d.status === "stalled";
    await dialog(`Skip event #${fmtInt(at)} for ${d.name}?`, html`<span>Moves this delivery past ${stalled ? "an event it cannot deliver" : "events it has not delivered"}. The event stays stored until it ages out, and the gap is recorded on the feed for good.</span>
      ${field("Skip up to and including", input(`#${fmtInt(at)}`, { name: "to", mono: true }), { help: stalled ? "The one event every run fails on" : "The next event this delivery would read" })}
      ${field("Reason", html`<textarea class="area sans fd-area" name="reason" rows="2"></textarea>`, { req: true, help: "Shown on the feed and in the audit log" })}
      <div data-count>${alert("1 event will never reach this delivery’s destination.", "Other deliveries of this feed are not affected.", "warn")}</div><div class="help bad" data-err hidden></div>`,
      html`${btn("Cancel", { data: { act: "no" } })}${btn("Skip 1 event", { kind: "danger", icon: "right", data: { act: "yes", keep: "1" } })}`, { icon: "alert", onOpen: (el) => {
        const to = el.querySelector("[name=to]");
        const go = el.querySelector('[data-act="yes"]');
        const err = el.querySelector("[data-err]");
        const count = el.querySelector("[data-count]");
        const n = () => { const x = intText(to.value.trim().replace(/^#/, "")); return x >= at && x <= max ? x - at + 1 : 0; };
        if (at > max) { go.disabled = true; go.classList.add("dis"); setHtml(count, alert("Nothing is waiting to be skipped.", "This delivery has delivered every stored event.", "")); }
        to.addEventListener("input", () => {
          const k = n();
          setLabel(go, k ? `Skip ${plural(k, "event")}` : "Skip");
          go.disabled = !k;
          go.classList.toggle("dis", !k);
          setHtml(count, k ? alert(`${plural(k, "event")} will never reach this delivery’s destination.`, "Other deliveries of this feed are not affected.", "warn") : alert(`Choose an event from #${fmtInt(at)} to #${fmtInt(max)}.`, "", "bad"));
        });
        go.addEventListener("click", async () => {
          const reason = el.querySelector("[name=reason]").value.trim();
          const k = n();
          if (!reason) { err.hidden = false; err.textContent = "A reason is required."; return; }
          try {
            await api(`${dpath(f, d)}/skip${q(f.tenant)}`, { method: "POST", body: { to: at + k, reason } });
            el.querySelector('[data-act="no"]').click();
            toast(html`<b>${d.name}</b> skipped ${plural(k, "event")}`, "ok");
            after();
          } catch (e) { err.hidden = false; err.textContent = e.message; }
        });
      } });
  }

  async function recover(f) {
    const held = f.deliveries.filter((d) => d.status === "held");
    const r = await dialog(`Recover ${f.name}?`, html`<span>Continues ${held.length > 1 ? `its ${held.length} held deliveries` : held[0] ? held[0].name : "the feed"} from the store’s newest event${f.watermark != null ? html` (${seqNo(f.watermark)})` : ""}. What was lost with the restore is recorded as a gap in History, and receiving starts again.</span>${alert("Events the store no longer holds cannot be delivered.", "", "warn")}`,
      html`${btn("Cancel", { data: { act: "no" } })}${btn("Recover", { kind: "primary", icon: "refresh", data: { act: "yes" } })}`, { icon: "refresh" });
    if (!r || r.act !== "yes") return;
    try {
      for (const d of held) await api(`${dpath(f, d)}/recover${q(f.tenant)}`, { method: "POST" });
      toast(html`Recovered <b>${f.name}</b>`, "ok");
      after();
    } catch (e) { toast(e.message, "bad"); }
  }

  function showRefusal(err, e) {
    const rs = (e.details && e.details.runs) || [];
    err.hidden = false;
    if (e.status !== 409 || !rs.length) { err.textContent = e.message; return; }
    setHtml(err, html`A run is in flight. Pause the delivery and wait for the run to end, or cancel it, then retry: ${join(rs.map((x) => (x.run_id ? html`${x.delivery} · <a class="lnk" href="${href.run(x.run_id)}">open the run</a>` : html`${x.delivery} · its run is starting`)), "; ")}`);
  }

  async function removeFeed(f) {
    const und = lagOf(f);
    await dialog(`Delete ${f.name}?`, html`<span>Its URL stops accepting events at once, and its <b>${bytes(f.retained_bytes || 0)}</b> of stored events are removed${und ? html`, including <b>${fmtInt(und)} that ${und === 1 ? "is" : "are"} not delivered yet</b>` : ""}. Runs it already started stay in history.</span>${field(html`Type <code>${f.name}</code> to confirm`, input("", { name: "confirm", mono: true }))}<div class="help bad" data-err hidden></div>`,
      html`${btn("Cancel", { data: { act: "no" } })}${btn("Delete feed and its events", { kind: "danger", icon: "trash", data: { act: "yes", keep: "1" } })}`, { icon: "trash", onOpen: (el) => {
        el.addEventListener("click", (ev) => { if (ev.target.closest("[data-err] a")) el.querySelector('[data-act="no"]').click(); });
        el.querySelector('[data-act="yes"]').addEventListener("click", async () => {
          const err = el.querySelector("[data-err]");
          if (el.querySelector("[name=confirm]").value.trim() !== f.name) { err.hidden = false; err.textContent = "The name does not match."; return; }
          try {
            const res = await api(`${base(f)}${q(f.tenant)}`, { method: "DELETE" });
            el.querySelector('[data-act="no"]').click();
            if (res && res.status === "pending_approval") { askedApproval(res, "delete"); return; }
            toast(html`Deleted <b>${f.name}</b>`, "ok");
            detail = null;
            await load();
            refreshBadges(true);
            navigate(hashFor(href.feeds(), listQuery()));
          } catch (e) { showRefusal(err, e); }
        });
      } });
  }

  async function removeDelivery(f, d) {
    await dialog(`Delete ${d.name}?`, html`<span>It stops running and its position is forgotten. The feed keeps its events for its other deliveries${d.lag ? html`; <b>${plural(d.lag, "event")}</b> it has not delivered will not reach its destination` : ""}.</span><div class="help bad" data-err hidden></div>`,
      html`${btn("Cancel", { data: { act: "no" } })}${btn("Delete delivery", { kind: "danger", icon: "trash", data: { act: "yes", keep: "1" } })}`, { icon: "trash", onOpen: (el) => {
        el.addEventListener("click", (ev) => { if (ev.target.closest("[data-err] a")) el.querySelector('[data-act="no"]').click(); });
        el.querySelector('[data-act="yes"]').addEventListener("click", async () => {
          const err = el.querySelector("[data-err]");
          try {
            const res = await api(`${dpath(f, d)}${q(f.tenant)}`, { method: "DELETE" });
            el.querySelector('[data-act="no"]').click();
            if (res && res.status === "pending_approval") { askedApproval(res, "delete"); return; }
            toast(html`Deleted <b>${d.name}</b>`, "ok");
            await load();
            navigate(feedHref(f));
          } catch (e) { showRefusal(err, e); }
        });
      } });
  }

  async function newToken(f) {
    await dialog(`New token for ${f.name}`, html`<div data-step="form" class="stack" style="gap:12px">${field("Name", input("", { name: "name", mono: true, ph: "sender-prod" }), { req: true })}${field("Expires", select([["", "never"], ["30", "in 30 days"], ["90", "in 90 days"], ["365", "in a year"]], "", { name: "expires" }), { help: "Or never" })}<div class="help bad" data-err hidden></div></div>
      <div data-step="made" class="stack" style="gap:12px" hidden>${alert("Copy it now. It is not shown again.", "", "warn")}<div class="input mono fd-url"><span class="fd-url-t" data-token></span><button type="button" class="cpi" data-copy="" title="Copy" aria-label="Copy">${ic("copy")}</button></div></div>`,
      html`${btn("Cancel", { data: { act: "no" } })}${btn("Make token", { kind: "primary", icon: "key", data: { act: "yes", keep: "1" } })}`, { icon: "key", onOpen: (el) => {
        const go = el.querySelector('[data-act="yes"]');
        const cancel = el.querySelector('[data-act="no"]');
        const err = el.querySelector("[data-err]");
        let made = false;
        go.addEventListener("click", async () => {
          if (made) { cancel.click(); return; }
          const name = el.querySelector("[name=name]").value.trim();
          if (!NAME.test(name)) { err.hidden = false; err.textContent = "Lowercase letters, digits and dashes only."; return; }
          const days = Number(el.querySelector("[name=expires]").value);
          try {
            const t = await api(`${base(f)}/tokens${q(f.tenant)}`, { method: "POST", body: days ? { name, expires_at: new Date(Date.now() + days * 864e5).toISOString() } : { name } });
            made = true;
            el.querySelector('[data-step="form"]').hidden = true;
            el.querySelector('[data-step="made"]').hidden = false;
            el.querySelector("[data-token]").textContent = t.token;
            el.querySelector("[data-copy]").dataset.copy = t.token;
            cancel.hidden = true;
            setLabel(go, "Done");
            loadDetail();
          } catch (e) { err.hidden = false; err.textContent = e.message; }
        });
      } });
  }

  async function revokeToken(f, id) {
    const t = (f.tokens || []).find((x) => x.id === id);
    if (!t) return;
    const r = await dialog(`Revoke ${t.name}?`, html`<span>Requests that use this token are refused with <b>401</b> within 5 seconds. Events it already sent stay stored. This cannot be undone.</span>`,
      html`${btn("Cancel", { data: { act: "no" } })}${btn("Revoke token", { kind: "danger", icon: "trash", data: { act: "yes" } })}`, { icon: "key" });
    if (!r || r.act !== "yes") return;
    try {
      await api(`${base(f)}/tokens/${enc(id)}${q(f.tenant)}`, { method: "DELETE" });
      toast(html`Revoked <b>${t.name}</b>`, "ok");
      loadDetail();
    } catch (e) { toast(e.message, "bad"); }
  }

  async function sendTest(f) {
    const a = f.spec.ingest.auth;
    const here = ingestHere();
    const url = here ? ingestUrl(f) : `https://FEEDS-LISTEN-HOST${f.ingest_path}`;
    const sample = '{"id": 0, "status": "test", "note": "sent from the console"}';
    const ct = f.spec.ingest.accept.includes("application/json") ? "application/json" : f.spec.ingest.accept[0];
    const fromBrowser = here && !a.signature;
    const curlOf = (body) => [`curl -X POST ${url} \\`, a.tokens ? '  -H "Authorization: Bearer $FEED_TOKEN" \\' : "", `  -H "Content-Type: ${ct}" \\`, `  -d '${body.replace(/'/g, "'\\''")}'`].filter(Boolean).join("\n");
    const cli = `faucet feed send --url ${url}${a.tokens ? " --token-env FEED_TOKEN" : ""}${a.signature ? " --signing-secret-env FEED_SECRET" : ""} -f event.json`;
    const why = !here ? html`This server takes events on its <code>--feeds-listen</code> address, which this page cannot reach.` : html`This feed checks a signature made with its sealed secret. The console never holds that secret, so a browser cannot sign a request.`;
    const body = fromBrowser
      ? html`<span>Stores one event exactly as a sender would, with a feed token you paste. Its deliveries will run it like any other event.</span>
        ${field("Body", html`<textarea class="area mono fd-area" name="body" rows="3">${sample}</textarea>`, { help: html`<span data-size></span>` })}
        ${field("Feed token", input("", { name: "token", type: "password", ph: "fdt_…" }), { req: true, help: "Used for this request only and never kept. Make one on the Tokens tab." })}
        <div data-result></div>`
      : html`<span>${why} Send the test from a terminal instead:</span>${code(cli, { lang: "text" })}<span class="muted">Or with curl${a.signature ? html`, adding the <code>${a.signature.header}</code> header your sender computes` : ""}:</span>${code(curlOf(sample), { lang: "text" })}`;
    const actions = fromBrowser
      ? html`${btn("Copy as curl", { icon: "copy", data: { act: "curl", keep: "1" } })}${btn("Close", { data: { act: "no" } })}${btn("Send", { kind: "primary", icon: "play", data: { act: "yes", keep: "1" } })}`
      : btn("Close", { kind: "primary", data: { act: "no" } });
    await dialog(`Send a test event to ${f.name}`, body, actions, { icon: "play", wide: !fromBrowser, onOpen: (el) => {
      if (!fromBrowser) return;
      const ta = el.querySelector("[name=body]");
      const tok = el.querySelector("[name=token]");
      const size = el.querySelector("[data-size]");
      const result = el.querySelector("[data-result]");
      const go = el.querySelector('[data-act="yes"]');
      const curl = el.querySelector('[data-act="curl"]');
      const sz = () => { size.textContent = `${acceptWord(ct)} · ${bytes(new TextEncoder().encode(ta.value).length)}`; };
      ta.addEventListener("input", sz);
      sz();
      curl.dataset.copy = curlOf(ta.value);
      ta.addEventListener("input", () => { curl.dataset.copy = curlOf(ta.value); });
      go.addEventListener("click", async () => {
        if (!tok.value.trim()) { setHtml(result, alert("Paste a feed token.", "The feed accepts only requests that carry one.", "bad")); tok.focus(); return; }
        go.disabled = true;
        const t0 = performance.now();
        try {
          const resp = await fetch(f.ingest_path, { method: "POST", headers: { "Content-Type": ct, Authorization: `Bearer ${tok.value.trim()}` }, body: ta.value });
          const ms = Math.round(performance.now() - t0);
          const text = await resp.text();
          let j = null;
          try { j = JSON.parse(text); } catch { /* not JSON */ }
          if (resp.ok) {
            setHtml(result, alert(j && j.first_seq != null ? `Stored as event #${fmtInt(j.first_seq)} · answered ${resp.status} in ${fmtInt(ms)} ms.` : `Answered ${resp.status} in ${fmtInt(ms)} ms: a repeat of an event already stored.`, "", "ok"));
            setLabel(go, "Send again");
            after();
          } else setHtml(result, alert(`Refused with ${resp.status}.`, (j && j.error && j.error.message) || text.slice(0, 200), "bad"));
        } catch (e) { setHtml(result, alert("The request did not reach the server.", e.message, "bad")); }
        go.disabled = false;
      });
    } });
  }

  async function olderEvents(f) {
    const last = events.pages[events.pages.length - 1];
    if (!last || last.next_before == null) return;
    try {
      events.pages.push(await api(`${base(f)}/events${q(f.tenant, { limit: "25", before: String(last.next_before) })}`));
    } catch (e) { toast(e.message, "bad"); }
    paint();
  }

  // ── Events ────────────────────────────────────────────────────────────────
  L.list.addEventListener("click", (e) => {
    const tb = e.target.closest("[data-tab]");
    if (tb) setQuery({ tab: tb.dataset.tab === "All" ? "" : tb.dataset.tab });
    if (e.target.closest("[data-reload]")) load();
    if (e.target.closest('[data-act="new"]') && can("feed_write") && !atLimit()) openForm(null);
  });
  function pickSeg(item) {
    const which = item.closest("[data-seg]").dataset.seg;
    const val = item.dataset.v;
    const d = form ? form.d : dform;
    if (which === "signin" && form) form.signin = val;
    else if (which === "ondup" && form) form.onDup = val;
    else if (which === "d-vmode" && d) { d.vmode = val; d.vtext = ""; loadDeclared(d); }
    paint();
  }
  L.detail.addEventListener("keydown", (e) => {
    const item = (e.key === "Enter" || e.key === " ") && e.target.closest("[data-seg] [data-v]");
    if (item && (form || dform)) { e.preventDefault(); pickSeg(item); }
  });
  L.detail.addEventListener("click", (e) => {
    if (e.target.closest("[data-reload]")) { load().then(loadDetail); return; }
    const a = e.target.closest("[data-act]");
    if (a && !a.disabled) {
      const f = current();
      const d = f ? shownDelivery(f) : null;
      const act = a.dataset.act;
      if (act === "new") { if (can("feed_write")) openForm(null); }
      else if (act === "cancel") { closeForms(); paint(); }
      else if (act === "submit") submit();
      else if (act === "delivery-submit") submitDelivery();
      else if (act === "created-done") { created = null; paint(); }
      else if (!f) return;
      else if (act === "test") sendTest(f);
      else if (act === "edit") openForm(f);
      else if (act === "delete") removeFeed(f);
      else if (act === "recover") recover(f);
      else if (act === "delivery-new") openDeliveryForm(null);
      else if (act === "token-new") newToken(f);
      else if (act === "token-revoke") revokeToken(f, a.dataset.token);
      else if (act === "older") olderEvents(f);
      else if (!d) return;
      else if (act === "deliver-now") deliverNow(f, d);
      else if (act === "pause") pause(f, d, true);
      else if (act === "resume") pause(f, d, false);
      else if (act === "delivery-edit") openDeliveryForm(d);
      else if (act === "delivery-delete") removeDelivery(f, d);
      else if (act === "replay") replay(f, d);
      else if (act === "skip") skip(f, d);
      return;
    }
    const item = e.target.closest("[data-seg] [data-v]");
    if (item && (form || dform)) pickSeg(item);
  });
  L.detail.addEventListener("submit", (e) => {
    const g = e.target.closest("[data-goto]");
    if (!g) return;
    e.preventDefault();
    const f = current();
    const n = intText(g.querySelector("input").value.trim().replace(/^#/, ""));
    if (f && n > 0) navigate(feedHref(f, { dtab: "Events", event: n }));
  });
  L.detail.addEventListener("change", (e) => {
    const t = e.target;
    const n = t.name || "";
    const d = form ? form.d : dform;
    if (form && n.startsWith("a:")) { if (t.checked) form.accept.add(n.slice(2)); else form.accept.delete(n.slice(2)); }
    else if (form && n === "f-store") form.store = t.value;
    else if (form && n === "f-seal") form.seal = t.checked;
    else if (d && n === "d-template") { d.template = t.value; d.params = {}; d.declared = null; d.sink = ""; loadDeclared(d); }
    else if (d && n === "d-sink") { d.sink = t.value; loadDeclared(d); }
    else if (d && n === "d-start") d.start = t.value;
    else if (d && n === "d-vtext") loadDeclared(d);
    else if (d && n.startsWith("d-p:") && t.tagName === "SELECT") d.params[n.slice(4)] = t.value;
    else if (form || dform) { if (t.tagName !== "SELECT") paint(); return; }
    else return;
    paint();
  });
  const F = { "f-name": "name", "f-sigheader": "sigHeader", "f-sigsecret": "sigSecret", "f-addresses": "addresses", "f-maxevent": "maxEvent", "f-rate": "rate", "f-dedupe": "dedupe", "f-keep": "keep", "f-refuse": "refuse" };
  const D = { "d-name": "name", "d-vtext": "vtext", "d-events": "events", "d-size": "size", "d-age": "age" };
  L.detail.addEventListener("input", (e) => {
    const t = e.target;
    const n = t.name || "";
    const d = form ? form.d : dform;
    if (form && F[n]) { form[F[n]] = t.value; if (n === "f-rate" && data.quotas) paint(); }
    else if (d && D[n]) d[D[n]] = t.value;
    else if (d && n.startsWith("d-p:")) {
      const k = n.slice(4);
      // A blank secret field keeps the stored (sealed) value.
      if (t.type === "password" && !t.value && d.orig && d.orig.template && isSealed((d.orig.template.params || {})[k])) d.params[k] = { [SEALED]: true };
      else d.params[k] = t.value;
    }
  });

  paint();
  await load();
  if (state.name) loadDetail();
  if (state.mode === "new" && can("feed_write") && !off && !noStore()) openForm(null);
  const stop = poll(async () => {
    if (form || dform || document.querySelector(".scrim")) return;
    await load();
    if (state.name) await loadDetail();
  }, 15000);
  return {
    teardown() { alive = false; stop(); },
    update(p) {
      const next = read(p);
      const changed = next.name !== state.name || next.ft !== state.ft;
      const tabChanged = next.dtab !== state.dtab || next.delivery !== state.delivery || next.event !== state.event;
      state = next;
      if (changed) { closeForms(); if (created && created.key !== selKey()) created = null; detail = null; detailErr = null; events = null; opened = null; refusals = null; runs = null; }
      else if (tabChanged) { form = null; dform = null; if (next.dtab !== "Overview") created = null; }
      paint();
      if (changed) loadDetail();
      else if (tabChanged) loadTab();
    },
  };
}

// Exported for tests that check the pure helpers.
export const _test = { sizeBytes, spanSecs, bytes, span, sizeText, spanText };

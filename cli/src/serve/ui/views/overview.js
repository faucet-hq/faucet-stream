// Overview (#1006): fleet trends from the server's run history — run outcomes,
// records, governance and cost per bucket, the pipelines failing or running
// slowest, catalog freshness, and what needs someone. `GET /v1/overview`.
import { api } from "../api.js";
import { escapeHtml, fmtInt, fmtCompact, fmtAge } from "../utils.js";
import { icon, statStrip, card, sparkline, tabs } from "../components.js";
import { getTz, formatTs } from "../tz.js";
import { withTenant } from "../tenant.js";
import { fmtBytes } from "./usage.js";

const RANGES = [
  { id: "24h", label: "24h" },
  { id: "7d", label: "7d" },
  { id: "30d", label: "30d" },
];
const RANGE_KEY = "faucet.overview.range";
const esc = escapeHtml;

function readRange(query) {
  const q = query?.get("range");
  if (RANGES.some((r) => r.id === q)) return q;
  try {
    const saved = localStorage.getItem(RANGE_KEY);
    if (RANGES.some((r) => r.id === saved)) return saved;
  } catch {
    /* storage unavailable */
  }
  return "7d";
}

function saveRange(r) {
  try {
    localStorage.setItem(RANGE_KEY, r);
  } catch {
    /* storage unavailable */
  }
}

const pipelineName = (p) => p || "(unnamed)";
const pct = (v) => `${(Number(v) * 100).toFixed(Number(v) >= 0.1 ? 0 : 1)}%`;

function fmtMs(ms) {
  const n = Number(ms) || 0;
  if (n < 1000) return `${fmtInt(n)} ms`;
  const s = n / 1000;
  if (s < 60) return `${s.toFixed(1)} s`;
  const m = s / 60;
  return m < 60 ? `${m.toFixed(1)} min` : `${(m / 60).toFixed(1)} h`;
}

/** A span of milliseconds, compactly: `45m`, `24h`, `3d`. */
function fmtSpan(ms) {
  const m = Math.max(0, Math.round(ms / 60000));
  if (m < 120) return `${m}m`;
  const h = Math.round(m / 60);
  return h < 48 ? `${h}h` : `${Math.round(h / 24)}d`;
}

function fmtMoney(v, currency) {
  const n = Number(v) || 0;
  const digits = n !== 0 && Math.abs(n) < 0.01 ? 4 : 2;
  return `${currency} ${n.toFixed(digits)}`;
}

// Bucket labels in the display time zone (buckets are UTC on the wire).
function bucketLabel(iso, bucket) {
  const d = new Date(iso);
  const tz = getTz();
  const opt = tz === "local" ? {} : { timeZone: tz };
  try {
    return new Intl.DateTimeFormat("en-US", bucket === "hour"
      ? { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false, ...opt }
      : { month: "short", day: "numeric", ...opt }).format(d);
  } catch {
    return iso;
  }
}

/** Stacked bars of succeeded / failed / cancelled runs per bucket. */
function runBars(series, bucket) {
  const w = 600;
  const h = 96;
  const n = series.length || 1;
  const max = Math.max(1, ...series.map((b) => b.succeeded + b.failed + b.cancelled));
  const gap = n > 60 ? 0.5 : 1.5;
  const bw = Math.max(0.6, w / n - gap);
  const rects = series
    .map((b, i) => {
      const x = (i * w) / n;
      let y = h;
      const parts = [
        ["ok", b.succeeded],
        ["bad", b.failed],
        ["muted", b.cancelled],
      ];
      const tip = `${bucketLabel(b.bucket_start, bucket)} · ${fmtInt(b.succeeded)} succeeded · ${fmtInt(b.failed)} failed · ${fmtInt(b.cancelled)} cancelled`;
      const bars = parts
        .filter(([, v]) => v > 0)
        .map(([tone, v]) => {
          const bh = (v / max) * (h - 2);
          y -= bh;
          return `<rect class="ov-bar ${tone}" x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${bw.toFixed(2)}" height="${bh.toFixed(2)}"/>`;
        })
        .join("");
      return `<g><title>${esc(tip)}</title><rect class="ov-hit" x="${x.toFixed(2)}" y="0" width="${(w / n).toFixed(2)}" height="${h}"/>${bars}</g>`;
    })
    .join("");
  return `<svg class="ov-bars" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" role="img" aria-label="Runs per ${bucket}">${rects}</svg>${axis(series, bucket)}`;
}

function axis(series, bucket) {
  if (!series.length) return "";
  const first = series[0].bucket_start;
  const last = series[series.length - 1].bucket_start;
  return `<div class="ov-axis"><span>${esc(bucketLabel(first, bucket))}</span><span>${esc(bucketLabel(last, bucket))}</span></div>`;
}

/** A trend line that fills the card width. */
function trend(values, color) {
  return `<div class="ov-trend">${sparkline(values, { width: 600, height: 44, color, min: 0 }).replace("<svg ", '<svg preserveAspectRatio="none" ')}</div>`;
}

/** A labelled horizontal bar list: `[{ label, value, text, href? }]`. */
function barList(items, tone = "") {
  const max = Math.max(1, ...items.map((i) => Number(i.value) || 0));
  return `<ul class="ov-list">${items
    .map((i) => {
      const width = Math.max(2, ((Number(i.value) || 0) / max) * 100);
      const label = i.href
        ? `<a href="${esc(i.href)}" title="${esc(i.label)}">${esc(i.label)}</a>`
        : `<span title="${esc(i.label)}">${esc(i.label)}</span>`;
      return `<li>${label}<b>${esc(i.text)}</b><i class="ov-meter ${tone}"><i style="width:${width.toFixed(1)}%"></i></i></li>`;
    })
    .join("")}</ul>`;
}

function empty(text) {
  return `<div class="c-card-empty">${text}</div>`;
}

function grafanaLink(data, key) {
  const url = data.grafana && data.grafana[key];
  return url
    ? `<a class="ov-graf" href="${esc(url)}" target="_blank" rel="noopener" title="Open this in Grafana">${icon("link")}Grafana</a>`
    : "";
}

// `card()` escapes its header; the Grafana link goes in after it.
function section(opts, data, key) {
  const html = card(opts);
  const link = grafanaLink(data, key);
  return link ? html.replace("</div>", `${link}</div>`) : html;
}

function attentionCard(a) {
  const groups = [];
  const more = (g) => (g.more ? "+" : "");
  if (a.pending_changes) {
    const g = a.pending_changes;
    groups.push({
      title: "Change requests waiting for approval",
      count: `${fmtInt(g.count)}${more(g)}`,
      href: "#/changes",
      items: g.items.map((c) => ({
        href: `#/changes/${encodeURIComponent(c.id)}`,
        text: `${c.kind.replace(/_/g, " ")} by ${c.requester}`,
        meta: `${fmtAge(c.created_at)} ago · expires in ${fmtSpan(Date.parse(c.expires_at) - Date.now())}`,
      })),
    });
  }
  if (a.unhealthy_triggers) {
    const g = a.unhealthy_triggers;
    groups.push({
      title: "Unhealthy triggers",
      count: fmtInt(g.count),
      items: g.items.map((t) => ({
        text: `${t.name} (${t.kind})`,
        meta: `${fmtInt(t.consecutive_failures)} failed polls${t.last_error ? ` · ${t.last_error}` : ""}`,
      })),
    });
  }
  if (a.failing_templates) {
    const g = a.failing_templates;
    groups.push({
      title: "Templates whose tests fail",
      count: `${fmtInt(g.count)}${more(g)}`,
      href: "#/templates",
      items: g.items.map((t) => ({
        href: `#/templates/${encodeURIComponent(t.id)}`,
        text: `${t.id} v${t.version}`,
        meta: `${fmtInt(t.failed_cases)} failing case${t.failed_cases === 1 ? "" : "s"} · ${fmtAge(t.recorded_at)} ago`,
      })),
    });
  }
  if (a.connections_needing_reauth) {
    const g = a.connections_needing_reauth;
    groups.push({
      title: "Connections that need re-authorization",
      count: `${fmtInt(g.count)}${more(g)}`,
      href: "#/tenants",
      items: g.items.map((c) => ({
        href: `#/tenants/${encodeURIComponent(c.tenant)}`,
        text: `${c.tenant} / ${c.name}`,
        meta: c.reason || "",
      })),
    });
  }
  return groups;
}

function attentionHtml(data) {
  const groups = attentionCard(data.attention || {});
  const failed = (data.runs.by_status || {}).failed || 0;
  if (failed) {
    groups.unshift({
      title: "Failed runs",
      count: fmtInt(failed),
      href: "#/runs?status=failed",
      items: (data.runs.failure_rate_by_pipeline || []).slice(0, 5).map((p) => ({
        href: `#/runs?status=failed&name=${encodeURIComponent(p.pipeline)}`,
        text: pipelineName(p.pipeline),
        meta: `${fmtInt(p.failed)} of ${fmtInt(p.runs)} runs failed`,
      })),
    });
  }
  const live = groups.filter((g) => g.items.length || g.count !== "0");
  if (!live.length) {
    return card({
      title: "Needs attention",
      iconName: "check",
      html: empty("Nothing needs you right now: no failed runs in this window, and nothing waiting for approval."),
    });
  }
  const body = live
    .map((g) => {
      const head = g.href
        ? `<a href="${esc(g.href)}">${esc(g.title)}</a>`
        : `<span>${esc(g.title)}</span>`;
      const rows = g.items.length
        ? `<ul>${g.items
            .map((i) => {
              const t = i.href ? `<a href="${esc(i.href)}">${esc(i.text)}</a>` : `<span>${esc(i.text)}</span>`;
              return `<li>${t}${i.meta ? `<small title="${esc(i.meta)}">${esc(i.meta)}</small>` : ""}</li>`;
            })
            .join("")}</ul>`
        : "";
      return `<div class="ov-att-g"><div class="ov-att-h">${head}<span class="c-pill ${g.count === "0" ? "" : "warn"}">${esc(g.count)}</span></div>${rows}</div>`;
    })
    .join("");
  return `<section class="c-card ov-att"><div class="c-card-h">${icon("triggers")}<b>Needs attention</b></div><div class="ov-att-body">${body}</div></section>`;
}

function runsCard(data) {
  const r = data.runs;
  if (!r.total) {
    return section(
      {
        title: "Runs",
        iconName: "runs",
        html: empty(
          `No runs were submitted in this window. <a href="#/templates">Trigger a template</a><span data-perm="config_submit"> or <a href="#/submit">submit a config</a></span> to see trends here.`,
        ),
      },
      data,
      "runs",
    );
  }
  const statuses = Object.entries(r.by_status)
    .map(([s, n]) => `<span class="ov-legend-i ${esc(s)}">${esc(s)} <b>${fmtInt(n)}</b></span>`)
    .join("");
  return section(
    {
      title: "Runs",
      iconName: "runs",
      aside: `per ${data.bucket} · ${fmtInt(r.queued)} queued · ${fmtInt(r.running)} running now`,
      html: `<div class="c-card-pad">${runBars(r.series, data.bucket)}<div class="ov-legend">${statuses}</div></div>`,
    },
    data,
    "runs",
  );
}

function recordsCard(data) {
  const rec = data.records;
  const html = rec.total
    ? `<div class="c-card-pad">${trend(rec.series.map((b) => b.written), "var(--text-2)")}${axis(rec.series, data.bucket)}${barList(
        rec.top_pipelines.map((p) => ({
          label: pipelineName(p.pipeline),
          value: p.written,
          text: fmtCompact(p.written),
          href: `#/runs?name=${encodeURIComponent(p.pipeline)}`,
        })),
      )}</div>`
    : empty("No records were written in this window.");
  return section(
    { title: "Records written", iconName: "datasets", aside: rec.total ? `${fmtInt(rec.total)} in total` : "", html },
    data,
    "runs",
  );
}

function failuresCard(data) {
  const f = data.runs.failure_rate_by_pipeline;
  const html = f.length
    ? `<div class="c-card-pad">${barList(
        f.map((p) => ({
          label: pipelineName(p.pipeline),
          value: p.rate,
          text: `${pct(p.rate)} · ${fmtInt(p.failed)}/${fmtInt(p.runs)}`,
          href: `#/runs?status=failed&name=${encodeURIComponent(p.pipeline)}`,
        })),
        "bad",
      )}</div>`
    : empty(data.runs.total ? "No pipeline failed in this window." : "No finished runs in this window.");
  return section({ title: "Failure rate by pipeline", iconName: "x", html }, data, "reliability");
}

function slowestCard(data) {
  const s = data.runs.slowest;
  const sample = data.runs.duration_sample;
  const html = s.length
    ? `<div class="c-card-pad">${barList(
        s.map((p) => ({
          label: pipelineName(p.pipeline),
          value: p.p95_ms,
          text: `${fmtMs(p.p95_ms)} · ${fmtInt(p.runs)}`,
          href: `#/runs?name=${encodeURIComponent(p.pipeline)}`,
        })),
        "info",
      )}</div>`
    : empty("No finished invocations in this window.");
  const aside = s.length
    ? `p95 invocation time · ${fmtInt(sample.invocations)}${sample.truncated ? " newest" : ""} invocations`
    : "";
  return section({ title: "Slowest pipelines", iconName: "schedules", aside, html }, data, "reliability");
}

const GOV = [
  ["filtered", "filtered by transforms"],
  ["quarantined", "quarantined"],
  ["dlq", "sent to the DLQ"],
  ["coalesced", "coalesced"],
  ["skipped", "skipped by the sink"],
  ["dropped", "dropped"],
];

function governanceCard(data) {
  const g = data.governance;
  const total = GOV.reduce((n, [k]) => n + (g.totals[k] || 0), 0);
  const html = total
    ? `<div class="c-card-pad">${trend(
        g.series.map((b) => GOV.reduce((n, [k]) => n + (b[k] || 0), 0)),
        "var(--warn)",
      )}${axis(g.series, data.bucket)}<div class="ov-kv">${GOV.map(
        ([k, label]) => `<div><small>${esc(label)}</small><b class="${g.totals[k] ? "" : "zero"}">${fmtInt(g.totals[k] || 0)}</b></div>`,
      ).join("")}</div></div>`
    : empty("Every row offered was written: nothing was filtered, quarantined or sent to a DLQ in this window.");
  return section(
    { title: "Rows not written", iconName: "policy", aside: total ? `${fmtInt(total)} rows` : "", html },
    data,
    "governance",
  );
}

function costCard(data) {
  const c = data.cost;
  if (!c) return "";
  const html = !c.estimated
    ? empty("The connectors in these runs report no priced cost signal, so the compute estimate is zero. The hosted equivalent above is what a per-row-priced service would charge.")
    : `<div class="c-card-pad">${trend(c.series.map((b) => b.estimated), "var(--text-2)")}${axis(c.series, data.bucket)}${barList(
    c.by_pipeline.map((p) => ({
      label: pipelineName(p.pipeline),
      value: p.estimated,
      text: fmtMoney(p.estimated, c.currency),
    })),
  )}</div>`;
  const other = c.other_currencies && c.other_currencies.length ? ` · also priced in ${c.other_currencies.join(", ")} (not shown)` : "";
  return section(
    {
      title: "Estimated cost",
      iconName: "usage",
      aside: `${fmtMoney(c.estimated, c.currency)} · hosted equivalent ${fmtMoney(c.hosted_equivalent, c.currency)}`,
      html,
      footnote: !c.estimated ? "" : `Estimates from each run's usage pricing table, over ${fmtInt(c.invocations)} invocations${other}. Details on the Usage page.`,
    },
    data,
    "cost",
  );
}

function freshnessCard(data) {
  const f = data.freshness;
  if (!f) return "";
  const stale = f.stale.length
    ? `<ul class="ov-plain">${f.stale
        .map(
          (d) =>
            `<li><a href="#/catalog/${encodeURIComponent(d.id)}" title="${esc(d.uri)}">${esc(d.uri)}</a><small title="${esc(formatTs(d.last_success))}">${esc(fmtAge(d.last_success))} ago · ${esc(pipelineName(d.pipeline))}</small></li>`,
        )
        .join("")}</ul>`
    : `<p class="ov-none">Every dataset succeeded within the stale window.</p>`;
  const changed = f.schema_changed.length
    ? `<ul class="ov-plain">${f.schema_changed
        .map(
          (d) =>
            `<li><a href="#/catalog/${encodeURIComponent(d.id)}" title="${esc(d.uri)}">${esc(d.uri || d.id)}</a><small>${fmtInt(d.changes)} change${d.changes === 1 ? "" : "s"} · ${esc(fmtAge(d.last_changed))} ago</small></li>`,
        )
        .join("")}</ul>`
    : `<p class="ov-none">No schema changed in this window.</p>`;
  const staleAfter = fmtSpan(Date.parse(data.until) - Date.parse(f.stale_before));
  return section(
    {
      title: "Freshness",
      iconName: "db",
      aside: `${fmtInt(f.datasets)} datasets`,
      html: `<div class="ov-fresh"><div><h4>Stale <span class="c-pill ${f.stale_count ? "warn" : ""}">${fmtInt(f.stale_count)}</span></h4><p class="ov-hint">no successful run in ${esc(staleAfter)}</p>${stale}</div><div><h4>Schema changed <span class="c-pill ${f.schema_changed_count ? "info" : ""}">${fmtInt(f.schema_changed_count)}</span></h4><p class="ov-hint">a new schema version in this window</p>${changed}</div></div>`,
    },
    data,
    "governance",
  );
}

function stats(data) {
  const r = data.runs;
  const finished = (r.by_status.completed || 0) + (r.by_status.failed || 0) + (r.by_status.cancelled || 0);
  const failed = r.by_status.failed || 0;
  const items = [
    { label: "Runs", value: r.total },
    {
      label: "Failure rate",
      value: finished ? pct(failed / finished) : "—",
      tone: failed ? "bad" : finished ? "ok" : "",
    },
    { label: "Records written", value: fmtCompact(data.records.total) },
  ];
  if (data.cost) items.push({ label: "Estimated cost", value: fmtMoney(data.cost.estimated, data.cost.currency) });
  items.push({ label: "Running now", value: r.running, tone: r.running ? "info" : "" });
  return statStrip(items);
}

function skeleton() {
  const block = (h) => `<div class="ov-skel" style="height:${h}px"></div>`;
  return `${block(64)}${block(58)}<div class="ov-grid">${block(190)}${block(190)}${block(150)}${block(150)}</div>`;
}

export async function renderOverview(container, params = {}) {
  let range = readRange(params.query);
  let alive = true;
  container.innerHTML = `
    <div class="page p3 ov">
      <div class="page-head">
        <h1>Overview</h1>
        ${tabs(RANGES, range, { cls: "ov-range" })}
        <span class="ov-graf-top" id="ov-graf"></span>
        <button class="icon-btn" id="ov-refresh" title="Refresh" aria-label="Refresh">${icon("refresh")}</button>
      </div>
      <p class="p3-sub" id="ov-sub">Fleet trends from the run history.</p>
      <div id="ov-body" aria-busy="true">${skeleton()}</div>
    </div>`;
  const body = container.querySelector("#ov-body");
  const sub = container.querySelector("#ov-sub");

  async function load() {
    const p = withTenant(new URLSearchParams({ since: range }));
    let data;
    try {
      data = await api(`/v1/overview?${p}`);
    } catch (e) {
      if (!alive) return;
      body.innerHTML = `<div class="c-card"><div class="c-card-empty">${esc(e.message)}</div></div>`;
      return;
    }
    if (!alive) return;
    body.removeAttribute("aria-busy");
    sub.textContent = `Fleet trends from the run history, ${formatTs(data.since)} → ${formatTs(data.until)}, per ${data.bucket} (UTC buckets). Real-time metrics live in Grafana.`;
    const top = container.querySelector("#ov-graf");
    top.innerHTML = data.grafana
      ? `<a class="c-btn" href="${esc(data.grafana.serve)}" target="_blank" rel="noopener">${icon("link")}Grafana</a>`
      : "";
    body.innerHTML = `
      ${attentionHtml(data)}
      ${stats(data)}
      <div class="ov-grid">
        ${runsCard(data)}
        ${recordsCard(data)}
        ${failuresCard(data)}
        ${slowestCard(data)}
        ${governanceCard(data)}
        ${costCard(data)}
        ${freshnessCard(data)}
      </div>`;
  }

  container.querySelectorAll(".ov-range .c-tab").forEach((b) => {
    b.onclick = () => {
      range = b.dataset.tab;
      saveRange(range);
      container.querySelectorAll(".ov-range .c-tab").forEach((x) => {
        const on = x.dataset.tab === range;
        x.classList.toggle("on", on);
        x.setAttribute("aria-selected", String(on));
      });
      history.replaceState(null, "", `#/overview?range=${range}`);
      load();
    };
  });
  container.querySelector("#ov-refresh").onclick = load;
  await load();
  return () => {
    alive = false;
  };
}

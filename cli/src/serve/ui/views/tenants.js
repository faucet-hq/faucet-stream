// Tenants (#709): the tenants this server runs for beside the selected one's
// limits, connections and recent runs. An admin creates, edits, suspends and
// deletes tenants; an operator stores connections and starts hosted OAuth
// connect flows. Stored credentials are never shown — only names, types and
// status.
import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml, fmtInt, fmtAge } from "../utils.js";
import { fmtTime, runTitle } from "./runs.js";
import { setTenant } from "../tenant.js";
import { setCrumbTitle } from "../shell.js";
import { icon, statStrip, listRow, pill, tabs } from "../components.js";
import { splitPane, sectionList } from "./split.js";

const LIMIT_FIELDS = [
  ["max_concurrent_runs", "Concurrent runs"],
  ["max_records_per_run", "Records per run"],
  ["max_bytes_per_run", "Bytes per run"],
  ["max_duration_secs", "Seconds per run"],
];

const RUN_RING = {
  running: ["info", "half"],
  sharded: ["info", "half"],
  queued: ["warn", "hollow"],
  pending: ["warn", "hollow"],
  completed: ["ok", "full"],
  failed: ["bad", "full"],
  cancelled: ["muted", "hollow"],
};

function limitsText(limits) {
  const parts = LIMIT_FIELDS.filter(([k]) => limits && limits[k] != null).map(([k, label]) => `${label.toLowerCase()} ${fmtInt(limits[k])}`);
  return parts.length ? parts.join(" · ") : "no limits";
}

function reauthCount(t) {
  return (t.connections || []).filter((c) => c.status === "needs_reauth").length;
}

export async function renderTenants(container, params = {}) {
  let rows = [];
  let unavailable = "";
  let selected = params.id || null;
  let alive = true;
  let gen = 0;

  const pane = splitPane(container, {
    name: "Tenants",
    listHead: `
      <div class="sp-tabs">
        <span class="sp-head-t">Tenants</span>
        <span class="sp-tools">
          <button type="button" class="icon-btn" id="tn-refresh" title="Refresh" aria-label="Refresh">${icon("refresh")}</button>
          <button type="button" class="icon-btn" id="tn-new" data-perm="tenant_admin" title="Create a tenant" aria-label="Create a tenant" aria-expanded="false">${icon("plus")}</button>
        </span>
      </div>
      <form class="sp-filters" id="tn-create" data-perm="tenant_admin" hidden>
        <input id="tn-id" placeholder="tenant id (e.g. acme)" required />
        <input id="tn-name" placeholder="display name (optional)" />
        <button class="btn-primary" type="submit">Create tenant</button>
      </form>
      <div id="tn-stats"></div>`,
  });
  const $ = (sel) => pane.head.querySelector(sel);
  const list = sectionList(pane.body, "faucet.tenants.sections");

  $("#tn-new").onclick = () => {
    const f = $("#tn-create");
    f.hidden = !f.hidden;
    $("#tn-new").classList.toggle("on", !f.hidden);
    $("#tn-new").setAttribute("aria-expanded", String(!f.hidden));
    if (!f.hidden) $("#tn-id").focus();
  };
  $("#tn-create").onsubmit = async (ev) => {
    ev.preventDefault();
    const id = $("#tn-id").value.trim();
    const name = $("#tn-name").value.trim();
    try {
      await api("/v1/tenants", { method: "POST", body: name ? { id, name } : { id } });
      toast(`Tenant ${id} created`);
      $("#tn-create").reset();
      await load();
      navigate(`#/tenants/${encodeURIComponent(id)}`);
    } catch (e) {
      toast(e.message, "error");
    }
  };
  $("#tn-refresh").onclick = () => {
    load();
    if (selected) showDetail(selected);
  };

  async function load() {
    try {
      rows = await api("/v1/tenants");
      unavailable = "";
    } catch (e) {
      rows = [];
      unavailable = e.status === 404 ? "This server was built without the <code>tenants</code> feature." : escapeHtml(e.message);
    }
    if (!alive) return;
    paint();
  }

  function paint() {
    const conns = rows.reduce((n, t) => n + (t.connections || []).length, 0);
    const reauth = rows.reduce((n, t) => n + reauthCount(t), 0);
    $("#tn-stats").innerHTML = statStrip([
      { label: "Tenants", value: rows.length },
      { label: "Running", value: rows.reduce((n, t) => n + (t.active_runs || 0), 0), tone: "info" },
      { label: "Connections", value: conns, unit: reauth ? `${fmtInt(reauth)} re-auth` : "", tone: reauth ? "bad" : "" },
    ]);
    const now = Date.now();
    const row = (t) => {
      const r = reauthCount(t);
      const [tone, shape] = t.suspended ? ["muted", "hollow"] : r ? ["warn", "full"] : ["ok", "full"];
      const n = (t.connections || []).length;
      return listRow({
        id: t.id,
        href: `#/tenants/${encodeURIComponent(t.id)}`,
        tone,
        shape,
        title: t.name || t.id,
        tooltip: `${t.name || t.id}\n${t.id}`,
        age: fmtAge(t.created_at, now),
        ageTitle: `created ${fmtTime(t.created_at)}`,
        metaLead: t.suspended ? "suspended" : r ? `${fmtInt(r)} need re-auth` : `${fmtInt(t.active_runs || 0)} active run${t.active_runs === 1 ? "" : "s"}`,
        meta: [t.name ? t.id : "", `${fmtInt(n)} connection${n === 1 ? "" : "s"}`, limitsText(t.limits)].filter(Boolean).join(" · "),
        selected: t.id === selected,
      });
    };
    list.render(
      [
        { id: "active", label: "Active", rows: rows.filter((t) => !t.suspended).map(row) },
        { id: "suspended", label: "Suspended", rows: rows.filter((t) => t.suspended).map(row) },
      ],
      {
        empty: unavailable
          ? `<div class="sp-empty"><b>Tenants are not available.</b><span>${unavailable}</span></div>`
          : `<div class="sp-empty"><b>No tenants yet.</b><span>Create one with the + button above or with <code>POST /v1/tenants</code>.</span></div>`,
      },
    );
  }

  function showNone() {
    pane.setSelected(false);
    pane.detail.innerHTML = rows.length
      ? `<div class="sp-none"><b>Select a tenant</b><span>Pick one on the left for its limits, connections and recent runs.</span></div>`
      : `<div class="sp-none"><b>No tenants yet</b><span>A tenant owns its connections, limits and a state namespace. Create one to run pipelines on its behalf.</span></div>`;
  }

  async function showDetail(id) {
    selected = id;
    list.select(id);
    if (!id) {
      showNone();
      return;
    }
    pane.setSelected(true);
    const mine = ++gen;
    if (!pane.detail.querySelector(".c-dhead")) pane.detail.innerHTML = `<div class="sp-loading">loading…</div>`;
    let t;
    let providers = [];
    try {
      t = await api(`/v1/tenants/${encodeURIComponent(id)}`);
    } catch (e) {
      if (!alive || mine !== gen) return;
      pane.detail.innerHTML = `<div class="c-dhead"><a class="sp-back" href="#/tenants">${icon("back")}Tenants</a></div><div class="sp-none"><b>${e.status === 404 ? "Tenant not found" : "Could not load this tenant"}</b><span>${escapeHtml(e.message)}</span></div>`;
      return;
    }
    try {
      providers = await api("/v1/connect/providers");
    } catch {
      providers = [];
    }
    if (!alive || mine !== gen) return;
    renderDetail(t, providers);
    loadRuns(t.id, mine);
  }

  let detailTab = "overview";
  function renderDetail(t, providers) {
    const id = t.id;
    setCrumbTitle(t.name || t.id);
    const back = `${location.origin}${location.pathname}#/tenants/${encodeURIComponent(id)}`;
    const limits = t.limits || {};
    const conns = t.connections || [];
    const r = reauthCount(t);
    const sub = [
      t.suspended ? pill("Suspended", "") : r ? pill(`${fmtInt(r)} connection${r === 1 ? "" : "s"} need re-auth`, "warn") : pill("Active", "ok", "check"),
      `<span>id <span class="mono">${escapeHtml(t.id)}</span></span>`,
      `<span>created by <b>${escapeHtml(t.created_by || "?")}</b> · <span title="${escapeHtml(fmtTime(t.created_at))}">${escapeHtml(fmtAge(t.created_at))} ago</span></span>`,
      `<span>${fmtInt(t.active_runs || 0)} active run${t.active_runs === 1 ? "" : "s"}</span>`,
    ];
    const labels = Object.entries(t.labels || {});
    pane.detail.innerHTML = `
      <div class="c-dhead">
        <a class="sp-back" href="#/tenants">${icon("back")}Tenants</a>
        ${tabs([{ id: "overview", label: "Overview" }, { id: "connections", label: "Connections", count: conns.length }], detailTab)}
        <span class="c-dhead-r">
          <button class="c-btn" id="td-runs">${icon("runs")}Runs for this tenant</button>
          <button class="c-btn${t.suspended ? "" : " danger"}" id="td-suspend" data-perm="tenant_admin">${t.suspended ? "Resume" : "Suspend"}</button>
        </span>
      </div>
      <div><h1 class="c-title">${escapeHtml(t.name || t.id)}</h1><div class="c-sub">${sub.join('<span class="c-sub-sep">·</span>')}</div></div>
      <div class="d-pane" data-pane="overview">
        <section class="c-card">
          <div class="c-card-h"><b>Limits</b><span class="c-card-aside">ceilings every run of this tenant honours</span></div>
          <form class="tn-limits" id="td-limits">
            ${LIMIT_FIELDS.map(([k, label]) => `<label>${label}<input type="number" min="1" name="${k}" value="${limits[k] ?? ""}" placeholder="none" /></label>`).join("")}
            <button class="c-btn primary" type="submit" data-perm="tenant_admin">Save limits</button>
          </form>
        </section>
        ${labels.length ? `<section class="c-card"><div class="c-card-h"><b>Labels</b></div><div class="d-labels">${labels.map(([k, v]) => `<span class="c-chip c-chip-mono">${escapeHtml(k)}=${escapeHtml(v)}</span>`).join("")}</div></section>` : ""}
        <section class="c-card">
          <div class="c-card-h">${icon("runs")}<b>Recent runs</b><span class="c-card-r" id="td-runs-n"></span></div>
          <div class="tn-runs" id="td-runs-list"><div class="c-card-empty">loading…</div></div>
        </section>
        <section class="c-card" data-perm="tenant_admin">
          <div class="c-card-h"><b>Delete tenant</b><span class="c-card-aside">removes its runs, usage, change requests, state and connections</span></div>
          <div class="tn-danger">
            <input id="td-confirm" placeholder="type ${escapeHtml(t.id)} to delete" />
            <button class="btn-danger" id="td-delete" disabled>Delete tenant</button>
          </div>
        </section>
      </div>
      <div class="d-pane" data-pane="connections" hidden>
        <section class="c-card">
          <div class="c-card-h">${icon("key")}<b>Connections</b><span class="c-card-aside">sealed auth providers its runs resolve with <code>auth: { ref }</code></span></div>
          <div class="d-scroll-x"><table class="c-tbl tn-conn-tbl">
            <colgroup><col style="width:22%" /><col style="width:22%" /><col style="width:22%" /><col style="width:20%" /><col style="width:150px" /></colgroup>
            <thead><tr><th>Name</th><th>Type</th><th>Status</th><th>Updated</th><th></th></tr></thead>
            <tbody>${
              conns.length
                ? conns
                    .map(
                      (c) => `<tr>
                        <td class="mono c-ell" title="${escapeHtml(c.name)}">${escapeHtml(c.name)}</td>
                        <td>${escapeHtml(c.provider_type)}${c.connect_provider ? ` <span class="c-muted">via ${escapeHtml(c.connect_provider)}</span>` : ""}</td>
                        <td>${c.status === "needs_reauth" ? `<span class="c-nowrap"><span class="c-dot bad"></span>needs re-auth</span>` : `<span class="c-nowrap"><span class="c-dot ok"></span>active</span>`}${c.reauth_reason ? `<div class="c-muted tn-why">${escapeHtml(c.reauth_reason)}</div>` : ""}</td>
                        <td title="${escapeHtml(fmtTime(c.updated_at))}">${escapeHtml(fmtAge(c.updated_at))} ago <span class="c-muted">by ${escapeHtml(c.updated_by)}</span></td>
                        <td class="tn-row-actions" data-perm="connection_manage">${
                          c.connect_provider
                            ? `<button class="c-btn tn-reconnect" data-name="${escapeHtml(c.name)}" data-provider="${escapeHtml(c.connect_provider)}">Reconnect</button>`
                            : ""
                        }<button class="c-btn danger tn-del-conn" data-name="${escapeHtml(c.name)}">Delete</button></td>
                      </tr>`,
                    )
                    .join("")
                : `<tr><td colspan="5" class="c-muted">No connections yet.</td></tr>`
            }</tbody>
          </table></div>
        </section>
        <div class="c-two tn-two" data-perm="connection_manage">
          <section class="c-card">
            <div class="c-card-h"><b>Connect through a provider</b><span class="c-card-aside">hosted OAuth</span></div>
            ${
              providers.length
                ? `<form class="tn-connect" id="td-connect">
                    <select id="td-provider">${providers.map((p) => `<option value="${escapeHtml(p.name)}">${escapeHtml(p.name)}</option>`).join("")}</select>
                    <input id="td-conn-name" placeholder="connection name" required />
                    <input id="td-redirect" value="${escapeHtml(back)}" title="where the browser returns — must be under the provider's allowed_redirects" />
                    <button class="c-btn primary" type="submit">Connect</button>
                  </form>`
                : `<div class="c-card-empty">No providers configured. Start the server with <code>--connect-providers</code>.</div>`
            }
          </section>
          <section class="c-card">
            <div class="c-card-h"><b>Store credentials</b><span class="c-card-aside">sealed at rest</span></div>
            <form class="tn-connect" id="td-store">
              <input id="td-store-name" placeholder="connection name" required />
              <select id="td-store-type">
                ${["static", "oauth2", "oauth2_refresh", "token_endpoint", "flow"].map((k) => `<option>${k}</option>`).join("")}
              </select>
              <textarea id="td-store-config" rows="3" placeholder='config JSON, e.g. {"token": "…"}' required></textarea>
              <button class="c-btn primary" type="submit">Save connection</button>
            </form>
          </section>
        </div>
      </div>`;

    const d = pane.detail;
    const showTab = (x) => {
      detailTab = x;
      d.querySelectorAll(".c-dhead .c-tab").forEach((b) => {
        b.classList.toggle("on", b.dataset.tab === x);
        b.setAttribute("aria-selected", String(b.dataset.tab === x));
      });
      d.querySelectorAll(".d-pane").forEach((p) => (p.hidden = p.dataset.pane !== x));
    };
    d.querySelectorAll(".c-dhead .c-tab").forEach((b) => (b.onclick = () => showTab(b.dataset.tab)));
    showTab(detailTab);

    const reload = () => {
      showDetail(id);
      load();
    };
    d.querySelector("#td-runs").onclick = () => {
      setTenant(t.id);
      navigate("#/runs");
    };
    d.querySelector("#td-limits").onsubmit = async (ev) => {
      ev.preventDefault();
      const next = {};
      for (const [k] of LIMIT_FIELDS) {
        const v = ev.target.elements[k].value.trim();
        if (v) next[k] = Number(v);
      }
      try {
        await api(`/v1/tenants/${encodeURIComponent(id)}`, { method: "PATCH", body: { limits: next } });
        toast("Limits saved");
        reload();
      } catch (e) {
        toast(e.message, "error");
      }
    };
    d.querySelectorAll(".tn-del-conn").forEach((b) => {
      b.onclick = async () => {
        try {
          await api(`/v1/tenants/${encodeURIComponent(id)}/connections/${encodeURIComponent(b.dataset.name)}`, { method: "DELETE" });
          toast(`Connection ${b.dataset.name} deleted`);
          reload();
        } catch (e) {
          toast(e.message, "error");
        }
      };
    });
    const startConnect = async (provider, connection, redirect) => {
      try {
        const r = await api(`/v1/tenants/${encodeURIComponent(id)}/connect/${encodeURIComponent(provider)}`, {
          method: "POST",
          body: { connection, redirect },
        });
        window.open(r.authorize_url, "_blank", "noopener");
        toast("Authorization opened in a new tab");
      } catch (e) {
        toast(e.message, "error");
      }
    };
    d.querySelectorAll(".tn-reconnect").forEach((b) => {
      b.onclick = () => startConnect(b.dataset.provider, b.dataset.name, back);
    });
    const connectForm = d.querySelector("#td-connect");
    if (connectForm) {
      connectForm.onsubmit = (ev) => {
        ev.preventDefault();
        startConnect(d.querySelector("#td-provider").value, d.querySelector("#td-conn-name").value.trim(), d.querySelector("#td-redirect").value.trim());
      };
    }
    d.querySelector("#td-store").onsubmit = async (ev) => {
      ev.preventDefault();
      let config;
      try {
        config = JSON.parse(d.querySelector("#td-store-config").value);
      } catch {
        toast("The config must be JSON", "error");
        return;
      }
      const name = d.querySelector("#td-store-name").value.trim();
      try {
        await api(`/v1/tenants/${encodeURIComponent(id)}/connections/${encodeURIComponent(name)}`, {
          method: "PUT",
          body: { provider: { type: d.querySelector("#td-store-type").value, config } },
        });
        toast(`Connection ${name} saved`);
        reload();
      } catch (e) {
        toast(e.message, "error");
      }
    };
    d.querySelector("#td-suspend").onclick = async () => {
      try {
        await api(`/v1/tenants/${encodeURIComponent(id)}`, { method: "PATCH", body: { suspended: !t.suspended } });
        toast(t.suspended ? "Tenant resumed" : "Tenant suspended");
        reload();
      } catch (e) {
        toast(e.message, "error");
      }
    };
    const confirmBox = d.querySelector("#td-confirm");
    const del = d.querySelector("#td-delete");
    confirmBox.oninput = () => {
      del.disabled = confirmBox.value.trim() !== t.id;
    };
    del.onclick = async () => {
      try {
        const res = await api(`/v1/tenants/${encodeURIComponent(id)}`, { method: "DELETE" });
        toast(`Deleted ${t.id}: ${res.runs} run(s), ${res.state_keys_deleted} state key(s)`);
        setTenant("");
        await load();
        navigate("#/tenants");
      } catch (e) {
        toast(e.message, "error");
      }
    };
  }

  async function loadRuns(id, mine) {
    const host = pane.detail.querySelector("#td-runs-list");
    let data;
    try {
      data = await api(`/v1/runs?${new URLSearchParams({ tenant: id, limit: "10" })}`);
    } catch (e) {
      if (host && mine === gen) host.innerHTML = `<div class="c-card-empty">${escapeHtml(e.message)}</div>`;
      return;
    }
    if (!alive || mine !== gen || !host.isConnected) return;
    const runs = data.runs || [];
    pane.detail.querySelector("#td-runs-n").textContent = runs.length ? `latest ${fmtInt(runs.length)}${data.next_cursor ? "+" : ""}` : "";
    const now = Date.now();
    host.innerHTML = runs.length
      ? runs
          .map((r) => {
            const [tone, shape] = RUN_RING[r.status] || ["muted", "full"];
            return listRow({
              id: r.run_id,
              href: `#/runs/${encodeURIComponent(r.run_id)}`,
              tone,
              shape,
              title: runTitle(r),
              tooltip: r.run_id,
              age: fmtAge(r.submitted_at, now),
              ageTitle: `submitted ${fmtTime(r.submitted_at)}`,
              metaLead: r.status,
              meta: `${fmtInt(r.records_written ?? 0)} records`,
            });
          })
          .join("")
      : `<div class="c-card-empty">No runs for this tenant yet.</div>`;
  }

  await load();
  if (!alive) return;
  showDetail(selected);

  return {
    teardown() {
      alive = false;
    },
    update(p) {
      const id = p.id || null;
      if (id === selected) return;
      showDetail(id);
    },
  };
}

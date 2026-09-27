// Tenants (#709): the tenants this server runs for, their limits and
// connections. An admin creates, edits, suspends and deletes tenants; an
// operator stores connections and starts hosted OAuth connect flows. Stored
// credentials are never shown — only names, types and status.
import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml, fmtInt } from "../utils.js";
import { fmtTime } from "./runs.js";
import { setTenant } from "../tenant.js";

const LIMIT_FIELDS = [
  ["max_concurrent_runs", "Concurrent runs"],
  ["max_records_per_run", "Records per run"],
  ["max_bytes_per_run", "Bytes per run"],
  ["max_duration_secs", "Seconds per run"],
];

function limitsLine(limits) {
  const parts = LIMIT_FIELDS.filter(([k]) => limits && limits[k] != null).map(
    ([k, label]) => `<span class="chg-kv"><b>${label}</b> ${fmtInt(limits[k])}</span>`,
  );
  return parts.length ? parts.join("") : `<span class="chg-muted">no limits</span>`;
}

function connSummary(conns) {
  const reauth = conns.filter((c) => c.status === "needs_reauth").length;
  const active = conns.length - reauth;
  return `${fmtInt(active)} active${reauth ? ` · <span class="pill pill-failed">${fmtInt(reauth)} need re-auth</span>` : ""}`;
}

function statusPill(c) {
  return c.status === "needs_reauth"
    ? `<span class="pill pill-failed">needs re-auth</span>`
    : `<span class="pill pill-completed">active</span>`;
}

export async function renderTenants(container, params = {}) {
  if (params.id) return renderTenantDetail(container, params);
  container.innerHTML = `
    <div class="page page-wide">
      <div class="page-head">
        <h1>Tenants</h1>
        <button class="btn-ghost" id="tn-refresh" title="refresh">↻</button>
      </div>
      <form class="tn-create" id="tn-create" data-perm="tenant_admin">
        <input id="tn-id" placeholder="tenant id (e.g. acme)" required />
        <input id="tn-name" placeholder="display name (optional)" />
        <button class="btn-primary" type="submit">Create tenant</button>
      </form>
      <table class="chg-table">
        <thead><tr><th>tenant</th><th>connections</th><th>active runs</th><th>limits</th><th>created</th></tr></thead>
        <tbody id="tn-list"><tr><td colspan="5" class="empty">loading…</td></tr></tbody>
      </table>
    </div>`;
  const list = container.querySelector("#tn-list");

  async function load() {
    let rows;
    try {
      rows = await api("/v1/tenants");
    } catch (e) {
      list.innerHTML = `<tr><td colspan="5" class="empty">${
        e.status === 404
          ? "This server was built without the <code>tenants</code> feature."
          : escapeHtml(e.message)
      }</td></tr>`;
      return;
    }
    if (!rows.length) {
      list.innerHTML = `<tr><td colspan="5" class="empty">No tenants yet. Create one above or with <code>POST /v1/tenants</code>.</td></tr>`;
      return;
    }
    list.innerHTML = rows
      .map(
        (t) => `<tr class="chg-tr" data-id="${escapeHtml(t.id)}">
          <td><b>${escapeHtml(t.name || t.id)}</b>${t.name ? ` <code>${escapeHtml(t.id)}</code>` : ""}${t.suspended ? ` <span class="pill pill-cancelled">suspended</span>` : ""}</td>
          <td>${connSummary(t.connections || [])}</td>
          <td>${fmtInt(t.active_runs || 0)}</td>
          <td><div class="chg-kvs">${limitsLine(t.limits)}</div></td>
          <td title="${escapeHtml(t.created_at)}">${fmtTime(t.created_at)}</td>
        </tr>`,
      )
      .join("");
    list.querySelectorAll(".chg-tr").forEach((tr) => {
      tr.onclick = () => navigate(`#/tenants/${encodeURIComponent(tr.dataset.id)}`);
    });
  }

  container.querySelector("#tn-create").onsubmit = async (ev) => {
    ev.preventDefault();
    const id = container.querySelector("#tn-id").value.trim();
    const name = container.querySelector("#tn-name").value.trim();
    try {
      await api("/v1/tenants", { method: "POST", body: name ? { id, name } : { id } });
      toast(`Tenant ${id} created`);
      navigate(`#/tenants/${encodeURIComponent(id)}`);
    } catch (e) {
      toast(e.message, "error");
    }
  };
  container.querySelector("#tn-refresh").onclick = load;
  await load();
}

async function renderTenantDetail(container, { id }) {
  let t;
  let providers = [];
  try {
    t = await api(`/v1/tenants/${encodeURIComponent(id)}`);
  } catch (e) {
    container.innerHTML = `<div class="page"><div class="empty">${escapeHtml(e.message)}</div></div>`;
    return;
  }
  try {
    providers = await api("/v1/connect/providers");
  } catch {
    providers = [];
  }
  const back = `${location.origin}${location.pathname}#/tenants/${encodeURIComponent(id)}`;
  const limits = t.limits || {};
  container.innerHTML = `
    <div class="page page-wide">
      <div class="page-head">
        <h1>${escapeHtml(t.name || t.id)} ${t.suspended ? `<span class="pill pill-cancelled">suspended</span>` : ""}</h1>
        <button class="btn-ghost" id="td-runs">Runs for this tenant</button>
        <button class="btn-ghost" id="td-back">All tenants</button>
      </div>
      <p class="chg-meta">id <code>${escapeHtml(t.id)}</code> · created by ${escapeHtml(t.created_by)} · ${fmtTime(t.created_at)} · ${fmtInt(t.active_runs || 0)} active run(s)</p>
      <div class="chg-detail">
        <h3>Limits</h3>
        <form class="tn-limits" id="td-limits">
          ${LIMIT_FIELDS.map(
            ([k, label]) => `<label>${label}<input type="number" min="1" name="${k}" value="${limits[k] ?? ""}" placeholder="none" /></label>`,
          ).join("")}
          <button class="btn-primary" type="submit" data-perm="tenant_admin">Save limits</button>
        </form>
        <h3>Connections</h3>
        <table class="chg-plan">
          <thead><tr><th>name</th><th>type</th><th>status</th><th>updated</th><th></th></tr></thead>
          <tbody>${
            (t.connections || []).length
              ? t.connections
                  .map(
                    (c) => `<tr>
                      <td><code>${escapeHtml(c.name)}</code></td>
                      <td>${escapeHtml(c.provider_type)}${c.connect_provider ? ` <span class="chg-muted">via ${escapeHtml(c.connect_provider)}</span>` : ""}</td>
                      <td>${statusPill(c)}${c.reauth_reason ? `<div class="chg-reason">${escapeHtml(c.reauth_reason)}</div>` : ""}</td>
                      <td>${fmtTime(c.updated_at)} <span class="chg-muted">by ${escapeHtml(c.updated_by)}</span></td>
                      <td class="tn-row-actions" data-perm="connection_manage">${
                        c.connect_provider
                          ? `<button class="btn-ghost tn-reconnect" data-name="${escapeHtml(c.name)}" data-provider="${escapeHtml(c.connect_provider)}">Reconnect</button>`
                          : ""
                      }<button class="btn-ghost tn-del-conn" data-name="${escapeHtml(c.name)}">Delete</button></td>
                    </tr>`,
                  )
                  .join("")
              : `<tr><td colspan="5" class="chg-muted">No connections yet.</td></tr>`
          }</tbody>
        </table>
        <div class="chg-cols" data-perm="connection_manage">
          <div>
            <h3>Connect through a provider</h3>
            ${
              providers.length
                ? `<form class="tn-connect" id="td-connect">
                    <select id="td-provider">${providers.map((p) => `<option value="${escapeHtml(p.name)}">${escapeHtml(p.name)}</option>`).join("")}</select>
                    <input id="td-conn-name" placeholder="connection name" required />
                    <input id="td-redirect" value="${escapeHtml(back)}" title="where the browser returns — must be under the provider's allowed_redirects" />
                    <button class="btn-primary" type="submit">Connect</button>
                  </form>`
                : `<p class="chg-muted">No providers configured. Start the server with <code>--connect-providers</code>.</p>`
            }
          </div>
          <div>
            <h3>Store credentials</h3>
            <form class="tn-connect" id="td-store">
              <input id="td-store-name" placeholder="connection name" required />
              <select id="td-store-type">
                ${["static", "oauth2", "oauth2_refresh", "token_endpoint", "flow"].map((k) => `<option>${k}</option>`).join("")}
              </select>
              <textarea id="td-store-config" rows="3" placeholder='config JSON, e.g. {"token": "…"}' required></textarea>
              <button class="btn-primary" type="submit">Save connection</button>
            </form>
          </div>
        </div>
        <div class="chg-actions" data-perm="tenant_admin">
          <button class="btn-warn" id="td-suspend">${t.suspended ? "Resume tenant" : "Suspend tenant"}</button>
          <span class="tn-spacer"></span>
          <input id="td-confirm" placeholder="type ${escapeHtml(t.id)} to delete" />
          <button class="btn-danger" id="td-delete" disabled>Delete tenant</button>
        </div>
      </div>
    </div>`;

  const reload = () => renderTenantDetail(container, { id });
  container.querySelector("#td-back").onclick = () => navigate("#/tenants");
  container.querySelector("#td-runs").onclick = () => {
    setTenant(t.id);
    navigate("#/runs");
  };
  container.querySelector("#td-limits").onsubmit = async (ev) => {
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
  container.querySelectorAll(".tn-del-conn").forEach((b) => {
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
  container.querySelectorAll(".tn-reconnect").forEach((b) => {
    b.onclick = () => startConnect(b.dataset.provider, b.dataset.name, back);
  });
  const connectForm = container.querySelector("#td-connect");
  if (connectForm) {
    connectForm.onsubmit = (ev) => {
      ev.preventDefault();
      startConnect(
        container.querySelector("#td-provider").value,
        container.querySelector("#td-conn-name").value.trim(),
        container.querySelector("#td-redirect").value.trim(),
      );
    };
  }
  container.querySelector("#td-store").onsubmit = async (ev) => {
    ev.preventDefault();
    let config;
    try {
      config = JSON.parse(container.querySelector("#td-store-config").value);
    } catch {
      toast("The config must be JSON", "error");
      return;
    }
    const name = container.querySelector("#td-store-name").value.trim();
    try {
      await api(`/v1/tenants/${encodeURIComponent(id)}/connections/${encodeURIComponent(name)}`, {
        method: "PUT",
        body: { provider: { type: container.querySelector("#td-store-type").value, config } },
      });
      toast(`Connection ${name} saved`);
      reload();
    } catch (e) {
      toast(e.message, "error");
    }
  };
  container.querySelector("#td-suspend").onclick = async () => {
    try {
      await api(`/v1/tenants/${encodeURIComponent(id)}`, { method: "PATCH", body: { suspended: !t.suspended } });
      toast(t.suspended ? "Tenant resumed" : "Tenant suspended");
      reload();
    } catch (e) {
      toast(e.message, "error");
    }
  };
  const confirm = container.querySelector("#td-confirm");
  const del = container.querySelector("#td-delete");
  confirm.oninput = () => {
    del.disabled = confirm.value.trim() !== t.id;
  };
  del.onclick = async () => {
    try {
      const r = await api(`/v1/tenants/${encodeURIComponent(id)}`, { method: "DELETE" });
      toast(`Deleted ${t.id}: ${r.runs} run(s), ${r.state_keys_deleted} state key(s)`);
      setTenant("");
      navigate("#/tenants");
    } catch (e) {
      toast(e.message, "error");
    }
  };
}

// Tenant scope for the console (#709). The topbar switcher picks a tenant and
// every tenant-aware view (Runs, Usage, Changes) adds `tenant=` to its query.
// A tenant-scoped principal is pinned to its own tenant. On a server built
// without the `tenants` feature the list is null and the switcher hides.
import { api } from "./api.js";
import { whoami } from "./access.js";

const KEY = "faucet.tenant";
let tenants = null;

/** The known tenants, or null when the server has no tenants API. */
export const tenantList = () => tenants;

/** The tenant the views are scoped to, or "" for every tenant. */
export function currentTenant() {
  const me = whoami();
  if (me && me.tenant) return me.tenant;
  try {
    return localStorage.getItem(KEY) || "";
  } catch {
    return "";
  }
}

export function setTenant(id) {
  try {
    if (id) localStorage.setItem(KEY, id);
    else localStorage.removeItem(KEY);
  } catch {
    /* storage unavailable: the choice lasts for this page only */
  }
}

/** Add the current tenant to a query. */
export function withTenant(params) {
  const t = currentTenant();
  if (t) params.set("tenant", t);
  return params;
}

/** Fetch the tenant list and render the switcher. */
export async function loadTenants(onChange) {
  try {
    tenants = await api("/v1/tenants");
  } catch {
    tenants = null;
  }
  const sel = document.getElementById("tenant-select");
  const nav = document.getElementById("nav-tenants");
  if (nav) nav.hidden = tenants === null;
  if (!sel) return;
  if (tenants === null || (!tenants.length && !currentTenant())) {
    sel.hidden = true;
    return;
  }
  const me = whoami();
  const pinned = !!(me && me.tenant);
  const cur = currentTenant();
  if (cur && !tenants.some((t) => t.id === cur) && !pinned) setTenant("");
  sel.innerHTML =
    (pinned ? "" : `<option value="">All tenants</option>`) +
    tenants
      .map((t) => `<option value="${escapeAttr(t.id)}">${escapeAttr(t.name || t.id)}</option>`)
      .join("");
  sel.value = currentTenant();
  sel.disabled = pinned;
  sel.hidden = false;
  sel.onchange = () => {
    setTenant(sel.value);
    onChange();
  };
}

function escapeAttr(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}

// What the signed-in caller may do (#698), from `GET /v1/whoami`. Controls that
// need a permission carry `data-perm="<permission>"`; one stylesheet rule per
// missing permission hides them, so the gating survives every re-render without
// each view checking. The server stays the authority — hiding a control is UX.
import { api, onForbidden } from "./api.js";
import { refresh } from "./router.js";

let me = null;

/** The caller's identity, or null before it loads (or on an older server). */
export const whoami = () => me;

/** Whether the caller holds `perm`. Unknown identity allows everything: the
 *  server still refuses what the caller may not do. */
export const can = (perm) => !me || me.permissions.includes(perm);

const ALL = [
  "run_write", "doctor", "dlq_manage", "template_admin", "local_output_manage", "audit_read", "reload",
];

function applyStyle() {
  let style = document.getElementById("access-style");
  if (!style) {
    style = document.createElement("style");
    style.id = "access-style";
    document.head.appendChild(style);
  }
  style.textContent = ALL.filter((p) => !can(p))
    .map((p) => `[data-perm~="${p}"]{display:none!important}`)
    .join("\n");
}

function renderBadge() {
  const el = document.getElementById("whoami");
  if (!el) return;
  if (!me) {
    el.hidden = true;
    return;
  }
  el.hidden = false;
  el.dataset.role = me.role;
  el.title = `${me.principal} · ${me.role}${me.role === "viewer" ? " (read-only access)" : ""}`;
  el.innerHTML = `<span class="whoami-name"></span><span class="whoami-role"></span>`;
  el.querySelector(".whoami-name").textContent = me.principal;
  el.querySelector(".whoami-role").textContent = me.role === "viewer" ? "read-only" : me.role;
}

/** Fetch the caller's identity and apply it. A server without the endpoint
 *  (404) or an unauthenticated caller leaves everything visible. */
export async function loadAccess() {
  try {
    me = await api("/v1/whoami");
  } catch {
    me = null;
  }
  applyStyle();
  renderBadge();
  document.documentElement.dataset.role = me ? me.role : "";
}

// A 403 means the role changed under us (e.g. a rotated token): re-read it and
// re-render, so the controls the caller just lost disappear.
onForbidden(async () => {
  const before = me?.role;
  await loadAccess();
  if (me?.role !== before) refresh();
});

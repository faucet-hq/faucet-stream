// Console entry (#998): theme, timezone, access token, routes, shell.
import { token, onUnauthorized, authOk } from "./api.js";
import { startRouter, refresh, route } from "./router.js";
import { getTz, setTz, TZ_OPTIONS } from "./tz.js";
import { loadTenants } from "./tenant.js";
import { loadAccess } from "./access.js";
import { startShell, refreshBadges } from "./shell.js";
import { installGlobalBehaviours, dialog } from "./page.js";
import { installPalette } from "./palette.js";
import { html, btn, input } from "./components.js";
import { renderOverview } from "./views/overview.js";
import { renderRuns } from "./views/runs.js";
import { renderChanges } from "./views/changes.js";
import { renderTriggers } from "./views/triggers.js";
import { renderSchedules } from "./views/schedules.js";
import { renderDatasets } from "./views/datasets.js";
import { renderLineage } from "./views/lineage.js";
import { renderTemplates } from "./views/templates.js";
import { renderSchemas } from "./views/schemas.js";
import { renderSubmit } from "./views/submit.js";
import { renderPolicy } from "./views/policy.js";
import { renderUsage } from "./views/usage.js";
import { renderTenants } from "./views/tenants.js";
import { renderAudit } from "./views/audit.js";
import { renderSettings } from "./views/settings.js";

const THEME_KEY = "faucet.theme";
export function applyTheme(t) {
  document.documentElement.dataset.theme = t;
  try { localStorage.setItem(THEME_KEY, t); } catch { /* this page only */ }
}
function initTheme() {
  let t = "auto";
  try { t = localStorage.getItem(THEME_KEY) || "auto"; } catch { /* default */ }
  document.documentElement.dataset.theme = t;
}

/** Ask for the API bearer token (opens on a 401 and from Settings). */
export async function askToken() {
  const r = await dialog(
    "Access token",
    html`<div>This server requires a bearer token for the API. It is kept in this browser only.</div>${input(token.get(), { type: "password", ph: "Paste a bearer token", id: "token-input" })}`,
    html`${btn("Clear", { kind: "quiet", data: { act: "clear" } })}${btn("Save", { kind: "primary", data: { act: "save" } })}`,
    { icon: "key", onOpen: (el) => el.querySelector("#token-input").addEventListener("keydown", (e) => { if (e.key === "Enter") el.querySelector('[data-act="save"]').click(); }) },
  );
  if (r && r.act === "save") {
    const val = (r.values["token-input"] || "").trim();
    if (val) token.set(val); else token.clear();
    location.reload();
  } else if (r && r.act === "clear") {
    token.clear();
    location.reload();
  }
}

function wireChrome() {
  document.getElementById("theme-toggle").onclick = () => {
    const cur = document.documentElement.dataset.theme;
    const dark = cur === "dark" || (cur !== "light" && !matchMedia("(prefers-color-scheme: light)").matches);
    applyTheme(dark ? "light" : "dark");
  };
  const tz = document.getElementById("tz-select");
  tz.innerHTML = TZ_OPTIONS.map((o) => `<option value="${o.value}">${o.label}</option>`).join("");
  tz.value = getTz();
  tz.onchange = () => { setTz(tz.value); refresh(); };
  let asked = false;
  onUnauthorized(() => { if (!asked) { asked = true; askToken().finally(() => (asked = false)); } });
}

function registerRoutes() {
  route("#/overview", renderOverview);
  route("#/runs", renderRuns, { group: "runs" });
  route("#/runs/:id", renderRuns, { group: "runs" });
  route("#/changes", renderChanges, { group: "changes" });
  route("#/changes/:id", renderChanges, { group: "changes" });
  route("#/triggers", renderTriggers, { group: "triggers" });
  route("#/triggers/:id", renderTriggers, { group: "triggers" });
  route("#/schedules", renderSchedules);
  route("#/catalog", renderDatasets, { group: "catalog" });
  route("#/catalog/:id", renderDatasets, { group: "catalog" });
  route("#/lineage", renderLineage, { group: "lineage" });
  route("#/lineage/:root", renderLineage, { group: "lineage" });
  route("#/templates", renderTemplates, { group: "templates" });
  route("#/templates/:id", renderTemplates, { group: "templates" });
  route("#/schemas", renderSchemas, { group: "schemas" });
  route("#/schemas/:kind/:name", renderSchemas, { group: "schemas" });
  route("#/submit", renderSubmit, { group: "submit" });
  route("#/policy", renderPolicy);
  route("#/usage", renderUsage, { group: "usage" });
  route("#/tenants", renderTenants, { group: "tenants" });
  route("#/tenants/:id", renderTenants, { group: "tenants" });
  route("#/audit", renderAudit, { group: "audit" });
  route("#/audit/:id", renderAudit, { group: "audit" });
  route("#/settings", renderSettings, { group: "settings" });
}

async function main() {
  initTheme();
  wireChrome();
  registerRoutes();
  installGlobalBehaviours();
  installPalette();
  if (!(await authOk())) askToken();
  await loadAccess();
  await loadTenants(() => { refresh(); refreshBadges(true); });
  await startShell();
  startRouter(document.getElementById("view"));
}
main();

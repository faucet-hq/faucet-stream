import { token, onUnauthorized, authOk } from "./api.js";
import { startRouter, refresh } from "./router.js";
import { getTz, setTz, TZ_OPTIONS } from "./tz.js";
import { renderRuns } from "./views/runs.js";
import { renderSubmit } from "./views/submit.js";
import { renderSchemas } from "./views/schemas.js";
import { renderTemplates } from "./views/templates.js";
import { renderDatasets } from "./views/datasets.js";
import { renderLineage } from "./views/lineage.js";
import { renderUsage } from "./views/usage.js";
import { renderChanges } from "./views/changes.js";
import { renderTenants } from "./views/tenants.js";
import { loadTenants } from "./tenant.js";
import { route } from "./router.js";
import { loadAccess } from "./access.js";
import { startShell, refreshBadges } from "./shell.js";

// --- theme ---
const THEME_KEY = "faucet.theme";
function applyTheme(t) {
  document.documentElement.dataset.theme = t;
  localStorage.setItem(THEME_KEY, t);
}
function initTheme() {
  applyTheme(localStorage.getItem(THEME_KEY) || "auto");
}

// --- token modal ---
function openTokenModal() {
  const modal = document.getElementById("token-modal");
  modal.classList.add("open");
  const input = document.getElementById("token-input");
  input.value = token.get();
  input.focus();
}
function closeTokenModal() {
  document.getElementById("token-modal").classList.remove("open");
}

function wireChrome() {
  // One click always flips what is on screen. `auto` (follow the OS) is the
  // starting state only: cycling through it made the first click a no-op
  // whenever the OS theme matched the next state.
  document.getElementById("theme-toggle").onclick = () => {
    const cur = document.documentElement.dataset.theme;
    const dark = cur === "dark" || (cur !== "light" && !matchMedia("(prefers-color-scheme: light)").matches);
    applyTheme(dark ? "light" : "dark");
  };
  // Display-timezone selector: populate, reflect the saved choice, and re-render
  // the current view (all timestamps + the date picker follow it) on change.
  const tzSel = document.getElementById("tz-select");
  if (tzSel) {
    tzSel.innerHTML = TZ_OPTIONS.map(
      (o) => `<option value="${o.value}">${o.label}</option>`,
    ).join("");
    tzSel.value = getTz();
    tzSel.onchange = () => { setTz(tzSel.value); refresh(); };
  }
  document.getElementById("token-btn").onclick = openTokenModal;
  document.getElementById("token-save").onclick = () => {
    const v = document.getElementById("token-input").value.trim();
    if (v) token.set(v); else token.clear();
    closeTokenModal();
    location.reload();
  };
  document.getElementById("token-clear").onclick = () => {
    token.clear();
    closeTokenModal();
    location.reload();
  };
  onUnauthorized(openTokenModal);
}

function registerRoutes() {
  route("#/runs", renderRuns, { group: "runs" });
  route("#/runs/:id", renderRuns, { group: "runs" });
  route("#/submit", renderSubmit);
  route("#/schemas", renderSchemas);
  route("#/templates", renderTemplates, { group: "templates" });
  route("#/templates/:id", renderTemplates, { group: "templates" });
  route("#/catalog", renderDatasets, { group: "catalog" });
  route("#/catalog/:id", renderDatasets, { group: "catalog" });
  route("#/lineage", renderLineage);
  route("#/lineage/:root", renderLineage);
  route("#/usage", renderUsage);
  route("#/changes", renderChanges, { group: "changes" });
  route("#/changes/:id", renderChanges, { group: "changes" });
  route("#/tenants", renderTenants, { group: "tenants" });
  route("#/tenants/:id", renderTenants, { group: "tenants" });
}

async function main() {
  initTheme();
  wireChrome();
  registerRoutes();
  // If the server requires auth and we have no valid token, prompt first.
  if (!(await authOk())) openTokenModal();
  await loadAccess();
  await loadTenants(() => {
    refresh();
    refreshBadges(true);
  });
  startShell();
  startRouter(document.getElementById("view"));
}
main();

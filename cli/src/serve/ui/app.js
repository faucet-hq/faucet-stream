import { token, onUnauthorized, authOk } from "./api.js";
import { startRouter, navigate, refresh } from "./router.js";
import { getTz, setTz, TZ_OPTIONS } from "./tz.js";
import { renderRuns } from "./views/runs.js";
import { renderDetail } from "./views/detail.js";
import { renderSubmit } from "./views/submit.js";
import { renderSchemas } from "./views/schemas.js";
import { renderTemplates, renderTemplateDetail } from "./views/templates.js";
import { renderDatasets, renderDatasetDetail } from "./views/datasets.js";
import { renderLineage } from "./views/lineage.js";
import { route } from "./router.js";
import { loadAccess } from "./access.js";

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
  document.getElementById("nav-runs").onclick = () => navigate("#/runs");
  document.getElementById("nav-submit").onclick = () => navigate("#/submit");
  document.getElementById("nav-schemas").onclick = () => navigate("#/schemas");
  document.getElementById("nav-templates").onclick = () => navigate("#/templates");
  document.getElementById("nav-datasets").onclick = () => navigate("#/catalog");
  document.getElementById("nav-lineage").onclick = () => navigate("#/lineage");
  // One click always flips what is on screen. `auto` (follow the OS) is the
  // starting state only: cycling through it made the first click a no-op
  // whenever the OS theme matched the next state.
  document.getElementById("theme-toggle").onclick = () => {
    const cur = document.documentElement.dataset.theme;
    const dark = cur === "dark" || (cur !== "light" && matchMedia("(prefers-color-scheme: dark)").matches);
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
  route("#/runs", renderRuns);
  route("#/runs/:id", renderDetail);
  route("#/submit", renderSubmit);
  route("#/schemas", renderSchemas);
  route("#/templates", renderTemplates);
  route("#/templates/:id", renderTemplateDetail);
  route("#/catalog", renderDatasets);
  route("#/catalog/:id", renderDatasetDetail);
  route("#/lineage", renderLineage);
  route("#/lineage/:root", renderLineage);
}

async function main() {
  initTheme();
  wireChrome();
  registerRoutes();
  // If the server requires auth and we have no valid token, prompt first.
  if (!(await authOk())) openTokenModal();
  await loadAccess();
  startRouter(document.getElementById("view"));
}
main();

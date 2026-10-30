// Tenants (#998): being rebuilt to the approved mockups.
import { html, v, empty } from "../components.js";
import { layout } from "../page.js";

export function renderTenants(host) {
  const { detail } = layout(host, "canvas");
  detail.innerHTML = v(html`${empty("Tenants", "This page is being rebuilt.")}`);
}

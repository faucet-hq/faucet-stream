// Policy (#998): a designed "soon" page — policies are enforced today, but the
// server exposes no endpoint to read the loaded policy.
import { html, head, tag, card, code, lnk, row, ic, v } from "../components.js";
import { layout } from "../page.js";
import { href } from "../links.js";

const planned = (i, title, text) => row(html`<span class="ri">${ic(i)}</span>`, title, "", text);

export function renderPolicy(host) {
  const { detail } = layout(host, "canvas");
  detail.innerHTML = v(html`
    ${head("Policy", [tag("soon"), html`<span>enforced today, not yet browsable here</span>`], "The server already refuses runs that send labelled columns to a sink the policy forbids. This page will show the loaded policy, which rules matched recent runs, and the refusals they caused.")}
    <div class="two">
      ${card({ icon: "policy", title: "What will live here", body: html`<div class="st-ul">${planned("layers", "Classifications", "which columns carry each label, by name or value detector")}${planned("shield", "Rules", "which sinks a label may reach: require, mask or deny")}${planned("alert", "Refusals", "runs and change requests the policy stopped, with the rule")}${planned("diff", "Dry run a config", "check a pipeline against the policy before submitting")}</div>` })}
      ${card({ icon: "code", title: "Use it today", body: html`<div class="pad stack" style="gap:10px"><span>Check a pipeline against a policy from the CLI:</span>${code("faucet policy pipeline.yaml --policy eu-pii.yaml", { lang: "text" })}<span>Refusals on this server appear in the ${lnk("Audit log", href.audit({ action: "policy.denied" }))} as <code>policy.denied</code>, and Submit’s ${lnk("Plan", href.submit())} shows violations before a run starts.</span></div>` })}
    </div>`);
}

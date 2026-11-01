// Schedules (#998): a designed "soon" page — the server has no schedules API yet.
import { html, head, tag, card, code, lnk, row, ic, v } from "../components.js";
import { layout } from "../page.js";
import { href } from "../links.js";

const planned = (i, title, text) => row(html`<span class="ri">${ic(i)}</span>`, title, "", text);

export function renderSchedules(host) {
  const { detail } = layout(host, "canvas");
  detail.innerHTML = v(html`
    ${head("Schedules", [tag("soon"), html`<span>the server has no schedules API yet</span>`], "This page will list every cron schedule the server runs, its next and last tick, and what each tick started. Until then, schedules run either as a separate process or as a schedule trigger on this server.")}
    <div class="two">
      ${card({ icon: "sched", title: "What will live here", body: html`<div class="st-ul">${planned("clock", "Next and last tick per schedule", "in the display timezone, with overlap policy and lateness")}${planned("runs", "Runs each tick started", "linked to the Runs page")}${planned("pause", "Pause and resume", "with an audit entry")}${planned("alert", "Missed ticks", "from faucet_schedule_* metrics")}</div>` })}
      ${card({ icon: "code", title: "Use it today", body: html`<div class="pad stack" style="gap:10px"><span>Run one pipeline on a cron in its own process:</span>${code('schedule:\n  cron: "*/15 * * * *"\n  timezone: Europe/Berlin\n  overlap: skip')}${code("faucet schedule pipeline.yaml", { lang: "text" })}<span>Or let this server tick it: add a <code>schedule</code> trigger, then find it on ${lnk("Triggers", href.triggers())}.</span></div>` })}
    </div>`);
}

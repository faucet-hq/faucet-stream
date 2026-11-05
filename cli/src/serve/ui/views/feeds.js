// Feeds (#1034): a designed "soon" page — feeds are designed but not built yet.
import { html, head, tag, card, code, lnk, row, ic, v } from "../components.js";
import { layout } from "../page.js";
import { href } from "../links.js";

const planned = (i, title, text) => row(html`<span class="ri">${ic(i)}</span>`, title, "", text);

export function renderFeeds(host) {
  const { detail } = layout(host, "canvas");
  detail.innerHTML = v(html`
    ${head("Feeds", [tag("soon"), html`<span>designed, not built yet</span>`], "A feed is a named, durable inbox that senders push events into over HTTP. The server stores each event before acknowledging it, and consumers turn the events into ordinary runs in batches. This page will list every feed, what it holds and how far each consumer has read.")}
    <div class="two">
      ${card({ icon: "feed", title: "What will live here", body: html`<div class="st-ul">${planned("link", "Ingest URL and credentials", "per feed, with a write-only token or HMAC secret")}${planned("layers", "Events received, pending and delivered", "with storage used against retention limits")}${planned("runs", "Consumers", "their pipeline, flush policy, cursor, lag and the runs each flush started")}${planned("alert", "Refused events", "rejected signatures, duplicates and a full inbox")}</div>` })}
      ${card({ icon: "code", title: "Use it today", body: html`<div class="pad stack" style="gap:10px"><span>For events you can't lose, have senders write files to object storage and start a run per arrival:</span>${code("triggers:\n  - name: new-events\n    type: object_arrival\n    store:\n      type: s3\n      bucket: events\n      prefix: incoming/\n    config: ./pipelines/events_to_lake.yaml")}<span>The <code>webhook</code> source collects pushed events for one bounded run only, in memory. Event triggers are on ${lnk("Triggers", href.triggers())}.</span></div>` })}
    </div>`);
}

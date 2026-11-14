---
title: Documentation
description: faucet-stream is a data-movement platform. One faucet binary moves data between sources and sinks from a YAML file, with incremental state, CDC and exactly-once delivery built in.
editUrl: https://github.com/faucet-hq/faucet-stream/edit/main/docs/site/authored/index.md
tableOfContents:
  minHeadingLevel: 2
  maxHeadingLevel: 2
---

<p class="fs-lead">faucet-stream is a data-movement platform. One <code>faucet</code> binary moves data between <b><!--COUNT:sources-->43<!--/COUNT--> sources</b> and <b><!--COUNT:sinks-->35<!--/COUNT--> sinks</b> from a YAML file, with incremental state, CDC and exactly-once delivery built in.</p>

<div class="fs-btns not-content"><a class="fs-btn fs-btn-primary" href="/faucet-stream/getting-started/installation/">Get started<svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg></a><a class="fs-btn fs-btn-ghost" href="/faucet-stream/getting-started/learn/"><svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="M22 10 12 5 2 10l10 5 10-5Z"/><path d="M6 12v5c3 3 9 3 12 0v-5"/></svg>Learn the architecture</a></div>

```sh
cargo install faucet-cli
faucet init my_pipeline --source postgres --sink bigquery
faucet run my_pipeline.yaml
```

## What do you want to do?

<div class="fs-cards not-content">
<a class="fs-card" href="/faucet-stream/getting-started/first-pipeline/"><span class="fs-card-ic"><svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="M13 2 3 14h9l-1 8 10-12h-9l1-8z"/></svg></span><span><span class="fs-card-title">Move your first data<svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg></span><span class="fs-card-text">Install faucet and run a pipeline in five minutes.</span></span></a>
<a class="fs-card" href="/faucet-stream/cookbook/state/"><span class="fs-card-ic"><svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="M3 12a9 9 0 0 1 15-6.7L21 8"/><path d="M21 3v5h-5"/><path d="M21 12a9 9 0 0 1-15 6.7L3 16"/><path d="M8 16H3v5"/></svg></span><span><span class="fs-card-title">Keep tables in sync<svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg></span><span class="fs-card-text">Incremental replication, upserts and CDC mirrors.</span></span></a>
<a class="fs-card" href="/faucet-stream/operations/deploying/"><span class="fs-card-ic"><svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><rect width="20" height="8" x="2" y="2" rx="2"/><rect width="20" height="8" x="2" y="14" rx="2"/><path d="M6 6h.01M6 18h.01"/></svg></span><span><span class="fs-card-title">Run it in production<svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg></span><span class="fs-card-text">Deploy, schedule, monitor and upgrade safely.</span></span></a>
<a class="fs-card" href="/faucet-stream/reference/connectors/"><span class="fs-card-ic"><svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 22v-5M9 8V2M15 8V2M18 8v5a4 4 0 0 1-4 4h-4a4 4 0 0 1-4-4V8Z"/></svg></span><span><span class="fs-card-title">Find a connector<svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg></span><span class="fs-card-text">Browse every source and sink and what each supports.</span></span></a>
</div>

<ul class="fs-links2 not-content">
<li><a href="/faucet-stream/cookbook/transforms/">Shape data with transforms</a></li>
<li><a href="/faucet-stream/cookbook/quality/">Check data quality</a></li>
<li><a href="/faucet-stream/cookbook/template-hub/">Use Template Hub templates</a></li>
<li><a href="/faucet-stream/tutorials/library/">Embed faucet in Rust</a></li>
<li><a href="/faucet-stream/extending/authoring-connectors/">Write your own connector</a></li>
<li><a href="/faucet-stream/comparison/">Compare with other tools</a></li>
</ul>

## Understand the foundations

Four ideas explain almost everything faucet does.

<div class="fs-dlist not-content">
<a href="/faucet-stream/getting-started/concepts/"><b>How a pipeline runs</b><span>Source, transforms and sink, streamed page by page in bounded memory.</span></a>
<a href="/faucet-stream/cookbook/state/"><b>State and resuming</b><span>Bookmarks that let every run pick up exactly where the last one stopped.</span></a>
<a href="/faucet-stream/getting-started/learn/"><b>Delivery guarantees</b><span>At-least-once by default, exactly-once where the sink can commit atomically.</span></a>
<a href="/faucet-stream/cookbook/policies/"><b>Where your data may go</b><span>Data-flow policies, PII masking and contracts, checked before a row moves.</span></a>
</div>

## Where else to look

<div class="fs-cards fs-cards-3 not-content">
<a class="fs-card fs-card-left" href="https://docs.rs/faucet-stream"><span class="fs-card-ic"><svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg></span><span><span class="fs-card-title">API docs</span><span class="fs-card-text">Every crate on docs.rs, with all features on.</span></span></a>
<a class="fs-card fs-card-left" href="https://github.com/faucet-hq/template-hub"><span class="fs-card-ic"><svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="m7.5 4.27 9 5.15"/><path d="M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z"/><path d="m3.3 7 8.7 5 8.7-5M12 22V12"/></svg></span><span><span class="fs-card-title">Template Hub</span><span class="fs-card-text">Ready-made source and sink templates.</span></span></a>
<a class="fs-card fs-card-left" href="https://github.com/faucet-hq/faucet-stream/tree/main/cli/examples"><span class="fs-card-ic"><svg class="fs-i" viewBox="0 0 24 24" aria-hidden="true"><path d="m4 17 6-6-6-6M12 19h8"/></svg></span><span><span class="fs-card-title">Runnable examples</span><span class="fs-card-text">A config for nearly every connector pair.</span></span></a>
</div>

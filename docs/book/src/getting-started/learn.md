# Learn the architecture

Two ways to understand how faucet-stream works. Pick the one that fits you — the
switch remembers your choice as you browse.

- **🎓 Beginner's guide** builds the whole system up as a story, one idea at a time.
- **🏛 Architect reference** is the condensed, subsystem-by-subsystem view for people who already have the mental model.

<div class="mode-toggle-bar" data-mode-toggle>
<div class="mode-switch" role="group" aria-label="Reading mode">
<button type="button" data-mode="beginner">🎓 Beginner's guide</button>
<button type="button" data-mode="architect">🏛 Architect reference</button>
</div>
</div>

> The buttons above switch this page in place **on the published documentation
> site**. If you're reading the raw Markdown on GitHub (which doesn't run the
> site's scripts), both sections simply appear one after the other below.

<div class="mode-content active" data-mode="beginner">

## The one-sentence idea

**faucet-stream moves data from one place to another.**

Picture a kitchen faucet: water comes *from* a pipe (the **source**), flows
through the tap, and out into the **sink**. faucet-stream is the tap — you say
where the data comes from and where it goes, and it moves the data reliably,
without losing or scrambling it.

<div class="fs-flow" data-flow="Source|faucet pipeline|Sink">Source → faucet pipeline → Sink</div>

Everything else — pages, bookmarks, retries, exactly-once — exists to keep that
one sentence true *even when things go wrong*. We'll add those ideas one at a
time.

### Chapter 1 — The two characters: Source and Sink

The whole system is built from two roles:

- A **Source** knows how to **read** records from somewhere (a database, an API, a file, a queue).
- A **Sink** knows how to **write** them somewhere else.

A **connector** is just a Source or Sink for one system (`faucet-source-postgres`,
`faucet-sink-bigquery`, …). They all speak the same two-role language, which is
why *any* source can feed *any* sink.

Records are just **JSON**. A database row, an API response, a file line — they all
become plain JSON objects flowing through the pipe. At its simplest, a Source is
one function ("give me your records") and a Sink is one function ("here are
records, write them"). That's a working connector; everything else is optional.

### Chapter 2 — Moving data once

Connect a Source to a Sink and you have a **pipeline**: read everything, write
everything.

<div class="fs-flow" data-flow="source.fetch::read all|sink.write|done::wrote N records">source.fetch → sink.write → done</div>

For a one-time copy, this is all you need. Two real-world problems push us
further: you don't want to re-copy everything every run (Chapter 3), and your
data might be too big for memory (Chapter 4).

### Chapter 3 — Only the new stuff (incremental)

To avoid re-reading everything each run, the Source leaves itself a note —
a **bookmark** — saying *"I got up to here"* (a timestamp, a log position, an
offset). Next run it resumes from that note instead of the beginning.

Here's the single most important rule in the whole project, and it's just common
sense:

> **The bookmark is saved only *after* the data is safely written.**

If we saved "got to row 1000" *first* and then crashed before writing those rows,
they'd be lost forever. So the order is always **write → make sure it's really
saved → then save the bookmark**. Crash in between, and the worst case is redoing
a little work (safe) — never skipping data (catastrophic). Keep this rule in your
pocket; every advanced feature respects it.

### Chapter 4 — Bigger than memory (streaming)

Reading a billion rows into memory won't work. So instead of "all the data," the
Source produces a stream of **pages** — chunks of, say, 1,000 records at a time —
and the pipeline handles one page at a time:

<div class="fs-flow" data-flow="page 1|write|page 2|write|page 3::+ bookmark|write|flush|save bookmark">page 1 → write → page 2 → write → page 3 (+ bookmark) → write → flush → save bookmark</div>

Only one page is ever in memory, so a thousand rows or a billion, memory stays
flat. The bookmark rides along on the pages, and it's still saved *after* the
page is safely written — Chapter 3's rule, now per-page.

### Chapter 5 — The production toolbox (reach for these when you need them)

You now understand the **spine**: a source streams pages, the pipeline writes each
page and checkpoints safely, so you can resume after a crash. Everything below is
**optional** — a toolbox you pull from the day you hit the problem a tool solves.
Find your situation, then follow the tool to its how-to. The family almost every
real pipeline reaches for — shaping the data — comes first.

**Shaping the data**

| The situation you're in | The tool you reach for |
|---|---|
| The data isn't in the shape the destination wants | [Transforms](../cookbook/transforms.md) |
| You need joins, aggregates, or real query power | [SQL transform](../cookbook/sql-transform.md) |

**Guarding the data**

| The situation you're in | The tool you reach for |
|---|---|
| Some incoming rows are garbage (nulls, out-of-range) | [Quality checks](../cookbook/quality.md) |
| Downstream must never get a surprise shape | [Contracts](../cookbook/contracts.md) |
| The data has PII you must never leak | [Masking](../cookbook/masking.md) |
| The incoming shape drifts from the destination's | [Schema drift](../cookbook/schema-drift.md) |

**Moving it reliably**

| The situation you're in | The tool you reach for |
|---|---|
| A few bad rows keep killing the whole run | [Dead-letter queue](../cookbook/dlq.md) |
| The network or endpoint is flaky | [Retries & resilience](../cookbook/resilience.md) |
| You must never write a row twice, even after a crash | [Exactly-once](../cookbook/state.md) |
| You need a destination table kept mirrored (upserts, deletes) | [Upsert / write modes](../cookbook/upsert.md) |

**Getting data in and out at scale**

| The situation you're in | The tool you reach for |
|---|---|
| One source is too big for a single worker | [Sharding](../cookbook/cluster.md) |
| Bootstrap a table, then follow its changes with no gap | [Replication](../cookbook/replication.md) |
| Replay a bounded slice of history | [Backfill](../cookbook/backfill.md) |
| Auto-generate configs from a live catalog | [Discovery](../cookbook/discover.md) |
| Read or write compressed files | [Compression](../cookbook/compression.md) |

**Running & operating it**

| The situation you're in | The tool you reach for |
|---|---|
| Run on a cron schedule | [Scheduling](../cookbook/scheduling.md) |
| Run as a long-lived HTTP service | [Serve](../cookbook/serve.md) |
| Spread runs across many machines | [Cluster](../cookbook/cluster.md) |
| Start runs on events (a file lands, a webhook, a queue fills) | [Triggers](../cookbook/triggers.md) |
| Turn one config into many pipelines (a DAG) | [Matrix & composition](../cookbook/composition.md) |
| Pull credentials from a secrets manager | [Secrets](../cookbook/secrets.md) |

**Seeing what happened**

| The situation you're in | The tool you reach for |
|---|---|
| Get metrics and traces | [Observability](../operations/observability.md) |
| See where data came from and went | [Lineage](../cookbook/lineage.md) |
| Alert when data goes stale or volume looks wrong | [SLA monitoring](../cookbook/sla.md) |
| Browse every dataset your pipelines have touched | [Data Movement Catalog](../cookbook/catalog.md) |
| Get paged (Slack / PagerDuty) when something breaks | [Notifications](../cookbook/notifications.md) |

When several of the *data-guarding* tools are on, each page runs them in a fixed,
*safe* order — mask first (so PII can't leak), then validate (so bad data never
lands), then write, then save the bookmark last:

<div class="fs-flow" data-flow="page|mask|quality|contract|drift|write|flush|save bookmark">page → mask → quality → contract → drift → write → flush → save bookmark</div>

The golden rule never bends, no matter how many tools you add.

### The one rule that ties it all together

> **A bookmark is saved only after the sink has durably written and flushed the
> page.** Write → flush → checkpoint. Always.

Every failure mode, retry, and exactly-once guarantee is a consequence of that
one ordering.

### Where to go next

- **Run a real pipeline:** [Your first pipeline](./first-pipeline.md).
- **The concepts, precisely:** [Core concepts](./concepts.md).
- **The full story with diagrams and code:** the [beginner guide on GitHub](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/learn.md).
- **Flip this page to 🏛 Architect reference** for the condensed deep view.

</div>

<div class="mode-content" data-mode="architect">

## Architecture at a glance

faucet-core is a **lean library**: it knows how to move one source to one sink and
checkpoint safely. All orchestration (matrix DAGs, scheduling, the HTTP control
plane, clustering) is CLI-layer code built on top. The full reference lives in the
repository under [`docs/architecture/`](https://github.com/faucet-hq/faucet-stream/tree/main/docs/architecture); this is the condensed view.

### How a run is assembled

<div class="fs-flow" data-flow="config|compose|interpolate|secrets|parse|expand|executor|Pipeline|run_stream">config → compose → interpolate → secrets → parse → expand → executor → Pipeline → run_stream</div>

`expand` is where a config becomes runnable and where the **load-time gates** run
(exactly-once, write-mode × sink, quarantine-requires-DLQ) — an impossible
topology fails `faucet validate` before any record moves. Deep dive:
[execution model](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/execution.md).

### The pipeline loop

`run_stream` consumes one `StreamPage { records, bookmark }` at a time and, per
page, runs the fixed-order passes then one of three write paths:

<div class="fs-flow" data-flow="page|mask|quality|contract|drift|write path|flush|checkpoint">page → mask → quality → contract → drift → write path → flush → checkpoint</div>

- **Default (at-least-once):** `write_batch` → flush → persist bookmark.
- **Exactly-once (atomic watermark):** `write_batch_idempotent(scope, token)` → flush → persist `(bookmark, seq)`; a replayed token-stamped write is a no-op.
- **DLQ:** `write_batch_partial` routes per-row failures aside → flush → persist.

Deep dive:
[pipeline engine](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/pipeline.md),
[stream pages](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/stream-pages.md).

### The load-bearing invariant

> A page's bookmark is persisted **only after** the sink has durably written and
> flushed that page. Write → flush → checkpoint, in all three paths.

The state store is therefore never ahead of the sink, so recovery can only ever
replay attempted work — never skip it. Deep dive:
[design invariants](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/invariants.md),
[recovery](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/recovery.md).

### Delivery guarantees

| Guarantee | Requires | On the crash window |
|---|---|---|
| **At-least-once** (default) | nothing | replays the page — may duplicate |
| **Effectively-once / atomic-watermark** | idempotent sink + deterministic-replay source + durable state + no DLQ | skips or re-anchors — no duplication |
| **Effectively-once / keyed-upsert** | upsert-capable sink + `write_mode: upsert\|delete` + `key` | re-upsert is a no-op — no duplication |

### Retry safety

A non-idempotent `write_batch` is retried **only** when the sink advertises
idempotence — otherwise a lost response could silently duplicate every row. Deep
dive:
[retries](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/retries.md),
[resilience](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/resilience.md).

### The subsystems

| Area | Reference |
|---|---|
| Connector SDK (`Source`/`Sink` traits) | [connector-sdk](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/connector-sdk.md) |
| State & bookmarks | [state-management](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/state-management.md) |
| Batching & adaptive control | [batching](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/batching.md) |
| Schema / quality / contracts / masking | [schema](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/schema.md) |
| Observability | [observability](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/observability.md) |
| Security model | [security](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/security.md) |
| Performance & extensibility | [performance](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/performance.md) · [extensibility](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/extensibility.md) |

Decision history lives in the
[ADRs](https://github.com/faucet-hq/faucet-stream/tree/main/docs/adr); proposals
in the [RFCs](https://github.com/faucet-hq/faucet-stream/tree/main/rfcs).

**Flip this page to 🎓 Beginner's guide** if you'd like the same story from zero.

</div>

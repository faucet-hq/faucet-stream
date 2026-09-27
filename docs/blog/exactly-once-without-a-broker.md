# Exactly-once delivery without a broker

*An engineering deep-dive into how faucet-stream delivers effectively-once
without Kafka transactions, a two-phase commit coordinator, or an external
dedup store — just a commit token that rides along with your data.*

> Grounded in [`cli/examples/kafka_to_postgres_exactly_once.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/kafka_to_postgres_exactly_once.yaml).
> Reflects faucet-stream as of 2026-07.

## The problem

Move data from A to B, and sooner or later a run dies halfway — a network blip, a
deploy, an OOM. When it restarts, you get one of two bad outcomes:

- **At-most-once**: you advanced your read position *before* the write landed, so
  the crash ate the in-flight batch. Data lost.
- **At-least-once**: you advanced your read position *after* the write, but the
  crash happened in between, so the restart re-reads and re-writes the batch.
  Duplicates.

The usual fix is heavy: Kafka's transactional producer, an external two-phase
commit, or a dedup table keyed on every record. All of them add a moving part
you now have to operate.

faucet-stream takes a smaller path. The insight: **the only durable fact that
matters is "which records has the destination already accepted?" — so store that
fact *in the destination*, atomically with the data itself.**

## The setup

Exactly-once in faucet composes three things, and validation rejects the config
if any is missing:

1. A **positional-replay source** — one whose stream position is an immutable,
   re-readable coordinate. Kafka (partition offsets), Postgres/MySQL/MongoDB CDC
   (log positions). You can ask it "give me everything after position X" and get
   a deterministic answer.
2. An **idempotent sink** — one that can commit rows *and* a watermark in a
   single atomic unit. Eleven sinks qualify today (Postgres, MySQL, SQLite,
   MSSQL, BigQuery, Snowflake, Spanner, MongoDB, Redis, Kafka, Iceberg).
3. A **durable state store** — file, Redis, or Postgres. `memory` is rejected.

You ask for it with one line:

```yaml
delivery: exactly_once
```

`faucet validate` then reports the guarantee it derived, per row:

```
delivery=effectively-once (atomic watermark)
```

Note the honest word: **effectively-once**, not a physics-defying "exactly-once."
No record is duplicated and none is skipped from the *consumer's* perspective —
which is what people actually mean when they say exactly-once.

## The mechanism: a commit token in the destination

Here's the whole trick. Each page faucet reads from the source carries a
**complete bookmark** — for Kafka, the next offset for every partition in the
page. When the sink writes that page, it does **not** just insert the rows. It
inserts the rows **and** a monotonic **commit token** — which embeds that
bookmark — inside **one transaction**.

For the Postgres sink, that token lives in a `_faucet_commit_token` watermark
table. The transaction looks like:

```sql
BEGIN;
  INSERT INTO orders_events (...) VALUES ...;         -- the page's rows
  UPDATE _faucet_commit_token SET token = <bookmark>; -- the watermark
COMMIT;
```

Because both statements are in the same transaction, they are **all-or-nothing**.
There is no window where the rows exist but the watermark doesn't, or vice versa.
The destination's own ACID guarantee is doing the coordination — no external
coordinator required.

## What happens on a crash

Walk the dangerous window: the sink has `COMMIT`ted, but the process dies
*before* faucet persists the bookmark to the state store. Naively, the restart
would re-read from the last state-store position and duplicate the page.

faucet doesn't trust the state store as the source of truth here. On resume, it
reads the **commit token back out of the sink's watermark table**, and
re-anchors the source consumer to *that* position — not the (possibly stale)
state-store bookmark. The sink knows the truth about what it accepted, because
the truth was committed atomically with the data.

So:

- Rows committed, watermark committed, state store stale → resume from the
  **watermark**, skip what's already there. No duplicates.
- Transaction never committed → the rows were rolled back too; resume re-reads
  and writes them for the first time. No loss.

This works **even though page boundaries can differ on replay.** Kafka might
hand you differently-sized pages the second time; doesn't matter, because the
watermark is a stream *position*, not a page identity. faucet skips everything at
or before the recovered position regardless of how it's re-chunked.

## Why not just use Kafka transactions?

You can — the Kafka *sink* uses a transactional producer for its effectively-once
path. But the watermark approach generalizes to sinks that have **no** notion of
producer transactions: Postgres, BigQuery, Spanner, MongoDB. As long as the sink
can commit "data + one extra row" atomically, it gets effectively-once for free,
with the same mental model across all eleven. One pattern, not eleven
special-cases.

It also means **no dedup table keyed on every record** — the watermark is O(1)
per page, not O(rows). You're storing one position, not a set of every id you've
ever seen.

## The keyed-upsert alternative

Positional replay isn't the only route. If your source isn't a log but your sink
is upsert-capable, you get idempotency a different way: write with
`write_mode: upsert` keyed on the primary key. A replayed row overwrites itself
instead of duplicating. That's a weaker guarantee (it needs a stable key and
last-write-wins semantics) but it covers non-log sources into any of the eight
upsert-capable sinks. See the [upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html).

## Try it

```bash
export DEST_PG_URL=postgres://faucet:faucet@localhost:5432/warehouse
faucet run cli/examples/kafka_to_postgres_exactly_once.yaml
```

Kill it mid-run. Restart it. Count the rows. They'll be right.

---

*faucet-stream is an MIT/Apache-2.0 Rust library + CLI for moving data between
<!--COUNT:sources-->42<!--/COUNT--> sources and <!--COUNT:sinks-->33<!--/COUNT--> sinks. [Docs](https://faucet-hq.github.io/faucet-stream/) ·
[GitHub](https://github.com/faucet-hq/faucet-stream).*

# Parallel range partitioning

A source that accepts a range filter — `?id_from=&id_to=`, a SQL `WHERE`, an
offset/limit pair, a dated object prefix — is trivially parallelizable. Serial
pagination leaves most of the available bandwidth unused.

`partition:` splits the range into chunks and runs each as an independent
invocation:

```yaml
execution:
  max_concurrent: 8          # chunks share this budget with every other row

partition:
  kind: integer
  from: 0
  to: 1000000
  chunk_size: 10000
  bounds: inclusive          # required — see below

pipeline:
  source:
    type: rest
    config:
      path: "/records?id_from=${partition.start}&id_to=${partition.end}"
  sink:
    type: file
    config:
      path: "./out/records-${partition.id}.jsonl"
```

## It works with any source

Substitution walks the **string leaves** of a connector config, so no connector
knows this feature exists:

```yaml
# REST
path: "/records?id_from=${partition.start}&id_to=${partition.end}"
# Postgres / MySQL / MSSQL / SQLite / BigQuery / Snowflake / Spanner
query: "SELECT * FROM orders WHERE id >= ${partition.start} AND id <= ${partition.end}"
# S3 / GCS
prefix: "dt=${partition.start_date}/"
# MongoDB
filter: '{"seq": {"$gte": ${partition.start}, "$lte": ${partition.end}}}'
```

## `bounds` — the one field with no default

With `chunk_size: 10000` from 0:

| `bounds` | chunk 1 | chunk 2 | emitted `end` |
|---|---|---|---|
| `inclusive` | `[0, 9999]` | `[10000, 19999]` | `9999` |
| `half_open` | `[0, 10000)` | `[10000, 20000)` | `10000` |

It has to match how *your source* reads the upper bound:

- **half-open chunks against an inclusive source** → record 10000 is fetched
  **twice**, once per adjacent chunk.
- **inclusive chunks against an exclusive source** → record 9999 is **never
  fetched**.

Neither raises an error, and both scale with the chunk count. A guessed default
would be a coin flip on silent data loss, so you state which your source is.

## Kinds

### `integer` — id ranges

```yaml
partition: { kind: integer, from: 0, to: 1000000, chunk_size: 10000, bounds: inclusive }
```

Tokens: `${partition.start}`, `${partition.end}`, `${partition.index}`,
`${partition.id}`.

### `timestamp` — time windows

```yaml
partition: { kind: timestamp, from: 2026-01-01, to: 2026-08-01, chunk_size: 1d, timezone: UTC }
```

The same DST-correct windowing [`faucet backfill`](./backfill.md) uses — `1d` is a
*calendar* day, so it differs from `24h` across a transition, deliberately.
Windows are always half-open.

Tokens: `start`, `end`, `start_date`, `end_date`, `start_unix`, `end_unix`,
`index`, `id`.

### `offset` — countable result sets

```yaml
partition: { kind: offset, total: 1234567, chunk_size: 10000 }
```

The parallel form of what serial offset pagination already does. Tokens:
`${partition.offset}`, `${partition.limit}`, `index`, `id`.

> **A count is not a maximum id.** `total` chunks by *position*; `to` chunks by
> *key*. They are on different kinds precisely so the two cannot be mixed up: a
> count equals the largest id only when ids are dense and 1-based, so chunking an
> id range from a count stops early the moment ids are sparse — deletions,
> sharded id allocation, non-sequential keys — and every record above it is never
> fetched.

## Discovering the bound

Often you know where a range starts but not where it ends. `to` (and `total`) can
be **discovered** by running any source once:

```yaml
partition:
  kind: integer
  from: 0
  chunk_size: 10000
  bounds: inclusive
  to:
    from_source:
      type: postgres
      config:
        connection_url: "${env:PG_URL}"
        query: "SELECT MAX(id) AS max_id FROM orders"
    value_path: "$.max_id"
```

The probe is an ordinary source config, so a REST "last record" request works the
same way:

```yaml
  to:
    from_source:
      type: rest
      config:
        path: "/records?sort=-id&limit=1"
        records_path: "$.data"
    value_path: "$.id"
```

**`to_unbounded` defaults on when the bound is discovered.** A probed maximum is
stale the instant it returns, so the final chunk drops its upper bound and reads
whatever arrived since: its `${partition.end}` renders as the largest integer
(`9223372036854775807`), so `id <= ${partition.end}` and `id <
${partition.end}` both admit every later row. Set it explicitly to `false` for a
range you know is closed.

With `bounds: half_open` a probed maximum is planned as `to = MAX + 1`, so the
row holding the maximum is read (and a range whose `from` equals the maximum is
one row, not empty).

A probe that returns no rows, `null`, or a non-numeric value is an **error**, not
a zero — `MAX(id)` over an empty table returns `NULL`, and treating that as 0
would plan one degenerate chunk and read nothing.

`faucet validate` is offline, so it reports that a row discovers its bound at run
time rather than pretending to have planned the chunks (the rest of the config is
checked against a one-chunk placeholder; `explain`, `policy`, `test` and template
registration plan the same way). Every command that runs or inspects the real
rows — `run`, `schedule` (each tick), `serve`, `preview`, `doctor`, `plan`,
`backfill`, `verify`, `rollback`, `dlq replay`, `profiling` — runs the probe
first, so they all plan the chunks `faucet run` would.

## What partitioning does not change

- **Concurrency.** Chunks are ordinary sibling rows and share the single
  `execution.max_concurrent` semaphore. They do not get a private pool.
- **State.** Each chunk gets its own state key (`{name}::{row}::partition::{id}`),
  so bookmarks never collide and a resumed run picks up per chunk.
- **Failure handling.** `execution.on_error` applies as usual: a failed chunk is
  reported and, under the default `continue`, its siblings still finish.

## Limits

- A partitioned row **cannot be referenced** by another row's `parent:` or
  `depends_on:`. It expands into one node per chunk, so there is no single node to
  attach to — this is rejected at load time rather than silently dropping the
  edge.
- A `partition:` block whose source references no `${partition.*}` token is
  rejected: every chunk would run the identical query.
- The chunk count is capped (10,000). A tiny `chunk_size` over a huge range is a
  config error, not a workload.

## Relationship to `shard:` and `backfill`

| | `shard: { count }` | `partition:` | `faucet backfill` |
|---|---|---|---|
| Splits by | target **count** | **chunk size** | window size |
| Bounds from | the connector (`MIN`/`MAX`, hash) | config, or a probe | the command line |
| Sources | the 7 implementing `Shardable` | **any** | any |
| Runs | across cluster workers | within one run | as a bounded replay |

They compose conceptually but are separate tools: `shard:` spreads one huge table
across workers, `partition:` widens a single run, and `backfill` replays history.
`partition:` and `backfill` share the same windowing code, so their time
boundaries are identical by construction.

# Incremental replication & state

For pipelines that run repeatedly, you usually want to fetch only what's new.
That requires two things: an **incremental replication method** on the source and
a **state store** to persist the bookmark between runs.

## Replication methods

- `FullTable` — fetch everything every run.
- `Incremental` — track a high-water mark on a `replication_key` (e.g. `updated_at`,
  an auto-increment id) and only emit records past the last seen value.

```yaml
source:
  type: rest
  config:
    # …
    replication_method:
      type: Incremental
    replication_key: updated_at
```

### Nested replication keys

Many APIs put the cursor inside a nested object (`fields.updated`,
`commit.committer.date`, `segments.date`). `replication_key` accepts
three forms:

| Form | Example | Resolves |
|------|---------|----------|
| Field name | `updated_at` | a top-level field |
| Dot path | `fields.updated`, `items.0.date` | nested objects; numeric segments index arrays. A top-level field literally named `fields.updated` still wins, so flat CSV columns such as `Account.LastModifiedDate` keep working |
| JSON Pointer | `/fields/updated`, `/a.b/c` | RFC 6901, for field names that themselves contain dots |

The key must resolve to a single value; it is not a JSONPath. It is resolved on
the raw records, before transforms run, so no `flatten` is needed.

### How cursor values compare

Values are compared by what they hold, not as raw text:

- Numbers compare numerically, integers exactly beyond 2^53.
- Decimal strings (`"99"`, `"1234.50"`, as DECIMAL columns often arrive)
  compare as exact decimals, so `"100"` is newer than `"99"`. A number and a
  decimal string compare numerically too.
- Timestamp strings compare as instants: RFC 3339 values with different
  offsets or fraction widths are converted to UTC first; values without an
  offset compare as local date-times against each other.
- Any other string compares lexicographically, so a date-only cursor
  (`2024-06-01`) orders correctly as long as it is fixed width.

### Pushing the bookmark into a nested request body

A `replication_bind` with `into: body` writes the bookmark into the JSON request
body — as a top-level field (`name`) or at any location addressed by a JSON
Pointer (`path`):

```yaml
replication_bind:
  into: body
  path: /filterGroups/0/filters/0/value   # CRM search filter
  format: epoch_ms
  value_type: number                      # write 1717200000000, not "1717200000000"
```

The pointer must resolve to an existing scalar (or `null`) in the configured
`body`, or to a new key of an existing object; array elements are never
created. `window` binds accept the same `path` / `value_type`, so report-API
`dateRanges[0].startDate` / `endDate` can be windowed. Set exactly one of
`name` or `path`, configure a JSON object `body`, and don't point two binds at
the same location — all three are checked at load time.

### Incremental GraphQL

GraphQL APIs are synced incrementally two ways.

**The `graphql` source** takes the same `replication_method` /
`replication_key` / `on_missing_key` / `start_replication_value` / `state_key`
fields as `rest`, and its `replication_bind` names a GraphQL **variable** — a
top-level name, or a JSON Pointer into `variables`:

```yaml
source:
  type: graphql
  config:
    query: |
      query($after: String, $first: Int, $query: String) {
        orders(first: $first, after: $after, query: $query) {
          edges { node { id updatedAt } } pageInfo { hasNextPage endCursor }
        }
      }
    variables: { query: "updated_at:>2020-01-01T00:00:00Z" }   # first run
    records_path: "$.data.orders.edges[*].node"
    pagination:
      has_next_page_path: "$.data.orders.pageInfo.hasNextPage"
      cursor_path: "$.data.orders.pageInfo.endCursor"
    replication_method: { type: Incremental }
    replication_key: updatedAt                # dot path / JSON Pointer also accepted
    replication_bind:
      variable: query                         # or /filter/updatedAt/gte
      template: "updated_at:>${bookmark}"
      format: iso8601
```

The bookmark is written only after the last cursor page (a crash mid-run
re-reads rather than skipping rows), the client-side filter stays on as a
safety net, and a GraphQL `errors[]` response fails the run before any
bookmark is stored. When the bookmark lands inside a query-language string, as
above, escaping it is the template's job.

**GraphQL through the `rest` source** pages too: `CursorInBody.body_cursor_field`
(and `OffsetInBody`'s `offset_field` / `limit_field`) accept a JSON Pointer, so
the cursor lands in `variables`:

```yaml
source:
  type: rest
  config:
    method: POST
    body: { query: "query($after: String) { … }", variables: {} }
    records_path: "$.data.orders.edges[*].node"
    pagination:
      type: CursorInBody
      next_token_path: "$.data.orders.pageInfo.endCursor"
      body_cursor_field: /variables/after
```

Combine it with a `replication_bind` of `into: body, path: /variables/since`
for incremental reads.

### Incremental SQL queries (postgres, mysql)

The `postgres` and `mysql` query sources take a `replication:` block instead
of `replication_method` / `replication_key`, with the same shape as `mssql`:

```yaml
source:
  type: postgres
  config:
    connection_url: "postgres://faucet:faucet@localhost:5432/app"
    query: "SELECT id, status, total, updated_at FROM public.orders"
    replication:
      type: incremental
      column: updated_at                    # an output column of the query
      initial_value: "1970-01-01T00:00:00Z" # inclusive, first run only
state:
  type: file
  config: { path: ./.faucet-state }
```

The source runs `SELECT * FROM (<query>) AS q WHERE q.updated_at >= <bookmark>
ORDER BY q.updated_at`, so the server filters and orders by the cursor. Write
the token — `${bookmark}` for postgres, `@bookmark` for mysql — inside the
query with `>=` to apply the cursor in a CTE or join as well; a strict `>` is
refused when the config loads.

**Ties are never skipped.** The stored bookmark is the last cursor value read
plus a fingerprint of every row already written at that value
(`{"value": "...", "boundary": ["..."]}`). The next run re-reads that value and
drops only those rows, so a row that committed late with the same `updated_at`
is written, and so is a boundary row updated in place. Above 10,000 rows at one
value the fingerprints are dropped and the next run re-reads the value whole
(duplicates, not loss).

**Every page is checkpointed.** Each page carries the bookmark after its last
row, persisted only after the sink has written the page, so a crash replays at
most one page. Rows with a `NULL` cursor are never read, and a row committed
with a cursor below the stored bookmark is not seen — use the CDC source when
that matters. `faucet state show|set|reset` work on these bookmarks like any
other. Example: `cli/examples/postgres_incremental_to_jsonl.yaml`.

### Records without the key

A record whose key is missing or `null` is **kept** by default — dropping it
would be silent data loss. Each one is counted in
`faucet_source_replication_key_missing_total{pipeline,row,connector}` and the
source logs one warning per run. Choose explicitly with `on_missing_key`:

| Value | Behaviour |
|-------|-----------|
| `keep` (default) | Write the record; counted and warned |
| `drop` | Skip the record; counted and warned |
| `fail` | Fail the run |

When every record on a run lacks the key, the bookmark does not advance (and the
warning fires), so a misspelled key shows up immediately instead of as an empty
incremental run.

## State stores

Attach a `state:` block so the bookmark survives between runs:

```yaml
state:
  type: file          # built into faucet-core
  config:
    path: ./state
```

Available backends:

| Backend | Crate | Use when |
|---------|-------|----------|
| `memory` | `faucet-core` | tests, one-shot runs (not persistent) |
| `file` | `faucet-core` | single host; one JSON file per key, atomic writes |
| `redis` | `faucet-state-redis` | shared/ephemeral state across hosts |
| `postgres` | `faucet-state-postgres` | shared, durable, transactional state |

```yaml
# Redis
state:
  type: redis
  config:
    url: redis://localhost:6379
    namespace: faucet

# Postgres
state:
  type: postgres
  config:
    url: postgres://user:pass@localhost/faucet
    table: faucet_state     # optional, default `faucet_state`
    ensure_table: true      # optional, run CREATE TABLE IF NOT EXISTS on startup
    max_connections: 10     # optional, default 5 — pool size for the state store
```

`max_connections` sizes the Postgres state-store connection pool (default `5`).
Raise it when many concurrent matrix rows share one state store; lower it
against a connection-limited managed Postgres. A value of `0` is rejected at
config-load time.

### Encryption at rest (`file` backend)

Bookmarks can embed source positions and key values. On a shared or
compliance-scoped host, seal the `file` backend's bookmark files with
AES-256-GCM (requires a build with the `encryption` feature — included in
`--features full`):

```yaml
state:
  type: file
  config:
    path: ./state
    encryption:
      key: ${vault:secret/faucet#state-key}   # or ${env:FAUCET_STATE_KEY}
      # previous_keys: ["${env:OLD_KEY}"]     # rotation: read-only candidates
      # algorithm: aes-256-gcm                # default (and only) option
```

- **Key handling** — the 32-byte AES key is derived as SHA-256 of the key
  string. That is a derivation, not a stretching KDF: use high-entropy
  material from a secrets manager, not a human password. The `state:` block
  is covered by the secrets pass, so `${vault:…}` / `${aws-sm:…}` keys work
  and are redacted from faucet's logs.
- **Rotation** — move the old key into `previous_keys` and set the new `key`:
  old files stay readable and every write re-seals with the new key.
- **Backward compatible** — plaintext bookmarks written before encryption was
  enabled remain readable and are sealed on their next write.
- **Bound to its key** — each sealed file carries its state key as
  authenticated data, so a file copied over another key's file fails to
  decrypt instead of restoring the wrong position. Files sealed by earlier
  releases stay readable and are re-sealed this way on their next write (an
  older faucet cannot read the new format).
- **Failure behavior** — a wrong/rotated-away key or a tampered file is a
  *typed error*, never a silent "no bookmark" (which would trigger a full
  re-sync); an encrypted file read by a store with no `encryption` block
  errors with instructions rather than parsing garbage. The atomic
  temp-file + fsync + rename write path is unchanged.

For the Redis / Postgres backends, rely on the backend's own at-rest
encryption. To seal a **file-backed DLQ** the same way, see
[Dead-letter queues](./dlq.md#encryption-at-rest).

## How bookmarks advance

The pipeline reads the bookmark before fetching, and persists a new one **only
after the sink confirms** the page. Most sources emit a bookmark on the final
page; CDC-style sources emit one per committed transaction and get
per-transaction durability automatically. Either way, a crash can never advance
the bookmark past data that wasn't written — the next run re-fetches from the
last confirmed point.

## State keys

Each invocation has a state key so concurrent matrix rows don't collide:
`{name}::{row_id}` for roots and `{name}::{row_id}::{parent_record_key}` for DAG
children. The CDC source uses `postgres-cdc:<slot>`. Because the key is built
from the pipeline `name` and the row `id`, renaming either starts the row from
an empty bookmark; read the old one with `faucet state show` and set it on the
new row with `faucet state set`.

To inspect, move, reset, back up or migrate these keys — and to see each row's
health at a glance — use `faucet state` and `faucet status`; see
[Pipeline state & status](state-and-status.md).

## Effectively-once delivery

> **What the guarantee is — and is not.** faucet provides **effectively-once**
> delivery: each record is *observably applied* exactly once. This is
> **idempotent at-least-once** — it is **not** distributed-consensus
> exactly-once (there is no cross-system two-phase commit or consensus
> protocol). The config key is spelled `delivery: exactly_once` for the mode,
> but the honest description of the resulting guarantee is *effectively-once*.
>
> Two **mechanisms** can provide it, and `faucet validate` reports which one a
> pipeline actually gets (`delivery=effectively-once (atomic watermark)` /
> `(keyed upsert)` on each row line):
>
> 1. **Atomic watermark** — the sink commits each page's records *and* a
>    monotonic commit token in one transaction (SQL sinks, Iceberg, BigQuery,
>    Kafka, Snowflake, Redis, MongoDB), paired with a source that resumes
>    positionally from a per-page bookmark (CDC, Kafka).
> 2. **Keyed upsert** — the sink is configured with `write_mode: upsert` (or
>    `delete`) and a `key`, so re-applying a record converges on the same keyed
>    row instead of duplicating. Works with **any** source.
>
> **Failure-mode boundary (atomic watermark).** The atomicity is
> per-sink-transaction: the records and the commit token commit together or not
> at all. The committed token also **embeds the page's resume bookmark**, so if
> the process crashes *after* the sink transaction commits but *before* the
> state store persists, the next run recovers the exact stream position from
> the sink's watermark and **re-anchors the source there** — nothing is
> re-written and nothing is skipped, even for sources (like Kafka) whose page
> *boundaries* differ on replay. Pre-existing watermarks written before
> bookmarks were embedded fall back to count-based skip-on-resume.

### The at-least-once crash window

By default (`delivery: at_least_once`) the pipeline persists the bookmark
*after* the sink confirms the write. A crash in the small window between
"sink durably wrote the page" and "state store persisted the bookmark" causes the
page to be re-delivered on the next run. For most workloads, duplicates in the
destination can be handled by upsert logic or deduplication downstream.

For CDC pipelines landing into SQL databases or Iceberg, faucet can close that
window entirely.

### How effectively-once closes the gap

When `delivery: exactly_once`, the pipeline issues a monotonic **commit token** for
every bookmark-carrying page. Instead of a plain `write_batch`, it calls
`write_batch_idempotent(records, scope, token)`. The sink commits both the
records and the token atomically inside its own transaction:

- **SQL sinks** (postgres, mysql, mssql, sqlite) — an in-transaction `UPSERT`
  into a `_faucet_commit_token(scope TEXT, token TEXT)` watermark table.
- **Iceberg sink** — the token is written as snapshot summary properties
  `faucet.commit-scope` and `faucet.commit-token` on the committed snapshot.
- **BigQuery sink** — the rows and the token are written in one BigQuery
  multi-statement transaction (a typed `INSERT … SELECT FROM
  UNNEST(JSON_QUERY_ARRAY(@payload))` plus a `MERGE` into the
  `_faucet_commit_token` watermark table in the target dataset), so both land
  atomically.
- **Kafka sink** — a transactional producer writes each page's records plus a
  commit-token record into a compacted side-topic (default
  `__faucet_commit_token`, auto-created with `cleanup.policy=compact`) inside one
  Kafka transaction, so the data and the watermark commit atomically. The
  `transactional.id` is auto-derived from the pipeline scope. Downstream
  consumers should read the destination with `isolation.level=read_committed`.

- **Snowflake sink** — one multi-statement SQL API request
  (`BEGIN; INSERT …; MERGE INTO _faucet_commit_token …; COMMIT;`) commits the
  page and the watermark in a single Snowflake transaction.
- **Redis sink** — one `MULTI`/`EXEC` transaction appends the page's commands
  plus a `SET _faucet_commit_token:<scope> <token>`.
- **Cloud Spanner sink** — one read-write transaction buffers the page's
  mutations plus an `InsertOrUpdate` on the `faucet_commit_token` table (no
  leading underscore — Spanner identifiers must start with a letter), so
  data and watermark commit atomically (the client retries `ABORTED` commits
  automatically).
- **MongoDB sink** — one multi-document transaction (replica set required)
  commits the page plus a `{_id: scope, token}` watermark document in the
  `_faucet_commit_token` collection.
- **Oracle sink** — the page's array DML and a `MERGE` of `(scope, token)` into
  `_faucet_commit_token` (created beside the target) commit in one transaction.
- **Databricks sink** — Databricks SQL has no multi-table transaction, so the
  page write is made idempotent per token instead: an append page is written
  with one atomic `INSERT … REPLACE WHERE _faucet_scope = … AND _faucet_seq >= …`
  (an upsert page with its keyed `MERGE`), then the watermark row is `MERGE`d
  into `_faucet_commit_token`. A crash between the two replays the page under
  the same sequence, which replaces the earlier attempt's rows.

On the *next run*, the pipeline reads the sink's `last_committed_token` for the
current scope. The token **embeds the committed page's bookmark**: when the
sink is ahead of the state store (the crash window), the pipeline re-anchors
the source at that exact position and continues — no page is re-written and no
record is skipped. For tokens written before bookmarks were embedded, the
count-based path applies: a page whose token is ≤ the stored token is already
durably committed, so the pipeline **skips the write** and advances the state
store. Zero duplicates result from a crash at any point in the sequence.

### Supported sources and sinks

Only certain connectors are allowed in an effectively-once (`delivery: exactly_once`) pipeline:

| Role | Allowed connectors | Why others are excluded |
|------|--------------------|------------------------|
| Source | `postgres-cdc`, `mysql-cdc`, `mssql-cdc`, `mongodb-cdc`, `oracle-cdc`, `kafka` | The source must emit a complete resume position (bookmark) on every page, over an immutable log, so resuming from a bookmark continues the record stream at exactly that position. Query-based sources (REST, SQL query, etc.) can return different data on replay — the pipeline would silently skip records it never wrote. |
| Sink | `sqlite`, `postgres`, `mysql`, `mssql`, `iceberg`, `bigquery`, `kafka`, `snowflake`, `redis`, `mongodb`, `spanner`, `databricks`, `oracle` | The sink must be able to commit data and a watermark token atomically in a single transaction or snapshot. Sinks without transaction support cannot provide this guarantee (they can still reach effectively-once via keyed upsert, below). The MongoDB sink requires a replica set (or sharded cluster) — multi-document transactions are unavailable on a standalone server. |

**Keyed upsert relaxes the source restriction entirely**: any source feeding an
upsert-capable sink (`postgres`, `sqlite`, `mysql`, `mssql`, `mongodb`,
`elasticsearch`, `bigquery`, `spanner`, `dynamodb`, `databricks`, `oracle`) configured with `write_mode: upsert` + `key` is
accepted under `delivery: exactly_once` and reported as
`effectively-once (keyed upsert)`. There is no watermark in this mode — the
idempotence comes from the sink converging on the keyed row.

A **durable** state store is required: `delivery: exactly_once` rejects
`state: { type: memory }` at config-load. The commit-token watermark must survive
a restart for the resume-and-skip logic to work — an in-memory store loses it on
process exit, so a crash would silently re-deliver an already-committed page. Use
`file`, `redis`, or `postgres` (see [State stores](#state-stores)).

A DLQ (`dlq:` block) is incompatible with `exactly_once` in this version.

### Hard gate at config-load time

`delivery: exactly_once` means "require at least effectively-once": the config
is accepted when either mechanism is achievable and rejected otherwise. The
atomic-watermark requirements (positional-replay source, idempotent sink, a
**durable** state store — not `memory` — and no DLQ) are validated when the
config is loaded — `faucet validate` reports a clear `config error` naming the
limiting side (and suggests the keyed-upsert alternative when the sink supports
it) before any run starts. There is no runtime fallback.

### Example: PostgreSQL CDC → PostgreSQL sink

```yaml
version: 1
name: cdc_exactly_once

pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://faucet:faucet@localhost:5432/appdb
      slot_name: faucet_slot
      publication_name: faucet_pub
      create_slot_if_missing: true
      idle_timeout: 30
  sink:
    type: postgres
    config:
      connection_url: postgres://writer:pass@localhost:5432/warehouse
      table_name: change_events
      column_mapping: auto_map
      batch_size: 1000
  state:
    type: file
    config:
      path: ./state

delivery: exactly_once
```

Validate the config before the first run:

```bash
faucet validate pipeline.yaml
```

### Monitoring

The `faucet_pipeline_pages_skipped_total{pipeline,row}` counter increments
each time the pipeline skips a page on resume because the sink already
committed it. A non-zero value on the first run after a crash is expected; a
persistently non-zero value on steady-state runs may indicate a state-store
or sink connectivity issue worth investigating.

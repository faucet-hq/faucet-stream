# Connector catalog

faucet-stream ships **<!--COUNT:sources-->43<!--/COUNT--> sources** and **<!--COUNT:sinks-->35<!--/COUNT--> sinks**. Each is a Cargo feature
(`source-<name>` / `sink-<name>`) and an independently published crate. Full API
docs are on [docs.rs](https://docs.rs/faucet-stream).

Run `faucet list` to see what's compiled into your binary, and
`faucet schema source <name>` / `faucet schema sink <name>` for a connector's
exact config fields. Not sure which to pick? See
[Choosing a connector](./choosing.md).

Browse them on the [Connector Hub](https://faucet-hq.github.io/connectors) — a
page per connector with its capabilities, tier, install line, config fields and
a `faucet init` snippet, plus community `faucet-source-*` / `faucet-sink-*`
crates found on crates.io. It is generated from `faucet conformance --export`.

Legend: ✓ supported · ✗ not applicable. Tier: T1 = passes the faucet-conformance battery in CI; T2 = not yet wired into the battery.

> **Two "tier" signals, distinct on purpose.** The `Tier` column below (T1/T2/T3)
> tracks whether a connector is wired into the reusable **faucet-conformance test
> battery** in CI. Separately, [`faucet conformance`](./conformance.md) computes a
> **maturity tier** — 🟢 Stable / 🟡 Experimental / 🟠 Beta / ⚪ Draft — from each
> connector's config schema and advertised capabilities; that tier shows in
> `faucet list` and `cli/connectors/registry.json`. See
> [Connector conformance & tiers](./conformance.md).

## Sources

| Connector | Tier¹¹ | Feature | Streams¹ | Resumable² | Effectively-once³ | Compression | Discover¹⁰ | Underlying primitive |
|-----------|:---:|---------|:---:|:---:|:---:|:---:|:---:|----------------------|
| REST | T1 ✅ᵐ | `source-rest` | ✓ | ✓ | ✗ | ✗ | ✓ᵒ | HTTP + 6 pagination styles, JSONPath extraction; `response_format: csv\|excel` parses an authed file body (cloud-drive download/signed URL), Excel via `source-rest-excel`; `odata:` block adds OData paging/query + `$metadata` discovery; `replication_bind` pushes the bookmark server-side; `window` slices incremental into rolling `[start,end)` datetime windows |
| GraphQL | T1 ✅ᵐ | `source-graphql` | ✓ | ✓ | ✗ | ✗ | ✗ | cursor / offset pagination, variable injection; `replication_method: incremental` + `replication_bind` binds the bookmark into a GraphQL variable |
| XML / SOAP | T1 ✅ᵐ | `source-xml` | ✓ | ✗ | ✗ | ✗ | ✗ | streaming XML→JSON, dot-path extraction, first-class `soap:` block (envelope + headers + fault handling) |
| gRPC | T1 ✅ | `source-grpc` | ✓⁴ | ✗ | ✗ | ✗ | ✗ | dynamic protobuf; unary + server-streaming |
| PostgreSQL | T1 ✅ | `source-postgres` | ✓ | ✗ | ✗ | ✗ | ✓ | SQL query, rows as JSON |
| PostgreSQL CDC | T1 ✅ | `source-postgres-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | logical replication (pgoutput), LSN bookmarks |
| MySQL | T1 ✅ | `source-mysql` | ✓ | ✗ | ✗ | ✗ | ✓ | SQL query, rows as JSON |
| MySQL CDC | T1 ✅ | `source-mysql-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | binlog row events, file/pos or GTID bookmarks |
| Microsoft SQL Server | T1 ✅ | `source-mssql` | ✓ | ✓⁸ | ✗ | ✗ | ✓ | SQL query (tiberius), rows as JSON |
| Microsoft SQL Server CDC | T1 ✅ | `source-mssql-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | CDC change tables (`fn_cdc_get_all_changes`), LSN bookmarks, `__op`-normalized |
| Oracle Database | T1 ✅ | `source-oracle` | ✓ | ✓ | ✗ | ✗ | ✓ | SQL query (ODPI-C), exact `NUMBER`/temporal typing, incremental `:bookmark` replication, PK-range sharding; needs Oracle Instant Client at runtime ([installation](../getting-started/installation.md#oracle-instant-client)) |
| Oracle CDC | T1 ✅ | `source-oracle-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | LogMiner over online + archived redo; committed transactions only, SCN bookmarks, `cdc_unwrap`-compatible envelopes; needs Oracle Instant Client at runtime ([installation](../getting-started/installation.md#oracle-instant-client)) |
| SQLite | T1 ✅ | `source-sqlite` | ✓ | ✗ | ✗ | ✗ | ✓ | SQL query, rows as JSON |
| DuckDB | T2 | `source-duckdb` | ✓ | ✗ | ✗ | ✗ | ✗ | SQL query (file or `:memory:`), rows as JSON; blocking-task + channel streaming |
| AWS SQS | T2 | `source-sqs` | ✓ | ✗ | ✗ | ✗ | ✗ | long-poll ReceiveMessage, delete-after-emit (at-least-once), idle/max-messages termination |
| NATS | T2 | `source-nats` | ✓ | ✗ | ✗ | ✗ | ✗ | subject subscription or JetStream durable consumer; idle/max-messages termination |
| RabbitMQ | T2 | `source-rabbitmq` | ✓ | ✗ | ✗ | ✗ | ✗ | AMQP 0.9.1 queue consumer (`lapin`), optional declare + bind; a page is acked (`basic.ack` multiple) only after the sink flushes it — at-least-once; idle/max-messages termination |
| SFTP | T2 | `source-sftp` | ✓ | ✗ | ✗ | ✗ | ✗ | list/glob a remote dir over SSH; JSONL, JSON array, raw text, Parquet (`parquet.columns`), plus CSV / XML / Excel / Avro / ORC via [file formats](../cookbook/file-formats.md) |
| AWS S3 | T1 ✅ | `source-s3` | ✓⁵ | ✗ | ✗ | ✓ | ✓ | object reader: JSONL, JSON array, raw text, Parquet, plus CSV / XML / Excel / Avro / ORC via [file formats](../cookbook/file-formats.md) |
| Google Cloud Storage | T2 | `source-gcs` | ✓⁵ | ✗ | ✗ | ✓ | ✓ | object reader: JSONL, JSON array, raw text, Parquet, plus CSV / XML / Excel / Avro / ORC via [file formats](../cookbook/file-formats.md) |
| Azure Blob / ADLS Gen2 | T1 ✅ | `source-azure-blob` | ✓⁵ | ✗ | ✗ | ✓ | ✗ | object reader (object_store): JSONL, JSON array, raw text, Parquet, plus CSV / XML / Excel / Avro / ORC via [file formats](../cookbook/file-formats.md) |
| MongoDB | T1 ✅ | `source-mongodb` | ✓ | ✗ | ✗ | ✗ | ✓ | `find()` with filter/projection/sort |
| MongoDB CDC | T1 ✅ | `source-mongodb-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | Change Streams, resumeToken bookmarks; `max_staged_records` buffer cap |
| Redis | T1 ✅ | `source-redis` | ✓ | ✗ | ✗ | ✗ | ✗ | streams, lists, key patterns |
| Webhook | T2 | `source-webhook` | ✗⁶ | ✗ | ✗ | ✗ | ✗ | temporary HTTP server collecting POSTs |
| WebSocket | T1 ✅ | `source-websocket` | ✓ | ✗ | ✗ | ✗ | ✗ | live push feed; subscribe frames, reconnect, ping keepalive |
| CSV *(deprecated)* | T1 ✅ | `source-csv` | ✓ | ✗ | ✗ | ✓ | ✗ | **Deprecated alias** of the [file source](../cookbook/file-formats.md#moving-off-the-csv-jsonl-and-parquet-kinds): `type: csv` is built as `type: file` with `format: csv` and warns |
| Local files | T1 ✅ | `source-file` | ✓ | ✓ | ✗ | ✓ | ✓ | [file source](../cookbook/file-formats.md#the-local-file-source): a path, directory, glob or `http(s)://` URL; JSONL / JSON / CSV / Excel / XML / Parquet / Avro / ORC, format + compression resolved per file; incremental by mtime or name; hash-of-path sharding; Avro / ORC / Parquet join the columnar path |
| Elasticsearch | T1 ✅ᵐ | `source-elasticsearch` | ✓ | ✗ | ✗ | ✗ | ✓ | search/scroll API |
| Apache Kafka | T1 ✅ | `source-kafka` | ✓ | ✓ | **✓** | ✗ | ✗ | consumer; idle/max-messages termination, offset bookmarks |
| AWS Kinesis | T1 ✅ | `source-kinesis` | ✓ | ✓ | ✗ | ✗ | ✗ | per-shard GetRecords workers; sequence-number bookmarks, idle/max-messages termination |
| Google Cloud Pub/Sub | T1 ✅ᵉ | `source-pubsub` | ✓ | ✓ | ✗ | ✗ | ✗ | streaming pull; per-message records + attributes, ack at durable page boundary (at-least-once), idle/max-messages termination |
| Apache Parquet *(deprecated)* | T1 ✅ | `source-parquet` | ✓ | ✗ | ✗ | ✗ | ✗ | **Deprecated alias** of the [file source](../cookbook/file-formats.md#moving-off-the-csv-jsonl-and-parquet-kinds) with `format: parquet`; an S3 location still uses the old reader |
| Apache Delta Lake | T1 ✅ | `source-delta` | ✓ | ✗ | ✗ | ✗ | ✗ | local FS or S3/Azure/GCS; time travel (version/timestamp), projection pushdown, partition reconstruction |
| Databricks SQL | T1 ✅ᵐ | `source-databricks` | ✓ | ✓ | ✗ | ✗ | ✗ | Statement Execution API; async poll, chunk pagination, typed decode, incremental `${bookmark}` |
| Apache Iceberg | T1 ✅ | `source-iceberg` | ✓ | ✓ | ✗ | ✗ | ✓ | REST/Glue/SQL/HMS catalog; Arrow scan with column projection + filter pushdown, snapshot / timestamp time travel, `mode: incremental` reads only snapshots appended since the bookmark; file-task sharding |
| Amazon DynamoDB | T1 ✅ᵉ | `source-dynamodb` | ✓ | ✓ | ✗ | ✗ | ✓ | `mode: scan` (parallel segments, shardable) / `query` / `streams` (DynamoDB Streams change capture, per-shard sequence bookmarks, `__op` envelope; anchors a [`faucet mirror`](../cookbook/replication.md) handoff) |
| Amazon Redshift | T1 ✅ | `source-redshift` | ✓ | ✓ | ✗ | ✗ | ✗ | PostgreSQL wire; SQL query, rows as JSON; incremental replication |
| ClickHouse | T1 ✅ | `source-clickhouse` | ✓ | ✓ | ✗ | ✗ | ✗ | HTTP interface, `FORMAT JSONEachRow` streaming; incremental replication |
| BigQuery | T1 ✅ᵐ | `source-bigquery` | ✓ | ✗ | ✗ | ✗ | ✓ | `jobs.query` + pageToken pagination |
| Snowflake | T1 ✅ᵐ | `source-snowflake` | ✓ | ✗ | ✗ | ✗ | ✓ | SQL REST API, server-side partitions |
| Cloud Spanner | T1 ✅ᵉ | `source-spanner` | ✓ | ✓⁸ | ✗ | ✗ | ✓ | streaming SQL (gRPC), incremental `@bookmark` replication, stale reads, PK-range sharding |
| Singer bridge ⚠️ | T2 ⚠️ | `source-singer` | ✓ | ✓⁹ | ✗ | ✗ | ✗ | runs an external Singer tap; NDJSON over stdout, STATE→bookmark. **Tier-2 / experimental** |

¹⁰ **Discover** = enumerates the datasets behind the connection for
[`faucet discover`](../cookbook/discover.md) (tables / collections / indices /
prefixes with schemas + row estimates where the catalog provides them).
ᵒ REST supports discovery with an `odata:` block (via the OData `$metadata`
(EDMX) catalog — one dataset per entity set) **or** a generic `discovery:`
recipe (a config-driven list → describe → emit pipeline — one dataset per
listed object, with a templated source config, typed schema, and optional
per-object sink `table_id`).
¹ **Streams** = yields records in bounded-memory batches rather than buffering the
whole result. ² **Resumable** = persists a bookmark to a [state store](../cookbook/state.md)
so re-runs continue where they left off (incremental replication / CDC / Kafka
offsets). ³ **Effectively-once** = the source emits a complete resume position on
every page and replaying from a bookmark continues the record stream at exactly
that position (immutable-log sources: CDC WAL/binlog/change streams, Kafka
partition offsets); required for the atomic-watermark mechanism behind
`delivery: exactly_once` — see
[Effectively-once delivery](../cookbook/state.md#effectively-once-delivery).
⁴ gRPC streams natively in *server-streaming* mode; unary buffers the
single response. ⁵ S3/GCS stream in JSONL and raw-text modes; JSON-array mode
buffers one object. ⁶ Webhook is buffer-shaped by nature (it collects POSTs over
a window). ⁸ MSSQL is resumable only in `replication: incremental` mode (it
persists a tracking-column bookmark); in `full` mode it is not.
⁹ The Singer bridge is resumable via the tap's `STATE` messages, but the
*granularity* of resume (and whether re-emitted rows overlap) depends on the
individual tap — pair it with a keyed/upsert sink for clean, effectively-once
(idempotent at-least-once) behavior.

> **Support tiers** (the **Tier** column above). A connector is **Tier-1 ✅**
> when it invokes and passes the `faucet-conformance` battery in CI against the
> connector's real backend — config-schema validity, bounded-memory streaming,
> and (where applicable) bookmark round-trip, idempotent replay, truthful
> capabilities, and errors-not-panics (see the Faucet Connector Protocol spec,
> `docs/spec/faucet-connector-spec-v0.md`). Each Tier-1 connector wires the
> battery from its own `tests/conformance.rs`; that battery **is** the tiering
> mechanism — there is no separate scheme.
>
> **ᵐ** marks a connector whose battery runs in CI against a **wiremock HTTP
> mock**, not a live service instance — the `rest`, `graphql`, `xml`,
> `elasticsearch`, `bigquery`, `snowflake`, and `databricks` sources and the
> `http` and `databricks` sinks. The mock faithfully drives the paging, schema, and error-handling
> behavior the checks assert, but it is not an end-to-end test against the real
> system (no credentialed cloud/service backend runs in CI). **ᵉ** marks a
> connector whose battery runs against an official **emulator** in Docker — a
> real implementation, closer to end-to-end than a wiremock but still not the
> managed service: the Cloud **Spanner** pair (Spanner emulator, gRPC), the
> **Pub/Sub** source and sink (Pub/Sub emulator, gRPC), the **DynamoDB** source
> and sink (DynamoDB Local), and the **Azure Blob** sink (Azurite). Unmarked **T1 ✅** connectors run against a real backend with
> no emulator caveat — a local filesystem (`delta`, `parquet`, `csv`, the
> `iceberg` source over a SQLite catalog + local warehouse), or a
> testcontainers-launched real server (`postgres`, `mysql`, `mongodb`, `redis`,
> `clickhouse`, `kafka`, …).
>
> The connectors still marked **Tier-2** are the ones whose full battery cannot
> run in CI (so they are not conformance-certified — Tier-2 means "not certified,"
> **not** "low quality"; they keep their own extensive wiremock/testcontainers
> tests): the **BigQuery** and **Snowflake** sinks and the **Elasticsearch** sink
> are cloud-only and tested against wiremock, which cannot validate real
> idempotent dedup; the **GCS** source's bounded-memory check needs a real gRPC
> backend (the emulator is REST-only); the **GCS** sink cannot be durably counted
> against the emulator; the **webhook** source is buffer-shaped (no bounded-memory
> page check); and the **Iceberg** sink is append-only with a terminal `flush`
> that does not fit the effectively-once replay check on iceberg-rust 0.10.0. The
> **Singer bridge ⚠️** source passes the battery but is additionally **experimental
> (v0, single-stream)**; the Singer bridge **sink** is experimental too — its
> guarantees are only as strong as the target it runs (upsert is handed to the
> target as `key_properties`, so faucet does not count it as key-deduplicating).

### Streaming: native vs. buffered

The **Streams¹** column above is not all-or-nothing — every source participates in the
bounded-memory streaming loop, but there are two ways it gets there:

- **Native streaming (override).** The source reads from its underlying primitive
  incrementally — a database cursor, a WAL/binlog/change stream, an object read line by
  line, a scroll cursor, a Kafka partition — and emits each `StreamPage` as it goes.
  Memory stays at `O(batch_size)` no matter how large the result set. These sources
  **override** `Source::stream_pages`.
- **Buffered fallback (default).** The source implements only the one required method,
  `fetch_with_context`; the default `stream_pages` calls it, buffers the whole result,
  then chunks the buffer into pages. Correct and still streamed *to the sink*, but peak
  memory is the full result set because the fetch buffered it first.

A connector author gets the buffered path for free and opts into native streaming only
where the primitive supports it — see [ADR 0001](https://github.com/faucet-hq/faucet-stream/blob/main/docs/adr/0001-stream-pages.md)
and the [stream-pages architecture note](https://github.com/faucet-hq/faucet-stream/blob/main/docs/architecture/stream-pages.md).

**Sources that override `stream_pages` for native streaming:** `rest`, `graphql`,
`xml`, `postgres`, `postgres-cdc`, `mysql`, `mysql-cdc`, `mssql`, `mssql-cdc`, `sqlite`,
`mongodb`, `mongodb-cdc`, `s3`/`gcs`/`azure-blob` (JSONL & raw-text modes), `parquet`,
`csv`, `elasticsearch` (scroll), `kafka`, `kinesis`, `spanner`, `websocket`, `redis`,
and `grpc` (server-streaming mode).

**Sources that intentionally keep the buffered default:** `grpc` unary mode (a single
response — no paging primitive) and `webhook` (buffer-shaped by nature — it collects
POSTs over a window). S3/GCS/Azure fall back to buffered for the JSON-array format only
(one array object must be parsed whole).

## Sinks

Every sink exposes a `batch_size` knob for write-side re-chunking. For the
file/append sinks (`jsonl`, `csv`, `stdout`) it's a no-op — they write per record.

**Object rollover (`max_records_per_file` / `max_bytes_per_file`, #618).** The
object-store sinks — `s3`, `gcs`, `azure-blob`, `sftp` — **accumulate across
`write_batch` calls** and roll to a new object when either cap is reached.
Before this each upstream page became its own object, so a small `batch_size`
produced a swarm of tiny objects: the small-files problem that dominates read
time on S3/Athena/Spark. With no cap set the whole run lands in one object,
closed at `flush`. The byte cap is what bounds buffered memory (rows are a poor
proxy for size), and `s3`/`azure-blob` additionally stream large objects
through **multipart** so peak memory is O(part size), not O(object size).

**Commit accumulation (`commit_rows` / `commit_bytes`, #617).** The warehouse
sinks — `snowflake`, `clickhouse`, `redshift` — accumulate records across
`write_batch` calls and commit once per threshold, plus once at `flush`. The
commit unit used to be the page unit and `batch_size` could only *split* a
page, never merge two, so a small source page meant one expensive warehouse
operation per page — and on ClickHouse, one MergeTree part per page, which
fails outright once they accumulate. Only the **append** path accumulates:
`delivery: exactly_once` and the DLQ path commit per page, because a watermark
must land with its own page and a DLQ must name which rows of *this* page
failed.

**Auto-create (`create_table`, #580).** Every **table-based** sink —
`bigquery`, `postgres`, `mysql`, `sqlite`, `mssql`, `duckdb`, `snowflake`,
`redshift`, `clickhouse`, `spanner`, `delta`, `iceberg` — takes
`create_table: bool`, **default `true`**: a first-ever sync cannot assume the
destination exists, so a missing table is created from the first written
page's inferred columns. Set `false` to require a pre-existing target and fail
fast with one uniform error naming both ways out.

Every inferred column is created **nullable**. A column that happened to be
present in page 1 is not required forever, and a `NOT NULL` inferred from one
page turns page 2 into a hard failure the first time a record omits the field;
narrowing later is the [`schema:` drift policy](../cookbook/schema-drift.md)'s
job, which can see more than one page. Three dialect-specific notes:

- **clickhouse** creates `MergeTree ORDER BY tuple()` and **redshift** creates
  with no DISTKEY/SORTKEY — faucet has no basis to pick a sort or distribution
  key, and a wrong one is baked into the table. Define the table yourself and
  set `create_table: false` when the physical layout matters.
- **snowflake** creates `STRING` columns, because its insert path projects
  every value with `::string` and a typed column would reject its own writer's
  cast.
- **spanner** needs a primary key on every table, so it auto-creates only when
  `key:` is set; without one it errors naming that requirement rather than
  inventing a key column that can never be changed.

`delta` and `iceberg` already created their tables and now spell the knob
`create_table` like everyone else (their historical `create_if_not_missing` /
`create_if_missing` stay accepted as aliases). The **schemaless** destinations
— `mongodb`, `elasticsearch` — deliberately have **no** knob: their servers
create a collection/index on first write and cannot be told not to, so the
field would be inert in one direction, which is exactly the silently-ignored
config this project treats as a defect.

| Connector | Tier¹¹ | Feature | `batch_size` | Compression | Upsert⁸ | Effectively-once⁷ | Batch atomicity⁹ | Write unit |
|-----------|:---:|---------|:---:|:---:|:---:|:---:|:---:|------------|
| BigQuery | T2 | `sink-bigquery` | ✓ | ✗ | **✓** | **✓** | per-row (`batch_size: 0`); atomic keyed | Bucket-free resumable load job by default (`media_load`); in-place `MERGE` for upsert + effectively-once |
| PostgreSQL | T1 ✅ | `sink-postgres` | ✓ | ✗ | **✓** | **✓** | atomic (JSONB + `batch_size: 0`; keyed when journaled) | multi-row `INSERT` (JSONB or mapped cols); `COPY FROM STDIN` fast-path for append (`write_method: copy`) |
| JSON Lines *(deprecated)* | T1 ✅ | `sink-jsonl` | no-op | ✓ | ✗ | ✗ | best-effort | **Deprecated alias** of the [file sink](../cookbook/file-formats.md#moving-off-the-csv-jsonl-and-parquet-kinds) with `format: json_lines` |
| Local files | T1 ✅ | `sink-file` | no-op | ✓ | ✗ | ✗ | atomic (no rollover) | [file sink](../cookbook/file-formats.md#the-local-file-sink): JSONL / JSON / CSV / XML / Excel / Avro / Parquet by extension; temp-then-rename finalisation, record/byte rollover with a `{part}` template, `write_mode: overwrite` swaps the whole output set; Parquet (and Avro) take the columnar path |
| Snowflake | T2 | `sink-snowflake` | ✓ | ✗ | ✗ | **✓** | atomic (`batch_size: 0`) | SQL REST API; multi-statement `BEGIN;INSERT;MERGE;COMMIT` transaction for effectively-once |
| Amazon Redshift | T1 ✅ | `sink-redshift` | ✓ | ✗ | ✗ | ✗ | atomic (COPY + `batch_size: 0`) | COPY-from-S3 (staged) or multi-row `INSERT`; append-only; auto-creates the table (`create_table`) |
| ClickHouse | T1 ✅ | `sink-clickhouse` | ✓ | ✗ | ✗ | ✗ | atomic (`batch_size: 0` or staged) | `INSERT … FORMAT JSONEachRow`; optional `async_insert`; append-only; auto-creates the table (`create_table`) |
| MySQL | T1 ✅ | `sink-mysql` | ✓ | ✗ | **✓** | **✓** | atomic keyed | multi-row `INSERT` |
| Microsoft SQL Server | T1 ✅ | `sink-mssql` | ✓ | ✗ | **✓** | **✓** | atomic keyed | multi-row `INSERT` (2100-param auto-split, per-row DLQ) |
| Oracle Database | T1 ✅ | `sink-oracle` | ✓ | ✗ | **✓** | **✓** | atomic | array DML per page (per-row DLQ), keyed `MERGE` upsert / `DELETE`, `_faucet_commit_token` in the same transaction; needs Oracle Instant Client at runtime ([installation](../getting-started/installation.md#oracle-instant-client)) |
| SQLite | T1 ✅ | `sink-sqlite` | ✓ | ✗ | **✓** | **✓** | atomic (`batch_size: 0` or keyed) | transaction-wrapped batch |
| DuckDB | T2 | `sink-duckdb` | ✓ | ✗ | ✗ | ✗ | atomic | transaction-wrapped multi-row `INSERT` (JSON column or auto-mapped); append-only |
| AWS SQS | T2 | `sink-sqs` | ✓ | ✗ | ✗ | ✗ | best-effort | batched SendMessageBatch (10/req), per-entry partial-failure retry; FIFO group/dedup |
| NATS | T2 | `sink-nats` | ✓ | ✗ | ✗ | ✗ | best-effort | publish to a subject (optional subject-per-record), flush per batch |
| RabbitMQ | T2 | `sink-rabbitmq` | ✓ | ✗ | ✗ | ✗ | best-effort | publish to an exchange with a static / field / JSONPath routing key; publisher confirms per batch; `mandatory` returns surface as per-row (DLQ-routable) errors |
| SFTP | T2 | `sink-sftp` | ✓ | ✗ | ✗ | ✗ | atomic (no rollover cap) | files over SSH, temp-then-rename upload; every [file format](../cookbook/file-formats.md) the `file` sink writes (JSONL, JSON array, CSV, XML, Excel, Avro, Parquet, raw text) through the shared file writer, with `file_name` templates, rollover, `mode` / `write_mode: overwrite`, compression and encryption |
| Singer bridge ⚠️ | T2 ⚠️ | `sink-singer` | no-op | ✗ | `key_properties` | ✗ | best-effort | runs an external Singer target; `SCHEMA`/`RECORD` over stdin with back-pressure, bookmarks advance only after the target confirms (echoed `STATE` or clean exit, `flush_on`); `write_mode: overwrite` → `ACTIVATE_VERSION`. **Tier-2 / experimental** |
| AWS S3 | T1 ✅ | `sink-s3` | ✓ | ✓ | ✗ | ✗ | atomic (no rollover cap) | objects in every [file format](../cookbook/file-formats.md) the `file` sink writes (JSONL, JSON array, CSV, XML, Excel, Avro, Parquet, raw text) through the shared file writer, with `path` templates, rollover, `mode` / `write_mode: overwrite`, compression and encryption; multipart upload past 8 MiB |
| Google Cloud Storage | T2 | `sink-gcs` | ✓ | ✓ | ✗ | ✗ | atomic (no rollover cap) | objects in every [file format](../cookbook/file-formats.md) the `file` sink writes (JSONL, JSON array, CSV, XML, Excel, Avro, Parquet, raw text) through the shared file writer, with `path` templates, rollover, `mode` / `write_mode: overwrite`, compression and encryption; resumable upload for large objects |
| Azure Blob / ADLS Gen2 | T1 ✅ᵉ | `sink-azure-blob` | ✓ | ✓ | ✗ | ✗ | atomic (no rollover cap) | blobs (object_store) in every [file format](../cookbook/file-formats.md) the `file` sink writes (JSONL, JSON array, CSV, XML, Excel, Avro, Parquet, raw text) through the shared file writer, with `path` templates, rollover, `mode` / `write_mode: overwrite`, compression and encryption; block-list upload past 8 MiB |
| MongoDB | T1 ✅ | `sink-mongodb` | ✓ | ✗ | **✓** | **✓** | best-effort | `insert_many`; multi-document transaction for effectively-once (replica set required) |
| Redis | T1 ✅ | `sink-redis` | ✓ | ✗ | ✗ | **✓** | best-effort | streams, lists, key-value (pipelined); `MULTI`/`EXEC` transaction for effectively-once |
| CSV *(deprecated)* | T1 ✅ | `sink-csv` | no-op | ✓ | ✗ | ✗ | best-effort | **Deprecated alias** of the [file sink](../cookbook/file-formats.md#moving-off-the-csv-jsonl-and-parquet-kinds) with `format: csv`; keeps the old `on_unknown_field: warn` default |
| Elasticsearch | T2 | `sink-elasticsearch` | ✓ | ✗ | **✓** | ✗ | per-row (`batch_size: 0`) | `_bulk` NDJSON (per-row DLQ) |
| HTTP | T1 ✅ᵐ | `sink-http` | ✓ | ✗ | ✗ | ✗ | per-row | POST, concurrent under a semaphore |
| Stdout | T1 ✅ | `sink-stdout` | no-op | ✗ | ✗ | ✗ | best-effort | JSON Lines / pretty JSON / TSV |
| Apache Kafka | T1 ✅ | `sink-kafka` | ✓ | ✗ | ✗ | **✓** | best-effort | producer, batched sends, multi-topic routing; transactional producer + compacted watermark side-topic for effectively-once |
| AWS Kinesis | T1 ✅ | `sink-kinesis` | ✓ | ✗ | ✗ | ✗ | best-effort | batched PutRecords; partition-key routing, per-entry partial-failure retry (DLQ-routable) |
| Google Cloud Pub/Sub | T1 ✅ᵉ | `sink-pubsub` | ✓ | ✗ | ✗ | ✗ | per-row | batched publish; optional ordering key, per-entry partial-failure retry (DLQ-routable) |
| Cloud Spanner | T1 ✅ᵉ | `sink-spanner` | ✓ | ✗ | **✓** | **✓** | best-effort | batched mutations (`insert` / `insert_or_update` / `delete`), cell-budget chunking, commit-token transaction for effectively-once |
| Apache Parquet *(deprecated)* | T1 ✅ | `sink-parquet` | ✓ | ✗⁶ | ✗ | ✗ | best-effort | **Deprecated alias** of the [file sink](../cookbook/file-formats.md#moving-off-the-csv-jsonl-and-parquet-kinds) with `format: parquet`; an S3 destination still uses the old writer |
| Apache Delta Lake | T1 ✅ | `sink-delta` | ✓ | ✗⁶ | ✗ | ✗ | atomic (unless `target_file_size` splits a page) | append-only; local FS or S3/Azure/GCS; schema-inferred table creation, partitioning, one commit per flush |
| Apache Iceberg | T2 | `sink-iceberg` | ✓ | ✗⁶ | ✗ | **✓** | per-row | REST/Glue/SQL/HMS catalog, local + cloud (S3/GCS) warehouses, `fast_append` snapshot, Parquet data files |
| Amazon DynamoDB | T1 ✅ᵉ | `sink-dynamodb` | ✓ | ✗ | **✓** | ✗ | per-row | `BatchWriteItem` (25 items / 16 MiB per request) with unprocessed-item retry, bounded concurrency; `write_mode: upsert\|delete` by the table key; optional `condition_expression` writes (`on_condition_failure`) |
| Databricks SQL | T1 ✅ᵐ | `sink-databricks` | ✓ | ✗ | **✓** | **✓** | best-effort | Statement Execution API into a Delta table: multi-row `INSERT` / `MERGE`, or staged `COPY INTO` (Unity Catalog volume, or `s3://`/`gs://`/`abfss://` with the `sink-databricks-staging` feature); per-token idempotent page write + commit-token table for effectively-once |

⁶ Parquet and Iceberg both handle compression internally at the Parquet column
level, so the file-level `compression` feature doesn't apply to either.
⁷ **Effectively-once** = commits data and a watermark token atomically; required for
`delivery: exactly_once`. The BigQuery sink does this via a multi-statement
`MERGE` transaction (distinct from its default bulk-load append path); the
Kafka sink uses a transactional producer that writes each page's records plus a
commit-token record into a compacted side-topic in one Kafka transaction; the
Snowflake sink runs one multi-statement `BEGIN;INSERT;MERGE;COMMIT` request; the
Redis sink wraps the page plus a `_faucet_commit_token:<scope>` key in one
`MULTI`/`EXEC`; the Databricks sink makes each page's write idempotent per token
(`INSERT … REPLACE WHERE _faucet_scope/_faucet_seq`, or a keyed `MERGE`) and then
advances a `_faucet_commit_token` Delta table, so a replayed page replaces its
earlier attempt; the MongoDB sink commits the page plus a watermark document in
one multi-document transaction (replica set required); the Cloud Spanner sink
buffers the page's mutations plus a `faucet_commit_token` row in one
read-write transaction; the Oracle sink upserts a `_faucet_commit_token`
row in the page's transaction. Sinks configured with
`write_mode: upsert` + `key` also reach effectively-once via keyed dedup, with
any source. See
[Effectively-once delivery](../cookbook/state.md#effectively-once-delivery).
⁹ **Batch atomicity** (#737) = what a failed sink write leaves behind: **atomic**
(nothing lands), **per-row** (per-row outcomes; a whole-write error means nothing
from that write landed), or **best-effort** (some rows may already have
landed). `on_batch_error: dlq_all` is refused on a best-effort sink — replaying
the DLQ would write the landed rows a second time — unless the sink writes by
key (`write_mode: upsert|delete`) or the `dlq:` block sets
`allow_duplicates_on_dlq_all: true`. The conditions in brackets are the sink
config that makes a write all-or-nothing; see
[Batch atomicity and `dlq_all`](../cookbook/dlq.md#batch-atomicity-and-dlq_all).
⁸ **Upsert** = supports `write_mode: upsert` / `delete` (insert-or-update and
delete by `key`) in addition to plain `append`. The SQL sinks require
column-mapping mode (`auto_map`, or `auto_columns` for mssql) and a
UNIQUE/PRIMARY KEY on `key`; the
schemaless sinks (MongoDB, Elasticsearch) map `key` to a match filter / `_id`;
DynamoDB requires `key` to name the table's partition (+ sort) key; Databricks
`MERGE`s on `key`.
Iceberg upsert is not yet supported (a follow-up, blocked on `iceberg-rust`).
`write_mode: overwrite` (full-refresh: atomically replace the whole
destination each run) is additionally supported by **PostgreSQL, SQLite, MySQL,
MSSQL, Oracle, MongoDB, BigQuery, Databricks, and Elasticsearch** (via an atomic alias
swap — the configured `index` must be an alias), and by the append-only **local file**
sink (files staged in a hidden directory and moved into place on success) — not
Spanner or DynamoDB. See
[Upsert / mirror tables](../cookbook/upsert.md).

Every sink in this column except **DynamoDB**, **Databricks** and **Oracle** also supports
**scoped cleanup** (`complete_for.on_missing: delete` on the source), which deletes
destination rows inside the declared scope that a run did not write — the only way
an incremental sync can remove records deleted at the source; see
[Removing records deleted at the source](../cookbook/upsert.md#removing-records-deleted-at-the-source-scoped-cleanup).

## Arrow columnar (Parquet) fast path

An opt-in, additive Arrow columnar path (RFC 0002 / #375, behind a crate-local
`arrow` feature) lets a run move records end-to-end as Arrow `RecordBatch`es
with no `serde_json::Value` materialization. It engages automatically when
**both** ends of the pipeline are Arrow-native **and** every configured
transform has an Arrow kernel; otherwise the pipeline transparently falls back
to the row path.

**Governance no longer disqualifies it (#636).** `masking:`, `quality:`,
`contract:` and `schema:` now run *inside* the columnar loop, via the same
pass the row path uses — so a `parquet → mask + quality → parquet` run stays
columnar instead of dropping to `Value` the moment a policy is attached.
**Quarantine works there too**: quarantined rows are written to the `dlq:`
sink under the same per-page and total budgets, and a budget abort still
writes the overshoot before stopping, so no quarantined row is dropped. The
one configuration that still falls back is `on_batch_error: dlq_all`, which
routes a *failed write* row-by-row — `write_batch_columnar` reports no
per-row outcomes to route. The governance pass materializes `Value` for the
page it inspects; the source→sink transfer stays columnar.

Arrow-native connectors:

- **Parquet** source/sink and **Delta Lake** source/sink — Arrow-native by
  nature.
- **AWS S3** and **Google Cloud Storage** source — with `file_format: parquet`.
- **AWS S3** and **Google Cloud Storage** sink — with `format: parquet` (each
  object is a self-contained ZSTD-compressed Parquet file).
- **Databricks SQL** source — with `arrow_native: true` (fetches
  `EXTERNAL_LINKS` + `ARROW_STREAM`; requires `replication: full`).
- **REST** source — with an `async_job:` (bulk-export-style) CSV extract,
  no custom `decode:` chain, and a **header** locator (#635). Every column is
  `Utf8`: types are never inferred, so a batch's columns match the `Value`
  path's keys exactly.
- **BigQuery** source — with `read_api: true` + `read_table` (reads the table
  via the Storage Read API gRPC service as Arrow; full extract only).
- **BigQuery** sink — a `PARQUET` load job, for `append` **and** `overwrite`.
  A `bulk_load` block stages the Parquet on a GCS bucket first; **without one
  the Parquet is uploaded with the job itself** (bucket-free, #635), so a
  bucket is now only worth configuring for very large batches. Under
  `overwrite` the first batch truncates and the rest append, so a mid-run
  failure leaves the prior table intact.
- **Snowflake** sink — with a `bulk_load` block (Parquet uploaded to an external
  stage then `COPY INTO … FILE_FORMAT=(TYPE=PARQUET)`; append only). The
  Snowflake *source* has no Arrow path (its v2 SQL API is jsonv2-only).

So chains like `s3(parquet) → parquet`, `gcs(parquet) → delta`,
`databricks(arrow) → parquet`, `bigquery(read-api) → parquet`,
`rest(async_job csv) → bigquery`, or `parquet → snowflake(bulk-load)` run
Arrow end-to-end. See each connector's
README for the exact config field and feature flag.

## Platform support

Every connector in the default build runs on Linux, macOS and Windows (x86_64; the
prebuilt binaries cover all three — see
[Installation](../getting-started/installation.md#windows-notes)), with two
exceptions on Windows:

- **Kafka** (source, sink, the lineage transport and the queue-depth trigger):
  Kerberos (`sasl.mechanism: GSSAPI` via `extra_client_config`) needs Cyrus SASL,
  which does not build on Windows. `PLAIN`, `SCRAM`, `OAUTHBEARER` and TLS work.
- **Singer** (source and sink): Windows has no `SIGTERM`, so a tap or target still
  running after the shutdown grace period is terminated rather than signalled.

The Oracle connectors need Oracle Instant Client at runtime on every platform
(on Windows, put its directory on `PATH`).

## Data-integrity notes

A few connectors enforce defaults that prevent silent data loss or corruption.
Inspect the exact fields with `faucet schema source <name>` / `faucet schema sink <name>`.

- **CSV source** — strict by default. A row whose field count differs from the
  header raises an error naming the offending line. Set `flexible: true` to
  tolerate ragged rows (the pre-1.x behaviour). *(Breaking default change.)*
- **CSV sink** — the column set is frozen from the first batch (the header cannot
  be rewritten in place). A field that first appears in a later page is dropped;
  `on_unknown_field: warn` (default) emits a one-shot warning naming the dropped
  field(s), while `on_unknown_field: error` aborts with a typed error.
- **Parquet sink** — the Arrow schema is re-inferred per output file on rollover,
  so a file written after the source widens picks up the new schema. A Parquet
  file's schema is immutable once opened, so a field appearing only later *within
  a single file* is dropped with a per-file one-shot warning.
- **MongoDB CDC source** — `max_staged_records` (default unbounded) caps the
  in-memory change-event buffer (including under `batch_size: 0`) and aborts with
  a typed error rather than risking OOM, mirroring `postgres-cdc` / `mysql-cdc`.

## Schema evolution

The pipeline-level [`schema:`](../cookbook/schema-drift.md) block detects when an
incoming page's top-level shape diverges from the sink's destination schema and
applies one policy (`warn` / `ignore` / `fail` / `quarantine` / `evolve`). Which
sinks can actually *act* on it varies:

| Sink | Schema evolution |
|------|------------------|
| `postgres`, `mysql`, `mssql`, `sqlite`, `bigquery` | **✓ evolve** — in-place additive/widening DDL |
| `elasticsearch` | **✓ evolve** — can add fields only (existing-field type change is incompatible) |
| `spanner` | **✓ evolve** — additive columns + NOT NULL relax; base-type widening is not supported by Spanner (use `allow_type_widening: false`) |
| `oracle` | **✓ evolve** — `ADD` columns, widen integer `NUMBER`s to decimals, relax `NOT NULL` |
| `databricks` | **✓ evolve** — `ALTER TABLE … ADD COLUMNS`, numeric widening to `DOUBLE` via Delta type widening (tinyint/smallint/int/float), and `DROP NOT NULL`; other widenings are incompatible |
| `iceberg` | detect-only — `warn`/`ignore`/`fail`/`quarantine` work; `evolve` blocked on upstream `iceberg-rust` (#255) |
| `jsonl`, `csv`, `stdout`, `mongodb`, `redis`, `http`, `kafka`, `s3`, `gcs`, `snowflake`, `parquet`, `dynamodb` | — (schemaless; the `schema:` policy is inert) |

`on_drift: evolve` against a detect-only or schemaless sink is rejected at
config-load. See [Schema drift](../cookbook/schema-drift.md) for the per-sink
nuances (e.g. SQLite widening is a no-op; Elasticsearch can only add fields).

## Authentication at a glance

| Family | Auth options |
|--------|--------------|
| REST / GraphQL / XML | Bearer, Basic, ApiKey (header), ApiKeyQuery, OAuth2 (client-credentials), TokenEndpoint, Custom headers — see [Auth cookbook](../cookbook/auth.md) |
| BigQuery | service-account key (path or inline JSON), application-default credentials |
| Snowflake | JWT key-pair, OAuth |
| Cloud Spanner | service-account key (path or inline JSON), application-default credentials |
| Databricks | personal access token, OAuth bearer, or a shared `auth: { ref }` provider (OAuth2 M2M service principal) |
| DynamoDB / Kinesis / SQS | AWS default chain, named profile, static access key, assume-role, web identity |
| Iceberg | REST catalog bearer / OAuth2 credential; Glue / SQL / HMS catalog properties; warehouse object-store credentials via `catalog.properties` |
| Kafka | SASL (PLAIN/SCRAM) + TLS |
| WebSocket | none, Bearer token, Custom headers |
| Elasticsearch | basic, API key, bearer, none |
| S3 / GCS | cloud SDK credential chains (env, profile, metadata) |
| SQL databases | connection URL (with embedded credentials / TLS params) |
| Oracle | username/password with `connect_string` or `host` + `service_name`/`sid`; TLS via wallet (`tls`) |

Inspect any connector's exact auth shape with `faucet schema source <name>` /
`faucet schema sink <name>`.

## Batching

Default `batch_size` is 1000; max is 1,000,000. `batch_size: 0` means "no
batching" — the source emits the whole result set in one page and the sink writes
it in one request (good for small lookup tables or load-job-style sinks). See
[Performance tuning](../operations/tuning.md).

---

¹¹ **Tier** = conformance status. **T1 ✅** means the connector adds a
`tests/conformance.rs` that invokes the reusable `faucet-conformance` battery
against the real connector and passes it in CI (valid config schema,
bounded-memory streaming, honest capabilities, and the further checks as they
land) — that battery is the single source of truth for the tier. **T2** means
the connector is not yet wired into the battery; most still have their own
integration tests, so T2 does **not** mean low quality. See the
[Faucet Connector Protocol (FCP v0)](../spec/faucet-connector-spec-v0.md) for the
full contract.

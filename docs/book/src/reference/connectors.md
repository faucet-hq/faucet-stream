# Connector catalog

faucet-stream ships **<!--COUNT:sources-->37<!--/COUNT--> sources** and **<!--COUNT:sinks-->29<!--/COUNT--> sinks**. Each is a Cargo feature
(`source-<name>` / `sink-<name>`) and an independently published crate. Full API
docs are on [docs.rs](https://docs.rs/faucet-stream).

Run `faucet list` to see what's compiled into your binary, and
`faucet schema source <name>` / `faucet schema sink <name>` for a connector's
exact config fields. Not sure which to pick? See
[Choosing a connector](./choosing.md).

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
| REST | T1 ✅ᵐ | `source-rest` | ✓ | ✓ | ✗ | ✗ | ✓ᵒ | HTTP + 6 pagination styles, JSONPath extraction; `response_format: csv\|excel` parses an authed file body (Graph/OneDrive/signed URL), Excel via `source-rest-excel`; `odata:` block adds OData paging/query + `$metadata` discovery; `replication_bind` pushes the bookmark server-side; `window` slices incremental into rolling `[start,end)` datetime windows |
| GraphQL | T1 ✅ᵐ | `source-graphql` | ✓ | ✗ | ✗ | ✗ | ✗ | cursor pagination, variable injection |
| XML / SOAP | T1 ✅ᵐ | `source-xml` | ✓ | ✗ | ✗ | ✗ | ✗ | streaming XML→JSON, dot-path extraction, first-class `soap:` block (envelope + headers + fault handling) |
| gRPC | T1 ✅ | `source-grpc` | ✓⁴ | ✗ | ✗ | ✗ | ✗ | dynamic protobuf; unary + server-streaming |
| PostgreSQL | T1 ✅ | `source-postgres` | ✓ | ✗ | ✗ | ✗ | ✓ | SQL query, rows as JSON |
| PostgreSQL CDC | T1 ✅ | `source-postgres-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | logical replication (pgoutput), LSN bookmarks |
| MySQL | T1 ✅ | `source-mysql` | ✓ | ✗ | ✗ | ✗ | ✓ | SQL query, rows as JSON |
| MySQL CDC | T1 ✅ | `source-mysql-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | binlog row events, file/pos or GTID bookmarks |
| Microsoft SQL Server | T1 ✅ | `source-mssql` | ✓ | ✓⁸ | ✗ | ✗ | ✓ | SQL query (tiberius), rows as JSON |
| Microsoft SQL Server CDC | T1 ✅ | `source-mssql-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | CDC change tables (`fn_cdc_get_all_changes`), LSN bookmarks, `__op`-normalized |
| SQLite | T1 ✅ | `source-sqlite` | ✓ | ✗ | ✗ | ✗ | ✓ | SQL query, rows as JSON |
| DuckDB | T2 | `source-duckdb` | ✓ | ✗ | ✗ | ✗ | ✗ | SQL query (file or `:memory:`), rows as JSON; blocking-task + channel streaming |
| AWS SQS | T2 | `source-sqs` | ✓ | ✗ | ✗ | ✗ | ✗ | long-poll ReceiveMessage, delete-after-emit (at-least-once), idle/max-messages termination |
| NATS | T2 | `source-nats` | ✓ | ✗ | ✗ | ✗ | ✗ | subject subscription or JetStream durable consumer; idle/max-messages termination |
| SFTP | T2 | `source-sftp` | ✓ | ✗ | ✗ | ✗ | ✗ | list/glob a remote dir over SSH; JSONL, JSON array, raw text |
| AWS S3 | T1 ✅ | `source-s3` | ✓⁵ | ✗ | ✗ | ✓ | ✓ | object reader: JSONL, JSON array, raw text |
| Google Cloud Storage | T2 | `source-gcs` | ✓⁵ | ✗ | ✗ | ✓ | ✓ | object reader: JSONL, JSON array, raw text |
| Azure Blob / ADLS Gen2 | T1 ✅ | `source-azure-blob` | ✓⁵ | ✗ | ✗ | ✓ | ✗ | object reader (object_store): JSONL, JSON array, raw text |
| MongoDB | T1 ✅ | `source-mongodb` | ✓ | ✗ | ✗ | ✗ | ✓ | `find()` with filter/projection/sort |
| MongoDB CDC | T1 ✅ | `source-mongodb-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | Change Streams, resumeToken bookmarks; `max_staged_records` buffer cap |
| Redis | T1 ✅ | `source-redis` | ✓ | ✗ | ✗ | ✗ | ✗ | streams, lists, key patterns |
| Webhook | T2 | `source-webhook` | ✗⁶ | ✗ | ✗ | ✗ | ✗ | temporary HTTP server collecting POSTs |
| WebSocket | T1 ✅ | `source-websocket` | ✓ | ✗ | ✗ | ✗ | ✗ | live push feed; subscribe frames, reconnect, ping keepalive |
| CSV | T1 ✅ | `source-csv` | ✓ | ✗ | ✗ | ✓ | ✗ | CSV files as JSON; strict field count by default (`flexible: true` to tolerate ragged rows) |
| Elasticsearch | T1 ✅ᵐ | `source-elasticsearch` | ✓ | ✗ | ✗ | ✗ | ✓ | search/scroll API |
| Apache Kafka | T1 ✅ | `source-kafka` | ✓ | ✓ | **✓** | ✗ | ✗ | consumer; idle/max-messages termination, offset bookmarks |
| AWS Kinesis | T1 ✅ | `source-kinesis` | ✓ | ✓ | ✗ | ✗ | ✗ | per-shard GetRecords workers; sequence-number bookmarks, idle/max-messages termination |
| Google Cloud Pub/Sub | T1 ✅ᵉ | `source-pubsub` | ✓ | ✓ | ✗ | ✗ | ✗ | streaming pull; per-message records + attributes, ack at durable page boundary (at-least-once), idle/max-messages termination |
| Apache Parquet | T1 ✅ | `source-parquet` | ✓ | ✗ | ✗ | ✗ | ✗ | local/glob/S3, vectorized Arrow reader, projection |
| Apache Delta Lake | T1 ✅ | `source-delta` | ✓ | ✗ | ✗ | ✗ | ✗ | local FS or S3/Azure/GCS; time travel (version/timestamp), projection pushdown, partition reconstruction |
| Databricks SQL | T1 ✅ᵐ | `source-databricks` | ✓ | ✓ | ✗ | ✗ | ✗ | Statement Execution API; async poll, chunk pagination, typed decode, incremental `${bookmark}` |
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
> `http` sink. The mock faithfully drives the paging, schema, and error-handling
> behavior the checks assert, but it is not an end-to-end test against the real
> system (no credentialed cloud/service backend runs in CI). **ᵉ** marks a
> connector whose battery runs against an official **emulator** in Docker — a
> real implementation, closer to end-to-end than a wiremock but still not the
> managed service: the Cloud **Spanner** pair (Spanner emulator, gRPC), the
> **Pub/Sub** source and sink (Pub/Sub emulator, gRPC), and the **Azure Blob**
> sink (Azurite). Unmarked **T1 ✅** connectors run against a real backend with
> no emulator caveat — a local filesystem (`delta`, `parquet`, `csv`), or a
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
> that does not fit the effectively-once replay check on iceberg-rust 0.9.1. The
> **Singer bridge ⚠️** passes the battery but is additionally **experimental
> (v0, single-stream)**.

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

| Connector | Tier¹¹ | Feature | `batch_size` | Compression | Upsert⁸ | Effectively-once⁷ | Write unit |
|-----------|:---:|---------|:---:|:---:|:---:|:---:|------------|
| BigQuery | T2 | `sink-bigquery` | ✓ | ✗ | **✓** | **✓** | `tabledata.insertAll` streaming; in-place `MERGE` for upsert + effectively-once |
| PostgreSQL | T1 ✅ | `sink-postgres` | ✓ | ✗ | **✓** | **✓** | multi-row `INSERT` (JSONB or mapped cols); `COPY FROM STDIN` fast-path for append (`write_method: copy`) |
| JSON Lines | T1 ✅ | `sink-jsonl` | no-op | ✓ | ✗ | ✗ | buffered file append |
| Snowflake | T2 | `sink-snowflake` | ✓ | ✗ | ✗ | **✓** | SQL REST API; multi-statement `BEGIN;INSERT;MERGE;COMMIT` transaction for effectively-once |
| Amazon Redshift | T1 ✅ | `sink-redshift` | ✓ | ✗ | ✗ | ✗ | COPY-from-S3 (staged) or multi-row `INSERT`; append-only |
| ClickHouse | T1 ✅ | `sink-clickhouse` | ✓ | ✗ | ✗ | ✗ | `INSERT … FORMAT JSONEachRow`; optional `async_insert`; append-only |
| MySQL | T1 ✅ | `sink-mysql` | ✓ | ✗ | **✓** | **✓** | multi-row `INSERT` |
| Microsoft SQL Server | T1 ✅ | `sink-mssql` | ✓ | ✗ | **✓** | **✓** | multi-row `INSERT` (2100-param auto-split, per-row DLQ) |
| SQLite | T1 ✅ | `sink-sqlite` | ✓ | ✗ | **✓** | **✓** | transaction-wrapped batch |
| DuckDB | T2 | `sink-duckdb` | ✓ | ✗ | ✗ | ✗ | transaction-wrapped multi-row `INSERT` (JSON column or auto-mapped); append-only |
| AWS SQS | T2 | `sink-sqs` | ✓ | ✗ | ✗ | ✗ | batched SendMessageBatch (10/req), per-entry partial-failure retry; FIFO group/dedup |
| NATS | T2 | `sink-nats` | ✓ | ✗ | ✗ | ✗ | publish to a subject (optional subject-per-record), flush per batch |
| SFTP | T2 | `sink-sftp` | ✓ | ✗ | ✗ | ✗ | JSONL files over SSH; atomic temp-then-rename upload |
| AWS S3 | T1 ✅ | `sink-s3` | ✓ | ✓ | ✗ | ✗ | JSONL objects, parallel uploads |
| Google Cloud Storage | T2 | `sink-gcs` | ✓ | ✓ | ✗ | ✗ | JSONL objects |
| Azure Blob / ADLS Gen2 | T1 ✅ᵉ | `sink-azure-blob` | ✓ | ✓ | ✗ | ✗ | JSONL blobs (object_store), batch/byte rollover |
| MongoDB | T1 ✅ | `sink-mongodb` | ✓ | ✗ | **✓** | **✓** | `insert_many`; multi-document transaction for effectively-once (replica set required) |
| Redis | T1 ✅ | `sink-redis` | ✓ | ✗ | ✗ | **✓** | streams, lists, key-value (pipelined); `MULTI`/`EXEC` transaction for effectively-once |
| CSV | T1 ✅ | `sink-csv` | no-op | ✓ | ✗ | ✗ | buffered file rows; column set frozen from first batch (`on_unknown_field: warn`/`error`) |
| Elasticsearch | T2 | `sink-elasticsearch` | ✓ | ✗ | **✓** | ✗ | `_bulk` NDJSON (per-row DLQ) |
| HTTP | T1 ✅ᵐ | `sink-http` | ✓ | ✗ | ✗ | ✗ | POST, concurrent under a semaphore |
| Stdout | T1 ✅ | `sink-stdout` | no-op | ✗ | ✗ | ✗ | JSON Lines / pretty JSON / TSV |
| Apache Kafka | T1 ✅ | `sink-kafka` | ✓ | ✗ | ✗ | **✓** | producer, batched sends, multi-topic routing; transactional producer + compacted watermark side-topic for effectively-once |
| AWS Kinesis | T1 ✅ | `sink-kinesis` | ✓ | ✗ | ✗ | ✗ | batched PutRecords; partition-key routing, per-entry partial-failure retry (DLQ-routable) |
| Google Cloud Pub/Sub | T1 ✅ᵉ | `sink-pubsub` | ✓ | ✗ | ✗ | ✗ | batched publish; optional ordering key, per-entry partial-failure retry (DLQ-routable) |
| Cloud Spanner | T1 ✅ᵉ | `sink-spanner` | ✓ | ✗ | **✓** | **✓** | batched mutations (`insert` / `insert_or_update` / `delete`), cell-budget chunking, commit-token transaction for effectively-once |
| Apache Parquet | T1 ✅ | `sink-parquet` | ✓ | ✗⁶ | ✗ | ✗ | local/S3, schema inference (re-inferred per file on rollover), row/byte rollover |
| Apache Delta Lake | T1 ✅ | `sink-delta` | ✓ | ✗⁶ | ✗ | ✗ | append-only; local FS or S3/Azure/GCS; schema-inferred table creation, partitioning, one commit per flush |
| Apache Iceberg | T2 | `sink-iceberg` | ✓ | ✗⁶ | ✗ | **✓** | REST/Glue/SQL/HMS catalog, local + cloud (S3/GCS) warehouses, `fast_append` snapshot, Parquet data files |

⁶ Parquet and Iceberg both handle compression internally at the Parquet column
level, so the file-level `compression` feature doesn't apply to either.
⁷ **Effectively-once** = commits data and a watermark token atomically; required for
`delivery: exactly_once`. The BigQuery sink does this via a multi-statement
`MERGE` transaction (distinct from its default streaming `insertAll` path); the
Kafka sink uses a transactional producer that writes each page's records plus a
commit-token record into a compacted side-topic in one Kafka transaction; the
Snowflake sink runs one multi-statement `BEGIN;INSERT;MERGE;COMMIT` request; the
Redis sink wraps the page plus a `_faucet_commit_token:<scope>` key in one
`MULTI`/`EXEC`; the MongoDB sink commits the page plus a watermark document in
one multi-document transaction (replica set required); the Cloud Spanner sink
buffers the page's mutations plus a `faucet_commit_token` row in one
read-write transaction. Sinks configured with
`write_mode: upsert` + `key` also reach effectively-once via keyed dedup, with
any source. See
[Effectively-once delivery](../cookbook/state.md#effectively-once-delivery).
⁸ **Upsert** = supports `write_mode: upsert` / `delete` (insert-or-update and
delete by `key`) in addition to plain `append`. The SQL sinks require
column-mapping mode (`auto_map`, or `auto_columns` for mssql) and a
UNIQUE/PRIMARY KEY on `key`; the
schemaless sinks (MongoDB, Elasticsearch) map `key` to a match filter / `_id`.
Iceberg upsert is not yet supported (a follow-up, blocked on `iceberg-rust`).
`write_mode: overwrite` (full-refresh: atomically replace the whole
destination each run) is additionally supported by **PostgreSQL, SQLite, MySQL,
MSSQL, MongoDB, BigQuery, and Elasticsearch** (via an atomic alias swap — the
configured `index` must be an alias) — not Spanner. See
[Upsert / mirror tables](../cookbook/upsert.md).

Every sink in this column also supports **scoped cleanup**
(`complete_for.on_missing: delete` on the source), which deletes destination rows
inside the declared scope that a run did not write — the only way an incremental sync
can remove records deleted at the source. The two sets are identical today, so
there is no separate column; see
[Removing records deleted at the source](../cookbook/upsert.md#removing-records-deleted-at-the-source-scoped-cleanup).

## Arrow columnar (Parquet) fast path

An opt-in, additive Arrow columnar path (RFC 0002 / #375, behind a crate-local
`arrow` feature) lets a run move records end-to-end as Arrow `RecordBatch`es
with no `serde_json::Value` materialization. It engages automatically when
**both** ends of the pipeline are Arrow-native **and** no `Value`-shaped
transform is configured; otherwise the pipeline transparently falls back to the
row path.

Arrow-native connectors:

- **Parquet** source/sink and **Delta Lake** source/sink — Arrow-native by
  nature.
- **AWS S3** and **Google Cloud Storage** source — with `file_format: parquet`.
- **AWS S3** and **Google Cloud Storage** sink — with `format: parquet` (each
  object is a self-contained ZSTD-compressed Parquet file).
- **Databricks SQL** source — with `arrow_native: true` (fetches
  `EXTERNAL_LINKS` + `ARROW_STREAM`; requires `replication: full`).
- **BigQuery** source — with `read_api: true` + `read_table` (reads the table
  via the Storage Read API gRPC service as Arrow; full extract only).
- **BigQuery** sink — with a `bulk_load` block (Parquet on a GCS staging bucket
  then a `PARQUET` load job; append only).
- **Snowflake** sink — with a `bulk_load` block (Parquet uploaded to an external
  stage then `COPY INTO … FILE_FORMAT=(TYPE=PARQUET)`; append only). The
  Snowflake *source* has no Arrow path (its v2 SQL API is jsonv2-only).

So chains like `s3(parquet) → parquet`, `gcs(parquet) → delta`,
`databricks(arrow) → parquet`, `bigquery(read-api) → parquet`, or
`parquet → snowflake(bulk-load)` run Arrow end-to-end. See each connector's
README for the exact config field and feature flag.

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
| `iceberg` | detect-only — `warn`/`ignore`/`fail`/`quarantine` work; `evolve` blocked on upstream `iceberg-rust` (#255) |
| `jsonl`, `csv`, `stdout`, `mongodb`, `redis`, `http`, `kafka`, `s3`, `gcs`, `snowflake`, `parquet` | — (schemaless; the `schema:` policy is inert) |

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
| Kafka | SASL (PLAIN/SCRAM) + TLS |
| WebSocket | none, Bearer token, Custom headers |
| Elasticsearch | basic, API key, bearer, none |
| S3 / GCS | cloud SDK credential chains (env, profile, metadata) |
| SQL databases | connection URL (with embedded credentials / TLS params) |

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

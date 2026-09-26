# Choosing a connector

Several connectors overlap. This page resolves the common "which one?" questions.
For the full feature grid see the [connector catalog](./connectors.md).

## PostgreSQL: query source vs. CDC

- **`source-postgres`** runs a SQL query and returns the rows. Use it for
  one-shot extracts, snapshots, or when you control an `updated_at` column and
  parameterize the query yourself. Simple, no special Postgres config.
- **`source-postgres-cdc`** streams every `INSERT`/`UPDATE`/`DELETE` from the
  write-ahead log via logical replication. Use it when you need **every change**
  (including deletes), low-latency capture, or resumability without a cursor
  column. Requires `wal_level = logical` and a publication, and retains WAL
  between runs. See the [CDC tutorial](../tutorials/postgres-cdc.md).

**Rule of thumb:** periodic snapshot → query source; continuous change feed → CDC.

## MySQL: query source vs. CDC

- **`source-mysql`** runs a SQL query and returns the rows — one-shot extracts,
  snapshots, or `updated_at`-driven incremental pulls you parameterize yourself.
  Simple, no special MySQL config.
- **`source-mysql-cdc`** streams every `INSERT`/`UPDATE`/`DELETE` from the binary
  log via row-based replication. Use it when you need **every change** (including
  deletes), low-latency capture, or resumability without a cursor column. Requires
  `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL` (for
  column names), a unique `server_id`, and `REPLICATION SLAVE`/`REPLICATION CLIENT`
  grants; resumes from a `{file,pos}` (or GTID) bookmark. Targets transactional
  (InnoDB) tables. See the [connector reference](connectors.md).

**Rule of thumb (MySQL too):** periodic snapshot → query source; continuous change feed → CDC.

## MongoDB: query source vs. Change Streams (CDC)

- **`source-mongodb`** runs a `find()` with filter/projection/sort — snapshots and
  bounded extracts.
- **`source-mongodb-cdc`** tails MongoDB Change Streams for every document change,
  resumable via the opaque `resumeToken`. Requires a replica set or sharded
  cluster. See the [connector reference](connectors.md).

## Object storage: S3/GCS source vs. Parquet source

- **`source-s3` / `source-gcs`** read objects as JSONL, a JSON array, or raw
  text. Use them for line-delimited JSON, logs, or text dumps.
- **`source-parquet`** reads columnar Parquet (local, glob, or S3) with a
  vectorized Arrow reader and column projection. Use it for analytical datasets —
  it's far faster and can skip columns you don't need.

**Rule of thumb:** the file is `.parquet` → Parquet source; it's JSON/text →
S3/GCS source. (The Parquet source reads from S3 directly, so you don't need the
S3 source in front of it.)

## Live feeds: WebSocket vs. Webhook vs. Kafka/Redis

- **`source-websocket`** — connects *out* to a live push endpoint (`ws://`/`wss://`),
  optionally sends subscription frames, and streams each incoming message as a record.
  Use it for market data, chat feeds, telemetry, or any server that pushes over WebSocket.
  Live-only — no replay, no durable offset.
- **`source-webhook`** — opens a temporary HTTP server and *receives* inbound HTTP
  POSTs from external systems over a time window. Use it when the remote system pushes
  to you over HTTP rather than WebSocket.
- **`source-kafka`** / **`source-redis`** — broker-backed streaming with durable,
  replayable offsets and resumable bookmarks. Use these when you need guaranteed delivery
  and the ability to continue from where a previous run left off.

**Rule of thumb:** connecting out to a live WebSocket feed → `source-websocket`; receiving
inbound HTTP POST payloads → `source-webhook`; durable, replayable event stream →
`source-kafka` or `source-redis`.

## Streaming: Redis vs. Kafka vs. Kinesis vs. RabbitMQ

- **`source-redis`** reads streams, lists, or key patterns. Great when Redis is
  already in your stack and volumes are modest.
- **`source-kafka`** is a real consumer with consumer-group offsets and
  resumable bookmarks. Use it for high-throughput event pipelines and durable,
  replayable streams.
- **`source-kinesis`** consumes AWS Kinesis Data Streams shard-by-shard with
  resumable per-shard sequence checkpoints. Use it when your event stream is
  already on AWS — same termination knobs as the Kafka source.
- **`source-rabbitmq`** drains an AMQP 0.9.1 queue (optionally declaring it and
  binding it to exchanges). The broker owns the position: each page is acked
  only after the sink flushes it, so a crash redelivers instead of losing
  messages. Use it when RabbitMQ is already your integration backbone.

**Rule of thumb:** durable, high-volume event stream → Kafka (self-managed /
Confluent) or Kinesis (AWS-native); work queues and exchange fan-out →
RabbitMQ; lightweight queue/cache already on hand → Redis.

## HTTP APIs: REST vs. GraphQL vs. XML vs. gRPC

- **`source-rest`** — JSON REST APIs. The most full-featured source: six
  pagination styles, seven auth strategies, incremental replication, partitions.
- **`source-graphql`** — GraphQL endpoints with cursor pagination and variable
  injection.
- **`source-xml`** — XML/SOAP APIs; converts XML to JSON with dot-path extraction.
- **`source-grpc`** — gRPC services via dynamic protobuf (`prost-reflect`), unary
  or server-streaming.

**Rule of thumb:** match the protocol the API speaks. For incremental/resumable
ingestion, REST has the richest support.

## Warehouses: when to read with BigQuery / Snowflake sources

Use **`source-bigquery`** / **`source-snowflake`** to *read out of* a warehouse
(e.g. to move a query result elsewhere). To *load into* one, use the matching
**sink**. To transform data already inside the warehouse, reach for
[dbt](https://www.getdbt.com/) — that's not faucet's job.

## Cloud Spanner: OLTP system of record

Use **`source-spanner`** to move data *out of* Spanner into a warehouse or lake
(the common direction — Spanner is an expensive OLTP system of record). It
streams arbitrary SQL over gRPC, supports incremental replication via a
monotonic column (`@bookmark`), stale reads to offload the leader, and PK-range
sharding. Use **`sink-spanner`** when Spanner *is* the destination — its
mutation API pairs naturally with `write_mode: upsert` (`InsertOrUpdate` keyed
on the primary key) and supports effectively-once delivery via a commit-token
read-write transaction.

## Sinks: column-mapped vs. JSON blob (SQL databases)

The Postgres/MySQL/SQLite/SQL Server sinks can write either:

- **a single JSON/JSONB column** (`column_mapping: { type: jsonb, column: data }`)
  — schemaless, no DDL coupling, easiest to start with; or
- **auto-mapped columns** — one column per top-level field, for queryable
  relational tables.

**Rule of thumb:** exploratory / evolving schema → JSON column; stable schema you
query with SQL → mapped columns.

## File sinks: JSONL vs. CSV vs. Parquet vs. stdout

- **`sink-stdout`** — debugging and pipelines (`faucet preview` uses it).
- **`sink-jsonl`** — line-delimited JSON; lossless, streaming-friendly,
  gzip/zstd-capable.
- **`sink-csv`** — flat tabular output for spreadsheets/BI; nested fields flatten.
- **`sink-parquet`** — columnar analytical output with built-in compression and
  schema inference; best for large datasets consumed by analytics engines.

**Rule of thumb:** machine-to-machine JSON → JSONL; tabular for humans → CSV;
analytics at scale → Parquet.

## Parquet sink vs. Iceberg sink

Both write columnar Parquet files, but they serve different use cases:

- **`sink-parquet`** — writes raw Parquet files to a local path or S3 prefix.
  Simple, zero catalog dependency, compatible with any Parquet reader. Use it
  when you want portable files and don't need schema evolution, time-travel, or
  ACID snapshot isolation.
- **`sink-iceberg`** — writes Parquet data files and registers them in an Iceberg
  catalog (REST, AWS Glue, SQL-backed, or Hive Metastore). The catalog tracks
  schema, partitioning, and snapshot history, enabling time-travel queries,
  schema evolution, and atomic reads across concurrent writers. Requires a
  running catalog service.

**Rule of thumb:** portable raw files with no catalog → `sink-parquet`; managed
lakehouse table with snapshots, time-travel, and catalog-aware readers → `sink-iceberg`.

## Lakehouse tables: Delta Lake vs. Iceberg

Delta and Iceberg are the two open lakehouse table formats; faucet ships a sink
(and source) for each. Pick by which format your query engines read:

- **`sink-delta` / `source-delta`** — the **Delta Lake** format on object
  storage, read natively by **Databricks** (via Unity Catalog) as well as Spark,
  Trino, DuckDB, and Microsoft Fabric. No catalog service is required — the
  transaction log lives beside the data in the table directory — so a bare
  `table_uri` on local FS or S3/Azure/GCS is enough. Append-only today;
  time-travel reads via `version`/`timestamp`.
- **`sink-iceberg` / `source-iceberg`** — the **Iceberg** format, registered in
  a catalog (REST, Glue, SQL, or HMS). Choose it when your platform is
  Iceberg-native or you need a shared catalog across engines. The source adds
  filter pushdown, snapshot/timestamp time travel, and `mode: incremental`,
  which reads only the snapshots appended since the last run.

**Rule of thumb:** landing data for Databricks, or you want a catalog-free Delta
table → `delta`; an Iceberg-native platform or shared catalog → `iceberg`.

## Reading from Databricks: Delta source vs. Databricks SQL source

Two ways to read from Databricks — pick by whether you want a **table** or a
**query result**:

- **`source-delta`** — scans a whole Delta table on object storage. Highest
  throughput, no running/billed compute, time travel, projection pushdown. Use
  it for full-table extracts and backfills.
- **`source-databricks`** — runs an arbitrary **SQL query** against a running
  Databricks **SQL Warehouse** via the Statement Execution API and streams the
  *result rows* (joins, aggregates, filtered slices). Use it when you need the
  output of a query rather than a raw table, and don't mind that a warehouse
  must be running (and billed) for the duration.

**Rule of thumb:** whole table, cheapest + fastest → `delta`; the result of a
SQL query (joins/aggregates/filters) → `databricks`.

## Writing to Databricks: Delta sink vs. Databricks SQL sink

- **`sink-delta`** — writes Delta files straight to object storage. No warehouse
  is billed, but it is append-only and bypasses Unity Catalog governance.
- **`sink-databricks`** — writes through a running **SQL Warehouse**, so Unity
  Catalog permissions, lineage and table features apply. It supports
  `write_mode: upsert | delete | overwrite`, `delivery: exactly_once`, and
  `schema: { on_drift: evolve }`; large pages are staged and loaded with
  `COPY INTO` (a Unity Catalog volume, or cloud storage with the
  `sink-databricks-staging` feature).

**Rule of thumb:** high-volume append into a lake path → `delta`; a governed
Unity Catalog table, keyed upserts, or exactly-once → `databricks`.

## Oracle: query source vs. LogMiner CDC

- **`source-oracle`** — runs SQL and streams rows; incremental via a bookmark
  column pushed down with `:bookmark`. Misses hard deletes.
- **`source-oracle-cdc`** — mines the redo logs with LogMiner and emits every
  committed insert/update/delete, resumable by SCN and exactly-once capable
  (ARCHIVELOG mode + supplemental logging required). Pair it with
  `faucet mirror` (an `oracle` snapshot, then CDC) for a full mirror.
- **`sink-oracle`** — array DML with `upsert` / `delete` / `overwrite`,
  exactly-once, and schema evolution.

All three load **Oracle Instant Client** at runtime, so they are not in the
default build or the prebuilt binaries — enable them with
`--features "source-oracle,source-oracle-cdc,sink-oracle"` (or `full`) and see
[Installation](../getting-started/installation.md#oracle-instant-client).

## Amazon DynamoDB

- **`source-dynamodb`** — `mode: scan` for full-table extracts (parallel
  segments; shardable across `faucet serve --cluster` workers), `mode: query`
  for one partition / index slice, and `mode: streams` for change capture from
  DynamoDB Streams. `faucet mirror` pairs a scan snapshot with a streams CDC
  pipeline; because Streams replays its retained window rather than an exact
  position, the mirror requires a keyed `write_mode: upsert` sink.
- **`sink-dynamodb`** — batched `BatchWriteItem` with `write_mode: upsert |
  delete` keyed on the table's primary key. Pair it with any source and
  `delivery: exactly_once` to get effectively-once through keyed dedup.

## Still unsure?

Run `faucet list` to see what's installed, `faucet schema source <name>` to
inspect a connector's config, and `faucet preview <config> --limit 10` to try a
source without writing anywhere.

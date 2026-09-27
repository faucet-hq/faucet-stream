# Examples

Every connector pair ships a ready-to-run config in
[`cli/examples/`](../cli/examples/). This directory adds the **local
infrastructure** to actually run the ones that need a database, broker, or
object store — one `docker compose up` brings up Postgres, MySQL, Kafka
(Redpanda), Redis, MongoDB, Elasticsearch, and MinIO (S3-compatible).

## Quick start

```bash
# 1. Build/install the CLI (full build, all connectors)
cargo install --path cli            # or: cargo install faucet-cli

# 2. Bring up the local stack
docker compose -f examples/docker-compose.yml up -d

# 3. Run an example (Postgres CDC → JSONL)
faucet run cli/examples/postgres_cdc_to_jsonl.yaml

# 4. Tear it down (and wipe volumes)
docker compose -f examples/docker-compose.yml down -v
```

A `Makefile` wraps the common steps: `make demo` (no-infra smoke test),
`make infra-up`, `make infra-down`.

## No infrastructure required

These run immediately after installing the CLI — great for a first smoke test:

| Example | Notes |
|---------|-------|
| `csv_to_jsonl.yaml` | the canonical smoke test (`make demo` runs this) |
| `csv_to_jsonl_with_contract.yaml` | a versioned data contract (`contract:` block) quarantining breaching records to a DLQ; inspect with `faucet contract` |
| `csv_to_jsonl_with_sla.yaml` | freshness/volume SLA monitoring (`sla:` block) — staleness, min-rows floor, and learned-baseline volume anomaly detection |
| `csv_to_jsonl_sql.yaml` | CSV → SQL GROUP BY + LEFT JOIN (embedded DuckDB) → JSONL; requires `--features transform-sql` |
| `csv_to_sqlite.yaml` | CSV → local SQLite file |
| `csv_to_delta.yaml` | CSV → local Apache Delta Lake table (point `table_uri` at `s3://…` + add `credentials:` for cloud) |
| `delta_to_jsonl.yaml` | local Delta Lake table → JSONL (run `csv_to_delta.yaml` first); supports `version`/`timestamp` time travel |
| `sqlite_to_jsonl.yaml`, `sqlite_to_csv.yaml` | local SQLite → file |
| `duckdb_to_jsonl.yaml` | local DuckDB (file or `:memory:`) query → JSONL |
| `sqs_to_jsonl.yaml` | AWS SQS → JSONL; runs against LocalStack (`docker run -p 4566:4566 -e SERVICES=sqs localstack/localstack`) |
| `nats_to_jsonl.yaml` | NATS → JSONL; runs against a local NATS (`docker run -p 4222:4222 nats:latest -js`) |
| `rabbitmq_to_jsonl.yaml` | RabbitMQ → JSONL, acking each page after the sink flushes it; runs against the compose `rabbitmq` service (or `docker run -p 5672:5672 -p 15672:15672 rabbitmq:3-management`) |
| `sftp_to_jsonl.yaml` | SFTP directory → JSONL over SSH; point `host`/`username`/`path` at a real server (set `SFTP_PASSWORD`) |
| `airtable_to_jsonl.yaml` | Airtable base/table → JSONL via the generic `rest` source (bearer PAT + offset-token pagination); set `AIRTABLE_TOKEN` + `AIRTABLE_BASE_ID` |
| `rest_to_jsonl.yaml`, `rest_streaming.yaml`, `rest_to_stdout_preview.yaml` | point `base_url` at any HTTP API; preview needs no sink setup |
| `rest_filter_explode_to_stdout.yaml` | `filter` + `explode` + `keys_case` against DummyJSON; demonstrates the v1 JSONPath subset and the merge rule |
| `shared_auth_rest.yaml` | one OAuth2 provider in the top-level `auth:` block shared across four matrix rows via `auth: { ref }` — single token, single-flight refresh (point `base_url` / token endpoint at a real API) |
| `rest_to_jsonl_with_vault.yaml` | Vault KV v2 secret injected as a Bearer token via `${vault:…#field}`; requires `VAULT_ADDR` + `VAULT_TOKEN` and `--features secrets-vault` |
| `websocket_to_jsonl.yaml` | none (live public WS endpoint — Binance BTC/USDT trade stream, no auth) |
| `kinesis_to_jsonl.yaml` | AWS Kinesis → JSONL with resumable per-shard checkpoints; runs against LocalStack (`docker run -p 4566:4566 -e SERVICES=kinesis localstack/localstack`) |
| `dynamodb_to_jsonl.yaml` | DynamoDB parallel scan → JSONL with per-segment bookmarks; runs against DynamoDB Local (`docker run -p 8000:8000 amazon/dynamodb-local`, then set `endpoint_url: http://localhost:8000`) |
| `dynamodb_mirror_to_postgres.yaml` | `faucet mirror` — DynamoDB scan snapshot, then DynamoDB Streams CDC into a keyed Postgres upsert (DynamoDB Local + the compose `postgres` service) |
| `oracle_to_postgres.yaml` | Oracle → Postgres incremental copy with keyed upsert; needs `--features source-oracle`, Oracle Instant Client, and an Oracle database (`docker run -p 1521:1521 -e ORACLE_PASSWORD=… gvenzl/oracle-free:23-slim`) |
| `oracle_cdc_mirror_to_postgres.yaml` | `faucet mirror` — Oracle snapshot, then LogMiner CDC into a keyed Postgres upsert; needs `--features source-oracle,source-oracle-cdc`, Instant Client, ARCHIVELOG + supplemental logging |
| `iceberg_to_jsonl.yaml` | Apache Iceberg table → JSONL with `mode: incremental` (only snapshots appended since the last run); needs an Iceberg REST catalog (Polaris / Nessie / Lakekeeper) |
| `spanner_to_jsonl.yaml` | Cloud Spanner → JSONL with incremental `@bookmark` replication; runs against the Spanner emulator (`docker run -p 9010:9010 gcr.io/cloud-spanner-emulator/emulator`) |
| `backfill_sqlite_to_jsonl.yaml` | `faucet backfill` — replay a date range from a local SQLite table one day per window unit, one JSONL file per unit (`${backfill.*}` tokens, durable `--resume` marker) |
| `scheduled_nightly.yaml` | `faucet schedule` — CSV→JSONL pipeline on a nightly cron at 02:00 Pacific; demonstrates timezone, overlap_policy, and max_consecutive_failures |

> REST / GraphQL / XML / gRPC / webhook source examples hit an external endpoint
> (the configs use a placeholder `base_url`). Edit it to a real API — there's no
> mock server in the stack.

## Orchestration (Airflow / Dagster + dbt)

[`orchestration/`](orchestration/) is a complete **ELT** recipe: load with
faucet, transform with dbt, schedule with Airflow or Dagster. faucet is
complementary to dbt — it owns Extract + Load (raw lossless rows into Postgres),
dbt owns Transform (typed, tested staging models), and because faucet is a single
binary the orchestrator just shells out to `faucet run`. See
[`orchestration/README.md`](orchestration/README.md) and the
[Orchestration cookbook page](../docs/book/src/cookbook/orchestration.md).

## Dashboards & alerts

The stack also provisions the shipped observability artifacts (issue #200):

```bash
docker compose -f examples/docker-compose.yml up -d prometheus grafana
```

Grafana at <http://localhost:3000> (admin / admin) pre-loads the four
`observability/grafana/` dashboards; Prometheus at <http://localhost:9095>
evaluates `observability/prometheus/alerts.yml` and scrapes a faucet process
on the host (enable the exporter with
`observability: { prometheus: { listen_addr: 0.0.0.0:9464 } }`).

## Covered by the local Docker stack

The service column lists what each example touches; all are provided by
`docker-compose.yml`. **S3** examples use MinIO — set the S3 endpoint to
`http://localhost:9000` with credentials `faucet` / `faucetfaucet`.

| Example | Services |
|---------|----------|
| `postgres_cdc_to_jsonl.yaml` | postgres (logical replication preconfigured) |
| `postgres_cdc_to_postgres_upsert.yaml` | postgres (CDC source + an upsert mirror table; `cdc_unwrap` + `write_mode: upsert`, effectively-once) |
| `postgres_cdc_to_bigquery_upsert.yaml` | postgres (CDC source) + BigQuery (`write_mode: upsert`, in-place MERGE, effectively-once; requires GCP credentials) |
| `mongodb_cdc_to_jsonl.yaml` | mongodb (single-node replica set preconfigured; Change Streams) |
| `mysql_cdc_to_jsonl.yaml` | mysql (binlog enabled; `repl` user with replication grants preconfigured) |
| `rest_to_postgres.yaml`, `rest_to_postgres_with_quality.yaml`, `mongodb_to_postgres.yaml`, `graphql_to_postgres.yaml`, `webhook_to_postgres.yaml` | postgres (+ source) |
| `mysql_to_postgres.yaml` | mysql, postgres |
| `csv_to_mysql.yaml`, `redis_to_mysql.yaml` | mysql (+ source) |
| `mssql_to_jsonl.yaml` | mssql + mssql-init (incremental source; seeds `sales.dbo.users`) |
| `kafka_to_mssql.yaml` | mssql + mssql-init, redpanda (produce JSON to `events` matching `analytics.dbo.events`) |
| `rest_to_mssql.yaml` | mssql + mssql-init (json_column; auto-creates `raw.dbo.products_raw`) |
| `redis_to_sqlite.yaml`, `mongodb_to_redis.yaml`, `elasticsearch_to_redis.yaml` | redis (+ source) |
| `kafka_to_jsonl.yaml`, `rest_to_kafka.yaml` | redpanda (Kafka API) |
| `kafka_to_postgres_exactly_once.yaml` | redpanda (Kafka API) + postgres (effectively-once: Kafka offsets bookmark + atomic `_faucet_commit_token` watermark) |
| `mongodb_to_elasticsearch.yaml`, `postgres_to_elasticsearch.yaml`, `grpc_to_elasticsearch.yaml` | elasticsearch (+ source) |
| `elasticsearch_to_s3.yaml`, `postgres_to_s3.yaml`, `rest_to_s3.yaml`, `xml_to_s3.yaml` | minio (S3) (+ source) |
| `s3_to_postgres.yaml`, `s3_to_mongodb.yaml` | minio (S3), target |
| `mongodb_to_redis.yaml`, `xml_to_mongodb.yaml`, `mongodb_to_parquet.yaml` | mongodb (single-node replica set; query mode connects to the primary); `mongodb_to_parquet` writes local Parquet files (no cloud infra) |
| `webhook_to_csv.yaml`, `webhook_to_http.yaml`, `grpc_to_http.yaml` | none external beyond the source/HTTP target |
| `dag_users_posts.yaml`, `rest_users_posts_dag.yaml`, `rest_to_bigquery_matrix.yaml`, `templates_*.yaml` | demonstrate matrix / DAG / template syntax (REST source) |
| `matrix_depends_on.yaml` | demonstrates `depends_on` completion ordering between matrix rows (local CSV/JSONL, no infra) |

## Cloud credentials required (not in the stack)

BigQuery, Snowflake, and GCS need real cloud projects and credentials, so they
can't run against local Docker. Provide credentials via env vars / service-account
files as each config shows:

`csv_to_bigquery.yaml`, `graphql_to_bigquery.yaml`, `mysql_to_bigquery.yaml`,
`postgres_to_bigquery.yaml`, `rest_to_bigquery.yaml`, `s3_to_bigquery.yaml`,
`mysql_to_snowflake.yaml`, `postgres_to_snowflake.yaml`, `s3_to_snowflake.yaml`.

Databricks needs a workspace, a SQL warehouse and a token (`DATABRICKS_TOKEN`):
`databricks_to_jsonl.yaml` (query source) and `postgres_to_databricks_upsert.yaml`
(`MERGE` upserts into a Unity Catalog Delta table, staged through a volume, with
`schema: { on_drift: evolve }`).

## OTLP / OpenTelemetry export

| File | Notes |
|------|-------|
| `examples/infra/otel-collector.yaml` | OTLP collector for testing `observability.otel` export (build the CLI with `--features otel`). Run with `otelcol --config examples/infra/otel-collector.yaml` to receive traces and metrics on gRPC `:4317` and HTTP `:4318`. |

## OpenLineage emission

| Example | Notes |
|---------|-------|
| `postgres_to_bigquery_with_lineage.yaml` | Postgres → BigQuery with a top-level `lineage:` block — emits OpenLineage RunEvents (schema + column-lineage facets) to a Marquez HTTP endpoint. Needs `--features lineage`, a BigQuery project (`GCP_KEY_JSON`), and `MARQUEZ_URL` (or swap in the commented `transport: { type: file }` for local testing) |
| `clickhouse_to_jsonl.yaml` | ClickHouse query source (HTTP `JSONEachRow`) → JSONL |
| `csv_to_clickhouse.yaml` | CSV → ClickHouse sink (`INSERT … FORMAT JSONEachRow`, optional `async_insert`) |
| `redshift_to_jsonl.yaml` | Amazon Redshift query source (Postgres wire) → JSONL |
| `pubsub_to_jsonl.yaml` | Google Cloud Pub/Sub subscription → JSONL (streaming pull, at-least-once ack at page boundaries) |
| `azure_blob_to_jsonl.yaml` | Azure Blob / ADLS Gen2 object source → JSONL |
| `mssql_cdc_to_postgres_upsert.yaml` | SQL Server CDC → Postgres upsert mirror (`cdc_unwrap` + `write_mode: upsert`) |

## Tips

- `faucet validate <config>` checks any config without running it (and without
  infra) — useful in CI.
- `faucet preview <config> --limit 10` runs just the source and prints records.
- Most examples read secrets from the environment via `${env:VAR}`; export them
  or drop a sibling `.env` before running.
- The Postgres service seeds a `users` table and a `faucet_pub` publication (see
  [`infra/postgres-init.sql`](infra/postgres-init.sql)) so the CDC and query
  examples work immediately.

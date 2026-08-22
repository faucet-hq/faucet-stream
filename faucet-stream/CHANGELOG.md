# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.8.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.7.5...faucet-stream-v1.8.0) - 2026-08-22

### Features

- Config-driven parity — REST headers/async-url, XML decode/paging, flow-auth sign/capture (#539–#544) ([#545](https://github.com/faucet-hq/faucet-stream/pull/545))
- Rest partitions fan-out + repeated query params, cross_join transform, ClickHouse staged load (#535/#536/#534/#528) ([#537](https://github.com/faucet-hq/faucet-stream/pull/537))
- Datetime window slicing, tree_flatten, staged-load foundation, persistent run logs, chained discovery (#527–#531) ([#532](https://github.com/faucet-hq/faucet-stream/pull/532))
- *(transforms)* Inbuilt reshape transforms — json_encode, unpivot, lookup ([#520](https://github.com/faucet-hq/faucet-stream/pull/520))
- *(source-rest)* Add response_format json|csv|excel for authed file bodies ([#508](https://github.com/faucet-hq/faucet-stream/pull/508))
- MTLS, ES overwrite, OAuth1, and completeness reconciliation ([#506](https://github.com/faucet-hq/faucet-stream/pull/506))

## [1.7.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.7.4...faucet-stream-v1.7.5) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-source-graphql, faucet-source-xml, faucet-source-grpc, faucet-source-postgres, faucet-source-mysql, faucet-source-gcs, faucet-source-s3, faucet-source-mongodb, faucet-source-redis, faucet-source-sqlite, faucet-source-csv, faucet-source-elasticsearch, faucet-source-parquet, faucet-sink-postgres, faucet-sink-snowflake, faucet-sink-mysql, faucet-sink-sqlite, faucet-sink-gcs, faucet-sink-s3, faucet-sink-mongodb, faucet-sink-redis, faucet-sink-csv, faucet-sink-http, faucet-sink-kafka, faucet-sink-stdout, faucet-sink-parquet, faucet-source-duckdb, faucet-source-sftp, faucet-auth, faucet-lineage, faucet-transform-sql, faucet-transform-wasm, faucet-common-bigquery, faucet-common-gcs, faucet-common-kafka, faucet-common-snowflake, faucet-common-kinesis, faucet-common-spanner, faucet-common-redshift, faucet-common-pubsub, faucet-common-clickhouse, faucet-common-azure, faucet-source-rest, faucet-source-singer, faucet-source-kafka, faucet-source-kinesis, faucet-source-mssql, faucet-source-mongodb-cdc, faucet-source-mysql-cdc, faucet-source-mssql-cdc, faucet-source-redshift, faucet-source-pubsub, faucet-source-clickhouse, faucet-source-azure-blob, faucet-source-webhook, faucet-source-websocket, faucet-source-postgres-cdc, faucet-source-bigquery, faucet-source-snowflake, faucet-source-spanner, faucet-source-delta, faucet-source-databricks, faucet-sink-bigquery, faucet-sink-jsonl, faucet-sink-mssql, faucet-sink-elasticsearch, faucet-sink-kinesis, faucet-sink-iceberg, faucet-sink-delta, faucet-sink-spanner, faucet-sink-redshift, faucet-sink-pubsub, faucet-sink-clickhouse, faucet-sink-azure-blob, faucet-sink-duckdb, faucet-common-sqs, faucet-source-sqs, faucet-sink-sqs, faucet-common-nats, faucet-source-nats, faucet-sink-nats, faucet-common-sftp, faucet-sink-sftp, faucet-state-redis, faucet-state-postgres

## [1.7.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.7.3...faucet-stream-v1.7.4) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-sink-bigquery, faucet-sink-postgres, faucet-sink-mysql, faucet-sink-sqlite, faucet-sink-mssql, faucet-sink-mongodb, faucet-sink-elasticsearch, faucet-sink-spanner, faucet-auth, faucet-lineage, faucet-transform-sql, faucet-transform-wasm, faucet-common-bigquery, faucet-common-gcs, faucet-common-kafka, faucet-common-snowflake, faucet-common-kinesis, faucet-common-spanner, faucet-common-redshift, faucet-common-pubsub, faucet-common-clickhouse, faucet-common-azure, faucet-source-rest, faucet-source-singer, faucet-source-graphql, faucet-source-xml, faucet-source-grpc, faucet-source-kafka, faucet-source-kinesis, faucet-source-postgres, faucet-source-mysql, faucet-source-mssql, faucet-source-gcs, faucet-source-s3, faucet-source-mongodb, faucet-source-mongodb-cdc, faucet-source-mysql-cdc, faucet-source-mssql-cdc, faucet-source-redshift, faucet-source-pubsub, faucet-source-clickhouse, faucet-source-azure-blob, faucet-source-redis, faucet-source-webhook, faucet-source-websocket, faucet-source-sqlite, faucet-source-csv, faucet-source-elasticsearch, faucet-source-parquet, faucet-source-postgres-cdc, faucet-source-bigquery, faucet-source-snowflake, faucet-source-spanner, faucet-source-delta, faucet-source-databricks, faucet-sink-jsonl, faucet-sink-snowflake, faucet-sink-gcs, faucet-sink-s3, faucet-sink-redis, faucet-sink-csv, faucet-sink-http, faucet-sink-kafka, faucet-sink-kinesis, faucet-sink-stdout, faucet-sink-parquet, faucet-sink-iceberg, faucet-sink-delta, faucet-sink-redshift, faucet-sink-pubsub, faucet-sink-clickhouse, faucet-sink-azure-blob, faucet-source-duckdb, faucet-sink-duckdb, faucet-common-sqs, faucet-source-sqs, faucet-sink-sqs, faucet-common-nats, faucet-source-nats, faucet-sink-nats, faucet-common-sftp, faucet-source-sftp, faucet-sink-sftp, faucet-state-redis, faucet-state-postgres

## [1.7.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.7.2...faucet-stream-v1.7.3) - 2026-08-10

### Miscellaneous

- Updated the following local packages: faucet-source-postgres, faucet-source-mysql, faucet-source-mssql, faucet-source-gcs, faucet-source-s3, faucet-source-mongodb, faucet-source-sqlite, faucet-source-elasticsearch, faucet-source-bigquery, faucet-source-snowflake, faucet-source-spanner, faucet-sink-gcs, faucet-sink-s3

## [1.7.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.7.1...faucet-stream-v1.7.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-transform-sql, faucet-transform-wasm, faucet-common-spanner, faucet-source-graphql, faucet-source-postgres, faucet-source-redshift, faucet-source-sqlite, faucet-source-spanner, faucet-sink-mysql, faucet-sink-mssql, faucet-sink-redshift, faucet-sink-duckdb, faucet-source-sqs, faucet-auth, faucet-common-bigquery, faucet-common-gcs, faucet-common-kafka, faucet-common-snowflake, faucet-common-kinesis, faucet-common-redshift, faucet-common-pubsub, faucet-common-clickhouse, faucet-common-azure, faucet-source-rest, faucet-source-singer, faucet-source-grpc, faucet-source-kafka, faucet-source-kinesis, faucet-source-mysql, faucet-source-mssql, faucet-source-gcs, faucet-source-s3, faucet-source-mongodb, faucet-source-mongodb-cdc, faucet-source-mysql-cdc, faucet-source-mssql-cdc, faucet-source-pubsub, faucet-source-clickhouse, faucet-source-azure-blob, faucet-source-redis, faucet-source-webhook, faucet-source-websocket, faucet-source-csv, faucet-source-elasticsearch, faucet-source-parquet, faucet-source-postgres-cdc, faucet-source-bigquery, faucet-source-snowflake, faucet-source-delta, faucet-source-databricks, faucet-sink-bigquery, faucet-sink-postgres, faucet-sink-jsonl, faucet-sink-snowflake, faucet-sink-sqlite, faucet-sink-gcs, faucet-sink-s3, faucet-sink-mongodb, faucet-sink-redis, faucet-sink-csv, faucet-sink-elasticsearch, faucet-sink-http, faucet-sink-kafka, faucet-sink-kinesis, faucet-sink-stdout, faucet-sink-parquet, faucet-sink-iceberg, faucet-sink-delta, faucet-sink-spanner, faucet-sink-pubsub, faucet-sink-clickhouse, faucet-sink-azure-blob, faucet-source-duckdb, faucet-common-sqs, faucet-sink-sqs, faucet-common-nats, faucet-source-nats, faucet-sink-nats, faucet-common-sftp, faucet-source-sftp, faucet-sink-sftp, faucet-state-redis, faucet-state-postgres

## [1.7.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.6.0...faucet-stream-v1.7.0) - 2026-08-01

### Features

- *(deploy)* Dockerfile + Helm chart for container/Kubernetes deployment ([#439](https://github.com/faucet-hq/faucet-stream/pull/439))

## [1.6.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.5.0...faucet-stream-v1.6.0) - 2026-07-31

### Bug Fixes

- Keep transform additions a minor release — pin touched crates to 1.x

### Documentation

- Single source of truth for connector/crate counts
- *(gtm)* Elevate Singer on-ramp, fix conformance + connector-count drift
- *(readme)* Keep downloads badge link on faucet-stream
- *(readme)* Point downloads badge at faucet-core

### Features

- *(transform-wasm)* Add WebAssembly (wasmtime) per-record transform ([#124](https://github.com/faucet-hq/faucet-stream/pull/124)) ([#426](https://github.com/faucet-hq/faucet-stream/pull/426))
- *(transforms)* [**breaking**] Hash / json_parse / coalesce / split / join, value_case title+capitalize, keys_case dot, stdout csv (#403–#409) — faucet-core 2.0.0 ([#418](https://github.com/faucet-hq/faucet-stream/pull/418))
- *(connectors)* Add DuckDB, SQS, NATS, SFTP connector pairs + Airtable REST recipe

### Testing

- *(conformance)* Wire the battery into delta/clickhouse/pubsub/azure-blob; fix databricks tier (#396, #397)

## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.4.0...faucet-stream-v1.5.0) - 2026-07-24

### Documentation

- Architecture-review follow-ups — ADRs, SDK streaming docs, build_pipeline refactor, Arrow benchmark ([#324](https://github.com/faucet-hq/faucet-stream/pull/324)) ([#373](https://github.com/faucet-hq/faucet-stream/pull/373))
- WS-11 — Airflow/Dagster+dbt orchestration recipe & evergreen posts ([#314](https://github.com/faucet-hq/faucet-stream/pull/314)) ([#361](https://github.com/faucet-hq/faucet-stream/pull/361))
- Reconcile counts, annotate benchmark, per-page SEO (closes #335, #336, #337) ([#339](https://github.com/faucet-hq/faucet-stream/pull/339))
- Upgrade Mermaid diagrams to V2 role-coded style ([#340](https://github.com/faucet-hq/faucet-stream/pull/340))
- Brand all GitHub-viewed Mermaid diagrams with the faucet teal theme ([#338](https://github.com/faucet-hq/faucet-stream/pull/338))
- Brand the docs site with the faucet-stream teal (#14B8A6) ([#332](https://github.com/faucet-hq/faucet-stream/pull/332))
- Architecture learning path, interactive docs site, platform/governance repositioning (+ release-plz retry fix) ([#326](https://github.com/faucet-hq/faucet-stream/pull/326))

### Features

- Live `run` progress + Arrow BigQuery & Snowflake paths (#385, #380, #381) ([#395](https://github.com/faucet-hq/faucet-stream/pull/395))
- Arrow columnar path for S3, GCS, and Databricks — RFC 0002 Phase 4 ([#375](https://github.com/faucet-hq/faucet-stream/pull/375)) ([#382](https://github.com/faucet-hq/faucet-stream/pull/382))
- *(cli)* Config-change preview — `faucet plan --diff` ([#374](https://github.com/faucet-hq/faucet-stream/pull/374)) ([#378](https://github.com/faucet-hq/faucet-stream/pull/378))
- *(connectors)* Redshift, Pub/Sub, ClickHouse, Azure Blob, and SQL Server CDC ([#362](https://github.com/faucet-hq/faucet-stream/pull/362))

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.3.0...faucet-stream-v1.4.0) - 2026-07-17

### Features

- *(databricks)* Databricks SQL query source via Statement Execution API ([#320](https://github.com/faucet-hq/faucet-stream/pull/320))
- *(delta)* Apache Delta Lake source + sink via delta-rs ([#319](https://github.com/faucet-hq/faucet-stream/pull/319))
- Encryption at rest for state/DLQ + live TUI for faucet run ([#315](https://github.com/faucet-hq/faucet-stream/pull/315))
- Google Cloud Spanner source + sink connectors ([#312](https://github.com/faucet-hq/faucet-stream/pull/312))
- Connector conformance battery + tiers, FCP spec, sink-bound benchmark, sink config fixes ([#307](https://github.com/faucet-hq/faucet-stream/pull/307))
- *(cli)* Plugin loading, schema config, connector scaffolding + registry, plan/dev, hot reload ([#306](https://github.com/faucet-hq/faucet-stream/pull/306))
- AWS Kinesis source + sink connectors and shipped Grafana dashboards / Prometheus alerts
- Faucet discover (live source introspection) + faucet backfill (resumable historical replay)

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/faucet-hq/faucet-stream/pull/311))

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.2.0...faucet-stream-v1.3.0) - 2026-07-10

### Features

- Typed delivery guarantees, effectively-once coverage expansion, and prebuilt binary distribution ([#294](https://github.com/faucet-hq/faucet-stream/pull/294))
- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

### Miscellaneous

- *(dist)* Homebrew tap homebrew-faucet-stream, formula faucet-cli ([#295](https://github.com/faucet-hq/faucet-stream/pull/295))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.1.1...faucet-stream-v1.2.0) - 2026-07-08

### Documentation

- Ship interactive local demo (try-local.sh) + quickstart & console screenshots ([#288](https://github.com/faucet-hq/faucet-stream/pull/288))

### Features

- *(masking)* PII detection + column-level masking policies ([#206](https://github.com/faucet-hq/faucet-stream/pull/206))
- *(cli)* Depends_on — completion ordering between matrix rows ([#276](https://github.com/faucet-hq/faucet-stream/pull/276))
- *(cli)* Data-freshness & volume SLA monitoring with anomaly alerts ([#275](https://github.com/faucet-hq/faucet-stream/pull/275))
- *(cli)* Faucet test — fixture-based offline pipeline testing ([#273](https://github.com/faucet-hq/faucet-stream/pull/273))
- *(core)* Data contracts — versioned output schema/constraints enforced per page ([#272](https://github.com/faucet-hq/faucet-stream/pull/272))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.1.0...faucet-stream-v1.1.1) - 2026-06-22

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-lineage, faucet-common-gcs, faucet-source-rest, faucet-source-graphql, faucet-source-xml, faucet-source-postgres, faucet-source-mysql, faucet-source-mssql, faucet-source-gcs, faucet-source-s3, faucet-source-mongodb-cdc, faucet-source-mysql-cdc, faucet-source-redis, faucet-source-sqlite, faucet-source-csv, faucet-source-parquet, faucet-source-postgres-cdc, faucet-source-snowflake, faucet-sink-bigquery, faucet-sink-postgres, faucet-sink-snowflake, faucet-sink-mysql, faucet-sink-sqlite, faucet-sink-mssql, faucet-sink-csv, faucet-sink-elasticsearch, faucet-sink-http, faucet-sink-kafka, faucet-sink-parquet, faucet-sink-iceberg, faucet-auth, faucet-transform-sql, faucet-common-bigquery, faucet-common-kafka, faucet-common-snowflake, faucet-source-grpc, faucet-source-kafka, faucet-source-mongodb, faucet-source-webhook, faucet-source-websocket, faucet-source-elasticsearch, faucet-source-bigquery, faucet-sink-jsonl, faucet-sink-gcs, faucet-sink-s3, faucet-sink-mongodb, faucet-sink-redis, faucet-sink-stdout, faucet-state-redis, faucet-state-postgres

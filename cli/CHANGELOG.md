# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.11.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.10.0...faucet-cli-v1.11.0) - 2026-08-22

### Features

- Config-driven parity — REST headers/async-url, XML decode/paging, flow-auth sign/capture (#539–#544) ([#545](https://github.com/faucet-hq/faucet-stream/pull/545))
- Rest partitions fan-out + repeated query params, cross_join transform, ClickHouse staged load (#535/#536/#534/#528) ([#537](https://github.com/faucet-hq/faucet-stream/pull/537))
- Datetime window slicing, tree_flatten, staged-load foundation, persistent run logs, chained discovery (#527–#531) ([#532](https://github.com/faucet-hq/faucet-stream/pull/532))
- Response-decode + async-job (rest), scoped overwrite, run/lineage metadata columns ([#526](https://github.com/faucet-hq/faucet-stream/pull/526))
- *(source)* Composable auth flows, OData mode + $metadata discovery, server-side incremental push-down ([#524](https://github.com/faucet-hq/faucet-stream/pull/524))
- *(transforms)* Inbuilt reshape transforms — json_encode, unpivot, lookup ([#520](https://github.com/faucet-hq/faucet-stream/pull/520))
- *(cli)* Discovery-driven request matrix (discover + for_each cross-product) ([#509](https://github.com/faucet-hq/faucet-stream/pull/509))
- *(source-rest)* Add response_format json|csv|excel for authed file bodies ([#508](https://github.com/faucet-hq/faucet-stream/pull/508))
- MTLS, ES overwrite, OAuth1, and completeness reconciliation ([#506](https://github.com/faucet-hq/faucet-stream/pull/506))
- *(sinks)* Add write_mode: overwrite (full-refresh) across data-storage sinks ([#493](https://github.com/faucet-hq/faucet-stream/pull/493))

## [1.10.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.9.0...faucet-cli-v1.10.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))
- *(sources)* Fail-fast config validation for csv/duckdb/elasticsearch/gcs/graphql ([#489](https://github.com/faucet-hq/faucet-stream/pull/489))

## [1.9.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.8.1...faucet-cli-v1.9.0) - 2026-08-15

### Features

- Generic range partitioning with bound discovery ([#486](https://github.com/faucet-hq/faucet-stream/pull/486))
- Scoped cleanup — delete records missing from a source's completeness claim ([#484](https://github.com/faucet-hq/faucet-stream/pull/484))
- *(serve)* Per-run completion callbacks, with run identity in the notify payload ([#482](https://github.com/faucet-hq/faucet-stream/pull/482))

## [1.8.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.8.0...faucet-cli-v1.8.1) - 2026-08-10

### Miscellaneous

- Updated the following local packages: faucet-source-postgres, faucet-source-mysql, faucet-source-mssql, faucet-source-gcs, faucet-source-s3, faucet-source-mongodb, faucet-source-sqlite, faucet-source-elasticsearch, faucet-source-bigquery, faucet-source-snowflake, faucet-source-spanner, faucet-sink-gcs, faucet-sink-s3

## [1.8.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.7.1...faucet-cli-v1.8.0) - 2026-08-09

### Bug Fixes

- Second-pass audit — wide-integer corruption in the Arrow/SQL shim and SQL binds, backfill DST windows (#460, #461, #462) ([#463](https://github.com/faucet-hq/faucet-stream/pull/463))
- Resolve the fourth hardening audit — topology governance bypass, SQS at-most-once, control-plane secret leaks ([#456](https://github.com/faucet-hq/faucet-stream/pull/456)) ([#457](https://github.com/faucet-hq/faucet-stream/pull/457))

### Features

- *(conformance,xml)* Registry-allowlist parity + capability matrix ([#465](https://github.com/faucet-hq/faucet-stream/pull/465)), soap: ergonomics block ([#468](https://github.com/faucet-hq/faucet-stream/pull/468)) ([#469](https://github.com/faucet-hq/faucet-stream/pull/469))
- *(topology)* Exactly-once delivery + per-node SLA/notify/lineage/catalog ([#464](https://github.com/faucet-hq/faucet-stream/pull/464))
- *(templates)* Release lifecycle (launch/rollback/deprecate) + console versions page ([#455](https://github.com/faucet-hq/faucet-stream/pull/455))
- *(serve)* Pipeline template registry + parameterized trigger API ([#452](https://github.com/faucet-hq/faucet-stream/pull/452))

## [1.7.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.6.0...faucet-cli-v1.7.0) - 2026-07-31

### Bug Fixes

- Keep transform additions a minor release — pin touched crates to 1.x

### Features

- *(transform-wasm)* Add WebAssembly (wasmtime) per-record transform ([#124](https://github.com/faucet-hq/faucet-stream/pull/124)) ([#426](https://github.com/faucet-hq/faucet-stream/pull/426))
- *(iceberg)* Additive schema evolution via iceberg-rust 0.10.0 ([#255](https://github.com/faucet-hq/faucet-stream/pull/255)); fix(cli): run summary → stderr ([#424](https://github.com/faucet-hq/faucet-stream/pull/424)) ([#425](https://github.com/faucet-hq/faucet-stream/pull/425))
- *(cli)* MCP server — agent-operable control plane ([#420](https://github.com/faucet-hq/faucet-stream/pull/420)) ([#422](https://github.com/faucet-hq/faucet-stream/pull/422))
- *(cli)* Topology mode — fan-out (tee), fan-in (merge), and cross-source join (#71, #72) ([#421](https://github.com/faucet-hq/faucet-stream/pull/421))
- *(transforms)* [**breaking**] Hash / json_parse / coalesce / split / join, value_case title+capitalize, keys_case dot, stdout csv (#403–#409) — faucet-core 2.0.0 ([#418](https://github.com/faucet-hq/faucet-stream/pull/418))
- *(connectors)* Add DuckDB, SQS, NATS, SFTP connector pairs + Airtable REST recipe

### Testing

- *(cli)* Whitelist SFTP_PASSWORD + AIRTABLE_* env for example-validate test

## [1.6.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.5.0...faucet-cli-v1.6.0) - 2026-07-24

### Documentation

- Architecture-review follow-ups — ADRs, SDK streaming docs, build_pipeline refactor, Arrow benchmark ([#324](https://github.com/faucet-hq/faucet-stream/pull/324)) ([#373](https://github.com/faucet-hq/faucet-stream/pull/373))

### Features

- Live `run` progress + Arrow BigQuery & Snowflake paths (#385, #380, #381) ([#395](https://github.com/faucet-hq/faucet-stream/pull/395))
- *(cli)* Tier-3 commands — fmt, explain, history, run --output json (#387,#389,#390,#391) ([#394](https://github.com/faucet-hq/faucet-stream/pull/394))
- *(cli)* `faucet migrate` + `doctor --offline` config linter (#388, #392) ([#393](https://github.com/faucet-hq/faucet-stream/pull/393))
- *(cli)* Shell tab-completion — `faucet completions` + dynamic (registry/config-aware) completion ([#383](https://github.com/faucet-hq/faucet-stream/pull/383)) ([#384](https://github.com/faucet-hq/faucet-stream/pull/384))
- Arrow columnar path for S3, GCS, and Databricks — RFC 0002 Phase 4 ([#375](https://github.com/faucet-hq/faucet-stream/pull/375)) ([#382](https://github.com/faucet-hq/faucet-stream/pull/382))
- *(cli)* Runtime matrix-row selection model (#370, #371, #376, #377) ([#379](https://github.com/faucet-hq/faucet-stream/pull/379))
- *(cli)* Config-change preview — `faucet plan --diff` ([#374](https://github.com/faucet-hq/faucet-stream/pull/374)) ([#378](https://github.com/faucet-hq/faucet-stream/pull/378))
- *(cli)* Surface connector conformance maturity tiers ([#330](https://github.com/faucet-hq/faucet-stream/pull/330)) ([#367](https://github.com/faucet-hq/faucet-stream/pull/367))
- *(connectors)* Redshift, Pub/Sub, ClickHouse, Azure Blob, and SQL Server CDC ([#362](https://github.com/faucet-hq/faucet-stream/pull/362))

## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.4.0...faucet-cli-v1.5.0) - 2026-07-17

### Bug Fixes

- Resolve #321 medium/low audit findings (quality/contract equality, CDC, pagination, serve, observability) ([#323](https://github.com/faucet-hq/faucet-stream/pull/323))
- Resolve #321 critical/high audit findings (exactly-once, cluster, transform-sql, compression) ([#322](https://github.com/faucet-hq/faucet-stream/pull/322))

### Features

- *(databricks)* Databricks SQL query source via Statement Execution API ([#320](https://github.com/faucet-hq/faucet-stream/pull/320))
- *(delta)* Apache Delta Lake source + sink via delta-rs ([#319](https://github.com/faucet-hq/faucet-stream/pull/319))
- Encryption at rest for state/DLQ + live TUI for faucet run ([#315](https://github.com/faucet-hq/faucet-stream/pull/315))
- Google Cloud Spanner source + sink connectors ([#312](https://github.com/faucet-hq/faucet-stream/pull/312))
- Connector conformance battery + tiers, FCP spec, sink-bound benchmark, sink config fixes ([#307](https://github.com/faucet-hq/faucet-stream/pull/307))
- *(cli)* Plugin loading, schema config, connector scaffolding + registry, plan/dev, hot reload ([#306](https://github.com/faucet-hq/faucet-stream/pull/306))
- AWS Kinesis source + sink connectors and shipped Grafana dashboards / Prometheus alerts
- Faucet discover (live source introspection) + faucet backfill (resumable historical replay)

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.3.0...faucet-cli-v1.4.0) - 2026-07-10

### Features

- Typed delivery guarantees, effectively-once coverage expansion, and prebuilt binary distribution ([#294](https://github.com/faucet-hq/faucet-stream/pull/294))
- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

### Miscellaneous

- *(dist)* Homebrew tap homebrew-faucet-stream, formula faucet-cli ([#295](https://github.com/faucet-hq/faucet-stream/pull/295))

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.2.0...faucet-cli-v1.3.0) - 2026-07-08

### Features

- Persistent Data Movement Catalog — datasets, schema timelines & lineage graph ([#286](https://github.com/faucet-hq/faucet-stream/pull/286))
- *(cli)* DLQ replay & management — faucet dlq inspect / replay / discard ([#281](https://github.com/faucet-hq/faucet-stream/pull/281)) ([#285](https://github.com/faucet-hq/faucet-stream/pull/285))
- *(cli)* Notification & incident routing — Slack / PagerDuty / webhook ([#280](https://github.com/faucet-hq/faucet-stream/pull/280)) ([#284](https://github.com/faucet-hq/faucet-stream/pull/284))
- *(masking)* PII detection + column-level masking policies ([#206](https://github.com/faucet-hq/faucet-stream/pull/206))
- *(serve)* RBAC + audit log for the control plane ([#205](https://github.com/faucet-hq/faucet-stream/pull/205)) ([#277](https://github.com/faucet-hq/faucet-stream/pull/277))
- *(cli)* Depends_on — completion ordering between matrix rows ([#276](https://github.com/faucet-hq/faucet-stream/pull/276))
- *(cli)* Data-freshness & volume SLA monitoring with anomaly alerts ([#275](https://github.com/faucet-hq/faucet-stream/pull/275))
- *(cli)* Faucet test — fixture-based offline pipeline testing ([#273](https://github.com/faucet-hq/faucet-stream/pull/273))
- *(core)* Data contracts — versioned output schema/constraints enforced per page ([#272](https://github.com/faucet-hq/faucet-stream/pull/272))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.1.0...faucet-cli-v1.2.0) - 2026-06-22

### Bug Fixes

- Resolve all 18 Low reliability/data-integrity findings (F40–F57, #264) ([#267](https://github.com/faucet-hq/faucet-stream/pull/267))
- Resolve all 20 Medium reliability/data-integrity findings (F20–F39, #264) ([#266](https://github.com/faucet-hq/faucet-stream/pull/266))
- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/faucet-hq/faucet-stream/pull/264)) ([#265](https://github.com/faucet-hq/faucet-stream/pull/265))
- *(triggers)* Reject unknown fields in --triggers config ([#232](https://github.com/faucet-hq/faucet-stream/pull/232)) ([#246](https://github.com/faucet-hq/faucet-stream/pull/246))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))

### Features

- Serve cluster Mode B — source-shard distribution across workers ([#230](https://github.com/faucet-hq/faucet-stream/pull/230)) ([#263](https://github.com/faucet-hq/faucet-stream/pull/263))
- *(observability)* OpenTelemetry (OTLP) trace + metric export ([#201](https://github.com/faucet-hq/faucet-stream/pull/201)) ([#259](https://github.com/faucet-hq/faucet-stream/pull/259))
- Schema-drift handling policy (warn/evolve/ignore/quarantine/fail) ([#194](https://github.com/faucet-hq/faucet-stream/pull/194))
- *(sink-kafka)* Exactly-once delivery via transactional producer ([#216](https://github.com/faucet-hq/faucet-stream/pull/216)) ([#253](https://github.com/faucet-hq/faucet-stream/pull/253))
- Unified resilience policy (retry / circuit-breaker / poison-pill) ([#252](https://github.com/faucet-hq/faucet-stream/pull/252))
- *(cli)* Add execution schema output ([#244](https://github.com/faucet-hq/faucet-stream/pull/244))
- *(sink-bigquery)* Write_mode upsert/delete via in-place MERGE ([#245](https://github.com/faucet-hq/faucet-stream/pull/245))
- Consistent snapshot → CDC replication handoff — faucet replicate ([#189](https://github.com/faucet-hq/faucet-stream/pull/189))

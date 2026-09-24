# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

> [!NOTE]
> **This file is a frozen historical archive.** As of #248, releases are
> tracked in **per-package changelogs** — each crate has its own
> `CHANGELOG.md` in its directory (e.g. `crates/core/CHANGELOG.md`,
> `cli/CHANGELOG.md`, `faucet-stream/CHANGELOG.md`), maintained automatically
> by release-plz. This root file preserves the consolidated history up to and
> including the 1.1.0 release; new entries are **not** added here.
>
> Every release since, across all crates, is on the site's
> [changelog](https://faucet-hq.github.io/changelog/).

## [Unreleased]

### Added

- **`faucet-source-singer`** — a Singer tap bridge source that runs any Singer
  tap and adapts its output into faucet records (single-stream v0). Tier-2 /
  experimental; reintroduces a runtime (usually Python) dependency, Singer-class
  throughput, and tap-dependent resume granularity.
- **`faucet-conformance`** — a reusable connector conformance test battery
  (config-schema validity + bounded-memory streaming implemented; bookmark /
  idempotent-replay / capability / error-not-panic checks scaffolded). Passing it
  is the Tier-1 (supported) criterion; wired into `faucet-source-csv` and
  `faucet-source-singer`.

### Testing

- **Resume/checkpoint property tests** for `faucet-source-singer` — `proptest`
  coverage of the effectively-once resume path over arbitrary Singer message
  interleavings and arbitrary crash points, generalizing the single hand-written
  crash-resume case. Asserts the five resume invariants: no loss, no duplicates
  (keyed sink), the checkpoint is never ahead of durable data, monotonic
  checkpoints, and empty-with-bookmark.

### CI & Build

- **API-stability gate** — a `cargo-semver-checks` CI job fails on any breaking
  change to the public `faucet-core` API vs the last crates.io release, so the
  connector trait surface (FCP §8) can only break with a deliberate major-version
  bump. Documented in CONTRIBUTING.md.

### Documentation

- **Benchmark batch disclosure** — BENCHMARKS.md Scenario C (Postgres → Postgres)
  now states the write-batch size and insert strategy on both sides (both pinned
  to 5,000-row batches; Meltano's opt-in `use_copy` COPY path noted as left off),
  so the sink-bound gap cannot be read as a Meltano batch misconfiguration.

## `faucet-cli` — [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.0.1...faucet-cli-v1.1.0) - 2026-06-12

### Bug Fixes

- *(serve)* Retry transient run-history connect before degrading ([#236](https://github.com/faucet-hq/faucet-stream/pull/236))
- *(lineage)* Standard { type, config } shape for transport + auth

### CI & Build

- Migrate to Rust 1.96 ([#175](https://github.com/faucet-hq/faucet-stream/pull/175))

### Features

- *(serve)* Event-driven pipeline triggers (object-arrival / webhook / queue-depth) ([#196](https://github.com/faucet-hq/faucet-stream/pull/196))
- *(cli)* Config composition — extends / profiles / !include ([#231](https://github.com/faucet-hq/faucet-stream/pull/231))
- *(serve)* Distributed / clustered execution (Mode A) ([#197](https://github.com/faucet-hq/faucet-stream/pull/197))
- *(bigquery-sink)* Exactly-once delivery via MERGE transaction ([#215](https://github.com/faucet-hq/faucet-stream/pull/215))
- Unified write_mode upsert/delete (merge by key) across SQL/Mongo/ES sinks ([#226](https://github.com/faucet-hq/faucet-stream/pull/226))
- Exactly-once / idempotent delivery mode ([#217](https://github.com/faucet-hq/faucet-stream/pull/217))
- *(serve)* Embedded web console (serve-ui) ([#214](https://github.com/faucet-hq/faucet-stream/pull/214))
- *(transform-sql)* SQL-as-transform via embedded DuckDB ([#129](https://github.com/faucet-hq/faucet-stream/pull/129))
- OpenLineage event emission for pipeline runs ([#123](https://github.com/faucet-hq/faucet-stream/pull/123))
- Add Apache Iceberg sink connector (append-only) ([#180](https://github.com/faucet-hq/faucet-stream/pull/180))
- Add MySQL binlog (CDC) source connector ([#178](https://github.com/faucet-hq/faucet-stream/pull/178))
- Add MongoDB Change Streams (CDC) source connector ([#176](https://github.com/faucet-hq/faucet-stream/pull/176))

### Miscellaneous

- Release ([#177](https://github.com/faucet-hq/faucet-stream/pull/177))

### Performance

- *(cli)* Bound matrix fan-out memory by projecting captured parent records ([#186](https://github.com/faucet-hq/faucet-stream/pull/186))

### Testing

- Raise workspace coverage to ~94% ([#222](https://github.com/faucet-hq/faucet-stream/pull/222))

## `faucet-stream` — [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.0.2...faucet-stream-v1.1.0) - 2026-06-12

### Bug Fixes

- *(serve)* Retry transient run-history connect before degrading ([#236](https://github.com/faucet-hq/faucet-stream/pull/236))

### Features

- *(serve)* Event-driven pipeline triggers (object-arrival / webhook / queue-depth) ([#196](https://github.com/faucet-hq/faucet-stream/pull/196))
- Unified write_mode upsert/delete (merge by key) across SQL/Mongo/ES sinks ([#226](https://github.com/faucet-hq/faucet-stream/pull/226))
- *(serve)* Embedded web console (serve-ui) ([#214](https://github.com/faucet-hq/faucet-stream/pull/214))
- *(transform-sql)* SQL-as-transform via embedded DuckDB ([#129](https://github.com/faucet-hq/faucet-stream/pull/129))
- OpenLineage event emission for pipeline runs ([#123](https://github.com/faucet-hq/faucet-stream/pull/123))
- Add Apache Iceberg sink connector (append-only) ([#180](https://github.com/faucet-hq/faucet-stream/pull/180))
- Add MySQL binlog (CDC) source connector ([#178](https://github.com/faucet-hq/faucet-stream/pull/178))
- Add MongoDB Change Streams (CDC) source connector ([#176](https://github.com/faucet-hq/faucet-stream/pull/176))

### Miscellaneous

- Release ([#177](https://github.com/faucet-hq/faucet-stream/pull/177))

## `faucet-sink-iceberg` — [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-sink-iceberg-v1.0.0) - 2026-06-12

### Features

- Unified write_mode upsert/delete (merge by key) across SQL/Mongo/ES sinks ([#226](https://github.com/faucet-hq/faucet-stream/pull/226))
- Exactly-once / idempotent delivery mode ([#217](https://github.com/faucet-hq/faucet-stream/pull/217))
- *(iceberg)* Cloud-warehouse (S3/GCS) storage for SQL/Glue/HMS catalogs ([#185](https://github.com/faucet-hq/faucet-stream/pull/185))
- OpenLineage event emission for pipeline runs ([#123](https://github.com/faucet-hq/faucet-stream/pull/123))
- Add Apache Iceberg sink connector (append-only) ([#180](https://github.com/faucet-hq/faucet-stream/pull/180))

## `faucet-sink-parquet` — [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-parquet-v1.0.1...faucet-sink-parquet-v1.1.0) - 2026-06-12

### Features

- OpenLineage event emission for pipeline runs ([#123](https://github.com/faucet-hq/faucet-stream/pull/123))

### Miscellaneous

- Release ([#177](https://github.com/faucet-hq/faucet-stream/pull/177))

### Testing

- Raise workspace coverage to ~94% ([#222](https://github.com/faucet-hq/faucet-stream/pull/222))

## `faucet-state-postgres` — [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-state-postgres-v1.0.1...faucet-state-postgres-v1.0.2) - 2026-06-12

### Miscellaneous

- Updated the following local packages: faucet-core

## `faucet-state-redis` — [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-state-redis-v1.0.1...faucet-state-redis-v1.0.2) - 2026-06-12

### Miscellaneous

- Updated the following local packages: faucet-core

## `faucet-common-mssql` — [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.0.1...faucet-common-mssql-v1.0.2) - 2026-06-12

### Miscellaneous

- Updated the following local packages: faucet-core

## `faucet-common-snowflake` — [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-snowflake-v1.0.0...faucet-common-snowflake-v1.0.1) - 2026-06-12

### Miscellaneous

- Updated the following local packages: faucet-core

## `faucet-common-kafka` — [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-kafka-v1.0.0...faucet-common-kafka-v1.0.1) - 2026-06-12

### Miscellaneous

- Updated the following local packages: faucet-core

## `faucet-common-gcs` — [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-gcs-v1.0.0...faucet-common-gcs-v1.0.1) - 2026-06-12

### Miscellaneous

- Updated the following local packages: faucet-core

## `faucet-common-elasticsearch` — [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-elasticsearch-v1.0.0...faucet-common-elasticsearch-v1.0.1) - 2026-06-12

### Miscellaneous

- Updated the following local packages: faucet-core

## `faucet-common-bigquery` — [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-bigquery-v1.0.0...faucet-common-bigquery-v1.0.1) - 2026-06-12

### Miscellaneous

- Updated the following local packages: faucet-core

## `faucet-auth` — [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-auth-v1.0.0...faucet-auth-v1.0.1) - 2026-06-12

### Miscellaneous

- Updated the following local packages: faucet-core

## `faucet-stream` — [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-stream-v1.0.1...faucet-stream-v1.0.2) - 2026-06-02

### Bug Fixes

- Absolute PNG banner for crates.io README hero + Codecov upload token

## `faucet-cli` — [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-cli-v1.0.0...faucet-cli-v1.0.1) - 2026-06-02

### Miscellaneous

- Updated the following local packages: faucet-source-kafka, faucet-source-gcs, faucet-source-elasticsearch, faucet-sink-gcs, faucet-sink-elasticsearch, faucet-sink-kafka, faucet-source-mssql, faucet-sink-mssql

## `faucet-sink-mssql` — [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mssql-v1.0.0...faucet-sink-mssql-v1.0.1) - 2026-06-02

### Miscellaneous

- Updated the following local packages: faucet-common-mssql

## `faucet-source-mssql` — [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mssql-v1.0.0...faucet-source-mssql-v1.0.1) - 2026-06-02

### Miscellaneous

- Updated the following local packages: faucet-common-mssql

### Bug Fixes

- Add multi-parent DAG validation and move futures to workspace deps
- Resolve SQL injection, JSON corruption, and semaphore deadlock
- Seek to bookmark in rebalance, eliminate restart duplicate (#50)
- Durable LSN feedback + transaction buffer cap (#78 findings #1, #2) (#81)

### CI & Build

- Backfill into release.yml ordering and project docs (#69)
- Fail fast if the derived publish list is incomplete (part of #78) (#90)

### Documentation

- Update CLAUDE.md and verify umbrella crate for SourceDAG
- Add runnable examples for pipeline, streaming, and DAG
- Cover the source × sink connector matrix
- Add 10 more popular source-sink combinations
- Exercise the full builder surface in every example
- Add cleanup-after-PR-merge rule to CLAUDE.md (#47)
- Condense CLAUDE.md, drop redundant per-crate enumeration (#51)
- Harden docs.rs rendering across all crates (WS-1 of #91) (#92)
- Positioning, comparison, architecture diagram + de-flake test (WS-2 & WS-3 of #91) (#93)
- MdBook site + runnable examples & local Docker stack (WS-4 & WS-5 of #91) (#94)
- Connector capability matrix, selection guide & community scaffolding (WS-6 & WS-7 of #91) (#95)

### Features

- Add substitute_context and extract_context utilities
- Make fetch_with_context the primary Source trait method
- Migrate all source crates to fetch_with_context
- Add SourceDAG data structures and builder
- Implement SourceDAG::run() execution engine
- Wire parent context into REST, DB, GraphQL, and S3 sources
- Wire parent context into remaining 7 source connectors
- Stdout/stderr sink + pluggable replication state stores
- Wire state-store resume into RestStream
- Add 'faucet' config-driven pipeline runner binary (#41)
- Apache Kafka source + sink + common crate (#46)
- Apache Parquet source + sink connectors (#48)
- PostgreSQL logical replication source (#49)
- --from-env pipeline mode (Closes #42) (#53)
- Pipeline+matrix config and cwd auto-discovery (#56)
- Streaming Pipeline::run with Source::stream_pages contract (#58)
- Observability — OTel-compatible tracing + Prometheus metrics (Closes #31) (#63)
- Dead-letter queue support for sinks (#65)
- GCS source + sink connectors (Closes #26, #27) (#66)
- Schema-driven faucet init --source X --sink Y scaffolder (#67)
- Faucet-elasticsearch-common shared auth crate (closes #43) (#68)
- Named source/sink templates + matrix `ref:` syntax (#73)
- Add Snowflake + BigQuery query source connectors (#75)
- Gzip/zstd compression for file connectors (closes #33) (#76)
- Add server-streaming RPC support (closes #34) (#77)

### Miscellaneous

- Increase publish wave wait to 15 minutes
- Ignore docs/superpowers/ AI workflow artifacts

### Other

- Pre-1.0 hardening: CRITICAL batch 1 (#78 findings #3, #4, #5, #6, #7, #8, #11) (#79)
- Pre-1.0 hardening: CRITICAL #9 + 10 fixes (#78 findings #9,#10,#14,#15,#16,#18,#20,#21,#22,#27,#28) (#86)
- Pre-1.0 hardening: 10 HIGH-tier fixes (part of #78) (#87)
- Pre-1.0 hardening: 17 MEDIUM-tier fixes (closes out #78) (#88)
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) (#89)

### Refactor

- Replace local resolve_path with faucet_core::util::substitute_context
- Struct variants for newtype auth enums (Closes #40) (#52)
- Remove unused SourceDAG executor (closes #62) (#74)

### Testing

- Add GitHub-style integration test for SourceDAG
- De-flake on_error=stop abort test (part of #78 finding #24) (#80)

## [0.2.0] - 2026-04-03

### Bug Fixes

- Fix security issues, error semantics, and code quality across workspace
- Fix CI: install libcurl-dev for rdkafka-sys build
- Fix release.toml: remove invalid publish-delay key
- Fix release workflow: publish in waves to respect crates.io rate limit

### Miscellaneous

- Bump version to 0.1.4
- Set faucet-stream version to 0.1.0

### Other

- Add Auth::TokenEndpoint with ResponseValidator for fetching credentials from APIs
- Restructure into multi-crate workspace with source/sink categories and add comprehensive test coverage
- Add Pipeline orchestration for source-to-sink data transfer
- Add 6 new connectors: GraphQL, XML, gRPC sources and Postgres, JSONL, Snowflake sinks
- Extract shared utilities into faucet-core::util module
- Add 18 new connectors: 9 sources + 9 sinks
- Remove Kafka source+sink (rdkafka C dependency too heavy for CI)
- Add SQLite source connector (faucet-source-sqlite)
- Improve third-party connector developer experience
- Optimize all connectors for throughput
- Add config loading from JSON files and env vars
- Add config_schema() to Source and Sink traits via schemars
- Update README with config loading, schema introspection, and version fixes
- Add extensive README for every source, sink, core, and umbrella crate
- Add automated release workflow with cargo-release
- Add crate README update rule to CLAUDE.md
- Remove old publish.yml, replaced by release.yml
- Restrict releases to main branch only
- Optimize CI: parallelize jobs and test all features in isolation
- {{crate_name}} v{{version}}

## [0.1.4] - 2026-03-25

### Other

- Add all features from reststream meltano
- Update readme and docs for all meltano features

## [0.1.3] - 2026-03-23

### Bug Fixes

- Reduce keywords to crates.io limit of 5

### Miscellaneous

- Bump version to 0.1.3

### Other

- Add wf for crates publish
- Add support for next link
- Add support for next link
- Automatic update of crate version

## [0.1.2] - 2026-03-23

### Other

- Initial code for faucet-stream
- Add CI workflow
- Add precommit
- Add precommit
- Precommit run
- Add support for docs
- Add stream pages support
- Add wf for crates publish
- Add wf for crates publish



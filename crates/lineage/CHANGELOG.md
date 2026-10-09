# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [2.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-lineage-v2.2.0...faucet-lineage-v2.2.1) - 2026-10-08

### Bug Fixes

- #789 engine findings (core, files, transforms, CLI) + open bug issues ([#840](https://github.com/faucet-hq/faucet-stream/pull/840))
- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- MEDIUM/LOW security findings from the #789 audit (serve, supply chain, auth, secrets) ([#827](https://github.com/faucet-hq/faucet-stream/pull/827))
- Close the API source, file and messaging findings of the production-readiness audit (#789 group D)

## [2.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-lineage-v2.1.0...faucet-lineage-v2.2.0) - 2026-09-29

### Features

- One shared file writer for local, S3, GCS, Azure and SFTP; deprecate the csv, jsonl and parquet connectors ([#782](https://github.com/faucet-hq/faucet-stream/pull/782))
- REST/GraphQL source gaps, silent-data-loss fixes and Google service-account auth ([#760](https://github.com/faucet-hq/faucet-stream/pull/760))
- Multi-table faucet mirror over a table set, and prebuilt Windows binaries ([#758](https://github.com/faucet-hq/faucet-stream/pull/758))
- Local file source and sink, Avro/ORC formats, Singer target bridge, source throttling metrics and template row selection ([#745](https://github.com/faucet-hq/faucet-stream/pull/745))
- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))
- Generic REST discovery, partitioned typed OData, byte-passthrough loads + BigQuery/serve upgrades ([#650](https://github.com/faucet-hq/faucet-stream/pull/650))
- *(serve)* Retention GC for local sink outputs + Datasets-page cleanup controls ([#596](https://github.com/faucet-hq/faucet-stream/pull/596))

## [2.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-lineage-v2.0.2...faucet-lineage-v2.1.0) - 2026-08-23

### Features

- *(sinks)* Add write_mode: overwrite (full-refresh) across data-storage sinks ([#493](https://github.com/faucet-hq/faucet-stream/pull/493))

## [2.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-lineage-v2.0.1...faucet-lineage-v2.0.2) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core

## [2.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-lineage-v2.0.0...faucet-lineage-v2.0.1) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [2.0.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-lineage-v1.2.3...faucet-lineage-v2.0.0) - 2026-08-09

### Features

- *(topology)* Exactly-once delivery + per-node SLA/notify/lineage/catalog ([#464](https://github.com/faucet-hq/faucet-stream/pull/464))

## [1.2.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-lineage-v1.2.1...faucet-lineage-v1.2.2) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-lineage-v1.2.0...faucet-lineage-v1.2.1) - 2026-07-17

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-lineage-v1.1.0...faucet-lineage-v1.2.0) - 2026-07-10

### Features

- Typed delivery guarantees, effectively-once coverage expansion, and prebuilt binary distribution ([#294](https://github.com/faucet-hq/faucet-stream/pull/294))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-lineage-v1.0.1...faucet-lineage-v1.1.0) - 2026-07-08

### Features

- Persistent Data Movement Catalog — datasets, schema timelines & lineage graph ([#286](https://github.com/faucet-hq/faucet-stream/pull/286))

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-lineage-v1.0.0...faucet-lineage-v1.0.1) - 2026-06-22

### Bug Fixes

- Resolve all 18 Low reliability/data-integrity findings (F40–F57, #264) ([#267](https://github.com/faucet-hq/faucet-stream/pull/267))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))

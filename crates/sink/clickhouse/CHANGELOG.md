# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-clickhouse-v1.2.1...faucet-sink-clickhouse-v1.3.0) - 2026-10-15

### Features

- *(core)* RFC 0013 phase 3a — family drivers, governed-page typestate, deprecations (RFC 0014) ([#896](https://github.com/faucet-hq/faucet-stream/pull/896))
- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))
- Template test bundles with test-gated labels; one container start helper; refuse --no-auth on a network address ([#859](https://github.com/faucet-hq/faucet-stream/pull/859))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-clickhouse-v1.2.0...faucet-sink-clickhouse-v1.2.1) - 2026-10-08

### Bug Fixes

- #789 engine findings (core, files, transforms, CLI) + open bug issues ([#840](https://github.com/faucet-hq/faucet-stream/pull/840))
- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- Close the SQL and CDC connector findings of the production-readiness audit (#789 group C)

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-clickhouse-v1.1.0...faucet-sink-clickhouse-v1.2.0) - 2026-09-29

### Features

- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-clickhouse-v1.0.4...faucet-sink-clickhouse-v1.1.0) - 2026-08-23

### Features

- Rest partitions fan-out + repeated query params, cross_join transform, ClickHouse staged load (#535/#536/#534/#528) ([#537](https://github.com/faucet-hq/faucet-stream/pull/537))

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-clickhouse-v1.0.3...faucet-sink-clickhouse-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-clickhouse

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-clickhouse-v1.0.2...faucet-sink-clickhouse-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-clickhouse

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-clickhouse-v1.0.1...faucet-sink-clickhouse-v1.0.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-clickhouse

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-sink-clickhouse-v1.0.0) - 2026-07-24

### Features

- *(connectors)* Redshift, Pub/Sub, ClickHouse, Azure Blob, and SQL Server CDC ([#362](https://github.com/faucet-hq/faucet-stream/pull/362))

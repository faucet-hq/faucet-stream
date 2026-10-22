# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-clickhouse-v1.2.0...faucet-common-clickhouse-v1.2.1) - 2026-10-22

### Bug Fixes

- Harden connectors, serve, CLI flows and core guarantees (#930 medium findings) ([#933](https://github.com/faucet-hq/faucet-stream/pull/933))
- Harden serve input screening, data-safety flows, incremental cursors and CDC positions (#930 critical + high) ([#931](https://github.com/faucet-hq/faucet-stream/pull/931))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-clickhouse-v1.1.1...faucet-common-clickhouse-v1.2.0) - 2026-10-15

### Features

- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-clickhouse-v1.1.0...faucet-common-clickhouse-v1.1.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-clickhouse-v1.0.5...faucet-common-clickhouse-v1.1.0) - 2026-09-29

### Bug Fixes

- Replace every message-grep classification with a typed one, and close the core retry gate ([#656](https://github.com/faucet-hq/faucet-stream/pull/656))

### Features

- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-clickhouse-v1.0.4...faucet-common-clickhouse-v1.0.5) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-clickhouse-v1.0.3...faucet-common-clickhouse-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-clickhouse-v1.0.2...faucet-common-clickhouse-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-clickhouse-v1.0.1...faucet-common-clickhouse-v1.0.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-common-clickhouse-v1.0.0) - 2026-07-24

### Features

- *(connectors)* Redshift, Pub/Sub, ClickHouse, Azure Blob, and SQL Server CDC ([#362](https://github.com/faucet-hq/faucet-stream/pull/362))

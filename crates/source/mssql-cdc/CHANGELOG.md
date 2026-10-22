# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mssql-cdc-v1.2.0...faucet-source-mssql-cdc-v1.2.1) - 2026-10-22

### Bug Fixes

- Harden serve input screening, data-safety flows, incremental cursors and CDC positions (#930 critical + high) ([#931](https://github.com/faucet-hq/faucet-stream/pull/931))

### Testing

- *(mssql-cdc)* Retry enabling CDC while SQL Server Agent is starting ([#926](https://github.com/faucet-hq/faucet-stream/pull/926))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mssql-cdc-v1.1.1...faucet-source-mssql-cdc-v1.2.0) - 2026-10-15

### Bug Fixes

- Resolve the LOW and INFO findings of the #785–#861 review ([#869](https://github.com/faucet-hq/faucet-stream/pull/869))
- Per-change mirror replay skip, duckdb 1.10506; agent skills with requires_faucet; Helm values for every serve feature ([#858](https://github.com/faucet-hq/faucet-stream/pull/858))

### Features

- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))
- Template test bundles with test-gated labels; one container start helper; refuse --no-auth on a network address ([#859](https://github.com/faucet-hq/faucet-stream/pull/859))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mssql-cdc-v1.1.0...faucet-source-mssql-cdc-v1.1.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- Close the SQL and CDC connector findings of the production-readiness audit (#789 group C)

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mssql-cdc-v1.0.5...faucet-source-mssql-cdc-v1.1.0) - 2026-09-29

### Features

- Multi-table faucet mirror over a table set, and prebuilt Windows binaries ([#758](https://github.com/faucet-hq/faucet-stream/pull/758))
- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mssql-cdc-v1.0.4...faucet-source-mssql-cdc-v1.0.5) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-mssql

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mssql-cdc-v1.0.3...faucet-source-mssql-cdc-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-mssql

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mssql-cdc-v1.0.2...faucet-source-mssql-cdc-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-mssql

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mssql-cdc-v1.0.1...faucet-source-mssql-cdc-v1.0.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-mssql

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-source-mssql-cdc-v1.0.0) - 2026-07-24

### Bug Fixes

- *(ci)* Unblock main — ignore heavy mssql-cdc Docker tests, un-ignore tracked CI scripts ([#368](https://github.com/faucet-hq/faucet-stream/pull/368))

### Features

- *(cli)* Surface connector conformance maturity tiers ([#330](https://github.com/faucet-hq/faucet-stream/pull/330)) ([#367](https://github.com/faucet-hq/faucet-stream/pull/367))
- *(connectors)* Redshift, Pub/Sub, ClickHouse, Azure Blob, and SQL Server CDC ([#362](https://github.com/faucet-hq/faucet-stream/pull/362))

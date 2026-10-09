# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-duckdb-v1.1.0...faucet-sink-duckdb-v1.1.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- Close the SQL and CDC connector findings of the production-readiness audit (#789 group C)

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-duckdb-v1.0.5...faucet-sink-duckdb-v1.1.0) - 2026-09-29

### Features

- One shared file writer for local, S3, GCS, Azure and SFTP; deprecate the csv, jsonl and parquet connectors ([#782](https://github.com/faucet-hq/faucet-stream/pull/782))
- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-duckdb-v1.0.4...faucet-sink-duckdb-v1.0.5) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-duckdb-v1.0.3...faucet-sink-duckdb-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-duckdb-v1.0.2...faucet-sink-duckdb-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-duckdb-v1.0.1...faucet-sink-duckdb-v1.0.2) - 2026-08-09

### Bug Fixes

- Resolve the fourth hardening audit — topology governance bypass, SQS at-most-once, control-plane secret leaks ([#456](https://github.com/faucet-hq/faucet-stream/pull/456)) ([#457](https://github.com/faucet-hq/faucet-stream/pull/457))

### Testing

- *(conformance)* Adopt the new capability checks across all connectors ([#470](https://github.com/faucet-hq/faucet-stream/pull/470))

## [1.0.0] - 2026-07-28

### Features

- Initial release: DuckDB sink — writes JSON records to a DuckDB table via a
  JSON column or auto-mapped columns, each batch a transaction-wrapped
  multi-row INSERT. Conformance battery wired ([#413](https://github.com/faucet-hq/faucet-stream/issues/413)).

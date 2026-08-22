# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-duckdb-v1.1.0...faucet-source-duckdb-v1.1.1) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-duckdb-v1.0.3...faucet-source-duckdb-v1.1.0) - 2026-08-16

### Features

- *(sources)* Fail-fast config validation for csv/duckdb/elasticsearch/gcs/graphql ([#489](https://github.com/faucet-hq/faucet-stream/pull/489))

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-duckdb-v1.0.2...faucet-source-duckdb-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-duckdb-v1.0.1...faucet-source-duckdb-v1.0.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.0] - 2026-07-28

### Features

- Initial release: DuckDB query source — opens a DuckDB file or in-memory
  database, runs a configured SQL query, and streams rows as JSON with
  bounded memory. Conformance battery wired ([#413](https://github.com/faucet-hq/faucet-stream/issues/413)).

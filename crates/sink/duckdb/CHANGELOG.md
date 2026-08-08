# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-duckdb-v1.0.1...faucet-sink-duckdb-v1.0.2) - 2026-08-08

### Bug Fixes

- Resolve the fourth hardening audit — topology governance bypass, SQS at-most-once, control-plane secret leaks ([#456](https://github.com/faucet-hq/faucet-stream/pull/456)) ([#457](https://github.com/faucet-hq/faucet-stream/pull/457))

## [1.0.0] - 2026-07-28

### Features

- Initial release: DuckDB sink — writes JSON records to a DuckDB table via a
  JSON column or auto-mapped columns, each batch a transaction-wrapped
  multi-row INSERT. Conformance battery wired ([#413](https://github.com/faucet-hq/faucet-stream/issues/413)).

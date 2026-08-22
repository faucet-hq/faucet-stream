# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-redshift-v1.0.4...faucet-sink-redshift-v1.1.0) - 2026-08-22

### Features

- Datetime window slicing, tree_flatten, staged-load foundation, persistent run logs, chained discovery (#527–#531) ([#532](https://github.com/faucet-hq/faucet-stream/pull/532))

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-redshift-v1.0.3...faucet-sink-redshift-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-redshift

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-redshift-v1.0.2...faucet-sink-redshift-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-redshift

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-redshift-v1.0.1...faucet-sink-redshift-v1.0.2) - 2026-08-09

### Bug Fixes

- Resolve third-pass audit findings (contract DLQ index, GraphQL cycle guard, redshift null-row, spanner NUMERIC cursor) ([#467](https://github.com/faucet-hq/faucet-stream/pull/467))
- Second-pass audit — wide-integer corruption in the Arrow/SQL shim and SQL binds, backfill DST windows (#460, #461, #462) ([#463](https://github.com/faucet-hq/faucet-stream/pull/463))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-sink-redshift-v1.0.0) - 2026-07-24

### Features

- *(connectors)* Redshift, Pub/Sub, ClickHouse, Azure Blob, and SQL Server CDC ([#362](https://github.com/faucet-hq/faucet-stream/pull/362))

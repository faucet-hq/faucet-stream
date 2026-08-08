# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-redshift-v1.0.1...faucet-sink-redshift-v1.0.2) - 2026-08-08

### Bug Fixes

- Second-pass audit — wide-integer corruption in the Arrow/SQL shim and SQL binds, backfill DST windows (#460, #461, #462) ([#463](https://github.com/faucet-hq/faucet-stream/pull/463))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-sink-redshift-v1.0.0) - 2026-07-24

### Features

- *(connectors)* Redshift, Pub/Sub, ClickHouse, Azure Blob, and SQL Server CDC ([#362](https://github.com/faucet-hq/faucet-stream/pull/362))

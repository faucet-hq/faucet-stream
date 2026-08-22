# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-csv-v1.2.0...faucet-sink-csv-v1.2.1) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-csv-v1.1.8...faucet-sink-csv-v1.2.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.1.8](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-csv-v1.1.7...faucet-sink-csv-v1.1.8) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.7](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-csv-v1.1.6...faucet-sink-csv-v1.1.7) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-csv-v1.1.4...faucet-sink-csv-v1.1.5) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-csv-v1.1.3...faucet-sink-csv-v1.1.4) - 2026-07-17

### Bug Fixes

- Resolve #321 critical/high audit findings (exactly-once, cluster, transform-sql, compression) ([#322](https://github.com/faucet-hq/faucet-stream/pull/322))

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/faucet-hq/faucet-stream/pull/311))

## [1.1.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-csv-v1.1.2...faucet-sink-csv-v1.1.3) - 2026-07-10

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-csv-v1.1.1...faucet-sink-csv-v1.1.2) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-csv-v1.1.0...faucet-sink-csv-v1.1.1) - 2026-06-22

### Bug Fixes

- Resolve all 20 Medium reliability/data-integrity findings (F20–F39, #264) ([#266](https://github.com/faucet-hq/faucet-stream/pull/266))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

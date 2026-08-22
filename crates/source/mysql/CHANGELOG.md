# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.6.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-v1.6.0...faucet-source-mysql-v1.6.1) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.6.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-v1.5.1...faucet-source-mysql-v1.6.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.5.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-v1.5.0...faucet-source-mysql-v1.5.1) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-v1.4.3...faucet-source-mysql-v1.5.0) - 2026-08-10

### Features

- *(conformance)* Add discover-roundtrip and cancellation-flush integration checks ([#472](https://github.com/faucet-hq/faucet-stream/pull/472))

## [1.4.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-v1.4.2...faucet-source-mysql-v1.4.3) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.4.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-v1.4.0...faucet-source-mysql-v1.4.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-v1.3.0...faucet-source-mysql-v1.4.0) - 2026-07-17

### Features

- Faucet discover (live source introspection) + faucet backfill (resumable historical replay)

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/faucet-hq/faucet-stream/pull/311))

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-v1.2.0...faucet-source-mysql-v1.3.0) - 2026-07-10

### Features

- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-v1.1.1...faucet-source-mysql-v1.2.0) - 2026-07-08

### Features

- Extend cluster Mode B sharding to mysql, mssql, sqlite, gcs, and parquet sources ([#271](https://github.com/faucet-hq/faucet-stream/pull/271))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-v1.1.0...faucet-source-mysql-v1.1.1) - 2026-06-22

### Bug Fixes

- Resolve all 20 Medium reliability/data-integrity findings (F20–F39, #264) ([#266](https://github.com/faucet-hq/faucet-stream/pull/266))
- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/faucet-hq/faucet-stream/pull/264)) ([#265](https://github.com/faucet-hq/faucet-stream/pull/265))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

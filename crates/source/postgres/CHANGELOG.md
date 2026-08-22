# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.7.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-postgres-v1.7.0...faucet-source-postgres-v1.7.1) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.7.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-postgres-v1.6.1...faucet-source-postgres-v1.7.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.6.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-postgres-v1.6.0...faucet-source-postgres-v1.6.1) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.6.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-postgres-v1.5.3...faucet-source-postgres-v1.6.0) - 2026-08-10

### Features

- *(conformance)* Add discover-roundtrip and cancellation-flush integration checks ([#472](https://github.com/faucet-hq/faucet-stream/pull/472))

## [1.5.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-postgres-v1.5.2...faucet-source-postgres-v1.5.3) - 2026-08-09

### Bug Fixes

- Second-pass audit — wide-integer corruption in the Arrow/SQL shim and SQL binds, backfill DST windows (#460, #461, #462) ([#463](https://github.com/faucet-hq/faucet-stream/pull/463))

### Testing

- *(conformance)* Adopt the new capability checks across all connectors ([#470](https://github.com/faucet-hq/faucet-stream/pull/470))

## [1.5.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-postgres-v1.5.0...faucet-source-postgres-v1.5.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-postgres-v1.4.0...faucet-source-postgres-v1.5.0) - 2026-07-17

### Features

- Connector conformance battery + tiers, FCP spec, sink-bound benchmark, sink config fixes ([#307](https://github.com/faucet-hq/faucet-stream/pull/307))
- Faucet discover (live source introspection) + faucet backfill (resumable historical replay)

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-postgres-v1.3.0...faucet-source-postgres-v1.4.0) - 2026-07-10

### Features

- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-postgres-v1.2.0...faucet-source-postgres-v1.3.0) - 2026-07-08

### Features

- Extend cluster Mode B sharding to mysql, mssql, sqlite, gcs, and parquet sources ([#271](https://github.com/faucet-hq/faucet-stream/pull/271))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-postgres-v1.1.0...faucet-source-postgres-v1.2.0) - 2026-06-22

### Bug Fixes

- Resolve all 18 Low reliability/data-integrity findings (F40–F57, #264) ([#267](https://github.com/faucet-hq/faucet-stream/pull/267))
- Resolve all 20 Medium reliability/data-integrity findings (F20–F39, #264) ([#266](https://github.com/faucet-hq/faucet-stream/pull/266))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

### Features

- Serve cluster Mode B — source-shard distribution across workers ([#230](https://github.com/faucet-hq/faucet-stream/pull/230)) ([#263](https://github.com/faucet-hq/faucet-stream/pull/263))

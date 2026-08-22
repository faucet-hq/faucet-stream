# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mssql-v1.4.1...faucet-sink-mssql-v1.5.0) - 2026-08-22

### Features

- Rest partitions fan-out + repeated query params, cross_join transform, ClickHouse staged load (#535/#536/#534/#528) ([#537](https://github.com/faucet-hq/faucet-stream/pull/537))
- *(sinks)* Add write_mode: overwrite (full-refresh) across data-storage sinks ([#493](https://github.com/faucet-hq/faucet-stream/pull/493))

## [1.4.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mssql-v1.4.0...faucet-sink-mssql-v1.4.1) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-mssql

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mssql-v1.3.4...faucet-sink-mssql-v1.4.0) - 2026-08-15

### Features

- Scoped cleanup — delete records missing from a source's completeness claim ([#484](https://github.com/faucet-hq/faucet-stream/pull/484))

## [1.3.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mssql-v1.3.3...faucet-sink-mssql-v1.3.4) - 2026-08-09

### Bug Fixes

- Resolve the fourth hardening audit — topology governance bypass, SQS at-most-once, control-plane secret leaks ([#456](https://github.com/faucet-hq/faucet-stream/pull/456)) ([#457](https://github.com/faucet-hq/faucet-stream/pull/457))

### Testing

- *(conformance)* Adopt the new capability checks across all connectors ([#470](https://github.com/faucet-hq/faucet-stream/pull/470))

## [1.3.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mssql-v1.3.1...faucet-sink-mssql-v1.3.2) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-mssql

## [1.3.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mssql-v1.3.0...faucet-sink-mssql-v1.3.1) - 2026-07-17

### Bug Fixes

- Resolve #321 critical/high audit findings (exactly-once, cluster, transform-sql, compression) ([#322](https://github.com/faucet-hq/faucet-stream/pull/322))

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/faucet-hq/faucet-stream/pull/311))

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mssql-v1.2.1...faucet-sink-mssql-v1.3.0) - 2026-07-10

### Features

- Typed delivery guarantees, effectively-once coverage expansion, and prebuilt binary distribution ([#294](https://github.com/faucet-hq/faucet-stream/pull/294))
- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mssql-v1.2.0...faucet-sink-mssql-v1.2.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-mssql

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mssql-v1.1.0...faucet-sink-mssql-v1.2.0) - 2026-06-22

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))

### Features

- Schema-drift handling policy (warn/evolve/ignore/quarantine/fail) ([#194](https://github.com/faucet-hq/faucet-stream/pull/194))

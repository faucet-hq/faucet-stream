# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-iceberg-v1.3.4...faucet-sink-iceberg-v1.4.0) - 2026-08-22

### Features

- *(sinks)* Add write_mode: overwrite (full-refresh) across data-storage sinks ([#493](https://github.com/faucet-hq/faucet-stream/pull/493))

## [1.3.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-iceberg-v1.3.3...faucet-sink-iceberg-v1.3.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-iceberg-v1.3.2...faucet-sink-iceberg-v1.3.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-iceberg-v1.3.1...faucet-sink-iceberg-v1.3.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-iceberg-v1.2.2...faucet-sink-iceberg-v1.3.0) - 2026-07-31

### Features

- *(iceberg)* Additive schema evolution via iceberg-rust 0.10.0 ([#255](https://github.com/faucet-hq/faucet-stream/pull/255)); fix(cli): run summary → stderr ([#424](https://github.com/faucet-hq/faucet-stream/pull/424)) ([#425](https://github.com/faucet-hq/faucet-stream/pull/425))

## [1.2.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-iceberg-v1.2.1...faucet-sink-iceberg-v1.2.2) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-iceberg-v1.2.0...faucet-sink-iceberg-v1.2.1) - 2026-07-17

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-iceberg-v1.1.1...faucet-sink-iceberg-v1.2.0) - 2026-07-10

### Features

- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-iceberg-v1.1.0...faucet-sink-iceberg-v1.1.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-iceberg-v1.0.0...faucet-sink-iceberg-v1.1.0) - 2026-06-22

### Bug Fixes

- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/faucet-hq/faucet-stream/pull/264)) ([#265](https://github.com/faucet-hq/faucet-stream/pull/265))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

### Features

- *(sink-iceberg)* Opt-in orphan cleanup on definitive commit failure ([#193](https://github.com/faucet-hq/faucet-stream/pull/193)) ([#260](https://github.com/faucet-hq/faucet-stream/pull/260))
- Schema-drift handling policy (warn/evolve/ignore/quarantine/fail) ([#194](https://github.com/faucet-hq/faucet-stream/pull/194))

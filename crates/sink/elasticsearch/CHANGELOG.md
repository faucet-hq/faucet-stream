# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-elasticsearch-v1.4.1...faucet-sink-elasticsearch-v1.5.0) - 2026-08-22

### Features

- MTLS, ES overwrite, OAuth1, and completeness reconciliation ([#506](https://github.com/faucet-hq/faucet-stream/pull/506))
- *(sinks)* Add write_mode: overwrite (full-refresh) across data-storage sinks ([#493](https://github.com/faucet-hq/faucet-stream/pull/493))

## [1.4.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-elasticsearch-v1.4.0...faucet-sink-elasticsearch-v1.4.1) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-elasticsearch

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-elasticsearch-v1.3.4...faucet-sink-elasticsearch-v1.4.0) - 2026-08-15

### Features

- Scoped cleanup — delete records missing from a source's completeness claim ([#484](https://github.com/faucet-hq/faucet-stream/pull/484))

## [1.3.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-elasticsearch-v1.3.3...faucet-sink-elasticsearch-v1.3.4) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-elasticsearch

## [1.3.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-elasticsearch-v1.3.1...faucet-sink-elasticsearch-v1.3.2) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-elasticsearch

## [1.3.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-elasticsearch-v1.3.0...faucet-sink-elasticsearch-v1.3.1) - 2026-07-17

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-elasticsearch

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-elasticsearch-v1.2.1...faucet-sink-elasticsearch-v1.3.0) - 2026-07-10

### Features

- Typed delivery guarantees, effectively-once coverage expansion, and prebuilt binary distribution ([#294](https://github.com/faucet-hq/faucet-stream/pull/294))
- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-elasticsearch-v1.2.0...faucet-sink-elasticsearch-v1.2.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-elasticsearch

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-elasticsearch-v1.1.0...faucet-sink-elasticsearch-v1.2.0) - 2026-06-22

### Bug Fixes

- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/faucet-hq/faucet-stream/pull/264)) ([#265](https://github.com/faucet-hq/faucet-stream/pull/265))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

### Features

- Schema-drift handling policy (warn/evolve/ignore/quarantine/fail) ([#194](https://github.com/faucet-hq/faucet-stream/pull/194))

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.4.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-csv-v1.4.0...faucet-source-csv-v1.4.1) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-csv-v1.3.4...faucet-source-csv-v1.4.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))
- *(sources)* Fail-fast config validation for csv/duckdb/elasticsearch/gcs/graphql ([#489](https://github.com/faucet-hq/faucet-stream/pull/489))

## [1.3.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-csv-v1.3.3...faucet-source-csv-v1.3.4) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-csv-v1.3.2...faucet-source-csv-v1.3.3) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-csv-v1.3.0...faucet-source-csv-v1.3.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-csv-v1.2.0...faucet-source-csv-v1.3.0) - 2026-07-17

### Features

- Connector conformance battery + tiers, FCP spec, sink-bound benchmark, sink config fixes ([#307](https://github.com/faucet-hq/faucet-stream/pull/307))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-csv-v1.1.2...faucet-source-csv-v1.2.0) - 2026-07-10

### Features

- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-csv-v1.1.1...faucet-source-csv-v1.1.2) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-csv-v1.1.0...faucet-source-csv-v1.1.1) - 2026-06-22

### Bug Fixes

- Resolve all 20 Medium reliability/data-integrity findings (F20–F39, #264) ([#266](https://github.com/faucet-hq/faucet-stream/pull/266))
- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/faucet-hq/faucet-stream/pull/264)) ([#265](https://github.com/faucet-hq/faucet-stream/pull/265))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

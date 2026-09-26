# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-parquet-v1.2.1...faucet-sink-parquet-v1.3.0) - 2026-09-26

### Bug Fixes

- Replace every message-grep classification with a typed one, and close the core retry gate ([#656](https://github.com/faucet-hq/faucet-stream/pull/656))

### Features

- RabbitMQ connector pair, GCS emulator suites in CI, integration-coverage gate, sectioned trigger form ([#699](https://github.com/faucet-hq/faucet-stream/pull/699))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))
- *(serve)* Retention GC for local sink outputs + Datasets-page cleanup controls ([#596](https://github.com/faucet-hq/faucet-stream/pull/596))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-parquet-v1.2.0...faucet-sink-parquet-v1.2.1) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-parquet-v1.1.8...faucet-sink-parquet-v1.2.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.1.8](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-parquet-v1.1.7...faucet-sink-parquet-v1.1.8) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.7](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-parquet-v1.1.6...faucet-sink-parquet-v1.1.7) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-parquet-v1.1.4...faucet-sink-parquet-v1.1.5) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-parquet-v1.1.3...faucet-sink-parquet-v1.1.4) - 2026-07-17

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-parquet-v1.1.2...faucet-sink-parquet-v1.1.3) - 2026-07-10

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-parquet-v1.1.1...faucet-sink-parquet-v1.1.2) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-parquet-v1.1.0...faucet-sink-parquet-v1.1.1) - 2026-06-22

### Bug Fixes

- Resolve all 20 Medium reliability/data-integrity findings (F20–F39, #264) ([#266](https://github.com/faucet-hq/faucet-stream/pull/266))
- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/faucet-hq/faucet-stream/pull/264)) ([#265](https://github.com/faucet-hq/faucet-stream/pull/265))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))

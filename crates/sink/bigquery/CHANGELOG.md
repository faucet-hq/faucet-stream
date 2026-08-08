# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.4.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-bigquery-v1.4.1...faucet-sink-bigquery-v1.4.2) - 2026-08-08

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-bigquery, faucet-common-gcs

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-bigquery-v1.3.1...faucet-sink-bigquery-v1.4.0) - 2026-07-24

### Features

- Live `run` progress + Arrow BigQuery & Snowflake paths (#385, #380, #381) ([#395](https://github.com/faucet-hq/faucet-stream/pull/395))

## [1.3.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-bigquery-v1.3.0...faucet-sink-bigquery-v1.3.1) - 2026-07-17

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-bigquery

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-bigquery-v1.2.1...faucet-sink-bigquery-v1.3.0) - 2026-07-10

### Features

- Typed delivery guarantees, effectively-once coverage expansion, and prebuilt binary distribution ([#294](https://github.com/faucet-hq/faucet-stream/pull/294))
- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-bigquery-v1.2.0...faucet-sink-bigquery-v1.2.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-bigquery

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-bigquery-v1.1.0...faucet-sink-bigquery-v1.2.0) - 2026-06-22

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

### Features

- Schema-drift handling policy (warn/evolve/ignore/quarantine/fail) ([#194](https://github.com/faucet-hq/faucet-stream/pull/194))
- *(sink-bigquery)* Write_mode upsert/delete via in-place MERGE ([#245](https://github.com/faucet-hq/faucet-stream/pull/245))

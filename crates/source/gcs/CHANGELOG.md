# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.7.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-gcs-v1.6.1...faucet-source-gcs-v1.7.0) - 2026-09-26

### Bug Fixes

- Three inert config knobs — connector config validation, delta target_file_size, object-store concurrency ([#665](https://github.com/faucet-hq/faucet-stream/pull/665))
- Replace every message-grep classification with a typed one, and close the core retry gate ([#656](https://github.com/faucet-hq/faucet-stream/pull/656))

### Features

- Change requests (plan → approve → run), run budgets, and cost & usage accounting ([#730](https://github.com/faucet-hq/faucet-stream/pull/730))
- Data-flow policies (`policy:` / `--policy`) and change impact analysis (`plan --impact`)
- RabbitMQ connector pair, GCS emulator suites in CI, integration-coverage gate, sectioned trigger form ([#699](https://github.com/faucet-hq/faucet-stream/pull/699))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.6.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-gcs-v1.6.0...faucet-source-gcs-v1.6.1) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-gcs

## [1.6.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-gcs-v1.5.1...faucet-source-gcs-v1.6.0) - 2026-08-16

### Features

- *(sources)* Fail-fast config validation for csv/duckdb/elasticsearch/gcs/graphql ([#489](https://github.com/faucet-hq/faucet-stream/pull/489))

## [1.5.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-gcs-v1.5.0...faucet-source-gcs-v1.5.1) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-gcs

## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-gcs-v1.4.2...faucet-source-gcs-v1.5.0) - 2026-08-10

### Features

- *(conformance)* Add discover-roundtrip and cancellation-flush integration checks ([#472](https://github.com/faucet-hq/faucet-stream/pull/472))

## [1.4.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-gcs-v1.4.1...faucet-source-gcs-v1.4.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-gcs

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-gcs-v1.3.0...faucet-source-gcs-v1.4.0) - 2026-07-24

### Features

- Arrow columnar path for S3, GCS, and Databricks — RFC 0002 Phase 4 ([#375](https://github.com/faucet-hq/faucet-stream/pull/375)) ([#382](https://github.com/faucet-hq/faucet-stream/pull/382))

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-gcs-v1.2.1...faucet-source-gcs-v1.3.0) - 2026-07-17

### Features

- Faucet discover (live source introspection) + faucet backfill (resumable historical replay)

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/faucet-hq/faucet-stream/pull/311))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-gcs-v1.2.0...faucet-source-gcs-v1.2.1) - 2026-07-10

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-gcs

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-gcs-v1.1.1...faucet-source-gcs-v1.2.0) - 2026-07-08

### Features

- Extend cluster Mode B sharding to mysql, mssql, sqlite, gcs, and parquet sources ([#271](https://github.com/faucet-hq/faucet-stream/pull/271))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-gcs-v1.1.0...faucet-source-gcs-v1.1.1) - 2026-06-22

### Bug Fixes

- *(s3,gcs)* Verify object read integrity — length + opt-in checksum ([#161](https://github.com/faucet-hq/faucet-stream/pull/161)) ([#257](https://github.com/faucet-hq/faucet-stream/pull/257))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))

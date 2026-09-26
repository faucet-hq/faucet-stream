# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-s3-v1.4.1...faucet-sink-s3-v1.5.0) - 2026-09-26

### Features

- Change requests (plan → approve → run), run budgets, and cost & usage accounting ([#730](https://github.com/faucet-hq/faucet-stream/pull/730))
- Deployment overlays, template version deprecation, first-run tables, one-meaning vocabulary ([#693](https://github.com/faucet-hq/faucet-stream/pull/693))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

### Testing

- Pull MinIO from quay.io — the Docker Hub repository was withdrawn ([#653](https://github.com/faucet-hq/faucet-stream/pull/653))

## [1.4.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-s3-v1.4.0...faucet-sink-s3-v1.4.1) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-core, faucet-source-s3

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-s3-v1.3.4...faucet-sink-s3-v1.4.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.3.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-s3-v1.3.3...faucet-sink-s3-v1.3.4) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-core, faucet-source-s3

## [1.3.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-s3-v1.3.2...faucet-sink-s3-v1.3.3) - 2026-08-10

### Miscellaneous

- Updated the following local packages: faucet-source-s3

## [1.3.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-s3-v1.3.1...faucet-sink-s3-v1.3.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-core, faucet-source-s3

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-s3-v1.2.1...faucet-sink-s3-v1.3.0) - 2026-07-24

### Features

- Arrow columnar path for S3, GCS, and Databricks — RFC 0002 Phase 4 ([#375](https://github.com/faucet-hq/faucet-stream/pull/375)) ([#382](https://github.com/faucet-hq/faucet-stream/pull/382))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-s3-v1.2.0...faucet-sink-s3-v1.2.1) - 2026-07-17

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-s3-v1.1.2...faucet-sink-s3-v1.2.0) - 2026-07-10

### Features

- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-s3-v1.1.1...faucet-sink-s3-v1.1.2) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-s3-v1.1.0...faucet-sink-s3-v1.1.1) - 2026-06-22

### Miscellaneous

- Updated the following local packages: faucet-core

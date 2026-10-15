# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rabbitmq-v1.0.1...faucet-source-rabbitmq-v1.1.0) - 2026-10-15

### Features

- *(core)* RFC 0013 phase 3a — family drivers, governed-page typestate, deprecations (RFC 0014) ([#896](https://github.com/faucet-hq/faucet-stream/pull/896))
- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))
- Template test bundles with test-gated labels; one container start helper; refuse --no-auth on a network address ([#859](https://github.com/faucet-hq/faucet-stream/pull/859))

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rabbitmq-v1.0.0...faucet-source-rabbitmq-v1.0.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- Close the API source, file and messaging findings of the production-readiness audit (#789 group D)

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-source-rabbitmq-v1.0.0) - 2026-09-29

### Features

- One shared file writer for local, S3, GCS, Azure and SFTP; deprecate the csv, jsonl and parquet connectors ([#782](https://github.com/faucet-hq/faucet-stream/pull/782))
- RabbitMQ connector pair, GCS emulator suites in CI, integration-coverage gate, sectioned trigger form ([#699](https://github.com/faucet-hq/faucet-stream/pull/699))

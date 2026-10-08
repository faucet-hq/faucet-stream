# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-rabbitmq-v1.0.1...faucet-sink-rabbitmq-v1.0.2) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-rabbitmq-v1.0.0...faucet-sink-rabbitmq-v1.0.1) - 2026-09-30

### Bug Fixes

- *(release)* Move cross-connector tests into an unpublished crate so connectors never depend on each other ([#785](https://github.com/faucet-hq/faucet-stream/pull/785))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-sink-rabbitmq-v1.0.0) - 2026-09-29

### Features

- One shared file writer for local, S3, GCS, Azure and SFTP; deprecate the csv, jsonl and parquet connectors ([#782](https://github.com/faucet-hq/faucet-stream/pull/782))
- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- RabbitMQ connector pair, GCS emulator suites in CI, integration-coverage gate, sectioned trigger form ([#699](https://github.com/faucet-hq/faucet-stream/pull/699))

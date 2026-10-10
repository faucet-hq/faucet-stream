# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-oracle-v1.0.1...faucet-common-oracle-v1.1.0) - 2026-10-10

### Features

- Lossless incremental replication for postgres/mysql, and durable OTLP log shipping ([#852](https://github.com/faucet-hq/faucet-stream/pull/852))
- *(image)* The full image carries every feature, plus an opt-in full-oracle tag ([#847](https://github.com/faucet-hq/faucet-stream/pull/847))

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-oracle-v1.0.0...faucet-common-oracle-v1.0.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-common-oracle-v1.0.0) - 2026-09-29

### Features

- Oracle (source, LogMiner CDC, sink), Iceberg source, DynamoDB (source + Streams CDC, sink) and Databricks sink ([#739](https://github.com/faucet-hq/faucet-stream/pull/739))

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-oracle-v1.1.1...faucet-common-oracle-v1.2.0) - 2026-10-29

### Bug Fixes

- Resolve the #931/#933 review findings, Oracle pool flake and held #930 low packages ([#934](https://github.com/faucet-hq/faucet-stream/pull/934)) ([#949](https://github.com/faucet-hq/faucet-stream/pull/949))
- Resolve 310 low-severity audit findings across CLI, serve, core, connectors and tooling (#930 low) ([#947](https://github.com/faucet-hq/faucet-stream/pull/947))

### Features

- *(registry)* Faucet-registry crate for embedders, and review fixes for template param checks ([#996](https://github.com/faucet-hq/faucet-stream/pull/996))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-oracle-v1.1.0...faucet-common-oracle-v1.1.1) - 2026-10-22

### Bug Fixes

- Harden serve input screening, data-safety flows, incremental cursors and CDC positions (#930 critical + high) ([#931](https://github.com/faucet-hq/faucet-stream/pull/931))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-oracle-v1.0.1...faucet-common-oracle-v1.1.0) - 2026-10-15

### Bug Fixes

- Resolve the LOW and INFO findings of the #785–#861 review ([#869](https://github.com/faucet-hq/faucet-stream/pull/869))

### Features

- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))
- Lossless incremental replication for postgres/mysql, and durable OTLP log shipping ([#852](https://github.com/faucet-hq/faucet-stream/pull/852))
- *(image)* The full image carries every feature, plus an opt-in full-oracle tag ([#847](https://github.com/faucet-hq/faucet-stream/pull/847))

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-oracle-v1.0.0...faucet-common-oracle-v1.0.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-common-oracle-v1.0.0) - 2026-09-29

### Features

- Oracle (source, LogMiner CDC, sink), Iceberg source, DynamoDB (source + Streams CDC, sink) and Databricks sink ([#739](https://github.com/faucet-hq/faucet-stream/pull/739))

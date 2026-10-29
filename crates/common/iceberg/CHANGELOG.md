# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-iceberg-v1.1.0...faucet-common-iceberg-v1.2.0) - 2026-10-29

### Bug Fixes

- #966 review findings, integration tests, TLS everywhere, Iceberg HMS commits, Redshift IAM and Data API, Scorecard hardening ([#986](https://github.com/faucet-hq/faucet-stream/pull/986))
- Resolve the #931/#933 review findings, Oracle pool flake and held #930 low packages ([#934](https://github.com/faucet-hq/faucet-stream/pull/934)) ([#949](https://github.com/faucet-hq/faucet-stream/pull/949))
- Resolve 310 low-severity audit findings across CLI, serve, core, connectors and tooling (#930 low) ([#947](https://github.com/faucet-hq/faucet-stream/pull/947))

### Features

- Typed warehouse tables, sealed cluster secret params, execution pools, run label filter, Iceberg REST + Kafka codec fixes ([#966](https://github.com/faucet-hq/faucet-stream/pull/966))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-iceberg-v1.0.1...faucet-common-iceberg-v1.1.0) - 2026-10-15

### Features

- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-iceberg-v1.0.0...faucet-common-iceberg-v1.0.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW security findings from the #789 audit (serve, supply chain, auth, secrets) ([#827](https://github.com/faucet-hq/faucet-stream/pull/827))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-common-iceberg-v1.0.0) - 2026-09-29

### Features

- Oracle (source, LogMiner CDC, sink), Iceberg source, DynamoDB (source + Streams CDC, sink) and Databricks sink ([#739](https://github.com/faucet-hq/faucet-stream/pull/739))

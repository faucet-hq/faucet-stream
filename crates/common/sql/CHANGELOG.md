# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.1.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-sql-v1.1.2...faucet-common-sql-v1.1.3) - 2026-11-26

### Bug Fixes

- Read JSON floats with correct rounding, plus the design-first follow-ups of the performance pass ([#1107](https://github.com/faucet-hq/faucet-stream/pull/1107))
- Reliability and correctness fixes across the engine, serve, feeds and connectors ([#1096](https://github.com/faucet-hq/faucet-stream/pull/1096))

### Performance

- Clear measured bottlenecks across the engine, SQL, file, HTTP, messaging and serve paths ([#1101](https://github.com/faucet-hq/faucet-stream/pull/1101))

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-sql-v1.1.1...faucet-common-sql-v1.1.2) - 2026-11-12

### Bug Fixes

- *(serve)* Tighten what caller configs can reach; read connection strings like their clients; bound decoding; russh-sftp 3 ([#1063](https://github.com/faucet-hq/faucet-stream/pull/1063))
- *(serve)* Fail closed while the run history is unreachable, #1053 review fixes, real license texts ([#1056](https://github.com/faucet-hq/faucet-stream/pull/1056))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-sql-v1.1.0...faucet-common-sql-v1.1.1) - 2026-11-02

### Bug Fixes

- Tighten caller-config screening, connection defaults and serve request handling ([#1027](https://github.com/faucet-hq/faucet-stream/pull/1027))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-sql-v1.0.1...faucet-common-sql-v1.1.0) - 2026-10-29

### Bug Fixes

- #966 review findings, integration tests, TLS everywhere, Iceberg HMS commits, Redshift IAM and Data API, Scorecard hardening ([#986](https://github.com/faucet-hq/faucet-stream/pull/986))
- Resolve the #931/#933 review findings, Oracle pool flake and held #930 low packages ([#934](https://github.com/faucet-hq/faucet-stream/pull/934)) ([#949](https://github.com/faucet-hq/faucet-stream/pull/949))
- Resolve 310 low-severity audit findings across CLI, serve, core, connectors and tooling (#930 low) ([#947](https://github.com/faucet-hq/faucet-stream/pull/947))

### Features

- Typed warehouse tables, sealed cluster secret params, execution pools, run label filter, Iceberg REST + Kafka codec fixes ([#966](https://github.com/faucet-hq/faucet-stream/pull/966))

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-sql-v1.0.0...faucet-common-sql-v1.0.1) - 2026-10-22

### Bug Fixes

- Harden connectors, serve, CLI flows and core guarantees (#930 medium findings) ([#933](https://github.com/faucet-hq/faucet-stream/pull/933))
- Harden serve input screening, data-safety flows, incremental cursors and CDC positions (#930 critical + high) ([#931](https://github.com/faucet-hq/faucet-stream/pull/931))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-common-sql-v1.0.0) - 2026-10-15

### Bug Fixes

- *(core)* Second-review fixes for the RFC 0014 family drivers; instruction-count pipeline budget ([#897](https://github.com/faucet-hq/faucet-stream/pull/897))

### Features

- *(core)* RFC 0013 phase 3a — family drivers, governed-page typestate, deprecations (RFC 0014) ([#896](https://github.com/faucet-hq/faucet-stream/pull/896))
- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))

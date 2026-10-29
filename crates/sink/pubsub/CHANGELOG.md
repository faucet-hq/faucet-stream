# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-pubsub-v1.2.1...faucet-sink-pubsub-v1.3.0) - 2026-10-29

### Bug Fixes

- Template param path checks, run-request depends_on, YAML on yaml_serde, DuckDB writers, OpenTelemetry 0.33, MSSQL on tiberius 0.13 ([#993](https://github.com/faucet-hq/faucet-stream/pull/993))
- #966 review findings, integration tests, TLS everywhere, Iceberg HMS commits, Redshift IAM and Data API, Scorecard hardening ([#986](https://github.com/faucet-hq/faucet-stream/pull/986))
- Resolve the #931/#933 review findings, Oracle pool flake and held #930 low packages ([#934](https://github.com/faucet-hq/faucet-stream/pull/934)) ([#949](https://github.com/faucet-hq/faucet-stream/pull/949))
- Resolve 310 low-severity audit findings across CLI, serve, core, connectors and tooling (#930 low) ([#947](https://github.com/faucet-hq/faucet-stream/pull/947))

### Features

- *(registry)* Faucet-registry crate for embedders, and review fixes for template param checks ([#996](https://github.com/faucet-hq/faucet-stream/pull/996))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-pubsub-v1.2.0...faucet-sink-pubsub-v1.2.1) - 2026-10-23

### Bug Fixes

- Republish with `gcloud-pubsub`'s `jwt-aws-lc-rs` feature: 1.2.0 was published with `jwt-rust-crypto`, which conflicts with `faucet-common-pubsub` 1.2.1 (`gcloud-auth` refuses both features) and broke every fresh build depending on this crate

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-pubsub-v1.1.1...faucet-sink-pubsub-v1.2.0) - 2026-10-15

### Bug Fixes

- Resolve the HIGH and MEDIUM findings of the #785–#861 review ([#868](https://github.com/faucet-hq/faucet-stream/pull/868))

### Features

- *(core)* RFC 0013 phase 3a — family drivers, governed-page typestate, deprecations (RFC 0014) ([#896](https://github.com/faucet-hq/faucet-stream/pull/896))
- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))
- Template test bundles with test-gated labels; one container start helper; refuse --no-auth on a network address ([#859](https://github.com/faucet-hq/faucet-stream/pull/859))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-pubsub-v1.1.0...faucet-sink-pubsub-v1.1.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-pubsub-v1.0.5...faucet-sink-pubsub-v1.1.0) - 2026-09-29

### Bug Fixes

- Replace every message-grep classification with a typed one, and close the core retry gate ([#656](https://github.com/faucet-hq/faucet-stream/pull/656))

### Features

- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- Data-flow policies (`policy:` / `--policy`) and change impact analysis (`plan --impact`)
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-pubsub-v1.0.4...faucet-sink-pubsub-v1.0.5) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-pubsub

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-pubsub-v1.0.3...faucet-sink-pubsub-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-pubsub

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-pubsub-v1.0.2...faucet-sink-pubsub-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-pubsub

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-pubsub-v1.0.1...faucet-sink-pubsub-v1.0.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-pubsub

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-sink-pubsub-v1.0.0) - 2026-07-24

### Features

- *(connectors)* Redshift, Pub/Sub, ClickHouse, Azure Blob, and SQL Server CDC ([#362](https://github.com/faucet-hq/faucet-stream/pull/362))

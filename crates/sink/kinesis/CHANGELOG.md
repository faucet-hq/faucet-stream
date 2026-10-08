# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kinesis-v1.1.1...faucet-sink-kinesis-v1.1.2) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kinesis-v1.1.0...faucet-sink-kinesis-v1.1.1) - 2026-10-05

### Bug Fixes

- Build under a fresh dependency resolution ([#802](https://github.com/faucet-hq/faucet-stream/pull/802))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kinesis-v1.0.6...faucet-sink-kinesis-v1.1.0) - 2026-09-29

### Bug Fixes

- Replace every message-grep classification with a typed one, and close the core retry gate ([#656](https://github.com/faucet-hq/faucet-stream/pull/656))

### Features

- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.0.6](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kinesis-v1.0.5...faucet-sink-kinesis-v1.0.6) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kinesis

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kinesis-v1.0.4...faucet-sink-kinesis-v1.0.5) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kinesis

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kinesis-v1.0.3...faucet-sink-kinesis-v1.0.4) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kinesis

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kinesis-v1.0.2...faucet-sink-kinesis-v1.0.3) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kinesis

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kinesis-v1.0.0...faucet-sink-kinesis-v1.0.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kinesis

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-sink-kinesis-v1.0.0) - 2026-07-17

### Features

- AWS Kinesis source + sink connectors and shipped Grafana dashboards / Prometheus alerts

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/faucet-hq/faucet-stream/pull/311))

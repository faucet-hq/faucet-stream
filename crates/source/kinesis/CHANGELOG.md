# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.0.6](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-kinesis-v1.0.5...faucet-source-kinesis-v1.0.6) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kinesis

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-kinesis-v1.0.4...faucet-source-kinesis-v1.0.5) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kinesis

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-kinesis-v1.0.3...faucet-source-kinesis-v1.0.4) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kinesis

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-kinesis-v1.0.2...faucet-source-kinesis-v1.0.3) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kinesis

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-kinesis-v1.0.0...faucet-source-kinesis-v1.0.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kinesis

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-source-kinesis-v1.0.0) - 2026-07-17

### Bug Fixes

- Resolve #321 critical/high audit findings (exactly-once, cluster, transform-sql, compression) ([#322](https://github.com/faucet-hq/faucet-stream/pull/322))

### Features

- AWS Kinesis source + sink connectors and shipped Grafana dashboards / Prometheus alerts

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/faucet-hq/faucet-stream/pull/311))

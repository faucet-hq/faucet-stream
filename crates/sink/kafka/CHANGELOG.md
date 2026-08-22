# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.4.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kafka-v1.4.0...faucet-sink-kafka-v1.4.1) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kafka

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kafka-v1.3.5...faucet-sink-kafka-v1.4.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.3.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kafka-v1.3.4...faucet-sink-kafka-v1.3.5) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kafka

## [1.3.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kafka-v1.3.3...faucet-sink-kafka-v1.3.4) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kafka

## [1.3.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kafka-v1.3.1...faucet-sink-kafka-v1.3.2) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-kafka

## [1.3.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kafka-v1.3.0...faucet-sink-kafka-v1.3.1) - 2026-07-17

### Bug Fixes

- Resolve #321 critical/high audit findings (exactly-once, cluster, transform-sql, compression) ([#322](https://github.com/faucet-hq/faucet-stream/pull/322))

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/faucet-hq/faucet-stream/pull/311))

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kafka-v1.2.1...faucet-sink-kafka-v1.3.0) - 2026-07-10

### Features

- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kafka-v1.2.0...faucet-sink-kafka-v1.2.1) - 2026-07-08

### Bug Fixes

- *(sink-kafka)* Bound exactly-once commit-token reader to O(1) memory ([#269](https://github.com/faucet-hq/faucet-stream/pull/269))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-kafka-v1.1.0...faucet-sink-kafka-v1.2.0) - 2026-06-22

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))

### Features

- *(sink-kafka)* Exactly-once delivery via transactional producer ([#216](https://github.com/faucet-hq/faucet-stream/pull/216)) ([#253](https://github.com/faucet-hq/faucet-stream/pull/253))

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.6.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mongodb-v1.5.0...faucet-sink-mongodb-v1.6.0) - 2026-08-22

### Features

- *(sinks)* Add write_mode: overwrite (full-refresh) across data-storage sinks ([#493](https://github.com/faucet-hq/faucet-stream/pull/493))

## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mongodb-v1.4.0...faucet-sink-mongodb-v1.5.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mongodb-v1.3.3...faucet-sink-mongodb-v1.4.0) - 2026-08-15

### Features

- Scoped cleanup — delete records missing from a source's completeness claim ([#484](https://github.com/faucet-hq/faucet-stream/pull/484))

### Testing

- *(sinks)* Integration coverage for the scoped-cleanup delete paths ([#488](https://github.com/faucet-hq/faucet-stream/pull/488))

## [1.3.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mongodb-v1.3.2...faucet-sink-mongodb-v1.3.3) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mongodb-v1.3.0...faucet-sink-mongodb-v1.3.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mongodb-v1.2.0...faucet-sink-mongodb-v1.3.0) - 2026-07-17

### Features

- Connector conformance battery + tiers, FCP spec, sink-bound benchmark, sink config fixes ([#307](https://github.com/faucet-hq/faucet-stream/pull/307))

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/faucet-hq/faucet-stream/pull/311))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mongodb-v1.1.2...faucet-sink-mongodb-v1.2.0) - 2026-07-10

### Features

- Typed delivery guarantees, effectively-once coverage expansion, and prebuilt binary distribution ([#294](https://github.com/faucet-hq/faucet-stream/pull/294))

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mongodb-v1.1.1...faucet-sink-mongodb-v1.1.2) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-mongodb-v1.1.0...faucet-sink-mongodb-v1.1.1) - 2026-06-22

### Miscellaneous

- Updated the following local packages: faucet-core

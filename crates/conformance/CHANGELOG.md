# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.3.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-conformance-v1.3.2...faucet-conformance-v1.3.3) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-conformance-v1.3.1...faucet-conformance-v1.3.2) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-conformance-v1.3.0...faucet-conformance-v1.3.1) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-conformance-v1.2.0...faucet-conformance-v1.3.0) - 2026-08-10

### Features

- *(conformance)* Add discover-roundtrip and cancellation-flush integration checks ([#472](https://github.com/faucet-hq/faucet-stream/pull/472))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-conformance-v1.1.2...faucet-conformance-v1.2.0) - 2026-08-09

### Bug Fixes

- Resolve third-pass audit findings (contract DLQ index, GraphQL cycle guard, redshift null-row, spanner NUMERIC cursor) ([#467](https://github.com/faucet-hq/faucet-stream/pull/467))

### Features

- *(conformance,xml)* Registry-allowlist parity + capability matrix ([#465](https://github.com/faucet-hq/faucet-stream/pull/465)), soap: ergonomics block ([#468](https://github.com/faucet-hq/faucet-stream/pull/468)) ([#469](https://github.com/faucet-hq/faucet-stream/pull/469))

### Testing

- *(conformance)* Adopt the new capability checks across all connectors ([#470](https://github.com/faucet-hq/faucet-stream/pull/470))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-conformance-v1.1.0...faucet-conformance-v1.1.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-conformance-v1.0.0...faucet-conformance-v1.1.0) - 2026-07-17

### Features

- Connector conformance battery + tiers, FCP spec, sink-bound benchmark, sink config fixes ([#307](https://github.com/faucet-hq/faucet-stream/pull/307))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-conformance-v1.0.0) - 2026-07-10

### Features

- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.1.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-spanner-v1.1.2...faucet-source-spanner-v1.1.3) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-spanner

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-spanner-v1.1.1...faucet-source-spanner-v1.1.2) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-spanner

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-spanner-v1.1.0...faucet-source-spanner-v1.1.1) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-spanner

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-spanner-v1.0.3...faucet-source-spanner-v1.1.0) - 2026-08-10

### Features

- *(conformance)* Add discover-roundtrip and cancellation-flush integration checks ([#472](https://github.com/faucet-hq/faucet-stream/pull/472))

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-spanner-v1.0.2...faucet-source-spanner-v1.0.3) - 2026-08-09

### Bug Fixes

- Resolve third-pass audit findings (contract DLQ index, GraphQL cycle guard, redshift null-row, spanner NUMERIC cursor) ([#467](https://github.com/faucet-hq/faucet-stream/pull/467))

### Testing

- *(conformance)* Adopt the new capability checks across all connectors ([#470](https://github.com/faucet-hq/faucet-stream/pull/470))

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-spanner-v1.0.0...faucet-source-spanner-v1.0.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-spanner

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-source-spanner-v1.0.0) - 2026-07-17

### Features

- Google Cloud Spanner source + sink connectors ([#312](https://github.com/faucet-hq/faucet-stream/pull/312))

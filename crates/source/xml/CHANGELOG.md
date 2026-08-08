# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.2.6](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-xml-v1.2.5...faucet-source-xml-v1.2.6) - 2026-08-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-xml-v1.2.3...faucet-source-xml-v1.2.4) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-xml-v1.2.2...faucet-source-xml-v1.2.3) - 2026-07-17

### Bug Fixes

- Resolve #321 medium/low audit findings (quality/contract equality, CDC, pagination, serve, observability) ([#323](https://github.com/faucet-hq/faucet-stream/pull/323))

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/faucet-hq/faucet-stream/pull/311))

## [1.2.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-xml-v1.2.1...faucet-source-xml-v1.2.2) - 2026-07-10

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-xml-v1.2.0...faucet-source-xml-v1.2.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-xml-v1.1.0...faucet-source-xml-v1.2.0) - 2026-06-22

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

### Features

- Unified resilience policy (retry / circuit-breaker / poison-pill) ([#252](https://github.com/faucet-hq/faucet-stream/pull/252))

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-xml-v1.4.0...faucet-source-xml-v1.5.0) - 2026-08-22

### Features

- Config-driven parity — REST headers/async-url, XML decode/paging, flow-auth sign/capture (#539–#544) ([#545](https://github.com/faucet-hq/faucet-stream/pull/545))
- MTLS, ES overwrite, OAuth1, and completeness reconciliation ([#506](https://github.com/faucet-hq/faucet-stream/pull/506))

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-xml-v1.3.1...faucet-source-xml-v1.4.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.3.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-xml-v1.3.0...faucet-source-xml-v1.3.1) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-xml-v1.2.5...faucet-source-xml-v1.3.0) - 2026-08-09

### Features

- *(conformance,xml)* Registry-allowlist parity + capability matrix ([#465](https://github.com/faucet-hq/faucet-stream/pull/465)), soap: ergonomics block ([#468](https://github.com/faucet-hq/faucet-stream/pull/468)) ([#469](https://github.com/faucet-hq/faucet-stream/pull/469))

### Testing

- *(conformance)* Adopt the new capability checks across all connectors ([#470](https://github.com/faucet-hq/faucet-stream/pull/470))

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

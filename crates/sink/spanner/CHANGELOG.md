# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-spanner-v1.2.1...faucet-sink-spanner-v1.3.0) - 2026-10-15

### Bug Fixes

- Resolve the LOW and INFO findings of the #785–#861 review ([#869](https://github.com/faucet-hq/faucet-stream/pull/869))

### Features

- *(core)* RFC 0013 phase 3a — family drivers, governed-page typestate, deprecations (RFC 0014) ([#896](https://github.com/faucet-hq/faucet-stream/pull/896))
- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))
- Template test bundles with test-gated labels; one container start helper; refuse --no-auth on a network address ([#859](https://github.com/faucet-hq/faucet-stream/pull/859))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-spanner-v1.2.0...faucet-sink-spanner-v1.2.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- Close the SQL and CDC connector findings of the production-readiness audit (#789 group C)

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-spanner-v1.1.2...faucet-sink-spanner-v1.2.0) - 2026-09-29

### Bug Fixes

- Replace every message-grep classification with a typed one, and close the core retry gate ([#656](https://github.com/faucet-hq/faucet-stream/pull/656))

### Features

- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- Content verification (`faucet verify`) and run rollback (`faucet rollback`) ([#725](https://github.com/faucet-hq/faucet-stream/pull/725))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-spanner-v1.1.1...faucet-sink-spanner-v1.1.2) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-spanner

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-spanner-v1.1.0...faucet-sink-spanner-v1.1.1) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-spanner

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-spanner-v1.0.3...faucet-sink-spanner-v1.1.0) - 2026-08-15

### Features

- Scoped cleanup — delete records missing from a source's completeness claim ([#484](https://github.com/faucet-hq/faucet-stream/pull/484))

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-spanner-v1.0.2...faucet-sink-spanner-v1.0.3) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-spanner

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-spanner-v1.0.0...faucet-sink-spanner-v1.0.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-spanner

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-sink-spanner-v1.0.0) - 2026-07-17

### Features

- Google Cloud Spanner source + sink connectors ([#312](https://github.com/faucet-hq/faucet-stream/pull/312))

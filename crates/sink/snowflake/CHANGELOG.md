# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.7.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.7.0...faucet-sink-snowflake-v1.7.1) - 2026-10-22

### Bug Fixes

- Harden connectors, serve, CLI flows and core guarantees (#930 medium findings) ([#933](https://github.com/faucet-hq/faucet-stream/pull/933))
- Harden serve input screening, data-safety flows, incremental cursors and CDC positions (#930 critical + high) ([#931](https://github.com/faucet-hq/faucet-stream/pull/931))

## [1.7.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.6.1...faucet-sink-snowflake-v1.7.0) - 2026-10-15

### Bug Fixes

- Resolve the LOW and INFO findings of the #785–#861 review ([#869](https://github.com/faucet-hq/faucet-stream/pull/869))
- Resolve the HIGH and MEDIUM findings of the #785–#861 review ([#868](https://github.com/faucet-hq/faucet-stream/pull/868))

### Features

- *(core)* RFC 0013 phase 3a — family drivers, governed-page typestate, deprecations (RFC 0014) ([#896](https://github.com/faucet-hq/faucet-stream/pull/896))
- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))

## [1.6.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.6.0...faucet-sink-snowflake-v1.6.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- Close the SQL and CDC connector findings of the production-readiness audit (#789 group C)

## [1.6.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.5.0...faucet-sink-snowflake-v1.6.0) - 2026-09-29

### Features

- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.4.0...faucet-sink-snowflake-v1.5.0) - 2026-08-23

### Features

- Datetime window slicing, tree_flatten, staged-load foundation, persistent run logs, chained discovery (#527–#531) ([#532](https://github.com/faucet-hq/faucet-stream/pull/532))

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.3.3...faucet-sink-snowflake-v1.4.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.3.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.3.2...faucet-sink-snowflake-v1.3.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-snowflake

## [1.3.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.3.1...faucet-sink-snowflake-v1.3.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-snowflake

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.2.1...faucet-sink-snowflake-v1.3.0) - 2026-07-24

### Features

- Live `run` progress + Arrow BigQuery & Snowflake paths (#385, #380, #381) ([#395](https://github.com/faucet-hq/faucet-stream/pull/395))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.2.0...faucet-sink-snowflake-v1.2.1) - 2026-07-17

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-snowflake

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.1.2...faucet-sink-snowflake-v1.2.0) - 2026-07-10

### Features

- Typed delivery guarantees, effectively-once coverage expansion, and prebuilt binary distribution ([#294](https://github.com/faucet-hq/faucet-stream/pull/294))

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.1.1...faucet-sink-snowflake-v1.1.2) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-snowflake

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-snowflake-v1.1.0...faucet-sink-snowflake-v1.1.1) - 2026-06-22

### Bug Fixes

- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/faucet-hq/faucet-stream/pull/264)) ([#265](https://github.com/faucet-hq/faucet-stream/pull/265))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

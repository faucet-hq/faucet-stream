# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-transform-sql-v1.2.0...faucet-transform-sql-v1.3.0) - 2026-09-26

### Bug Fixes

- Replace every message-grep classification with a typed one, and close the core retry gate ([#656](https://github.com/faucet-hq/faucet-stream/pull/656))

### Features

- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-transform-sql-v1.1.4...faucet-transform-sql-v1.2.0) - 2026-08-23

### Features

- Meltano-migration connector gaps — REST pagination/records, graphql/sql/auth/transform features, overwrite fan-out fix (#547–#558) ([#563](https://github.com/faucet-hq/faucet-stream/pull/563))

## [1.1.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-transform-sql-v1.1.3...faucet-transform-sql-v1.1.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-transform-sql-v1.1.2...faucet-transform-sql-v1.1.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-transform-sql-v1.1.1...faucet-transform-sql-v1.1.2) - 2026-08-09

### Bug Fixes

- Second-pass audit — wide-integer corruption in the Arrow/SQL shim and SQL binds, backfill DST windows (#460, #461, #462) ([#463](https://github.com/faucet-hq/faucet-stream/pull/463))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-transform-sql-v1.0.4...faucet-transform-sql-v1.1.0) - 2026-07-24

### Documentation

- Architecture-review follow-ups — ADRs, SDK streaming docs, build_pipeline refactor, Arrow benchmark ([#324](https://github.com/faucet-hq/faucet-stream/pull/324)) ([#373](https://github.com/faucet-hq/faucet-stream/pull/373))

### Features

- *(cli)* Config-change preview — `faucet plan --diff` ([#374](https://github.com/faucet-hq/faucet-stream/pull/374)) ([#378](https://github.com/faucet-hq/faucet-stream/pull/378))

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-transform-sql-v1.0.3...faucet-transform-sql-v1.0.4) - 2026-07-17

### Bug Fixes

- Resolve #321 critical/high audit findings (exactly-once, cluster, transform-sql, compression) ([#322](https://github.com/faucet-hq/faucet-stream/pull/322))

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-transform-sql-v1.0.2...faucet-transform-sql-v1.0.3) - 2026-07-10

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-transform-sql-v1.0.1...faucet-transform-sql-v1.0.2) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-transform-sql-v1.0.0...faucet-transform-sql-v1.0.1) - 2026-06-22

### Miscellaneous

- Updated the following local packages: faucet-core

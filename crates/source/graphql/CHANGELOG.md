# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.5.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-graphql-v1.5.0...faucet-source-graphql-v1.5.1) - 2026-09-19

### Bug Fixes

- Replace every message-grep classification with a typed one, and close the core retry gate ([#656](https://github.com/faucet-hq/faucet-stream/pull/656))

### Testing

- Cover the three pure decisions that were only reachable through I/O ([#659](https://github.com/faucet-hq/faucet-stream/pull/659))

## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-graphql-v1.4.0...faucet-source-graphql-v1.5.0) - 2026-08-24

### Features

- BigQuery auto-create-table + Meltano-migration connector/CLI gaps (#567–#569, #573) ([#577](https://github.com/faucet-hq/faucet-stream/pull/577))

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-graphql-v1.3.0...faucet-source-graphql-v1.4.0) - 2026-08-23

### Features

- Meltano-migration connector gaps — REST pagination/records, graphql/sql/auth/transform features, overwrite fan-out fix (#547–#558) ([#563](https://github.com/faucet-hq/faucet-stream/pull/563))
- MTLS, ES overwrite, OAuth1, and completeness reconciliation ([#506](https://github.com/faucet-hq/faucet-stream/pull/506))

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-graphql-v1.2.7...faucet-source-graphql-v1.3.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))
- *(sources)* Fail-fast config validation for csv/duckdb/elasticsearch/gcs/graphql ([#489](https://github.com/faucet-hq/faucet-stream/pull/489))

## [1.2.7](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-graphql-v1.2.6...faucet-source-graphql-v1.2.7) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.6](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-graphql-v1.2.5...faucet-source-graphql-v1.2.6) - 2026-08-09

### Bug Fixes

- Resolve third-pass audit findings (contract DLQ index, GraphQL cycle guard, redshift null-row, spanner NUMERIC cursor) ([#467](https://github.com/faucet-hq/faucet-stream/pull/467))

### Testing

- *(conformance)* Adopt the new capability checks across all connectors ([#470](https://github.com/faucet-hq/faucet-stream/pull/470))

## [1.2.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-graphql-v1.2.3...faucet-source-graphql-v1.2.4) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-graphql-v1.2.2...faucet-source-graphql-v1.2.3) - 2026-07-17

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-graphql-v1.2.1...faucet-source-graphql-v1.2.2) - 2026-07-10

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-graphql-v1.2.0...faucet-source-graphql-v1.2.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-graphql-v1.1.0...faucet-source-graphql-v1.2.0) - 2026-06-22

### Bug Fixes

- Resolve all 18 Low reliability/data-integrity findings (F40–F57, #264) ([#267](https://github.com/faucet-hq/faucet-stream/pull/267))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

### Features

- Unified resilience policy (retry / circuit-breaker / poison-pill) ([#252](https://github.com/faucet-hq/faucet-stream/pull/252))

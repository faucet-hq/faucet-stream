# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rest-v1.4.4...faucet-source-rest-v1.5.0) - 2026-08-22

### Features

- Config-driven parity — REST headers/async-url, XML decode/paging, flow-auth sign/capture (#539–#544) ([#545](https://github.com/faucet-hq/faucet-stream/pull/545))
- Rest partitions fan-out + repeated query params, cross_join transform, ClickHouse staged load (#535/#536/#534/#528) ([#537](https://github.com/faucet-hq/faucet-stream/pull/537))
- Datetime window slicing, tree_flatten, staged-load foundation, persistent run logs, chained discovery (#527–#531) ([#532](https://github.com/faucet-hq/faucet-stream/pull/532))
- Response-decode + async-job (rest), scoped overwrite, run/lineage metadata columns ([#526](https://github.com/faucet-hq/faucet-stream/pull/526))
- *(source)* Composable auth flows, OData mode + $metadata discovery, server-side incremental push-down ([#524](https://github.com/faucet-hq/faucet-stream/pull/524))
- *(transforms)* Inbuilt reshape transforms — json_encode, unpivot, lookup ([#520](https://github.com/faucet-hq/faucet-stream/pull/520))
- *(source-rest)* Add response_format json|csv|excel for authed file bodies ([#508](https://github.com/faucet-hq/faucet-stream/pull/508))
- MTLS, ES overwrite, OAuth1, and completeness reconciliation ([#506](https://github.com/faucet-hq/faucet-stream/pull/506))
- *(rest,auth)* Enterprise HTTP auth & POST-search pagination gaps ([#505](https://github.com/faucet-hq/faucet-stream/pull/505))

## [1.4.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rest-v1.4.3...faucet-source-rest-v1.4.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.4.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rest-v1.4.2...faucet-source-rest-v1.4.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.4.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rest-v1.4.1...faucet-source-rest-v1.4.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rest-v1.3.1...faucet-source-rest-v1.4.0) - 2026-07-31

### Bug Fixes

- Keep transform additions a minor release — pin touched crates to 1.x

### Features

- *(transforms)* [**breaking**] Hash / json_parse / coalesce / split / join, value_case title+capitalize, keys_case dot, stdout csv (#403–#409) — faucet-core 2.0.0 ([#418](https://github.com/faucet-hq/faucet-stream/pull/418))

## [1.3.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rest-v1.3.0...faucet-source-rest-v1.3.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rest-v1.2.2...faucet-source-rest-v1.3.0) - 2026-07-17

### Bug Fixes

- Resolve #321 medium/low audit findings (quality/contract equality, CDC, pagination, serve, observability) ([#323](https://github.com/faucet-hq/faucet-stream/pull/323))
- Resolve #321 critical/high audit findings (exactly-once, cluster, transform-sql, compression) ([#322](https://github.com/faucet-hq/faucet-stream/pull/322))

### Features

- Connector conformance battery + tiers, FCP spec, sink-bound benchmark, sink config fixes ([#307](https://github.com/faucet-hq/faucet-stream/pull/307))

## [1.2.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rest-v1.2.1...faucet-source-rest-v1.2.2) - 2026-07-10

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rest-v1.2.0...faucet-source-rest-v1.2.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-rest-v1.1.0...faucet-source-rest-v1.2.0) - 2026-06-22

### Bug Fixes

- Resolve all 18 Low reliability/data-integrity findings (F40–F57, #264) ([#267](https://github.com/faucet-hq/faucet-stream/pull/267))
- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/faucet-hq/faucet-stream/pull/264)) ([#265](https://github.com/faucet-hq/faucet-stream/pull/265))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

### Features

- Unified resilience policy (retry / circuit-breaker / poison-pill) ([#252](https://github.com/faucet-hq/faucet-stream/pull/252))

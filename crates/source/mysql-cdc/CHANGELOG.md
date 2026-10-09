# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.3.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-cdc-v1.3.0...faucet-source-mysql-cdc-v1.3.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- MEDIUM/LOW security findings from the #789 audit (serve, supply chain, auth, secrets) ([#827](https://github.com/faucet-hq/faucet-stream/pull/827))
- Close the SQL and CDC connector findings of the production-readiness audit (#789 group C)

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-cdc-v1.2.7...faucet-source-mysql-cdc-v1.3.0) - 2026-09-29

### Features

- One shared file writer for local, S3, GCS, Azure and SFTP; deprecate the csv, jsonl and parquet connectors ([#782](https://github.com/faucet-hq/faucet-stream/pull/782))
- Multi-table faucet mirror over a table set, and prebuilt Windows binaries ([#758](https://github.com/faucet-hq/faucet-stream/pull/758))
- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.2.7](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-cdc-v1.2.6...faucet-source-mysql-cdc-v1.2.7) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.6](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-cdc-v1.2.5...faucet-source-mysql-cdc-v1.2.6) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-cdc-v1.2.4...faucet-source-mysql-cdc-v1.2.5) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-cdc-v1.2.3...faucet-source-mysql-cdc-v1.2.4) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-cdc-v1.2.1...faucet-source-mysql-cdc-v1.2.2) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-cdc-v1.2.0...faucet-source-mysql-cdc-v1.2.1) - 2026-07-17

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-cdc-v1.1.1...faucet-source-mysql-cdc-v1.2.0) - 2026-07-10

### Features

- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-cdc-v1.1.0...faucet-source-mysql-cdc-v1.1.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-mysql-cdc-v1.0.0...faucet-source-mysql-cdc-v1.1.0) - 2026-06-22

### Bug Fixes

- Resolve all 20 Medium reliability/data-integrity findings (F20–F39, #264) ([#266](https://github.com/faucet-hq/faucet-stream/pull/266))
- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/faucet-hq/faucet-stream/pull/264)) ([#265](https://github.com/faucet-hq/faucet-stream/pull/265))
- *(mysql-cdc)* Support MySQL 8.4+ current-position capture ([#242](https://github.com/faucet-hq/faucet-stream/pull/242)) ([#247](https://github.com/faucet-hq/faucet-stream/pull/247))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))

### Features

- Consistent snapshot → CDC replication handoff — faucet replicate ([#189](https://github.com/faucet-hq/faucet-stream/pull/189))

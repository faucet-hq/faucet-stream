# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.2.1...faucet-common-mssql-v1.3.0) - 2026-10-29

### Bug Fixes

- Template param path checks, run-request depends_on, YAML on yaml_serde, DuckDB writers, OpenTelemetry 0.33, MSSQL on tiberius 0.13 ([#993](https://github.com/faucet-hq/faucet-stream/pull/993))
- *(deps)* Zip 9.0.5, OSV config for the fuzz lockfile, MSRV badge, SPDX-named license file ([#992](https://github.com/faucet-hq/faucet-stream/pull/992))
- Fresh-resolve pin, last #930 finding, dependency cleanup, YAML module, template for_each and pair bootstrap ([#954](https://github.com/faucet-hq/faucet-stream/pull/954))
- Resolve 310 low-severity audit findings across CLI, serve, core, connectors and tooling (#930 low) ([#947](https://github.com/faucet-hq/faucet-stream/pull/947))

### Features

- *(registry)* Faucet-registry crate for embedders, and review fixes for template param checks ([#996](https://github.com/faucet-hq/faucet-stream/pull/996))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.2.0...faucet-common-mssql-v1.2.1) - 2026-10-22

### Bug Fixes

- Harden serve input screening, data-safety flows, incremental cursors and CDC positions (#930 critical + high) ([#931](https://github.com/faucet-hq/faucet-stream/pull/931))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.1.1...faucet-common-mssql-v1.2.0) - 2026-10-15

### Features

- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.1.0...faucet-common-mssql-v1.1.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- MEDIUM/LOW security findings from the #789 audit (serve, supply chain, auth, secrets) ([#827](https://github.com/faucet-hq/faucet-stream/pull/827))
- Close the SQL and CDC connector findings of the production-readiness audit (#789 group C)

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.0.12...faucet-common-mssql-v1.1.0) - 2026-09-29

### Features

- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.0.12](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.0.11...faucet-common-mssql-v1.0.12) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.11](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.0.10...faucet-common-mssql-v1.0.11) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.10](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.0.9...faucet-common-mssql-v1.0.10) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.9](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.0.8...faucet-common-mssql-v1.0.9) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.7](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.0.6...faucet-common-mssql-v1.0.7) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.6](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.0.5...faucet-common-mssql-v1.0.6) - 2026-07-17

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.0.4...faucet-common-mssql-v1.0.5) - 2026-07-10

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.0.3...faucet-common-mssql-v1.0.4) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-mssql-v1.0.2...faucet-common-mssql-v1.0.3) - 2026-06-22

### Miscellaneous

- Updated the following local packages: faucet-core

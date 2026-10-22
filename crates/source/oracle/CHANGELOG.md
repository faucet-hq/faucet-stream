# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-oracle-v1.1.0...faucet-source-oracle-v1.1.1) - 2026-10-22

### Bug Fixes

- Harden serve input screening, data-safety flows, incremental cursors and CDC positions (#930 critical + high) ([#931](https://github.com/faucet-hq/faucet-stream/pull/931))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-oracle-v1.0.1...faucet-source-oracle-v1.1.0) - 2026-10-15

### Bug Fixes

- Resolve the LOW and INFO findings of the #785–#861 review ([#869](https://github.com/faucet-hq/faucet-stream/pull/869))
- Resolve the HIGH and MEDIUM findings of the #785–#861 review ([#868](https://github.com/faucet-hq/faucet-stream/pull/868))

### Features

- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))
- Template test bundles with test-gated labels; one container start helper; refuse --no-auth on a network address ([#859](https://github.com/faucet-hq/faucet-stream/pull/859))

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-oracle-v1.0.0...faucet-source-oracle-v1.0.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- Close the SQL and CDC connector findings of the production-readiness audit (#789 group C)

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-source-oracle-v1.0.0) - 2026-09-29

### Features

- Multi-table faucet mirror over a table set, and prebuilt Windows binaries ([#758](https://github.com/faucet-hq/faucet-stream/pull/758))
- Oracle (source, LogMiner CDC, sink), Iceberg source, DynamoDB (source + Streams CDC, sink) and Databricks sink ([#739](https://github.com/faucet-hq/faucet-stream/pull/739))

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-sql-v1.0.0...faucet-common-sql-v1.0.1) - 2026-10-22

### Bug Fixes

- Harden connectors, serve, CLI flows and core guarantees (#930 medium findings) ([#933](https://github.com/faucet-hq/faucet-stream/pull/933))
- Harden serve input screening, data-safety flows, incremental cursors and CDC positions (#930 critical + high) ([#931](https://github.com/faucet-hq/faucet-stream/pull/931))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-common-sql-v1.0.0) - 2026-10-15

### Bug Fixes

- *(core)* Second-review fixes for the RFC 0014 family drivers; instruction-count pipeline budget ([#897](https://github.com/faucet-hq/faucet-stream/pull/897))

### Features

- *(core)* RFC 0013 phase 3a — family drivers, governed-page typestate, deprecations (RFC 0014) ([#896](https://github.com/faucet-hq/faucet-stream/pull/896))
- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))

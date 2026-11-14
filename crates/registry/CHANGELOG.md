# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-registry-v1.0.2...faucet-registry-v1.0.3) - 2026-11-14

### Bug Fixes

- *(webhook)* Review follow-ups — declared consumes_destructively capability, per-run paths, page cap ([#1081](https://github.com/faucet-hq/faucet-stream/pull/1081))
- Webhook answers after commit, numeric schedule versions, self-closing advisory issue, coverage cache budget ([#1074](https://github.com/faucet-hq/faucet-stream/pull/1074))

### Documentation

- Switch the docs site to Astro Starlight and remove mdBook ([#1080](https://github.com/faucet-hq/faucet-stream/pull/1080))

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-registry-v1.0.1...faucet-registry-v1.0.2) - 2026-11-12

### Bug Fixes

- *(serve)* Tighten what caller configs can reach; read connection strings like their clients; bound decoding; russh-sftp 3 ([#1063](https://github.com/faucet-hq/faucet-stream/pull/1063))
- *(serve)* Fail closed while the run history is unreachable, #1053 review fixes, real license texts ([#1056](https://github.com/faucet-hq/faucet-stream/pull/1056))

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-registry-v1.0.0...faucet-registry-v1.0.1) - 2026-11-02

### Bug Fixes

- Tighten caller-config screening, connection defaults and serve request handling ([#1027](https://github.com/faucet-hq/faucet-stream/pull/1027))

### Testing

- Kinesis LocalStack tests fail fast ([#999](https://github.com/faucet-hq/faucet-stream/pull/999)), and fixture-heavy unit tests move out of CodeQL's scan ([#1001](https://github.com/faucet-hq/faucet-stream/pull/1001))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-registry-v1.0.0) - 2026-10-29

### Features

- *(registry)* Faucet-registry crate for embedders, and review fixes for template param checks ([#996](https://github.com/faucet-hq/faucet-stream/pull/996))

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-rabbitmq-v1.1.0...faucet-common-rabbitmq-v1.1.1) - 2026-10-22

### Bug Fixes

- Harden serve input screening, data-safety flows, incremental cursors and CDC positions (#930 critical + high) ([#931](https://github.com/faucet-hq/faucet-stream/pull/931))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-rabbitmq-v1.0.0...faucet-common-rabbitmq-v1.1.0) - 2026-10-15

### Features

- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))
- Template test bundles with test-gated labels; one container start helper; refuse --no-auth on a network address ([#859](https://github.com/faucet-hq/faucet-stream/pull/859))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-common-rabbitmq-v1.0.0) - 2026-09-29

### Features

- RabbitMQ connector pair, GCS emulator suites in CI, integration-coverage gate, sectioned trigger form ([#699](https://github.com/faucet-hq/faucet-stream/pull/699))

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-nats-v1.0.4...faucet-source-nats-v1.0.5) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-nats

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-nats-v1.0.3...faucet-source-nats-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-nats

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-nats-v1.0.2...faucet-source-nats-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-nats

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-nats-v1.0.1...faucet-source-nats-v1.0.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-nats

## [1.0.0] - 2026-07-28

### Features

- Initial release: NATS source — subscribes to a subject (core NATS with
  `*`/`>` wildcards and optional queue groups) or pulls from a durable
  JetStream consumer, drains with `max_messages` / `idle_timeout_secs`
  termination, and streams payloads as JSON with bounded memory. Conformance
  battery wired ([#411](https://github.com/faucet-hq/faucet-stream/issues/411)).

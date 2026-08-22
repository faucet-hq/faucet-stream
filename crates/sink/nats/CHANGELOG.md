# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-nats-v1.0.4...faucet-sink-nats-v1.0.5) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-nats

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-nats-v1.0.3...faucet-sink-nats-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-nats

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-nats-v1.0.2...faucet-sink-nats-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-nats

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-nats-v1.0.1...faucet-sink-nats-v1.0.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-nats

## [1.0.0] - 2026-07-28

### Features

- Initial release: NATS sink — publishes each record as a JSON message to a
  fixed subject or a per-record subject (`subject_field`), flushing after each
  batch. Append-only. Conformance battery wired
  ([#411](https://github.com/faucet-hq/faucet-stream/issues/411)).

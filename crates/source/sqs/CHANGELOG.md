# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-sqs-v1.0.4...faucet-source-sqs-v1.0.5) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sqs

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-sqs-v1.0.3...faucet-source-sqs-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sqs

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-sqs-v1.0.2...faucet-source-sqs-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sqs

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-sqs-v1.0.1...faucet-source-sqs-v1.0.2) - 2026-08-09

### Bug Fixes

- Resolve the fourth hardening audit — topology governance bypass, SQS at-most-once, control-plane secret leaks ([#456](https://github.com/faucet-hq/faucet-stream/pull/456)) ([#457](https://github.com/faucet-hq/faucet-stream/pull/457))

### Testing

- *(conformance)* Adopt the new capability checks across all connectors ([#470](https://github.com/faucet-hq/faucet-stream/pull/470))

## [1.0.0] - 2026-07-28

### Features

- Initial release: AWS SQS source — long-polls `ReceiveMessage`, buffers to
  `batch_size` and streams pages with bounded memory, deletes each page's
  receipt handles before yielding (at-least-once), and terminates on
  `idle_timeout_secs` / `max_messages`. Conformance battery wired
  ([#412](https://github.com/faucet-hq/faucet-stream/issues/412)).

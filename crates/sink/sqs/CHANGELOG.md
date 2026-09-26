# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sqs-v1.0.5...faucet-sink-sqs-v1.1.0) - 2026-09-26

### Bug Fixes

- Replace every message-grep classification with a typed one, and close the core retry gate ([#656](https://github.com/faucet-hq/faucet-stream/pull/656))

### Features

- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sqs-v1.0.4...faucet-sink-sqs-v1.0.5) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sqs

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sqs-v1.0.3...faucet-sink-sqs-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sqs

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sqs-v1.0.2...faucet-sink-sqs-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sqs

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sqs-v1.0.1...faucet-sink-sqs-v1.0.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sqs

## [1.0.0] - 2026-07-28

### Features

- Initial release: AWS SQS sink — batched `SendMessageBatch` writes (≤10 entries
  / ≤256 KiB per request), bounded request concurrency, per-entry
  partial-failure retry, optional FIFO `message_group_id` /
  `message_deduplication_id`, and DLQ-routable per-record outcomes. Conformance
  battery wired ([#412](https://github.com/faucet-hq/faucet-stream/issues/412)).

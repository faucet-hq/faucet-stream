# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.4.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.4.1...faucet-sink-jsonl-v1.4.2) - 2026-10-08

### Bug Fixes

- #789 engine findings (core, files, transforms, CLI) + open bug issues ([#840](https://github.com/faucet-hq/faucet-stream/pull/840))

## [1.4.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.4.0...faucet-sink-jsonl-v1.4.1) - 2026-09-30

### Bug Fixes

- Post-merge review of #782 — data-loss fixes, async cancellable writer, streaming uploads, one shared sink, finished migration ([#788](https://github.com/faucet-hq/faucet-stream/pull/788))

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.3.6...faucet-sink-jsonl-v1.4.0) - 2026-09-29

### Features

- One shared file writer for local, S3, GCS, Azure and SFTP; deprecate the csv, jsonl and parquet connectors ([#782](https://github.com/faucet-hq/faucet-stream/pull/782))
- REST/GraphQL source gaps, silent-data-loss fixes and Google service-account auth ([#760](https://github.com/faucet-hq/faucet-stream/pull/760))
- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- RabbitMQ connector pair, GCS emulator suites in CI, integration-coverage gate, sectioned trigger form ([#699](https://github.com/faucet-hq/faucet-stream/pull/699))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))
- *(serve)* Retention GC for local sink outputs + Datasets-page cleanup controls ([#596](https://github.com/faucet-hq/faucet-stream/pull/596))

### Testing

- Engine-level reliability program — guarantee suites, fidelity corpus, state-format gates ([#660](https://github.com/faucet-hq/faucet-stream/pull/660))

## [1.3.6](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.3.5...faucet-sink-jsonl-v1.3.6) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-core

## [1.3.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.3.4...faucet-sink-jsonl-v1.3.5) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-core

## [1.3.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.3.3...faucet-sink-jsonl-v1.3.4) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-core

## [1.3.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.3.2...faucet-sink-jsonl-v1.3.3) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-core

## [1.3.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.3.0...faucet-sink-jsonl-v1.3.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-core

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.2.0...faucet-sink-jsonl-v1.3.0) - 2026-07-17

### Features

- Encryption at rest for state/DLQ + live TUI for faucet run ([#315](https://github.com/faucet-hq/faucet-stream/pull/315))
- Connector conformance battery + tiers, FCP spec, sink-bound benchmark, sink config fixes ([#307](https://github.com/faucet-hq/faucet-stream/pull/307))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.1.2...faucet-sink-jsonl-v1.2.0) - 2026-07-10

### Features

- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.1.1...faucet-sink-jsonl-v1.1.2) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-jsonl-v1.1.0...faucet-sink-jsonl-v1.1.1) - 2026-06-22

### Miscellaneous

- Updated the following local packages: faucet-core

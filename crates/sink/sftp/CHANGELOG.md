# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.1.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sftp-v1.1.1...faucet-sink-sftp-v1.1.2) - 2026-10-08

### Bug Fixes

- #789 engine findings (core, files, transforms, CLI) + open bug issues ([#840](https://github.com/faucet-hq/faucet-stream/pull/840))
- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- Close the API source, file and messaging findings of the production-readiness audit (#789 group D)

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sftp-v1.1.0...faucet-sink-sftp-v1.1.1) - 2026-09-30

### Bug Fixes

- Post-merge review of #782 — data-loss fixes, async cancellable writer, streaming uploads, one shared sink, finished migration ([#788](https://github.com/faucet-hq/faucet-stream/pull/788))
- *(release)* Move cross-connector tests into an unpublished crate so connectors never depend on each other ([#785](https://github.com/faucet-hq/faucet-stream/pull/785))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sftp-v1.0.5...faucet-sink-sftp-v1.1.0) - 2026-09-29

### Features

- One shared file writer for local, S3, GCS, Azure and SFTP; deprecate the csv, jsonl and parquet connectors ([#782](https://github.com/faucet-hq/faucet-stream/pull/782))
- Local file source and sink, Avro/ORC formats, Singer target bridge, source throttling metrics and template row selection ([#745](https://github.com/faucet-hq/faucet-stream/pull/745))
- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sftp-v1.0.4...faucet-sink-sftp-v1.0.5) - 2026-08-23

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sftp, faucet-common-sftp

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sftp-v1.0.3...faucet-sink-sftp-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sftp, faucet-common-sftp

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sftp-v1.0.2...faucet-sink-sftp-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sftp, faucet-common-sftp

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-sftp-v1.0.1...faucet-sink-sftp-v1.0.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sftp, faucet-common-sftp

## [1.0.0] - 2026-07-28

### Features

- Initial release: SFTP sink connector — writes records to an SFTP server as
  JSON Lines objects under a remote directory. Atomic writes (upload to a
  temporary name, then rename into place), append-only, lazy connect with a
  reused session, conformance battery wired ([#410](https://github.com/faucet-hq/faucet-stream/issues/410)).

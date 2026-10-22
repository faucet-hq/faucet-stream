# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-file-v1.1.0...faucet-source-file-v1.1.1) - 2026-10-22

### Bug Fixes

- Harden connectors, serve, CLI flows and core guarantees (#930 medium findings) ([#933](https://github.com/faucet-hq/faucet-stream/pull/933))
- Harden serve input screening, data-safety flows, incremental cursors and CDC positions (#930 critical + high) ([#931](https://github.com/faucet-hq/faucet-stream/pull/931))

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-file-v1.0.2...faucet-source-file-v1.1.0) - 2026-10-15

### Features

- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-file-v1.0.1...faucet-source-file-v1.0.2) - 2026-10-08

### Bug Fixes

- #789 engine findings (core, files, transforms, CLI) + open bug issues ([#840](https://github.com/faucet-hq/faucet-stream/pull/840))
- Close the API source, file and messaging findings of the production-readiness audit (#789 group D)
- Close the core engine and CLI runtime findings of the production-readiness audit (#789 group B)

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-file-v1.0.0...faucet-source-file-v1.0.1) - 2026-09-30

### Bug Fixes

- Post-merge review of #782 — data-loss fixes, async cancellable writer, streaming uploads, one shared sink, finished migration ([#788](https://github.com/faucet-hq/faucet-stream/pull/788))
- *(release)* Move cross-connector tests into an unpublished crate so connectors never depend on each other ([#785](https://github.com/faucet-hq/faucet-stream/pull/785))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-source-file-v1.0.0) - 2026-09-29

### Features

- One shared file writer for local, S3, GCS, Azure and SFTP; deprecate the csv, jsonl and parquet connectors ([#782](https://github.com/faucet-hq/faucet-stream/pull/782))
- Local file source and sink, Avro/ORC formats, Singer target bridge, source throttling metrics and template row selection ([#745](https://github.com/faucet-hq/faucet-stream/pull/745))

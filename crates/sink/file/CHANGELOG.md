# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-file-v1.0.0...faucet-sink-file-v1.0.1) - 2026-09-30

### Bug Fixes

- Post-merge review of #782 — data-loss fixes, async cancellable writer, streaming uploads, one shared sink, finished migration ([#788](https://github.com/faucet-hq/faucet-stream/pull/788))
- *(release)* Move cross-connector tests into an unpublished crate so connectors never depend on each other ([#785](https://github.com/faucet-hq/faucet-stream/pull/785))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-sink-file-v1.0.0) - 2026-09-29

### Features

- One shared file writer for local, S3, GCS, Azure and SFTP; deprecate the csv, jsonl and parquet connectors ([#782](https://github.com/faucet-hq/faucet-stream/pull/782))
- REST/GraphQL source gaps, silent-data-loss fixes and Google service-account auth ([#760](https://github.com/faucet-hq/faucet-stream/pull/760))
- Local file source and sink, Avro/ORC formats, Singer target bridge, source throttling metrics and template row selection ([#745](https://github.com/faucet-hq/faucet-stream/pull/745))

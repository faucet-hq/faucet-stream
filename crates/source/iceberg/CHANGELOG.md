# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-iceberg-v1.0.1...faucet-source-iceberg-v1.0.2) - 2026-10-08

### Bug Fixes

- #789 engine findings (core, files, transforms, CLI) + open bug issues ([#840](https://github.com/faucet-hq/faucet-stream/pull/840))

## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-iceberg-v1.0.0...faucet-source-iceberg-v1.0.1) - 2026-09-30

### Bug Fixes

- Post-merge review of #782 — data-loss fixes, async cancellable writer, streaming uploads, one shared sink, finished migration ([#788](https://github.com/faucet-hq/faucet-stream/pull/788))

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-source-iceberg-v1.0.0) - 2026-09-29

### Features

- One shared file writer for local, S3, GCS, Azure and SFTP; deprecate the csv, jsonl and parquet connectors ([#782](https://github.com/faucet-hq/faucet-stream/pull/782))
- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- Oracle (source, LogMiner CDC, sink), Iceberg source, DynamoDB (source + Streams CDC, sink) and Databricks sink ([#739](https://github.com/faucet-hq/faucet-stream/pull/739))

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-common-feed-v1.0.0) - 2026-11-26

### Bug Fixes

- Iceberg vended credentials, single-precision cursors, DST schedules and tzdata 2026f, plus the #1100/#1093 follow-ups ([#1121](https://github.com/faucet-hq/faucet-stream/pull/1121))
- Reliability and correctness fixes across the engine, serve, feeds and connectors ([#1096](https://github.com/faucet-hq/faucet-stream/pull/1096))

### Features

- Feeds, durable push ingestion for faucet serve ([#1085](https://github.com/faucet-hq/faucet-stream/pull/1085))

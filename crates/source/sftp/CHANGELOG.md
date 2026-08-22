# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.1.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-sftp-v1.1.0...faucet-source-sftp-v1.1.1) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sftp, faucet-common-sftp

## [1.1.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-sftp-v1.0.3...faucet-source-sftp-v1.1.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-sftp-v1.0.2...faucet-source-sftp-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sftp, faucet-common-sftp

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-source-sftp-v1.0.1...faucet-source-sftp-v1.0.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-common-sftp, faucet-common-sftp

## [1.0.0] - 2026-07-28

### Features

- Initial release: SFTP source connector — lists a remote directory (or reads a
  single file) over SFTP and streams the files as JSON Lines, JSON arrays, or
  raw text with bounded memory. Filename glob filter, lazy connect, conformance
  battery wired ([#410](https://github.com/faucet-hq/faucet-stream/issues/410)).

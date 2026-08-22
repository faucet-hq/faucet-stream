# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).

## [1.0.5](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-sftp-v1.0.4...faucet-common-sftp-v1.0.5) - 2026-08-22

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.4](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-sftp-v1.0.3...faucet-common-sftp-v1.0.4) - 2026-08-16

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-sftp-v1.0.2...faucet-common-sftp-v1.0.3) - 2026-08-15

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.2](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-sftp-v1.0.1...faucet-common-sftp-v1.0.2) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.0.0] - 2026-07-28

### Features

- Initial release: shared SFTP connection config (`SftpConnectionConfig`,
  `SftpAuth`, `HostKeyPolicy`) and the async `connect` helper used by the
  `faucet-source-sftp` and `faucet-sink-sftp` connectors. Password and
  private-key auth; host-key verification via strict / accept-new / insecure
  policies; secret-safe `Debug` ([#410](https://github.com/faucet-hq/faucet-stream/issues/410)).

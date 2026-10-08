# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.0.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-common-singer-v1.0.0...faucet-common-singer-v1.0.1) - 2026-10-08

### Bug Fixes

- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- Close the security and trust-boundary findings of the production-readiness audit (#789 group A)

## [1.0.0](https://github.com/faucet-hq/faucet-stream/releases/tag/faucet-common-singer-v1.0.0) - 2026-09-29

### Features

- Local file source and sink, Avro/ORC formats, Singer target bridge, source throttling metrics and template row selection ([#745](https://github.com/faucet-hq/faucet-stream/pull/745))

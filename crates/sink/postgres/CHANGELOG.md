# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.9.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.9.0...faucet-sink-postgres-v1.9.1) - 2026-10-22

### Bug Fixes

- Harden connectors, serve, CLI flows and core guarantees (#930 medium findings) ([#933](https://github.com/faucet-hq/faucet-stream/pull/933))
- Harden serve input screening, data-safety flows, incremental cursors and CDC positions (#930 critical + high) ([#931](https://github.com/faucet-hq/faucet-stream/pull/931))

### Miscellaneous

- Enable database features only in the respective connectors ([#901](https://github.com/faucet-hq/faucet-stream/pull/901))

## [1.9.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.8.1...faucet-sink-postgres-v1.9.0) - 2026-10-15

### Bug Fixes

- *(core)* Second-review fixes for the RFC 0014 family drivers; instruction-count pipeline budget ([#897](https://github.com/faucet-hq/faucet-stream/pull/897))
- Resolve the LOW and INFO findings of the #785–#861 review ([#869](https://github.com/faucet-hq/faucet-stream/pull/869))
- Resolve the HIGH and MEDIUM findings of the #785–#861 review ([#868](https://github.com/faucet-hq/faucet-stream/pull/868))

### Features

- *(core)* RFC 0013 phase 3a — family drivers, governed-page typestate, deprecations (RFC 0014) ([#896](https://github.com/faucet-hq/faucet-stream/pull/896))
- RFC 0013 phase 2 — shared SQL engine, fidelity model, FileSink backends, runtime API ([#894](https://github.com/faucet-hq/faucet-stream/pull/894))
- RFC 0013 phase 1 — structural core guarantees ([#891](https://github.com/faucet-hq/faucet-stream/pull/891))
- Template test bundles with test-gated labels; one container start helper; refuse --no-auth on a network address ([#859](https://github.com/faucet-hq/faucet-stream/pull/859))

## [1.8.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.8.0...faucet-sink-postgres-v1.8.1) - 2026-10-08

### Bug Fixes

- #789 engine findings (core, files, transforms, CLI) + open bug issues ([#840](https://github.com/faucet-hq/faucet-stream/pull/840))
- MEDIUM/LOW connector findings from the #789 audit (SQL/CDC, messaging, API) ([#839](https://github.com/faucet-hq/faucet-stream/pull/839))
- Close the SQL and CDC connector findings of the production-readiness audit (#789 group C)
- Close the core engine and CLI runtime findings of the production-readiness audit (#789 group B)

## [1.8.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.7.0...faucet-sink-postgres-v1.8.0) - 2026-09-29

### Bug Fixes

- Replace every message-grep classification with a typed one, and close the core retry gate ([#656](https://github.com/faucet-hq/faucet-stream/pull/656))

### Features

- Pipeline status, source lag, state management, versioned state and safe partial batches ([#742](https://github.com/faucet-hq/faucet-stream/pull/742))
- Content verification (`faucet verify`) and run rollback (`faucet rollback`) ([#725](https://github.com/faucet-hq/faucet-stream/pull/725))
- Deployment overlays, template version deprecation, first-run tables, one-meaning vocabulary ([#693](https://github.com/faucet-hq/faucet-stream/pull/693))
- File formats, template test suites, auto-create tables, strict config keys + the ≥580 throughput pass ([#668](https://github.com/faucet-hq/faucet-stream/pull/668))

### Testing

- Finish the reliability program — drift matrix, fidelity pairs, container faults ([#663](https://github.com/faucet-hq/faucet-stream/pull/663))

## [1.7.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.6.0...faucet-sink-postgres-v1.7.0) - 2026-08-23

### Features

- Response-decode + async-job (rest), scoped overwrite, run/lineage metadata columns ([#526](https://github.com/faucet-hq/faucet-stream/pull/526))
- *(sinks)* Add write_mode: overwrite (full-refresh) across data-storage sinks ([#493](https://github.com/faucet-hq/faucet-stream/pull/493))

## [1.6.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.5.0...faucet-sink-postgres-v1.6.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.4.3...faucet-sink-postgres-v1.5.0) - 2026-08-15

### Features

- Scoped cleanup — delete records missing from a source's completeness claim ([#484](https://github.com/faucet-hq/faucet-stream/pull/484))

### Testing

- *(sinks)* Integration coverage for the scoped-cleanup delete paths ([#488](https://github.com/faucet-hq/faucet-stream/pull/488))

## [1.4.3](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.4.2...faucet-sink-postgres-v1.4.3) - 2026-08-09

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.4.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.4.0...faucet-sink-postgres-v1.4.1) - 2026-07-24

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.3.0...faucet-sink-postgres-v1.4.0) - 2026-07-17

### Features

- *(sink-postgres)* COPY bulk-load fast-path (write_method: copy) ([#313](https://github.com/faucet-hq/faucet-stream/pull/313))
- Connector conformance battery + tiers, FCP spec, sink-bound benchmark, sink config fixes ([#307](https://github.com/faucet-hq/faucet-stream/pull/307))

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/faucet-hq/faucet-stream/pull/311))

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.2.1...faucet-sink-postgres-v1.3.0) - 2026-07-10

### Features

- Typed delivery guarantees, effectively-once coverage expansion, and prebuilt binary distribution ([#294](https://github.com/faucet-hq/faucet-stream/pull/294))
- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.2.1](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.2.0...faucet-sink-postgres-v1.2.1) - 2026-07-08

### Miscellaneous

- Updated the following local packages: faucet-core

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-sink-postgres-v1.1.0...faucet-sink-postgres-v1.2.0) - 2026-06-22

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

### Features

- Schema-drift handling policy (warn/evolve/ignore/quarantine/fail) ([#194](https://github.com/faucet-hq/faucet-stream/pull/194))

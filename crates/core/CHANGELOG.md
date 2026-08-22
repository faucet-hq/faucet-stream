# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.11.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-core-v1.10.0...faucet-core-v1.11.0) - 2026-08-22

### Bug Fixes

- *(transforms)* Drop dangling transform-unpivot cfg from CdcUnwrap match arms ([#522](https://github.com/faucet-hq/faucet-stream/pull/522))

### Features

- Rest partitions fan-out + repeated query params, cross_join transform, ClickHouse staged load (#535/#536/#534/#528) ([#537](https://github.com/faucet-hq/faucet-stream/pull/537))
- Datetime window slicing, tree_flatten, staged-load foundation, persistent run logs, chained discovery (#527–#531) ([#532](https://github.com/faucet-hq/faucet-stream/pull/532))
- Response-decode + async-job (rest), scoped overwrite, run/lineage metadata columns ([#526](https://github.com/faucet-hq/faucet-stream/pull/526))
- *(source)* Composable auth flows, OData mode + $metadata discovery, server-side incremental push-down ([#524](https://github.com/faucet-hq/faucet-stream/pull/524))
- *(transforms)* Inbuilt reshape transforms — json_encode, unpivot, lookup ([#520](https://github.com/faucet-hq/faucet-stream/pull/520))
- MTLS, ES overwrite, OAuth1, and completeness reconciliation ([#506](https://github.com/faucet-hq/faucet-stream/pull/506))
- *(sinks)* Add write_mode: overwrite (full-refresh) across data-storage sinks ([#493](https://github.com/faucet-hq/faucet-stream/pull/493))

## [1.10.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-core-v1.9.0...faucet-core-v1.10.0) - 2026-08-16

### Features

- *(cli)* --json for list/validate, schema --list, connector labels, and validation & test hardening ([#491](https://github.com/faucet-hq/faucet-stream/pull/491))

## [1.9.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-core-v1.8.0...faucet-core-v1.9.0) - 2026-08-15

### Features

- Scoped cleanup — delete records missing from a source's completeness claim ([#484](https://github.com/faucet-hq/faucet-stream/pull/484))

## [1.8.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-core-v1.7.1...faucet-core-v1.8.0) - 2026-08-09

### Bug Fixes

- Resolve third-pass audit findings (contract DLQ index, GraphQL cycle guard, redshift null-row, spanner NUMERIC cursor) ([#467](https://github.com/faucet-hq/faucet-stream/pull/467))
- Second-pass audit — wide-integer corruption in the Arrow/SQL shim and SQL binds, backfill DST windows (#460, #461, #462) ([#463](https://github.com/faucet-hq/faucet-stream/pull/463))
- Resolve the fourth hardening audit — topology governance bypass, SQS at-most-once, control-plane secret leaks ([#456](https://github.com/faucet-hq/faucet-stream/pull/456)) ([#457](https://github.com/faucet-hq/faucet-stream/pull/457))

### Features

- *(topology)* Exactly-once delivery + per-node SLA/notify/lineage/catalog ([#464](https://github.com/faucet-hq/faucet-stream/pull/464))

## [1.7.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-core-v1.6.0...faucet-core-v1.7.0) - 2026-07-31

### Bug Fixes

- Keep transform additions a minor release — pin touched crates to 1.x

### Features

- *(cli)* Topology mode — fan-out (tee), fan-in (merge), and cross-source join (#71, #72) ([#421](https://github.com/faucet-hq/faucet-stream/pull/421))
- *(transforms)* [**breaking**] Hash / json_parse / coalesce / split / join, value_case title+capitalize, keys_case dot, stdout csv (#403–#409) — faucet-core 2.0.0 ([#418](https://github.com/faucet-hq/faucet-stream/pull/418))

## [1.6.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-core-v1.5.0...faucet-core-v1.6.0) - 2026-07-24

### Documentation

- Architecture-review follow-ups — ADRs, SDK streaming docs, build_pipeline refactor, Arrow benchmark ([#324](https://github.com/faucet-hq/faucet-stream/pull/324)) ([#373](https://github.com/faucet-hq/faucet-stream/pull/373))

### Features

- *(cli)* Config-change preview — `faucet plan --diff` ([#374](https://github.com/faucet-hq/faucet-stream/pull/374)) ([#378](https://github.com/faucet-hq/faucet-stream/pull/378))

## [1.5.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-core-v1.4.0...faucet-core-v1.5.0) - 2026-07-17

### Bug Fixes

- Resolve #321 medium/low audit findings (quality/contract equality, CDC, pagination, serve, observability) ([#323](https://github.com/faucet-hq/faucet-stream/pull/323))
- Resolve #321 critical/high audit findings (exactly-once, cluster, transform-sql, compression) ([#322](https://github.com/faucet-hq/faucet-stream/pull/322))

### Features

- Encryption at rest for state/DLQ + live TUI for faucet run ([#315](https://github.com/faucet-hq/faucet-stream/pull/315))
- Connector conformance battery + tiers, FCP spec, sink-bound benchmark, sink config fixes ([#307](https://github.com/faucet-hq/faucet-stream/pull/307))
- Faucet discover (live source introspection) + faucet backfill (resumable historical replay)

## [1.4.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-core-v1.3.0...faucet-core-v1.4.0) - 2026-07-10

### Features

- Typed delivery guarantees, effectively-once coverage expansion, and prebuilt binary distribution ([#294](https://github.com/faucet-hq/faucet-stream/pull/294))
- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/faucet-hq/faucet-stream/pull/289))

## [1.3.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-core-v1.2.0...faucet-core-v1.3.0) - 2026-07-08

### Features

- Persistent Data Movement Catalog — datasets, schema timelines & lineage graph ([#286](https://github.com/faucet-hq/faucet-stream/pull/286))
- *(cli)* DLQ replay & management — faucet dlq inspect / replay / discard ([#281](https://github.com/faucet-hq/faucet-stream/pull/281)) ([#285](https://github.com/faucet-hq/faucet-stream/pull/285))
- *(masking)* PII detection + column-level masking policies ([#206](https://github.com/faucet-hq/faucet-stream/pull/206))
- *(core)* Data contracts — versioned output schema/constraints enforced per page ([#272](https://github.com/faucet-hq/faucet-stream/pull/272))
- Extend cluster Mode B sharding to mysql, mssql, sqlite, gcs, and parquet sources ([#271](https://github.com/faucet-hq/faucet-stream/pull/271))

## [1.2.0](https://github.com/faucet-hq/faucet-stream/compare/faucet-core-v1.1.0...faucet-core-v1.2.0) - 2026-06-22

### Bug Fixes

- Resolve all 18 Low reliability/data-integrity findings (F40–F57, #264) ([#267](https://github.com/faucet-hq/faucet-stream/pull/267))
- Resolve all 20 Medium reliability/data-integrity findings (F20–F39, #264) ([#266](https://github.com/faucet-hq/faucet-stream/pull/266))
- Resolve all Critical & High reliability/data-integrity findings ([#264](https://github.com/faucet-hq/faucet-stream/pull/264)) ([#265](https://github.com/faucet-hq/faucet-stream/pull/265))
- *(s3,gcs)* Verify object read integrity — length + opt-in checksum ([#161](https://github.com/faucet-hq/faucet-stream/pull/161)) ([#257](https://github.com/faucet-hq/faucet-stream/pull/257))

### Documentation

- Extensive standardized READMEs for all crates + badge/category fixes ([#250](https://github.com/faucet-hq/faucet-stream/pull/250))
- *(readme)* Use `cargo add` for install examples (no pinned versions) ([#240](https://github.com/faucet-hq/faucet-stream/pull/240))

### Features

- Serve cluster Mode B — source-shard distribution across workers ([#230](https://github.com/faucet-hq/faucet-stream/pull/230)) ([#263](https://github.com/faucet-hq/faucet-stream/pull/263))
- *(observability)* OpenTelemetry (OTLP) trace + metric export ([#201](https://github.com/faucet-hq/faucet-stream/pull/201)) ([#259](https://github.com/faucet-hq/faucet-stream/pull/259))
- Schema-drift handling policy (warn/evolve/ignore/quarantine/fail) ([#194](https://github.com/faucet-hq/faucet-stream/pull/194))
- Unified resilience policy (retry / circuit-breaker / poison-pill) ([#252](https://github.com/faucet-hq/faucet-stream/pull/252))
- Consistent snapshot → CDC replication handoff — faucet replicate ([#189](https://github.com/faucet-hq/faucet-stream/pull/189))

# faucet-common-spanner

Shared configuration and conversion types for the
[faucet-stream](https://github.com/faucet-hq/faucet-stream) Google Cloud
Spanner connectors — [`faucet-source-spanner`](https://crates.io/crates/faucet-source-spanner)
and [`faucet-sink-spanner`](https://crates.io/crates/faucet-sink-spanner).

Both connectors re-export these types, so you normally depend on the source or
sink crate rather than this one directly.

## Contents

- **`SpannerCredentials`** — the auth enum, serialized in faucet's consistent
  `{ type: <method>, config: { … } }` wire shape:

  | `type` | Fields | Meaning |
  |--------|--------|---------|
  | `application_default` | — | Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default login`, GCE/GKE metadata) — the default |
  | `service_account_key_path` | `path` | Service-account key JSON loaded from a file |
  | `service_account_key` | `json` | Inline service-account key JSON — prefer `${vault:…}` / `${env:…}` interpolation over literals; `Debug` masks it |

- **`SpannerConnection`** — the flattened connection block shared by both
  connector configs: `project_id` / `instance` / `database` / `auth` /
  `max_sessions` (session-pool bound; channels sized at one per 100 sessions)
  / `emulator_host` (per-connector emulator override — the process-global
  `SPANNER_EMULATOR_HOST` env var is also honored when unset) / `dialect`
  (`google_standard_sql` by default, or `postgresql`). Provides
  `connect()` (data-plane client) and `connect_admin()` (DDL/instance admin
  client) plus `database_path()`.

- **`SpannerDialect`** (module `dialect`) — the GoogleSQL / PostgreSQL
  differences in one place: identifier quoting, placeholders and bind names,
  catalog queries, DDL type names, scope casts, and `named_to_positional`
  (rewrites `@name` to `$N` for PostgreSQL). The GoogleSQL output is the
  pre-existing text, unchanged.

- **`types`** — `parse_spanner_type` (`INFORMATION_SCHEMA` type strings →
  `SpannerType`, including `ARRAY<…>`), `parse_pg_spanner_type` (the
  PostgreSQL spellings) and `spanner_type_to_json_schema`.

- **`decode`** — generic Spanner row → `serde_json::Value` decoding for
  arbitrary queries: INT64 stays a lossless JSON integer, NUMERIC stays a
  string, JSON columns parse to structured values, BYTES stay base64,
  ARRAY/STRUCT recurse.

- **`encode`** — `serde_json::Value` → mutation value encoding keyed by the
  destination column type, with typed mismatch errors.

## License

MIT OR Apache-2.0

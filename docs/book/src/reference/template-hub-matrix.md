# Template Hub — source × sink matrix

<!-- GENERATED from hub/ by `cargo test -p faucet-cli --test hub_catalog -- --ignored regenerate`; do not edit by hand. -->

2 source templates × 4 sink templates. ✓ = every stream has a write mode the sink supports; ◐ = some streams do; — = none. Each source section below carries the copy-paste command for every compatible sink.

| source \ sink | [faucet-hq/bigquery](#sink-faucet-hq-bigquery) | [faucet-hq/jsonl](#sink-faucet-hq-jsonl) | [faucet-hq/postgres](#sink-faucet-hq-postgres) | [faucet-hq/sqlite](#sink-faucet-hq-sqlite) |
|---|:---:|:---:|:---:|:---:|
| [faucet-hq/example-csv](#faucet-hq-example-csv) | ✓ | ✓ | ✓ | ✓ |
| [faucet-hq/example-rest-api](#faucet-hq-example-rest-api) | ✓ | ✓ | ✓ | ✓ |

## Sinks

### sink: faucet-hq/bigquery

<a id="sink-faucet-hq-bigquery"></a>Google BigQuery — one table per stream, atomic overwrite or keyed MERGE upsert

- connector: `bigquery` · write modes: `append`, `upsert`, `delete`, `overwrite`
- params:
  - `bq_dataset` (default `"raw"`) — Dataset every stream's table lands in
  - `bq_project` (required) — GCP project id that owns the dataset
  - `bq_sa_key` (required, secret) — Service-account key JSON, inline. Pass it with --param-env from a secret store; copy this template and switch `auth` to application_default to use ADC instead.

### sink: faucet-hq/jsonl

<a id="sink-faucet-hq-jsonl"></a>Local JSON Lines files, one per stream — the local validation destination

- connector: `jsonl` · write modes: `append`
- satisfies by construction: `overwrite`→`append`
- params:
  - `out_dir` (default `"./out"`) — Directory to write <source>/<stream>.jsonl under

### sink: faucet-hq/postgres

<a id="sink-faucet-hq-postgres"></a>PostgreSQL — one auto-mapped table per stream, transactional overwrite or ON CONFLICT upsert

- connector: `postgres` · write modes: `append`, `upsert`, `delete`, `overwrite`
- params:
  - `pg_schema` (default `"public"`) — Schema every stream's table is created in
  - `pg_url` (required, secret) — postgres://user:pass@host:5432/db connection URL

### sink: faucet-hq/sqlite

<a id="sink-faucet-hq-sqlite"></a>Local SQLite database — one auto-mapped table per stream, real overwrite/upsert semantics without infrastructure

- connector: `sqlite` · write modes: `append`, `upsert`, `delete`, `overwrite`
- params:
  - `sqlite_path` (default `"./out/faucet.db"`) — Database file (created if missing)

## Sources

### faucet-hq/example-csv

<a id="faucet-hq-example-csv"></a>Example — two CSV exports as two streams (runs offline, no credentials)

- tags: `example`, `file`
- connector: `csv` · 2 stream(s)
- params:
  - `data_dir` (default `"./hub/examples/data"`) — Directory holding orders.csv and customers.csv

| stream | write (preference) | primary keys |
|---|---|---|
| `orders` | `overwrite` → `upsert` | `id` |
| `customers` | `overwrite` → `upsert` | `id` |

**→ faucet-hq/bigquery**

```bash
faucet run --source faucet-hq/example-csv --sink faucet-hq/bigquery \
  --param bq_project=<bq_project> \
  --param bq_sa_key="$BQ_SA_KEY"
```

**→ faucet-hq/jsonl** — 2 stream(s) run through an alias: `orders` overwrite→append, `customers` overwrite→append

```bash
faucet run --source faucet-hq/example-csv --sink faucet-hq/jsonl
```

**→ faucet-hq/postgres**

```bash
faucet run --source faucet-hq/example-csv --sink faucet-hq/postgres \
  --param pg_url="$PG_URL"
```

**→ faucet-hq/sqlite**

```bash
faucet run --source faucet-hq/example-csv --sink faucet-hq/sqlite
```

### faucet-hq/example-rest-api

<a id="faucet-hq-example-rest-api"></a>Example — a bearer-authenticated REST API with cursor pagination and one stream per endpoint

- tags: `example`, `rest`
- connector: `rest` · 2 stream(s)
- params:
  - `api_token` (required, secret) — Bearer token
  - `base_url` (required) — API root, e.g. https://api.example.com/v1

| stream | write (preference) | primary keys |
|---|---|---|
| `accounts` | `overwrite` → `upsert` | `id` |
| `events` | `upsert` → `append` | `id` |

**→ faucet-hq/bigquery**

```bash
faucet run --source faucet-hq/example-rest-api --sink faucet-hq/bigquery \
  --param api_token="$API_TOKEN" \
  --param base_url=<base_url> \
  --param bq_project=<bq_project> \
  --param bq_sa_key="$BQ_SA_KEY"
```

**→ faucet-hq/jsonl** — 1 stream(s) run through an alias: `accounts` overwrite→append

```bash
faucet run --source faucet-hq/example-rest-api --sink faucet-hq/jsonl \
  --param api_token="$API_TOKEN" \
  --param base_url=<base_url>
```

**→ faucet-hq/postgres**

```bash
faucet run --source faucet-hq/example-rest-api --sink faucet-hq/postgres \
  --param api_token="$API_TOKEN" \
  --param base_url=<base_url> \
  --param pg_url="$PG_URL"
```

**→ faucet-hq/sqlite**

```bash
faucet run --source faucet-hq/example-rest-api --sink faucet-hq/sqlite \
  --param api_token="$API_TOKEN" \
  --param base_url=<base_url>
```


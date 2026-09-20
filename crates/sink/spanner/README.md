# faucet-sink-spanner

Google Cloud Spanner sink connector for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

Writes JSON records to a Spanner table as batched **mutations** — `Insert` for append, Spanner's native `InsertOrUpdate` for `write_mode: upsert`, and `Delete` mutations for delete mode — one atomic commit per chunk. Supports exactly-once delivery (`delivery: exactly_once`) via a `faucet_commit_token` watermark row committed in the same read-write transaction as the page, and additive schema evolution through the database-admin DDL API.

## Example

```yaml
version: 1
name: kafka-to-spanner
pipeline:
  source:
    kind: kafka
    config:
      brokers: ["localhost:9092"]
      topics: ["users"]
      group_id: faucet
      max_messages: 10000
  sink:
    kind: spanner
    config:
      project_id: my-project
      instance: my-instance
      database: my-db
      table_name: users
      write_mode: upsert
      key: [id]                  # must equal the table's PRIMARY KEY
      auth:
        type: application_default
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `project_id` | string | — | GCP project that owns the Spanner instance. |
| `instance` | string | — | Spanner instance ID. |
| `database` | string | — | Database name within the instance. |
| `table_name` | string | — | Target table. Must already exist (mutations require a schema). |
| `auth` | object | `application_default` | Credentials — see [Authentication](#authentication). |
| `max_sessions` | integer | `100` | Upper bound on pooled Spanner sessions. Channels are sized at one per 100 sessions. |
| `emulator_host` | string | — | Emulator endpoint (`host:port`). Plaintext + unauthenticated; scoped to this connector (no env-var races). `SPANNER_EMULATOR_HOST` is still honored when unset. |
| `batch_size` | integer | `1000` | Rows per commit. `0` = no row-count re-chunking (the cell budget below still applies). |
| `ddl_timeout_secs` | integer | `300` | Bound on schema-change (long-running DDL operation) waits. |
| `write_mode` | string | `append` | `append` \| `upsert` \| `delete`. |
| `key` | array | `[]` | Key columns for upsert/delete. **Must equal the table's PRIMARY KEY columns** (order-insensitive) — Spanner mutations always key on the PK. |
| `delete_marker` | object | — | Upsert only: `{ field, values }` — rows whose `field` matches become deletes; the marker field is stripped. |

### Authentication

The `auth` block uses the project-wide `{ type, config }` shape:

```yaml
auth:
  type: service_account_key_path   # or service_account_key | application_default
  config:
    path: /secrets/sa.json
```

- `application_default` (default) — `GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default login`, or the GCE/GKE metadata server.
- `service_account_key_path` — `config.path` to a key file.
- `service_account_key` — `config.json` inline key (prefer `${vault:…}` / `${env:…}` interpolation).

## Value encoding

Values are encoded against the destination column types read once from `INFORMATION_SCHEMA`:

| Spanner column | Accepted JSON | Notes |
|---|---|---|
| `INT64` | integer, integer-string | String-encoded on the wire — lossless above 2^53. |
| `FLOAT64` / `FLOAT32` | number, `"NaN"`/`"Infinity"`/`"-Infinity"` | |
| `BOOL` | boolean | |
| `STRING` | any | Scalars coerce to text; containers serialize as JSON text. |
| `TIMESTAMP` / `DATE` | RFC 3339 string | |
| `BYTES` | base64 string | Same form the Spanner source decodes to. |
| `NUMERIC` | string or number | Strings pass through losslessly. |
| `JSON` | any | The value's JSON serialization. |
| `ARRAY<T>` | array | Elements encoded as `T`. |

Record fields with no matching column are dropped with a one-shot warning per field. A type mismatch fails that row with an error naming the column.

## Write modes

- **`append`** (default) — `Insert` mutations. A duplicate PRIMARY KEY fails the whole commit (chunk); route pages through a DLQ with `on_batch_error: dlq_all` if you need to absorb duplicates, or use `upsert`.
- **`upsert`** — `InsertOrUpdate` keyed on the table's PRIMARY KEY. `key` must equal the PK column set; this is validated on first write and by `faucet doctor`.
- **`delete`** / `delete_marker` — `Delete` mutations. Key values are re-ordered into PK order automatically. Note Spanner cascades deletes to `INTERLEAVE IN PARENT … ON DELETE CASCADE` child tables.

## Batching and the 80,000-cell commit limit

Spanner rejects commits above ~80,000 mutated cells (rows × columns, **plus** secondary-index amplification the client cannot see). The sink chunks each page at `batch_size` rows *and* a conservative 60,000-cell budget, whichever comes first. If the target table has many secondary indexes, lower `batch_size` further.

## Exactly-once delivery

`delivery: exactly_once` commits each page's mutations **and** an `InsertOrUpdate` on the watermark table in one read-write transaction:

```sql
CREATE TABLE IF NOT EXISTS faucet_commit_token (
  scope      STRING(MAX) NOT NULL,
  token      STRING(MAX) NOT NULL,
  updated_at TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (scope)
```

The table is auto-created via the admin DDL API on first use. On crash either the page and its token both landed or neither did; on resume the pipeline reads `last_committed_token` and skips already-committed pages. The idempotent page is a **single commit** (no re-chunking), so it is bounded by the 80,000-cell limit — size the source's `batch_size` down for very wide tables. `ABORTED` commit contention is retried automatically by the client with its recommended backoff.

## Schema evolution (drift `on_drift: evolve`)

- **New columns** — `ALTER TABLE … ADD COLUMN IF NOT EXISTS` (`integer→INT64`, `number→FLOAT64`, `boolean→BOOL`, `string→STRING(MAX)`, `object→JSON`), always nullable.
- **Nullability relaxations** — the column is re-emitted at its current type without `NOT NULL`.
- **Base-type widenings (e.g. INT64→FLOAT64) are not supported by Spanner.** The sink returns a typed error advising `allow_type_widening: false`, which makes the drift policy classify the change `incompatible` and route it via `on_incompatible` (`fail` or `quarantine`).

DDL runs as a Spanner long-running operation, bounded by `ddl_timeout_secs`.

## Emulator

Point the sink at the [Cloud Spanner emulator](https://cloud.google.com/spanner/docs/emulator) (`gcr.io/cloud-spanner-emulator/emulator`, gRPC port 9010) with `emulator_host: localhost:9010`. Credentials are ignored against the emulator. The crate's integration tests bootstrap instance + database programmatically through the admin API.


## Auto-create (`create_table`)

`create_table` (**default `true`**, #580) creates the target table from the
first written page's inferred columns when it does not exist — a first-ever
sync cannot assume the destination is already there. Every inferred column is
created **nullable**: a column present in page 1 is not required forever, and a
`NOT NULL` inferred from one page fails page 2 the first time a record omits
the field (narrowing later is the `schema:` drift policy's job). Spanner requires a primary key, so auto-create needs `key:`; without one a missing table errors naming that requirement rather than inventing a key column.

Set `create_table: false` to require a pre-existing target; a missing one then
fails fast with the same error every table sink raises, naming both ways out.

## License

MIT OR Apache-2.0

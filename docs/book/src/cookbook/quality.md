# Data-quality checks

Add a `quality:` block under `pipeline:` to assert invariants on every page of
records as they flow through the pipeline. The quality pass runs **after**
transforms and **before** the sink write:

1. **Per-record checks** partition the page into survivors and quarantined rows
   (first-failure-wins per record).
2. **Per-batch checks** run over the survivors.
3. Quarantined rows are routed to the DLQ sink; survivors flow to the main sink.
4. The page bookmark advances only after the sink confirms — an `abort` never
   commits partial progress.

Quality checks require the `quality` Cargo feature (included in `full` and in
`faucet-cli`'s default build). The `json_schema` check additionally requires
`quality-jsonschema`.

> Quality checks are *ad-hoc rules*. For a first-class, **versioned promise**
> about the dataset's whole output shape — enforced at runtime and exportable
> as JSON Schema / an OpenLineage facet — see
> [Data contracts](./contracts.md).

## Full example

The following config fetches users from a REST API, normalises keys to snake_case,
and enforces several quality invariants before writing survivors to PostgreSQL.
Quarantined rows land in a local JSONL file.

```yaml
# rest_to_postgres_with_quality.yaml
version: 1
name: users_api_to_postgres_with_quality

pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com/v1
      path: /users
      method: GET
      auth:
        type: bearer
        config:
          token: ${env:API_TOKEN}
      query_params:
        per_page: "100"
      pagination:
        type: Cursor
        next_token_path: $.meta.next_cursor
        param_name: cursor
      max_retries: 3
      retry_backoff: 2
      tolerated_http_errors: []
      replication_method:
        type: Incremental
      replication_key: updated_at
      primary_keys: ["id"]
      requests: []
      schema_sample_size: 100
      state_key: users_api:users

  transforms:
    - type: keys_case
      config: { mode: snake }

  quality:
    record:
      - type: not_null
        field: id
        on_failure: abort             # abort: a null id is a catastrophic upstream bug
      - type: not_null
        field: email
        on_failure: quarantine        # quarantine: route bad rows to the DLQ
      - type: regex_match
        field: email
        pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
        on_failure: quarantine
      - type: value_in_set
        field: status
        values: ["active", "inactive", "pending", "suspended"]
        on_failure: quarantine
      - type: compare
        field: age
        op: gte
        value: 0
        on_failure: quarantine
    batch:
      - type: row_count
        min: 1
        on_failure: abort             # empty pages indicate a misconfigured source
      - type: unique
        fields: [id]
        on_failure: quarantine        # route duplicate ids to the DLQ

  dlq:
    sink:
      type: jsonl
      config:
        path: ./dlq/users_quality_failures.jsonl
    on_batch_error: propagate
    max_failures_per_page: 50
    max_failures_total: 500

  sink:
    type: postgres
    config:
      connection_url: ${env:PG_URL}
      table_name: users
      column_mapping:
        type: jsonb
        column: data
      batch_size: 500
      max_connections: 5

  state:
    type: file
    config:
      path: ./.faucet-state
```

## Check catalog

### Per-record checks

Evaluated in declared order; **first failure wins** for a given record.
`on_failure` may be `quarantine` (route the row to the DLQ) or `abort` (raise
`FaucetError::QualityFailure` and stop the run immediately).

| Check | Key fields | Passes when | Missing field |
|---|---|---|---|
| `not_null` | `field`, `treat_missing_as_null` (default `true`) | value present and non-null | fail (pass iff `treat_missing_as_null: false`) |
| `not_empty` | `field` | value is a non-empty string after trimming whitespace | fail |
| `regex_match` | `field`, `pattern` | value is a string matching `pattern` | fail |
| `value_in_set` | `field`, `values: [...]` | value is in the allowed set (exact JSON equality) | fail |
| `not_in_set` | `field`, `values: [...]` | value is NOT in the forbidden set | pass (trivially not in set) |
| `compare` | `field`, `op`, `value` | ordering or equality holds (see below) | fail |
| `type_is` | `field`, `expected` | JSON type of the value matches `expected` | fail |
| `string_length` | `field`, `min?`, `max?` | char count in `[min, max]` (at least one bound required) | fail |
| `json_schema` | `schema` | whole record validates against a JSON Schema document | (whole-record check) |

**`compare` operators:** `gt`, `gte`, `lt`, `lte` require both the field value and
the configured `value` to be JSON numbers; integer operands compare exactly (no
`f64` rounding above 2^53). `eq` and `ne` compare two numbers by **numeric
value** (so `1` and `1.0` are equal, and large 64-bit integers compare exactly),
and all other types by exact structural equality — there is no cross-type
coercion, so a string `"5"` never equals a number `5`.

**`json_schema`** requires the `quality-jsonschema` Cargo feature. It is the most
expressive check; its cost scales with schema complexity — for very large or deeply
nested schemas on hot paths, prefer the granular checks above and benchmark your
case.

### Per-batch checks

Evaluated per page over the **survivors** (records that passed all per-record
checks). Aggregate checks (`row_count`, `null_rate`, `distinct_count`) are not
row-attributable, so they offer `quarantine_batch` (route all survivors to the DLQ,
write nothing this page) or `abort`. `unique` is row-attributable and accepts
`quarantine` (route the duplicate rows) or `abort`.

| Check | Key fields | Passes when |
|---|---|---|
| `row_count` | `min?`, `max?` (at least one required) | survivor count in `[min, max]` |
| `null_rate` | `field`, `max` (0.0–1.0) | null-or-missing rate ≤ `max`; zero survivors → 0.0 → pass |
| `unique` | `fields: [...]` (composite key) | every survivor's composite key is unique within the page |
| `distinct_count` | `field`, `min?`, `max?` | distinct values of `field` in `[min, max]` |

## Failure policies

| Policy | Meaning | Allowed on |
|---|---|---|
| `quarantine` | Route the specific offending row(s) to the DLQ; keep the rest as survivors | per-record checks; `unique` |
| `quarantine_batch` | Route all current survivors of the page to the DLQ; nothing written this page | aggregate batch checks (`row_count`, `null_rate`, `distinct_count`) |
| `abort` | Raise `FaucetError::QualityFailure` and stop the run | every check |

## DLQ requirement

Any check that uses `quarantine` or `quarantine_batch` requires a `dlq:` block.
Omitting it fails validation with an error explaining that a `dlq:` block is
required (`faucet validate` catches this before the run starts; the core
guards it again at run start).

See the [Dead-letter queues](./dlq.md) cookbook page for `dlq:` options.

## Observability

The quality pass emits `faucet_quality_*` metrics automatically:

- `faucet_quality_checks_total{pipeline,row,check,outcome=pass|fail}`
- `faucet_quality_records_quarantined_total{pipeline,row,check,field}`
- `faucet_quality_aborts_total{pipeline,row,check}`
- `faucet_quality_check_duration_seconds{check}`

These are available alongside the standard `faucet_source_*`, `faucet_sink_*`, and
`faucet_transform_*` metrics. See [Observability](../operations/observability.md)
for the full metrics reference.

## Validate the config

```bash
faucet validate rest_to_postgres_with_quality.yaml
# ok: 'users_api_to_postgres_with_quality' rows=1 (roots=1, children=0)
```

`faucet schema quality` prints the full JSON Schema for the `quality:` block, and
`faucet list` shows all available checks with descriptions.

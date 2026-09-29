# Data contracts

A **data contract** is a declarative, versioned promise about a pipeline's
*output*: which fields exist, their types, whether they may be null, which
values are allowed, and what patterns/bounds they must satisfy. Producers and
consumers agree on the contract; faucet enforces it at runtime.

Contracts complement the other governance layers:

- [Quality checks](./quality.md) validate records against *ad-hoc rules*
  (per-record and per-batch). A contract is a stronger, first-class, versioned
  promise about the dataset's whole shape.
- [Schema drift](./schema-drift.md) decides *how the destination table
  evolves* when the shape changes. A contract decides *what is allowed to
  change at all*.

## Declaring a contract

The `contract:` block is pipeline-level (a sibling of `source` / `sink` /
`transforms` inside `pipeline:`; no matrix-row override in v1):

```yaml
version: 1
name: orders

pipeline:
  source: { type: file, config: { path: ./orders.csv } }

  contract:
    version: "1.0.0"                  # required, non-empty
    description: Orders exported for the analytics team.
    owner: data-platform
    on_breach: quarantine             # fail (default) | quarantine | warn
    allow_extra_fields: true          # default true
    fields:
      - name: order_id
        type: string                  # string | integer | number | boolean | object | array
        min_length: 1
      - name: status
        type: string
        enum: [open, shipped, cancelled]
      - name: amount
        type: number
        min: 0
      - name: customer_email
        type: string
        pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
        required: false               # default true
        nullable: true                # default false

  dlq:
    sink: { type: file, config: { path: ./dlq/contract_breaches.jsonl } }

  sink: { type: file, config: { path: ./orders_out.jsonl } }
```

Runnable example: `cli/examples/csv_to_jsonl_with_contract.yaml`.

### Field rules

| Field | Default | Purpose |
|-------|---------|---------|
| `name` | — | Top-level field name. Contracts describe the output's **top-level** shape; a nested object is typed as one `object` column (matching the schema-drift convention). |
| `type` | — | `string`, `integer` (a JSON number with no fractional part), `number` (any JSON number), `boolean`, `object`, `array`. |
| `required` | `true` | The field must be present in every record. An absent *optional* field skips all other checks. |
| `nullable` | `false` | An explicit JSON `null` is allowed (and skips the value checks). |
| `enum` | — | Allowed values (exact JSON equality). Values must match the declared `type`; use `nullable` for null (null inside `enum` is rejected). |
| `pattern` | — | Regex the value must match. String fields only. |
| `min` / `max` | — | Inclusive numeric bounds. Integer/number fields only. |
| `min_length` / `max_length` | — | Inclusive string length bounds (characters). String fields only. |
| `description` | — | Documentation; carried into every export format. |

Contract-level: `version` (required), `description`, `owner`, `on_breach`,
and `allow_extra_fields` (when `false`, an undeclared top-level key is a
breach).

Per record, the **first breach wins** — fields are checked in declared order
(presence → null → type → enum → pattern → range → length), then the
extra-field check. Each breach carries a stable `rule` label: `missing`,
`null`, `type`, `enum`, `pattern`, `range`, `length`, `extra_field`,
`not_object`.

## Enforcement policies (`on_breach`)

The pass runs per page **after** transforms and quality checks and **before**
the sink write (and before the schema-drift pass):

- **`fail`** (default) — the run aborts with a typed
  `ContractViolation` error on the first breach. Nothing from the breaching
  page is written: a contract must never commit breaching data.
- **`quarantine`** — breaching records are routed to the
  [DLQ](./dlq.md) wrapped in the standard envelope (`error.kind:
  "ContractViolation"`, the message names the field, rule, and contract
  version); conforming records are written. Requires a `dlq:` block —
  validated at config-load time. DLQ failure budgets
  (`max_failures_per_page` / `max_failures_total`) count contract breaches
  alongside quality quarantines and sink-side row failures.
- **`warn`** — breaches are logged (once per run) and counted in metrics, but
  every record is written unchanged. Use this to trial a contract against
  live traffic before turning on enforcement.

A malformed contract — empty version, duplicate/empty field names, an invalid
regex, an empty or type-mismatched `enum`, constraints on the wrong type,
`min > max` — is rejected at config-load time (`faucet validate` catches it),
never mid-run.

**Effectively-once:** `fail` and `warn` compose with `delivery: exactly_once`;
`quarantine` does not (effectively-once forbids a DLQ).

## Validating, printing, and publishing (`faucet contract`)

```console
$ faucet contract pipeline.yaml
contract v1.0.0 — valid (4 fields)
  owner: data-platform
  on_breach: quarantine
  allow_extra_fields: true
  fields:
    - order_id: string (length)
    - status: string (enum[3])
    ...
```

`--export` emits a machine-readable artifact for downstream consumers:

| Format | Output |
|--------|--------|
| `--export contract` | The canonical contract document as JSON. |
| `--export json-schema` | A standalone JSON Schema (draft 2020-12): `required` from the required fields, `additionalProperties` from `allow_extra_fields`, `nullable` widening `type` to `[..., "null"]`, and the contract version as `x-faucet-contract-version`. |
| `--export openlineage` | An OpenLineage `SchemaDatasetFacet` document — the same facet shape `faucet-lineage` emits, so OpenLineage consumers can ingest the contract as a schema promise. |

`faucet schema contract` prints the JSON Schema of the `contract:` block
itself (for editor autocompletion / config linting).

### Versioning

The `version` string travels into every breach error, DLQ envelope, and
export, so consumers can pin the exact promise they built against.
Recommendation: treat it like semver — bump the **major** version for
breaking changes (removing a field, narrowing a type, tightening a
constraint) and the **minor** version for additive ones (a new optional
field). Enforcement is always against the version in the running config; a
central contract registry is out of scope for v1.

## Observability

- `faucet_contract_violations_total{pipeline,row,field,rule,mode}` — one
  increment per breach under `warn` / `quarantine`.
- `faucet_contract_aborts_total{pipeline,row}` — a `fail`-policy abort.
- The pass runs inside a `faucet.contract.apply` tracing span carrying the
  contract version.
- Quarantined pages surface through the standard DLQ metrics
  (`faucet_sink_dlq_records_total`, …).

## Library usage

```rust,ignore
use faucet_core::{CompiledContract, ContractSpec, Pipeline};
use std::sync::Arc;

let spec: ContractSpec = serde_yaml::from_str(yaml)?;   // or serde_json
let compiled = Arc::new(CompiledContract::compile(&spec)?);

let result = Pipeline::new(&source, &sink)
    .with_contract(compiled)        // requires the `contract` feature
    .run()
    .await?;
```

Exports are plain functions: `faucet_core::contract::to_json_schema(&spec)`
and `to_openlineage_facet(&spec, producer)`.

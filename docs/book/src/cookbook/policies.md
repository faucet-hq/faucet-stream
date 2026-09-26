# Data-flow policies

A **data-flow policy** answers "may this column go there?" before any data
moves. It labels columns (`classifications`) and constrains where a label may
flow (`rules`): a customer email must reach only a sink that declares
`residency: eu`, a salary may leave the region only hashed, a finance column
must never land in a plain file in production. The verdict is decided
statically by `faucet validate` / `policy` / `plan` / `doctor`, refuses
`faucet run` and the `faucet serve` submit path, and is backed at run time by
value detectors on the real records.

> Masking ([cookbook](./masking.md)) says *how* to hide a value inside one
> pipeline. A policy says *whether* a movement is allowed, and is applied
> centrally to configs it does not own (`--policy`, `serve --policy`).

## A policy

```yaml
# cli/examples/policy/eu-pii.yaml
version: 1
description: PII stays in the EU and never lands in plain files in production.
classifications:
  - label: pii
    fields: [email, phone, ssn, date_of_birth]     # full dot-path or leaf key
    field_pattern: "(?i)(^|[._])(first|last|full)_?name$"   # regex over the dot-path
    value_detector: email                          # email|credit_card|ssn|phone|ipv4
  - label: pii
    value_detector: ssn
  - label: finance
    field_pattern: "^(amount|balance|salary)"
rules:
  - name: pii-eu
    when: { label: pii }
    require: { residency: [eu] }        # the sink must declare one of these
    mask: [hash, tokenize, redact]      # or the column reaches it masked
  - name: finance-no-prod-files
    when:
      label: finance
      sink_kind: [jsonl, csv, stdout]
      sink: { environment: [prod] }     # only sinks that declare this
    deny: true
    on_runtime: quarantine              # fail (default) | quarantine
```

- A **classification** labels a column by name (`fields` matches the full
  dot-path or its leaf key — `ssn` covers `user.ssn`; `field_pattern` is a
  regex over the dot-path) and/or by **value** (`value_detector`, the
  [masking detectors](./masking.md)). Name matches are decided before any
  data moves; value detectors are enforced by the runtime backstop.
- A **rule** applies to every column carrying `when.label`, optionally only
  for some `sink_kind`s and only for sinks whose `when.sink` attributes have
  a listed value (a sink that declares no `environment` never matches a rule
  scoped to `environment: prod`). It is satisfied when every `require`
  attribute is present with a listed value, or the column reaches the sink
  masked with a listed `mask` action. `deny: true` is never satisfied.
- `on_runtime` is what the backstop does when a value detector fires on a
  record heading for a non-compliant sink: `fail` the page, or `quarantine`
  the row to the DLQ (a `dlq:` block is required).

`faucet schema policy` prints the JSON Schema.

## Sink attributes

Rules reason about free-form string pairs on the **sink**:

```yaml
pipeline:
  sinks:
    eu_warehouse:
      type: postgres
      attributes: { residency: eu, environment: prod }
      config: { … }
matrix:
  - id: customers
    sink: { ref: eu_warehouse, attributes: { environment: staging } }   # adds / overrides
```

A sink template's attributes apply to every row that resolves to it; a
matrix row's `sink.attributes` adds or overrides keys.

## Applying a policy

```bash
faucet policy   pipeline.yaml --policy eu-pii.yaml   # the per-row report; exit = violations
faucet validate pipeline.yaml --policy eu-pii.yaml   # invalid on any violation
faucet plan     pipeline.yaml --policy eu-pii.yaml   # reports, never fails (a preview)
faucet doctor   pipeline.yaml --policy eu-pii.yaml   # a `policy` probe per root row
faucet run      pipeline.yaml --policy eu-pii.yaml   # refused before any connector is built
faucet serve --policy eu-pii.yaml                    # every submission
```

The policy can also live in the config as a top-level `policy:` block (a
[deployment overlay](./template-hub.md) may carry one), and `--policy` merges
on top: classifications and rules concatenate; two rules with one name are a
load error.

## The report

```text
$ faucet policy pipeline.yaml --policy eu-pii.yaml
policy: 2 rule(s), 3 classification(s), value detectors enforced at run time — 1 violation(s)
  row customers → sink files (jsonl; environment=prod): 3 column(s) from the contract; labelled: email[pii] amount[finance]
    ! rule `pii-eu`: column `email` (pii) → sink `files` (jsonl) missing attribute `residency` (one of eu)
```

Per row the report says **what the static pass knows**:

- **Columns** come from the row's data contract ([contracts](./contracts.md)),
  or from a schema the caller supplies — `plan --sample` runs the sample and
  labels its columns; the serve `POST /v1/plan` does the same. A row with
  neither has *no static knowledge*: the report says so and the runtime
  backstop is the enforcement.
- **Labels follow the transform chain** through the same column-lineage ops
  OpenLineage emission uses, so `rename_field: { email: contact }` yields a
  labelled `contact` (`via: lineage`). An **opaque** transform (`flatten`,
  `explode`, `keys_case`, `sql`, `wasm`, custom) carries every labelled input
  **conservatively**: a rename cannot hide a label, and the report flags the
  row as opaque.
- A column the row's masking policy provably rewrites at that sink
  (name-matched rules, honouring `applies_to`) counts as **masked**, so
  `mask: [hash]` is satisfied by a `hash` masking rule on that field.

Topology graphs (`pipeline.nodes`) are evaluated per sink node from the
pipeline-level contract; any transform node in the graph makes the labels
conservative.

## The runtime backstop

Whatever the static pass could not see — a column no contract declared, a
value that only looks like PII — the **policy sink** catches. It is the
outermost sink decorator (after masking, so a masked value no longer trips
its detector): every record's scalar leaves are classified by name and by
value detector and the same rules are evaluated against the sink's
attributes.

- `on_runtime: fail` — the page is refused before it is written;
  `faucet run` fails with `Policy `pii-eu` violated on column `mail`` and
  `faucet serve` audits `policy.denied` by principal `runtime`.
- `on_runtime: quarantine` — the offending rows go to the [DLQ](./dlq.md)
  with the rule in the envelope; the rest of the page is written.

`faucet_policy_violations_total{pipeline,row,rule,phase,action}` counts both
phases (`phase` = `static` | `runtime`, `action` = `refuse` | `fail` |
`quarantine`).

## Under `faucet serve`

`faucet serve --policy FILE` checks every submission (`POST /v1/runs`, a
template trigger, a backfill, a trigger fire) against the policy merged with
the config's own block: a violation is a `422` whose `details` carry the
report, and a `policy.denied` audit entry. Registering a violating
**pipeline template** warns (`warnings[]` on the summary) rather than
refusing — a source template may still be composed with a compliant sink —
and triggering it hits the submit gate. `POST /v1/plan` reports the verdict
under `policy` (viewer-readable).

## Worked example

[`cli/examples/csv_to_jsonl_with_policy.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/csv_to_jsonl_with_policy.yaml)
is compliant: its sink declares `residency: eu` and the salary column is
hashed. Remove the attribute and `faucet validate` exits 1 naming `pii-eu`;
remove the masking rule and `finance-hashed-or-nothing` fires.

## Reference

- [`policy`](../reference/config.md#policy) and
  [`attributes`](../reference/config.md#pipeline) in the config reference
- [`faucet policy`](../reference/cli.md#policy), the `--policy` flag on
  `run` / `validate` / `plan` / `doctor`, `serve --policy`
- [`POST /v1/plan`](../reference/http-api.md#post-v1plan) and the `422`
  submit refusal
- [RFC 0010](https://github.com/faucet-hq/faucet-stream/blob/main/rfcs/0010-policies-and-impact.md)

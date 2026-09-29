# PII detection & column masking

A **masking policy** classifies sensitive fields — by field-name pattern, by a
value detector (email / credit card / SSN / phone / IPv4), or by an explicit
field list — and rewrites them in place before the data leaves the pipeline.
It is the built-in defence against personally identifiable information (PII)
reaching a destination it should not.

Masking complements the other governance layers:

- The [`redact` transform](./transforms.md#redact) nulls or masks a *named,
  top-level* field you already know about. Masking is a stronger,
  policy-driven layer: it can *detect* PII by value (whatever the column is
  called), reach into nested paths, hash/tokenize for joinable pseudonyms, and
  scope rules per destination sink.
- [Data-quality checks](./quality.md) and [contracts](./contracts.md) *validate*
  records; masking *rewrites* them. The masking pass runs first, so those
  checks see masked values.

## The ordering guarantee

The masking pass runs **first** — before the quality, contract, and
schema-drift passes and before every sink write, the [DLQ](./dlq.md), and
lineage sampling. This is the headline guarantee: **PII never reaches any sink
(including the DLQ) or an OpenLineage facet unmasked.** Because masking runs
ahead of quality and contract enforcement, those passes evaluate the *masked*
values, not the raw ones.

Masking is value-only and key-preserving: matching fields are rewritten in
place. It **never fails a run and never quarantines** — so, unlike quarantining
quality/contract policies, masking does not require a `dlq:` block.

## Declaring a masking policy

The `masking:` block is pipeline-level (a sibling of `source` / `sink` /
`transforms` inside `pipeline:`). This is the runnable example
`cli/examples/csv_to_jsonl_with_masking.yaml`:

```yaml
version: 1
name: customers_csv_with_masking

pipeline:
  source:
    type: file
    config:
      path: ./customers.csv

  masking:
    description: Mask customer PII before it lands anywhere.
    key: change-me-pull-from-a-secrets-manager
    rules:
      # Redact anything that looks like an email address, whatever the column.
      - name: emails
        match:
          value_detector: email
        action:
          type: redact

      # Hash the SSN (keyed, deterministic → still joinable).
      - name: ssn
        match:
          field_pattern: '(?i)^ssn$|social'
        action:
          type: hash

      # Show only the last 4 digits of any card number.
      - name: cards
        match:
          value_detector: credit_card
        action:
          type: partial
          keep_last: 4

      # Tokenize the user id with a stable prefix.
      - name: user-id
        match:
          fields: [user_id]
        action:
          type: tokenize
          prefix: usr_

  sink:
    type: file
    config:
      path: ./customers_masked.jsonl
```

`rules` is required and non-empty. Rules are evaluated in declared order and
**the first matching rule wins for a given field** — so put your most specific
rules first.

### How a rule matches (`match`)

A `match` block must set **at least one** of the three criteria; a field
matches the rule if *any* configured criterion matches:

| Criterion | Matches |
|-----------|---------|
| `field_pattern` | A regex over the field's **dot-path** (e.g. `user.email`, `contacts.0.phone`). Case-sensitive unless the pattern opts in with `(?i)`. Cheap and precise when you know your field names. |
| `value_detector` | A [built-in detector](#detectors) run over each *string* value — catches PII whatever the column is called. |
| `fields` | Explicit dot-paths masked unconditionally — the tagging / escape hatch. A name-based match on a container (e.g. `fields: [address]`) masks the whole subtree. |

**Nested paths.** Rules match dot-paths like `user.email` or
`contacts.0.email`. A `field_pattern` or `fields` entry that names a container
(an object or array) rewrites the entire subtree — see the
`fields: [address]` case in `masking_tests.yaml`, which redacts the whole
`address` object.

### Actions

The `action` block is tagged by `type`:

| `type` | Behavior | Options |
|--------|----------|---------|
| `redact` | Replace the value wholesale with a fixed `mask`. Irreversible, not joinable. | `mask` — any JSON value; default `"***"`. Set `mask: null` to null the field (e.g. for a nullable DB column). |
| `hash` | Replace with a hex digest — HMAC-SHA256 when a `key` is set, plain SHA-256 otherwise. Deterministic → joinable; irreversible. | — |
| `tokenize` | Replace with a short opaque token derived from the keyed digest. Deterministic → joinable. | `prefix` — optional literal prepended to every token (e.g. `usr_`); when set it must be non-empty. |
| `partial` | Reveal only the last `keep_last` characters, masking the rest. Preserves format/length for readability (e.g. `****1234`). | `keep_last` — trailing chars kept (default `4`); if `keep_last >= len` the whole value is masked, so a short value never leaks whole. `mask_char` — masking character (default `*`). |

### Detectors

All detectors are **conservative** — fully anchored full-string regexes — so
false positives stay rare. This matters because masking silently rewrites
data: a false positive is a data-quality bug, not just noise.

| `value_detector` | Matches |
|------------------|---------|
| `email` | An RFC-5322-ish email address. |
| `credit_card` | A 13–19 digit card number (spaces/dashes allowed) that passes the **Luhn** checksum. |
| `ssn` | A US SSN `NNN-NN-NNNN`, excluding never-issued ranges (000/666/9xx area, 00 group, 0000 serial). |
| `phone` | An E.164 / North-American phone number. |
| `ipv4` | An IPv4 dotted-quad address. |

## Determinism & joinability

`hash` and `tokenize` are **deterministic** — equal input always produces equal
output. Two pipelines that share the same `key` therefore produce the same
pseudonym for the same value, so masked columns stay joinable across datasets.
This is exactly the property the `keyed hash is deterministic` case in
`masking_tests.yaml` asserts: two records with `uid: "u1"` collapse to the same
hash.

## Keyed vs unkeyed, and secrets

The `key` field controls the strength of `hash` / `tokenize`:

- **Keyed** (`key` set) — HMAC-SHA256. Irreversible without the key, so it is a
  proper pseudonymization boundary while staying deterministic.
- **Unkeyed** (`key` absent) — plain SHA-256. Still deterministic, but *not*
  secret: anyone can recompute the digest from the raw value. Use it for
  stable IDs where secrecy is not the goal, not for protecting PII.

Because the masking pass runs after secret resolution, pull the key from a
[secrets manager](./secrets.md) in production rather than hard-coding it:

```yaml
  masking:
    key: ${vault:secret/faucet#masking_key}   # or ${aws-sm:...}, ${gcp-sm:...}, ${azure-kv:...}
    rules:
      ...
```

## Destination scoping (`applies_to`)

`applies_to` scopes a rule to specific sinks — matched by the sink **template
name** (declared under `pipeline.sinks:`) or by the connector **kind** (e.g.
`bigquery`). An empty or absent `applies_to` applies the rule to every sink.
This lets the same source be fully masked to one destination and only partially
masked to another:

```yaml
pipeline:
  sinks:
    warehouse: { type: bigquery, config: { ... } }   # analytics — hashed IDs kept joinable
    lake:      { type: s3,       config: { ... } }    # cold storage — everything redacted

  masking:
    key: ${vault:secret/faucet#masking_key}
    rules:
      # Redact emails everywhere.
      - match: { value_detector: email }
        action: { type: redact }
      # Keep a joinable hashed user id only in the warehouse.
      - match: { fields: [user_id] }
        action: { type: hash }
        applies_to: [warehouse]      # template name — or "bigquery" for the kind
```

## Inspecting a policy (`faucet masking`)

`faucet masking [config]` validates the `masking:` block and prints, per
destination sink, which rules apply — the fast way to confirm your
`applies_to` scoping is right. It is offline-safe (no secrets are fetched):

```console
$ faucet masking cli/examples/csv_to_jsonl_with_masking.yaml
masking — valid (4 rules)
  description: Mask customer PII before it lands anywhere.
  key: configured (keyed HMAC-SHA256 for hash/tokenize)
  rules:
    - emails: detector email → redact (all sinks)
    - ssn: field_pattern /(?i)^ssn$|social/ → hash (all sinks)
    - cards: detector credit_card → partial (keep_last 4) (all sinks)
    - user-id: fields[user_id] → tokenize (prefix 'usr_') (all sinks)
  destinations:
    - default [jsonl]: emails, ssn, cards, user-id
```

`faucet schema masking` prints the JSON Schema of the `masking:` block itself
(for editor autocompletion / config linting).

## Testing offline (`faucet test`)

Because masking is a pure per-page rewrite, you can assert its behavior with
fixture records and no source or sink — see
`cli/examples/tests/masking_tests.yaml`:

```console
$ faucet test cli/examples/tests/masking_tests.yaml
```

Fixture records stream through the real masking → transform → quality →
contract path with an in-memory sink. Offline there is no destination sink, so
**every rule applies regardless of its `applies_to` scoping**. The example spec
covers value detectors (email + Luhn-valid card), keyed-hash determinism, and
name-pattern + explicit-field + nested-path masking. See the
[Testing pipelines](./testing.md) cookbook page for the spec grammar.

## Observability

- `faucet_masking_fields_total{pipeline,row,rule,action,detector}` — one
  increment per masked field. `rule` is the rule's `name` (or the generated
  `rule_<n>`), `action` is `redact` / `hash` / `tokenize` / `partial`, and
  `detector` is the detector name for a value-based match or empty for a
  name-based match.

## Library usage

```rust,ignore
use faucet_core::masking::{CompiledMasking, MaskingSpec};
use faucet_core::Pipeline;
use std::sync::Arc;

let spec: MaskingSpec = serde_yaml::from_str(yaml)?;   // or serde_json
let compiled = Arc::new(CompiledMasking::compile(&spec)?);   // requires the `masking` feature

// or scope to one destination sink by its template name / connector kind:
let scoped = CompiledMasking::compile_for_sink(&spec, &["warehouse", "bigquery"])?;
```

The `masking` Cargo feature is in the CLI default build (and the umbrella
`masking` feature and `full`).

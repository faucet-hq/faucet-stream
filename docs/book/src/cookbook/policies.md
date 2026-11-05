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
    value_detector: email                          # email|credit_card|ssn|phone|phone_loose|ipv4
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
  labelled `contact` (`via: lineage`). A contract describes the row's
  **output** (it is enforced after the transforms), so it lists `contact`;
  each contract field is traced back through the chain to the input it came
  from and keeps that input's labels. An **opaque** transform (`flatten`,
  `explode`, `keys_case`, `sql`, `wasm`, custom) carries every labelled input
  **conservatively**: a rename cannot hide a label, and the report flags the
  row as opaque.
- A column the row's masking policy provably rewrites at that sink
  (name-matched rules, honouring `applies_to`) counts as **masked**, so
  `mask: [hash]` is satisfied by a `hash` masking rule on that field. The
  match is the masking pass's own: `fields` names the full dot-path, and a
  rule on a container (`fields: [phones]`) masks every leaf beneath it.

Topology graphs (`pipeline.nodes`) are evaluated per sink node from the
pipeline-level contract; any transform node in the graph makes the labels
conservative, and a contract field inherits the labels of every name any
transform node renames into it.

## The runtime backstop

Whatever the static pass could not see — a column no contract declared, a
value that only looks like PII — the **policy sink** catches. It is the
outermost sink decorator (after masking, so a masked value no longer trips
its detector): every record's scalar leaves are classified by name and by
value detector and the same rules are evaluated against the sink's
attributes. A label on a container (`fields: [phones]`) covers every leaf
beneath it (`phones.0`, `phones.1`), and a leaf the masking pass rewrote by
name counts as masked with that action, exactly as in the static pass. The
sink sees records after the transforms, so a column a row's transforms
renamed (`ssn` → `tax_id`) carries the labels of its original name there too.

- `on_runtime: fail` — the page is refused before it is written;
  `faucet run` fails with `Policy `pii-eu` violated on column `mail`` and
  `faucet serve` audits `policy.denied` by principal `runtime`.
- `on_runtime: quarantine` — the offending rows go to the [DLQ](./dlq.md)
  with the rule in the envelope; the rest of the page is written. The DLQ
  receives the whole record, so it is evaluated as a destination too (sink
  id `dlq`, with the `attributes:` of `dlq.sink`): when the rules would not
  let the record reach the DLQ either, the run fails instead of moving it
  there. Every labelled column of the record is checked at the DLQ, not only
  the one that broke the main sink's rule. `faucet validate` / `faucet policy`
  report those violations against sink `dlq`.
- **Every row that reaches the DLQ is checked there**, whichever pass sent it:
  a quality, contract or schema-drift quarantine, a per-row sink failure, or a
  policy quarantine. The DLQ is a governed destination of its own, so a row
  whose labels may not reach the DLQ's `attributes` fails the run instead of
  landing there. The static pass evaluates sink `dlq` whenever a `dlq:` block
  is configured, with the masking that reaches it — a masking rule whose
  `applies_to` names `dlq` (or the DLQ's connector kind) masks the envelope
  payloads and satisfies a `mask:` rule there.
- A `quarantine` rule makes the run take the row path, so the same input
  quarantines the same rows whichever fast path (columnar, native) the
  connectors would otherwise take. It cannot be combined with
  `delivery: exactly_once` on the atomic-watermark path (that commits whole
  pages and forbids a DLQ): `faucet validate` refuses the pair by name and
  suggests `on_runtime: fail` or keyed-upsert effectively-once. Only a
  quarantine rule that can reach the row's sink counts — one whose
  `when.sink_kind` / `when.sink` attributes exclude that sink does not block
  an exactly-once row.

```yaml
pipeline:
  dlq:
    sink:
      type: jsonl
      config: { path: /eu-bucket/quarantine.jsonl }
      attributes: { residency: eu }
```

`faucet_policy_violations_total{pipeline,row,rule,phase,action}` counts both
phases (`phase` = `static` | `runtime`, `action` = `refuse` | `fail` |
`quarantine`).

## Under `faucet serve`

A server checks every submission (`POST /v1/runs`, a template trigger, a
backfill, a trigger fire) against its policy merged with the config's own
block: a violation is a `422` whose `details` carry the report, and a
`policy.denied` audit entry naming the rule, column and sink. Registering a
violating **pipeline template** warns (`warnings[]` on the summary) rather
than refusing — a source template may still be composed with a compliant sink
— and triggering it hits the submit gate. `POST /v1/plan` reports the verdict
under `policy`.

### Where the server policy comes from

| Started with | The policy is | Changed by |
|---|---|---|
| `--policy FILE` | the file, read once at start | editing the file and restarting. The console and API show it **read-only**; every change endpoint answers `409`. |
| no `--policy` | the newest **stored version**, kept in the run-history store (so a SQLite / Postgres history keeps it across restarts and shares it between replicas) | an approved change request — see below. With no version stored yet, nothing is enforced, as before. |

A server-managed policy is never edited in place. An admin proposes a new
version (`POST /v1/policy`, or **Policy → Edit policy** in the console); the
request is a `policy_update` change that **always** needs approval, by default
from an admin other than the requester (the `approvals:` block of
`--auth-config` can name other approvers for `policy_update`). On approval the
draft is checked again and stored as the next version only if the version it
was based on is still the one in force — a second change approved after the
first is `invalidated` instead of overwriting it. Every replica reads the
version in force at admission, so an approved change reaches all of them for
the next submission.

```bash
# Preview a draft: its problems (with lines), or its diff and impact
curl -s -X POST localhost:8899/v1/policy/preview -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' -d "$(jq -Rs '{policy: .}' eu-pii.yaml)"

# Propose it (base_version: the version you edited; null for the first)
curl -s -X POST localhost:8899/v1/policy -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' \
  -d "$(jq -Rs '{policy: ., reason: "Hash-only for email", base_version: 7}' eu-pii.yaml)"

# Another admin approves it
curl -s -X POST localhost:8899/v1/changes/$ID/approve -H "Authorization: Bearer $OTHER_ADMIN"
```

The **preview** checks the draft with the same validation as a submission and
measures its impact: every registered pipeline template (its stable version,
params bound to placeholders) and the stored configs of the last 7 days of runs
are checked against the policy in force and the draft, and the ones the draft
would newly refuse or newly allow are listed. A template whose columns are
unknown before a run (no `contract:`) is counted as checked at run time only —
the runtime backstop still applies to it. The approver sees the same diff and
impact on the change request.

Versions are append-only (`GET /v1/policy/versions`): each names who requested
it, who approved it and why. A **rollback** (`POST /v1/policy/rollback {version,
reason}`) is a change request too; approved, it stores a new version with the
earlier document (`restores: N`).

**Runs keep the version they were admitted under.** The stored versions a run
was checked against are recorded on its record (`policy_pin`), and a claim,
shard or resume on another replica enforces those versions rather than a newer
one.

### Refusals and the dry run

`GET /v1/policy/refusals` lists what the policy stopped — submissions the
static gate refused and runs the backstop failed or quarantined — with the
rule, column, label, sink and the layer (`server` or `tenant`) whose rule it
was. `POST /v1/policy/check` (operator) dry-runs a config, or a registered
pipeline template, against the policy in force without running anything; with
`sample` records for one row, their values label the columns too.

### Tenant policies

Each [tenant](./embedded-integrations.md) can add its own classifications and
rules on top of the server policy. A tenant policy is **add-only**: it is merged
with the server's (`server ⊕ tenant ⊕ pipeline`), so it can tighten what the
server allows but never remove, rename or loosen a server rule — a draft that
reuses a server rule's name, or tries to `disable` one, is refused. Its rules
may govern labels the server classifies. It applies to that tenant's runs only.

```yaml
# acme's policy: personal data never reaches a partner-tier sink
version: 1
rules:
  - name: acme-no-partner
    when: { label: pii }          # a server classification
    require: { tier: [internal] }
```

The tenant's own admin principals (`--auth-config` entries with `tenant:` and
`role: admin`) and server admins propose changes (`POST
/v1/tenants/{tenant}/policy`, preview and rollback alongside); the
`tenant_policy_update` request is approved by **another admin of that tenant or
a server admin**, never the requester. A tenant's viewers and operators read it;
another tenant's is a `404`. A tenant principal sees the server rules that
apply to it as outlines (name, label, what they require) — not the rest of the
server policy — and its own refusals. A server policy change that would reuse a
tenant rule's name is refused, so the layers can always be merged. Deleting the
tenant deletes its policy versions.

### In the console

**Govern → Policy** shows the policy in force (classifications, rules, and how
many columns the catalog has seen per label), its refusals, a dry run and the
version history, with **Edit policy** for admins on a server-managed policy. The
editor validates as you type, then previews the diff and impact and asks for a
reason before it creates the change request; pending changes show as a banner
until approved. With the tenant switcher set to a tenant, the page shows the
server rules beside that tenant's rules and history; a tenant principal sees its
own tenant's view. See [the web console](./web-console.md#policy).

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
- [The policy endpoints](../reference/http-api.md#policy) (`/v1/policy*`,
  `/v1/tenants/{tenant}/policy*`) and [change requests](./approvals.md)
- [RFC 0010](https://github.com/faucet-hq/faucet-stream/blob/main/rfcs/0010-policies-and-impact.md)

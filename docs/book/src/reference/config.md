# Configuration file format

A faucet config is a YAML or JSON document with this top-level shape:

```yaml
version: 1                 # required, must be 1
name: my_pipeline          # optional; used in state keys and metrics
vars: {}                   # optional; reusable values referenced as ${vars.X}
auth: {}                   # optional; named shared auth providers (see below)
schedule: {}               # optional; cron schedule for faucet schedule (see below)
pipeline:                  # required
  source: { type: …, config: { … } }
  transforms: []           # optional list
  sink:   { type: …, config: { … } }
  state:  { type: …, config: { … } }   # optional
  dlq:    { … }            # optional dead-letter queue
matrix: []                 # optional per-row overrides / DAG
execution:                 # optional
  max_concurrent: 4
  on_error: continue       # continue | stop
selection:                 # optional; row-selection policy (see Row selection)
  include_parents: off     # off | eligible | all
```

> **Unknown keys are rejected.** The structural blocks (`pipeline`, each
> `source`/`sink`/`transform`/`state` spec, `matrix` rows, `execution`) reject
> unrecognized fields, so a typo like `transorms:` or `parnet:` is a load-time
> error rather than a silently-ignored field. A connector's own `config: { … }`
> object is still passed through verbatim to that connector.

## `pipeline`

`source` and `sink` each take a `type` (the connector name) and a `config`
object whose fields are that connector's schema — see `faucet schema source
<name>`. `transforms` is an ordered list applied to every record. `state`
attaches a [state store](../cookbook/state.md); `dlq` attaches a
[dead-letter queue](../cookbook/dlq.md).

### Transforms layering

Transforms can be declared at three layers and are resolved additively per
matrix row in lifecycle order:

```
final = T_pipeline ++ T_source ++ T_row
```

- `pipeline.transforms` — cross-cutting policy, runs first on every row.
- `pipeline.sources.<name>.transforms` — bound to a source template; runs for
  every row that resolves to this source.
- `matrix[i].transforms` — row-specific extras, runs last.

Each declaring layer (source template, matrix row) carries an
`inherit_transforms: bool` (default `true`); setting it `false` drops every
upstream layer for that scope.

Sinks reject both `transforms:` and `inherit_transforms:` at expand time —
destination shaping belongs at the pipeline or row layer. See the
[transforms cookbook](../cookbook/transforms.md) for the full model and
worked examples.

### Available transforms

The full catalogue (with shapes and worked examples) lives in the
[transforms cookbook](../cookbook/transforms.md); `faucet list` prints the
same set, and `faucet schema transform <name>` returns the JSON schema for
each. Highlights:

- `filter` — keep records where a JSONPath predicate is true. See the cookbook for the operator set and path syntax.
- `explode` — expand an array field into one record per element. See the cookbook for the merge rule and `on_missing` semantics.

## Config composition

Three top-level mechanisms let a config be assembled from reusable pieces.
They are resolved when the file is read, **before** any `${...}` interpolation.

| Mechanism | Form | Effect |
|-----------|------|--------|
| `extends:` | `extends: ./base.yaml` or a list | Inherit one or more base files; the child deep-merges on top. |
| `profiles:` | `profiles: { dev: {…}, prod: {…} }` | Named overlays, selected at run time with `--profile NAME` / `FAUCET_PROFILE`. |
| `!include` | `key: !include ./frag.yaml` | Substitute a YAML fragment at any node (**YAML only**). |

```yaml
# app.yaml — inherits a base and pulls in a transform fragment.
extends: ./base.yaml          # single path, or a list (merged left-to-right)
pipeline:
  transforms: !include ./transforms.yaml
```

```yaml
# base.yaml — shared connection + sink, with named per-environment overlays.
version: 1
name: composed-pipeline
pipeline:
  source: { type: csv,   config: { path: ./data/input.csv } }
  sink:   { type: jsonl, config: { path: ./out/dev.jsonl } }
profiles:
  dev:  { pipeline: { sink: { config: { path: ./out/dev.jsonl } } } }
  prod: { pipeline: { sink: { config: { path: ./out/prod.jsonl } } } }
```

- **`extends`** — relative paths resolve against the directory of the file that
  declares them. A list of bases merges left-to-right; the child document
  overrides them all. Bases may themselves `extends:` further files (depth-capped,
  cycle-detected).
- **`profiles`** — nothing is applied unless a profile is selected. Select with
  `--profile prod` or `FAUCET_PROFILE=prod`; **the flag overrides the env var**.
  An undeclared name is a load-time error.
- **`!include`** — a YAML tag (no JSON equivalent) that replaces the tagged node
  with the parsed contents of another YAML file (sequence, mapping, or scalar).
  Paths resolve against the including file's directory.

**Merge rule and precedence.** Everything composes with the same deep-merge used
by `matrix` rows (objects merge recursively, arrays replace wholesale, scalars
replace). Lowest-to-highest priority (last wins):

```
extended base(s)  →  child document  →  selected profile  →  matrix row
```

**Load-time ordering.** Composition runs first, then interpolation:

1. **Composition** — `extends` / `!include` stitched, then the selected
   `profile` overlaid; the `extends:` / `profiles:` metadata keys are stripped.
2. `${env:…}` / `${file:…}` / `${secret:…}`, then `${vars.X}` and
   `${sources.X}` / `${sinks.X}` (see [Interpolation](#interpolation)).
3. Secrets-manager directives (`${vault:…}` etc.).
4. `matrix` expansion.

**Inspect the result** with `faucet validate --show-composed` — it prints the
fully composed document (bases merged, profile applied, fragments substituted,
metadata stripped) before interpolation.

> **Composition is file-loads-only.** `extends` / `profiles` / `!include` apply
> to configs faucet reads from disk (`run`, `validate`, `preview`, `doctor`,
> `schedule`). They are **not** honored for configs submitted to `faucet serve`
> over HTTP — a submitted body is a single self-contained document with no
> filesystem access. See the [config-composition cookbook](../cookbook/composition.md).

## Interpolation

Three stages resolve placeholders:

- **Load time:** `${env:VAR}`, `${file:PATH}`, `${secret:VAR}` are resolved when
  the file is read. `${vars.X}` resolves against the top-level `vars:` block;
  `${sources.NAME.PATH}` / `${sinks.NAME.PATH}` resolve against named templates.
  Secret-manager directives (see below) run as the final load-time stage.
- **Trigger time:** `${param.NAME}` resolves against the top-level [`params:`](#params)
  block, bound from `--param` / an HTTP `params` object before the config is
  parsed.
- **Runtime:** `${row_id.dotted.path}` tokens are resolved per parent record in
  DAG runs. `${now.*}` tokens are resolved per invocation at run time (see
  below).

Reference cycles surface as a clear `InterpolationCycle` error.

### `${now.*}` — run-clock interpolation

`${now.*}` tokens inject the current wall time into **source and sink config
values**. Each invocation evaluates them once at run time:

| Token | Example output | Notes |
|-------|---------------|-------|
| `${now.date}` | `2026-03-08` | `YYYY-MM-DD` |
| `${now.datetime}` | `2026-03-08T14:05:09+00:00` | RFC 3339; alias: `${now.iso}` |
| `${now.iso}` | `2026-03-08T14:05:09+00:00` | Alias for `${now.datetime}` |
| `${now.year}` | `2026` | Zero-padded 4-digit year |
| `${now.month}` | `03` | Zero-padded month (01–12) |
| `${now.day}` | `08` | Zero-padded day (01–31) |
| `${now.hour}` | `14` | Zero-padded hour (00–23) |
| `${now.minute}` | `05` | Zero-padded minute (00–59) |
| `${now.second}` | `09` | Zero-padded second (00–59) |
| `${now.unix}` | `1741442709` | Unix epoch seconds |
| `${now.strftime.<fmt>}` | `2026/03/08/14` | Arbitrary [chrono strftime](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) — e.g. `${now.strftime.%Y/%m/%d/%H}` |

An unknown token (e.g. `${now.foo}`) is a config error at run time. An invalid
strftime format produces a clean config error rather than a panic.

**Clock source:**

- **`faucet run`** — the process start time in UTC. Override with
  `--clock <value>` for backfills: an RFC 3339 timestamp
  (`2026-03-01T00:00:00Z`) or a bare date (`2026-03-01`, treated as midnight
  UTC). See the [`run` command reference](cli.md#run).
- **`faucet schedule`** — the tick's **scheduled time**, rendered in the
  schedule's `timezone`. `${now.date}` therefore reflects the date in the
  timezone the cron fires in (e.g. `America/Los_Angeles`), not UTC. Queued
  runs use their original scheduled time; `--once` uses the current wall clock.

**Scope:** `${now.*}` tokens (and `${row_id.path}` parent-record references) are
resolved only in source and sink config values. Using one in a `state:`,
`dlq:`, or `transforms:` config is a **config error at validate/expand time** —
it is rejected rather than silently passed to the connector as a literal
`${…}` string. (`${env:…}` / `${vars.X}` / `${sources.X}` still resolve
everywhere.)

**Reserved id:** `now` is a reserved matrix row id — a matrix row cannot be
named `now`.

**SQL caveat:** `${now.*}` substitutes as plain text into config values — the
same semantics as `${row_id.path}` tokens. For SQL sources that interpolate
`${now.*}` into a query string, prefer the connector's bind-parameter path
(`substitute_context_bind_params`) over raw text substitution to avoid
injection risk.

### Secrets-manager directives

Four additional load-time schemes pull values from external secrets managers.
Each requires the matching build feature (`--features secrets-vault`, etc.;
`--features secrets` enables all four). Values are fetched concurrently and
de-duplicated; they are never written to disk.

| Directive | Backend | Auth |
|-----------|---------|------|
| `${vault:<path>[#field]}` | HashiCorp Vault KV v2 | `VAULT_ADDR` + `VAULT_TOKEN` (+ optional `VAULT_NAMESPACE`) |
| `${aws-sm:<name-or-ARN>[#field]}` | AWS Secrets Manager | `aws-config` default chain (env / profile / instance / web-identity) |
| `${gcp-sm:projects/<p>/secrets/<s>/versions/<v>}` | GCP Secret Manager (`versions/latest` ok) | Application Default Credentials |
| `${azure-kv:<vault>/<secret>[/<version>]}` | Azure Key Vault | `AZURE_*` env / managed identity / `az login` |

The `#field` selector (Vault and AWS only) parses the secret body as a JSON
object and extracts a single key. Use `faucet schema secrets` for the machine-readable
grammar reference and `faucet validate --no-secrets` to check grammar offline.

See the [secrets cookbook](../cookbook/secrets.md) for full examples, the
redaction guarantee, and the known limitation around the `auth:` catalog.

## `params`

Declares the config's **trigger-time** override surface: the values that change
per run, each typed. Referenced anywhere in the config as `${param.NAME}` and
bound *before* the config is parsed, so a param can never alter the document's
structure and never reaches a connector unresolved.

```yaml
params:
  tenant_id: { type: string, required: true, description: "Tenant to sync" }
  since:     { default: "1970-01-01" }
  page_size: { type: int, default: 500 }
  api_token: { required: true, secret: true }
```

| Field | Default | Meaning |
|---|---|---|
| `type` | `string` | `string` · `int` · `float` · `bool` |
| `required` | `false` | The caller must supply a value. Mutually exclusive with `default`. |
| `default` | — | Value when none is supplied. An ordinary config scalar, so `default: "${env:SINCE}"` resolves. |
| `secret` | `false` | Registered for redaction the instant it is bound — never reaches a log, error, API response, audit record, or the template registry. |
| `description` | — | Surfaced by `faucet template list`/`show`, `GET /v1/templates`, and the MCP `get_template` tool. |

**Supplying values.** `faucet run --param name=value` (repeatable),
`faucet template run <id> --param …`, or `POST /v1/templates/{id}/runs` with a
`params` object. `--param-env NAME[=VALUE]` overrides an environment variable for
that run's `${env:VAR}` resolution only, without mutating the process
environment.

**Typing.** When `${param.NAME}` is a scalar's entire text the declared type
survives (an `int` param lands as a JSON number); embedded in a longer string it
is stringified, like every other namespace. Values are accepted as JSON (`500`)
or as strings (`"500"`) and coerced to the declared type, so CLI and HTTP behave
identically. A type mismatch, a missing `required` param, an undeclared
`--param`, or an undeclared `${param.x}` reference is an error naming the param.

**Validation.** `faucet validate` with no `--param` binds required params to
type-shaped placeholders, so a parameterized config validates in CI without
inventing values; passing any `--param` switches to strict binding. `faucet
schema params` prints the JSON Schema for one entry.

Persisting a parameterized config for register-once / trigger-by-id use is the
[pipeline template registry](../cookbook/templates.md).

## `matrix`

Each row is deep-merged onto `pipeline` (scalars replace, objects merge, arrays
replace). A row with `parent:` runs once per parent record. See the
[matrix DAG tutorial](../tutorials/matrix-dag.md). For DRY configs with many
rows, define named templates under `pipeline.sources` / `pipeline.sinks` and
select them per row with `ref:`.

### `depends_on` — completion ordering between rows

A row with `depends_on: [row_id, …]` starts only after **every listed row's
invocations finish successfully**. Unlike `parent:`, no records are consumed
and there is no per-record fan-out — it is pure run ordering ("load
dimensions, then facts"), typically paired with a downstream row whose source
reads what the upstream row's sink wrote.

```yaml
matrix:
  - id: dims
    source: { config: { query: "SELECT * FROM src_dims" } }
    sink:   { config: { table_name: dims } }
  - id: facts
    depends_on: [dims]        # starts only after `dims` succeeds
    source: { config: { query: "SELECT * FROM src_facts" } }
    sink:   { config: { table_name: facts } }
```

Semantics:

- Rows whose dependencies are all satisfied run concurrently under the usual
  `execution.max_concurrent` budget.
- A failed or skipped dependency **skips** the dependent row (and its own
  children and dependents in turn); the run's exit code reflects the original
  failure.
- Waiting on a row waits for that row's own invocations only. To also wait
  for its per-record children, list them explicitly.
- `parent:` and `depends_on:` compose on the same row (the parent edge is an
  implicit dependency).
- Unknown ids, self-dependencies, and cycles through any mix of `parent:` /
  `depends_on:` edges are rejected at load time by `faucet validate`.
- Ordering works identically under `faucet run`, `schedule`, and `serve` —
  they all execute the same expanded plan.

## `pipeline.nodes` / `pipeline.edges` (topology mode)

An alternative to `matrix:` for pipelines that need fan-out, fan-in, or joins:
declare an explicit graph of typed nodes (`source` / `transform` / `tee` /
`merge` / `join` / `sink`) under `pipeline.nodes`, connected by
`pipeline.edges` (`{ from, to, as? }`). Topology mode is **mutually exclusive**
with `matrix:` — both non-empty is a load-time error. `faucet run` / `validate`
/ `preview` all understand it. See
[Topology mode](../cookbook/topology.md) for the full grammar, the `join:`
node, state semantics, and runnable examples.

**What applies in topology mode.** Every top-level block does, each scoped to the
node where it makes sense. The per-page governance passes — `pipeline.masking`,
`pipeline.quality`, `pipeline.contract`, `schema:` — are enforced per sink node,
and `resilience:` applies to its writes. `sla:` keeps per-sink-node history under
`{pipeline}::{node_id}`; `notifications:` reports per sink node; `lineage:` emits
one job per sink node (`{pipeline}.{node_id}`) whose inputs are every source that
reaches it; `catalog:` records a dataset per source and per sink plus an edge for
each pair the graph connects. So do `--dry-run` / `--limit`, `${now.*}` /
`--clock`, `state:`, and `dlq:`. See the
[applies-per-node table](../cookbook/topology.md#observability) for the detail.

Column lineage is the one thing deliberately withheld: with several inputs
feeding a sink, the per-column derivation is not knowable from the graph, so the
facet is omitted for a multi-input sink rather than guessed. Single-input sinks
emit it as usual.

**`delivery: exactly_once` is supported**, with five requirements checked at load
time: exactly one source node, a replayable source, **every** sink idempotent, a
durable `state:`, and no `dlq:`. Each sink node carries its own commit watermark
and a restart resumes from the lowest committed sequence, so no sink is resumed
past its own progress. See
[Exactly-once delivery](../cookbook/topology.md#exactly-once-delivery).

**At-least-once resume is deliberately conservative.** Each sink node owns a
bookmark under `{pipeline}::{node_id}`. Without exactly-once the source resumes
from a stored position only when the graph has **exactly one source node** and
**every sink's bookmark is identical**; otherwise it replays in full (with a
warning). Bookmarks are compared for equality, never ordered — a resume position
is often structured (a CDC LSN map, a Kafka offset map), and an ordered "minimum"
over those can sit *ahead* of the true minimum and skip the lagging sink's
records. Replaying costs duplicates on a non-idempotent sink; skipping would lose
data, so the trade is made in that direction. Use `write_mode: upsert` on the
sinks — or `delivery: exactly_once` — when a graph is resumed routinely.

## Row selection

Register many rows, run a few. Selection resolves **after** expansion and never
changes a row's state key (`{name}::{row_id}`), so bookmarks are identical across a full
run and any subset. It runs on `run`, `validate`, and `preview` via the flags in the
[CLI reference](cli.md#run). Four axes compose through one formula:

```
1. eligible  = status gate ({mandatory, active} ∪ --status)
2. narrowed  = (eligible ∩ --tag) ∪ (--select / --only by id)
3. parents   = apply include_parents policy to narrowed
4. run set   = parents − (--skip)
```

**`status:` (readiness ladder)** — a field on the row's **source** (template or
`source:` override; deep-merges as a scalar). Default `active`, so existing configs are
unchanged. `mandatory` always runs; `active` runs by default; `available`/`draft`/
`archived` run only when their tier is added with `--status`. `mandatory` is removable
only by an explicit `--skip <id>`.

**`tags:`** — free-form `^[a-z0-9][a-z0-9_-]*$` labels on a row, **union-merged** with the
source template's `tags:` (the one deliberate exception to array-replace). Tags *narrow
within* the eligible set (`--tag`); they never resurrect a parked row — raise `--status`
for that.

**`selection.include_parents:`** — the single policy deciding whether a selected row's
`parent:` / `depends_on:` ancestor (not independently selected) is pulled in. `off`
(default, strict) errors naming each missing pair; `eligible` auto-includes
status-eligible ancestors (errors on a parked one); `all` includes any (warns on parked).
`--select <id>` by name always satisfies a dependency. Overridable with
`--include-parents`; env `FAUCET_INCLUDE_PARENTS`; precedence flag > env > config >
default.

```yaml
matrix:
  - id: people
    source: { ref: hibob, status: active,    config: { path: /v1/people } }
    tags: [core, daily]
  - id: payroll
    source: { ref: hibob, status: mandatory, config: { path: /v1/payroll } }
    tags: [finance]
  - id: audit
    source: { ref: hibob, status: available, config: { path: /v1/audit } }
    tags: [finance]

selection:
  include_parents: off   # off (default) | eligible | all
```

| Command | Runs |
|---|---|
| `faucet run cfg.yaml` | people, payroll |
| `faucet run cfg.yaml --tag finance` | payroll *(audit is finance but `available`)* |
| `faucet run cfg.yaml --status available --tag finance` | payroll, audit |
| `faucet run cfg.yaml --select audit` | audit *(opt-in, forced by name)* |

Unknown tokens, an empty run set, and a missing required ancestor are hard, fail-fast
errors (no partial run).

## `auth`

A map of named auth providers, each `{ type, config }` (`type` ∈ `static` /
`oauth2` / `oauth2_refresh` / `token_endpoint`). A connector references one with
`auth: { ref: <name> }` instead of inline auth; faucet builds each provider once
and shares it across every connector that references it (one token, single-flight
refresh). See the [authentication cookbook](../cookbook/auth.md).

```yaml
auth:
  api:
    type: oauth2_refresh
    config:
      token_url: ${env:API_TOKEN_URL}
      client_id: ${secret:API_CLIENT_ID}
      client_secret: ${secret:API_CLIENT_SECRET}
      refresh_token: ${secret:API_REFRESH_TOKEN}
```

## `delivery`

Controls the delivery guarantee for every pipeline row.

```yaml
delivery: at_least_once   # default — no behaviour change
# or:
delivery: exactly_once
```

| Value | Behaviour |
|-------|-----------|
| `at_least_once` | Default. A crash between the sink write and the bookmark persist causes the page to be re-delivered on the next run. Downstream must tolerate duplicates. |
| `exactly_once` | Require **at least effectively-once**. Two mechanisms qualify: the **atomic watermark** (the sink durably records a per-page commit token — which embeds the page's resume bookmark — atomically with the data; on resume the pipeline recovers the exact stream position from the sink's watermark, or skips already-committed pages for legacy tokens), and **keyed upsert** (`write_mode: upsert` + `key` on an upsert-capable sink, any source). `faucet validate` prints which mechanism each row derives. |

**Per-row override:** set `delivery:` directly on a matrix row to override the top-level value for that row.

```yaml
delivery: at_least_once    # top-level default

matrix:
  - id: critical_row
    delivery: exactly_once  # this row uses effectively-once
  - id: best_effort_row
    # inherits top-level at_least_once
```

### Requirements for `exactly_once`

The config is accepted when either effectively-once mechanism is achievable and
rejected otherwise, at config-load time (`faucet validate` and `faucet run`). A
violation is a hard `config error` naming the limiting side — no run is started.

**Keyed-upsert path** (any source): the sink must be upsert-capable
(`postgres`, `sqlite`, `mysql`, `mssql`, `mongodb`, `elasticsearch`,
`bigquery`) and configured with `write_mode: upsert` (or `delete`) and a
non-empty `key`. No other requirement — no watermark is used.

**Atomic-watermark path**, all four conditions:

1. **Positional-replay source** — the source must be one of: `postgres-cdc`, `mysql-cdc`, `mongodb-cdc`, `kafka`. These emit a complete resume position on every page over an immutable log. Query-based sources are rejected because different data on replay would cause the pipeline to silently skip records it never wrote.
2. **Idempotent sink** — the sink must be one of: `sqlite`, `postgres`, `mysql`, `mssql`, `iceberg`, `bigquery`, `kafka`, `snowflake`, `redis`, `mongodb` (MongoDB requires a replica set at run time). These sinks atomically commit both the data and a watermark token inside the same transaction or snapshot.
3. **Durable state store** — a `state:` block is required, and it must be a durable backend (`file`, `redis`, or `postgres`) — `memory` is rejected. The pipeline stores the per-page sequence number alongside the bookmark; the watermark must survive a restart, so an in-memory store (lost on process exit) would silently re-deliver an already-committed page on resume.
4. **No DLQ** — a `dlq:` block is incompatible with the atomic-watermark path in this version. (The keyed-upsert path permits a DLQ.)

See the [Effectively-once delivery cookbook](../cookbook/state.md#effectively-once-delivery) for a worked example and the full rationale.

## `schema`

Optional **pipeline-level** block (a sibling of `source` / `sink` / `transforms`
/ `state` inside `pipeline:`) that declares one uniform policy for **schema
drift** — when an incoming page's top-level shape diverges from the sink's live
destination schema. Fully opt-in: with no block, sinks keep their existing
per-connector behaviour. See the [Schema drift cookbook](../cookbook/schema-drift.md)
for the full model, sink-support matrix, and per-sink nuances.

```yaml
pipeline:
  schema:
    on_drift: warn                     # warn | evolve | ignore | quarantine | fail
    allow_type_widening: true          # default true; only consulted by `evolve`
    on_incompatible: fail              # fail | quarantine — `evolve` only (default fail)
    relax_nullability_on_missing: false # default false; `evolve` only
  source: { ... }
  sink: { ... }
```

| Field | Default | Purpose |
|-------|---------|---------|
| `on_drift` | `warn` | Policy applied when drift is detected: `warn` (metric + log, write unchanged), `ignore` (drop unknown fields), `fail` (abort with a `SchemaDrift` error), `quarantine` (route drift-exhibiting rows to the DLQ, write the rest), `evolve` (apply additive/widening DDL, then write). |
| `allow_type_widening` | `true` | Whether a lossless widening (`integer → number`, gaining nullability) counts as evolvable rather than incompatible. Only consulted by `evolve`. |
| `on_incompatible` | `fail` | `evolve` only — action for an incompatible residue (narrowing / type swap): `fail` aborts, `quarantine` routes the offending rows to the DLQ. |
| `relax_nullability_on_missing` | `false` | `evolve` only — whether a `NOT NULL` destination column **absent** from a page may have its `NOT NULL` constraint dropped. Default `false`: an omitted column is not evidence of optionality, so the constraint is left untouched (a genuinely-missing required value then fails at write time). Set `true` only to deliberately let omission relax nullability. Relaxation from an *observed* null in a present column (a widening) is unaffected. |

Detection is **top-level only** — a nested object is one column, so changes
inside it are invisible.

### Gates (validated at config-load time)

A violation is a hard `config error` naming the offending row; no run is started.

1. **`evolve` needs an evolution-capable sink** — one of `postgres`, `mysql`,
   `mssql`, `sqlite`, `bigquery`, `elasticsearch`. `iceberg` supports detection
   but not `evolve` (blocked on upstream `iceberg-rust`, #255); schemaless sinks
   have nothing to evolve. Both are rejected for `on_drift: evolve`.
2. **`quarantine` needs a `dlq:` block** — `on_drift: quarantine`, or `evolve`
   with `on_incompatible: quarantine`.
3. **`quarantine` is incompatible with `delivery: exactly_once`** (effectively-once
   forbids a DLQ). `evolve` / `ignore` / `fail` / `warn` all compose with
   effectively-once and with `write_mode: upsert`.

Against a **schemaless** sink (jsonl, csv, stdout, mongodb, redis, http, kafka,
s3, gcs, snowflake, parquet) any non-`evolve` policy is inert — the sink reports
no schema to diverge from.

## `contract`

Optional **pipeline-level** block (a sibling of `source` / `sink` /
`transforms` inside `pipeline:`; no matrix-row override in v1) declaring a
**data contract**: a versioned promise about the pipeline's output shape,
enforced per page after transforms and quality checks and before the sink
write. Requires the `contract` Cargo feature (in the default build). See the
[Data contracts cookbook](../cookbook/contracts.md) for the full model and
`faucet schema contract` for the block's JSON Schema.

```yaml
pipeline:
  contract:
    version: "1.0.0"            # required, non-empty
    description: Orders feed.   # optional metadata
    owner: data-platform        # optional metadata
    on_breach: fail             # fail (default) | quarantine | warn
    allow_extra_fields: true    # default true
    fields:                     # required, non-empty; names unique
      - name: order_id
        type: string            # string | integer | number | boolean | object | array
        required: true          # default true
        nullable: false         # default false
        min_length: 1           # string-only (with max_length)
      - name: status
        type: string
        enum: [open, shipped, cancelled]
      - name: amount
        type: number
        min: 0                  # numeric-only (with max)
```

| Field | Default | Purpose |
|-------|---------|---------|
| `version` | — | Carried into breach errors, DLQ envelopes, and exports. Semver recommended (major = breaking, minor = additive). |
| `on_breach` | `fail` | `fail` aborts on the first breach (nothing from the page is written); `quarantine` routes breaching records to the DLQ and writes the rest (requires a `dlq:` block — validated at load time); `warn` logs + counts but writes everything. |
| `allow_extra_fields` | `true` | When `false`, an undeclared top-level key is a breach (`extra_field`). |
| `fields[]` | — | Per-field type + constraints: `required`, `nullable`, `enum`, `pattern` (string), `min`/`max` (numeric, inclusive), `min_length`/`max_length` (string, inclusive), `description`. |

A malformed contract (empty version, duplicate fields, invalid regex, empty or
type-mismatched `enum`, constraints on the wrong type, `min > max`) is a
config-load error — `faucet validate` catches it. `fail`/`warn` compose with
`delivery: exactly_once`; `quarantine` does not (effectively-once forbids a DLQ).
Inspect or export the contract with [`faucet contract`](./cli.md#contract).

## `masking`

Optional **pipeline-level** block (a sibling of `source` / `sink` /
`transforms` inside `pipeline:`) declaring a **PII detection + column-masking**
policy. The masking pass runs **first** — before the quality, contract, and
schema-drift passes and before every sink write, the DLQ, and lineage sampling
— so PII never reaches a sink (including the DLQ) or an OpenLineage facet
unmasked. Masking is value-only and key-preserving: it never fails a run or
quarantines (no `dlq:` required). Requires the `masking` Cargo feature (in the
default build). See the [masking cookbook](../cookbook/masking.md) for the full
model and `faucet schema masking` for the block's JSON Schema.

```yaml
pipeline:
  masking:
    description: Mask customer PII.       # optional metadata
    key: ${vault:secret/faucet#mask_key}  # optional — keyed HMAC-SHA256 for hash/tokenize
    rules:                                # required, non-empty; first match per field wins
      - name: emails                      # optional label (logs + metric); default rule_<n>
        match:                            # at least one of the three must be set
          value_detector: email          # email | credit_card | ssn | phone | ipv4
        action: { type: redact }          # replace with `mask` (default "***")
      - match: { field_pattern: '(?i)^ssn$' }   # regex over the field dot-path
        action: { type: hash }            # HMAC-SHA256 (keyed) / SHA-256 (unkeyed) hex
      - match: { fields: [card] }         # explicit dot-paths
        action: { type: partial, keep_last: 4 }   # reveal only the last N chars
        applies_to: [warehouse]           # scope to sink template name(s) / connector kind(s)
```

| Field | Default | Purpose |
|-------|---------|---------|
| `description` | — | Documentation metadata. |
| `key` | — | Secret for keyed HMAC-SHA256 `hash`/`tokenize` (deterministic + irreversible). Absent → unkeyed SHA-256 (deterministic but recomputable). Resolved after secrets, so `${vault:...}` etc. work. |
| `rules[]` | — | Required, non-empty. Each rule = `name` (optional label) + `match` + `action` + optional `applies_to`. Evaluated in order; the first rule that matches a field wins. |
| `rules[].match` | — | At least one of `field_pattern` (regex over the dot-path), `value_detector` (`email`/`credit_card`/`ssn`/`phone`/`ipv4`, run over string values), `fields` (explicit dot-paths). A match on a container masks the whole subtree. |
| `rules[].action` | — | Tagged by `type`: `redact` (`mask`, default `"***"`; `mask: null` nulls the field), `hash`, `tokenize` (`prefix`), `partial` (`keep_last` default `4`, `mask_char` default `*`; `keep_last >= len` masks everything). |
| `rules[].applies_to` | `[]` (all sinks) | Scope the rule to specific sinks by template name (under `pipeline.sinks:`) or connector kind (e.g. `bigquery`). |

Detectors are conservative (fully anchored; `credit_card` requires a valid
Luhn checksum; `ssn` excludes never-issued ranges) so false positives stay
rare. `hash`/`tokenize` are deterministic → masked values stay joinable across
pipelines that share a `key`. A malformed policy (empty rules, an empty
`match`, an invalid regex, an empty `tokenize` prefix) is a config-load error —
`faucet validate` and [`faucet masking`](./cli.md#masking) catch it.

- `faucet_masking_fields_total{pipeline,row,rule,action,detector}` — one
  increment per masked field (`detector` empty for name-based matches).

## `execution`

- `max_concurrent` — one shared concurrency budget across roots and child
  fan-outs.
- `on_error` — `continue` (siblings finish; failed subtree skipped) or `stop`
  (abort pending and in-flight work on first failure).

### Adaptive batch sizing

The optional `adaptive_batch_size:` sub-block enables the AIMD controller that
auto-tunes the effective write batch size from observed sink latency and error
rate. Default `enabled: false` (opt-in).

```yaml
execution:
  adaptive_batch_size:
    enabled: true          # master switch
    controller: aimd       # only "aimd" is supported in v1
    min: 100               # lower bound (rows)
    max: 50000             # upper bound; inert above the source page size
    increase_step: 250     # additive growth per clean batch
    decrease_factor: 0.5   # multiplicative shrink on error/high latency  (0, 1)
    cooldown_batches: 5    # batches to skip after a shrink
    target_latency_ms: null  # optional write-latency target (ms)
    latency_window: 10     # rolling window size for p50 latency
    error_threshold: 0.01  # per-batch error rate that triggers a shrink
    respect_source_max: true  # cap at source page size (see Caveats)
    log_every: 50          # tracing::info every N adjustments
```

**Key caveats:**

- **Error-driven shrink requires a `dlq:` block.** Without one the controller
  sees no per-row errors; only `target_latency_ms` can drive shrinks.
- **Effective ceiling = source page size.** In v1 the controller reslices pages
  in-memory — it cannot buffer across pages. Setting `max` higher than the
  source `batch_size` is harmless but inert. Raise the source `batch_size` to
  allow bigger write batches.
- **No-op for per-record sinks.** `jsonl`, `csv`, and `stdout` write one record
  at a time; the controller adjusts normally but the write granularity is
  unchanged.

See the [Adaptive batching cookbook](../cookbook/adaptive-batching.md) for a
full worked example, the AIMD trajectory, and the four Prometheus metrics
(`faucet_pipeline_adaptive_batch_*`).

## `resilience`

Optional top-level block giving the pipeline one declarative place to configure
retry, a circuit breaker, and per-row poison-pill handling. Fully opt-in: with
no `resilience:` block, sink writes are not retried and source connectors keep
their built-in retry defaults. See the
[Resilience cookbook](../cookbook/resilience.md) for the full model, composition
notes, and metrics.

```yaml
resilience:
  retry:
    max_attempts: 5            # total tries including the first (1 = no retry)
    backoff: exponential       # none | fixed | exponential
    base_ms: 200
    max_ms: 30000              # per-sleep cap, before jitter
    jitter: true
  retry_on: [http_5xx, rate_limited, connection, timeout]
  circuit_breaker:
    consecutive_failures: 5
    cooldown_secs: 60
  poison:
    max_row_attempts: 3
    action: dlq                # dlq | drop | fail
```

- **`retry`** — `max_attempts` (default `5`; `1` disables retry), `backoff`
  (`none` / `fixed` / `exponential`, default `exponential`), `base_ms` (default
  `200`), `max_ms` (per-sleep cap, default `30000`), `jitter` (default `true`,
  applies `[0.5, 1.5)` decorrelated jitter).
- **`retry_on`** — the transient error classes that are retried:
  `http_5xx` (HTTP 5xx), `rate_limited` (HTTP 429 / rate-limit signals),
  `connection` (DNS / refused / reset), `timeout` (request timeouts). Omit for
  all four; an empty list is rejected at config load.
- **`circuit_breaker`** — `consecutive_failures` consecutive fully-failed pages
  open the breaker and fail the run with a `CircuitOpen` error;
  `cooldown_secs` is advisory for `faucet schedule` (delays the next cron tick).
- **`poison`** — per-row DLQ-path handling: `max_row_attempts` re-submits a
  still-failing retriable row before the terminal `action` — `dlq` (requires a
  `dlq:` block), `drop`, or `fail`.

The `rest` source's legacy `max_retries` / `retry_backoff` fields win when set
explicitly; otherwise the injected policy's `max_attempts` + `base` apply (its
`retry_on` / `max` / `jitter` are inert on REST, honored on `xml` / `graphql`
and on every sink-side write).

## `sla`

Optional top-level block declaring a freshness/volume SLA for the pipeline
(evaluated after every root invocation by `faucet run` / `schedule` / `serve` /
`replicate`). Fully opt-in and **never fails a run**: violations emit the
`faucet_pipeline_sla_violations_total{pipeline,row,kind}` counter and a
structured warning, and `faucet doctor` reports staleness / baseline health.
See the [SLA monitoring cookbook](../cookbook/sla.md).

```yaml
sla:
  max_staleness_secs: 7200     # stale when no successful run within 2h
  min_rows_per_run: 1          # a successful run writing fewer records violates
  volume_anomaly:              # learned-baseline anomaly detection
    method: zscore             # zscore | iqr
    sensitivity: 3.0           # zscore default 3.0; iqr default 1.5
    min_history: 5             # successful runs before detection starts
    window: 20                 # rolling baseline size
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_staleness_secs` | int | — | Maximum seconds since the last **successful** run. Evaluated when a run fails (against the previous success) and by `faucet doctor`. Requires a `state:` block. |
| `min_rows_per_run` | int | — | Static volume floor for a successful run (catches a source silently returning nothing). Stateless — works without a `state:` block. |
| `volume_anomaly.method` | `zscore` \| `iqr` | `zscore` | How a successful run's volume is compared against the rolling baseline of recent successful runs. |
| `volume_anomaly.sensitivity` | float | `3.0` / `1.5` | `zscore`: max \|x − mean\| / std. `iqr`: Tukey fence multiplier. Defaults per method. |
| `volume_anomaly.min_history` | int | `5` | Cold-start guard: successful runs of history required before detection fires (min 2). |
| `volume_anomaly.window` | int | `20` | Rolling window of successful-run volumes kept as the baseline (≥ `min_history`). |

At least one of the three checks must be set. `max_staleness_secs` /
`volume_anomaly` require a `state:` block (enforced at config load); the
history is persisted next to the pipeline's bookmarks under
`{name}::{row}::__sla__`. With a `memory` state store the history only
persists within a single `faucet schedule` / `serve` process. Schema:
`faucet schema sla`.

## `notifications`

*(requires the `notify` build feature)*

A list of rules that fan pipeline lifecycle / health events out to Slack,
PagerDuty, or a signed webhook. Events: `run_failure`, `run_success`,
`sla_breach`, `circuit_open`, `contract_abort`, `dlq_threshold`,
`scheduler_stuck`. Fires from every runtime; delivery never fails a run.

```yaml
notifications:
  - name: oncall
    on: [run_failure, circuit_open, contract_abort]
    dedupe_window_secs: 300     # optional leading-edge coalesce
    min_severity: error         # optional floor: info|warning|error|critical
    channel:
      type: pagerduty           # slack | pagerduty | webhook — {type, config}
      config:
        routing_key: "${env:PAGERDUTY_ROUTING_KEY}"
```

Per-rule fields: `name` (unique), `on` (event kinds; empty = all),
`min_severity`, `dedupe_window_secs`, `dlq_threshold` (min DLQ rows for the
`dlq_threshold` event), and `channel` (`{ type, config }`). Channel secrets
should come from `${env:...}` / `${secret:...}` so they are log-redacted. See
the [Notifications cookbook](../cookbook/notifications.md) for channel details,
metrics, and `faucet notify test`. Schema: `faucet schema notifications`.

## `replication`

Present only when you run [`faucet replicate`](cli.md#replicate). It turns the
main `pipeline` (whose `source` is a CDC connector) into a snapshot→CDC mirror by
adding a one-time bulk-read snapshot source. `faucet run` ignores this block, the
same way it ignores `schedule:`.

```yaml
replication:
  mode: snapshot_then_cdc          # REQUIRED. Only mode in v1.
  continuous: true                 # After the snapshot, keep streaming CDC until SIGTERM. Default true.
  snapshot:                        # REQUIRED. The one-time bulk-read source.
    source:
      type: postgres               # A non-CDC query reader of the same upstream DB.
      config:
        connection_url: ${env:SOURCE_PG_URL}
        query: "SELECT * FROM public.orders"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | `snapshot_then_cdc` | **required** | Replication strategy. Only `snapshot_then_cdc` exists in v1: capture the CDC position, bulk-snapshot the table, then stream CDC from that position. |
| `snapshot.source` | connector | **required** | A **non-CDC** bulk-read source (e.g. `postgres` / `mysql` / `mongodb` running a query) pointing at the same upstream database. Back-fills the destination through `pipeline.sink` before CDC starts. |
| `continuous` | bool | `true` | When `true`, keep streaming CDC after the snapshot completes until Ctrl-C / SIGTERM; a transient CDC-phase failure is logged, backed off (capped, reset on success), and resumed from the persisted bookmark rather than crash-exiting. When `false`, drain CDC once and exit (surfacing a transient error as a non-zero exit). |

**Requirements** (enforced at config-load time, also reported by `faucet validate`):

- `pipeline.source` must be a CDC connector — `postgres-cdc`, `mysql-cdc`, or
  `mongodb-cdc` (the capture-capable set).
- `pipeline.sink` should use [`write_mode: upsert`](../cookbook/upsert.md) with a
  `key` for a true mirror; an append sink validates with a warning (boundary
  duplicates are possible).
- A durable [`state:`](../cookbook/state.md#state-stores) backend is required
  (`file` / `redis` / `postgres`) — `memory` is rejected, since the snapshot→CDC
  handoff and resume depend on the persisted phase marker and bookmark.
- No `matrix:` — replication is a single pipeline in v1.
- For `postgres-cdc`, a **permanent** replication slot (`slot_type: permanent`,
  the default) is required so WAL is retained across the snapshot.

See the [replication cookbook](../cookbook/replication.md) for the correctness
model (capture-before-snapshot + upsert idempotency), the resume behaviour, and
the per-database log-retention caveats.

## `backfill`

Optional **defaults** for [`faucet backfill`](cli.md#backfill) — the range
itself always comes from the command line. `faucet run` ignores this block, the
same way it ignores `schedule:` / `replication:`. Whenever the block is
present, `faucet validate` also checks that at least one root source references
a `${backfill.*}` / `${now.*}` scoping token (an unscoped source would replay
identical data into every window).

```yaml
backfill:
  window: 1d                  # default --window: 45s / 30m / 6h / 1d / 1w
  concurrency: 4              # default --concurrency (max units in flight); default 1
  timezone: America/New_York  # default --timezone (IANA); default UTC
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `window` | string | — (whole range as one unit) | Chunk duration for the requested range. |
| `concurrency` | int ≥ 1 | `1` | Max concurrently-running window units. |
| `timezone` | string | `UTC` | IANA zone for date boundaries and `${now.*}` rendering. |

`faucet schema backfill` prints the JSON Schema. See the
[backfill cookbook](../cookbook/backfill.md) for the token table, resume
semantics, and the HTTP endpoint.

## `schedule`

Present only when you run `faucet schedule`. Absent configs are rejected by that
command with a hint to use `faucet run` instead. All fields except `cron` are
optional.

```yaml
schedule:
  cron: "0 2 * * *"               # REQUIRED. Standard 5-field cron, or 6-field with leading seconds.
  timezone: "UTC"                 # IANA timezone name. Default UTC.
  overlap_policy: skip            # skip | queue | forbid. Default skip.
  max_runs: null                  # null = run forever; N = exit 0 after N successful runs.
  max_consecutive_failures: null  # null = never exit on failure; N = exit non-zero after N straight failures.
  on_failure: continue            # continue | stop. Default continue.
  start_immediately: false        # Run once on startup before waiting for the first tick. Default false.
  run_timeout_secs: null          # Per-run wall-clock kill switch (seconds). Timed-out runs count as failed.
  shutdown_grace_secs: 30         # SIGTERM: wait this long for the in-flight run before aborting. Default 30.
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cron` | string | **required** | 5-field standard Unix cron (`MIN HOUR DOM MON DOW`) or 6-field with a leading seconds field (`SEC MIN HOUR DOM MON DOW`). Validated at load time. |
| `timezone` | string | `"UTC"` | IANA timezone name (e.g. `"America/Los_Angeles"`, `"Europe/Berlin"`). Affects how the cron expression is interpreted. |
| `overlap_policy` | `skip` \| `queue` \| `forbid` | `skip` | What to do when a tick fires while a run is already in flight. `skip` drops the tick; `queue` buffers one missed tick (in-memory only, lost on restart); `forbid` exits non-zero. |
| `max_runs` | integer \| null | `null` | Stop the scheduler cleanly (exit 0) after this many *successful* runs. `null` means run forever. `0` is rejected as a config error. |
| `max_consecutive_failures` | integer \| null | `null` | Exit non-zero after this many consecutive failed runs without a success in between. A successful run resets the counter. `null` means never exit on failures alone. |
| `on_failure` | `continue` \| `stop` | `continue` | `stop` exits non-zero immediately after the first failed run. `continue` keeps scheduling; use `max_consecutive_failures` to bound sustained outages. |
| `start_immediately` | bool | `false` | When `true`, the first run fires right on startup before the cron clock reaches its first tick. |
| `run_timeout_secs` | integer \| null | `null` | Per-run time limit in seconds. A run that exceeds this is killed and counts as a failure. `null` means no timeout. |
| `shutdown_grace_secs` | integer | `30` | On SIGTERM/SIGINT, wait this many seconds for the in-flight run to finish before forcibly aborting it. |

**Validation:** `faucet validate pipeline.yaml` checks the `schedule:` block at parse time — bad cron
syntax, unknown timezone names, `max_runs: 0`, and a cron expression that can never fire all produce
a clear `config error: schedule: …` message before any run starts.

See the [scheduling cookbook](../cookbook/scheduling.md) for worked examples, the DST/timezone
details, the overlap-policy decision tree, and the full Prometheus metric set.

## `lineage`

Optional. When present, every pipeline run emits [OpenLineage](https://openlineage.io/) `RunEvent`s
describing the job, its input/output datasets, inferred schemas, and column-level lineage. Emission
**never fails a run** — transport errors are logged and counted but do not propagate.

```yaml
lineage:
  namespace: prod.warehouse      # REQUIRED. Logical namespace for all jobs and datasets.
  transport:                     # REQUIRED. Where to send events.
    type: http                   # http | file | kafka (kafka requires lineage-kafka feature)
    config:
      url: ${env:MARQUEZ_URL}
  job_name: ${name}::${row_id}   # Default. Resolved per matrix row at run time.
  include_schema_facet: false    # Emit DatasetFacets.schema (inferred from a sample).
  include_column_lineage: false  # Emit column-level lineage where statically derivable.
  include_source_code_facet: false  # Emit resolved config as a sourceCode job facet (warns; may expose secrets).
  emit_on:
    start: true
    running: false               # RUNNING heartbeats; see heartbeat_interval.
    complete: true
    fail: true
    abort: true
  sample_records: 100            # Max records sampled for schema/column facets.
  heartbeat_interval: 30         # Seconds between RUNNING heartbeats (when emit_on.running is true).
```

See the [Lineage cookbook](../cookbook/lineage.md) for the full field reference, the three
transports (HTTP, file, Kafka), the column-lineage support matrix, schema-facet behavior, and
the Prometheus metrics (`faucet_lineage_events_total`, etc.).

## `catalog`

Optional. When present, `faucet run` / `schedule` / `replicate` record every
successful **root** invocation into the [Data Movement Catalog](../cookbook/catalog.md) —
the persistent, cross-run store of datasets, schema timelines, volume/freshness
stats, and lineage edges. Recording **never fails a run**. `faucet serve`
ignores this block: it records into its `--history` backend automatically.
Requires a build with the `catalog` feature (in `--features full`).

```yaml
catalog:
  url: sqlite:./faucet-catalog.db   # REQUIRED. sqlite:<path> | postgres://… | memory
  sample_records: 100               # Records sampled per side for schema inference.
```

SQL stores additionally require the matching `serve-history-sqlite` /
`serve-history-postgres` build feature. Browse the store with
[`faucet catalog`](./cli.md#catalog), the `/v1/catalog/*`
[HTTP endpoints](./http-api.md), or the web console's Datasets / Lineage views.
Schema: `faucet schema catalog`.

## `observability`

Optional top-level block that enables runtime observability backends. All
sub-blocks are independently optional; omitting the entire `observability:` key
leaves the defaults (no Prometheus server, no OTLP export).

### `otel:`

Pushes traces and metrics to any OTLP-compatible collector. Requires building
the CLI with `--features otel` (included in `full`).

```yaml
observability:
  otel:
    endpoint: "http://localhost:4317"
    protocol: grpc
    headers: {}
    sample_ratio: 1.0
    export: [traces, metrics]
    service_name: faucet
    timeout_secs: 10
    metric_interval_secs: 60
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | `http://localhost:4317` (grpc) / `http://localhost:4318` (http) | OTLP collector URL. For `http`, if the URL does not already contain a per-signal path (`/v1/traces`, `/v1/metrics`), faucet appends it automatically. |
| `protocol` | `grpc` \| `http` | `grpc` | Transport protocol. `grpc` uses tonic; `http` uses HTTP/Protobuf. The `faucet` CLI always runs inside a tokio runtime, so both work without extra setup. |
| `headers` | map&lt;string, string&gt; | `{}` | Extra headers sent on every export request — auth tokens, team keys, etc. Values are secret-interpolated the same as any config value (e.g. `"${env:HONEYCOMB_KEY}"`). |
| `sample_ratio` | float | `1.0` | Head-based trace sampling probability, `0.0`–`1.0`. `1.0` exports every trace; `0.1` keeps ~10%. Does not affect metric export. |
| `export` | list | `[traces, metrics]` | Which signals to push. Each element is `traces` or `metrics`. Omit a signal to disable it entirely. |
| `service_name` | string | `faucet` | Value of the OpenTelemetry resource attribute `service.name` attached to every span and metric point. |
| `timeout_secs` | integer | `10` | Per-export timeout in seconds. Timed-out exports are counted in `faucet_otel_export_failures_total` but do not fail the run. |
| `metric_interval_secs` | integer | `60` | How often (in seconds) accumulated metric points are pushed to the collector. |

**Coexistence:** `observability.otel:` and `observability.prometheus:` are
fully independent; both can be active at the same time and metrics fan out to
both exporters. Export failures are never propagated to the pipeline — they
increment `faucet_otel_export_failures_total{signal}` and are logged.

## Discovery & env files

`run` / `validate` / `preview` / `schedule` auto-discover `faucet.yaml` → `.yml` → `.json` in
the current directory, and load a sibling `.env` unless `--no-env-file` is given
(`--env-file PATH` points elsewhere).

> The authoritative, exhaustive grammar — including every matrix and template
> edge case — is in
> [`cli/README.md`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/README.md).

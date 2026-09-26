# RFC 0010 — Data-flow policies and change impact analysis

*Decide, before any data moves, whether a labelled column may reach a sink (`policy:`, #702), and who downstream a planned schema change would break (`faucet plan --impact`, #707) — both computed from what faucet already knows: the config's own column lineage and the Data Movement Catalog's cross-pipeline lineage graph.*

| | |
|---|---|
| **RFC** | 0010 |
| **Title** | Data-flow policies and change impact analysis |
| **Status** | Accepted |
| **Authors** | faucet-stream maintainers |
| **Related issues** | #702 (policies) · #707 (impact) · #206 (masking) · #204 (contracts) · #279 (catalog) · #283 / #374 (plan) · #123 (lineage) · epic #38 |
| **Related ADRs** | — |

## Summary

Two governance questions come up in review of every data-movement change and
today need someone to read other teams' configs to answer:

1. **"May this column go there?"** — a customer email must stay in the EU, a
   salary must not land in a plain file in production. Masking (#206) can
   rewrite a value; nothing says where a value is *allowed* to flow.
2. **"Who breaks if I rename this column?"** — `faucet plan` (#283) shows the
   change's effect on its own destination; the catalog (#279) knows which
   pipelines read that destination; nobody connects the two.

This RFC adds both as pure analyses over data faucet already has. A
**data-flow policy** labels columns (`classifications`) and constrains where a
label may go (`rules`); it is evaluated statically by `validate` / `policy` /
`plan` / `doctor` / `run` / the serve submit path / template registration, and
backed at run time by a sink decorator that classifies the real records with
value detectors. **Impact analysis** takes the schema `plan` would write,
diffs it against the catalog's last observation of the sink, and walks the
lineage graph downstream — following each edge's recorded column lineage,
naming a downstream contract that promises an affected column, and the
dataset's declared owners and consumers.

## Motivation

- **Residency and destination rules are policy, not code.** Teams express
  "PII stays in region X" once and expect every pipeline to honour it. A
  per-pipeline masking rule is the wrong level: it says *how* to hide a value,
  not *whether* the movement is allowed, and it cannot be checked centrally.
- **Silent downstream breakage is the worst class of bug** (CLAUDE.md). A
  dropped column is green at the pipeline that drops it and red three
  pipelines later, in someone else's on-call. The lineage graph exists
  precisely to answer this; it just had no consumer.
- **Both must be decided before data moves.** A refusal after the first page
  is a leak; a breakage report after the run is an incident. So the primary
  surface for both is `validate` / `plan` / the submit gate, with the runtime
  backstop as defence in depth, never the first line.

## Design

### Vocabulary (PRINCIPLES.md §8)

| Word | Means exactly |
|---|---|
| `policy` | The top-level data-flow policy block / file: classifications + rules about which sinks a label may reach. Not to be confused with `masking` (how to rewrite a value), `contract` (a promise about the output shape) or `quality` (per-record assertions). |
| `classification` / `label` | A name attached to a column by a policy: by field name, by field-name pattern, or by value detector. |
| `attributes` | Free-form string pairs on a **sink** (`residency: eu`) a policy rule reasons about. |
| `impact` | The downstream consequence of a planned schema change, per affected dataset / contract / consumer, with a severity. |
| `consumer` | A declared external reader of a dataset (a dashboard, a model, an export). Pipelines are consumers automatically through lineage edges and are never declared. |
| `owner` | A declared party responsible for a dataset; what an impact report tells you to notify. |

### Data-flow policies (#702)

**Spec** (`faucet_core::policy::PolicySpec`, `faucet schema policy`):

```yaml
policy:
  version: 1
  classifications:
    - label: pii
      fields: [email, phone]              # full dot-path or leaf key
      field_pattern: "(?i)_?name$"        # regex over the dot-path
      value_detector: email               # email|credit_card|ssn|phone|ipv4
  rules:
    - name: pii-eu
      when: { label: pii, sink_kind: [postgres], sink: { environment: [prod] } }
      require: { residency: [eu] }        # sink attribute must be one of
      mask: [hash, tokenize]              # or the column reaches it masked
      deny: false
      on_runtime: fail                    # fail | quarantine
```

A rule **applies** to a column when the column carries `when.label`, the
sink's kind is in `when.sink_kind` (when set), and every `when.sink`
attribute is present with a listed value (a missing attribute means the rule
does not apply — a rule scoped to `environment: prod` never fires on a sink
that declares no environment). It is **satisfied** when every `require`
attribute is present with a listed value, or the column is masked with a
listed `mask` action; `deny: true` is never satisfied. Three violation kinds
exist: `denied`, `missing_attribute`, `attribute_not_allowed`.

**Where a policy comes from**, merged in this order: the config's top-level
`policy:` (a deployment overlay's `policy:` lands here), then a `--policy
<file>` (`run` / `validate` / `plan` / `doctor` / `policy`) or `faucet serve
--policy` for every submission. `PolicySpec::merge` concatenates
classifications and rules and refuses two rules with one name.

**Sink attributes** live on `ConnectorSpec.attributes` (a sink template
carries them for every row; a matrix row's `sink.attributes` adds or
overrides).

**Static pass** (`cli/src/policy`): for each expanded row, the columns the
row is known to carry (its data contract, or a caller-supplied schema — a
`plan --sample`, the catalog's last source schema) are labelled by name and
pushed through the row's transform chain with the same column-lineage ops
OpenLineage emission uses, so a rename keeps its label (`via: lineage`). An
**opaque** transform (`flatten`, `explode`, `keys_case`, `sql`, `wasm`,
custom) carries every labelled input **conservatively** — a rename cannot
hide a label. A column the row's masking policy provably rewrites at that
sink (name-matched rules, honouring `applies_to`) counts as masked. Topology
mode is evaluated per sink node from the pipeline-level contract, carrying
labels conservatively when any transform node exists. A row with no
contract and no supplied schema has *no static knowledge*; the report says
so and the runtime backstop is the enforcement.

**Runtime backstop** (`faucet_core::PolicySink`): the outermost sink
decorator (after masking and profiling, so a masked value no longer trips
its detector) classifies each record's scalar leaves by name and by value
detector and evaluates the same rules. `on_runtime: fail` errors before the
page is written; `quarantine` fails the offending rows through
`write_batch_partial` (DLQ-routable; a `dlq:` block is required at load
time). `FaucetError::PolicyViolation { rule, column, message }` is the typed
error; `faucet_policy_violations_total{pipeline,row,rule,phase,action}` the
metric, shared by both phases.

**Surfaces**: `faucet policy <config> [--policy F] [--row] [--json]` (exit
code = violations); `validate` prints the report and fails (`valid: false` +
`policy` in `--json`); `plan` reports (`policy` in the JSON, never fails — a
preview); `doctor` adds a `policy` probe per root; `run` refuses before any
connector is built (`CliError::PolicyViolations`, exit code = count);
`faucet serve --policy` refuses a violating submission with 422 and audits
`policy.denied`, warns on a violating pipeline-template registration
(`warnings[]` on the summary — it may be composed with a compliant sink
later) and audits a runtime denial as `policy.denied` by principal `runtime`.

### Change impact analysis (#707)

**Storage**: `CatalogDataset` gains `owners: Vec<String>` and `consumers:
Vec<CatalogConsumer { name, kind, contact, columns, registered_by,
registered_at }>` (serde-defaulted, so existing rows read back unchanged;
the SQL backends keep the dataset in its `body` column, no DDL change).
`RunHistory::catalog_annotate(id, &CatalogAnnotation)` merges owners
(replace) and consumers (upsert by name; `replace_consumers` drops the
unlisted) on the memory + SQL backends, forwarded by the fallback wrapper.
Annotations come from `catalog.datasets[]` (merged after every run that
touches the dataset, attributed `config`), `POST
/v1/catalog/datasets/{id}/consumers` (`CatalogAnnotate`, operator+, audited
`catalog.annotate`) and `faucet catalog annotate`. `RowSnapshot.contract`
(the serialized `ContractSpec`) is recorded on config snapshots so a
downstream contract can be named; `faucet serve` now records config
snapshots too.

**Analysis** (`cli/src/impact`, pure over catalog reads):

1. The row's sink dataset is the lineage edge `(pipeline, row)` last
   recorded. No edge → "never run", no walk, exit 0.
2. **Planned schema**: the plan sample's output (`planned_from: sample`),
   else the catalog's last *source* schema pushed through the row's column
   ops (`lineage`; each output typed like its single origin), else unknown
   (opaque chain and no sample).
3. **Delta** against the sink's last observed schema: `added` / `removed` /
   `retyped`; a removed+added pair the row's own `Rename` op maps is a
   `renamed` entry.
4. **Downstream walk**: BFS over `src → dst` edges up to `--depth` (default
   5). An edge's recorded column lineage (`{"fields": {out: [in…]}}`) says
   which downstream columns read an affected one; only non-additive changes
   flow. An edge without column lineage makes that dataset and everything
   past it `unknown` — never a false `none`. A dataset nothing it reads
   changed is dropped, and the walk stops there.
5. **Contracts**: the affected pipeline's last config snapshot row
   `contract`; a declared field among the affected columns escalates to
   `breaking` and names the version (an opaque path names the contract but
   stays `unknown`).
6. **Owners / consumers**: the dataset's `owners`; a consumer is affected
   when it declares no `columns` or one of them is affected, with its own
   severity.

Severity: `breaking` > `unknown` > `additive` > `none`; the report's
severity is the maximum. Surfaces: `faucet plan --impact [--depth N]
[--sample F]` (human + `--json`), `POST /v1/plan { impact: true }` (`Plan`,
viewer+ — read-only, audited `plan`), the console's Owners & consumers
section on a dataset.

### What this deliberately does not do

- **No notification on approval.** The issue sketches messaging owners when
  a breaking change is approved; there is no approvals workflow in
  faucet-stream today, so the report *names* the owners and stops. When an
  approvals surface exists, `impact.owners` is the recipient list.
- **No SQL parsing of external tools.** Consumers are declared, not inferred.
- **No column-level lineage through opaque transforms.** `unknown` is the
  honest answer; a `--sample` gives the exact planned schema for the row
  itself, and downstream opacity is a property of the recorded edges.

## Alternatives considered

- **Policy as masking rules with a `deny` action.** Rejected: masking is
  per-pipeline and value-level; a policy is destination-level and must be
  applied centrally (`--policy`, `serve --policy`) to configs it does not own.
- **Runtime-only enforcement.** Rejected: a page refused after the source
  was read is a leak in the log at best. The static pass is the product; the
  backstop catches what no contract declared.
- **Impact from OpenLineage events in an external server.** Rejected for
  v1: the catalog already holds the same edges with column lineage, needs no
  extra infrastructure, and answers the question at `plan` time.

## Compatibility

Additive throughout: new optional config keys (`policy`, `attributes`,
`catalog.datasets`), new defaulted trait methods (`RunHistory::catalog_annotate`),
new defaulted struct fields (`CatalogDataset.owners/consumers`,
`RowSnapshot.contract`, `TemplateSummary.warnings`), one new `FaucetError`
variant (the enum is `#[non_exhaustive]`), two new permissions (`Plan`,
`CatalogAnnotate`) and two new audit actions. `policy` is a Cargo feature on
`faucet-core` (implies `masking`), forwarded by the umbrella and in the CLI
default build.

# RFC 0008 — A kind-aware template registry

*Make the template registry store the Template Hub's two halves — `source-template` and `sink-template` — as first-class kinds that compose at trigger time, keep the complete `pipeline` document as an explicit third kind, and deprecate registering a document that does not say which it is.*

| | |
|---|---|
| **RFC** | 0008 |
| **Title** | Kind-aware template registry |
| **Status** | Accepted |
| **Authors** | faucet-stream maintainers |
| **Related issues** | #678 (this RFC's implementation) · #679 (deployment overlay — the open question below) · #571 (Template Hub) · #444 (template registry) · #589 / RFC 0006 (hosting + sync) · #648 (parameter-space suites) · #677 (hosted catalogs) · epic #38 |
| **Related ADRs** | — |

## Summary

The registry (#444) stores one thing: a complete pipeline config with a
`params:` block, registered once and triggered by id. The Template Hub (#571)
introduced two other documents — a `kind: source-template` (one system: its
connector, shared transforms, and streams with per-stream write preferences)
and a `kind: sink-template` (one destination and how a stream is addressed) —
that compose into a pipeline at run time from files on disk.

This RFC makes the registry understand all three. A `source-template` and a
`sink-template` register under their own `name`, are validated as what they
are, and a trigger on a source template names the sink template to compose
with. The complete config keeps working as an explicit `kind: pipeline`.
Registering a document with no `kind:` still works, as a pipeline, but is
deprecated: it prints a notice on the CLI and logs a warning on the server.

## Motivation

The hub model is only as useful as the place templates live. Files under
`--hub` work for one machine; the registry is what `faucet serve`, the console,
the MCP tools, and sync origins (RFC 0006) share. Without this RFC the two are
disconnected in the worst way: to put a hub template in the registry you
compose it first (`faucet hub compose --out f.yaml && faucet template register
f.yaml`), which bakes one sink into it — exactly the coupling the hub exists to
remove. A registry of `acme-billing-to-bigquery`, `acme-billing-to-postgres`,
… is the `netsuite-to-bigquery` problem again, one level up.

The registry also could not tell the documents apart. `register` parsed
everything as a `PipelineConfig`, so a hub document was a validation error
with a hint to compose it; the console's trigger form, the suites, and MCP had
no notion of a sink to pick. And the kind-less complete config, being the only
shape, was implicit: nothing in the document said what it was, so nothing
could ever be added beside it without a guess.

The platform's direction (#677) is a hosted catalog people browse and
register from. That catalog is made of source and sink templates, not
pre-composed pipelines. The registry has to hold them natively for that to be
more than a file download.

## Guide-level explanation

Three kinds, told apart by the document's `kind:` line:

| `kind:` | What it is | How it runs |
|---|---|---|
| `source-template` | one system — connector, shared `transforms`, `streams[]` with `write` preferences and `primary_keys` | composed with a registered `sink-template` at trigger time |
| `sink-template` | one destination — connector, `per_stream` addressing, `write_mode_aliases` | never alone; named as the `sink` of a source template's run |
| `pipeline` | a complete config with `params:` | alone, as today |

Registering is the same verb for all three:

```bash
faucet template register hub/source-templates/acme-billing.yaml --launch   # id = acme-billing
faucet template register hub/sink-templates/bigquery.yaml --launch         # id = bigquery
faucet template register pipeline.yaml --launch                            # kind: pipeline
faucet template list --kind sink-template
```

Running names the sink for a source template:

```bash
faucet template run acme-billing --sink bigquery --sink-version stable \
  --param api_token="$T" --param bq_project=my-project
```

```http
POST /v1/templates/acme-billing/runs
{ "sink": "bigquery", "sink_version": "stable", "params": { … } }
→ 202 { "template_id": "acme-billing", "template_version": 1,
        "sink_template": "bigquery", "sink_template_version": 1,
        "streams": [ { "stream": "bills", "requested": ["overwrite","upsert"], "chosen": "overwrite", "key": ["id"] }, … ],
        "params": { … } }
```

The MCP `run_template` tool takes the same `sink` / `sink_version` pair. The
console lists every template with a kind pill and a kind filter; a source
template's page has a sink dropdown whose params join the form; a sink
template's page lists the source templates it can be composed with.
`faucet template test` suites for a source template name a `sink:` (and
`sink_select:`) and every case exercises the composed pipeline.

What stays the same: versions, channels, launching, rollback, deprecation,
the launch log, RBAC, audit, sync origins. Each half has its own release
state, so a sink template can be launched, promoted, and rolled back
independently of the sources that use it.

## Reference-level explanation

### The record carries a kind

`TemplateRecord`, `TemplateSummary`, and `TemplateDraft` gain
`kind: TemplateKind` (`SourceTemplate | SinkTemplate | Pipeline`), serialised
as the document's own spelling. The field has a serde default of `pipeline`,
so a row written before this RFC reads as a pipeline; the SQL backends store
the encoded record in the existing `body` column, so there is no DDL change
and no migration.

### Register dispatches on kind

`templates::register` reads `kind:` first:

- `source-template` / `sink-template`: deserialise into the hub type, run its
  `validate()`, then the publishability lint (`hub::catalog::lint_*`) as a
  registry gate. A literal credential, a private hostname, or an undeclared
  `${param.*}` is refused; a missing `description` is not (it is a catalog
  nit, and the template's own description is used when the request has
  none). The id **is** the template's `name` — compose uses the name for the
  pipeline name and state keys, so the two must not diverge; an explicit
  `--id` must match.
- `pipeline` / absent: the existing structural validation of a
  placeholder-bound copy (`expand` + per-row transform compilation, or the
  topology checks). A missing `kind:` emits the deprecation notice.

A template's kind is fixed for its id: a register whose kind differs from the
stored one is refused, because every pairing that names the id would break at
trigger time.

### One trigger path

`templates::materialize_for_run(store, id, version, &SinkChoice, …)` is the
single function the CLI, HTTP, MCP, and suites call. It fetches the version
and dispatches on the record's kind:

- `pipeline`: a supplied `sink` is an error ("takes no sink"); otherwise the
  existing `materialize`.
- `source-template`: `sink.id` is required (the error names `--sink` / `sink`
  and the `list --kind sink-template` command); the sink's version is resolved
  through the same `resolve_version` as the source's (so `stable` means the
  launched sink build); `materialize_pair` checks both kinds, composes with
  `hub::compose`, and binds the merged params.
- `sink-template`: an error pointing at the source side.

`MaterializedConfig` gains `sink_id`, `sink_version`, and `streams:
Vec<StreamPlan>` (the per-stream write-mode plan). The HTTP response and the
MCP result echo them; the run is labelled `sink_template` /
`sink_template_version` beside `template` / `template_version`. The composed
pipeline's `name` is the source template's, so state keys are
`{source}::{stream}` and survive swapping the sink — the property the hub was
built for.

The plain `materialize` refuses hub kinds with the same actionable messages,
so no older caller can run a half.

### Surfaces

- CLI: `template run --sink <id> [--sink-version <sel>]`, `template list
  --kind <k>` (+ a KIND column), `template show` prints the kind, `template
  register` prints the deprecation notice for a kind-less document.
- HTTP: `TriggerBody.sink` / `sink_version`; `TriggerResponse.sink_template`
  / `sink_template_version` / `streams`; `GET /v1/templates?kind=`;
  `TemplateSummary.kind`. `docs/openapi.yaml` updated.
- MCP: `run_template` `sink` / `sink_version`; tool descriptions name the
  kinds.
- Suites: `SuiteFile.sink` / `sink_select`; `Target::Registered.sink`,
  `Target::Document.sink_body`; the effective document of a source template
  is the composition, so `auto:` sweeps the merged param surface.
- Console: kind pills + filter, sink selector on a source template's trigger
  form (the sink's params join the form, tagged), a pairings list instead of
  a trigger form on a sink template's page, kinds in the register editor's
  placeholder.
- `PipelineConfig.kind: Option<ConfigKind>` accepts `kind: pipeline` on any
  config, so a complete config can carry its kind through `faucet run` /
  `validate` / `fmt` unchanged. `schemas/faucet.schema.json` regenerated.

### Deprecation

Kind-less registration is deprecated, not removed: the CLI prints
`deprecated: … add kind: pipeline`, the server logs a `warn!`, every sync
origin keeps working. The removal is a later, separately announced step;
this RFC only makes the implicit explicit.

## Drawbacks

- Two release states per pairing. A source at `stable` and a sink at
  `stable` may never have been run together. `sink_version` pins the sink
  when that matters, and a suite with `sink:` is how a pairing is proven
  before launch — but the operator now has two levers where they had one.
- The registry gains a lint the file path did not have. A hub template that
  `faucet run --source f.yaml` accepts (a literal token, say) is refused by
  `register`. That is the point — a shared registry is a shared place — but
  it is a difference.
- Composition happens at trigger time, on every trigger. It is pure and
  cheap (two documents, no I/O beyond the two fetches), but a bug in
  `compose` now affects every source-template run rather than one
  `hub compose` invocation.

## Rationale and alternatives

- **Keep composing to files.** Rejected: it bakes a sink into every registered
  template and multiplies ids by destinations; the console/MCP/suites could
  never offer a sink choice.
- **Store the pairing as its own record.** Rejected: a pairing has no
  independent content — it is the two halves and their versions — and a
  stored pairing would need its own launch/deprecate lifecycle for nothing.
  The run labels record which builds were composed.
- **Remove kind-less registration now.** Rejected in favour of deprecation:
  every existing registry row and sync origin is kind-less; a warning costs
  nothing and a hard error would break every deployment on upgrade.
- **Drop `kind: pipeline` and make everything source × sink.** Rejected: a
  topology graph, a multi-source merge, or a one-off with a hand-tuned matrix
  is not a single-source stream list. The complete config stays a first-class
  kind, made explicit.

## Prior art

Airbyte and Meltano separate source and destination definitions and pair them
per connection; dbt packages separate models from targets/profiles. Neither
versions the two halves independently with a launch log; both compose at
run time.

## Unresolved questions

- ~~A **deployment overlay** for composed runs~~ — resolved by #679: a fourth
  kind, `kind: deployment`, holds only operational blocks (`state`, `dlq`,
  `notifications`, `sla`, `resilience`, `execution`, `delivery`, `schedule`,
  plus per-stream `sla` / `dlq` / `delivery`). It is registered like any
  template or passed inline, applied last by `Composition::apply_overlay`, and
  can never change connectors or streams (those keys are refused). Every
  trigger surface takes it (`--overlay`, HTTP / MCP `overlay`, a suite's
  `overlay:`, the console's deployment selector).
- Whether a sink template should be able to declare which write modes it
  guarantees (beyond the registry's connector capabilities) for a hosted
  catalog to display without composing.

## Future possibilities

- The hosted catalog (#677): browse a remote hub, register a source template
  from it with one action, and the console already offers every registered
  sink.
- Pairing suites in CI: `faucet template test suite.yaml --select prod
  --sink-select prod` as the promotion gate for a sink template.

## Related

- RFC 0006 — Template hosting and sync
- `docs/book/src/cookbook/template-hub.md`, `docs/book/src/cookbook/templates.md`
- `cli/src/templates/store.rs` (`register`, `materialize_for_run`, `materialize_pair`)

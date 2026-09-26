# Data Movement Catalog

The **Data Movement Catalog** is faucet's first-party, persistent record of
everything your pipelines touch. Where a run's logs and metrics describe *one*
run, the catalog accumulates **across** runs:

- **Datasets** — every source and sink a pipeline has read or written, keyed
  by a canonical, credential-redacted dataset URI.
- **Schema timelines** — the observed record schema of each dataset, stored as
  a deduplicated timeline: a new version is appended only when the schema
  actually changes, together with a computed diff (added / widened /
  incompatible / removed columns).
- **Volume & freshness** — per-run record counts and the last-success
  timestamp for each dataset.
- **Lineage edges** — which dataset feeds which, with per-edge column lineage
  whenever the transform chain is expressible (the same derivation the
  [OpenLineage emitter](./lineage.md) uses).
- **Column profiles** — for a pipeline with a [`profiling:`](./profiling.md)
  block, each run's learned per-column statistics (null rate, distinct
  estimate, numeric / string summaries, top values) and drift findings,
  recorded on the sink dataset.
- **Provenance** — every catalog row is linked to the run that produced it
  (the serve run id under `faucet serve`, the invocation run id otherwise).

After a few weeks of runs the catalog answers the operational questions that
otherwise require spelunking logs: *what's the schema history of this table?*,
*what feeds it?*, *when did this pipeline last land data, and how much?*

Recording is **observational only**: a catalog write never fails or slows a
run — a broken store logs a warning and the pipeline continues.

> Requires a build with the `catalog` Cargo feature (included in
> `--features full`), plus `serve-history-sqlite` / `serve-history-postgres`
> for persistent stores.

## Recording from `faucet run` / `schedule` / `replicate`

Add a top-level `catalog:` block naming the store:

```yaml
# cli/examples/csv_to_jsonl_with_catalog.yaml
version: 1
name: csv_to_jsonl_with_catalog

catalog:
  url: sqlite:./faucet-catalog.db
  sample_records: 100        # schema-inference sample per side (default 100)

pipeline:
  source: { type: csv,   config: { path: ./data/input.csv } }
  sink:   { type: jsonl, config: { path: ./out/records.jsonl } }
```

`url` accepts `sqlite:<path>`, a `postgres://…` URL, or `memory`
(process-lifetime only — for tests). Every successful **root** invocation then
folds its observations into the store: dry runs, `--limit` runs, shard
executions, and cancelled runs are excluded so partial or synthetic volumes
never pollute the history.

## Recording from `faucet serve`

`faucet serve` needs no config block: every run is recorded into the server's
`--history` backend automatically, attributed to its serve run id. Use a
persistent history for a persistent catalog:

```bash
faucet serve --history sqlite:./faucet-catalog.db --auth-token "$TOKEN"
```

The run-record retention window does **not** purge the catalog — the
accumulated history is the point. Only per-dataset volume points are capped
(newest 500 kept).

## Browsing: CLI

```bash
faucet catalog datasets --config pipeline.yaml            # list (newest first)
faucet catalog datasets --config pipeline.yaml --kind csv --q users
faucet catalog show 3f2a9c1e0b7d4a55 --config pipeline.yaml
faucet catalog lineage --config pipeline.yaml --root 3f2a9c1e0b7d4a55 --depth 3
```

`show` accepts a unique prefix of the dataset id. Every subcommand takes
`--json` for machine-readable output. `faucet schema catalog` prints the
`catalog:` block's JSON Schema. `faucet catalog annotate <id> --owner … --consumer …`
sets a dataset's owners and declared consumers (the one write; see
[change impact analysis](./impact.md)).

`show` renders the schema timeline with diff markers:

```text
schema timeline (2 versions):
  v1  2026-07-01T02:00:04Z  2 column(s)  run 0197e6…
  v2  2026-07-06T02:00:03Z  3 column(s)  run 0197f1…  [+email]
```

## Browsing: HTTP API + web console

Three read-only endpoints (viewer-readable under RBAC):

| Endpoint | Returns |
|---|---|
| `GET /v1/catalog/datasets` | Paginated dataset list (`kind`, `q`, `limit`, `cursor` filters) |
| `GET /v1/catalog/datasets/{id}` | Current schema, schema timeline (with diffs), recent volume points, upstream/downstream edges, column profiles (`profile.latest` + `profile.history`) |
| `GET /v1/catalog/lineage` | The edge graph (`root` + `depth` for a bounded slice) |
| `POST /v1/catalog/datasets/{id}/consumers` | Merge owners / declared consumers into a dataset (operator+) — see [impact analysis](./impact.md) |

The embedded [web console](./web-console.md) adds a **Datasets** browser
(filterable list → per-dataset detail with the schema timeline, volume bars,
and the column-profile table with null-rate sparklines and drift markers) and
a **Lineage** graph view (layered SVG; click a node for its detail).

## Owners & consumers

A dataset carries declared **owners** and **consumers** (#707): who is
responsible for it and what reads it besides faucet pipelines (those are
consumers automatically, through the lineage edges). Declare them in the
`catalog.datasets:` block (merged after every run that touches the dataset),
with `faucet catalog annotate`, over `POST /v1/catalog/datasets/{id}/consumers`,
or in the console's **Owners & consumers** section. They are what
[`faucet plan --impact`](./impact.md) names when a change reaches the dataset.

## Dataset identity & cardinality

The catalog key is the connector's dataset URI after two normalizations:

1. **Credentials are redacted** (`postgres://user:***@host/db/table`).
2. **`${now.*}`-derived path segments are folded back to their tokens** — a
   sink writing `./out/dt=${now.date}/part.jsonl` catalogues as one dataset
   (`…/dt=${now.date}/part.jsonl`), not one per day.

Matrix rows that resolve to the same URI converge on one dataset with one
provenance trail per run.

## Schema observation

Schemas are inferred from a bounded sample of the records actually read
(source side, pre-transform) and written (sink side, post-transform) — the
same samplers the lineage emitter uses, capped by `sample_records`. The
timeline dedupes by a content hash, so re-running an unchanged pipeline never
grows it; a real change appends one version whose `diff` is computed with the
same engine as [schema-drift handling](./schema-drift.md).

## Column profiles

A pipeline with a [`profiling:`](./profiling.md) block records every run's
column profile on the **sink** dataset it wrote — the latest profile with its
drift findings plus the newest 100 runs (the detail read returns 30). `faucet
catalog show <id>` prints a `profile:` section and the console renders the
per-column table. The catalog copy is for browsing: the drift detector reads
its baseline from the pipeline's `state:` store, so `faucet profiling reset`
re-baselines there and the catalog history stays as a record of what each run
looked like.

## Relationship to lineage emission

[OpenLineage emission](./lineage.md) *exports* run events to an external
backend (Marquez, DataHub, …); the catalog is the *first-party store* faucet
keeps for itself. They compose — the catalog's per-edge column lineage matches
the OpenLineage column-lineage facet, and both can be active at once.

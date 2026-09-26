# Change impact analysis

`faucet plan --impact` answers "who breaks if I ship this?" before a change
ships. It takes the schema the row would now write, diffs it against what the
[Data Movement Catalog](./catalog.md) last observed at the sink, and walks the
catalog's lineage graph **downstream** — naming every dataset, pipeline,
contract, owner and declared consumer the change reaches, with a severity
each:

| Severity | Meaning |
|---|---|
| `breaking` | A column something downstream reads is removed, retyped, or renamed |
| `additive` | Only new columns appear (nothing downstream reads them yet) |
| `unknown` | The path is opaque — an edge carries no column lineage, so the change may or may not reach it |
| `none` | Nothing it reads changes (such datasets are left out) |

## Prerequisites

- A `catalog:` block, and at least one recorded run of the row (the row's
  sink is found through the lineage edge its last run recorded; a row that
  has never run reports "no recorded run" and stops).
- Downstream pipelines recorded into the **same** catalog store — that is
  what makes their edges, contracts and consumers visible.

## Running it

```bash
faucet plan pipeline.yaml --impact                        # planned schema from the catalog + the transform chain
faucet plan pipeline.yaml --impact --sample fixture.jsonl # exact planned schema from a sample
faucet plan pipeline.yaml --impact --depth 3 --json       # bounded walk, machine-readable
```

```text
Plan for row `default`:
  source:   csv
  sink:     csv  (write_mode: append)
  …
  impact: breaking (2 affected dataset(s), planned schema from the sample)
    sink dataset: file:///data/a.csv (last run 0199…)
    schema delta: -email
    [breaking] depth 0 file:///data/a.csv (written by a / default)
      email: removed
      owners: team-a
    [breaking] depth 1 file:///data/b.jsonl (written by b / default)
      contact reads email (removed)
      contract v7 of b / default declares: contact
      owners: team-b
      consumer contacts-dashboard [breaking] (dashboard) → #bi
    owners to notify: team-a, team-b
```

**Where the planned schema comes from** (`planned_from`): the `--sample`'s
output after the row's transforms (`sample`, exact); else the catalog's last
*source* schema pushed through the row's transform chain by column lineage
(`lineage` — each output column typed like its single origin); else
`unknown` (an opaque chain and no sample — pass `--sample`).

**The delta** lists `added` / `removed` / `retyped` columns; a removed +
added pair that the row's own `rename_field` / `rename_keys` maps is
reported as a **rename** (`email→mail`) — a rename breaks readers of the old
name just like a drop, but the report says what happened.

**The walk** follows source → sink edges up to `--depth` hops (default 5).
Each edge's recorded column lineage (`{out: [in…]}`, the same derivation
OpenLineage emission uses) says which downstream columns read an affected
one; only non-additive changes flow, and a dataset nothing it reads changed
is dropped along with everything past it. An edge with **no** column lineage
— an opaque transform (`flatten`, `explode`, `keys_case`, `sql`, `wasm`,
custom), or an edge recorded before column lineage existed — makes that
dataset and everything past it `unknown`, never a false `none`.

**Contracts**: when a downstream pipeline's last recorded config declares a
[data contract](./contracts.md) over an affected column, the dataset
escalates to `breaking` and the report names the contract version and
fields. Config snapshots are recorded by every runtime (`run`, `schedule`,
`mirror`, `serve`).

## Owners and consumers

Pipelines that read a dataset are consumers automatically (lineage). What
the catalog cannot see — dashboards, models, exports — is **declared**, and
so is who owns a dataset:

```yaml
catalog:
  url: sqlite:./faucet-catalog.db
  datasets:
    - dataset: file://./out/records.jsonl      # canonical URI as `faucet catalog datasets` prints it, or the id
      owners: [data-platform]
      consumers:
        - name: records-dashboard
          kind: dashboard                       # free-form
          contact: "#analytics"
          columns: [id, name]                   # what it reads; empty = every column
```

The block is merged into the catalog after every run that touches the
dataset. The same annotation can be made over the CLI or the API:

```bash
faucet catalog annotate 3f2a9c1e --config pipeline.yaml \
  --owner team-b --consumer contacts-dashboard=dashboard --contact "#bi" --columns contact
```

```http
POST /v1/catalog/datasets/{id}/consumers      (operator+, audited catalog.annotate)
{ "owners": ["team-b"],
  "consumers": [{ "name": "contacts-dashboard", "kind": "dashboard", "contact": "#bi", "columns": ["contact"] }],
  "replace": false }
```

Owners replace the list when given; consumers are upserted by name
(`replace: true` drops the unlisted ones first). The web console shows and
edits both on a dataset's page (**Owners & consumers**). A consumer that
declares `columns` is affected only when one of them is; one that declares
none is affected by any change. `impact.owners` is every owner of an
affected dataset — the list to notify.

## Over HTTP

`POST /v1/plan { config, sample?, impact: true, depth? }` returns the same
report (`Plan` permission — viewer-readable, nothing is written; audited
`plan`). It uses the server's own catalog, so a control plane that runs the
pipelines can answer for all of them.

## Reference

- [`faucet plan`](../reference/cli.md#plan) · [`faucet catalog annotate`](../reference/cli.md#catalog)
- [`catalog.datasets`](../reference/config.md#catalog)
- [`POST /v1/plan`](../reference/http-api.md#post-v1plan) · [`POST /v1/catalog/datasets/{id}/consumers`](../reference/http-api.md#get-v1catalog-data-movement-catalog)
- [RFC 0010](https://github.com/faucet-hq/faucet-stream/blob/main/rfcs/0010-policies-and-impact.md)

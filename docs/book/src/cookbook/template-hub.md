# Template Hub: source × sink templates

A pipeline config bundles two very different kinds of knowledge: **how to
read a system** — auth, pagination, incremental cursors, which endpoints
become which tables, how records are shaped — and **where to put the
result**. The first is hard-won and reusable; the second is a handful of
credentials. Bundling them means a `netsuite-to-bigquery` template is useless
to a Snowflake or Postgres user.

The Template Hub splits them:

- a **`source-template`** owns the source side: one connector, its shared
  `transforms`, and a list of **streams** (tables), each declaring the write
  semantics it needs;
- a **`sink-template`** owns the destination: one connector and how a stream
  is addressed (`per_stream`).

Any source × any sink composes into an ordinary pipeline at run time:

```bash
faucet run --source acme-billing --sink bigquery \
  --param api_token="$ACME_TOKEN" --param bq_project=my-project --param bq_sa_key="$BQ_SA_KEY"

faucet run --source acme-billing --sink jsonl     # the same source, validated locally first
faucet run --source acme-billing --sink postgres  # real upsert/overwrite semantics
```

The repository ships the **layout and tooling** under
[`hub/`](https://github.com/faucet-hq/faucet-stream/tree/main/hub): sink templates
for BigQuery, PostgreSQL, SQLite, and JSON Lines, plus two example source
templates (`example-csv` runs offline; `example-rest-api` is a skeleton to copy).
Real source templates belong in a shared catalog — a repository with the same
layout that you point `--hub` (or `$FAUCET_HUB`) at — so the engine repo does
not become a vendor directory. The generated
[source × sink matrix](../reference/template-hub-matrix.md) renders whatever
catalog the docs are built from, with a copy-paste command per pairing.

## A source template

```yaml
kind: source-template
name: acme-billing               # hub id, and the composed pipeline's `name:`
description: Acme Billing — invoices, payments, and customers
tags: [finance, billing]
params:
  client_id:     { type: string, required: true, secret: true }
  client_secret: { type: string, required: true, secret: true }
source:                          # the connector every stream shares
  type: rest
  config:
    base_url: https://api.acme-billing.example/v1
    auth: { type: oauth2, config: { token_url: …, client_id: "${param.client_id}", client_secret: "${param.client_secret}", scopes: […] } }
    records_path: $.data[*]
    pagination: { type: NextLinkInBody, next_link_path: $.page.next }
transforms:                      # shaping that travels with the source — runs before EVERY sink
  - { type: keys_case, config: { mode: snake } }
streams:
  - name: bills                  # → matrix row id, state-key suffix, and the destination table
    source: { config: { path: /bills } }          # merged onto source.config
    transforms: [{ type: json_encode, config: { fields: [line_items, vendor] } }]
    primary_keys: [id]
    write: [overwrite, upsert]   # preference order
  - name: transactions
    source: { config: { path: /transactions, replication_method: { type: Incremental, … } } }
    primary_keys: [id]
    write: [upsert, append]
```

| Field | Purpose |
|---|---|
| `name` | Hub id (`^[a-z0-9][a-z0-9_-]*$`, equal to the file stem) **and** the composed pipeline's `name:` — so per-stream state keys are `{source}::{stream}` and bookmarks survive swapping the sink. |
| `params`, `auth` | Same grammar as a pipeline's `params:` / `auth:` blocks. Merged with the sink template's at compose time; a name declared by both with different specs is an error. |
| `source` | The connector every stream reads through. Shared transforms go in the top-level `transforms`, not here. |
| `sources` | Additional named connectors for streams that read a second endpoint family (a reports API beside the entity API). A stream picks one with `source.ref`. |
| `transforms` | The pipeline layer — runs before any sink, so every destination receives the same record shape (`keys_case`, `json_encode` for nested fields, `cast`, …). |
| `contract` | Optional data contract, passed through as `pipeline.contract`. |
| `streams[]` | One table each: `name`, `source.config` override, per-stream `transforms` (the matrix-row layer), `primary_keys`, `write`, and optionally `parent` / `parent_key` (per-record fan-out) and `inherit_transforms: false`. |

`write` is a single mode or an **ordered preference list**: `overwrite` for a
full refresh, `upsert` (needs `primary_keys`, which become the sink's `key`),
`append`, `delete`. Default `append`.

## A sink template

```yaml
kind: sink-template
name: bigquery
description: Google BigQuery — one table per stream
params:
  bq_project: { type: string, required: true }
  bq_dataset: { type: string, default: raw }
  bq_sa_key:  { type: string, required: true, secret: true }
sink:
  type: bigquery
  config:
    project_id: "${param.bq_project}"
    dataset_id: "${param.bq_dataset}"
    auth: { type: service_account_key, config: { json: "${param.bq_sa_key}" } }
per_stream:
  table_id: "${stream}"          # rendered once per stream
```

`per_stream` is the addressing rule: every key is copied into the sink config
of each stream's row with `${stream}` (the stream name) and `${source}` (the
source template's name) substituted — `table_id: "${stream}"` for a
warehouse, `path: "${param.out_dir}/${source}/${stream}.jsonl"` for files.
`write_mode` and `key` are **never** written in a sink template: the composer
injects them per stream.

### Satisfying a mode by construction

A JSON Lines file rewritten on every run *is* a full refresh, even though the
`jsonl` connector only knows `append`. A sink template can say so:

```yaml
sink:
  type: jsonl
  config: { append: false }
per_stream:
  path: "${param.out_dir}/${source}/${stream}.jsonl"
write_mode_aliases:
  overwrite: append              # a stream that wants overwrite runs as append here
```

The composer records the substitution (`overwrite→append` in `faucet hub
check`) and validates it against the connector registry: the target mode must
be one the connector supports, an alias for a natively supported mode is
refused as redundant, and keyed modes (`upsert`, `delete`) cannot be aliased —
only a sink that dedups by key can honour them.

## Composition and the compatibility matrix

`faucet run --source X --sink Y` (and `faucet validate --source X --sink Y`,
`faucet hub compose`) build an ordinary config document:

```yaml
version: 1
name: acme-billing                           # the source's name
params: { …merged… }
pipeline:
  sources: { default: <source-template.source> }
  sinks:   { default: <sink-template.sink> }
  transforms: <source-template.transforms>
matrix:
  - id: bills
    source: { ref: default, config: { path: /bills } }
    sink:   { ref: default, config: { table_id: bills, write_mode: overwrite } }
    transforms: [{ type: json_encode, … }]
  - id: transactions
    source: { ref: default, config: { path: /transactions, … } }
    sink:   { ref: default, config: { table_id: transactions, write_mode: upsert, key: [id] } }
```

For each stream the composer walks its `write` list and picks the **first
mode the sink supports** — natively (from the connector registry's write-mode
capabilities) or through an alias. A stream with no viable mode fails the
pairing with a per-stream message naming both sides:

```
source-template 'acme-billing' cannot compose with sink-template 'plain' — 2 stream(s) have no viable write mode:
  - bills: needs overwrite|upsert; sink 'jsonl' supports only append
  - transactions: needs upsert|append; …
```

Everything after composition is the existing run path — params binding,
secret resolution, `expand` (with its write-mode × sink gate), the executor —
so a composed pipeline inherits every guarantee a hand-written one has.

`faucet hub matrix` renders the whole catalog: `--format table` for the
terminal, `markdown` for the docs page, `json` for `hub/index.json`.

## Commands

```bash
faucet hub list      [--hub DIR] [--json]
faucet hub check     --source X --sink Y [--json]          # per-stream write modes; exit≠0 if incompatible
faucet hub compose   --source X --sink Y [--out FILE|--json]
faucet hub matrix    [--format table|markdown|json] [--out FILE]
faucet hub lint      [--hub DIR] [FILE…]                   # publishability lint
faucet run           --source X --sink Y [--param k=v] …    # compose + run
faucet validate      --source X --sink Y [--show-composed]  # compose + validate offline
faucet schema source-template | sink-template
```

`--source` / `--sink` take a **path** or a **hub id**, resolved as
`<hub>/source-templates/<id>.yaml` and `<hub>/sink-templates/<id>.yaml`. The
hub directory is `--hub`, else `$FAUCET_HUB`, else `./hub`.

A composed config is just a config: `faucet hub compose … --out f.yaml` and
register it (`faucet template register f.yaml`), hand-edit it, or commit it.

## Publishing rules

`faucet hub lint` enforces what a public template must satisfy, and the
repository's catalog test runs it plus a full composition of every pairing:

- credentials are `${param.NAME}` (`secret: true`) or `${env:…}` /
  `${secret:…}` — never a literal value; a param whose name looks like a
  credential must be marked `secret`, and a secret param has no default;
- no private infrastructure or placeholder text (`.internal`, managed-DB
  hostnames, `REPLACE_ME`);
- a `description`; `name` equal to the file stem; unique stream names; every
  `${param.*}` reference declared.

See also: [Parameters & pipeline templates](./templates.md) (the registry a
composed pipeline can be registered into), [Write modes / upsert](./upsert.md),
[Transforms](./transforms.md).

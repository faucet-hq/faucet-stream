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

The engine repository ships the **layout and tooling** under
[`hub/`](https://github.com/faucet-hq/faucet-stream/tree/main/hub): sink templates
for BigQuery, PostgreSQL, SQLite, and JSON Lines, plus two example source
templates (`example-csv` runs offline; `example-rest-api` is a skeleton to copy).
Real source templates live in the **public hub**,
[faucet-hq/template-hub](https://github.com/faucet-hq/template-hub) — the
default `--hub`, browsable at [faucet-hq.github.io/hub](https://faucet-hq.github.io/hub) —
so the engine repo does not become a vendor directory. The generated
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
hub is `--hub`, else `$FAUCET_HUB`, else `./hub` when that directory exists,
else the **public hub** (next section).

## The public hub

The shared catalog lives at
[github.com/faucet-hq/template-hub](https://github.com/faucet-hq/template-hub)
and is browsable at [faucet-hq.github.io/hub](https://faucet-hq.github.io/hub).
It is the default hub, so with no `./hub` checkout and no `--hub`:

```bash
faucet hub list                                   # fetches github:faucet-hq/template-hub (cached)
faucet run --source acme-billing --sink bigquery --param api_token="$T" …
```

A remote hub is any GitHub repository laid out like `hub/`:
`--hub github:owner/repo[@ref][/path]` or a `https://github.com/…[/tree/ref/path]`
URL. The CLI resolves the ref to a commit with one API request, downloads the
catalog into `~/.cache/faucet/hub/<repo>/<ref>/<commit>/` the first time, and
reuses the snapshot until the ref moves. Offline, the last snapshot is used
with a warning (`FAUCET_HUB_OFFLINE=1` skips the network altogether); it never
falls back to an empty catalog. `GITHUB_TOKEN` (or `FAUCET_GITHUB_TOKEN`) is
sent when set — needed for a private catalog, and it lifts the anonymous API
rate limit.

### Mirror the hub into your server

`faucet serve` pulls the catalog into its template registry with a sync file
([hosting templates](./templates.md#hosting-templates-in-a-repo-or-bucket-sync)),
so the console's Templates view lists every hub template with a kind pill, a
source template's page offers every registered sink in its trigger form, and
the **Compatibility** grid (`GET /v1/templates/matrix`) shows which pairings
work:

```yaml
# cli/examples/templates/hub-sync.yaml
version: 1
origins:
  - name: hub
    source:
      type: github
      config: { repo: faucet-hq/template-hub, paths: [source-templates, sink-templates] }
    launch: always
    interval_secs: 3600
```

```bash
faucet serve --history sqlite:./faucet.db --templates-sync cli/examples/templates/hub-sync.yaml
```

`paths` reads both catalog directories as one origin. A hub's source and sink
names share the registry's id namespace, so a stem may appear in only one of
them.

### Publish a template

Registering a template in the public hub is a pull request to the catalog
repository — the lint is the review bar, and CI composes every pairing. The
website's **Publish** button opens a pre-filled new-file form; or copy the
closest existing file and follow
[CONTRIBUTING](https://github.com/faucet-hq/template-hub/blob/main/CONTRIBUTING.md).
Your own organisation's templates can live in a private repository with the
same layout: point `--hub github:org/catalog` (with `GITHUB_TOKEN`) or a sync
origin at it.

## Registering hub templates

The [template registry](./templates.md) stores source and sink templates as
first-class kinds — there is no need to compose first. Register each file
(its id is its `name`), then run any pairing by id; the server composes at
trigger time, so a new sink template is immediately usable with every
registered source template:

```bash
faucet template register hub/source-templates/acme-billing.yaml --launch
faucet template register hub/sink-templates/bigquery.yaml --launch
faucet template register hub/sink-templates/postgres.yaml --launch
faucet template list --kind source-template
faucet template run acme-billing --sink bigquery --param api_token="$T" --param bq_project=p --param bq_sa_key="$K"
faucet template run acme-billing --sink postgres --param api_token="$T" --param pg_url="$PG"
```

Over HTTP the trigger is `POST /v1/templates/acme-billing/runs` with
`{"sink": "bigquery", "params": {…}}`; the console's template page offers the
registered sink templates in a dropdown. Registration runs the same
publishability lint as `faucet hub lint`, so a literal credential never lands in
a shared registry. A sync origin ([hosting templates](./templates.md#hosting-templates-in-a-repo-or-bucket-sync))
may hold hub templates too — a repository laid out like `hub/` syncs straight
into the registry.

A composed config is also just a config: `faucet hub compose … --out f.yaml` to
inspect it, hand-edit it, or commit it as a complete `kind: pipeline` template.

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

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
faucet run --source acme/billing --sink faucet-hq/bigquery \
  --param api_token="$ACME_TOKEN" --param bq_project=my-project --param bq_sa_key="$BQ_SA_KEY"

faucet run --source acme/billing --sink faucet-hq/jsonl     # the same source, validated locally first
faucet run --source acme/billing --sink faucet-hq/postgres  # real upsert/overwrite semantics
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
name: billing
owner: acme                      # hub id acme/billing: the composed pipeline's `name:`
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
| `name` | Short name (`^[a-z0-9][a-z0-9_-]*$`, equal to the file stem). With `owner`, the hub id is `owner/name`; the id is the composed pipeline's `name:` — so per-stream state keys are `{id}::{stream}` and bookmarks survive swapping the sink. |
| `owner` | Publisher namespace — the GitHub user or org login the file lives under (`source-templates/<owner>/`). `faucet-hq` for the hub's official templates. |
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
name: acme/billing                           # the source's hub id
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
source-template 'acme/billing' cannot compose with sink-template 'plain' — 2 stream(s) have no viable write mode:
  - bills: needs overwrite|upsert; sink 'jsonl' supports only append
  - transactions: needs upsert|append; …
```

Everything after composition is the existing run path — params binding,
secret resolution, `expand` (with its write-mode × sink gate), the executor —
so a composed pipeline inherits every guarantee a hand-written one has.

`faucet hub matrix` renders the whole catalog: `--format table` for the
terminal, `markdown` for the docs page, `json` for `hub/index.json`.

## Deployment overlays

A composed run still needs the blocks that belong to **neither** template: a
state store for incremental bookmarks, a DLQ, notifications, an SLA. A source
template is published for everyone, so it cannot name *your* state store; a
sink template describes a destination, not an operations policy. Those blocks
live in a third document, a `kind: deployment` overlay, applied last:

```yaml
# ops/prod.yaml
kind: deployment
name: prod
description: Production state, DLQ and paging for composed runs
params:
  state_dsn: { type: string, required: true, secret: true }
state: { type: postgres, config: { connection_url: "${param.state_dsn}" } }
dlq:   { sink: { type: jsonl, config: { path: /var/faucet/dlq/${now.date}.jsonl } } }
notify:
  - { name: oncall, on: [run_failure, sla_breach], channel: { type: pagerduty, config: { routing_key: "${env:PD_KEY}" } } }
sla: { max_staleness_secs: 86400 }
streams:
  invoices: { sla: { min_rows_per_run: 1 } }   # per-stream override
  scratch:  { dlq: null }                       # no DLQ for this one
```

```bash
faucet run      --source acme/billing --sink faucet-hq/bigquery --overlay ops/prod.yaml --param state_dsn=…
faucet validate --source acme/billing --sink faucet-hq/bigquery --overlay ops/prod.yaml
```

An overlay may set only operational blocks — `state`, `dlq`,
`notifications` (alias `notify`), `sla`, `resilience`, `execution`,
`delivery`, `schedule` — and per-stream `sla` / `dlq` / `delivery` under
`streams:`. Anything that would change which connectors run or what the streams
produce (`pipeline`, `matrix`, `source`, `sink`, `transforms`) is refused with a
message saying so, so the shape of a run is always fixed by its two templates.
`state` and `dlq` land under `pipeline.`, the rest at the top level, and an
overlay's value replaces whatever the composition carried. Its `params:` merge
with the templates' (a name declared on both sides must be declared
identically). The run keeps the source's `name`, so its state keys are the
same with or without an overlay, and across sink swaps.

`faucet validate` and `faucet hub check --overlay` print what the overlay set
(`pipeline.state, notifications, matrix.invoices.sla`, …). Two warnings are
worth knowing:

- a source with **incremental** streams composed with **no** `state:` re-reads
  everything each run, and the plan says so, naming the overlay as the fix;
- an overlay whose state store is `memory` loses those bookmarks when the
  process exits.

An overlay passed as `--overlay` is a file, or an id under `<hub>/deployments/`.
In the [template registry](./templates.md) it is a registered template like any
other (`faucet template register ops/prod.yaml`), picked per trigger:
`faucet template run acme/billing --sink bigquery --overlay prod`, HTTP
`{"sink": "bigquery", "overlay": "prod"}` (or an inline mapping), MCP
`run_template {overlay}`, a suite's `overlay:`, or the console's **deployment**
selector. `faucet hub lint` checks an overlay for literal credentials — a
password in a connection URL included — since the values it holds are usually
secrets.

## Commands

```bash
faucet hub list      [--hub DIR] [--json]
faucet hub check     --source X --sink Y [--overlay O] [--json]  # per-stream write modes; exit≠0 if incompatible
faucet hub compose   --source X --sink Y [--overlay O] [--out FILE|--json]
faucet hub matrix    [--format table|markdown|json] [--out FILE]
faucet hub lint      [--hub DIR] [FILE…]                   # publishability lint
faucet run           --source X --sink Y [--overlay O] [--param k=v] …  # compose + run
faucet validate      --source X --sink Y [--overlay O] [--show-composed]  # compose + validate offline
faucet schema source-template | sink-template | deployment
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
faucet run --source acme/billing --sink faucet-hq/bigquery --param api_token="$T" …
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

### Namespaces: `owner/name`

A hundred teams will want their own NetSuite template, so a template's hub id
is **`owner/name`** — the owner being the publisher's GitHub user or org login
— and the catalog is laid out to match: `source-templates/acme/netsuite.yaml`
carries `owner: acme` and is addressed as `acme/netsuite`. The hub's own,
maintained templates are simply the **`faucet-hq`** namespace
(`source-templates/faucet-hq/…`, `owner: faucet-hq`) — owned by the faucet-hq
org exactly like any other namespace, and marked **official**. A bare name is
shorthand for it: `--source netsuite` means `faucet-hq/netsuite`; when there is
none, the CLI lists the community variants instead of guessing.

The full id names the composed pipeline, so state keys are
`acme/netsuite::invoices` and two publishers' templates never collide in a
shared state store or registry (`/` is a legal state-key character; the file
store encodes it). In `per_stream` addressing `${source}` stays the short
name — a table cannot contain `/` — and `${owner}` is available for paths
(`"${param.out_dir}/${owner}/${source}/${stream}.jsonl"`).

Ownership is enforced by the catalog's CI: the first pull request into a
namespace adds `<owner>/OWNERS` with the author's numeric GitHub id, and every
later change must come from a listed id — `faucet-hq/` included, whose
OWNERS lists the hub's maintainers. Nothing lives at the top level.

### Versions: v1, v2, v3 and `stable`

Every merged change to a template's meaning is its next numeric version —
computed from git history by the catalog, never written by the author.
A sidecar beside the template decides what is **stable**: `launch: false`
publishes a version as a preview without moving `stable`; `stable: 3` pins
it. The catalog records all of this in its `index.json`, and the CLI honours
it:

```bash
faucet run --source acme/netsuite --sink faucet-hq/bigquery …          # stable (the default)
faucet run --source acme/netsuite@newest --sink faucet-hq/bigquery …   # the tip
faucet run --source acme/netsuite@3 --sink faucet-hq/bigquery …        # pinned — always the same body
```

A version whose body is not the snapshot's is fetched from the catalog at that
commit and cached, so a pinned run composes the same document every time. A
local directory hub has no history: selectors are an error there.

#### Retiring a version

A version cannot be edited: `@3` must always mean the same bytes, or a pinned
pipeline changes under its owner. There are three supported moves instead:

- **Fix forward.** Commit the fix; it becomes the next version.
- **Roll back.** Re-commit an older body. It becomes a new version with the old
  content, and the sidecar points `stable` at it.
- **Retire.** Deprecate the bad version in the sidecar, with a reason that
  names the replacement:

```yaml
# source-templates/acme/netsuite.faucet.yaml
stable: 4
deprecated:
  2: "drops the invoices stream; use v3+"
  1: "superseded"
```

A deprecated version stays resolvable, so nothing already pinned to it breaks.
It is dropped from everything that *chooses* a version for you:

- `@newest` resolves to the highest version that is **not** deprecated.
- An explicit pin still runs, and prints
  `warning: acme/netsuite v2 is deprecated: drops the invoices stream; use v3+ — stable is v4`.
- The hub website hides deprecated versions behind **Show deprecated versions**.
- A server mirroring the hub never registers a body the catalog marks deprecated.

The catalog's CI refuses a sidecar that deprecates the `stable` version, or a
version that does not exist, so the default selector always lands on a live
version. Un-deprecating is deleting the entry.

### Choosing between variants: stars and trust

When several namespaces publish a template for the same system, the catalog
records facts that help you choose, in `index.json` under each entry's `trust`:

| Signal | What it is |
|---|---|
| `stars` | upvotes (↑) on the template's discussion in the catalog (**Discussions → Templates**). GitHub allows one upvote per account. |
| `updated` / `stable_since` | when the newest version landed, and when the stable one did |
| `open_issues` | open catalog issues labelled `template:<id>` |
| `compatible_sinks` | how many sink templates the source composes with in full |
| `publisher` | how many templates the namespace publishes, and its GitHub account age |

Stars measure popularity, not correctness, so they are one signal among
these. They are never used to pick a template for you. The CLI shows the
signals and orders by them:

```bash
faucet hub list --sort stars       # most starred first; ★ and last-updated columns
faucet hub list --sort updated     # most recently changed first
faucet run --source netsuite --sink faucet-hq/bigquery
# error: no hub template 'netsuite' at the top level or under faucet-hq/, but 2 published one:
#   octo/netsuite (★ 37 · updated 2026-09-12), acme/netsuite (★ 9 · updated 2026-09-22)
#   — pick one with `--source <owner>/netsuite`
```

Variants are ranked official first, then by stars, then by recency. The
[hub page](https://faucet-hq.github.io/hub) shows the same signals on every
card and sorts by them. To star a template, upvote its discussion; to report a
problem, open an issue with its `template:<id>` label.

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

The pull reads the catalog's `index.json` too. When a template's newest body is
a version the publisher deprecated, the sync skips it (the report names the
reason) instead of registering a retired version. Catalog sidecar keys
(`stable`, `deprecated`) are accepted by the sync; they describe catalog
versions, which the registry numbers separately.

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
(its id is its hub id: `acme/billing`, `faucet-hq/bigquery`), then run any pairing by id; the server composes at
trigger time, so a new sink template is immediately usable with every
registered source template:

```bash
faucet template register hub/source-templates/acme/billing.yaml --launch
faucet template register hub/sink-templates/faucet-hq/bigquery.yaml --launch
faucet template register hub/sink-templates/faucet-hq/postgres.yaml --launch
faucet template list --kind source-template
faucet template run acme/billing --sink faucet-hq/bigquery --param api_token="$T" --param bq_project=p --param bq_sa_key="$K"
faucet template run acme/billing --sink faucet-hq/postgres --param api_token="$T" --param pg_url="$PG"
```

Over HTTP the trigger is `POST /v1/templates/acme%2Fbilling/runs` with
`{"sink": "faucet-hq/bigquery", "params": {…}}`; the console's template page offers the
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

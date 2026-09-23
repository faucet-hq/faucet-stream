# Template Hub

The **hub layout** for reusable pipeline templates, split so a template written
once serves every destination. This directory ships the sink templates and two
example source templates; real source templates are contributed to the public
catalog at [faucet-hq/template-hub](https://github.com/faucet-hq/template-hub)
(the default `--hub` when this directory is not present; browse it at
[faucet-hq.github.io/hub](https://faucet-hq.github.io/hub)) rather than
committed to the engine repo.

- **`source-templates/`** — one file per system. Owns the hard part: how to
  talk to the API (auth, pagination, incremental cursors), how records are
  shaped (`transforms`), and which **streams** (tables) it produces, each with
  the write semantics it needs (`write: [overwrite, upsert]`, `append`, …).
- **`sink-templates/`** — one file per destination. Thin: where records land,
  and which config key receives the stream name (`per_stream`).

Any source × any sink composes into an ordinary pipeline config at run time:

```bash
faucet hub list                                            # what's in the catalog at --hub / $FAUCET_HUB / ./hub
faucet hub check   --source example-rest-api --sink bigquery   # per-stream write modes
faucet run         --source example-csv --sink jsonl           # runs offline: ./out/example-csv/*.jsonl
faucet run         --source example-rest-api --sink bigquery \
  --param base_url=https://api.example.com/v1 --param api_token="$API_TOKEN" \
  --param bq_project=my-project --param bq_sa_key="$BQ_SA_KEY"
faucet hub compose --source example-rest-api --sink postgres --out my-pipeline.yaml   # inspect / register / edit
```

The composer resolves each stream's write preference against the sink's real
capabilities (from the connector registry) in order — `overwrite` for a full
refresh, `upsert` on the declared `primary_keys`, `append` — and fails
**per stream**, naming both sides, when nothing fits. The pipeline `name` is
the source template's, so state keys (`{source}::{stream}`) survive a sink
swap.

The matrix with a copy-paste command for every compatible pairing is generated
into the docs site:
[Template Hub — source × sink matrix](../docs/book/src/reference/template-hub-matrix.md)
(and `index.json` here, for machines).

## Contributing a template

Real templates go to the public catalog, under your own namespace
(`source-templates/<your-github-login>/<name>.yaml` with `owner: <login>`; the
hub id is `<owner>/<name>`). Top-level files are the official set. The rules:

1. Copy the closest existing file; keep `name` equal to the file stem
   (`^[a-z0-9][a-z0-9_-]*$`) and `owner` equal to the directory.
2. Credentials are **always** `${param.NAME}` with `secret: true` — never a
   literal, never a private hostname or placeholder value.
3. Declare every stream with its `write` preference and `primary_keys`; use
   `parent` for per-record fan-out and `sources:` + `source.ref` for a second
   endpoint family.
4. Run the checks that CI runs:

```bash
faucet hub lint                                   # publishability lint
faucet hub check --source <yours> --sink jsonl    # and against bigquery / postgres / sqlite
cargo test -p faucet-cli --test hub_catalog -- --ignored regenerate   # refresh the matrix page + index.json
cargo test -p faucet-cli --test hub_catalog
```

Schemas: `faucet schema source-template`, `faucet schema sink-template`.
Docs: [Template Hub cookbook](../docs/book/src/cookbook/template-hub.md).

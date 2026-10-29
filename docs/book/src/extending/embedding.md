# Embedding faucet in another tool

Tools that move data — an orchestrator, a message bridge, a binding for
another language — can use faucet's connectors without running the `faucet`
CLI. The [`faucet-registry`](https://crates.io/crates/faucet-registry) crate is
the same name → connector map the CLI uses: give it a connector's `type:` and
its `config:` as JSON, and it builds the connector.

It depends on `faucet-core`, `faucet-auth` and the connector crates you enable,
and nothing from the CLI (no server, scheduler, UI or MCP). It follows semver
and is covered by the workspace's API-stability gate, so a minor release of
faucet cannot break your build.

## Add it

Turn on only the connectors you need:

```toml
[dependencies]
faucet-registry = { version = "1", features = ["source-postgres", "sink-jsonl"] }
faucet-core = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

Each connector has a `source-<kind>` / `sink-<kind>` feature. `source` and
`sink` turn on every first-party source or sink except the Oracle connectors
(which load Oracle Instant Client at run time), and `full` turns on
everything. The connector crates' options are forwarded too: `compression`,
`encryption`, `arrow`, `file-formats`, `mtls`, `kafka-schema-registry`, the
Iceberg catalogs, the `*-staging` bulk loads, and so on. If you already depend
on `faucet-stream`, its `registry` feature re-exports the crate as
`faucet_stream::registry`.

## Build connectors and move rows

The `config` you pass is what a pipeline file holds under `source.config` /
`sink.config`, as JSON. `faucet schema source <kind>` prints its schema, and
`source_schema(kind)` returns it at run time.

```rust,ignore
use std::sync::Arc;

use faucet_core::{FileStateStore, Pipeline};
use faucet_registry::{auth_catalog, build_sink, build_source};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let auth = auth_catalog::build_auth_catalog(None)?;

    let source = build_source(
        "file",
        json!({ "path": "inbox/", "incremental": { "by": "name" } }),
        &auth,
        None,
    )
    .await?;
    let sink = build_sink("jsonl", json!({ "path": "out.jsonl", "append": true }), &auth).await?;

    let result = Pipeline::new(source.as_ref(), sink.as_ref())
        .with_state_store(Arc::new(FileStateStore::new("state")))
        .run()
        .await?;
    println!("{} records", result.records_written);
    Ok(())
}
```

`Pipeline` reads the stored bookmark before the run and writes it only after
the sink confirms each page, so the next run reads only what is new. You can
also drive the connectors yourself: `source.stream_pages(&context, batch_size)`
yields pages with their bookmarks, and `sink.write_batch` / `sink.flush` write
them.

## Shared auth providers

A pipeline's top-level `auth:` block maps names to `{ type, config }` provider
specs. `auth_catalog::build_auth_catalog(Some(&specs))` builds each provider
once; every connector config that says `auth: { ref: <name> }` then gets the
same provider, so one token cache and one refresh serve them all.

## What you must do yourself

**Resolve references first.** The registry uses a config exactly as given. It
does not expand `${env:…}`, `${file:…}`, `${vars.…}` or secret-manager
references, and does not undo `$${` escapes. Resolve them with the policy your
host needs before you build. The registry never resolves a reference with a
permissive default on your behalf.

**Opt in to connectors that run programs.** A connector whose config names a
program to run on this host — the Singer tap and target bridges, or a plugin
whose schema has a field annotated `x-faucet-resource: command` — is refused
with `RegistryError::SubprocessRefused`. If you trust the config, allow it:

```rust,ignore
use faucet_registry::{BuildPolicy, build_source_with};

let policy = BuildPolicy::default().allow_subprocess(true);
let tap = build_source_with("singer", config, &auth, None, &policy).await?;
```

`source_runs_program(kind)` / `sink_runs_program(kind)` tell you in advance.

**Scrub the secrets the registry finds.** A `google_service_account` provider's
inline key and a Singer target's config are credentials you never resolved
yourself. Install a registrar to be told about each:

```rust,ignore
faucet_registry::secrets::install(Box::new(|secret: &str| {
    my_redactor.add(secret);
}));
```

## Look things up without building

| Function | Answers |
|---|---|
| `source_kinds()` / `sink_kinds()`, `source_exists(kind)` / `sink_exists(kind)` | Which connectors this build has |
| `source_descriptions()` / `sink_descriptions()` | One-line descriptions for a picker |
| `source_schema(kind)` / `sink_schema(kind)` | The config's JSON Schema, for a form or a validator |
| `validate_source_config(kind, name, config)` / `validate_sink_config(…)` | Whether a config is valid, without connecting |
| `source_capabilities(kind, &config)` / `sink_capabilities(kind, &config)` | What a connector can do: write modes, exactly-once, discover, shardable, … |
| `source_resumes_from_bookmark(kind, &config)` / `source_reads_a_delta(kind, &config)` | Whether a source reads only what changed since its last run |

## Errors

Every call returns a `RegistryError`: `UnknownConnector` (not compiled in, or
misspelled), `InvalidConnectorConfig` (its `path` names where in the config
deserializing stopped; values from the config never appear in the message),
`UnknownAuthProvider`, `AuthProviderBuild`, `SubprocessRefused`, `Config`,
`Faucet` (a connector's constructor failed) or `Other`. The enum is
`#[non_exhaustive]`, so match it with a `_` arm.

## Your own connectors

`PluginRegistry` adds connectors under `type:` names of your own; every lookup
above then finds them like built-ins. See
[Authoring a connector](./authoring-connectors.md) for the connector side and
the crate's README for registration.

## Coming from `faucet_cli::registry`

`faucet_cli::registry` and `faucet_cli::auth_catalog` still work: they
re-export this crate, and keep returning `faucet_cli::CliError`. Moving to
`faucet-registry` removes the CLI from your dependency tree. Two differences:
`faucet_cli::registry::build_*` undoes the CLI loader's `$${` escapes and
builds subprocess connectors without asking, because the CLI trusts the files
it loads; `faucet_registry::build_*` does neither.

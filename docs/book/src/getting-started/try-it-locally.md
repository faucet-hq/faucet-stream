# Try it locally (interactive demo)

The repo ships a single script — [`scripts/try-local.sh`](https://github.com/faucet-hq/faucet-stream/blob/main/scripts/try-local.sh) —
that builds the `faucet` CLI, generates a throwaway demo workspace, exercises a
broad slice of the toolkit against **file-only connectors (no Docker, no cloud,
no databases)**, and then leaves the [web console](../cookbook/web-console.md)
running so you can browse the results visually.

It's the fastest way to see pipelines, transforms, data-quality, masking,
lineage, the Data Movement Catalog, and dead-letter-queue replay working
end-to-end on your machine.

## Prerequisites

The default build is **light** and pure-Rust — it needs only:

- **rustup** with the toolchain pinned in `rust-toolchain.toml` (the script
  resolves it automatically, even if a Homebrew `rustc` is on your `PATH`).
- A C toolchain for a couple of transitive crates — on macOS that's the Xcode
  Command Line Tools (`xcode-select --install`); on Linux, `build-essential`.

`sqlite3` and `curl` are used by a few steps if present (both ship on macOS and
most Linux distros); missing ones are skipped gracefully.

> The optional `--full` build additionally compiles Kafka, gRPC, the cloud
> connectors, and the DuckDB SQL transform from source, which requires **CMake**
> and takes ~15–30 minutes. The light default builds in a few minutes.

## Running it

```bash
# From the repo root — builds the light feature set, runs the battery,
# then starts the web console and leaves it up (Ctrl+C to stop).
./scripts/try-local.sh
```

Useful flags:

| Flag | Effect |
|------|--------|
| _(none)_ | Light build → run battery → keep the web console running |
| `--full` | Build **every** feature (Kafka, gRPC, cloud, DuckDB SQL); needs CMake |
| `--release` | Optimised build (slower to compile, faster to run) |
| `--no-serve` | Run the battery and exit — no console (for CI / a quick check) |
| `--serve-only` | Skip the build + battery; just (re)launch the populated console |
| `--clean` | Wipe the demo workspace (`faucet-local-demo/`) first |
| `--no-build` | Reuse an already-built binary |
| `--port N` | Console / serve port (default `8899`) |

## What it exercises

Everything below runs against generated CSV data in `faucet-local-demo/`:

- **Core**: CSV → JSONL, record transforms (`set` / `cast` / `redact` /
  `flatten` / `filter` / `explode` / `value_case`), `preview`, `validate`,
  `doctor`.
- **Governance**: data-quality checks + DLQ quarantine, data contracts
  (`quarantine` and `fail` policies), PII masking (redact / hash / partial /
  tokenize).
- **Round-trips**: CSV ↔ SQLite, CSV ↔ Parquet.
- **Runtime**: matrix fan-out, `depends_on` ordering, `--from-env`, config
  composition (`extends:` + `profiles:`), JSON-format configs, schema-drift
  `evolve` (SQLite `ADD COLUMN`).
- **Observability**: SLA monitoring, file-based OpenLineage emission, the Data
  Movement Catalog.
- **Ops**: offline `faucet test`, `dlq inspect` / `replay`, and the `serve`
  HTTP control plane.
- **Templates**: a parameterized config registered into the template registry,
  then walked through the release lifecycle — `--launch`, `promote` up the
  channels, `launch` from a channel, `rollback`, `deprecate` — plus a run
  triggered by id + `--param`. Every template the demo launches carries a small
  `tests:` bundle, because a version needs passing tests before it can be
  launched or put on a channel (see
  [Template tests and labels](../cookbook/templates.md#template-tests-and-labels)).
  One version's fixture fails on purpose, so the registry also shows a version
  that cannot be launched, next to a draft and a deprecated template. A source
  template and a sink template name each other in their bundles: the first one
  registered is red until the second exists, and registering the second
  re-tests it.

The script prints a summary of passed and failed steps and exits non-zero
when any step failed, listing them. With the console left running, the list
is printed again when you stop it with Ctrl+C.

With `--full`, the embedded DuckDB **SQL transform** step is included too.

## The web console

When the battery finishes, the script submits a handful of demo runs through
the HTTP API and then keeps the server up, so the console arrives already
populated — you can open it and immediately browse **Runs**, **Datasets**,
**Lineage**, **Templates** (with a versions page you can launch and roll back
from), and the per-run **dead-letter-queue** panel. See
[Web console](../cookbook/web-console.md) for a screenshot tour.

The run-history database (`faucet-local-demo/faucet-meta.db`) is **not** wiped
between invocations, so run history accumulates over time. Use `--clean` to
reset the whole workspace.

> The `faucet-local-demo/` workspace is disposable — delete it any time with
> `rm -rf faucet-local-demo`. It is git-ignored.

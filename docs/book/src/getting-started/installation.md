# Installation

## The `faucet` CLI

### Prebuilt binaries (no Rust required)

Every `faucet-cli` release ships prebuilt binaries for macOS (Apple Silicon +
Intel) and Linux (x86_64 + aarch64), so you don't need a Rust toolchain to try
it.

**Homebrew (macOS / Linux):**

```bash
brew install faucet-hq/faucet-stream/faucet-cli
```

(The formula is named after the `faucet-cli` package; it installs the `faucet`
binary.)

**Shell installer (macOS / Linux):**

```bash
curl -LsSf https://github.com/faucet-hq/faucet-stream/releases/latest/download/faucet-cli-installer.sh | sh
```

**Direct download:** grab the archive for your platform from the latest
[`faucet-cli` GitHub Release](https://github.com/faucet-hq/faucet-stream/releases?q=faucet-cli&expanded=true)
(e.g. `faucet-cli-aarch64-apple-darwin.tar.xz`), verify it against the
published `.sha256` checksum, and put `faucet` on your `PATH`.

The prebuilt binary includes the CLI **default** feature set (every first-party
connector, transforms, quality checks, contracts, masking, compression) plus
`serve` (with the embedded web console), `schedule`, `lineage`, and `templates`
(the pipeline template registry — note that a registry surviving a restart also
needs a `serve-history-*` backend). Not included — build from source for these:
`transform-sql` (embedded DuckDB), `otel`, `triggers`, `catalog`, the
`serve-history-*` backends, and the Oracle connectors (`source-oracle`,
`source-oracle-cdc`, `sink-oracle`), which need Oracle Instant Client at runtime
(see [Oracle Instant Client](#oracle-instant-client)).

> **macOS Gatekeeper:** the binaries are not currently notarized. If macOS
> blocks the downloaded binary, clear the quarantine attribute:
> `xattr -d com.apple.quarantine $(which faucet)`. Homebrew installs are not
> affected.

### From source (crates.io)

For the full feature set, or any custom combination, install from crates.io:

```bash
cargo install faucet-cli                     # the default feature set
cargo install faucet-cli --features full     # everything (DuckDB, otel, triggers, …)
```

This gives you a `faucet` binary with every first-party connector compiled in
except the three Oracle connectors, which are opt-in because they load Oracle
Instant Client at runtime: `cargo install faucet-cli --features
"source-oracle,source-oracle-cdc,sink-oracle"` (they are also part of `full`).

### Choose your build (feature flags)

Every connector and runtime capability is a **Cargo feature**, so you can build exactly the
binary you need. Connector features are named **`source-<name>`** and **`sink-<name>`**.

**Bare minimum** — the smallest useful binary (REST in, JSON Lines out):

```bash
cargo install faucet-cli --no-default-features --features "source-rest,sink-jsonl"
```

**Add a source or sink** — list the connectors you want (plus `transforms` if you need in-flight shaping):

```bash
cargo install faucet-cli --no-default-features \
  --features "source-postgres,sink-bigquery,transforms"
```

**Add a runtime capability** — compose any of `serve`, `serve-ui`, `schedule`, `lineage`,
`transform-sql` (embedded DuckDB), `triggers`, `templates`, `catalog`, `otel`, `compression`,
`quality`, `contract`, `masking`:

```bash
cargo install faucet-cli --features "serve,schedule,transform-sql,lineage"
```

Run `faucet list` to see which sources, sinks, and transforms are compiled into your binary,
and the [connector catalog](../reference/connectors.md) for every feature name.

## The library

To embed pipelines in your own Rust program, depend on the umbrella crate and
enable the connectors you need:

```toml
[dependencies]
# Default features include the REST source only.
faucet-stream = "1.0"

# Or enable specific connectors:
faucet-stream = { version = "1.0", features = ["source-rest", "sink-postgres", "sink-s3"] }

# Or everything:
faucet-stream = { version = "1.0", features = ["full"] }
```

Feature groups: `source` (all sources), `sink` (all sinks), `state` (all
state-store backends), `full` (everything), and `compression` (gzip/zstd on the
file-shaped connectors you've enabled).

You can also depend on individual connector crates directly
(`faucet-source-rest`, `faucet-sink-bigquery`, …) — each depends only on
`faucet-core`.

## Requirements

- A recent stable Rust toolchain (see the repo's `rust-toolchain.toml` for the
  current MSRV).
- Some connectors link native libraries — the Kafka connectors build
  `librdkafka` and need `cmake` and a C toolchain available at compile time.

### Oracle Instant Client

The Oracle connectors (`source-oracle`, `source-oracle-cdc`, `sink-oracle`) are
built on the `oracle` crate (ODPI-C), which compiles without any Oracle software
but **loads Oracle Instant Client at runtime**. Install the Basic or Basic Light
package from <https://www.oracle.com/database/technologies/instant-client.html>
and put its directory on the library path:

```bash
# Linux (x86_64). Instant Client needs libaio (libaio1t64 on Ubuntu 24.04).
sudo apt-get install -y libaio1t64 || sudo apt-get install -y libaio1
unzip instantclient-basiclite-linuxx64.zip -d /opt/oracle
export LD_LIBRARY_PATH=/opt/oracle/instantclient_23_7:$LD_LIBRARY_PATH   # your version's directory
```

On macOS put the directory on `DYLD_LIBRARY_PATH` (or symlink
`libclntsh.dylib` into `~/lib`); on Windows add it to `PATH`. Without the
client an Oracle connector fails at connect time — including `faucet doctor`'s
probe — with a `DPI-1047` error and a hint naming this fix.

Next: [run your first pipeline](./first-pipeline.md).

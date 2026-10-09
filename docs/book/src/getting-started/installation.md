# Installation

## The `faucet` CLI

### Prebuilt binaries (no Rust required)

Every `faucet-cli` release ships prebuilt binaries for macOS (Apple Silicon +
Intel) and Linux (x86_64 + aarch64), so you don't need a Rust
toolchain to try it.

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

**Verify provenance.** The checksum is published next to the archive, so it
only catches a corrupt download. To check that an archive (or the shell
installer) was built by this repository's release workflow, verify its
[build provenance attestation](https://docs.github.com/actions/security-for-github-actions/using-artifact-attestations)
with the GitHub CLI:

```bash
gh attestation verify faucet-cli-aarch64-apple-darwin.tar.xz --repo faucet-hq/faucet-stream
```

Container images on GHCR carry provenance and an SBOM, and are signed with
Sigstore keyless signing:

```bash
gh attestation verify oci://ghcr.io/faucet-hq/faucet-stream:full --repo faucet-hq/faucet-stream
cosign verify ghcr.io/faucet-hq/faucet-stream:full \
  --certificate-identity-regexp '^https://github.com/faucet-hq/faucet-stream/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The prebuilt binary includes the CLI **default** feature set (every first-party
connector, transforms, quality checks, contracts, masking, compression) plus
`serve` with the embedded web console (`serve-ui`), `schedule`, `lineage`, `templates`
(the pipeline template registry), `mcp` (the `faucet mcp` server for AI
agents), `notify` (Slack / PagerDuty / webhook notifications), `secrets`
(`${vault:…}`, `${aws-sm:…}`, `${gcp-sm:…}`, `${azure-kv:…}`), `catalog` (the
Data Movement Catalog), and the `serve-history-sqlite` /
`serve-history-postgres` backends (so run history, the template registry and
the catalog survive a restart). Not included — build from source for these:
`transform-sql` (embedded DuckDB), `transform-wasm`, `otel`, `triggers`,
`tenants`, `encryption`, and the Oracle connectors (`source-oracle`,
`source-oracle-cdc`, `sink-oracle`), which need Oracle Instant Client at runtime
(see [Oracle Instant Client](#oracle-instant-client)).

### Container images

Every `faucet-cli` release publishes these images to
`ghcr.io/faucet-hq/faucet-stream` (tags `:<profile>` and `:<version>-<profile>`;
`:latest` and `:<version>` are `full`):

| Tag | What is compiled in |
|---|---|
| `full` | The CLI's `full` feature: **every** connector and **every** feature — serve with the web console, tenants, templates and template sync, triggers (all watchers), the SQL and WASM transforms, OTLP export, secret-manager references, encryption, every file format, lineage, the catalog, MCP. |
| `full-oracle` | `full` plus Oracle Instant Client, which the Oracle connectors load at run time. About 40 MiB larger compressed, so only pull it if you move Oracle data. In `full`, an Oracle connector fails at connect time with an error naming this tag. |
| `core` | `rest`, `postgres`, `s3`, `csv` → `postgres`, `s3`, `jsonl`, `stdout`. |
| `analytics` | `rest`, `postgres`, `s3`, `bigquery`, `snowflake` → `bigquery`, `snowflake`, `s3`, `jsonl`. |
| `cdc` | `postgres-cdc`, `mysql-cdc`, `mongodb-cdc` → `postgres`, `kafka`, `s3`. |

The lean profiles also carry serve, the console, templates, triggers (webhook),
the catalog and the run-history backends. Each image's size is recorded in the
summary of the release's *Docker images* workflow run. To build your own
connector set, see [`deploy/README.md`](https://github.com/faucet-hq/faucet-stream/blob/main/deploy/README.md).

### Windows

Windows is not a supported platform yet: there is no prebuilt Windows binary,
and faucet is run on macOS or Linux (including Linux containers on Kubernetes).
Building from source on Windows is possible; if you do, expect these platform
differences:

- **Kafka:** `PLAIN`, `SCRAM-SHA-256/512`, `OAUTHBEARER` and TLS work. Kerberos
  (`sasl.mechanism: GSSAPI` set through `extra_client_config`) is not available —
  it needs Cyrus SASL, which does not build on Windows.
- **Stopping long-running verbs:** `faucet serve`, `schedule`, `mirror` and
  `backfill` drain gracefully on Ctrl-C, Ctrl-Break, closing the console window,
  or a system shutdown (the Unix builds use `SIGTERM`). `faucet schedule`'s
  `SIGHUP` hot reload is Unix-only; restart the scheduler to pick up a changed
  config.
- **Singer taps / targets:** the child process gets the same grace period to
  exit, but Windows has no `SIGTERM`, so a tap that is still running when the
  grace period ends is terminated. The temporary config/state files are
  created in your per-user temp directory rather than with Unix `0600` mode.
- **Paths:** write Windows paths as plain YAML scalars (`path: C:\data\in.csv`)
  or with forward slashes; inside double quotes a backslash starts an escape.
  State-store keys are percent-encoded on disk, so `::` in a key is safe.
- **Template Hub cache:** remote hubs are cached under
  `%LOCALAPPDATA%\faucet\hub` (override with `FAUCET_HUB_CACHE`).

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
cargo install faucet-cli --no-default-features --features "source-rest,sink-file"
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
- Building from source on Windows needs the MSVC build tools, CMake, Strawberry
  Perl and [NASM](https://www.nasm.us/) on `PATH` (the vendored OpenSSL and
  `aws-lc` assemble with it).

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

In a container, use the `full-oracle` image, which ships the client.

On macOS put the directory on `DYLD_LIBRARY_PATH` (or symlink
`libclntsh.dylib` into `~/lib`); on Windows add it to `PATH`. Without the
client an Oracle connector fails at connect time — including `faucet doctor`'s
probe — with a `DPI-1047` error and a hint naming this fix.

Next: [run your first pipeline](./first-pipeline.md).

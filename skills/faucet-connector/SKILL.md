---
name: faucet-connector
description: >-
  Use when writing a new faucet source or sink connector in Rust: scaffolding a
  faucet-source-* or faucet-sink-* crate (or a faucet-common-* crate shared by
  a pair), implementing the Source or Sink trait from faucet-core, adding a
  config struct with a JSON Schema, streaming pages with bounded memory and
  resumable bookmarks, making a sink idempotent or effectively-once (keyed
  upsert or atomic watermark), mapping failures to typed FaucetError variants
  so retries behave, passing the faucet-conformance battery, publishing the
  crate to crates.io, running it from a custom faucet binary, or listing it in
  the connector registry index.
license: MIT OR Apache-2.0
---

# Building a faucet connector

A faucet connector is a Rust crate that implements one trait from
`faucet-core`: `Source` (reads records) or `Sink` (writes records). Records are
JSON values. The pipeline owns paging, checkpointing, retries, metrics and the
dead-letter queue; the connector owns talking to its backend correctly and
fast.

The engine contract a connector must honour (streaming and bookmarks, the
write → flush → checkpoint order, effectively-once, retries, error typing,
performance, config rules, versioning) is on
[Authoring a connector](https://faucet-hq.github.io/faucet-stream/extending/authoring-connectors.html);
the conformance battery and tiers on
[Connector conformance](https://faucet-hq.github.io/faucet-stream/reference/conformance.html);
publishing and discovery on
[Connector marketplace](https://faucet-hq.github.io/faucet-stream/extending/marketplace.html).
Read the authoring page before writing code.

Working, tested examples to copy: [examples/faucet-source-acme/](examples/faucet-source-acme/src/stream.rs)
(an HTTP API source with keyset pagination and resume) and
[examples/faucet-sink-acme/](examples/faucet-sink-acme/src/sink.rs) (a bulk
HTTP sink with append and keyed upsert). Both pass the conformance battery
against a `wiremock` backend.

## Step 0: use the project's faucet version

The project decides which faucet to use, not whatever is on the `PATH`: a
config written for a newer faucet can fail or be misread on an older one.

1. Find the pin: the `"github:faucet-hq/faucet-stream"` entry under `[tools]`
   in the project's `mise.toml` (`faucet init` writes it). A config may also
   carry `requires_faucet: ">=X.Y"`.
2. Run `faucet --version`. Use that binary if it equals the pin (with no pin:
   if it satisfies every `requires_faucet` in the project).
3. Otherwise, if [mise](https://mise.jdx.dev) is installed, run `mise install`
   in the project and prefix every command with `mise exec --`.
4. Otherwise use the pinned container image; its entrypoint is `faucet`:
   `docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/work" -w /work ghcr.io/faucet-hq/faucet-stream:<version> --version`.
5. Otherwise install exactly that version:
   `curl --proto '=https' --tlsv1.2 -LsSf https://github.com/faucet-hq/faucet-stream/releases/download/faucet-cli-v<version>/faucet-cli-installer.sh | sh`.
6. With no pin at all, install the latest release (the same installer from
   `releases/latest/download/`) and pin it as described in
   [Pinning the faucet version](https://faucet-hq.github.io/faucet-stream/operations/pinning.html).

Every version-specific fact (which connectors and blocks exist, config keys,
types, defaults, commands and flags) comes from that binary: `faucet list`,
`faucet schema --help`, `faucet schema source|sink|transform <name>`,
`faucet <command> --help`. Never from memory or from this skill. When a config
passes `faucet validate`, set its `requires_faucet:` to `">=<major>.<minor>"`
of that binary; if validate rejects `requires_faucet` as an unknown field, the
binary predates it, so leave it out.

## Step 0b: the faucet-core version is the API authority

The `faucet` binary only scaffolds. The API the crate compiles against is the
`faucet-core` (and `faucet-conformance`) version its `Cargo.lock` resolves:

```bash
cargo tree -p faucet-core --depth 0
cargo tree -p faucet-conformance --depth 0 -e dev
```

Every API fact — trait methods and their exact signatures, which methods are
defaulted, re-exports, `FaucetError` variants, helper functions, conformance
checks — comes from the docs for exactly those versions:
`https://docs.rs/faucet-core/<version>/faucet_core/` and
`https://docs.rs/faucet-conformance/<version>/faucet_conformance/`, or offline:

```bash
cargo doc -p faucet-core -p faucet-conformance --open
```

Method names below (`stream_pages`, `write_batch`, `flush`, ...) say where to
look; confirm each signature there before implementing it. The validator is
the compiler and the tests, not this skill.

## Workflow

1. **Scaffold.**
   ```bash
   faucet new connector --help
   faucet new connector acme --kind source
   faucet new connector acme --kind sink --common
   ```
   The name is the lowercase system name; it becomes the crate name and the
   YAML `type:`. `--common` adds a `faucet-common-<name>` crate for config a
   source/sink pair shares. Run `cargo test` immediately; the scaffold is green
   with a passthrough implementation and a wired `tests/conformance.rs`. If the
   project has no `faucet` binary, copy an example crate from `examples/`
   instead and rename it.

2. **Config.** Fill `src/config.rs`: every field documented (the doc comments
   become the schema `faucet schema` and editors show), safe bounded defaults,
   validation in the constructor returning a config error. Rules:
   [Config and schema](https://faucet-hq.github.io/faucet-stream/extending/authoring-connectors.html#config-and-schema).
   Keep all I/O out of `config.rs`.

3. **Implement the trait** in `src/stream.rs` or `src/sink.rs`, the only
   module that does I/O. Open the trait on docs.rs for the locked version and
   implement the required method, then override `config_schema`,
   `connector_name` and `dataset_uri`. Then, in order of importance:
   - **Source:** override `stream_pages` to read page by page from the
     backend's paging primitive (the default buffers everything), and make the
     whole-result method drain that stream. For incremental sync, add the state
     key, start-bookmark and per-page bookmark pieces
     ([Streaming pages and bookmarks](https://faucet-hq.github.io/faucet-stream/extending/authoring-connectors.html#streaming-pages-and-bookmarks)).
     Override `check` only if a one-page probe would block or have side
     effects.
   - **Sink:** `write_batch` through the backend's bulk API, split to its
     request limit; `flush` if anything is buffered; per-row outcomes when the
     backend reports them; keyed upsert/delete through the shared write spec
     and planner; an atomic watermark only when rows and token commit in one
     transaction
     ([Write, flush, checkpoint](https://faucet-hq.github.io/faucet-stream/extending/authoring-connectors.html#write-flush-checkpoint));
     a read-only `check` probe.

4. **Unit and integration tests.** Unit tests at the bottom of each file,
   integration tests in `tests/` against `wiremock` or `testcontainers`,
   asserting exact records, bookmarks and error variants
   ([Tests](https://faucet-hq.github.io/faucet-stream/extending/authoring-connectors.html#tests)).

5. **Conformance.** Grow `tests/conformance.rs` with every check that applies
   to what the connector does, and assert the honest branch for what it does
   not ([which checks apply](https://faucet-hq.github.io/faucet-stream/extending/authoring-connectors.html#self-certify-with-the-conformance-battery)).
   Give each sink check its own fresh destination.

6. **Gate.** All must pass before publishing:
   ```bash
   cargo check --all-targets
   cargo test
   cargo clippy --all-targets -- -D warnings
   cargo fmt --check
   cargo publish --dry-run
   ```
   `faucet conformance` scores only connectors compiled into a faucet binary;
   a third-party crate is proven by its own `tests/conformance.rs`.

7. **Publish and register.** Publish to crates.io, then make it usable and
   discoverable: users run it from a custom `faucet` binary that registers it
   ([Custom binaries with third-party connectors](https://github.com/faucet-hq/faucet-stream/blob/main/cli/README.md#custom-binaries-with-third-party-connectors)),
   and a PR to the registry index makes `faucet search` find it
   ([Connector marketplace](https://faucet-hq.github.io/faucet-stream/extending/marketplace.html#publishing-your-connector)).
   Check the entry with a local copy of the index:
   ```bash
   faucet search acme --index ./registry.json
   faucet install acme --kind source --index ./registry.json
   ```

## Hard rules

1. **Depend only on `faucet-core` among faucet crates** (plus
   `faucet-conformance` as a dev-dependency), at the major version only. Use
   its re-exports instead of adding the same crates yourself; add `serde` and
   `schemars` only for their derive macros, plus your backend's client.
2. **Build clients once, in the constructor, with no network I/O.** Never a
   client, pool or producer per call, page or record. Connect lazily; probe in
   `check`.
3. **Never move a position ahead of durable data.** Sources emit bookmarks on
   pages and never ack, commit offsets or delete source data themselves. A
   sink's `flush` leaves everything written so far durable.
4. **Never retry a non-idempotent write**, inside `write_batch` or by
   declaring it replay-safe when it is not. A lost response plus a retry
   duplicates every row.
5. **Every failure is a typed `FaucetError`.** Transient failures use the
   variants the pipeline retries, permanent ones the others; foreign errors
   are wrapped, not stringified. No `unwrap`/`expect` on anything that can
   fail at run time.
6. **No hardcoded credentials, hosts or URLs.** Everything comes from config.
   No secret in `Debug` output, logs, errors, probe reasons or `dataset_uri`.
7. **Keep the traits object-safe.** Implement only the trait's own methods
   with their exact signatures from docs.rs; no generics, associated types or
   driver types in a signature.
8. **Report capabilities honestly.** Return `true` from a capability probe or
   list a write mode only for what the connector really does for the current
   config. The CLI gates configs on them; a false `true` corrupts data.
9. **Bounded memory.** Stream pages; never collect a whole result the backend
   can page; bound every concurrent fan-out.
10. **Start at `1.0.0`; additive changes are minor releases.** A renamed or
    removed field or a changed bookmark shape without a migration is a major.

## Review checklist

- [ ] `cargo tree -p faucet-core --depth 0` checked, and every trait method
      matches docs.rs for that version.
- [ ] Only `faucet-core` (+ `serde`, `schemars`, the backend client) in
      `[dependencies]`, at the major version.
- [ ] Client or pool built once; no I/O in the constructor or `config.rs`.
- [ ] Native `stream_pages`; page size bounded; the one-page batch size handled.
- [ ] Bookmarks only on pages; never acked or committed early; a malformed
      bookmark is a state error.
- [ ] Sink `flush` makes everything durable; an empty batch is a cheap no-op.
- [ ] Capability probes match behaviour for every config.
- [ ] Errors typed so transient ones retry; no secrets anywhere.
- [ ] Unit, integration and conformance tests green; clippy and fmt clean.
- [ ] Version `1.0.0`, docs.rs metadata, README with a config example using
      `${env:...}` for secrets.

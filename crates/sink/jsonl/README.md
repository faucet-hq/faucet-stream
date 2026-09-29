# faucet-sink-jsonl

> **Deprecated.** Use [`faucet-sink-file`](https://crates.io/crates/faucet-sink-file) instead.
> This crate gets security fixes only until the next major release. In a
> pipeline config, `type: jsonl` still works: the CLI builds it as
> `type: file` with `format: json_lines` and warns. The migration table is in
> the [`faucet-sink-file` README](https://github.com/faucet-hq/faucet-stream/blob/main/crates/sink/file/README.md).

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-jsonl.svg)](https://crates.io/crates/faucet-sink-jsonl)
[![Docs.rs](https://docs.rs/faucet-sink-jsonl/badge.svg)](https://docs.rs/faucet-sink-jsonl)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-jsonl.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-jsonl.svg)](https://github.com/faucet-hq/faucet-stream#license)

JSON Lines (**`.jsonl`**) file sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes each record as one JSON object per line in [JSON Lines](https://jsonlines.org/) format.

It's the workhorse local-file destination: zero credentials, zero connection setup, and a streaming-friendly format that everything downstream understands (`jq`, `pandas`, DuckDB, BigQuery / Snowflake / Spark loaders). Records stream through a buffered async writer so even multi-million-row exports stay fast and bounded in memory, and dated output paths like `./data/dt=2026-03-08/part.jsonl` work without pre-creating the directory tree.

## Feature highlights

- **Buffered async writes** — records go through a `tokio::io::BufWriter`, so high-volume runs aren't bottlenecked on per-record syscalls.
- **Append or truncate** — `append: true` adds to an existing file; the default truncates on open for a clean export.
- **Auto-creates parent directories** — missing parents of `path` are created (`mkdir -p` semantics) before the file opens, so dated/partitioned subdirectory paths just work.
- **Optional compression** — behind the `compression` feature, gzip / zstd are selected automatically from the `.gz` / `.zst` suffix (or set explicitly). Multi-member output stays decoder-compatible across mid-stream flushes.
- **Optional encryption at rest** — behind the `encryption` feature, every record line is sealed with AES-256-GCM and written base64-encoded (#207). Append-safe; the natural fit for a file-backed DLQ. See [Encryption at rest](#encryption-at-rest).
- **Pretty-print mode** — `pretty: true` indents each record for human reading (no longer strict JSONL — records span multiple lines).
- **Flush-safe for CDC** — `flush()` finalises the writer and the next write reopens in append mode regardless of `append`, so a CDC source's per-transaction flush appends rather than truncates — no data loss mid-stream.
- **`faucet doctor` preflight** — `check()` verifies the parent directory is writable (via a throwaway temp file) without ever touching your real output file.

## Installation

```bash
# As a library:
cargo add faucet-sink-jsonl

# In the CLI (connector feature; in the CLI default build):
cargo install faucet-cli --features sink-jsonl

# With gzip / zstd compression:
cargo install faucet-cli --features "sink-jsonl,compression"
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features sink-jsonl
```

`sink-jsonl` is one of the most common destinations and ships in the CLI's default build. The crate-local `compression` feature is opt-in.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
name: csv_to_jsonl
pipeline:
  source:
    type: csv
    config:
      path: ./data/input.csv
  sink:
    type: jsonl
    config:
      path: ./out/records.jsonl
```

```bash
faucet run pipeline.yaml
```

Every row of `input.csv` is written to `./out/records.jsonl` as one compact JSON object per line. The `./out/` directory is created automatically if it doesn't exist.

## Configuration reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string (path) | *(required)* | Path to the output file. Missing parent directories are created automatically. |
| `append` | bool | `false` | Append to an existing file. When `false`, the file is **truncated** on first open. |
| `pretty` | bool | `false` | Pretty-print (indent) each record. This breaks strict JSONL — records span multiple lines. |
| `batch_size` | int | `1000` | Records per upstream `StreamPage`. **No behavioural effect** at this per-record sink; present only for config parity. See [Streaming & batching](#streaming--batching). |
| `compression` | `none` \| `gzip` \| `zstd` \| `auto` | `auto` | *(requires the `compression` feature)* Output codec. `auto` picks gzip/zstd from the file suffix; anything else writes uncompressed. See [Compression](#compression). |
| `encryption` | object | — | *(requires the `encryption` feature)* Seal every line with AES-256-GCM: `{ key, previous_keys?, algorithm? }`. Mutually exclusive with `compression`. See [Encryption at rest](#encryption-at-rest). |

## Examples

### Basic export (truncate mode)

```yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /v1/events
  sink:
    type: jsonl
    config:
      path: ./out/events.jsonl
```

### Append for incremental / repeated runs

```yaml
version: 1
pipeline:
  source:
    type: postgres
    config:
      connection_url: ${env:DATABASE_URL}
      query: SELECT id, name, signup_date FROM users
  sink:
    type: jsonl
    config:
      path: ./out/users.jsonl
      append: true   # each run adds to the file rather than overwriting it
```

### Partitioned, dated output path

```yaml
version: 1
pipeline:
  source:
    type: csv
    config:
      path: ./data/input.csv
  sink:
    type: jsonl
    config:
      # ${now.date} resolves to YYYY-MM-DD per run; the dt=... directory
      # is created automatically.
      path: ./data/dt=${now.date}/part.jsonl
```

### Gzip-compressed export (requires the `compression` feature)

```yaml
version: 1
pipeline:
  source:
    type: postgres
    config:
      connection_url: ${env:DATABASE_URL}
      query: SELECT * FROM events
  sink:
    type: jsonl
    config:
      path: ./out/events.jsonl.gz   # .gz suffix → gzip via compression: auto
```

## Streaming & batching

`Pipeline::run` drives the source's `stream_pages` and hands each emitted `StreamPage` to `Sink::write_batch`. This sink iterates the page **record by record**, serializing and appending each one through the buffered writer.

Because the write path is inherently per-record, `batch_size` has **no observable effect** here — `batch_size = 0` (the "no batching" sentinel) and any positive value produce byte-for-byte identical files. The field exists only so every sink shares one config shape; sinks like `faucet-sink-postgres` (multi-row INSERTs) or `faucet-sink-bigquery` (streaming-insert request sizing) genuinely use it. The per-run memory bound is set by the **source's** `batch_size` (the size of each `StreamPage`), not by this field.

When a bookmark-carrying page arrives (e.g. from a CDC source), the pipeline calls `flush()` after the page. This sink finalises the writer and the next `write_batch` reopens the file in **append mode regardless of `append`**, so per-transaction durability never truncates previously-written records.

## Compression

Behind the crate-local `compression` Cargo feature. Adds a `compression` config field with values `none`, `gzip`, `zstd`, or `auto` (the default — detects `.gz` / `.zst` from the file path):

```yaml
type: jsonl
config:
  path: ./out/events.jsonl.zst
  compression: auto   # or 'gzip' | 'zstd' | 'none'
```

`flush()` finalises the encoder (writes the trailer); a subsequent write appends a fresh gzip/zstd member, producing a multi-member compressed file that standard decoders read back transparently. The sink does not set any HTTP `Content-Encoding` — the file is a plain compressed file on disk.

## Config loading & schema

Configs load from YAML/JSON files, environment variables, or `.env` files via the CLI's normal loading path. From a library, use the `faucet_core::config` helpers:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_jsonl::JsonlSinkConfig;

let config: JsonlSinkConfig = load_json("config.json")?;
let config: JsonlSinkConfig = load_env_file(".env", "JSONL_SINK")?;
```

Inspect the full JSON Schema with:

```bash
faucet schema sink jsonl
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_jsonl::{JsonlSink, JsonlSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), faucet_core::FaucetError> {
let config = JsonlSinkConfig::new("./out/output.jsonl")
    .append(true)
    .pretty(false);
let sink = JsonlSink::new(config);

sink.write_batch(&[
    json!({"id": 1, "name": "Alice"}),
    json!({"id": 2, "name": "Bob"}),
])
.await?;
sink.flush().await?; // always flush before dropping the sink
# Ok(())
# }
```

Wire it to any source through a `Pipeline`:

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_jsonl::{JsonlSink, JsonlSinkConfig};

# async fn run() -> Result<(), faucet_core::FaucetError> {
let source = RestStream::new(RestStreamConfig::new("https://api.example.com", "/v1/events"));
let sink = JsonlSink::new(JsonlSinkConfig::new("./out/events.jsonl"));

let result = Pipeline::new(source, sink).run().await?;
println!("Exported {} records", result.records_written);
# Ok(())
# }
```

## How it works

1. The file is opened **lazily** on the first `write_batch` call and wrapped in a `tokio::io::BufWriter`. An empty batch is a no-op (no file is created).
2. Missing parent directories of `path` are created (`mkdir -p`) before the file opens.
3. The first open obeys `append`; **re-opens after a `flush()` always append**, so a flush-then-write sequence never truncates earlier data — important for CDC's per-transaction flush.
4. Each record is serialized to a single JSON line (or pretty-printed) followed by a newline; a `Mutex` guards the writer for thread-safe writes.
5. With the `compression` feature, the buffered file is wrapped in a gzip/zstd encoder chosen by `compression.resolve(path)`; a one-shot warning fires if an explicit codec disagrees with the file suffix.
6. `flush()` finalises and shuts down the writer (flushing the buffer and writing any compression trailer). The default `Sink` impl does **not** flush on `Drop` — call `flush()` explicitly before the program exits or the tail of the buffer is lost.

## Lineage dataset URI

`file://<path>` — e.g. `file:///tmp/output.jsonl`.

## Local output retention

The sink records every local file it opens, so faucet's local-output retention GC
(`faucet cleanup`, the `faucet serve` sweeper, and the console's Datasets page)
can reclaim them. Provenance is captured at the **first** open, before the file is
created:

- a path that did not exist is faucet's own output and is collectable;
- a path that **already existed** is flagged `pre_existing` and is **never**
  deleted — faucet wrote to a file it did not create, and no retention window
  makes deleting that acceptable.

The GC only ever deletes recorded paths — never a glob, never a directory. Set
`local_outputs.retention_days` in the config to change the window for a
pipeline's outputs (`0` keeps them forever), or `local_outputs.track: false` to
keep them out of the ledger entirely.

## Feature flags

| Feature | Default | Effect |
|---------|---------|--------|
| `compression` | off | Adds the `compression` config field and gzip/zstd encoding via `faucet-core/compression`. |

This sink does **not** support effectively-once delivery or upsert/delete write modes — it is an append-only file writer. For effectively-once or upsert semantics, use a transactional sink (`faucet-sink-postgres`, `faucet-sink-bigquery`, etc.).

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `failed to create parent directory '...'` | The parent path isn't creatable (permissions, or a path component is a file). Check write permissions on the closest existing ancestor; run `faucet doctor` to probe writability up front. |
| `failed to open <path>` | The file path is unwritable (read-only filesystem, no permission, or path is a directory). Verify the path and permissions. |
| File is empty / truncated after the run | You didn't `flush()`. The buffered/compressed writer only finalises on `flush()` — call it before dropping the sink (the CLI does this automatically). |
| Previous data disappeared on the next run | Default mode **truncates** on open. Set `append: true` to keep prior content across runs. |
| Output isn't valid line-delimited JSON | `pretty: true` indents records across multiple lines, which breaks strict JSONL. Use `pretty: false` (the default) for tooling that expects one object per line. |
| `.gz` / `.zst` file isn't compressed | The `compression` feature isn't enabled. Rebuild with `--features "sink-jsonl,compression"` (CLI) or `--features compression` (library); without it the suffix is treated as a plain filename. |
| Setting `batch_size` changes nothing | Expected — it's a no-op for this per-record sink. To bound per-page memory, set the **source's** `batch_size`. |
| Records lost mid-stream from a CDC source | Should not happen — re-opens after a per-page flush always append. If you see truncation, confirm nothing else is rewriting the file out-of-band. |

## See also

- [Sinks reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — the full connector capability matrix.
- [Compression cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/compression.html) — gzip/zstd across file sinks.
- [CLI reference](https://faucet-hq.github.io/faucet-stream/reference/cli.html) — `faucet run`, `faucet validate`, `faucet schema`, `faucet doctor`.
- [`faucet-sink-stdout`](https://crates.io/crates/faucet-sink-stdout) — the same JSON Lines to a standard stream instead of a file.
- [`faucet-sink-csv`](https://crates.io/crates/faucet-sink-csv) — CSV/TSV file output with full quoting.
- [`faucet-sink-s3`](https://crates.io/crates/faucet-sink-s3) / [`faucet-sink-gcs`](https://crates.io/crates/faucet-sink-gcs) — the same JSONL records to object storage.
- [`faucet-core`](https://crates.io/crates/faucet-core) — the `Sink` trait this connector implements.

## Encryption at rest

Behind the crate-local `encryption` Cargo feature (#207). Each serialized JSON
line is sealed with AES-256-GCM (a fresh random nonce per record) and written
as its base64 encoding plus a newline — the file stays line-oriented and
append-safe across flush/reopen cycles.

```yaml
sink:
  type: jsonl
  config:
    path: ./dlq/failed.jsonl
    encryption:
      key: ${vault:secret/faucet#dlq-key}   # high-entropy material, not a password
      # previous_keys: ["${env:OLD_KEY}"]   # rotation: read-only candidates
      # algorithm: aes-256-gcm              # default (and only) option
```

- The 32-byte AES key is derived as SHA-256 of the key string (a derivation,
  not a stretching KDF — source the key from a secrets manager).
- **Mutually exclusive with `compression`** — per-line sealed records cannot
  form a valid gzip/zstd stream; the conflict is a typed config error before
  any file is created.
- `faucet dlq inspect/replay/discard` read sealed DLQ files transparently
  (`--encryption-key`, or automatically from the config's `dlq:` block).
- The same `encryption` block seals `file` state-store bookmarks — see the
  state cookbook.


## Batch atomicity

What a failed write leaves behind (#737): **best-effort** — records are written to the file one by one. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

# faucet-sink-csv

> **Deprecated.** Use [`faucet-sink-file`](https://crates.io/crates/faucet-sink-file) instead.
> This crate gets security fixes only until the next major release. In a
> pipeline config, `type: csv` still works: the CLI builds it as
> `type: file` with `format: csv` and warns. The migration table is in
> the [`faucet-sink-file` README](https://github.com/faucet-hq/faucet-stream/blob/main/crates/sink/file/README.md).

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-csv.svg)](https://crates.io/crates/faucet-sink-csv)
[![Docs.rs](https://docs.rs/faucet-sink-csv/badge.svg)](https://docs.rs/faucet-sink-csv)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-csv.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-csv.svg)](https://github.com/faucet-hq/faucet-stream#license)

CSV / TSV file sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes JSON records to a delimited text file — comma, tab, semicolon, pipe, or any other single-byte delimiter — with an optional header row and RFC 4180 quoting handled automatically.

Reach for it whenever the destination is a spreadsheet, a BI import, a data-exchange handoff, or anything that consumes plain delimited text. All file I/O runs inside `tokio::task::spawn_blocking` and flows through a buffered `csv::Writer`, so it stays off the async runtime and doesn't bottleneck on per-record syscalls. This connector reports metrics under the observability label `connector="csv"`.

## Feature highlights

- **Any single-byte delimiter** — comma (CSV), tab (TSV), semicolon, pipe (`|`), or any other byte. One field controls the whole dialect.
- **RFC 4180 quoting, automatic** — the underlying `csv` crate quotes fields that contain the delimiter, quotes, or newlines, so values never break the layout. No quoting knobs to misconfigure.
- **Optional header row** — emit a header of column names (default) or write headerless data for append-style logs.
- **Stable, union-derived columns** — column order is the union of keys across every record in the first batch (first-seen order), so a field present only in a later record of that batch is still captured.
- **Visible late-field handling** — the header is frozen from the first batch and can't be rewritten in place; a field that first appears in a *later* batch can't become a column. Rather than silently dropping it, the sink emits a one-shot warning naming the dropped field(s), and `on_unknown_field: error` lets you fail the run instead.
- **Append or truncate** — append to an existing file for incremental exports, or truncate-on-open for a clean rewrite each run.
- **Creates missing parent dirs** — dated-subdirectory paths like `./data/dt=2026-03-08/part.csv` work with no `mkdir -p` step.
- **Buffered, off-runtime I/O** — every write runs in `spawn_blocking` behind a buffered writer; the async runtime is never blocked on disk.
- **Optional compression** — gzip / zstd output behind the `compression` feature, with auto-detection from the `.gz` / `.zst` suffix and an explicitly-finalised trailer (no silent corruption).

## Installation

```bash
# As a library:
cargo add faucet-sink-csv

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-csv

# With gzip / zstd output:
cargo install faucet-cli --features "sink-csv,compression"
```

The `sink-csv` feature is **not** in the CLI default build — enable it explicitly (or use a `full` build). Or pull it through the umbrella crate with `cargo add faucet-stream --features sink-csv`.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: sqlite
    config:
      database_url: sqlite://./data/app.db
      query: SELECT id, email, created_at FROM users ORDER BY id
  sink:
    type: csv
    config:
      path: ./out/users.csv
```

```bash
faucet run pipeline.yaml
```

Every row of the query result is written to `./out/users.csv` with a header row, comma-delimited, RFC 4180-quoted. The `out/` directory is created if it doesn't exist.

## Configuration reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | *(required)* | Path to the output file. Missing parent directories are created automatically (`mkdir -p`). |
| `delimiter` | byte (int) | `44` (`,`) | Single-byte field delimiter. Common values: `44` comma, `9` tab, `59` semicolon, `124` pipe. |
| `write_headers` | bool | `true` | Write a header row of column names on the first open. Skipped automatically on append-mode re-opens. |
| `append` | bool | `false` | Append to an existing file instead of truncating it on open. When `false`, the file is truncated on the first open. |
| `batch_size` | int | `1000` | Records per upstream `StreamPage`. **No behavioural impact at this sink** — present only for config parity. See [Streaming & batching](#streaming--batching). |
| `on_unknown_field` | `warn` \| `error` | `warn` | What to do when a record carries a field that is **not** in the frozen column set (the header is fixed from the first batch and cannot be extended). `warn` — emit a one-shot warning naming the dropped field(s) and keep writing (the value is dropped). `error` — abort the write with `FaucetError::Sink` the first time any such field appears. See [Column order & missing fields](#column-order--missing-fields). |
| `compression` | `none` \| `gzip` \| `zstd` \| `auto` | `auto` | Output compression. Requires the `compression` feature. `auto` infers from the `.gz` / `.zst` path suffix. See [Compression](#compression). |

In YAML/JSON `delimiter` is the **byte value** of the character, not the character itself — e.g. `9` for tab, `59` for semicolon. The library builder takes a `u8` (`b'\t'`).

## Examples

### Comma-separated export with headers

```yaml
version: 1
name: sqlite_to_csv
pipeline:
  source:
    type: sqlite
    config:
      database_url: sqlite://./data/app.db
      query: SELECT id, email, created_at FROM users ORDER BY id
  sink:
    type: csv
    config:
      path: ./out/users.csv
      delimiter: 44       # ','
      write_headers: true
      append: false
```

### Tab-separated values (TSV)

```yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      url: https://api.example.com/v1/orders
  sink:
    type: csv
    config:
      path: ./out/orders.tsv
      delimiter: 9        # tab
```

### Append mode for incremental / streaming exports

A webhook receiver appending every batch to one growing file — header written once, subsequent runs append:

```yaml
version: 1
name: webhook_to_csv
pipeline:
  source:
    type: webhook
    config:
      listen_addr: 127.0.0.1:9090
      path: /inbox
      max_payloads: 5000
      timeout_secs: 120
  sink:
    type: csv
    config:
      path: webhooks.csv
      delimiter: 59       # ';'
      write_headers: true
      append: true
```

### Dated partition path with gzip compression

```yaml
version: 1
pipeline:
  source:
    type: postgres
    config:
      connection_url: ${env:DATABASE_URL}
      query: SELECT * FROM events WHERE created_at::date = current_date
  sink:
    type: csv
    config:
      path: ./data/dt=${now.date}/events.csv.gz   # parent dir auto-created
      compression: auto                           # .gz suffix → gzip
```

## Streaming & batching

`Pipeline::run` drives the source's `stream_pages` and hands each emitted `StreamPage` to `Sink::write_batch`. This sink then iterates the page **record by record**, serializing and writing each row through a buffered `csv::Writer` inside `spawn_blocking`.

Because the write path is inherently per-record, the `batch_size` config field has **no observable effect** here. `batch_size = 0` (the "no batching" sentinel) and any positive value produce byte-for-byte identical output. The field exists only so every sink in the workspace shares one config shape — sinks like `faucet-sink-postgres` (multi-row INSERTs) or `faucet-sink-bigquery` (streaming-insert request sizing) genuinely use it; a per-record file sink has nothing to tune.

The per-run memory bound is set by the **source's** `batch_size` (the size of each `StreamPage`), not by this field.

### Column order & missing fields

- **Column order** is the union of keys across **all records in the first** `write_batch` call, in first-seen order — a field present only in a later record of that batch is still captured.
- All subsequent records use that same column order, regardless of their own key order.
- A record **missing** a column writes an empty string for that cell.
- Keys appearing only in a **later** `write_batch` call (after the header is fixed) **cannot be added to the header** — the header is written once, on the first call, and a CSV file can't be retroactively widened without rewriting the whole file. Such a field's value is **dropped** from the output. How that drop is surfaced is governed by `on_unknown_field`:
  - `warn` (default) — a single `tracing::warn!` per run names the dropped field(s), then writing continues with those values dropped. This keeps the streaming/append contract intact while making the loss **visible** instead of silent.
  - `error` — the write aborts with `FaucetError::Sink` (naming the offending field) the first time any record carries a field outside the frozen column set. Opt into this when silent column-level data loss is unacceptable.
  - Either way, normalize the record shape upstream (e.g. `select` / `set` transforms) — or sort the source so a fully-populated record lands in the first batch — if every column must be present.
- A non-object record (bare scalar or array) is rejected with `FaucetError::Sink` — every record must be a JSON object.

### Value conversion

| JSON type | CSV cell |
|-----------|----------|
| `null` | empty string |
| `string` | the string value |
| `number` | string representation |
| `boolean` | `true` / `false` |
| `object` / `array` | compact JSON (e.g. `{"nested":true}`) |

## Compression

Behind the crate-local `compression` Cargo feature. Adds the `compression` config field with values `none`, `gzip`, `zstd`, or `auto` (the default — selects gzip / zstd from the `.gz` / `.zst` path suffix).

```yaml
type: csv
config:
  path: ./out/users.csv.gz
  compression: auto    # or 'gzip' | 'zstd' | 'none'
```

`flush()` explicitly finalises the compression encoder (writing the gzip / zstd trailer) and propagates any trailer-write I/O error rather than swallowing it on drop — so a pipeline bookmark never advances over a truncated or corrupt file. Each mid-stream flush finalises one member; the next write starts a fresh member appended after it, which gzip / zstd decoders read back transparently.

S3 / GCS-style `Content-Encoding` headers don't apply here — this is a local-file sink; consumers decompress by file suffix.

## Config loading & schema

Configs load from YAML/JSON files, environment variables, or `.env` files via the CLI's normal loading path.

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_csv::CsvSinkConfig;

# fn run() -> Result<(), faucet_core::FaucetError> {
// From a JSON file
let config: CsvSinkConfig = load_json("config.json")?;

// From an .env file with a prefix (CSV_SINK_PATH, CSV_SINK_DELIMITER, …)
let config: CsvSinkConfig = load_env_file(".env", "CSV_SINK")?;
# Ok(())
# }
```

Inspect the full JSON Schema with:

```bash
faucet schema sink csv
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_csv::{CsvSink, CsvSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), faucet_core::FaucetError> {
let config = CsvSinkConfig::new("./out/users.csv")
    .delimiter(b'\t')        // TSV
    .write_headers(true)
    .append(false);

let sink = CsvSink::new(config);

sink.write_batch(&[
    json!({ "id": 1, "name": "Alice", "email": "alice@example.com" }),
    json!({ "id": 2, "name": "Bob",   "email": "bob@example.com" }),
])
.await?;
sink.flush().await?;       // always flush before dropping the sink
# Ok(())
# }
```

Wired into a `Pipeline` with any source:

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_csv::{CsvSink, CsvSinkConfig};

# async fn run() -> Result<(), faucet_core::FaucetError> {
let source = RestStream::new(RestStreamConfig::new("https://api.example.com", "/v1/users"));
let sink = CsvSink::new(CsvSinkConfig::new("./out/users.csv"));

let result = Pipeline::new(source, sink).run().await?;
println!("Exported {} records to CSV", result.records_written);
# Ok(())
# }
```

The builder methods are `delimiter(u8)`, `write_headers(bool)`, `append(bool)`, `with_batch_size(usize)`, `on_unknown_field(OnUnknownField)`, and `compression(CompressionConfig)` (the last only with the `compression` feature).

## How it works

1. The file is opened **lazily** on the first `write_batch` call. Column order is computed from the union of keys across that first batch's records.
2. The first open obeys `config.append` (truncate when `false`). A `Mutex` guards the writer state (column order + `csv::Writer`) for thread-safe access.
3. Each record's cells are written in column order; missing fields become empty strings, and the `csv` crate applies RFC 4180 quoting automatically.
4. All CSV I/O runs inside `tokio::task::spawn_blocking` so the async runtime is never blocked; the writer state is moved in and out of the `Mutex` across the blocking boundary (never held across an await).
5. `flush()` finalises the writer (and the compression encoder, with its error surfaced) and **clears the writer slot**. A subsequent `write_batch` reopens the file in **append mode regardless of `config.append`**, so the pipeline's per-bookmark flush is safe for CDC-style sources — every transaction appends rather than truncates. The header is written only on the very first open.
6. The default `Sink` impl does **not** flush on `Drop` — always call `flush()` before dropping the sink or reading the file.
7. `check()` (for `faucet doctor`) verifies the parent directory exists and is writable by creating then removing a temp file there; it never touches your actual output file.

## Lineage dataset URI

`file://<path>` — e.g. `file:///tmp/output.csv`.

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

| Feature | Default | Description |
|---------|---------|-------------|
| `compression` | off | Adds the `compression` config field (gzip / zstd) via `faucet-core/compression`. |

Enable the connector itself in the CLI / umbrella via the `sink-csv` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `CSV sink expects JSON objects, got non-object record` | The source emits bare scalars or arrays. CSV needs columnar objects — shape records into objects upstream (e.g. a `set` / `select` transform) before this sink. |
| A column present in some rows is missing from the file | The header is fixed from the **first** batch. A key that first appears in a *later* batch is dropped (the CSV header can't be extended in place). The sink emits a one-shot `dropping field(s) not in the frozen CSV column set` warning naming it. Normalize the record shape upstream so every expected column is present in the first batch (e.g. `select`/`set`), sort the source so a fully-populated record comes first, or set `on_unknown_field: error` to fail loudly instead of dropping silently. |
| Output file is empty or truncated | The sink buffers; you must `flush()` before reading or exiting. With compression, an un-flushed file is missing its trailer and won't decode. |
| Delimiter setting seems ignored | `delimiter` is the **byte value** (e.g. `9` for tab, `59` for semicolon), not the literal character. Setting `delimiter: ","` in YAML is wrong; use `delimiter: 44`. |
| Header keeps repeating across runs | You're truncating each run but expected append, or vice versa. Set `append: true` + `write_headers: false` for incremental log-style files; the header is written only on the first (truncating) open. |
| `failed to open CSV file …` / `failed to create parent directory …` | The path isn't writable. The sink creates parent dirs but can't fix permissions — check directory ownership/mode, or run `faucet doctor` to probe writability. |
| Setting `batch_size` changes nothing | Expected — `batch_size` is a no-op for this per-record sink (present only for config parity). To bound per-page memory, set the **source's** `batch_size`. |
| Fields with commas/newlines look fine but a downstream parser chokes | Output is RFC 4180-quoted. Ensure the consumer uses a real CSV parser (and the same delimiter), not a naive split-on-comma. |
| `.gz` / `.zst` output isn't compressed | The `compression` feature isn't enabled, so the `compression` field is absent and ignored. Rebuild with `--features "sink-csv,compression"`. |

## See also

- [Connectors reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — the full connector capability matrix.
- [Compression cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/compression.html) — gzip / zstd across file sinks.
- [CLI reference](https://faucet-hq.github.io/faucet-stream/reference/cli.html) — `faucet run`, `faucet schema`, `faucet doctor`.
- [`faucet-sink-jsonl`](https://crates.io/crates/faucet-sink-jsonl) — write JSON Lines to a file instead of delimited text.
- [`faucet-sink-stdout`](https://crates.io/crates/faucet-sink-stdout) — TSV/JSON to a standard stream for debugging and piping.
- [`faucet-source-csv`](https://crates.io/crates/faucet-source-csv) — read CSV files as a pipeline source.
- [`faucet-core`](https://crates.io/crates/faucet-core) — the `Sink` trait this connector implements.

## Batch atomicity

What a failed write leaves behind (#737): **best-effort** — records are written to the file one by one. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

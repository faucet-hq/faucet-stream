# faucet-sink-stdout

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-stdout.svg)](https://crates.io/crates/faucet-sink-stdout)
[![Docs.rs](https://docs.rs/faucet-sink-stdout/badge.svg)](https://docs.rs/faucet-sink-stdout)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-stdout.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-stdout.svg)](https://github.com/faucet-hq/faucet-stream#license)

Standard-stream (**stdout / stderr**) sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes each record to a standard stream in one of four formats — compact JSON Lines, pretty-printed JSON, tab-separated values, or RFC-4180 CSV.

Reach for it whenever you want to *see* what a pipeline produces: debugging a new source, eyeballing the shape of records before wiring up a real destination, building a quick demo, sampling the first few rows with `faucet preview`, or piping records into another shell tool (`jq`, `head`, `grep`, `column`). It's the zero-setup sink — no credentials, no connection, no files — so it's almost always the first sink you point a new source at.

## Feature highlights

- **Four output formats** — `json_lines` (one compact object per line, the de-facto debug format), `pretty_json` (indented and readable), `tsv` (tab-separated, spreadsheet-friendly), and `csv` (RFC-4180 comma-separated).
- **Pick the stream** — write to `stdout` (default, honors shell redirection) or `stderr` (when stdout is reserved for piping the pipeline's own output).
- **Buffered, async writes** — records go through a buffered `tokio` async writer, so high-volume runs aren't bottlenecked on per-record syscalls.
- **Live-preview flushing** — `flush_per_record` flushes after every record for low-latency, line-at-a-time output (e.g. tailing a streaming source).
- **Record cap** — `max_records` stops after *N* records, which is exactly what powers `faucet preview --limit N`.
- **Pipe-friendly** — a `BrokenPipe` from the consumer (e.g. `… | head`) is treated as a clean stop, not an error, so piping into truncating commands Just Works.
- **Writer override** — `StdoutSink::with_writer` accepts any `Box<dyn AsyncWrite + Unpin + Send>`, handy for tests, in-memory capture, or redirecting into a log file.

## Installation

```bash
# As a library:
cargo add faucet-sink-stdout

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-stdout
```

The `sink-stdout` feature is **not** in the CLI default build — enable it explicitly (or use a `full` build).

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: csv
    config:
      path: ./users.csv
  sink:
    type: stdout
    config:
      format: json_lines
```

```bash
faucet run pipeline.yaml
```

Every row of `users.csv` is printed to stdout as one compact JSON object per line — ready to pipe into `jq`, redirect to a file, or just read.

## Configuration reference

All fields are optional; an empty `config: {}` writes JSON Lines to stdout with no limit.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `destination` | `stdout` \| `stderr` | `stdout` | Which standard stream to write to. `stderr` is useful when stdout is reserved for the pipeline's own structured output. |
| `format` | `json_lines` \| `pretty_json` \| `tsv` \| `csv` | `json_lines` | How each record is serialized — see [Formats](#formats). |
| `flush_per_record` | bool | `false` | Flush the underlying writer after every record instead of at batch boundaries. Lower latency for live preview, slightly lower throughput. |
| `max_records` | int | *(unset → unlimited)* | Stop after this many records. Once reached, subsequent `write_batch` calls become no-ops. Powers `faucet preview --limit N`. |
| `batch_size` | int | `1000` | Records per upstream `StreamPage`. **No behavioural impact at this sink** — present only for config parity across the workspace. See [Streaming & batching](#streaming--batching). |

### Formats

| `format` | Output |
|----------|--------|
| `json_lines` | One compact JSON object per line (newline-delimited). The standard debug / pipe-into-`jq` format. |
| `pretty_json` | Indented (pretty-printed) JSON, each record separated by a newline. Easier to read by eye; not a single-line format. |
| `tsv` | Tab-separated values. Each record's keys are sorted alphabetically; scalar values are emitted raw (control characters in strings replaced by spaces), and nested objects/arrays are emitted as compact JSON. **Requires every record to be a JSON object.** |
| `csv` | RFC-4180 comma-separated values. Same column resolution as `tsv` (keys sorted, one line per record, no header row), but the `csv` crate handles quoting/escaping so values containing commas, quotes, or newlines round-trip; nested objects/arrays are emitted as compact JSON. Line terminator is `\n` (not CRLF), matching the other formats. **Requires every record to be a JSON object.** |

## Examples

### Tail a streaming source live, line by line

```yaml
version: 1
pipeline:
  source:
    type: websocket
    config:
      url: wss://stream.example.com/ticks
  sink:
    type: stdout
    config:
      format: json_lines
      flush_per_record: true   # emit each record the instant it arrives
```

### Preview just the first few rows on stderr

```yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      url: https://api.example.com/v1/orders
  sink:
    type: stdout
    config:
      destination: stderr      # keep stdout free for other tooling
      format: pretty_json
      max_records: 5           # sample the shape, then stop
```

### Emit TSV to pipe into spreadsheet-style tools

```yaml
version: 1
pipeline:
  source:
    type: postgres
    config:
      connection_url: ${env:DATABASE_URL}
      query: SELECT id, name, signup_date FROM users ORDER BY id
  sink:
    type: stdout
    config:
      format: tsv
```

```bash
faucet run pipeline.yaml | column -t -s $'\t'
```

## Streaming & batching

`Pipeline::run` drives the source's `stream_pages` and hands each emitted `StreamPage` to `Sink::write_batch`. This sink then iterates the page **record by record**, serializing and writing each one through a buffered async writer.

Because the write path is inherently per-record, the `batch_size` config field has **no observable effect** here. `batch_size = 0` (the "no batching" sentinel) and any positive value produce byte-for-byte identical output. The field exists only so every sink in the workspace shares one config shape — sinks like `faucet-sink-postgres` (multi-row INSERTs) or `faucet-sink-bigquery` (streaming-insert request sizing) genuinely use it; a per-record stream sink has nothing to tune.

The per-run memory bound is set by the **source's** `batch_size` (the size of each `StreamPage`), not by this field. To control flush cadence for live preview, use `flush_per_record` — `batch_size` does not influence flushing.

## Config loading & schema

Configs load from YAML/JSON files, environment variables, or `.env` files via the CLI's normal loading path. Inspect the full JSON Schema with:

```bash
faucet schema sink stdout
```

## Library usage

```rust
use faucet_core::Sink;
use faucet_sink_stdout::{StdStream, StdoutFormat, StdoutSink, StdoutSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), faucet_core::FaucetError> {
let sink = StdoutSink::new(
    StdoutSinkConfig::new()
        .destination(StdStream::Stdout)
        .format(StdoutFormat::JsonLines)
        .max_records(100),
);

sink.write_batch(&[
    json!({"id": 1, "name": "Alice"}),
    json!({"id": 2, "name": "Bob"}),
])
.await?;
sink.flush().await?;
# Ok(())
# }
```

Capture output into an in-memory buffer (e.g. in tests) with the writer override:

```rust
use faucet_core::Sink;
use faucet_sink_stdout::{StdoutSink, StdoutSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), faucet_core::FaucetError> {
let buf: Vec<u8> = Vec::new();
let sink = StdoutSink::with_writer(StdoutSinkConfig::new(), Box::new(buf));
sink.write_batch(&[json!({"hello": "world"})]).await?;
sink.flush().await?;
# Ok(())
# }
```

## How it works

1. `new()` opens the buffered async writer for the configured `destination` **eagerly** (at construction, not on first write).
2. `write_batch` iterates the page, serializing each record per the chosen `format` and writing it through the buffer. When `max_records` is set, it stops once the cap is hit and turns further writes into no-ops.
3. With `flush_per_record`, the buffer is flushed after every record; otherwise it flushes at batch boundaries and on `flush()`.
4. A `BrokenPipe` error from the consumer is caught and treated as a clean termination — subsequent `write_batch` calls return `Ok(0)` rather than erroring.
5. The default `Sink` impl does **not** flush on `Drop`, so call `flush()` explicitly if you need buffered output durably written before the program exits.

## Lineage dataset URI

`stdout://` or `stderr://`, depending on the configured destination.

This connector reports metrics under the label `connector="stdout"`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `sink-stdout` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `tsv` format errors on some records | TSV requires every record to be a JSON **object**. A source emitting bare scalars or arrays can't be flattened to columns — switch to `json_lines`, or shape records into objects with a transform first. |
| Output looks buffered / nothing prints until the run ends | The writer buffers by default. For line-at-a-time output (e.g. tailing a live source), set `flush_per_record: true`. |
| Setting `batch_size` changes nothing | Expected — `batch_size` is a no-op for this per-record sink (present only for config parity). To bound per-page memory, set the **source's** `batch_size`; to control flush cadence, use `flush_per_record`. |
| Pipeline stops early when piping into `head` / `sed` | Not an error — the consumer closed the pipe (`BrokenPipe`), which this sink treats as a clean stop. Remove the truncating command, or raise its limit, to see all records. |
| TSV column order changes between records | Keys are sorted **alphabetically** per record for stable columns. If different records have different key sets, their columns won't line up — normalize the shape upstream (e.g. `select` / `set` transforms). |
| Want only the first N records | Set `max_records: N` (this is what `faucet preview --limit N` uses), rather than relying on a downstream `head`. |
| Pretty JSON is hard to pipe into `jq` | Use `format: json_lines` for tooling that expects newline-delimited JSON; reserve `pretty_json` for human reading. |

## See also

- [Sinks reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — the full connector capability matrix.
- [CLI reference](https://faucet-hq.github.io/faucet-stream/reference/cli.html) — `faucet run`, `faucet preview`, `faucet schema`.
- [`faucet-sink-jsonl`](https://crates.io/crates/faucet-sink-jsonl) — write the same JSON Lines to a file instead of a stream.
- [`faucet-sink-csv`](https://crates.io/crates/faucet-sink-csv) — TSV/CSV to a file with full quoting.
- [`faucet-core`](https://crates.io/crates/faucet-core) — the `Sink` trait this connector implements.

## Batch atomicity

What a failed write leaves behind (#737): **best-effort** — records are written one by one. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

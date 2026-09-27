# faucet-source-csv

[![Crates.io](https://img.shields.io/crates/v/faucet-source-csv.svg)](https://crates.io/crates/faucet-source-csv)
[![Docs.rs](https://docs.rs/faucet-source-csv/badge.svg)](https://docs.rs/faucet-source-csv)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-csv.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-csv.svg)](https://github.com/faucet-hq/faucet-stream#license)

A **CSV file source** that reads delimited text files and yields each row as a `serde_json::Value` object. Part of the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

Built on the streaming RFC-4180 `csv-async` reader so the file is consumed lazily, line by line, instead of being slurped into memory — a multi-gigabyte export streams through with memory bounded by `batch_size`, not file size. Reach for it to load CSV/TSV/pipe-delimited exports, spreadsheet dumps, or legacy flat files into any faucet-stream sink.

## Feature highlights

- **True streaming reader** — `Source::stream_pages` reads from a `tokio` async reader and emits fixed-size pages; client-side memory is O(`batch_size`), not O(file size).
- **Configurable dialect** — set the field `delimiter` and `quote` characters (byte values) for CSV, TSV, pipe-delimited, or any single-char-delimited format.
- **Header or headerless** — with headers, columns are keyed by the header row; without, keys are generated as `column_0`, `column_1`, …. Duplicate header names (including two blank-named columns) are **rejected with `FaucetError::Config`** at read time — they would otherwise silently overwrite each other last-wins and drop column data for the whole run.
- **Strict by default** — a row whose field count differs from the header (a ragged/truncated line) **aborts the run with `FaucetError::Source`** naming the offending line, rather than silently emitting a record missing columns. Opt in to lenient parsing with `flexible: true` when ragged rows are expected.
- **RFC-4180 correct** — quoted fields containing embedded delimiters *and* embedded newlines parse as a single record, so files written by `faucet-sink-csv` round-trip losslessly.
- **Transparent compression** — opt-in `compression` feature reads `.gz` / `.zst` files, with auto-detection from the path suffix.
- **No type inference** — every field is returned as a JSON string; cast downstream with the `cast` transform if you need typed values.

## Installation

```bash
# As a library:
cargo add faucet-source-csv
cargo add tokio --features full

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-csv

# Via the umbrella crate:
cargo add faucet-stream --features source-csv
```

`source-csv` is not in the CLI default build — enable it explicitly (or use the `source` / `full` aggregate features).

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

With a header row of `id,name,email`, the file produces records like
`{"id": "1", "name": "Alice", "email": "alice@example.com"}`.

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | — *(required)* | Path to the CSV file on the local filesystem. |
| `has_headers` | bool | `true` | Whether the first row is a header. `true` → header names become object keys. `false` → keys are generated `column_0`, `column_1`, …. |
| `delimiter` | int (byte) | `44` (`,`) | Field delimiter, as a byte value. `9` = tab, `124` = pipe (`\|`), `59` = semicolon. |
| `quote` | int (byte) | `34` (`"`) | Quote character, as a byte value. `39` = single quote (`'`). |
| `flexible` | bool | `false` | Whether to tolerate rows whose field count differs from the header (or, when headerless, from the first data row). **Strict by default**: a ragged row aborts the run with `FaucetError::Source` naming the line — silently emitting an incomplete record would corrupt downstream data. Set `true` to accept short rows (records missing trailing columns) and long rows (extra `column_N` keys). |
| `null_values` | list of strings | `[]` | Fields equal to any listed string decode as JSON `null` instead of a string (`[""]` for empty-as-null, or sentinels such as `"NULL"`). Headers are never mapped; a quoted empty field maps like an unquoted one. |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Rows per emitted `StreamPage` in `Source::stream_pages`. **`0` = no batching**: the entire file is drained into a single page (handy for small lookup tables or load-job sinks that prefer one large request). Capped at `MAX_BATCH_SIZE` (1,000,000); validated at config-load time (an empty `path` is likewise rejected with `FaucetError::Config`). |

### Format

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `compression` | enum | `auto` | `none` \| `gzip` \| `zstd` \| `auto`. **Requires the `compression` feature.** `auto` selects the codec from the path suffix (`.gz` → gzip, `.zst` → zstd, otherwise none). |

> Byte fields (`delimiter`, `quote`) are integers in JSON/YAML. Common values: `44` comma, `9` tab, `124` pipe, `59` semicolon, `34` double-quote, `39` single-quote.

## Examples

### TSV (tab-separated) file

```yaml
source:
  type: csv
  config:
    path: /data/export.tsv
    delimiter: 9          # tab
```

### Headerless, pipe-delimited legacy export

```yaml
source:
  type: csv
  config:
    path: /data/legacy_export.csv
    has_headers: false
    delimiter: 124        # pipe |
    quote: 39             # single quote '
```

Rows become `{"column_0": "...", "column_1": "...", ...}`.

### Cast string fields to typed values en route to SQLite

```yaml
version: 1
name: csv_to_sqlite

pipeline:
  source:
    type: csv
    config:
      path: ./data/customers.csv

  transforms:
    - type: cast
      config:
        field: age
        to: integer

  sink:
    type: sqlite
    config:
      connection_url: sqlite://./customers.db
      table: customers
      auto_map: true
```

### Tolerate ragged rows (opt-in lenient parsing)

```yaml
source:
  type: csv
  config:
    path: /data/messy_export.csv
    flexible: true        # accept rows with uneven field counts
```

Without `flexible: true` (the default), a row shorter or longer than the
header aborts the run with `FaucetError::Source` naming the offending line.

### Compressed file (gzip), one page per file

```yaml
source:
  type: csv
  config:
    path: /data/events.csv.gz
    compression: auto      # detects .gz; explicit `gzip` also works
    batch_size: 0          # emit the whole file as one page
```

## Streaming & batching

`CsvSource` implements `Source::stream_pages` as a real client-side stream. It opens the file through `tokio::fs::File`, wraps it in a `csv_async::AsyncReaderBuilder` configured with the dialect (`delimiter` / `quote` / `has_headers`), and reads records incrementally — buffering up to `batch_size` rows before yielding a page. Because `csv-async` is a streaming RFC-4180 reader, it tracks quote state across physical lines, so quoted fields with embedded newlines and delimiters (e.g. `"hello, world"` or a multi-line cell) parse correctly as a single record.

`batch_size = 0` is the **no-batching sentinel**: the file is fully drained and emitted in one `StreamPage`. Use it for small lookup tables, or for sinks (SQL `COPY`, BigQuery load jobs) that prefer one large request over many small ones.

`fetch_all` / `fetch_with_context` are convenience methods that collect every page into a single `Vec<serde_json::Value>` by draining `stream_pages`.

Every page carries `bookmark: None` — there is no incremental-replication mode for a flat file (the whole file is read on each run).

## Compression

Behind the crate-local `compression` Cargo feature, the source gains a `compression` config field (`none` / `gzip` / `zstd` / `auto`, default `auto`). Auto-detection consults the path suffix at I/O time — `.gz` selects gzip, `.zst` selects zstd, anything else is treated as uncompressed. The streaming reader decompresses on the fly, so memory stays bounded and multi-line quoted records still parse correctly. Enable it with `cargo install faucet-cli --features "source-csv compression"` or `cargo add faucet-source-csv --features compression`.

## Config loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_csv::CsvSourceConfig;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let config: CsvSourceConfig = load_json("config.json")?;
let config: CsvSourceConfig = load_env_file(".env", "CSV_SOURCE")?;
# Ok(()) }
```

JSON config:

```json
{
  "path": "/data/exports/customers.csv",
  "has_headers": true,
  "delimiter": 44,
  "quote": 34,
  "batch_size": 1000
}
```

`.env` file:

```env
CSV_SOURCE_PATH=/data/exports/customers.csv
CSV_SOURCE_HAS_HEADERS=true
CSV_SOURCE_DELIMITER=44
CSV_SOURCE_QUOTE=34
```

## Schema introspection

```bash
faucet schema source csv
```

Prints the JSON Schema for `CsvSourceConfig` — the authoritative field list, types, and defaults.

## Library usage

```rust,no_run
use faucet_source_csv::{CsvSource, CsvSourceConfig};
use faucet_core::Source;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
// TSV without a header row
let config = CsvSourceConfig::new("/data/export.tsv")
    .has_headers(false)
    .delimiter(b'\t')
    .with_batch_size(5000);

let source = CsvSource::new(config);
let records = source.fetch_all().await?;

for record in &records {
    println!("{record}");
}
# Ok(()) }
```

To run it through a pipeline, pair the source with any sink and drive it via
`faucet_core::Pipeline` or `faucet_core::run_stream`.

## How it works

- The source holds only the parsed `CsvSourceConfig`; there is no client or pool to reuse, since reads are local-filesystem I/O.
- `stream_pages` builds one `csv_async::AsyncReader` per run and pulls records through it incrementally, so the whole file is never resident in memory.
- All values are emitted as JSON strings — the reader does no type inference. Use the `cast` transform to coerce numbers/booleans downstream.
- Empty rows and the trailing newline are handled by the underlying RFC-4180 reader; an empty file yields zero records (not an error).

## Lineage dataset URI

`file://<path>` — e.g. `file:///data/input.csv`.

## Feature flags

| Feature | Default | Effect |
|---------|---------|--------|
| `compression` | off | Adds the `compression` config field and `.gz` / `.zst` read support (pulls `faucet-core/compression`). |

The connector itself is enabled in the CLI/umbrella via the `source-csv` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `FaucetError::Source`: file not found / permission denied | `path` is wrong or unreadable. Check the path (relative paths resolve against the process working directory) and file permissions. |
| Every value is a string, including numbers | Intentional — the CSV reader does no type inference. Add a `cast` transform (`to: integer` / `float` / `boolean`) for fields you need typed. |
| Columns named `column_0`, `column_1`, … unexpectedly | `has_headers` is `false` (or defaulted off in an env config). Set `has_headers: true` so the first row supplies keys. |
| Fields split in the wrong place | Wrong `delimiter`. Set the byte value for your format — `9` (tab), `124` (pipe), `59` (semicolon). |
| Quoted text with commas is being split | The `quote` byte doesn't match the file. Set `quote` to the file's quote character (`34` double, `39` single). |
| Multi-line cell breaks into several records | The cell isn't actually quoted, so the reader can't tell the embedded newline from a record boundary. Ensure the producer quotes fields containing newlines (faucet-sink-csv does). |
| `FaucetError::Config` on `batch_size` | `batch_size` exceeds `MAX_BATCH_SIZE` (1,000,000). Lower it, or use `0` for no batching. |
| `.gz` / `.zst` file read as raw bytes / garbage | The `compression` feature isn't enabled. Rebuild with `--features compression` (or set `compression: gzip` explicitly). |
| A header name appears as a key but with surrounding whitespace | Header cells are taken verbatim. Trim upstream or rename with the `rename_keys` transform. |
| `FaucetError::Config`: `duplicate CSV header …` | The header row repeats a name (or has two blank-named columns). Keying each row by header name would silently drop the earlier column, so this is rejected. Rename the duplicate column, or set `has_headers: false` to fall back to generated `column_N` keys. |
| `FaucetError::Source`: `ragged CSV row at line …` | A data row has a different field count than the header (truncated or extra-column line). Strict mode rejects it rather than emitting an incomplete record. Fix the offending line, or set `flexible: true` to accept uneven rows. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [Compression cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/compression.html) · [Transforms cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/transforms.html)
- Related crates: [faucet-sink-csv](https://crates.io/crates/faucet-sink-csv) · [faucet-source-parquet](https://crates.io/crates/faucet-source-parquet) · [faucet-source-s3](https://crates.io/crates/faucet-source-s3)

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Observability

Metrics emitted by this source are labelled `connector="csv"`.

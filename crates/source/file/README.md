# faucet-source-file

The general local file source for [faucet-stream](https://github.com/faucet-hq/faucet-stream):
read **JSON Lines, JSON, CSV, Excel, XML, Parquet, Avro and ORC** from a file, a
directory, a glob, or a single `http://` / `https://` URL. It is the local
counterpart of the S3, GCS and Azure Blob sources and uses the same shared
format layer, so a file reads the same here as it does from a bucket.

`faucet-source-csv` stays as it was for existing configs. Use this crate for
anything new, CSV included.

```toml
[dependencies]
faucet-source-file = { version = "1.0.0", features = ["file-formats"] }
```

```yaml
source:
  type: file
  config:
    path: ./exports            # a file, a directory, a glob, or an http(s) URL
    recursive: true
    format: auto               # resolved per file from the extension
    compression: auto          # .gz / .zst resolved per file
    incremental: { by: mtime } # re-runs read only new files (needs `state:`)
    stable_for_secs: 30        # skip files still being written
```

## Paths

| `path` | Reads |
|---|---|
| `data/orders.jsonl` | that file |
| `data/` | the directory's regular files; subdirectories too with `recursive: true` |
| `data/**/*.csv.gz` | every file the glob matches |
| `https://example.com/export.csv?sig=…` | one remote file, fetched with `reqwest` and streamed |

Every listing is sorted by path, so runs are deterministic. Symlinks are
followed. An unreadable file or directory fails the run with its path.

`{key}` placeholders in `path` are filled from the fetch context (a library
caller's `fetch_with_context`), as the `csv` and `parquet` sources do.

## Formats

`format: auto` (the default) picks a format per file from its extension,
looking through a compression suffix, so `export.csv.gz` is gzip-compressed CSV:

| Extension | Format | Read |
|---|---|---|
| `.jsonl`, `.ndjson` | JSON Lines | streamed line by line |
| `.json` | JSON array (a lone object is one record) | whole file |
| `.csv` | CSV (dialect from `csv:`) | streamed row by row |
| `.xml` | XML (framing from `xml:`) | whole file |
| `.xlsx` | Excel (sheet from `excel:`) | whole file |
| `.parquet` | Parquet (projection from `parquet:`) | streamed by row group |
| `.avro` | Avro Object Container File (reader schema from `avro:`) | streamed by block |
| `.orc` | ORC (projection from `orc:`) | streamed by stripe |
| `.txt` | raw text: one record `{path, content}` per file | whole file |

With `format: auto`, a file whose extension names no format is skipped with a
warning. `strict: true` fails the run on such a file instead. An explicit
`format:` applies to every file whatever its name.

A directory mixing formats reads in one run. Avro files are resolved against
the first Avro file's schema, or against `avro.schema` when you set it. A later
file whose schema cannot be resolved against it fails with an error naming both
files. Parquet and ORC files must share one schema, and a mismatch likewise
names both files. For local Parquet files the schemas are compared from the
footers before any row is read, so a mismatch in a late file never leaves
earlier files half-delivered.

### CSV

| `csv.` field | Default | Meaning |
|---|---|---|
| `delimiter` | `","` | One byte; `"\t"` for tabs. |
| `has_headers` | `true` | The first row names the fields; otherwise `column_0`, `column_1`, …. A repeated header name fails the read (it would drop a column). |
| `quote` | `"\""` | The quote character. |
| `flexible` | `false` | Accept rows with more or fewer fields than the header. Off, the first ragged row fails the read naming its line. |
| `null_values` | `[]` | Cell values read as `null` (e.g. `["", "NULL"]`). |

Values are strings; cast them with a `cast` transform.

### Parquet

`parquet.columns` projects the read: only those column chunks are decoded, on
both the row and the columnar path, and a name a file does not have fails
naming the columns it does have. Nulls are explicit: a null column is read as
`"key": null` (the `parquet` source omitted the key).

Compression (`compression: auto | gzip | zstd | none`) resolves per file from
its suffix. A compressed Parquet, Avro or ORC file is decompressed into memory
before decoding, because those formats need random access or a whole stream.

## Encryption

`encryption: { key, previous_keys }` (feature `encryption`) reads files the
`file` or `jsonl` sink encrypted: a file sealed whole is decrypted and then
decompressed; JSON Lines or raw text sealed line by line is decrypted a line at
a time. A file or line that is not sealed fails the read rather than being
trusted as plaintext.

## Coming from the csv or parquet source

| Old field | File source |
|---|---|
| csv `path`, `has_headers`, `delimiter`, `quote`, `flexible`, `null_values`, `batch_size`, `compression` | `path`, `csv.has_headers`, `csv.delimiter`, `csv.quote`, `csv.flexible`, `csv.null_values`, `batch_size`, `compression` |
| parquet `source: {type: local_path, path}` / `{type: glob, pattern}` | `path` (a file, directory or glob) |
| parquet `columns`, `batch_size`, `concurrency` | `parquet.columns`, `batch_size`, `concurrency` |

Golden tests (`crates/interop-tests/tests/file_source_parity.rs`) read the same fixtures through the old sources
and the file source and compare the records.

## Columnar path

With an explicit `format: avro`, `orc` or `parquet`, the source advertises the
Arrow columnar path. A pipeline into a columnar sink (Parquet, Delta and
others) then moves `RecordBatch`es and never builds JSON rows. Avro logical
types arrive typed: `decimal` as `Decimal128`, `date` as `Date32`, and
timestamps as `Timestamp`. `format: auto` stays on the row path, because it
cannot promise that every file decodes to Arrow.

## Incremental mode

```yaml
incremental: { by: mtime }   # or: { by: name }
```

- `by: mtime` reads files modified after the newest file the previous run read.
  Ties are broken by path, so a second file with the same timestamp is still
  new. A rewritten file is read again.
- `by: name` reads files whose path sorts after the last one read. Use it for
  dated names that are never rewritten.

The bookmark advances after each file, and pages never span two files in this
mode, so an interrupted run resumes at the next unread file. It needs a
pipeline `state:` block; for library use the key is `file:<hash of path>`.
Over HTTP, the `Last-Modified` header is the modification time. A server that
sends none cannot be used with `by: mtime` or `stable_for_secs`, and the run
says so.

`stable_for_secs: N` skips files modified in the last N seconds, leaving them
for a later run once they settle.

## HTTP

A URL `path` is one file. `headers:` are sent with every request, for example
`Authorization: "Bearer ${env:TOKEN}"`. Connection errors, `429` and `5xx`
responses are retried up to `http_retries` times (default 3) with exponential
backoff that honours `Retry-After`. Any other status fails the run with the
status and the response body. Requests are counted as `get` / `head` round
trips on the pipeline's recorder, and 429s, retries and rate-limit waits feed
the run's throttling metrics.

## Sharding and discovery

Local paths are shardable by a hash of the file path (`shard: { count: N }` in
`faucet serve` cluster mode): each worker reads a disjoint set of files.
`faucet discover` lists each readable file as a dataset of kind `file`, with a
`{path, format}` config patch. No file is opened during discovery.

## Features

JSON Lines, JSON arrays and raw text are always available. The other formats
are behind features: `file-format-csv`, `-xml`, `-excel`, `-avro`, `-orc` and
`-parquet`, plus `arrow` for the columnar path. The `file-formats` feature
turns them all on. A build that lacks a format names the missing feature
instead of misreading the file. `encryption` enables the `encryption` block.

## Library

```rust,no_run
use faucet_core::Source;
use faucet_source_file::{FileSource, FileSourceConfig, IncrementalBy};

# async fn run() -> Result<(), faucet_core::FaucetError> {
let source = FileSource::new(
    FileSourceConfig::new("./exports").recursive(true).incremental(IncrementalBy::Mtime),
)?;
let records = source.fetch_all().await?;

// One call, every default:
let rows = faucet_source_file::read_records("./out/orders.jsonl").await?;
# Ok(()) }
```

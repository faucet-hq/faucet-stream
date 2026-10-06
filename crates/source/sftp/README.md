# faucet-source-sftp

SFTP source connector for the [faucet-stream](https://crates.io/crates/faucet-stream)
ecosystem.

Lists a remote directory (or reads a single file) over SFTP and streams the
files as JSON Lines, JSON arrays, or raw text. JSON Lines and raw text are
decoded incrementally, so memory stays bounded regardless of file size; JSON
arrays are buffered per file (the closing `]` is needed to validate the
structure) and then chunked. Up to `concurrency` files are read at once.

Connection, authentication, and host-key verification come from
[`faucet-common-sftp`](https://crates.io/crates/faucet-common-sftp).

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | string | — | Server hostname or IP. |
| `port` | integer | `22` | Server port. |
| `username` | string | — | SSH username. |
| `type` / `config` | auth | — | `password` or `private_key` (see `faucet-common-sftp`). |
| `known_hosts` | policy | `{ mode: accept_new }` | Host-key verification policy. |
| `path` | string | — | Remote directory to list, or a single file. |
| `glob` | string | none | Filename glob (`*` / `?`) applied to basenames when `path` is a directory. |
| `format` | enum | `jsonl` | `jsonl` \| `json_array` \| `raw_text` \| `csv` \| `xml` \| `xlsx`. |
| `batch_size` | integer | `1000` | Records per page; `0` = one page per file. |
| `concurrency` | integer | `4` | Files read concurrently. The prefetch is ordered, so records stay in listing order and a failing file is still blamed at its own position; `0` is clamped to 1. Lower than the object-store sources' default because every read shares one SSH channel. For `jsonl` it overlaps only the `open` round-trip (peak memory stays `O(batch_size)`); for `json_array` / `raw_text` up to `concurrency` whole files are resident. |

`raw_text` emits one record per file: `{ "path": <remote path>, "content": <file text> }`.

The SFTP source is not resumable — every page carries no bookmark.

## Faucet sinks' unfinished output

A directory listing skips a faucet sink's scratch files: an SFTP sink's
in-flight or orphaned upload (`<name>.faucet-tmp-upload-<id>`) and the other
`*.faucet-tmp*` names. The listing is not recursive, so an overwrite run's
`.faucet-overwrite-*` swap directory is never read.

## Example

```yaml
version: 1
pipeline:
  source:
    kind: sftp
    config:
      host: sftp.example.com
      port: 22
      username: reporting
      type: password
      config:
        password: ${env:SFTP_PASSWORD}
      known_hosts:
        mode: accept_new
      path: /exports/daily
      glob: "*.jsonl"
      format: jsonl
      batch_size: 1000
  sink:
    kind: stdout
    config: {}
```

Licensed under MIT OR Apache-2.0.

## File formats (#604)

Beyond JSON Lines, JSON array and raw text, this source reads **CSV**, **XML**
and **Excel** through `faucet_core::file_format`, so the records it produces
match what every other file connector produces for the same bytes.

```yaml
source:
  type: sftp
  config:
    host: files.example.com
    username: svc
    path: /exports
    glob: "*.csv"
    format: csv
    csv: { delimiter: ";" }
```

| Option block | Applies to | Fields |
|---|---|---|
| `csv` | `csv` | `delimiter` (one byte; `"\t"` for tabs), `has_headers` (default `true`; `false` names fields `column_0`, `column_1`, …) |
| `xml` | `xml` | `record_element` (the repeated element that delimits a record) |
| `excel` | `xlsx` | `sheet` (name, or an index as a string; default first), `header_row` (0-based) |

Enable with `--features file-formats` (or one of `file-format-csv` /
`file-format-xml` / `file-format-excel`), so a build that reads CSV does not
link an Excel reader. Format composes with `compression`.

**Memory:** these three are read **whole** and decoded before their records are
chunked into pages — a workbook is a zip container whose directory sits at the
end, and an XML document is a tree. `csv` and `xml` are text formats: every
value comes back a string. See the
[file-formats cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/file-formats.html).

## Avro and ORC (#719)

`format: avro` reads Avro Object Container Files. Every file is resolved
against one reader schema: `avro.schema` when set, else the first file's
writer schema. A file that cannot be resolved against it fails the run with
an error naming both. Logical types are mapped explicitly: `decimal` becomes
an exact string on the row path and `Decimal128` on the columnar path, and
`date` / `timestamp-*` / `uuid` likewise.

`format: orc` reads ORC (read-only; there is no ORC sink), projected by
`orc.columns`. The whole file is fetched first, because the footer is at the
end, and then decoded stripe by stripe. Every file must share one schema.

```yaml
format: avro
avro:
  schema: { type: record, name: order, fields: [ { name: id, type: long } ] }   # optional
# or
format: orc
orc:
  columns: [id, amount]
```

Both decode straight to Arrow, so with the `arrow` feature they take the
columnar path (`avro → parquet` never builds JSON rows). Enable with
`file-format-avro` / `file-format-orc` (ORC turns on `arrow`), or with
`file-formats`. Details: the
[file-formats cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/file-formats.html#avro).


## Parquet (#777)

`format: parquet` (the `arrow` feature) reads Apache Parquet files over byte
ranges (#783): the file is opened once, its footer locates every row group,
and each row group is read and decoded in turn into Arrow batches of at most
`batch_size` rows, so a file is never buffered whole. Peak memory is one row
group plus one batch, not the file — a 213 MiB file streams in under 10 MiB on
the row path and the columnar path alike. A short read is an error naming the
file, never a truncated decode.

`parquet.columns` projects top-level columns before any row group is read, so
unread columns are never transferred; an empty list is refused at
construction, and a name a file does not have fails the run naming the file
and its columns. With `arrow` the format joins the columnar path, where every
file in the listing must share the first file's schema; while one file's row
groups decode, the next `concurrency` files' footers are fetched ahead.

```yaml
format: parquet
parquet:
  columns: [id, amount]
```


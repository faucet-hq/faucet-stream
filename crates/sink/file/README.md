# faucet-sink-file

Local file sink for [faucet-stream](https://github.com/faucet-hq/faucet-stream):
write records to one file, or a directory of rolled files, as JSON Lines, a JSON
array, CSV, XML, Excel, Avro, Parquet or raw text. The format comes from the
path's extension; `.gz` / `.zst` add compression. It is the write-side
counterpart of [`faucet-source-file`](../../source/file), so one pipeline's
output is the next one's input — and the quickest way to see what a source
produces while you build it.

```yaml
sink:
  type: file
  config:
    path: ./out/contacts.jsonl      # or .csv, .json, .xml, .xlsx, .avro, .parquet, .txt
```

## Configuration

| Field | Default | Meaning |
|---|---|---|
| `path` | required | The file to write, as a template. `{part}` is the rollover part number (`00001`, …). With a rollover cap and no `{part}`, `-{part}` is inserted before the extension (`contacts-00001.jsonl`). A path ending in `/` is a directory of `part-{part}<ext>` files and needs `format`. `${now.*}` tokens are resolved by the CLI (`./out/${now.date}/x.csv`). |
| `format` | `auto` | `auto` resolves from the extension, looking through `.gz`/`.zst` (`x.csv.gz` is CSV). Explicit: `json_lines`, `json_array`, `csv`, `xml`, `xlsx`, `avro`, `parquet`, `raw_text`. `orc` is refused — ORC is read-only. A path with no recognised extension and no `format` is a load-time error naming the path. |
| `compression` | `auto` | `auto` (from the suffix), `none`, `gzip`, `zstd`. Applies to every format: a Parquet, Avro or Excel file is compressed whole on top of its internal compression (`x.parquet.gz`), and the file source reads it back. |
| `mode` | `overwrite` | What to do with a file that already exists: `overwrite` (write beside it, rename over it), `append`, `error_if_exists`. `append` extends a JSON Lines, CSV or raw-text file; for the whole-document formats it needs a numbered output (a `{part}` template or a rollover cap), and each run then adds new parts after the highest existing one. |
| `write_mode` | `append` | `overwrite` replaces the destination's whole output set when the run succeeds — see below. |
| `max_records_per_file` | none | Roll to a new file after this many records. `max_rows_per_file` is accepted as another name. |
| `max_bytes_per_file` | none | Roll once a file holds this many bytes, counted as the records' JSON size before compression. |
| `create_dirs` | `true` | Create missing directories. |
| `batch_size` | 1000 | Page-size hint; the sink writes whatever page it is handed. |
| `csv` | `{delimiter: ",", has_headers: true, quote: "\""}` | CSV dialect. `has_headers` (also spelled `write_headers`) writes the header row; `quote` is the quote character; `on_unknown_field` is `widen` (default: a field that appears later becomes a new column, and earlier rows are padded with empty cells), `warn` (the header is fixed by the first page and a later field is dropped with a warning) or `error` (fail the write naming the field). |
| `excel` | `{}` | `sheet` name for the workbook. |
| `xml` | `{root_element: records, record_element: record}` | XML framing. |
| `avro` | `{}` | Writer `schema` (inferred from the records when unset) and block `codec` (`null`, `deflate`, `snappy`, `zstd`). |
| `parquet` | `{compression: snappy, row_group_size: 1048576}` | `compression` is the column-chunk codec: `none` (also `uncompressed`), `snappy`, `gzip`, `zstd`, `lz4`. `row_group_size` caps the rows per row group. `schema` is an optional explicit column list — see below. |
| `json_lines` | `{pretty: false}` | `pretty: true` pretty-prints each record across several lines, as the `jsonl` sink's `pretty` does. |
| `encryption` | none | `{key, previous_keys, algorithm}` (feature `encryption`): encrypt the output at rest with AES-256-GCM — see below. |

## Durability

Every file is written to `<name>.faucet-tmp` and renamed into place (after an
`fsync`) when the pipeline flushes — at the end of a run and after every page
that carries a bookmark — or when a rollover closes it. The bookmark advances
only after that flush, so:

- a run killed mid-page leaves no final-named partial file, only a
  `.faucet-tmp`, which the next run removes; the resumed run rewrites the file
  from the last bookmark;
- a sink dropped without a flush (a failed or cancelled run) removes its
  temporary files;
- a disk-full or permission error names the file, and the half-written
  temporary file is discarded.

A file that keeps growing across several flushes in one run stays complete at
every step: JSON Lines and raw text are copied and extended (a compressed file
gains a new gzip/zstd member), CSV keeps its rows in a side file and rewrites
the header in front of them (so a later record can still add a column),
Parquet copies its row groups into a new file, and the whole-document formats
(JSON array, XML, Excel, Avro) re-encode the file's records, which they keep in
memory — bound that with `max_records_per_file` or `max_bytes_per_file`.

Batch atomicity is `atomic` without rollover (a page is visible entirely or not
at all) and `best_effort` with rollover (a page can span files and an earlier
file is already in place).

## Formats

- **JSON Lines / raw text** stream straight to disk.
- **CSV** columns are the union of every record's keys, in first-seen order;
  rows written before a column appeared get an empty cell, so every row has
  one cell per column. With `csv.on_unknown_field: warn` or `error` the header
  is fixed by the first page instead — the `csv` sink's behaviour. Every
  record must be an object; `null` is an empty cell and nested values are
  written as JSON.
- **JSON array, XML, Excel, Avro** are whole-document formats: a rollover
  finalises each file, and `mode: append` to a single file is refused because
  it cannot be extended without a rewrite (a numbered output appends new
  parts instead).
- **Parquet** goes through the Arrow writer. The schema comes from the first
  page with every field nullable; a later page that adds a field widens the
  file (earlier rows get null), and a field that changes type is an error
  naming it. With the `arrow` feature an Arrow source writes Parquet without
  building JSON values; Avro accepts the columnar path too.

### Explicit Parquet schema

```yaml
parquet:
  schema:
    - { name: id, type: int64, nullable: false }
    - { name: amount, type: { decimal: { precision: 12, scale: 2 } } }
    - { name: day, type: date }
    - { name: at, type: timestamp_us }
    - { name: note, type: string }
```

Types: `boolean`, `int32`, `int64`, `uint64`, `float32`, `float64`, `string`,
`date` (`YYYY-MM-DD`), `timestamp_ms` / `timestamp_us` / `timestamp_ns` (UTC,
from RFC 3339 text or epoch numbers), `decimal` (precision 1–38). With a schema
the file has exactly those columns and never widens: a record field the schema
does not name fails the write naming it, a value that does not fit its type
fails naming the column, and a non-nullable column that is missing fails. On
the columnar path batches are cast to the schema.

## Encryption

With `encryption: { key: ${vault:…} }`, uncompressed JSON Lines and raw text
seal each record on its own line (base64 of an AES-256-GCM payload) — the same
layout the `jsonl` sink writes, so the file stays appendable and every line
decrypts on its own. Every other file, compressed JSON Lines included, is
compressed and then sealed whole when it is finalised; appending to one
decrypts it first. Keys rotate through `previous_keys`. The file source's
`encryption` block reads both layouts, and `readback_source` carries the block.
A file that is still being written exists only as its scratch copy, which is
not encrypted until the file is finalised; a CSV's plaintext side file is
removed at each finalisation of an encrypted output.

## Write modes

`write_mode: overwrite` replaces the output set atomically: files are staged in
a hidden `.faucet-overwrite-<name>` directory beside the destination, and only
when the run succeeds are they moved into place and files of an earlier run
that match the path template (and were not rewritten) removed. A failed or
cancelled run removes the staging directory and leaves the previous output
untouched. Each file is replaced atomically; with rollover, readers can briefly
see new parts next to stale ones before the stale ones are removed. A run that
produces no records leaves the destination empty. `mode` must be `overwrite`.

The begin, write and commit steps may run on different sink instances (the CLI
does that), so all state lives on the filesystem.

## Other behaviour

- `check()` (`faucet doctor`) verifies the destination directory — or, when it
  will be created, its nearest existing ancestor — accepts a new file.
- `dataset_uri` is `file://<absolute path>`; the connector name is `file`.
- Usable as the `dlq:` sink.
- The serve console's local-output preview reads file-sink outputs back with
  the file source.
- Two matrix rows may not write the same path, and a fan-out row needs a
  per-invocation token (`${parent.id}`) in its path: both are refused at load.

## Coming from the csv, jsonl or parquet sink

| Old field | File sink |
|---|---|
| csv `path`, `delimiter`, `write_headers`, `batch_size`, `compression` | same names (`csv.delimiter`, `csv.write_headers`) |
| csv `append: true` | `mode: append` |
| csv `on_unknown_field: warn \| error` | `csv.on_unknown_field` (the file sink's default, `widen`, never drops a field) |
| jsonl `path`, `pretty`, `batch_size`, `compression`, `encryption` | `path`, `json_lines.pretty`, `batch_size`, `compression`, `encryption` |
| jsonl `append: true` | `mode: append` |
| parquet `destination: {type: local_path, path}` | `path` (a directory path ending in `/` names parts `part-00001.parquet`) |
| parquet `compression` (`uncompressed`, `snappy`, `gzip`, `zstd`, `lz4`) | `parquet.compression` |
| parquet `row_group_size`, `max_rows_per_file`, `max_bytes_per_file`, `batch_size` | `parquet.row_group_size`, `max_rows_per_file`, `max_bytes_per_file`, `batch_size` |
| parquet `schema: {type: inferred, sample_size}` | inference is the default and reads every record, so nothing is sampled |

Golden tests (`crates/interop-tests/tests/file_sink_parity.rs`) write the same records through the old sink
and the file sink and compare the output.

## Shared writer

The encoding, compression, encryption, rollover and overwrite staging live in
`faucet_common_file::write` (`FileWriter` over a `StorageBackend`); this sink is
that writer over `LocalBackend`, so object-store sinks share the same code.

## Features

JSON Lines, JSON arrays and raw text need nothing. `file-format-csv`,
`file-format-xml`, `file-format-excel`, `file-format-avro`,
`file-format-parquet` add the rest (`file-formats` is all of them); `arrow`
enables the columnar write path; `encryption` enables the `encryption` block.

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
| `compression` | `auto` | `auto` (from the suffix), `none`, `gzip`, `zstd`. Not allowed for Parquet, Avro or Excel, which compress internally. |
| `mode` | `overwrite` | What to do with a file that already exists: `overwrite` (write beside it, rename over it), `append` (JSON Lines, CSV and raw text only; with rollover, numbering continues after the highest existing part), `error_if_exists`. |
| `write_mode` | `append` | `overwrite` replaces the destination's whole output set when the run succeeds — see below. |
| `max_records_per_file` | none | Roll to a new file after this many records. |
| `max_bytes_per_file` | none | Roll once a file holds this many bytes, counted as the records' JSON size before compression. |
| `create_dirs` | `true` | Create missing directories. |
| `batch_size` | 1000 | Page-size hint; the sink writes whatever page it is handed. |
| `csv` | `{delimiter: ",", has_headers: true}` | CSV dialect. |
| `excel` | `{}` | `sheet` name for the workbook. |
| `xml` | `{root_element: records, record_element: record}` | XML framing. |
| `avro` | `{}` | Writer `schema` (inferred from the records when unset) and block `codec` (`null`, `deflate`, `snappy`, `zstd`). |
| `parquet` | `{compression: snappy}` | Column-chunk compression: `none`, `snappy`, `gzip`, `zstd`. |

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
  rows written before a column appeared leave it empty. Every record must be an
  object.
- **JSON array, XML, Excel, Avro** are whole-document formats: a rollover
  finalises each file, and `mode: append` is refused because they cannot be
  extended without a rewrite.
- **Parquet** goes through the Arrow writer. The schema comes from the first
  page with every field nullable; a later page that adds a field widens the
  file (earlier rows get null), and a field that changes type is an error
  naming it. With the `arrow` feature an Arrow source writes Parquet without
  building JSON values; Avro accepts the columnar path too.

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

## Features

JSON Lines, JSON arrays and raw text need nothing. `file-format-csv`,
`file-format-xml`, `file-format-excel`, `file-format-avro`,
`file-format-parquet` add the rest (`file-formats` is all of them); `arrow`
enables the columnar write path.

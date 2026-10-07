# File formats

Every file and object-store connector — **S3**, **GCS**, **Azure Blob**,
**SFTP** and **local files**, source *and* sink — reads and writes the same set
of formats, with the same option names:

| `format` | Source | Sink | Notes |
|---|:--:|:--:|---|
| `json_lines` *(default)* | ✅ | ✅ | One JSON value per line. The only format that streams a record at a time on both sides. |
| `json_array` | ✅ | ✅ | One JSON array per object. |
| `csv` | ✅ | ✅ | Delimited text. Values are strings on read; columns are the union of every record's keys on write. |
| `xml` | ✅ | ✅ | Compact element→object mapping. Requires a declared record element. |
| `xlsx` | ✅ | ✅ | An Excel worksheet. Carries types. Whole-workbook in memory. |
| `parquet` | ✅ | ✅ | Columnar, handled by each connector's own Arrow path — see [Arrow](../reference/connectors.md). |
| `raw_text` | ✅ | — | One record per object, carrying the whole body. Source-only. |
| `avro` | ✅ | ✅ | Avro Object Container File. Carries types, including logical types. See [Avro](#avro). |
| `orc` | ✅ | — | Apache ORC, with column projection. **Read-only.** See [ORC](#orc). |

Before this, what you could read depended on which store the file was in, and
what you could write was a strict subset of what you could read — the gap filled
by pre- and post-processing outside the pipeline.

Format composes with [compression](./compression.md): pick the format, pick the
codec, independently.

## Enable the feature

Each format pulls only its own parser and writer, so a build that reads CSV does
not link an Excel reader:

```bash
cargo install faucet-cli --features file-formats             # every format
cargo install faucet-cli --features file-format-csv          # just CSV
cargo install faucet-cli --features file-format-avro         # just Avro
cargo install faucet-cli --features file-format-orc          # just ORC (pulls Arrow)
```

```toml
# Library (umbrella) — activates the formats on whichever file connectors
# you've enabled; it does not pull connectors by itself.
faucet-stream = { version = "1.0", features = ["source-s3", "sink-s3", "file-formats"] }
```

`json_lines`, `json_array`, `raw_text` and `parquet` need no format feature.
`full` includes `file-formats`.

## Reading

```yaml
source:
  type: s3
  config:
    bucket: exports
    prefix: daily/
    file_format: csv
    compression: auto              # .gz / .zst resolved per object
    csv:
      delimiter: ","               # one byte; "\t" for tabs
      has_headers: true            # false → fields are column_0, column_1, …
      quote: "\""                  # the quote character
      flexible: true               # accept ragged rows (the `file` source defaults to false)
      null_values: ["", "NULL"]    # cells read as null
```

A repeated header name fails the read — rows are keyed by header, so a
duplicate would silently drop a column. With `flexible: false` a row with more
or fewer fields than the header fails naming its line. The object-store and
REST sources are lenient by default; the `file` source is strict, like the
`csv` source.

```yaml
source:
  type: sftp
  config:
    host: files.example.com
    path: /exports
    glob: "*.xlsx"
    format: xlsx
    excel:
      sheet: "Q3"                  # name, or an index as a string; default: first
      header_row: 0                # 0-based
```

```yaml
source:
  type: gcs
  config:
    bucket: feeds
    file_format: xml
    xml:
      record_element: order        # the repeated element that delimits a record
```

## Writing

```yaml
sink:
  type: s3
  config:
    bucket: reports
    prefix: monthly/
    format: xlsx
    file_extension: .xlsx
    excel: { sheet: Orders }
```

```yaml
sink:
  type: azure-blob
  config:
    container: reports
    format: csv
    file_extension: .csv.gz
    compression: gzip              # format and codec are independent
    csv: { delimiter: ";" }
```

## What each format does and does not carry

- **`json_lines` / `json_array`** are lossless: any JSON value round-trips.
- **`xlsx`** carries numbers and booleans as themselves. A spreadsheet stores
  every number as a double, so an integral value reads back as an integer.
  A date-formatted cell reads back as text: `2023-03-15` for a whole day,
  `2023-03-15T12:30:00` otherwise, an ISO 8601 duration for a time span
  (workbooks on the 1904 date system included).
- **`csv` and `xml` are text formats.** Every value comes back a string; a
  number written as `42` reads back as `"42"`. Use a
  [`cast` transform](./transforms.md) if downstream needs the type.
- **Nested structure** has no cell in a spreadsheet or a CSV, so an object or
  array is re-serialized as JSON text rather than dropped — lossy in shape, but
  never in content, and `json_parse` recovers it.
- **A record that gains a field mid-page widens the file.** Columns are the
  union of every record's keys in the group, so a late field is written for
  every row rather than silently lost.

### The edges, precisely

These are asserted by `crates/conformance/tests/format_fidelity.rs` against
the shared fidelity corpus, so they stay true as the layer changes:

| Value | `json_*` | `csv` | `xml` | `xlsx` |
|---|---|---|---|---|
| Integer past 2^53 | exact | exact digits, as text | exact digits, as text | **exact digits, as text** — no double represents it, so writing it as a number would silently round it |
| `null` | `null` | empty field (reads back `""`) | empty element | empty cell |
| `""` | `""` | `""` | `""` | reads back `null` — a spreadsheet cannot tell an empty cell from an empty string |
| `-0.0` | `-0.0` | `"-0.0"` | `"-0.0"` | `0` — no signed zero in a cell |
| Leading/trailing spaces | kept | kept | **trimmed** — XML text nodes are whitespace-normalised on read |
| `[]` (empty array) | `[]` | `"[]"` | **field absent** — a list is repeated elements, so an empty one is no element at all |

The two in bold worth planning around: **XML trims padding**, so quote-and-pad
alignment does not survive a round trip; and **xlsx returns a big integer as a
string**, which is visible and correctable, unlike a rounded number.

## Streaming and memory

Only `json_lines` can be built a record at a time. Every other format has a
header, a document element, a container index, or a pair of brackets, so its
records are **buffered and encoded together**:

- On the **sink** side the records accumulate to the same `max_records_per_file`
  / `max_bytes_per_file` caps that size a JSON Lines object, then the whole
  group is encoded and written as one object. Object sizing therefore means the
  same thing whatever the format — but peak memory is one group, not one record.
- On the **source** side `xml` and `xlsx` objects are read whole and decoded
  before their records are chunked into pages, the same way `json_array`
  already was. The object-store sources read `csv` whole too; the `file`
  source streams it row by row.

`xlsx` is the strictest case: a workbook is a zip container whose directory sits
at the end, so it cannot be decoded incrementally in either direction. Size
`batch_size` / `max_records_per_file` for the memory you have.

## Framing XML

XML has no canonical record boundary, so one is declared:

```yaml
xml:
  record_element: order            # read: select these; write: wrap each record
  root_element: orders             # write only: the document element
```

On read, every `<order>` element in the document becomes a record, at any depth.
When no element of that name exists, the document root's children are used
instead — right for the common `<rows><row/>…</rows>` shape without forcing
every config to spell it out. A root with several differently-named children is
an error naming `xml.record_element`, rather than a guess.

Attributes become `@name`, text becomes `#text` (or the value directly when an
element holds only text), and namespaces are stripped to their local name.

On write, a field name that is not a legal XML element name (a space, a slash, a
leading digit) has the offending characters replaced with `_` — the document
stays well-formed rather than the write failing on a field you cannot rename.

## Avro

Avro Object Container Files (`.avro`) read and write on every file connector.
Each file carries its own *writer* schema, so reading needs no configuration:

```yaml
source:
  type: s3
  config:
    bucket: exports
    prefix: kafka-dump/
    file_format: avro
    avro:
      schema:                      # optional reader schema
        type: record
        name: order
        fields:
          - { name: id, type: long }
          - { name: amount, type: { type: bytes, logicalType: decimal, precision: 12, scale: 2 } }
          - { name: channel, type: string, default: "web" }   # added later: files without it read "web"
```

**Many files, one shape.** Every file under the prefix is resolved against one
reader schema: `avro.schema` when set, otherwise **the first file's writer
schema**. Avro's schema resolution then applies. A later file that added a
field has it dropped. A field the reader declares with a default is filled for
files that lack it. Numeric promotions such as `int → long` apply. A file that
cannot be resolved fails the run with an error naming both files:

```text
avro schema of 'b.avro' cannot be resolved against 'a.avro' (the first file's schema): …
```

**Logical types are mapped explicitly.** Nothing goes through a lossy numeric
fallback:

| Avro | Record (JSON) | Columnar (Arrow) |
|---|---|---|
| `decimal(p, s)` | exact decimal string, `"12.30"` | `Decimal128(p, s)` (`Decimal256` above 38 digits) |
| `date` | `"2024-02-29"` | `Date32` |
| `time-millis` / `time-micros` | `"01:02:03.004"` / `"01:02:03.000004"` | `Time32(ms)` / `Time64(µs)` |
| `timestamp-millis` / `-micros` / `-nanos` | RFC 3339 UTC at that precision (`"…Z"`) | `Timestamp(unit, "UTC")` |
| `local-timestamp-*` | naive ISO 8601 | `Timestamp(unit)` without a zone |
| `uuid` | canonical string | `Utf8` |
| `duration` | `{months, days, millis}` | `Struct` |
| `bytes` / `fixed` | lowercase hex, the same as the Arrow JSON writer | `Binary` / `FixedSizeBinary` |
| `enum` | the symbol | `Utf8` |

A union of `null` and one type is that type, nullable. A union of several
non-null types reads as whichever branch the value holds. On the columnar path,
where a column has one type, it becomes a `Utf8` column holding the value's
JSON. A recursive record is a nested object on the record path and JSON text
on the columnar path, because Arrow has no recursive types. `NaN` and the
infinities, which JSON cannot hold, read as the strings `"NaN"`, `"Infinity"`
and `"-Infinity"`.

**Writing.** A sink encodes each object against `avro.schema`, or against a
schema inferred from that object's records:

```yaml
sink:
  type: gcs
  config:
    bucket: archive
    format: avro
    file_extension: .avro
    avro:
      codec: zstd                  # null (default) | deflate | snappy | zstd
      # schema: { … }              # optional writer schema
```

Inference maps `integer` to `long`, `number` to `double`, objects to nested
records, and arrays to arrays. A field that is null or absent in any record
becomes `["null", T]` with a `null` default, and a field whose values mix types
is written as `string`. Field names that are not valid Avro names are
sanitized (`first-name` becomes `first_name`, and a leading digit gains `_`).
The original name is kept in the field's `faucet.name` attribute. Two names
that sanitize to the same field are an error rather than a silent merge. With
an explicit schema, logical types accept the shapes above, and epoch integers
too.

## ORC

ORC (`.orc`) is **read-only**. The reader is `orc-rust`. Its writer covers only
primitive columns, panics on nested and temporal types, and writes no
compression, so faucet does not put it behind a sink. For a columnar output,
write Parquet.

```yaml
source:
  type: azure-blob
  config:
    container: lake
    prefix: hive/orders/
    file_format: orc
    orc:
      columns: [id, amount, day]   # top-level projection; default: every column
```

Files are decoded stripe by stripe, straight to Arrow, and the projection is
applied before any stripe is read. A column the file does not have is an
error, not an empty column. Every file must have the same (projected) schema,
and a mismatch names both files. On an object store the whole object is
fetched first, because the ORC footer sits at the end. The local file source
reads by stripe from disk.

Types follow Arrow: `decimal` becomes `Decimal128`, `date` becomes `Date32`,
timestamps become `Timestamp(ns)`, and nested types become `Struct` / `List` /
`Map`. On the record path they convert the same way a Parquet file's do, so
the two formats produce the same records for the same content.

## The columnar path for Avro and ORC

Both formats decode to Arrow `RecordBatch`es. On S3, GCS, Azure Blob, SFTP and
the local file source they take the columnar path whenever the pipeline can:
an `avro → parquet` run moves typed batches end to end, with no JSON records
in between, and Avro decimals and timestamps land in Parquet as
`DECIMAL` and `TIMESTAMP` columns. Build with `arrow` (ORC turns it on).

## The local file source

The `file` source reads local files with everything above. It takes a file, a
directory (`recursive: true` descends), a glob, or a single `http(s)://` URL:

```yaml
source:
  type: file
  config:
    path: ./inbox                  # or ./inbox/**/*.csv.gz, or https://…/export.avro
    format: auto                   # per file, from the extension
    incremental: { by: mtime }     # re-runs read only new files (needs `state:`)
    stable_for_secs: 30            # skip files still being written
    strict: false                  # true: refuse an unrecognised extension instead of skipping it
```

Under `format: auto` each file's format comes from its extension, looking
through a compression suffix: `.jsonl`/`.ndjson`, `.json`, `.csv`, `.xml`,
`.xlsx`, `.parquet`, `.avro`, `.orc`, `.txt`. So a directory holding `a.jsonl`,
`b.json`, `c.csv.gz` and `d.xlsx` reads in one run. A file with any other
extension is skipped with a warning, or fails the run with `strict: true`.

Incremental mode reads only new files. `by: mtime` reads files modified after
the newest one the previous run read; `by: name` reads files whose path sorts
after the last one read. The bookmark advances after each file. Over HTTP, the
`Last-Modified` header is the modification time. JSON Lines, Avro, ORC and
Parquet stream, and so does CSV (with the `csv:` dialect above); the other
formats are read whole per file. `parquet: { columns: [...] }` projects a
Parquet read to those columns, and Parquet schemas are compared from the
footers before the first row is read. `encryption:` decrypts files the `file`
or `jsonl` sink encrypted. See the
[crate README](https://github.com/faucet-hq/faucet-stream/tree/main/crates/source/file)
for sharding, discovery, HTTP retries and a field-by-field mapping from the
`csv` and `parquet` sources, which the `file` source matches option for option.

## The local file sink

The `file` sink writes any writable format to a local path — the general local
sink, and the fastest way to look at what a source produces while you build
it. Point a pipeline at `path: ./out/x.jsonl` and change the extension to get
`.csv`, `.json`, `.xml`, `.xlsx`, `.avro` or `.parquet` instead (`.gz` /
`.zst` add compression):

```yaml
sink:
  type: file
  config:
    path: ./out/contacts/${now.date}/contacts-{part}.csv.gz
    max_records_per_file: 100000   # or max_bytes_per_file; `{part}` numbers the files
    if_exists: replace             # an existing file is replaced; append | error
    write_mode: overwrite          # replace the whole part set, only when the run succeeds
```

Every file is written to `<name>.faucet-tmp` and renamed into place when the
pipeline flushes, and the bookmark advances only after that flush — so a run
that is killed leaves no complete-looking partial file, and the next run
resumes from the last bookmark and removes the leftover temporary file.
`write_mode: overwrite` stages the run's files in a hidden directory beside the
destination and swaps them in (removing stale parts of the previous run) only
after a successful run; a failed run leaves the previous output as it was.

Every option works with every writable format: `compression` (a Parquet or
Avro file is compressed whole on top of its own codec), `encryption`, rollover
by records or bytes, `{part}` templates and directories, and both write modes.
The one refusal is `if_exists: append` to a single whole-document file (JSON array,
XML, Excel, Avro, Parquet), which cannot be extended without a rewrite — a
numbered output appends new parts instead. ORC is read-only and refused.

```yaml
sink:
  type: file
  config:
    path: ./out/orders-{part}.parquet
    max_records_per_file: 5000000
    parquet:
      compression: zstd            # none | snappy | gzip | zstd | lz4
      row_group_size: 131072
      schema:                      # optional; inferred and widened when unset
        - { name: id, type: int64, nullable: false }
        - { name: amount, type: { decimal: { precision: 12, scale: 2 } } }
        - { name: placed_at, type: timestamp_us }
    encryption: { key: "${vault:secret/data/export#key}" }
```

CSV headers are written when a file is finalised, so a field that appears
later becomes a new column and earlier rows get an empty cell;
`csv: { on_unknown_field: warn | error }` fixes the header from the first page
instead (drop the field with a warning, or fail). `json_lines: { pretty: true }`
pretty-prints each record. Two matrix rows writing the same path, or a fan-out
row without a per-invocation token in its path, are refused at load time — for
the `file` sink and for `jsonl` / `csv` / `parquet` writing one fixed file
(root rows included). The
`jsonl`, `csv` and `parquet` sinks stay for existing configs, and the file sink
matches them option for option — see the
[crate README](https://github.com/faucet-hq/faucet-stream/tree/main/crates/sink/file)
for every field and the mapping from the old sinks' fields.

### The same writer on S3, GCS, Azure Blob and SFTP

The same writer (`faucet_common_file::write`) sits under the `s3`, `gcs`,
`azure-blob` and `sftp` sinks, with a remote storage backend — so they take
every format and option above with the same field names. The object's name
template is `path` (`file_name` on SFTP, whose `path` is the remote
directory), placed after the existing `prefix`:

```yaml
sink:
  type: s3
  config:
    bucket: my-data-lake
    prefix: exports/
    path: "orders/${now.date}/orders-{part}.parquet"
    max_records_per_file: 1000000
    write_mode: overwrite            # staged under a hidden prefix, swapped in on success
    parquet: { compression: zstd, row_group_size: 131072 }
```

On S3 and Azure, JSON Lines and raw text stream straight into multipart parts
(a block list on Azure) while the object is open, so memory is about
`concurrency` × part size and nothing is written to local disk. Every other
format — and every format on GCS and SFTP — is built in a scratch file under
`scratch_dir` (default: the system temp directory, so size it for your largest
object) and uploaded when it closes: a resumable upload on GCS, a temporary
name renamed into place on SFTP. Either way the object appears only when it is
complete, so a reader never sees a partial object and a bookmark never advances
past one that is not there. Without `path` each object gets a fresh name,
`{prefix}{uuidv7}-00001{file_extension}` (numbered on rollover), so
`if_exists` and `write_mode: overwrite` need a `path` to have something to
replace. The
format × option matrix that pins the local sink runs against MinIO,
fake-gcs-server, Azurite and an SFTP server too.

With a `path` (`file_name` on SFTP) the names are fixed, so the same load-time
rule as the local file sink applies: two rows may not write the same bucket /
container / host + `prefix` + `path`, and a fan-out row needs a per-invocation
token (`${parent.id}`) in the prefix or path — otherwise concurrent writers
overwrite each other's parts and the last to finish prunes the rest.

### Run it locally

```bash
faucet run cli/examples/rest_to_file.yaml        # REST API → ./out/posts/<date>/posts-00001.jsonl …
faucet run cli/examples/file_to_jsonl.yaml       # and read files back in the next pipeline
```

## Parquet is separate on purpose

`parquet` is columnar and self-describing, and each connector reads and writes
it through its own Arrow path so a `parquet → parquet` chain never materializes
`serde_json::Value`. Routing it through the record encoder would work and would
silently cost that fast path, so the shared helper refuses it.

The object-store sources (`s3`, `gcs`, `azure-blob`) read `file_format: parquet`
with column projection — the equivalent of the Parquet source's `columns` for
objects in a bucket or container:

```yaml
source:
  type: s3
  config:
    bucket: my-data-lake
    prefix: events/2026/
    file_format: parquet
    parquet:
      columns: [id, amount]   # decoded before any row group is read
```

A column an object does not have fails the run with an error naming the object
and its columns.

## Moving from the csv, jsonl and parquet connectors

`type: csv`, `type: jsonl` and `type: parquet` are deprecated. They keep
working on their own crates, unchanged, until the next major release — they are
not aliases of `type: file` — and `faucet validate` / `faucet run` print one
line naming the replacement:

```text
connector kind `jsonl` is deprecated: use `type: file` with a `.jsonl` path (or `format: json_lines`). It keeps working on its own crate, unchanged, until the next major release
```

The `file` connector does everything they did. To move a config, change the
kind and move these fields:

| Old | `type: file` |
|---|---|
| csv source `has_headers`, `delimiter`, `quote`, `flexible`, `null_values` | `csv.has_headers`, `csv.delimiter` and `csv.quote` (one-character strings: `";"` rather than `59`, `"'"` rather than `39`), `csv.flexible`, `csv.null_values` |
| csv sink `delimiter` (a byte), `write_headers` | `csv.delimiter` (a one-character string), `csv.has_headers` |
| csv sink `on_unknown_field` (default `warn`) | `csv.on_unknown_field` (default `widen`: a later field becomes a new column instead of being dropped) |
| csv / jsonl sink `append: true` | `if_exists: append` |
| jsonl sink `pretty` | `json_lines.pretty` |
| jsonl sink `encryption` | `encryption` |
| parquet source `source: {type: local_path, path}` / `{type: glob, pattern}` | `path` |
| parquet source `columns` | `parquet.columns` |
| parquet sink `destination: {type: local_path, path}` | `path`; for a directory of numbered files use `dir/part-{part}.parquet` with `if_exists: append` |
| parquet sink `compression`, `row_group_size` | `parquet.compression`, `parquet.row_group_size` |
| parquet sink `max_rows_per_file` | `max_records_per_file` |
| parquet sink `schema: {type: inferred, sample_size}` | drop it: the file sink infers from every record |

`path`, `batch_size` and `concurrency` keep their names, and so does the
file-level `compression` (gzip / zstd) of the csv and jsonl connectors. The
parquet sink's `compression` is the Parquet codec, which moves to
`parquet.compression`. Set
`format:` when the extension does not say it: a `jsonl` sink writing
`out.json` needs `format: json_lines`, or the file sink writes a JSON array.
A Parquet location on S3 moves to the `s3` source or sink with
`file_format: parquet` / `format: parquet`.

Two differences to know about:

- The file source reads a null Parquet column as `"key": null`; the parquet
  source left the key out.
- The file sink writes each file to a temporary name and renames it into place
  when it is complete, so a reader never sees a half-written file. Because of
  that, two rows cannot append to the same `type: file` path at once; give each
  row its own path. The old kinds still allow it.

## Behaviour changes

The `file` connectors, `faucet-common-file` and the file-writing fields of the
`s3`, `gcs`, `azure-blob` and `sftp` sinks (`path` / `file_name`, `if_exists`,
`write_mode`, `parquet`, `json_lines`, `encryption`, `scratch_dir`) are
**Experimental**: their shape may change in a minor release, and every change
is listed here. Renamed keys and values keep working as aliases, and
`faucet validate` / `faucet run` print one line naming the new spelling.

- **`mode` is now `if_exists`**, with the values `replace` (default), `append`
  and `error`. `mode:` and the values `overwrite` / `error_if_exists` still work.
- **Removed aliases:** `csv.write_headers` (use `csv.has_headers`),
  `max_rows_per_file` (use `max_records_per_file`) and the Parquet codec
  `uncompressed` (use `none`).
- **One `parquet:` block** on every file-writing sink. Only the default codec
  differs: `snappy` on the `file` sink, `zstd` on the object-store sinks.
- **SFTP rollover** caps each file at `min(batch_size, max_records_per_file)`,
  like the other sinks; it used to prefer `max_records_per_file`.
- **Default object names** on `s3`, `gcs`, `azure-blob` and `sftp` without a
  `path` / `file_name` are `{prefix}{uuidv7}-00001{ext}` (numbered on rollover),
  where they used to be `{prefix}{uuid}{ext}`.
- **Object-store uploads:** S3 and Azure stream JSON Lines and raw text as
  multipart parts again (no scratch file); other formats, and every format on
  GCS and SFTP, are built under `scratch_dir`.
- Internally the `write_mode: overwrite` staging area is now called the "swap"
  area; the hidden `.faucet-overwrite-*` directory on disk is unchanged.

## See also

- [Compression](./compression.md) — gzip / zstd, independent of format
- [`cli/examples/file_to_jsonl.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/file_to_jsonl.yaml) — a local inbox read incrementally
- [`cli/examples/rest_to_file.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/rest_to_file.yaml) — a REST API into dated, rolled local files
- [Connector reference](../reference/connectors.md) — the capability matrix
- [Transforms](./transforms.md) — `cast` and `json_parse` for text-format values

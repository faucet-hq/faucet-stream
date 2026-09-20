# File formats

Every file and object-store connector — **S3**, **GCS**, **Azure Blob** and
**SFTP**, source *and* sink — reads and writes the same set of formats, with the
same option names:

| `format` | Source | Sink | Notes |
|---|:--:|:--:|---|
| `json_lines` *(default)* | ✅ | ✅ | One JSON value per line. The only format that streams a record at a time on both sides. |
| `json_array` | ✅ | ✅ | One JSON array per object. |
| `csv` | ✅ | ✅ | Delimited text. Values are strings on read; columns are the union of every record's keys on write. |
| `xml` | ✅ | ✅ | Compact element→object mapping. Requires a declared record element. |
| `xlsx` | ✅ | ✅ | An Excel worksheet. Carries types. Whole-workbook in memory. |
| `parquet` | ✅ | ✅ | Columnar, handled by each connector's own Arrow path — see [Arrow](../reference/connectors.md). |
| `raw_text` | ✅ | — | One record per object, carrying the whole body. Source-only. |

Before this, what you could read depended on which store the file was in, and
what you could write was a strict subset of what you could read — the gap filled
by pre- and post-processing outside the pipeline.

Format composes with [compression](./compression.md): pick the format, pick the
codec, independently.

## Enable the feature

Each format pulls only its own parser and writer, so a build that reads CSV does
not link an Excel reader:

```bash
cargo install faucet-cli --features file-formats             # all three
cargo install faucet-cli --features file-format-csv          # just CSV
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
```

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
- **`csv` and `xml` are text formats.** Every value comes back a string; a
  number written as `42` reads back as `"42"`. Use a
  [`cast` transform](./transforms.md) if downstream needs the type.
- **Nested structure** has no cell in a spreadsheet or a CSV, so an object or
  array is re-serialized as JSON text rather than dropped — lossy in shape, but
  never in content, and `json_parse` recovers it.
- **A record that gains a field mid-page widens the file.** Columns are the
  union of every record's keys in the group, so a late field is written for
  every row rather than silently lost.

## Streaming and memory

Only `json_lines` can be built a record at a time. Every other format has a
header, a document element, a container index, or a pair of brackets, so its
records are **buffered and encoded together**:

- On the **sink** side the records accumulate to the same `max_records_per_file`
  / `max_bytes_per_file` caps that size a JSON Lines object, then the whole
  group is encoded and written as one object. Object sizing therefore means the
  same thing whatever the format — but peak memory is one group, not one record.
- On the **source** side `csv`, `xml` and `xlsx` objects are read whole and
  decoded before their records are chunked into pages, the same way
  `json_array` already was.

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

## Parquet is separate on purpose

`parquet` is columnar and self-describing, and each connector reads and writes
it through its own Arrow path so a `parquet → parquet` chain never materializes
`serde_json::Value`. Routing it through the record encoder would work and would
silently cost that fast path, so the shared helper refuses it.

## See also

- [Compression](./compression.md) — gzip / zstd, independent of format
- [Connector reference](../reference/connectors.md) — the capability matrix
- [Transforms](./transforms.md) — `cast` and `json_parse` for text-format values

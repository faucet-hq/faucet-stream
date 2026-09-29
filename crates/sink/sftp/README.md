# faucet-sink-sftp

SFTP sink connector for the [faucet-stream](https://crates.io/crates/faucet-stream)
ecosystem.

Writes records to an SFTP server as JSON Lines objects under a remote
directory. Append-only.

## Atomic writes

Each object is uploaded to a hidden temporary name (`<uuid>.jsonl.tmp`) and then
**renamed** to its final name (`<uuid>.jsonl`). A consumer watching the
directory therefore never observes a partially-written file — a downstream
reader either sees the complete object or does not see it at all.

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
| `path` | string | — | Remote directory prefix under which objects are written. |
| `format` | enum | `json_lines` | `json_lines` \| `json_array` \| `csv` \| `xml` \| `xlsx` — see [File formats](#file-formats-604). |
| `file_extension` | string | `.jsonl` | Extension for written objects. |
| `batch_size` | integer | `1000` | Records per object; `0` = one object per `write_batch` call. |

The sink opens the SSH connection lazily on the first write and reuses it. It
attempts to create the target directory on first connect (best-effort).

## Example

```yaml
version: 1
pipeline:
  source:
    kind: stdout   # replace with a real source
    config: {}
  sink:
    kind: sftp
    config:
      host: sftp.example.com
      username: uploader
      type: private_key
      config:
        path: /home/uploader/.ssh/id_ed25519
      known_hosts:
        mode: strict
      path: /incoming/events
      file_extension: .jsonl
      batch_size: 0
```

Licensed under MIT OR Apache-2.0.

## Object rollover (`max_records_per_file` / `max_bytes_per_file`)

Records **accumulate across `write_batch` calls** and roll to a new object when
either cap is reached (#618). Before this, every upstream page became its own
object, so a small `batch_size` produced a swarm of tiny objects — the
small-files problem that dominates read time on a data lake, where per-object
overhead outweighs the bytes.

- `max_records_per_file` — record cap. When unset, `batch_size` still sizes
  objects, so an existing config keeps the object size it asked for; what
  changed is that a page *smaller* than the cap now joins the open object.
- `max_bytes_per_file` — byte cap, counted on the **uncompressed** body. Rows
  are a poor proxy for size (10k wide rows and 10k `{"id":1}` rows differ by
  orders of magnitude), and this is the axis that bounds buffered memory. A
  single record larger than the cap still gets its own object rather than
  being split or dropped.

With neither cap set the whole run lands in one object, closed at `flush` —
which the pipeline calls at every bookmark-carrying page and at the end, so
the remainder is always written before a bookmark advances.

## File formats (#604)

Beyond JSON Lines this sink writes **JSON array**, **CSV**, **XML** and
**Excel** through `faucet_core::file_format`, so what it writes is exactly what
the file *sources* can read back.

```yaml
sink:
  type: sftp
  config:
    host: files.example.com
    username: svc
    path: /incoming
    format: xml
    file_extension: .xml
    xml: { root_element: orders, record_element: order }
```

| Option block | Applies to | Fields |
|---|---|---|
| `csv` | `csv` | `delimiter` (one byte; `"\t"` for tabs), `has_headers` (default `true`) |
| `xml` | `xml` | `record_element`, `root_element` |
| `excel` | `xlsx` | `sheet` (worksheet name) |

Enable with `--features file-formats` (or one of `file-format-csv` /
`file-format-xml` / `file-format-excel`). Format composes with `compression`.

**Only `json_lines` can be built a record at a time.** Every other format has a
header, a document element, a container index or a pair of brackets, so its
records are buffered and encoded together at the rollover — bounded by the same
`max_records_per_file` / `max_bytes_per_file` caps, so object sizing means the
same thing whatever the format. Columns are the union of every record's keys in
the group, so a record that gains a field mid-page widens the file rather than
losing it. See the
[file-formats cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/file-formats.html).

## Avro (#719)

`format: avro` writes one Avro Object Container File per object, encoded
against `avro.schema` or against a schema inferred from that object's records.
Nullable and absent fields become `["null", T]`, mixed-type fields become
`string`, and invalid names are sanitized (the original is kept in
`faucet.name`). The block codec comes from `avro.codec`: `null` (default),
`deflate`, `snappy` or `zstd`. With an explicit schema, logical types
(`decimal`, `date`, `timestamp-*`, `uuid`, …) accept their string forms or
epoch integers.

```yaml
format: avro
file_extension: .avro
avro:
  codec: zstd
```

ORC is read-only, so there is no `orc` format here. Enable with
`file-format-avro` (or `file-formats`).

## Shared file writer (#777)

This sink writes through the same file-writing layer as the local
[`file` sink](https://crates.io/crates/faucet-sink-file), so it takes every
format and option the file sink does, with the same field names:

| Field | Values | Notes |
|---|---|---|
| `format` | `json_lines` (default), `json_array`, `csv`, `xml`, `xlsx`, `avro`, `parquet`, `raw_text`, `auto` | `auto` takes the format from `file_name`'s extension (else `file_extension`), looking through `.gz` / `.zst`. `parquet` needs the `arrow` feature; the other shared formats their `file-format-*` feature. |
| `file_name` | a name template | file name template: `{part}` numbers the files, `${now.*}` tokens work, a trailing `/` is a directory of `part-{part}<extension>` files. |
| `mode` | `overwrite` (default), `append`, `error_if_exists` | What happens when a file of the same name exists. `append` works for JSON Lines, CSV and raw text, or with `{part}` for every format. |
| `write_mode` | `append` (default), `overwrite` | `overwrite` stages the run's files under a hidden `.faucet-overwrite-…/` prefix and swaps them in only after a successful run; a failed run leaves the old output untouched. |
| `parquet` | `compression` (`none`/`snappy`/`gzip`/`zstd`/`lz4`), `row_group_size`, `schema` (explicit fields) | The schema is inferred from each file's first page and widened by later pages. |
| `json_lines` | `pretty` | |
| `encryption` | `{ key: … }` | Encrypt at rest (the `encryption` feature); read back by the `file` source. |

`mode` and `write_mode: overwrite` need `file_name`: without it every run
writes new, uniquely named files (`<run id>-<part><file_extension>`), so
there is nothing to replace or append to.

**Publishing.** Each file is built in a local scratch file and published
with one upload to a hidden temporary name followed by a rename into place when it closes — at `max_records_per_file` /
`max_bytes_per_file` (encoded bytes) or at `flush` — so a reader never sees a
partial file, and a bookmark never advances past records that are not
there.

Files land under `path`. SFTP has no replacing rename, so replacing an existing file (`mode: overwrite` on a fixed name, or promoting a staged file over an existing one) removes it first; new names are published atomically.

```yaml
sink:
  type: sftp
  config:
    # … connection fields …
    file_name: "dt=${now.date}/part-{part}.parquet"
    format: auto
    max_records_per_file: 1000000
    parquet: { compression: zstd, row_group_size: 131072 }
```

## Batch atomicity

What a failed write leaves behind (#737): **atomic** without a rollover cap —
a page is encoded locally and published only at `flush` — otherwise
**best-effort**: files closed at an earlier cap stay. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

# faucet-sink-azure-blob

Azure Blob Storage / ADLS Gen2 **sink** connector for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Writes JSON records to an Azure blob container (or ADLS Gen2 filesystem) as JSON
Lines objects. Built on [`object_store`](https://crates.io/crates/object_store)'s
Azure backend, so classic Blob and ADLS Gen2 are served through one code path.

## Config

Connection fields come from `faucet-common-azure` and are set at the top level:

| Field | Type | Notes |
|---|---|---|
| `container` | string | **Required.** Blob container / ADLS filesystem (must already exist). |
| `account` | string | Storage-account name (optional with a connection string / emulator). |
| `auth` | `{ type, config }` | `account_key` / `sas_token` / `connection_string` / `managed_identity` / `service_principal` / `default`. |
| `endpoint` | string | Custom blob endpoint (emulator / sovereign cloud). |
| `allow_http` | bool | Permit plaintext HTTP (Azurite). |
| `use_emulator` | bool | Target the Azurite emulator. |
| `timeout_secs` | int | Seconds one request, body included, may take. Unset (default) = no limit, so a long body read paced by the pipeline is not cut off. |
| `connect_timeout_secs` | int | Seconds to wait for a connection (default `10`). |
| `max_retries` | int | Retries of a failed request, including resuming an interrupted body read (default `10`). |
| `retry_timeout_secs` | int | Seconds after a request first went out during which it may still be retried or resumed (default `600`); keep it within the credential's lifetime. |

Sink-specific fields:

| Field | Type | Default | Notes |
|---|---|---|---|
| `prefix` | string | `""` | Object-name prefix; a virtual "directory" in the flat blob namespace. |
| `format` | enum | `json_lines` | `json_lines` / `json_array` / `csv` / `xml` / `xlsx` / `avro` — see [File formats](#file-formats-604). |
| `file_extension` | string | `.jsonl` | Extension for written objects. |
| `max_records_per_file` | int | — | Cap records per object (file rollover). |
| `concurrency` | int | `10` | Max uploads in flight: a closed blob uploads in the background while the next is encoded, and a large blob's blocks go up this many at a time. `flush` waits for every upload. |
| `batch_size` | int | `1000` | Records per blob when `max_records_per_file` is unset (without `path`). `0` = no record cap: one blob per `flush`; Parquet one per `write_batch`. |
| `compression` | enum | `auto` | `auto` / `gzip` / `zstd` (requires the `compression` feature); resolved from `file_extension`. |

Object names are `{prefix}{uuidv7}{file_extension}` — time-sortable so a listing
returns objects in write order. The container is not created automatically; the
prefix is virtual (blob namespaces are flat).

## Example

```yaml
pipeline:
  sink:
    type: azure-blob
    config:
      container: exports
      account: mystorageacct
      auth: { type: account_key, config: { account_key: "${env:AZURE_KEY}" } }
      prefix: events/2026/
      batch_size: 0
```


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

Closed blobs upload in the background while the next one is encoded, up to
`concurrency` at a time; every `write_batch` waits for the uploads it started
(so a failed upload fails the page that wrote it) and `flush` waits for all of
them. `batch_size: 0` removes the record cap: one blob per `flush`, and for
Parquet one blob per `write_batch` call, so each page stays a self-contained
file.

Large objects stream through **multipart** upload: a part is sent as soon as it
fills and its buffer is dropped, so peak memory is O(part size) rather than
O(object size). The upload is started lazily on the first full part, so an
object that fits in one part stays a single request and leaves nothing
abandoned if the run dies early. With a `compression` codec configured each
part is compressed independently — gzip and zstd both concatenate, so the
object decodes transparently, and compressing the whole body instead would
mean buffering it, which is the bound multipart exists to remove.

## File formats (#604)

Beyond JSON Lines this sink writes **JSON array**, **CSV**, **XML** and
**Excel** through `faucet_core::file_format`, so what it writes is exactly what
the file *sources* can read back.

```yaml
sink:
  type: azure-blob
  config:
    container: reports
    format: json_array
    file_extension: .json
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

**Experimental** (PRINCIPLES.md §3): this block's shape may change in a minor release; any change is called out in the changelog.

| Field | Values | Notes |
|---|---|---|
| `format` | `json_lines` (default), `json_array`, `csv`, `xml`, `xlsx`, `avro`, `parquet`, `raw_text`, `auto` | `auto` takes the format from `path`'s extension (else `file_extension`), looking through `.gz` / `.zst`. `parquet` needs the `arrow` feature; the other shared formats their `file-format-*` feature. |
| `path` | a name template | blob name template: `{part}` numbers the blobs, `${now.*}` tokens work, a trailing `/` is a directory of `part-{part}<extension>` blobs. |
| `if_exists` | `replace` (default), `append`, `error` | What happens when a blob of the same name exists. `append` works for JSON Lines, CSV and raw text, or with `{part}` for every format. `mode` is accepted as another name for this key, and `overwrite` / `error_if_exists` for its values. |
| `write_mode` | `append` (default), `overwrite` | `overwrite` writes the run's blobs under a hidden `.faucet-overwrite-…/` prefix and moves them into place only after a successful run; a failed run leaves the old output untouched. The move is one blob at a time: a reader listing the prefix while it runs can see new blobs beside old ones, and a move that stops half-way is finished by the next run. |
| `parquet` | `compression` (`none`/`snappy`/`gzip`/`zstd`/`lz4`, default **`zstd`**, like the S3 and GCS sinks — smaller objects to move; the local `file` sink defaults to `snappy`), `row_group_size`, `schema` (explicit fields) | The schema is inferred from each blob's first page and widened by later pages. |
| `json_lines` | `pretty` | |
| `encryption` | `{ key: … }` | Encrypt at rest (the `encryption` feature); read back by the `file` source. |
| `scratch_dir` | a local directory | Where blobs are built before upload (default: the system temporary directory; a private subdirectory is created in it). JSON Lines and raw text go up as a block upload while they are written and need no scratch space; other formats need room for each blob being built (up to `concurrency` of them). Scratch files are not encrypted while the run is in progress. |

`if_exists: append` / `error` and `write_mode: overwrite` need `path`: without it every run
writes new, uniquely named blobs (`<run id>-<part><file_extension>`), so
there is nothing to replace or append to.

**Publishing.** JSON Lines and raw text go up as a block upload while they are written — memory holds at most `concurrency` parts, and no scratch file is written — and the blob becomes visible when it closes. Every other blob is built in a local scratch file and published
with one upload (a committed block list past 8 MiB, aborted on failure) when it closes — at `max_records_per_file` /
`max_bytes_per_file` (encoded bytes) or at `flush` — so a reader never sees a
partial blob, and a bookmark never advances past records that are not
there.

Blob names are `prefix + path`.

```yaml
sink:
  type: azure-blob
  config:
    # … connection fields …
    path: "dt=${now.date}/part-{part}.parquet"
    format: auto
    max_records_per_file: 1000000
    parquet: { compression: zstd, row_group_size: 131072 }
```


> **Shared destinations are refused at load.** With `path` set, the CLI refuses two matrix rows writing the same destination and a fan-out row without a per-invocation token (`${parent.id}`) in it: concurrent writers would overwrite each other's parts and prune the rest.

## Batch atomicity

What a failed write leaves behind (#737): **atomic** without a rollover cap —
a page is encoded locally and published only at `flush` — otherwise
**best-effort**: blobs closed at an earlier cap stay. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## Retries

The blob client retries throttling (429) and server errors (5xx) itself,
with exponential backoff, up to 10 times within 3 minutes per request. A
request that still fails after that is a sink error; the client does not
expose the final status, so a pipeline `resilience:` policy does not retry
it again.

## License

MIT

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

Sink-specific fields:

| Field | Type | Default | Notes |
|---|---|---|---|
| `prefix` | string | `""` | Object-name prefix; a virtual "directory" in the flat blob namespace. |
| `file_extension` | string | `.jsonl` | Extension for written objects. |
| `max_records_per_file` | int | — | Cap records per object (file rollover). |
| `concurrency` | int | `10` | Max concurrent uploads. |
| `batch_size` | int | `1000` | Records per object; `0` writes one object per `write_batch` (recommended). |
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

Large objects stream through **multipart** upload: a part is sent as soon as it
fills and its buffer is dropped, so peak memory is O(part size) rather than
O(object size). The upload is started lazily on the first full part, so an
object that fits in one part stays a single request and leaves nothing
abandoned if the run dies early. With a `compression` codec configured each
part is compressed independently — gzip and zstd both concatenate, so the
object decodes transparently, and compressing the whole body instead would
mean buffering it, which is the bound multipart exists to remove.

## License

MIT

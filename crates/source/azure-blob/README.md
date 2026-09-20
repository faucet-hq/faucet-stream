# faucet-source-azure-blob

Azure Blob Storage / ADLS Gen2 **source** connector for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Lists and reads objects from an Azure blob container (or ADLS Gen2 filesystem)
and emits them as JSON records. Built on
[`object_store`](https://crates.io/crates/object_store)'s Azure backend, so both
classic Blob and ADLS Gen2 hierarchical namespaces are supported through one
code path.

## Config

Connection fields come from `faucet-common-azure` and are set at the top level:

| Field | Type | Notes |
|---|---|---|
| `container` | string | **Required.** Blob container / ADLS filesystem. |
| `account` | string | Storage-account name (optional with a connection string / emulator). |
| `auth` | `{ type, config }` | `account_key` / `sas_token` / `connection_string` / `managed_identity` / `service_principal` / `default`. |
| `endpoint` | string | Custom blob endpoint (emulator / sovereign cloud). |
| `allow_http` | bool | Permit plaintext HTTP (Azurite). |
| `use_emulator` | bool | Target the Azurite emulator. |

Source-specific fields:

| Field | Type | Default | Notes |
|---|---|---|---|
| `prefix` | string | — | Object-name prefix filter. Ignored when `object_keys` is set. |
| `object_keys` | list | — | Explicit object names; skips listing. |
| `file_format` | enum | `json_lines` | `json_lines` / `json_array` / `raw_text` / `csv` / `xml` / `xlsx`. |
| `max_objects` | int | — | Hard cap on objects read. |
| `concurrency` | int | `10` | Max concurrent object reads, on the streaming path as well as the batch one. The streaming prefetch is ordered, so records stay in listing order; `0` is clamped to 1. For `json_lines` it overlaps only the request setup (peak memory stays `O(batch_size)`); for `json_array` / `raw_text` up to `concurrency` whole bodies are resident. |
| `batch_size` | int | `1000` | Records per `StreamPage`; `0` = one page per object. |
| `verify_length` | bool | `true` | Verify each object's byte count against the `size` Azure reports; a short (truncated) or over-long transfer fails with `FaucetError::Source`. See [Read-integrity verification](#read-integrity-verification). |
| `verify_checksum` | bool | `false` | **Not supported on Azure Blob** — `true` is rejected at config load. See [Read-integrity verification](#read-integrity-verification). |
| `compression` | enum | `auto` | `auto` / `gzip` / `zstd` (requires the `compression` feature). |

## Read-integrity verification

A transfer that terminates early but *cleanly* — a truncated body that still
yields EOF — would otherwise be parsed and emitted as a complete object: silent
data loss with a green run. To prevent it, every object body is read through
[`faucet_core::VerifyingReader`], which counts the raw bytes and validates them
at EOF.

- **Length** (`verify_length`, default `true`) — compares the bytes read against
  the `size` the store reports for the blob and fails the read on any mismatch.
  The check is cheap (a counter over a body that is read anyway) and wraps the
  **raw** stream, below any decompression, so it covers the *stored* bytes. It is
  skipped, with a debug log, for a blob served with a non-empty
  `Content-Encoding` (a store may transcode it on read, so the received byte
  count need not match the stored size).
- **Checksum** (`verify_checksum`) — **unsupported here.** Azure Blob does not
  expose a body checksum (`Content-MD5`) through the `object_store` read API this
  connector uses, so rather than accept a switch it cannot honour, the source
  **rejects `verify_checksum: true`** at config load with a typed
  `FaucetError::Config`. The `verify_length` guard above still applies. The S3
  and GCS sources, whose stores do advertise a checksum, support the field.

Both keys are named and behave identically across the S3, GCS, and Azure Blob
sources.

## File formats (#604)

- **`json_lines`** — one JSON record per line; streamed line-by-line (bounded memory).
- **`json_array`** — the whole object is a JSON array; buffered then chunked.
- **`raw_text`** — each object becomes one record `{ "key", "content" }`.
- **`csv` / `xml` / `xlsx`** — decoded through `faucet_core::file_format` (below); buffered then chunked.

Beyond JSON Lines, JSON array and raw text, this source reads **CSV**, **XML**
and **Excel** through `faucet_core::file_format`, so the records it produces
match what every other file connector produces for the same bytes.

```yaml
source:
  type: azure-blob
  config:
    container: exports
    file_format: xlsx
    excel: { sheet: "Q3", header_row: 0 }
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

## Example

```yaml
pipeline:
  source:
    type: azure-blob
    config:
      container: raw
      account: mystorageacct
      auth: { type: account_key, config: { account_key: "${env:AZURE_KEY}" } }
      prefix: events/2026/
      file_format: json_lines
```

## License

MIT

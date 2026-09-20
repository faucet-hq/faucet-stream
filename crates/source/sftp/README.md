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


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
| `format` | enum | `jsonl` | `jsonl` \| `json_array` \| `raw_text`. |
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

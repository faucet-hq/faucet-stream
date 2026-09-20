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

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


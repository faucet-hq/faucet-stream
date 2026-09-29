# faucet-common-file

Shared configuration types for the [faucet-stream](https://github.com/faucet-hq/faucet-stream)
local file connectors — `faucet-source-file` and `faucet-sink-file` — so both
spell the format choice, the path kinds and the extension rules the same way.

- `FileFormatChoice` — `auto` (resolve from the file's extension, through a
  `.gz` / `.zst` suffix) or one explicit format; `resolve(name, strict)` applies
  the rule.
- `FileFormatChoice::resolve_writable(name)` — the sink-side rule: the format
  a file is written in, refusing unknown extensions and read-only ORC.
- `resolve_compression(config, path)` — the codec for a local path or URL,
  resolved from its suffix under `auto`.
- `compresses_internally` / `appendable` — format properties: Parquet, Avro
  and xlsx compress internally; only JSON Lines, CSV and raw text can be
  appended to in place.
- `is_http_path` / `url_file_name` / `resolution_name` — how an `http(s)://`
  path is recognised and which part of it names the file.
- `is_directory_path` / `require_path` — directory paths and the empty-path
  refusal.
- `config_context(connector, error)` — prefix a config error with the
  connector's name; other errors pass through.

## `write`: the shared file-writing layer

`write::FileWriter` is everything a file-writing sink does that does not
depend on where files are stored: every writable format (JSON Lines, JSON
array, CSV, XML, Excel, Avro, Parquet, raw text), compression, encryption at
rest, `{part}` rollover by records or bytes, `mode: overwrite | append |
error_if_exists`, the `write_mode: overwrite` stage-and-swap, CSV headers
written at finalisation, and Parquet schema inference, widening or an explicit
schema. A sink builds a `WriteSettings` from its config and a `NameTemplate`
from its path, and hands the writer a `StorageBackend`:

| Method | Does |
|---|---|
| `scratch_path(area, name)` | a local path to build the file in before it is committed |
| `commit(scratch, area, name)` | publish a finished file atomically (rename, or complete an upload) |
| `fetch(area, name, to)` / `exists` / `list` / `delete` | read back, check, list and remove files (for append, rollover numbering and cleanup) |
| `prepare(area)` | create the directory, check the bucket |
| `begin_staging` / `staging_ready` / `promote(name)` / `clear_staging` | the overwrite staging area |
| `describe`, `local_path`, `remove_stale_scratch` | messages, local-output previews, crash leftovers |

`LocalBackend` is the local-filesystem backend the `file` sink uses: scratch
files beside the destination, `fsync`, and a rename into place.

Features mirror the formats: `file-format-csv`, `-xml`, `-excel`, `-avro`,
`-parquet` (`file-formats` is all of them), `arrow`, `encryption`.

Connector authors normally depend on the source or sink crate, not this one.

```toml
[dependencies]
faucet-common-file = "1.0.0"
```

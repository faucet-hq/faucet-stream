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
- `compresses_internally` / `appendable` — the format properties both sides
  consult (file-level compression does not apply to Parquet / Avro / xlsx;
  only JSON Lines, CSV and raw text can be appended to).
- `is_http_path` / `url_file_name` / `resolution_name` — how an `http(s)://`
  path is recognised and which part of it names the file.
- `is_directory_path` / `require_path` — directory paths and the empty-path
  refusal.

Connector authors normally depend on the source or sink crate, not this one.

```toml
[dependencies]
faucet-common-file = "1.0.0"
```

# faucet-common-file

Shared configuration types for the [faucet-stream](https://github.com/faucet-hq/faucet-stream)
local file connectors — `faucet-source-file` and `faucet-sink-file` — so both
spell the format choice, the path kinds and the extension rules the same way.

- `FileFormatChoice` — `auto` (resolve from the file's extension, through a
  `.gz` / `.zst` suffix) or one explicit format; `resolve(name, strict)` applies
  the rule.
- `is_http_path` / `url_file_name` — how an `http(s)://` path is recognised and
  which part of it names the file.

Connector authors normally depend on the source or sink crate, not this one.

```toml
[dependencies]
faucet-common-file = "1.0.0"
```

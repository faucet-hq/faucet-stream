# faucet-common-singer

Shared [Singer](https://www.singer.io/) protocol types for the
[faucet-stream](https://github.com/faucet-hq/faucet-stream) Singer bridges:

- `faucet-source-singer` — run any Singer **tap** as a faucet source.
- `faucet-sink-singer` — run any Singer **target** as a faucet sink.

This crate holds what both sides need:

| Module | Contents |
|---|---|
| `message` | `SingerMessage`, `parse_line` (one stdout line → message), and the `write_schema` / `write_record` / `write_state` / `write_activate_version` encoders |
| `redact` | `Redactor` (scrubs config string values out of echoed stderr) and `secret_like_values` (values under secret-looking keys, for a process-wide log redactor) |
| `temp` | `write_private_json` — a 0600 temp file for `--config` / `--catalog` / `--state` |
| `env` | `InheritEnv` — the `inherit_env` setting: inherit faucet's whole environment (`true`, default), only the `BASELINE_ENV` (`false`), or the baseline plus named variables (a list) |

You normally depend on the source or sink crate, which re-export these types.

```toml
[dependencies]
faucet-common-singer = "1"
```

License: MIT OR Apache-2.0

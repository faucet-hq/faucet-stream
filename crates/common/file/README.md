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

**Experimental** (PRINCIPLES.md §3): this block's shape may change in a minor
release; any change is called out in the changelog.

`write::FileWriter` is everything a file-writing sink does that does not
depend on where files are stored: every writable format (JSON Lines, JSON
array, CSV, XML, Excel, Avro, Parquet, raw text), compression, encryption at
rest, `{part}` rollover by records or bytes, `if_exists: replace | append |
error`, the `write_mode: overwrite` swap, CSV headers written when a file is
published, and Parquet schema inference, widening or an explicit schema.

A sink maps its config onto `WriteConfig` — the one mapping every file sink
shares (record and byte caps, `batch_size`, the path rules, the per-sink
Parquet codec default) — which gives it the `WriteSettings` and the
`NameTemplate`. It builds its store's `StorageBackend` and the one generic
`FileSink<B>` over it (`FileSink::build(settings, template, backend,
identity)`; the one `Sink` implementation: batch writes, flush, the overwrite
lifecycle, the end-of-run prune, the columnar path), and forwards its own
`Sink` impl with `delegate_sink!`, which forwards every hook through
`faucet_core::forward_sink!`. The connector supplies only its name, config
schema, dataset URI and readback source through `SinkIdentity`.

`FileSink` is the object-store family driver's sink (RFC 0014): the backend
is an `ObjectStoreDriver` (every `StorageBackend` is one) and `FileDriver<B>`
— a `faucet_core::driver::SinkDriver` with the swap area as its
`StagingDriver` and a `ColumnarDriver` for Parquet / Avro — runs inside a
`DriverSink`, which derives the sink's capabilities from it.

Every file sink is a `FileSink` (RFC 0013, #886):

| Sink | Is | Backend from |
|---|---|---|
| `file` | `FileSink<LocalBackend>` | this crate |
| `s3` (and the deprecated `parquet` sink's S3 destination) | `FileSink<S3Backend>` | `faucet-common-s3` |
| `gcs` | `FileSink<GcsBackend>` | `faucet-common-gcs` |
| `azure-blob` | `FileSink<AzureBackend>` | `faucet-common-azure` |
| `sftp` | `FileSink<SftpBackend>` | `faucet-common-sftp` |

So every one of them writes every format, compression, encryption, rollover,
`if_exists` and `write_mode: overwrite` behaviour the same way. A backend's
requests run through the run's call context and report to its round-trip
recorder (`StorageBackend::set_call_context` / `set_roundtrip_recorder`), and
its `check` is the sink's preflight probe; the identity's `check` answers
only when the backend has none (the local `file` sink). `WriterSink` is the
same sink with the backend type left unnamed, built from a `FileWriter`.

Every writer method is `async`: encoding runs on the calling task, storage I/O
is awaited, so a cancelled run stops waiting on a hung upload. A failure that
loses records an earlier call was told were written (a rollover, flush or
upload of a file holding them) poisons the writer: every later write and
flush fails.

| `StorageBackend` method | Does |
|---|---|
| `scratch_path(area, name)` | a local path to build the file in before it is published |
| `publish(scratch, area, name)` | publish a finished file atomically (rename, or complete an upload); may return before a background upload lands |
| `settle()` / `cancel()` | wait for background publishes and report the first error / wait for them and delete what they published |
| `fetch(area, name, to)` / `exists` / `list` / `delete` | read back, check, list and remove files |
| `promote(name)` | move a file from the overwrite swap area into place |
| `join_overwrite_run(token)` | (defaulted: refused) make the swap area this overwrite run's own, `.faucet-overwrite-<tag>.run-<token>`, so concurrent runs never share one; `LocalBackend` and `RemoteBackend` support it |
| `swap_runs()` / `drop_swap_run(run)` | (defaulted: none / no-op) list every run's tokened swap area; delete one (`None`: the untokened area) — how a tokened `begin` drops the untokened area and those a run abandoned more than 24 hours ago |
| `prepare(area)` | create the directory, remove this output's crash leftovers |
| `part_size()` / `open_stream(area, name)` | upload a line format in parts while it is written |
| `describe`, `local_path` | messages, local-output previews |
| `set_call_context` / `set_roundtrip_recorder` / `check` | the run's call context and recorder for every request; a preflight probe (`None`: the sink's own) |

`LocalBackend` is the local-filesystem backend the `file` sink uses: scratch
files beside the destination, `fsync`, and a rename into place, each call on
Tokio's blocking pool. `RemoteBackend` turns an `ObjectClient` (list, exists,
download, upload, delete, rename) into a backend with a private scratch
directory and up to `concurrency` background uploads; a `MultipartClient`
lets JSON Lines and raw text go up in parts as they are written, so neither
memory nor scratch disk grows with the object. A store's common crate wraps a
`RemoteBackend` over its `ObjectClient` in its own named backend type and
forwards the trait with `delegate_storage_backend!`.

The overwrite swap writes a `.faucet-swap` marker when it starts and a
`.faucet-commit` marker listing the files before the first one moves, so a move
that stops half-way is finished by the next commit, abort or run. Readers can
see the per-file move in progress: the set is not replaced atomically.

Scratch files are named `<file>.faucet-tmp` plus one of `SCRATCH_ROLES`
(`-body`, `-old`, `-seal`, `-prev`); `is_scratch_name` and `is_swap_dir_name`
let a reader skip them. Scratch files are not encrypted while a run is in
progress: those holding plaintext of an encrypted output are created
readable by their owner only. A remote backend's private scratch directory
(`faucet-remote-*`) holds an `.owner` file locked for the run's life; a new
backend removes every such directory whose lock it can take — the scratch of a
run that crashed (SIGKILL, OOM) — so plaintext does not outlive the run.

Features mirror the formats: `file-format-csv`, `-xml`, `-excel`, `-avro`,
`-parquet` (`file-formats` is all of them), `arrow`, `encryption`.

`sealed_lines` (feature `encryption`) is the per-line sealed JSON Lines layout:
`LineSeal` writes the header, bound record lines and the trailer (record count
plus a SHA-256 digest of the lines), and `open` / `plaintext` verify a file whole
— a file written before the header existed is read line by line with a warning.
`ObjectFilter` is the object-store listing filter the s3 / gcs / azure-blob
sources share: folder markers and `_`/`.`-prefixed keys are skipped unless an
`include` glob names exactly what to read.

Connector authors normally depend on the source or sink crate, not this one.

```toml
[dependencies]
faucet-common-file = "1.0.0"
```

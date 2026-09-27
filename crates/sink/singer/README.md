# faucet-sink-singer

> **Support tier: Tier-2 / experimental.** Best-effort — correctness bugs are
> fixed, but breadth of testing and upstream-drift tracking are not guaranteed.

A [Singer](https://www.singer.io/) target bridge sink for
[faucet-stream](https://github.com/faucet-hq/faucet-stream). It runs an
existing Singer **target** executable and feeds it faucet records as a Singer
stream — so a target you depend on keeps working while the rest of a Meltano
project moves to faucet. Together with
[`faucet-source-singer`](../../source/singer) (the tap bridge), a Meltano
pipeline can be migrated one side at a time.

## Honest trade-offs

- **It reintroduces a runtime dependency.** Most Singer targets are Python; a
  pipeline using this sink needs that interpreter (and the target) installed.
  Use a native faucet sink when one exists.
- **Throughput is Singer-class.** Records cross a process boundary as
  newline-delimited JSON.
- **At-least-once.** A bookmark is saved only after the target confirms, so a
  crash re-sends the unconfirmed records — the Singer contract. Pair with a
  target that merges on `key_properties` (`write_mode: upsert`) to absorb the
  replay.

## What the target receives

| Message | When |
|---|---|
| `SCHEMA` | First page a target process sees, and again whenever an inferred schema widens (a new field, a new type, a field turning nullable). The schema is `schema` if set, else the pipeline's `contract:` (the `faucet` CLI converts it to JSON Schema), else inferred from the records. `key_properties` = `key` under `write_mode: upsert` (override with `key_properties`). |
| `RECORD` | One per record. Records must be JSON objects. Under `write_mode: overwrite` each carries `version`. |
| `STATE` | At every pipeline flush: `{"faucet_flush": {"stream": …, "seq": N}}` — the flush marker. |
| `ACTIVATE_VERSION` | After a successful `write_mode: overwrite` run. |

## Durability — `flush_on`

The pipeline persists a bookmark only after the sink's `flush` returns, and
`flush` returns only after the target has confirmed everything written so far:

- **`exit` (default)** — the sink sends the flush `STATE`, closes the target's
  stdin and waits for it to exit `0`; the next page starts a fresh target (and
  a fresh `SCHEMA`). Works with every target: most (including Meltano SDK
  targets and `target-jsonl`) drain and emit `STATE` only at end of input.
- **`state`** — one long-lived target; the sink sends the flush `STATE` and
  waits for the target to echo it back (the Singer "everything before this
  state is persisted" signal). For targets that echo `STATE` as soon as the
  preceding records are persisted. A target that never echoes fails the flush
  after `flush_timeout_secs` with a hint to use `exit`.

A flush happens whenever a source page carries a bookmark and at the end of the
run.

## Back-pressure

Records are encoded into a fixed 64 KiB buffer and written to the target's
stdin pipe, awaiting the pipe on every write: a slow target stalls the pipeline
rather than records piling up in memory. The target's stdout is drained
continuously (only the newest echoed `STATE` is kept), so a chatty target never
blocks either.

## Failures

A target that exits non-zero, closes its stdin, or does not confirm in time
fails the write or flush with its last 20 stderr lines. Every string value of
`target_config` is scrubbed from that text (`***`), and values under
secret-looking keys (`password`, `token`, `secret`, `key`, …) are registered
with the `faucet` CLI's log redactor. Once a target has failed while holding
unconfirmed records, every later write and flush on that sink fails too — no
bookmark can advance past records that were lost.

## Overwrite

`write_mode: overwrite` uses Singer table versions: every record carries the
run's `version` (a millisecond timestamp shared by every writer of a run), and
after a fully successful run the sink starts the target once more to send
`SCHEMA` + `ACTIVATE_VERSION`, telling the target to make that version live and
discard rows from earlier versions. A failed or cancelled run sends no
`ACTIVATE_VERSION`, so the previous version stays live (and the next
successful overwrite discards the failed run's rows). How a target applies
versions is target-specific.

`write_mode: delete` and `delete_marker` are rejected — Singer has no delete
message. Upsert is passed to the target as `key_properties`; the merge itself
is the target's job, so faucet does not treat this sink as key-deduplicating
(`delivery: exactly_once` is not available).

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `target_command` | string | — (required) | Target executable on `PATH` or an absolute path |
| `args` | string[] | `[]` | Extra args appended after faucet's `--config <file>` |
| `target_config` | object | `{}` | The target's config — written to a private (0600) temp file passed as `--config` |
| `env` | map | `{}` | Extra environment variables for the target |
| `stream` | string | row id / pipeline name (`faucet` for library callers) | Singer stream name |
| `schema` | object | contract, else inferred | JSON Schema for the `SCHEMA` message |
| `key_properties` | string[] | `key` under upsert | `SCHEMA` `key_properties` |
| `flush_on` | `exit` \| `state` | `exit` | See [Durability](#durability--flush_on) |
| `flush_timeout_secs` | int | `600` | Max wait for the echo / exit at a flush |
| `write_mode` | `append` \| `upsert` \| `overwrite` | `append` | See above |
| `key` | string[] | `[]` | Upsert key (becomes `key_properties`) |

Stream naming in the `faucet` CLI: when `stream` is unset the matrix row id is
used (a single-row config uses the pipeline `name`); a Template Hub sink
template sets it per stream with `stream: "${stream}"`.

```yaml
version: 1
name: people
pipeline:
  source:
    type: csv
    config: { path: ./people.csv }
  sink:
    type: singer
    config:
      target_command: target-jsonl
      target_config:
        destination_path: ./out
        do_timestamp_file: false
```

## Library use

```rust,no_run
use faucet_core::Pipeline;
use faucet_sink_singer::{SingerSink, SingerSinkConfig};
# async fn run(source: &dyn faucet_core::Source) -> Result<(), faucet_core::FaucetError> {
let mut cfg = SingerSinkConfig::new("target-jsonl");
cfg.stream = Some("people".into());
cfg.target_config = serde_json::json!({ "destination_path": "./out" });
let sink = SingerSink::new(cfg)?;
Pipeline::new(source, &sink).run().await?;
# Ok(()) }
```

## Preflight (`faucet doctor`)

`check()` verifies the target executable resolves and its config can be written
to a private temp file. It does not run the target — Singer targets have no
side-effect-free probe mode.

## License

Licensed under either of Apache-2.0 or MIT at your option.

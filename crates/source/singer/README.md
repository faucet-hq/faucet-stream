# faucet-source-singer

> **Support tier: Tier-2 / experimental.** Best-effort — correctness bugs are
> fixed, but breadth of testing and upstream-drift tracking are not guaranteed.

A [Singer](https://www.singer.io/) tap bridge source for
[faucet-stream](https://github.com/faucet-hq/faucet-stream). It runs an
existing Singer **tap** executable and adapts its stdout message stream into
faucet records — so any of the hundreds of community taps can feed a faucet
pipeline. Its counterpart, [`faucet-sink-singer`](../../sink/singer), runs a
Singer **target** as a faucet sink; the protocol types both share live in
[`faucet-common-singer`](../../common/singer).

## Honest trade-offs

- **It reintroduces a runtime dependency.** Most Singer taps are Python; a
  pipeline using this source needs that interpreter (and the tap) installed on
  the box. This is the one place faucet steps outside its "single static binary"
  story — use a native faucet source when one exists.
- **Throughput is Singer-class, not faucet-class.** Records cross a process
  boundary as newline-delimited JSON; expect tap-bound throughput, not faucet's
  native streaming numbers.
- **Resume granularity depends on the tap.** faucet checkpoints at the tap's own
  `STATE` messages; how coarse or fine that is (and whether re-emitted rows
  overlap) is a property of the individual tap. Pair with an idempotent sink for
  clean **effectively-once** (idempotent at-least-once) behavior.
- **Stopping on Windows.** On Unix the tap gets `SIGTERM` and a grace period
  before it is killed. Windows has no `SIGTERM`, so the tap gets the same grace
  period to exit on its own and is then terminated. `executable` must be
  something Windows can launch directly: an `.exe` (pip's console-script
  launchers are) or a `.cmd`/`.bat` named with its extension.

## v0 scope

- **Single-stream.** Exactly the configured `stream` is emitted; RECORD messages
  for other streams are ignored. Multi-stream fan-out is future work.
- Handles `RECORD`, `SCHEMA` (pass-through — faucet sinks infer schema from
  records), and `STATE` (resume bookmark). `ACTIVATE_VERSION` / `BATCH` are
  logged and skipped.

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `executable` | string | — (required) | Tap binary on `PATH` or an absolute path |
| `stream` | string | — (required) | The single stream to emit. RECORDs match it or, with a `catalog`, the entry's other name (`stream` ↔ `tap_stream_id`). RECORDs for other streams are dropped with one warning per stream; if the tap emits RECORDs only for other streams, the run fails at the first STATE instead of checkpointing past rows it never delivered |
| `args` | string[] | `[]` | Extra args appended after faucet's `--config`/`--catalog`/`--state` |
| `tap_config` | object | `{}` | The tap's config (secret-resolved by faucet; written to a private temp file) |
| `catalog` | object | — | Singer catalog, passed as `--catalog` |
| `state_key` | string | `singer:{executable}:{stream}` | State-store key for the resume bookmark |
| `flush_on_state` | bool | `true` | Flush a page (and checkpoint) on every STATE message |
| `idle_timeout_secs` | int / null | `3600` | Abort if no output arrives within this many seconds (`null` waits forever). Also bounds `--discover` |
| `max_line_bytes` | int | `67108864` (64 MiB) | Longest tap output line accepted; a longer one fails the run instead of exhausting memory |
| `on_malformed` | `skip` \| `fail` | `skip` | What to do with a non-Singer output line |
| `inherit_env` | bool \| string[] | `true` | Which of faucet's environment variables the tap sees — see [Environment](#environment--inherit_env) |

## Environment — `inherit_env`

The tap runs as a child of faucet and, by default, sees faucet's whole
environment, as it would from a shell. That includes whatever credentials
faucet itself holds there (cloud keys, vault tokens, a `faucet serve`
server's secrets). To keep them away from the tap:

```yaml
inherit_env: false                 # only PATH, HOME, LANG, LC_ALL, TMPDIR
inherit_env: [TAP_API_TOKEN]       # that baseline plus the listed variables
```

Pass credentials the tap needs through `tap_config` (resolved by faucet and
written to a private file) rather than through the environment.

## Catalog-driven stream selection

Most database and Meltano-SDK taps **sync nothing** unless the target stream is
marked `selected` in the catalog — a catalog passed through verbatim is a silent
no-op. Parent-keyed taps go further: `tap-github`'s `issues` stream only syncs
when its parent `repositories` stream is *also* selected, even though faucet
emits just `issues`.

`faucet init --source singer --discover --executable <tap> --stream <name>` runs
the tap's discovery and writes a catalog with `<name>` — **and any inferable
parent streams** — marked `selected` (both the stream-level flag and the
`breadcrumb: []` metadata). When the tap doesn't express a parent relationship in
its catalog, `faucet init` prints a warning listing the other streams so you can
select a parent manually. Extraction may be multi-stream (parents are pulled to
satisfy the child), but faucet still **emits only the configured `stream`**.

> **`ACTIVATE_VERSION` / `FULL_TABLE` + an append sink can accumulate
> duplicates.** A tap doing full-table reloads re-emits every row each run; an
> append-only sink (jsonl/csv/stdout) keeps them all. Pair such taps with a
> **keyed sink** (`write_mode: upsert`, `key: [...]`) so re-emitted rows converge.

## Example

```yaml
version: 1
pipeline:
  source:
    type: singer
    config:
      executable: tap-github
      stream: issues
      tap_config:
        access_token: ${env:GITHUB_TOKEN}
        repository: faucet-hq/faucet-stream
  sink:
    type: file
    config:
      path: ./out/issues.jsonl
  state:
    type: file
    config:
      path: ./state
```

## License

Licensed under either of Apache-2.0 or MIT at your option.

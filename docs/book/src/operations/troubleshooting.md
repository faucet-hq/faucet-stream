# Troubleshooting & FAQ

## My config won't parse / validate

Run `faucet validate <config>` — it reports one line per expanded row. Common
causes:

- **`version` missing or not `1`** — the top-level `version: 1` is required.
- **Old top-level `source:` / `sink:`** — these must live under `pipeline:`.
  faucet rejects the pre-`pipeline:` shape with a hint.
- **Unknown connector `type`** — run `faucet list` to see what's compiled in; you
  may have a slim build without that feature.
- **`InterpolationCycle`** — a `${vars.X}` / template reference forms a loop.

## A `${env:VAR}` isn't being substituted

Load-time interpolation reads the environment and a sibling `.env`. If the value
is empty, the var isn't set (or `--no-env-file` disabled the `.env`). Use
`--env-file PATH` to point at a specific file.

## "feature not enabled" / connector missing

Your binary was built without that connector. Reinstall with the feature:
`cargo install faucet-cli --features "source-foo,sink-bar"`, or use the full
build (the default `cargo install faucet-cli`).

## docs.rs shows fewer APIs than I expected

It shouldn't anymore — every crate is configured to build with all features. If
you're looking at an old version, check the latest release.

## Kafka connector fails to build

The Kafka crates build `librdkafka`, which needs `cmake` and a C toolchain. Make
sure those are installed in your build environment (CI installs
`libsasl2-dev libssl-dev libcurl4-openssl-dev cmake build-essential`).

## Postgres CDC retains a lot of WAL

A CDC replication slot retains WAL until a run advances the bookmark. If you
created a permanent slot and stopped running the pipeline, Postgres keeps WAL
forever. Either run the pipeline regularly, drop the slot
(`PostgresCdcSource::drop_slot()` or `SELECT pg_drop_replication_slot(...)`)
when you are done with it. (`slot_type: temporary` is refused at config load: a
temporary slot is dropped with the session that creates it, before replication
starts.) A postgres-cdc row also needs a `state:` block — without one the slot
never advances and every run replays from its start. See the
[CDC tutorial](../tutorials/postgres-cdc.md).

## Some records failed but I don't want the run to abort

Attach a [dead-letter queue](../cookbook/dlq.md) so failing rows are captured and
the rest commit.

## Run is slower / using more memory than expected

Tune `batch_size` and concurrency — see [Performance tuning](./tuning.md). Use
the [metrics](./observability.md) to find the bottleneck.

## Where do I report a bug or request a connector?

Open an issue at
[github.com/faucet-hq/faucet-stream/issues](https://github.com/faucet-hq/faucet-stream/issues).

## See also

- [Troubleshooting with `faucet doctor`](../cookbook/troubleshooting.md) — the
  built-in pre-flight that probes every connector in a config before you run it.

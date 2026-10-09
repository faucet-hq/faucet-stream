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

## Error kinds

Every runtime failure carries a typed **kind**. It is the `kind` field in the
logs, the `last error: <Kind>: …` line of [`faucet status`](../cookbook/state-and-status.md),
the `kind` label on `faucet_pipeline_runs_total{status="err"}`, and
`error.kind` in a [DLQ envelope](../cookbook/dlq.md#the-envelope). The kind
says where to look first:

| Kind | Meaning | First move |
|---|---|---|
| `Http` | Transport failure: DNS, connect, TLS, timeout | [`faucet doctor`](../cookbook/troubleshooting.md) |
| `HttpStatus` | A non-success HTTP status, with the URL and a truncated body | Read the body; 401/403 means credentials or grants |
| `RateLimited` | A rate-limit signal outlasted the retries | [Source-side throttling](../cookbook/resilience.md#source-side-throttling) |
| `Auth` | A credential or token flow failed | `faucet doctor`; check the secret references |
| `Config` | Invalid configuration | `faucet validate` |
| `Url` | A URL could not be built | Check base URL and path templating |
| `Json`, `JsonPath` | A response did not parse, or a JSONPath failed | `faucet preview` a few records |
| `Transform` | A transform could not compile or apply | `faucet plan --live` |
| `Source`, `Sink` | A source- or sink-side operation failed (query, file, write) | The message names the operation |
| `QualityFailure` | A quality check with an `abort` policy failed | [Quality checks](../cookbook/quality.md) |
| `ContractViolation` | A record breached the contract under `on_breach: fail` | [Data contracts](../cookbook/contracts.md) |
| `SchemaDrift` | The page's shape diverged from the destination under a `fail` drift policy | [Schema drift](../cookbook/schema-drift.md) |
| `ProfileDrift` | Column profiles drifted under `on_drift: fail` — after the data was written | [Column profiling](../cookbook/profiling.md) |
| `PolicyViolation` | A labelled column would reach a sink the policy forbids | [Data-flow policies](../cookbook/policies.md) |
| `BudgetExceeded` | A run budget was crossed; the crossing page was refused before it was written | [Usage and budgets](../cookbook/usage.md) |
| `State` | A state-store read or write failed | `faucet doctor` (the state probe) |
| `StateIncompatible` | The stored bookmark was written by a newer faucet or another source; refused before the source is read | [Upgrading faucet safely](./upgrading.md) |
| `CircuitOpen` | The circuit breaker saw too many consecutive fully-failed pages | [Resilience](../cookbook/resilience.md) |
| `Custom` | An error from a third-party connector | Read the message |

A connector panic is isolated and counted under the metric kind `Panic`.

Only transient failures are retried automatically: `Http` transport errors
(connect, timeout), `HttpStatus` 429 and 5xx, and `RateLimited`. HTTP sources
retry their own requests; sink writes, flushes and state writes are retried
only under a [`resilience:`](../cookbook/resilience.md) block, and a plain sink
write only when replaying it is safe (a keyed `write_mode: upsert` / `delete`),
because retrying an append whose response was lost would duplicate rows.

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

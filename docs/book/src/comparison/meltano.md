# faucet-stream vs. Meltano (Singer)

*Running Meltano today, or evaluating it? Here's an honest, specific comparison — no strawmen.*

> Reflects each tool as of **2026-07**. Meltano is actively developed; check [meltano.com](https://meltano.com/) for its current state, and hold us to [our benchmarks](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md).

## The short version

**Meltano** is the most popular open-source runtime for the **Singer** spec — a mature, Python-based EL(T) platform with a **600+ tap** ecosystem and a large community. If tap breadth is your first requirement, Meltano is hard to beat.

**faucet-stream** makes a different bet: **one native Rust binary** (or an embeddable library), roughly an order of magnitude faster, with **data governance built into the movement path** — no Python environment to manage, no plugins to assemble for quality, contracts, masking, or lineage.

Move to faucet-stream when **throughput, operational simplicity, or in-flight governance** matter more than raw connector count.

## Where faucet-stream is different

- **Speed you can measure.** On a reproducible 1M-row CSV→JSONL move, faucet does **712k rows/s in 11.8 MiB** vs Meltano's **7.4k rows/s in 724 MiB** — **~96× faster, ~62× less memory**, output identical row-for-row. Sink-bound moves (e.g. Postgres→Postgres) narrow the gap — the [benchmarks](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md) show that scenario too, honestly. The difference is structural: no per-row Python overhead, native streaming with bounded memory.
- **No Python runtime.** faucet is a single static binary — `brew install`, drop it on a box, done. No virtualenv, no plugin resolution, no Python-version matrix to keep green in CI and prod.
- **Governance in the movement path, not bolted on.** Data-quality checks, versioned **data contracts**, **PII masking** (applied *before* any sink sees a row), schema-drift policy, column-level **lineage** (OpenLineage) + a data-movement catalog, and freshness/volume **SLAs** are native and zero-config. In the Singer world these are separate concerns you assemble (mappers, dbt tests, external tooling).
- **Effectively-once delivery.** Per-page commit tokens commit atomically with the data, so a resumed run drops duplicates — across **13 sinks** (SQL, Oracle, Kafka, Iceberg, BigQuery, Snowflake, Spanner, Databricks, MongoDB, Redis), plus a keyed-upsert path on any source into an upsert-capable sink.
- **Embeddable.** Compile the same engine into your own Rust service via the typed `Source` / `Sink` traits — not just a CLI.

## Where Meltano is the better choice

Straight with you, because it's what makes the rest credible:

- **Connector breadth.** 600+ Singer taps vs faucet's **58** built-in connectors. Need a long-tail SaaS source today? Meltano (or a Singer tap) probably already has it.
- **A mature ecosystem & community.** Years of taps, docs, Meltano Hub, and an active community. faucet is younger.
- **You're already invested in Singer/dbt.** If your stack is Singer taps + dbt and it's working, switching only pays off where the wins above are things you actually feel.

## Side-by-side

| | **faucet-stream** | Meltano (Singer) |
|---|---|---|
| Runtime | Rust, single native binary | Python |
| Install | one binary / `brew` / `cargo` | Python env + plugins |
| Connectors | <!--COUNT:connectors-->77<!--/COUNT--> (<!--COUNT:sources-->43<!--/COUNT--> sources, <!--COUNT:sinks-->34<!--/COUNT--> sinks), growing | 600+ taps |
| Throughput (1M-row CSV→JSONL) | **712k rows/s, 11.8 MiB** | 7.4k rows/s, 724 MiB |
| In-flight transforms | ✓ 11 record transforms + filter/explode/CDC-unwrap + embedded-DuckDB `sql` | mappers; dbt post-load |
| Data quality / contracts / masking | ✓ native, in-path | assemble (mappers, dbt tests) |
| Lineage + catalog | ✓ OpenLineage, native | external |
| Effectively-once delivery | ✓ (13 sinks incl. Kafka, Iceberg, BigQuery, Databricks, Oracle) | ✗ |
| Embeddable as a library | ✓ (Rust) | ✗ |
| License | MIT / Apache-2.0 | MIT |

## Migrating from Meltano

The mental model maps cleanly:

| Meltano / Singer | faucet-stream |
|---|---|
| extractor (tap) | a `source` |
| loader (target) | a `sink` |
| `meltano.yml` | a `faucet.yaml` `pipeline:` block |
| Singer `STATE` | a resumable `state:` bookmark |
| stream maps / mappers | `transforms:` (incl. the `sql` transform) |

### Keep a Singer target you depend on

You do not have to replace both sides at once. The `singer` **sink** runs any
existing Singer target and feeds it faucet records, so a pipeline can move one
side at a time: first swap the tap for a native faucet source and keep the
target, then swap the target when a native sink covers it (or run a tap
through the `singer` **source** into a native sink — the mirror image).

```yaml
# was: meltano run tap-csv target-jsonl
version: 1
name: orders
pipeline:
  source:
    type: csv                       # native faucet source replaces the tap
    config: { path: ./data/orders.csv }
  sink:
    type: singer                    # the Singer target you already run
    config:
      target_command: target-jsonl
      target_config:                # what used to be the loader's config in meltano.yml
        destination_path: ./out
```

What carries over and what changes:

| Meltano loader setting | faucet `singer` sink |
|---|---|
| `pip_url` / executable | install the target yourself; `target_command` is its path or `PATH` name |
| loader `config:` | `target_config:` — written to a private (0600) temp file passed as `--config`; its values are scrubbed from the target's stderr in faucet's logs and errors |
| environment variables | `env:` |
| stream name | the matrix row id (or the pipeline `name` for a single-row config); override with `stream:` |
| `key_properties` from the tap | `write_mode: upsert` + `key: [...]` (or `key_properties:`) |
| full-table / `ACTIVATE_VERSION` | `write_mode: overwrite` — records carry the run's version and `ACTIVATE_VERSION` is sent only after a successful run |
| Meltano's state backend | faucet's `state:` block — a bookmark is saved only after the target confirms the records before it |

Confirmation is `flush_on: exit` by default: at every flush the target's input
is closed and faucet waits for a clean exit, which works with every target
(most, including Meltano SDK targets, only emit `STATE` at end of input). For a
target that echoes `STATE` as soon as it has persisted the preceding records,
`flush_on: state` keeps one target running and waits for the echo. A target that
exits non-zero fails the run with its last stderr lines. See the
[`faucet-sink-singer` README](https://github.com/faucet-hq/faucet-stream/tree/main/crates/sink/singer)
and the example `cli/examples/csv_to_singer_target.yaml`.

For a full step-by-step walkthrough with before/after configs, see the
[**Migrating from Meltano/Singer** guide](https://github.com/faucet-hq/faucet-stream/blob/main/docs/blog/migrating-from-meltano.md).
Then start from [your first pipeline](../getting-started/first-pipeline.md) and the [connector catalog](../reference/connectors.md).

## See for yourself

- **[Benchmarks (vs Meltano)](./benchmarks.md)** — the numbers charted per scenario, with full methodology and honest caveats ([raw source](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md)).
- **[Try it in 60 seconds](../getting-started/try-it-locally.md)** — no infrastructure needed.
- **[Choosing a connector](../reference/choosing.md)** — confirm your sources and sinks are covered.

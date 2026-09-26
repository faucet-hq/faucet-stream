# Moving data in Rust shouldn't require a platform — introducing faucet-stream

*Draft launch post. Edit voice/details before publishing, then post to your blog / dev.to / Medium and link it from the Show HN and Reddit threads (see `README.md` in this folder).*

---

Every data team eventually hits the same wall: you need to move records from an
API into a warehouse, from Postgres into Kafka, from S3 into Elasticsearch — and
your options are a Python framework with a plugin runtime to operate, a hosted
SaaS with per-row pricing, or a pile of one-off scripts that rot.

**faucet-stream** is a different answer: a single fast Rust binary (and an
embeddable library) that runs data pipelines from a YAML file.

```bash
cargo install faucet-cli
faucet run pipeline.yaml
```

```yaml
version: 1
pipeline:
  source: { type: postgres, config: { connection_url: "${env:PG_URL}", query: "select * from events" } }
  transforms: [{ type: keys_case, config: { mode: snake } }]
  sink: { type: bigquery, config: { project_id: my-proj, dataset_id: analytics, table_id: events } }
```

No daemon, no scheduler, no Python environment. Drop the binary on a box, point
it at a config, run it on cron. Or `cargo add faucet-stream` and build the same
pipeline from Rust with typed `Source`/`Sink` traits.

## What's in the box

- **<!--COUNT:connectors-->75<!--/COUNT--> connectors** across REST, GraphQL, gRPC, Kafka, Postgres/MySQL/SQLite,
  Postgres/MySQL/Mongo/SQL-Server CDC, S3/GCS/Azure, Parquet/Delta/Iceberg,
  MongoDB, Redis, Elasticsearch, BigQuery, Snowflake, Databricks, and more.
- **Bring your existing Singer taps.** The `singer` source runs any Singer/Meltano
  tap unchanged, so you can adopt faucet incrementally and switch to native
  connectors where throughput matters. It's an experimental v0 bridge — single
  stream, and a bridged tap still runs its own Python process — but it drops the
  switching cost from *rewrite your pipeline* to *point faucet at the tap you
  already run*.
- **A real runtime, not just connectors:** native streaming with bounded memory,
  incremental + resumable replication, change-data-capture, effectively-once
  delivery, dead-letter queues, automatic retries, and built-in Prometheus
  metrics + `tracing` spans — with zero per-connector code.
- **Fast, and honest about it.** On a reproducible 1M-row CSV→JSONL move faucet
  sustains 712k rows/s in 11.8 MiB of RAM (~96× faster, ~62× less memory than
  Meltano, exact row parity); sink-bound moves like Postgres→Postgres narrow the
  gap toward ~16×. The harness is `make bench` — see
  [`BENCHMARKS.md`](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md)
  for the methodology and the caveats.
- **A connector marketplace you can trust.** Every connector is graded against the
  [Faucet Connector Protocol](https://faucet-hq.github.io/faucet-stream/spec/faucet-connector-spec-v0.html)
  by a conformance battery — valid config schema, bounded-memory streaming,
  bookmark round-trip, idempotent replay, truthful capabilities, errors-not-panics
  — that runs in CI and sets each connector's maturity tier.
- **Config-driven *or* embeddable:** the CLI and the library are the same engine.
- **Pay only for what you compile:** every connector is a Cargo feature.

## Why Rust

Performance and reliability are the whole point. Every connector reuses
connections, pools, batches into multi-row inserts and bulk APIs, and streams
with bounded memory — so a pipeline is the fastest way to move its data in Rust,
not a thin wrapper that falls over at scale. And because it's a single static
binary, "deploying" is copying a file.

## Where it fits (and where it doesn't)

faucet-stream is for **moving** data — the EL of ELT. If you need a connector we
don't ship yet, mature ecosystems like Meltano (600+ Singer taps) or Airbyte
still have far broader catalogs today, and we're honest about that in the README.
If your job is in-warehouse transformation, that's dbt's job — pair them. And if
you need a long-running streaming *service*, Benthos/Redpanda Connect or Vector
are purpose-built for that; faucet runs pipelines to completion.

But if you want one fast, trustworthy binary to move data between the systems you
already run — without standing up a platform — that's exactly what we built.

## Try it

- Docs: <https://faucet-hq.github.io/faucet-stream/>
- Source: <https://github.com/faucet-hq/faucet-stream>
- Crates: <https://crates.io/crates/faucet-stream>
- Deep dive: [Exactly-once delivery without a broker](https://github.com/faucet-hq/faucet-stream/blob/main/docs/blog/exactly-once-without-a-broker.md)
- Coming from Singer/Meltano? [Migration guide](https://github.com/faucet-hq/faucet-stream/blob/main/docs/blog/migrating-from-meltano.md)

It's pre-1.0 and moving fast. Connector requests and contributions welcome —
there's a [contributing guide](https://github.com/faucet-hq/faucet-stream/blob/main/CONTRIBUTING.md)
and an authoring guide for building your own connector crates.

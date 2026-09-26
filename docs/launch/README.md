# Launch kit (WS-10)

Ready-to-use copy and a checklist for the 1.0 launch. The drafts here are
deliverables — **the actual posting/submitting is a manual step only the
maintainer can do** (each lives on an external site or needs your account).

## Pre-launch checklist (in-repo, mostly done)

- [x] Per-crate crates.io **keywords** tuned so each connector ranks for its
      system name (e.g. `faucet-source-kafka` → `kafka`). See each crate's
      `Cargo.toml`.
- [x] docs.rs renders the full API (WS-1); docs site live (WS-4); README hero +
      comparison (WS-2); architecture diagram + badges (WS-3).
- [ ] **Logo / social-preview banner** set in repo Settings → Social preview
      (WS-3 — needs the logo asset).
- [ ] Tag and publish a release (coordinate the posts below with the tag).

## Manual launch steps (maintainer)

### 1. awesome-rust PR

Fork [`rust-unofficial/awesome-rust`](https://github.com/rust-unofficial/awesome-rust),
add this line under a relevant section (e.g. *Database* → *ETL* or *Data
processing*), and open a PR:

```markdown
* [faucet-stream](https://github.com/faucet-hq/faucet-stream) — A fast, config-driven data-movement platform: <!--COUNT:connectors-->75<!--/COUNT--> source and sink connectors wired by a single `faucet` binary (YAML) or embedded as a Rust library; runs your existing Singer taps unchanged, with streaming, CDC, DLQ, and built-in metrics.
```

Also consider [`awesome-data-engineering`](https://github.com/igorbarinov/awesome-data-engineering).

### 2. This Week in Rust

Submit via the ["Send us a PR" form / issue](https://github.com/rust-lang/this-week-in-rust)
(Crate of the Week nominations + project updates). Suggested blurb:

> **faucet-stream** — a config-driven data-movement platform: <!--COUNT:connectors-->75<!--/COUNT--> connectors, a
> `faucet` CLI that runs pipelines from YAML, and an embeddable library, with
> streaming, Postgres CDC, dead-letter queues, and built-in Prometheus/tracing.

### 3. Show HN

**Title:** `Show HN: faucet-stream – move data between APIs, DBs, and warehouses with one Rust binary`

**Body:**

> Hi HN — I built faucet-stream, a config-driven data-movement platform for Rust.
> It's a single static binary that runs pipelines from a YAML file (no platform,
> no Python, no daemon), or an embeddable library with typed Source/Sink traits.
>
> <!--COUNT:connectors-->75<!--/COUNT--> connectors today (REST, GraphQL, gRPC, Kafka, Postgres incl. CDC, MySQL,
> SQLite, S3/GCS/Azure, Parquet/Delta/Iceberg, MongoDB, Redis, Elasticsearch,
> BigQuery, Snowflake, Databricks), with native streaming (bounded memory),
> incremental/resumable replication, dead-letter queues, retries, and built-in
> Prometheus metrics + tracing — no per-connector code.
>
> It also runs your existing Singer/Meltano taps unchanged (an experimental
> bridge), so you can adopt it incrementally instead of rewriting pipelines.
>
> It's the EL of ELT — I'm upfront in the README about where Meltano/Airbyte/dbt
> fit better. Pre-1.0 and moving fast; connector requests welcome.
>
> Docs: https://faucet-hq.github.io/faucet-stream/
> Repo: https://github.com/faucet-hq/faucet-stream

Post early in the US morning (Pacific) on a weekday; reply to comments quickly.

### 4. Reddit — r/rust

**Title:** `faucet-stream: a config-driven data-movement platform (<!--COUNT:connectors-->75<!--/COUNT--> connectors, streaming, CDC) — CLI or embeddable library`

Lead with the Rust angle: single binary, typed traits, every connector a Cargo
feature, performance-first design (connection reuse, multi-row inserts, bounded
streaming). Link the repo + docs. (Follow r/rust self-promotion etiquette —
frame it as "I built this, feedback welcome," not an ad.)

### 5. Reddit — r/dataengineering

**Title:** `Built an open-source, single-binary alternative to Meltano/Airbyte for moving data (Rust)`

Lead with the data-engineering pain: no platform to operate, version-controlled
YAML pipelines, runs on cron/CI, CDC + incremental + DLQ built in. Be honest
about connector-count vs. incumbents (it's in the README comparison). Link repo +
docs + the blog post.

### 6. Blog post

`blog-post.md` in this folder is a full draft ("Moving data in Rust shouldn't
require a platform"). Publish on your blog / dev.to, then link it from the Show
HN and Reddit threads.

## Notes

- Coordinate all posts with the release tag so first-time visitors land on a
  tagged, installable version.
- The repo's **social-preview image** (Settings → Social preview, 1280×640) is
  what renders when these links are shared — set it before posting (WS-3).

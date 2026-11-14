# Distribution channels runbook

The **maintainer-driven** channels for keeping faucet-stream in front of Rust and
data-engineering audiences on an ongoing basis. Nothing here can be automated in-repo — each
item posts to a third-party platform or needs your account. This file captures the *what* and
*where* so a future session/maintainer can execute without re-deriving it.

For the **per-release cadence**, see [`RELEASE_PUBLICITY.md`](./RELEASE_PUBLICITY.md). For the
one-time 1.0 launch copy, see [`README.md`](./README.md). Reuse the single elevator pitch from
the root [`README.md`](../../README.md) hero verbatim across all of these.

## B. Community platforms & social

- **X / Twitter, Bluesky, Mastodon** (fosstodon.org / hachyderm.io — the Rust community is
  very active there) — launch + per-release threads.
- **LinkedIn** — the data-engineering audience skews here; a maintainer post per milestone
  reaches more DEs than Reddit does.
- **Reddit** — recurring *value* posts (not just self-promo) in r/rust, r/dataengineering,
  r/programming, r/ETL. Follow each sub's self-promotion etiquette; consider an AMA once there
  are real users.
- **Hacker News** — re-`Show HN` on major versions; participate genuinely in adjacent threads
  (Airbyte/Fivetran/dbt/CDC) where a link is welcome.
- **Chat communities** — introduce it once, on-topic: Rust Discord/Zulip, the dbt Community
  and Locally Optimistic Slacks, data-engineering Discords, DataTalks.Club.

## C. Aggregators, directories & newsletters

### Directories (high-intent, one-time, permanent SEO)

- **AlternativeTo** — list as an alternative to Airbyte / Fivetran / Meltano / Singer. Highest
  intent: people there are actively shopping.
- **StackShare, LibHunt, Openbase** — tool directories.
- **Console.dev** — submit for review.

### awesome-lists — status

| List | Status | Notes |
|---|---|---|
| [awesome-streaming](https://github.com/manuzhang/awesome-streaming) | ✅ PR opened | `Data Pipeline` section, `[Rust]` tag |
| [awesome-dataengineering](https://github.com/igorbarinov/awesome-data-engineering) | ✅ PR opened | `Data Ingestion` section |
| [awesome-rust](https://github.com/rust-unofficial/awesome-rust) | ⏳ Deferred | Acceptance bar: **stars > 50 OR crates.io downloads > 2000**. Open the PR once `faucet-core` crosses 2,000 downloads or the repo crosses 50 stars. Highest-value list. |
| [awesome-etl](https://github.com/pawl/awesome-etl) | ⏳ Deferred | Self-submissions need real third-party traction or the PR is closed; no Rust section (ask before adding one). Revisit once there's adoption signal. |
| awesome-cdc | ❌ Skip | No maintained canonical list exists (the only candidate repo is dead). |

*(Deferral status is also tracked on issue [#314](https://github.com/faucet-hq/faucet-stream/issues/314).)*

### Newsletters

Rust Weekly, This Week in Rust (see [`RELEASE_PUBLICITY.md`](./RELEASE_PUBLICITY.md)), Data
Engineering Weekly, Seattle Data Guy, Pointer, Bytes, Software Lead Weekly, Console.dev, TLDR.

### Product Hunt

Schedule Tue–Thu; prep the hunter, gallery images, and a strong first comment in advance.

## D. Video & talks

- **CLI demo** — the asciinema/GIF (WS-3) embedded in the README, plus a **3–5 min YouTube
  "your first pipeline"** walkthrough and a longer CDC-mirror screencast. A recorded talk is
  evergreen distribution.
- **Talks / lightning talks** — submit to RustConf, Rust meetups (incl. virtual), Data
  Council, and local data-eng meetups.

## Content that compounds (evergreen)

- **Engineering deep-dives** — repurpose the design specs in `docs/superpowers/specs/` (e.g.
  exactly-once delivery, Postgres CDC in Rust, the DLQ design, the embedded-DuckDB SQL
  transform). Each doubles as SEO and proof of depth.
- **Migration guides** (highest-intent) — Singer/Meltano → faucet-stream, Airbyte →
  faucet-stream, "replacing a pile of Python cron scripts." The docs-site
  [comparison pages](https://faucet-hq.github.io/faucet-stream/comparison/) are
  the on-site anchor for these.
- **"Build X in 10 minutes" tutorials** — REST → BigQuery, Postgres CDC → Kafka, S3 →
  Parquet. Cross-post to dev.to / Hashnode / Medium / lobste.rs.

## Answer questions where they're asked

Build organic, searchable presence: Stack Overflow (`[rust]` + data-pipeline tags), Reddit,
and issues in related repos (Airbyte/CDC/dbt threads) where a link genuinely helps.

---

*Grounded in issue [#314](https://github.com/faucet-hq/faucet-stream/issues/314) (WS-11 of
the adoption epic). Keep the awesome-list status table current as PRs merge and traction lands.*

# Release publicity checklist

A **repeatable, per-release** playbook so publicity is routine, not heroic. Where the WS-10
[`README.md`](./README.md) in this folder covered the *one-time 1.0 launch*, this file is the
**recurring** counterpart: run it on every notable release.

**Everything here is a maintainer action** — each step posts to an external site or needs your
account. Keep the copy consistent across channels: consistency compounds recognition.

> **Single source of truth:** the canonical pitch and the connector count live in the root
> [`README.md`](../../README.md) hero (currently **<!--COUNT:connectors-->75<!--/COUNT--> connectors — <!--COUNT:sources-->42<!--/COUNT--> sources / <!--COUNT:sinks-->33<!--/COUNT--> sinks**).
> Copy from there; don't invent variants. If the count changed this release, update the
> templates below in the same PR.

## When to run

Trigger on the **merge of a `chore: release` PR** (release-plz) that cuts a user-meaningful
release — i.e. the umbrella `faucet-cli-vX.Y.Z` / repo-level `vX.Y.Z` tag, **not** every
per-crate patch bump. Rule of thumb:

- **Minor / notable release** (new connector, new runtime feature) → run the full checklist.
- **Patch / internal release** → skip, or at most a single social note if it fixes something
  user-visible.

## Pre-flight

- [ ] Release PR merged; crates published; the umbrella tag is live and installable
      (`cargo install faucet-cli` / the Homebrew tap / the installer script all resolve).
- [ ] Connector count in the templates below still matches reality
      (`ls crates/source/* crates/sink/*` → currently 33 + 25 = **58**).
- [ ] Pull the release highlights from the per-crate `CHANGELOG.md` files (release-plz writes
      these from `feat`/`fix`/`perf` commits). Pick the 2–4 user-facing headlines.
- [ ] The repo **social-preview image** (Settings → Social preview, 1280×640) is set so
      shared links unfurl with the banner.

## The checklist (in order)

Front-load the high-signal, low-effort channels; do the rest as time allows.

1. [ ] **Changelog-highlights post** — a short "what's new in vX.Y.Z" note (blog / dev.to),
       built from the CHANGELOG highlights. This is the artifact every other channel links to.
2. [ ] **This Week in Rust** — submit the release blurb (template below) via the
       [TWiR repo](https://github.com/rust-lang/this-week-in-rust).
3. [ ] **dev.to cross-post** — repost the changelog-highlights post (canonical URL back to
       your blog), tagged `#rust #dataengineering`.
4. [ ] **Social threads** — one post each to X, Bluesky, Mastodon (fosstodon / hachyderm),
       and **LinkedIn** (largest data-eng reach). Use the release-thread template.
5. [ ] **Reddit** — a *value* post (not pure self-promo) in r/rust and/or r/dataengineering
       when the release has a genuine story (new connector, big perf win). See
       [`DISTRIBUTION.md`](./DISTRIBUTION.md) for per-subreddit angles.
6. [ ] **Newsletter tips** — submit to Rust Weekly, Data Engineering Weekly, TLDR, etc.
       (full list in [`DISTRIBUTION.md`](./DISTRIBUTION.md)).
7. [ ] **Aggregator refresh (major releases only)** — bump the entry/description on
       AlternativeTo, StackShare, LibHunt, and the awesome-lists (status in
       [`DISTRIBUTION.md`](./DISTRIBUTION.md)).
8. [ ] **Recirculate an evergreen post** — the deep-dives and migration guides in
       [`../blog/`](../blog/) don't expire. Each release, re-share one (dev.to / lobste.rs /
       the relevant subreddit) alongside the changelog post to reach people who missed it.

## Copy templates

Reuse verbatim; swap `vX.Y.Z` and the highlights. All counts are **58**; all benchmark
figures come from [`BENCHMARKS.md`](../../BENCHMARKS.md) (quote **~16×** for a realistic
DB→DB move, **~96×** as the CSV→JSONL upper bound — never the ~96× alone).

### Elevator pitch (one-liner — use everywhere)

> **faucet-stream** — the fast, config-driven way to move data in Rust. A data-movement
> platform with <!--COUNT:connectors-->75<!--/COUNT--> source and sink connectors for ETL, CDC, and streaming, run from a YAML
> file by a single binary or embedded as a Rust library.

### Release thread (X / Bluesky / Mastodon / LinkedIn)

> faucet-stream vX.Y.Z is out 🚰
>
> The fast, config-driven way to move data in Rust — a single binary (or embeddable library)
> that runs ETL/CDC/streaming pipelines from a YAML file. No Python runtime, no platform.
>
> This release: <highlight 1>, <highlight 2>, <highlight 3>.
>
> Docs: https://faucet-hq.github.io/faucet-stream/
> Repo: https://github.com/faucet-hq/faucet-stream

### This Week in Rust

> **faucet-stream vX.Y.Z** — a config-driven data-movement platform for Rust: <!--COUNT:connectors-->75<!--/COUNT--> connectors,
> a `faucet` CLI that runs pipelines from YAML, and an embeddable library, with streaming,
> Postgres CDC, dead-letter queues, and built-in Prometheus/tracing. This release:
> <one-line highlight>.

### dev.to / blog changelog post skeleton

> # What's new in faucet-stream vX.Y.Z
>
> *(1-sentence recap of the elevator pitch + link to the repo.)*
>
> ## Highlights
> - **<highlight 1>** — why it matters.
> - **<highlight 2>** — why it matters.
>
> ## Upgrade
> `cargo install faucet-cli` / `brew upgrade faucet-cli`
>
> Full changelog: <link to the release>. Feedback and connector requests welcome.

### Reddit angles

- **r/rust** — lead with the Rust angle: single binary, typed `Source`/`Sink` traits, every
  connector a Cargo feature, performance-first design. Frame as "I shipped this, feedback
  welcome," not an ad.
- **r/dataengineering** — lead with the DE pain: no platform to operate, version-controlled
  YAML pipelines, runs on cron/CI, CDC + incremental + DLQ built in. Be honest about
  connector-count vs. incumbents (link the [comparison pages](https://faucet-hq.github.io/faucet-stream/comparison/index.html)).

## Metrics view (know which channels convert)

Check a few days after each release, so you stop guessing and double down on what works:

- **crates.io downloads** — `faucet-core` / `faucet-cli` trend (per-crate API; use a
  descriptive `User-Agent`).
- **GitHub** — stars over time (star-history) and the repo **Insights → Traffic** (views,
  clones, top referrers — referrers tell you which channel drove visits).
- **Docs site** — a privacy-friendly analytics tracker (Plausible / Fathom / GoatCounter) to
  see which pages (especially the `vs.` comparison pages) pull search traffic.

---

*Keep this file in sync with the root README pitch and BENCHMARKS.md. If the connector count
or the benchmark headline changes, update the templates above in the same PR.*

# faucet-source-feed

[![Crates.io](https://img.shields.io/crates/v/faucet-source-feed.svg)](https://crates.io/crates/faucet-source-feed)
[![Docs.rs](https://docs.rs/faucet-source-feed/badge.svg)](https://docs.rs/faucet-source-feed)
[![MSRV](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2Ffaucet-hq%2Ffaucet-stream%2Fmain%2FCargo.toml&query=%24.workspace.package%5B%27rust-version%27%5D&label=MSRV&color=blue)](../../../Cargo.toml)
[![License](https://img.shields.io/crates/l/faucet-source-feed.svg)](https://github.com/faucet-hq/faucet-stream#license)

The **feed** source reads the events a durable feed store holds for one
delivery, in order, and moves the delivery's position only after the pipeline
wrote them. Part of the [faucet-stream](https://github.com/faucet-hq/faucet-stream)
ecosystem.

A *feed* is an ordered, durable log of events that senders push in. A
*delivery* is one reader of that feed with its own position (its *cursor*, the
next sequence number to read). The store is
[`faucet-common-feed`](../../common/feed) (SQLite or PostgreSQL).

## Feature highlights

- **Nothing is skipped.** The cursor moves when the pipeline polls the source
  again after a page, which it does only once the page is written, flushed and
  its bookmark stored. A failed write leaves the cursor where it was.
- **One reader per range.** A read holds a range claim in the store, renewed
  while it runs. A second reader of the same delivery fails at once; an
  earlier attempt of the same run is fenced out when a newer one attaches.
- **Exactly once with an idempotent sink.** With `delivery: exactly_once` and
  durable state, a crash between the sink's commit and the cursor's commit
  never writes the page twice: the next run proves the page from the sink's
  bookmark and moves the cursor past it without reading it again.
- **A restored store is held, never guessed at.** A bookmark that names an
  event the store does not hold stops the run with a clear error instead of
  skipping or re-reading.
- **One run, one page.** A run reads its range as a single page (one sink
  commit, one cursor move). `page_events` splits it for row sinks.
- **Exact values.** JSON numbers are parsed exactly; bytes are base64;
  timestamps are RFC 3339 with an offset.

## Installation

```bash
# As a library:
cargo add faucet-source-feed
cargo add tokio --features full

# In the CLI:
cargo install faucet-cli --features source-feed
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features source-feed
```

## Quick start

```yaml
version: 1
name: orders-to-lake
pipeline:
  source:
    type: feed
    config:
      feed: orders                 # the feed's uid in the store
      delivery: lake               # the delivery's id within the feed
      store:
        type: sqlite
        config:
          path: ./feeds.db
  sink:
    type: file
    config:
      path: ./orders.jsonl
      if_exists: append
  state:
    type: file
    config:
      path: ./state
```

`faucet run` reads what the feed holds past the delivery's cursor (up to
`max_events` / `max_bytes`), writes it, moves the cursor and exits. Run it
again to drain more. The feed and the delivery must exist in the store; a
missing one is an error that names it.

A file sink that replaces its output on every run is refused for this source
(the events would be gone from the feed's point of view and from the file):
use `if_exists: append`, or a per-run file name.

## Configuration reference

Every field is optional in the schema, so a delivery template whose source is
just `type: feed` validates.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `feed` | string | | The feed's uid in the store. Required under `faucet run`. |
| `delivery` | string | | The delivery's id within the feed. Required under `faucet run`. |
| `store` | `{type, config}` | | The store to open. Required under `faucet run`. |
| `store.type: sqlite` | `config.path` | | Path of the store's database file. It must exist. |
| `store.type: postgres` | `config.url` | | Connection URL of the database that holds the store. Use a `${env:…}` or secret reference for the password. |
| `page_events` | integer | one page per run | Split a run into pages of this many events; each page is written and committed on its own. |
| `include_metadata` | bool | `true` | Add `_feed: {seq, id, received_at, headers}` to every record. |
| `claim_ttl_secs` | integer | `60` under `faucet run`, `300` under `faucet serve` | How long the range claim lasts without a renewal. The source renews it every tenth of this so a `faucet run` that died leaves its claim for at most a minute. |
| `max_events` | integer | `10000` | Most events one `faucet run` drain reads. |
| `max_bytes` | integer | `67108864` | Most payload bytes one `faucet run` drain reads; a single larger event is still read. |

### Two ways it runs

| | Under `faucet run` (or a library caller) | Under a server's delivery run |
|---|---|---|
| Feed, delivery, store | from the config (all three required) | from the run; setting `feed`, `delivery` or `store` in the config is refused |
| Range | the source claims `[cursor, bound)` itself, owner `cli:<host>:<pid>` | the range the server claimed for the run; the source attaches to that claim |
| When it ends | the source releases its claim | the source leaves the claim to the server, which releases it when the run is over |
| Sealed payloads | an error (there is no key) | opened through the run's opener |

A server hands the source its store, claim and payload opener through a
`faucet_common_feed::FeedExecution` scoped over the run's task. They never
appear in the run's config.

## Records

Each event becomes one record, by its stored content type:

| Content type | Record |
|---|---|
| `application/json`, `*+json`: an object | the object |
| `application/json`, `*+json`: any other value | `{"value": <the value>}` |
| `application/x-www-form-urlencoded` | an object of string values |
| `text/*` | `{"value": "<the text>"}` |
| anything else | `{"value": "<base64>"}` |

With `include_metadata` (the default) the record also carries:

```json
{"_feed": {"seq": 42, "id": "0193…", "received_at": "2026-11-16T10:20:30.123456Z", "headers": {"x-request-id": "…"}}}
```

`seq` is the event's sequence number in the feed, `id` its time-ordered id,
`headers` the request headers the feed kept. An event whose payload is not
what its content type says (invalid JSON, text that is not UTF-8) fails the
run with a typed error; nothing is skipped.

## Resume and state

Every page carries this bookmark:

```json
{"feed": "orders", "delivery": "lake", "epoch": 0, "next": 43, "id": "0193…"}
```

`next` is the cursor once the page is delivered and `id` is the id of the
page's last event. The delivery's cursor in the store is the authority; the
bookmark exists to close the gap between the sink's commit and the cursor's
commit. On the first read of a run, after the claim is held:

| The stored bookmark | What happens |
|---|---|
| names another feed or delivery | The run fails: two pipelines share one state key (same pipeline `name` and row). Give the pipeline its own `name`. |
| has another epoch | Ignored: the cursor was moved by a replay, skip or recover. |
| `next` equals the cursor | Nothing; the normal case. |
| `next` is behind the cursor | Ignored. |
| `next` is ahead of the cursor | The sink committed a page whose cursor commit was lost. The source checks that event `next - 1` is in the store with that `id`, then moves the cursor to `next` without reading the page again. If the event is missing or has another id, the store was restored from an older copy: the delivery is **held** and the run fails, with nothing read or skipped. |

## Delivery guarantee

| Sink | Guarantee |
|---|---|
| Idempotent sink, `delivery: exactly_once`, durable `state:` | Exactly once, while the claim holds. |
| Keyed upsert (`write_mode: upsert` + `key`) | Effectively once. |
| Anything else | At least once: a crash between the sink's commit and the cursor's commit writes that run's page again. |

An exactly-once pipeline has no dead-letter queue, so a record the sink
rejects stops the delivery. Where bad records are expected, use a keyed upsert
with a DLQ and a `state:` block.

## Errors

Read failures are `FaucetError::Custom` holding a `FeedSourceError`
(`FeedSourceError::of(&err)`): `SharedState`, `Held`, `ClaimBusy`,
`ClaimLost`, `Sealed`, `Undecodable`, `Missing`, `BufferedFetch`. Store
failures are `faucet_common_feed::FeedError`.

The buffered fetch (`fetch_all` and friends) is refused: it would move the
cursor past events nobody wrote. Only a pipeline's streaming read consumes.

## Library usage

```rust,no_run
use faucet_core::{Pipeline, Sink};
use faucet_source_feed::{FeedSource, FeedSourceConfig, FeedStoreSpec};

# async fn run(sink: impl Sink) -> Result<(), faucet_core::FaucetError> {
let source = FeedSource::new(FeedSourceConfig::standalone(
    "orders",
    "lake",
    FeedStoreSpec::sqlite("./feeds.db"),
));
let result = Pipeline::new(&source, &sink).run().await?;
println!("delivered {} events", result.records_written);
# Ok(())
# }
```

`FeedSource::with_store(config, store)` reads from a store you already hold.

## Lineage dataset URI

`feed://<feed>`: the feed's name (`tenant/name` for a tenant feed) under a
server, its uid under `faucet run`.

## Feature flags

| Feature | Default | What it adds |
|---|---|---|
| `sqlite` | yes | Open a SQLite store named in `store:`. |
| `postgres` | yes | Open a PostgreSQL store named in `store:`. |

## Capabilities

The crate exports `DESCRIPTOR` (RFC 0013), the same for every config:
`exactly_once` (a resumed read replays exactly what follows the bookmark),
`consumes_destructively` (a read moves the delivery's cursor) and
`read_side_effects` (a read takes a range claim). So dry runs, `--limit`,
preview, `plan --live`, verify, overwrite, graph topologies and discovery
(`fan_out:`) rows refuse it. No discovery, sharding, columnar or native path.
`faucet list --json` prints the declaration.

## How it is built

The source runs on the family-neutral source driver (RFC 0014): `FeedDriver`
implements `faucet_core::driver::SourceDriver` and `FeedSource` wraps it in a
`DriverSource`.

- `read` attaches to the run's claim (or takes its own), applies the resume
  rules, reads the range and yields it as pages that each carry a bookmark.
  Being polled again means the page was written, flushed and bookmarked, so
  that poll commits the cursor (a compare-and-set fenced by the claim's
  holder token).
- A task renews the claim while the sink writes; a claim that is no longer
  held stops the read before it commits.
- `resume_from` only stores the bookmark; the first read checks it.
- `fetch` is refused.
- `set_cancel` takes the run's cancel: the source commits what was written
  and ends at the next poll.
- `check` looks the feed and the delivery up in the store; it reads and
  claims nothing.

Every store call goes through the run's `CallContext`.

## License

MIT OR Apache-2.0

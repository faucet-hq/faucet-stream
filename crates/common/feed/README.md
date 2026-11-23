# faucet-common-feed

The durable event store behind [faucet-stream](https://crates.io/crates/faucet-stream)
feeds: senders push events in, deliveries read them out in order. This crate
holds the store contract, its data types, two backends and the conformance
suite every backend passes. It has no HTTP surface and no connector in it.

| Piece | What it is |
|---|---|
| `FeedStore` | The contract: append, read, claim, commit, retention. Object-safe; every method takes a `&CallContext`. |
| `SqliteFeedStore` (feature `sqlite`) | One SQLite file (WAL, `synchronous = FULL`). One server per file. |
| `PostgresFeedStore` (feature `postgres`) | PostgreSQL. Safe for several servers; always its own connection pool. |
| `conformance` (feature `conformance`) | The suite: implement `Backend` for a store and run `CHECKS`. Also `StalledStore`, a store that never answers. |
| `FeedExecution`, `scope`, `current`, `inherit` | The handle a host scopes over a delivery run's task and the `feed` source reads through. |

## Install

```toml
[dependencies]
faucet-common-feed = { version = "1", features = ["sqlite"] }   # or "postgres"
```

## What the store guarantees

- **Durable before it answers.** `append` returns after its transaction
  committed. A request is stored whole or not at all.
- **Gap-free order.** Sequence numbers start at 1 and are assigned inside the
  append transaction under one lock per feed (`pg_advisory_xact_lock` on
  PostgreSQL, `BEGIN IMMEDIATE` on SQLite). The watermark is the committed
  head, so a reader that stops at it can never see event N+1 before N.
- **Dedupe in the same transaction.** An event whose `dedupe_key` was stored
  inside the feed's dedupe window is not stored again; the sender gets the
  first event's id. A key past its expiry is replaced, never honoured.
- **Limits refuse, never drop.** The tenant's stored-bytes quota and the
  feed's undelivered limits (events, bytes, age) are checked in the append
  transaction; a crossing append fails with `FeedError::Full` and stores
  nothing. The undelivered limits apply once the feed has a delivery.
- **A cursor is the next sequence number to read.** It moves only through
  `commit` (a compare-and-set fenced by the claim's holder token; it may pass
  the claim's end, never the watermark, and never goes back) or `move_cursor`
  (replay, skip or recover: refused while a claim is live, bumps the
  delivery's epoch, and a skip always records a gap).
- **One reader per range.** `claim` is a compare-and-set on the cursor with a
  TTL; of several concurrent claimers one wins. `attach` rotates the holder
  token, which fences out an earlier reader. `renew_claim` extends a claim by
  id without rotating anything. An expired claim is gone even if nobody
  replaced it.
- **The store's clock decides.** Claim expiry, dedupe expiry and retention age
  are computed in SQL, never from the caller's clock.
- **Retention.** `trim` removes events that are older than the feed's
  `max_age` and below every delivery's cursor, in batches; a feed with no
  delivery is trimmed by age alone. Sequence numbers are never reused.
- **An identity.** `store_id()` is a random number the store writes once,
  when its schema is first created (inside the same locked transaction, so
  two first opens agree), and reads back on every open: a restored backup
  keeps it, a database recreated empty gets a new one. A caller records it to
  tell whether a store name now points at another database. The SQL
  backends have one; the trait default is `None`.
- **Quota figures.** `tenant_bytes(tenant)` is what every feed of a tenant
  holds in the store, delivered events kept for replay included: the figure
  the tenant's stored-bytes quota is checked against.

## Example

```rust,no_run
use faucet_common_feed::{
    BatchLimits, ClaimOwner, DeliveryUid, FeedStore, FeedUid, NewEvent, SqliteFeedStore,
    SqliteOptions, Start, StoreLimits,
};
use faucet_core::io::CallContext;
use std::time::Duration;

# async fn run() -> Result<(), faucet_common_feed::FeedError> {
let store = SqliteFeedStore::open("feeds.db", &SqliteOptions::new()).await?;
let calls = CallContext::new();

let feed = FeedUid::generate();
store.ensure_feed(&calls, &feed, None, &StoreLimits::new()).await?;
let ack = store
    .append(&calls, &feed, vec![NewEvent::new("application/json", br#"{"id":1}"#.to_vec())])
    .await?;
println!("stored as {:?}", ack.first_seq);

let delivery = DeliveryUid::generate(feed.clone());
let state = store.ensure_delivery(&calls, &delivery, Start::Earliest).await?;
let to = store.bound(&calls, &feed, state.cursor, BatchLimits::default()).await?;
let owner = ClaimOwner::new("cli:host:1")?;
if let Some(claim) = store
    .claim(&calls, &delivery, state.cursor, to, &owner, Duration::from_secs(300))
    .await?
{
    let holder = store.attach(&calls, &claim.id).await?;
    let batch = store.read(&calls, &feed, claim.from, claim.to).await?;
    // ... write `batch.events` to the destination ...
    store.commit(&calls, &holder, claim.from, batch.to).await?;
    store.release(&calls, &claim.id).await?;
}
# Ok(())
# }
```

## Backends

| | SQLite | PostgreSQL |
|---|---|---|
| Several servers on one store | no (`StoreCaps::cluster_safe` is `false`) | yes |
| Append lock | `BEGIN IMMEDIATE` (the file's one writer) | `pg_advisory_xact_lock` per feed |
| A stuck appender | a waiter gives up after `busy_timeout` (5 s) | the database ends it: `lock_timeout` (5 s), `statement_timeout` (30 s), `idle_in_transaction_session_timeout` (30 s) on every session |
| File / pool | created owner-only (`0600`) | its own pool (`PostgresOptions::max_connections`, default 10) |

Both backends create their tables on first open (`faucet_feed_events`,
`faucet_feed_heads`, `faucet_feed_deliveries`, `faucet_feed_dedupe`,
`faucet_feed_gaps`, `faucet_feed_meta`). `faucet_feed_meta` records the format
version (`SCHEMA_VERSION`); a store written by a newer release is refused with
`FeedError::NewerSchema`. The events table carries a reserved `lane` column
(always `0`) in its primary key `(feed, lane, seq)`.

The tables are frozen per format version in `tests/fixtures/<backend>-store-v1.json`;
a test fails when they change, so a new shape means a new version, a migration
and a new fixture.

## The run-scoped handle

A host that runs a delivery gives the `feed` source its store, its claim and
its sealed-payload opener through a `FeedExecution` scoped over the run's
task. Nothing of it is in the run's config and nothing is process-global, so
several hosts in one process do not cross.

```rust,ignore
use faucet_common_feed::{FeedBinding, FeedExecution, scope};

let binding = FeedBinding::new(delivery, epoch, claim.id, claim.from, claim.to, "default");
let execution = FeedExecution::new(binding, store)   // Arc<dyn FeedStore>
    .opener(opener)                                  // optional: opens sealed payloads
    .dataset("acme/orders");                         // optional: the name lineage reports
let result = scope(execution, pipeline.run()).await;
```

- `current()` returns the handle inside the scope, `None` outside it.
- A task spawned from inside the scope sees it only when its future is
  wrapped with `inherit(..)`.
- `FeedBinding` holds names and ids only (feed, delivery, epoch, claim, range,
  store name) and is `Serialize` / `Deserialize`, so it can be stored on a run
  record.
- A `PayloadOpener` is `Fn(&[u8], &str) -> Result<Vec<u8>, OpenError>`; the
  second argument is `sealing_context(feed, tenant)`, i.e.
  `feed:{uid}:tenant:{tenant|-}:event`.

## Errors

`FeedError` is typed: `Full` (a limit), `ClaimLost`, `ClaimLive`,
`CursorConflict`, `PastWatermark`, `OutOfRange`, `RangeUnavailable`,
`FeedNotFound`, `DeliveryNotFound`, `TenantMismatch`, `NewerSchema`,
`Unavailable` (busy, timed out, unreachable or out of space: retry later),
`Backend`, `Corrupt`, `Invalid`, and `Call` (the `CallContext` cancelled the
call or it ran out of time). `FeedError::is_transient` says whether the same
request may succeed later. It converts into `faucet_core::FaucetError`
(`Custom`, so it can be downcast back).

## Testing a backend

```rust,ignore
use faucet_common_feed::conformance::{Backend, CHECKS};

#[tokio::test(flavor = "multi_thread")]
async fn my_store_conforms() {
    let backend = MyBackend::new();           // impl Backend: open + the format version
    for check in CHECKS {
        check.run(&backend).await;
    }
}
```

The suite covers: durable, ordered, gap-free appends; concurrent appenders
with a reader following the watermark; dedupe inside the window, across an
expired row and under concurrent retries; quota and undelivered-limit
refusals; claim exclusivity; claim expiry; holder fencing; commit bounds;
cursor moves, epochs and gaps; trim against age, cursors and claimed ranges;
`bound`, `seq_at` and `heads`; delivery start positions, delete and purge;
restart durability; the newer-format refusal; the maintenance claim.

## License

MIT OR Apache-2.0

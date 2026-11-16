---
title: "Feeds"
description: "A feed is a durable event log inside faucet serve. Senders push events into it over HTTP and get an answer once the events are stored; deliveries run pipelines over it, each with its own position."
---

A **feed** is a durable event log inside `faucet serve`. Senders push events
into it over HTTP and are answered only after the events are stored. A
**delivery** runs a pipeline over the feed with its own position, so the same
events can go to several destinations, each at its own pace, and can be
delivered again later.

Use a feed when a system pushes data at you and you cannot ask it to send the
data again.

| | A feed (this page) | The [`webhook` source](/faucet-stream/reference/choosing/#live-feeds-websocket-vs-webhook-vs-kafkaredis) | A [webhook trigger](/faucet-stream/cookbook/triggers/) |
|---|---|---|---|
| What a request does | Stores its events in the feed | Hands its payload to a pipeline that is running right now | Starts a run |
| Listens | For as long as the server runs | Only while a run with that source is open | For as long as the server runs |
| The sender is answered | After the events are stored (`202`) | After the pipeline wrote them (`ack: on_commit`) | When the run is queued |
| Destination down | Events wait in the feed; the sender is not affected | The sender gets an error and must retry | The run fails; the request carried no data to keep |
| Sender credential | A feed token, an HMAC signature, or both | The source's own token or signature | A serve bearer token |
| Several destinations, replay | Yes: one position per delivery, replay within the retention window | No | No |

Feeds need a server built with the `feeds` feature (the `full` image has it)
and a run history that survives a restart: with `--history memory` the server
refuses a feeds file and refuses to create a feed.

## A feed in five minutes

Start a server. The default history is a SQLite file, and the feed's events go
into `feeds.db` beside it.

```bash
export TOKEN=dev-admin-token
FAUCET_SERVE_AUTH_TOKEN=$TOKEN faucet serve --listen 127.0.0.1:8080 --history sqlite:./history.db
```

A delivery made through the API runs a registered
[template](/faucet-stream/cookbook/templates/) whose source is `type: feed`.
The server tells the source which feed and which events to read, so the source
takes no settings here. Save this as `orders-archive.yaml`:

```yaml
version: 1
name: orders_archive
pipeline:
  source:
    type: feed
  sink:
    type: file
    config:
      path: ./out/orders.jsonl
      # Append: a run moves the delivery past what it wrote, so the next run
      # must not replace this one's output.
      if_exists: append
  # With a bookmark, a run that crashed between the write and the position
  # move recognises the page the destination already holds.
  state:
    type: file
    config:
      path: ./state
# A template is launched only once its tests pass; this bundle checks that the
# template still loads with its default params.
tests:
  suite:
    auto: { defaults_baseline: true }
```

Register and launch it, then create the feed with one delivery. Registering
runs the template's `tests:` block, and `launch: true` is refused for a
template without one:

```bash
jq -Rs '{id: "orders-archive", config: ., config_format: "yaml", launch: true}' orders-archive.yaml \
  | curl -sX POST localhost:8080/v1/templates -H "Authorization: Bearer $TOKEN" \
      -H 'content-type: application/json' --data-binary @-

curl -sX POST localhost:8080/v1/feeds -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{
    "name": "orders",
    "ingest": { "auth": { "tokens": "managed" } },
    "deliveries": [
      { "name": "archive",
        "template": { "id": "orders-archive" },
        "flush": { "max_age": "5s" } }
    ]
  }'
```

Issue a token for senders. Its text is in the answer once and is never stored:

```bash
curl -sX POST localhost:8080/v1/feeds/orders/tokens -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"name": "shop"}'
# → 201 {"id":"…","name":"shop","prefix":"fdt_…","token":"fdt_…",…}
export FEED_TOKEN=fdt_…
```

Send an event as a sender would. The serve token is not accepted on this
route; the feed token is:

```bash
curl -sX POST localhost:8080/v1/feeds/orders/events \
  -H "Authorization: Bearer $FEED_TOKEN" -H 'content-type: application/json' \
  -d '{"order_id": 1001, "total": "49.90"}'
# → 202 {"ids":["…"],"first_seq":1,"last_seq":1}
```

`202` means the event is stored. Within a few seconds the delivery's flush
policy is met, a run starts, and the row lands:

```bash
cat out/orders.jsonl
# {"order_id":1001,"total":"49.90","_feed":{"seq":1,"id":"…","received_at":"…","headers":{…}}}
curl -s localhost:8080/v1/feeds/orders -H "Authorization: Bearer $TOKEN" | jq '.deliveries[0] | {status, position, lag}'
```

Each record carries a `_feed` object with the event's sequence number, id,
arrival time and kept headers. Set `include_metadata: false` on the source to
leave it out.

## Where feeds are defined

- **Through the API.** `POST /v1/feeds` (or `faucet feed create`). These
  feeds are stored in the run history, so every replica sharing that history
  sees them within a few seconds. Their deliveries name a registered
  template.
- **In a file.** `faucet serve --feeds feeds.yaml` loads feeds from a file
  (`faucet schema feeds` prints its schema). They are read-only at runtime:
  edits and deletes happen in the file, followed by a restart. A file
  delivery may name a `pipeline:` file on the server instead of a template,
  relative to the feeds file. The file is the operator's own, so `${env:…}`,
  `${file:…}` and secret-manager references in it are resolved at startup.

A name in the file wins over a stored feed with the same name, and the API
refuses to create a feed the file already defines.

```yaml
version: 1
feeds:
  - name: orders
    description: Order events from the shop
    ingest:
      auth:
        tokens: managed                   # write-only tokens, stored hashed
        signature:                        # and an HMAC over the body
          header: X-Signature
          secret: "${env:ORDERS_HMAC_SECRET}"
          prefix: "sha256="
      accept: [application/json, application/x-ndjson]
      max_event_bytes: 256KiB
      dedupe: { header: X-Event-Id, window: 24h }   # a retried event is stored once
      rate_limit: { events_per_sec: 200, burst: 1000 }
    retention:
      max_age: 7d                         # how long delivered events stay replayable
      undelivered: { max_age: 30d, max_bytes: 5GiB }   # past these the feed refuses new events
      warn_at: [0.5, 0.8]
    deliveries:
      - name: archive
        pipeline: orders_to_jsonl.yaml    # relative to this file; its source is `type: feed`
        flush: { max_events: 5000, max_bytes: 64MiB, max_age: 60s, min_interval: 10s }
        max_batch: { events: 10000, bytes: 64MiB }
        start: earliest
        retries: 5
```

The full example is
[`cli/examples/feeds/feeds.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/feeds/feeds.yaml)
with its pipeline
[`orders_to_jsonl.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/feeds/orders_to_jsonl.yaml).
Every field is in the [feeds reference](/faucet-stream/reference/feeds/).

Nothing feed-related runs on a server until a feed exists: the store, the
scheduler and the definitions refresh start when the server first sees one.

## Sending events

Senders `POST` to `/v1/feeds/{name}/events` (a tenant's feed:
`/v1/tenants/{tenant}/feeds/{name}/events`).

| Body | Stored as |
|---|---|
| `application/json`: one value | one event |
| `application/json`: an array | one event per element |
| `application/x-ndjson`: lines | one event per line |
| form data, text, anything else the feed accepts | one event |

JSON must parse and text must be UTF-8, or the request is refused with `400`,
so a delivery can always decode what the feed holds. A request is all or
nothing: either every event in it is stored or none is. A body may be sent
with `Content-Encoding: gzip`; every size limit applies to the expanded bytes.

Senders should retry on `408`, `429` and `503`, waiting the `Retry-After`
seconds the answer gives. The [reference](/faucet-stream/reference/feeds/#status-codes)
lists every status.

### Who may send

A feed must have at least one credential.

- **Tokens** (`tokens: managed`). Issue them per sender with
  `POST /v1/feeds/{name}/tokens`; only the SHA-256 hash is stored and the text
  is shown once. A token writes to its own feed and nothing else, can carry an
  expiry, and stops working within a few seconds of being revoked. Senders
  present it as `Authorization: Bearer <token>`.
- **Signature** (`signature:`). The sender signs the body with a shared
  secret (HMAC SHA-256 or SHA-512, hex or base64, an optional prefix, and an
  optional signed timestamp header that rejects old requests). For a gzip
  body the signature covers the bytes as sent.
  Set `timestamp_header` as well: a signed request that is sent again inside
  its tolerance is then stored once, on any server. Without it, a captured
  request can be replayed unless the feed has a `dedupe:` key.
- **Both.** With both set, a request needs a valid token and a valid
  signature.
- **Allowed addresses** (`allowed_addresses`). Addresses or CIDR ranges that
  may push. Behind a proxy every sender shares the proxy's address, so use
  this only where the server sees the real peer.

An unknown feed, a bad credential and a sender address the feed does not allow
get the same `401`, so feed names cannot be found by guessing. A serve bearer
token is never accepted on the ingest routes.

**URL tokens are off by default.** Some senders can only be given a URL. With
`url_token: true` the feed also accepts its token as `?token=…`. It is off by
default because a URL is logged by proxies, kept in browser history and shown
in referrers in ways a header is not. The server removes the parameter before
it logs, traces or counts the request, but it cannot do that for the systems
in front of it.

### Duplicates

Without a dedupe key, ingest is at least once: a sender that times out after
the server stored its events and retries stores them twice. Give the feed a
key to make a retry safe:

```yaml
ingest:
  dedupe: { header: X-Event-Id, window: 24h }
  # or a field of each JSON event:
  # dedupe: { body_path: event.id, window: 24h }
```

An event whose key was already seen within `window` is not stored again. The
sender gets `202` with `duplicate: true` (or `409` with
`on_duplicate: conflict`). In a request of several events the header key is
suffixed with each event's place in the request.

### Rate limits

`ingest.rate_limit` caps the events a second one feed accepts
(`events_per_sec`, with a `burst`). Past it the sender gets `429` with
`Retry-After`. The limit is kept per server, so a cluster of three replicas
accepts up to three times the figure.

## Deliveries

A delivery is a pipeline that reads the feed from its own position (the next
event it will read). The scheduler starts a run when the delivery's **flush
policy** is met:

- `flush.max_events` undelivered events (default 5000), or
- `flush.max_bytes` of them (default 64 MiB), or
- the oldest undelivered event is `flush.max_age` old (default 60s),

but never more often than `flush.min_interval` (default 10s).

**One run is one page, one sink commit and one position move.** A run reads at
most `max_batch` (default 10,000 events or 64 MiB), writes it as a single
page, and the delivery's position moves only after the pipeline wrote, flushed
and bookmarked that page. A larger backlog drains in consecutive runs. A run
holds its page in memory, so size `max_batch.bytes` with that in mind.

Give a delivery's pipeline a `state:` block. It lets a run recognise a page the
destination already holds after a crash, and it is required for the
exactly-once guarantee below.

A delivery's pipeline must be one root row whose source is `type: feed`: no
matrix, `partition`, `shard`, `fan_out` or node graph, and the source's own
`feed`, `delivery` and `store` settings stay unset (the server supplies them).
A file sink that replaces its output on every run is refused for it; use
`if_exists: append` or a per-run file name.

### When runs fail

- **The batch adapts.** After a run that fails without moving the position,
  the next run reads half as many events, down to one. After a success the
  size doubles back toward `max_batch`. So a destination with a request size
  ceiling, a run budget or a transform that multiplies records shrinks the
  batch until it fits instead of failing the same way forever.
- **`failing`.** After three failed runs in a row the delivery is reported as
  `failing`. It keeps retrying, with a back-off that starts at 5 seconds and
  doubles to a minute.
- **`stalled`.** When runs of a **single event** have failed `retries` times
  (default 5), the delivery stalls: no more runs start, and the feed keeps
  everything it has not delivered. A stall always names one event.

A refusal that passes with time (the run queue is full, a tenant's run limit,
the server is draining) backs off and is not counted. A refusal a retry cannot
fix (a policy refusal, a template or version that is gone, a suspended tenant)
counts like a failed run, so it surfaces instead of looping.

A stall clears when an operator acts:

| Action | What it does |
|---|---|
| Fix the destination, then **resume** | Clears the stall and the failure counts; the same event is tried again. |
| **Skip** the event | Moves the position past it and records a gap (below). |
| **Replay** | Moves the position back and clears the stall. |

### Pause, resume, deliver now

```bash
curl -sX POST localhost:8080/v1/feeds/orders/deliveries/archive/pause \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{"reason": "warehouse maintenance"}'
curl -sX POST localhost:8080/v1/feeds/orders/deliveries/archive/resume -H "Authorization: Bearer $TOKEN"
curl -sX POST localhost:8080/v1/feeds/orders/deliveries/archive/deliver-now -H "Authorization: Bearer $TOKEN"
```

A paused delivery keeps its position and starts no runs; the feed keeps
accepting events. **Resume** clears a pause, a stall and the failure counts.
**Deliver now** starts a run for whatever is undelivered without waiting for
the flush policy. A delivery with `paused: true` in its definition is resumed
by changing the definition.

Cancelling a delivery's run pauses the delivery, so the run is not submitted
again at once.

### Replay

Delivered events stay in the feed for `retention.max_age` (default 7 days). A
replay moves a delivery back so they are delivered again:

```bash
curl -sX POST localhost:8080/v1/feeds/orders/deliveries/archive/replay \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"time": "2026-11-15T00:00:00Z"}'      # or {"seq": 1200}
```

A replay needs the admin role, is audited, and is refused while the delivery
has a run in flight (pause it and wait). It delivers every event from that
point on again, so the destination should be a keyed upsert or otherwise
tolerate rows it already has.

### Skip and recover

**Skip** is the way past an event that will never be delivered:

```bash
curl -sX POST localhost:8080/v1/feeds/orders/deliveries/archive/skip \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"reason": "malformed order, fixed upstream in ticket 4411"}'
```

It moves the position forward (by one event unless `to` says further), needs
the admin role and a reason, and records a **gap**: the skipped range, the
reason, who did it and when. Gaps are listed by `GET /v1/feeds/{name}` and by
`faucet feed show`. Nothing is ever skipped on its own.

A delivery is **held** when its bookmark and the feed store disagree, which
happens when the store was restored from an older backup: the bookmark names
an event the store no longer has, or has under another id. Nothing is
delivered or skipped while it is held. **Recover** puts the position back on a
valid one, records a gap and lets runs start again:

```bash
curl -sX POST localhost:8080/v1/feeds/orders/deliveries/archive/recover -H "Authorization: Bearer $TOKEN"
```

Resume does not clear a hold; only recover does.

## Retention and a full feed

A feed keeps two promises, and neither drops data silently:

- **Delivered events** stay replayable for `retention.max_age`, then are
  trimmed. Trimming never reaches an event some delivery has not read. A feed
  with no delivery keeps events for `max_age` only.
- **Undelivered events** are kept until they are delivered. If they reach
  `retention.undelivered.max_age` (default 30 days) or `max_bytes` (default
  50 GiB), the feed is **full** and **refuses new events** with `503` and
  `Retry-After` instead of dropping old ones. Senders retry; the feed accepts
  again once a delivery has caught up.
  Only undelivered bytes count toward the limit: delivered events kept for
  replay do not fill a feed. The API reports both (`undelivered_bytes`,
  `retained_bytes`).

So a failing delivery stalls and a full feed refuses. There is no option that
drops the oldest events or skips a failing one automatically.

`retention.warn_at` (default `[0.5, 0.8]`) are the fractions of the
undelivered limits at which a `feed_near_full`
[notification](/faucet-stream/cookbook/notifications/) is sent, once per
crossing. A stall sends `feed_stalled`. Both go through the `notifications:`
block of each delivery's pipeline and, for a tenant's feed, the tenant's.

## Delivery guarantees

Ingest and delivery are separate promises.

**Ingest.** A `202` means the events are stored. Without a dedupe key a
sender's retry can store an event twice; with one, a retry within the window
is stored once.

**Delivery** depends on the destination:

| Destination | Guarantee |
|---|---|
| A sink with idempotent writes, `delivery: exactly_once` and a durable `state:` | Exactly once, while the run's claim on its range holds. |
| A keyed upsert (`write_mode: upsert` with a `key`) | Effectively once: a repeated page rewrites the same rows. |
| Anything else | At least once: a crash between the sink's commit and the position move writes that run's page again. |

Two limits on the first row. An exactly-once pipeline has no dead-letter
queue, so one record the sink rejects stalls the delivery; where bad records
are expected, use a keyed upsert with a [DLQ](/faucet-stream/cookbook/dlq/)
and a `state:` block. A delivery's dead-lettered records are replayed with
`POST /v1/dlq/replay` and the delivery's own config: the replay reads the DLQ
in place of the feed, so the `type: feed` source is accepted there (and in
`POST /v1/plan`), while running that config directly is still refused. And a run that loses its claim (the feed store was
unreachable for the claim's whole five minutes) cannot move the position, but
a successor may write the same page: the claim protects the position, not the
destination.

## Sealing

`seal: true` stores event payloads encrypted with the server's
[vault key](/faucet-stream/cookbook/embedded-integrations/) (`--vault-key` /
`FAUCET_VAULT_KEY`); it is on by default for a tenant's feed. A delivery run
opens them in memory; the key never enters a run's config. The event browser
never shows a sealed payload.

The vault key also seals the signature secret of a feed made through the API
and a delivery's secret template params. Without a vault key the server
refuses those, and refuses events for a sealed feed with `503`. If the key is
lost, sealed events cannot be opened: the delivery stalls with a clear error
and nothing is skipped. Every cluster member needs the same key.

## Approvals

Under [`--require-approval feed`](/faucet-stream/cookbook/approvals/),
creating, editing or deleting a feed or one of its deliveries answers `202`
with a pending change request instead of applying it.

A change to a tenant's feed belongs to that tenant even when a server admin
proposes it (`?tenant=`): the tenant's principals see the request and its
approvers may approve it; other tenants cannot see it.

Under `--require-approval run`, a feed change that adds or edits a delivery is
gated too, because a delivery starts runs. A delivery of an approved feed then
runs without a per-run approval only while its template versions are pinned to
numbers: a delivery that follows a channel such as `stable` could be moved by
a later launch that nobody approved, so it shows `needs_approval` and starts
no runs. Feeds in the `--feeds` file are the operator's own and are not gated.

## Tenants

A [tenant's](/faucet-stream/cookbook/embedded-integrations/) feed is managed
with `?tenant=<id>` on the feed routes (a tenant-scoped principal reaches only
its own), receives events at `/v1/tenants/{tenant}/feeds/{name}/events`, is
sealed by default, and its deliveries run on behalf of the tenant: its
limits, its budget and its state namespace.

| Tenant limit | Effect |
|---|---|
| `max_feeds` | Most feeds the tenant may have. |
| `max_feed_events_per_sec` | Most events a second its feeds accept together, per server (`429` past it). |
| `max_feed_bytes` | Most bytes its feeds may hold together; past it they refuse new events (`503`). |

`--tenant-delivery-min-interval-secs` (default 10) is the shortest gap between
two runs of a tenant feed's delivery, whatever its `flush.min_interval` says.
Deleting a tenant deletes its feeds, tokens, events and positions. Each
feed is marked deleted first, so a tenant delete that fails partway (`503`)
leaves nothing that takes events or starts runs: repeat the request, or let
any server's scheduler finish the feeds.

## Rolling back a delivery run

[Rolling back](/faucet-stream/cookbook/rollback/) a delivery's run works as
for any run: it removes that run's rows from the destination. It does **not**
move the feed's position, because the destination and the feed are two systems
with no shared transaction. Instead:

1. The rollback pauses the delivery, so nothing else is delivered before you
   decide.
2. Its report says what resuming will do. If the run never moved the position,
   resuming redelivers its events and no replay is needed. Otherwise it gives
   the event number to **replay** from.
3. A replay from that event delivers every later run's events again too.

The events stay in the feed for the whole replay window, so nothing is lost
between the steps.

## A second listener for senders

By default the ingest routes share the server's listener with the control
API. `--feeds-listen <addr>` serves them, and `/healthz`, on their own
listener, and the main listener then serves no ingest route:

```bash
faucet serve --listen 127.0.0.1:8080 \
  --feeds-listen 0.0.0.0:8081 --feeds-allowed-host ingest.example.com
```

Use it when senders are on the internet. They reach a port that has no
control API behind it, and the control API can stay on a private address. TLS
stays with the proxy or ingress in front. The Helm chart exposes this as
`serve.feeds.listen`, with its own Service and Ingress.

## Clusters

Any replica accepts events, and every replica runs the scheduler: the feed
store picks one winner for each range of events, so one run delivers it.
Delivery runs go through the usual claim loop and
[execution pools](/faucet-stream/cookbook/cluster/).

- **A cluster needs a PostgreSQL feed store.** A SQLite feed store takes one
  server. With `--cluster`, the server refuses to start with a feeds file on a
  SQLite history or with a `--feed-store` that is a SQLite file, and refuses
  to create a feed in one. A PostgreSQL `--history` gives a PostgreSQL
  `default` store.
- **Every member needs the `feeds` feature.** During a rolling upgrade from a
  release without feeds, creating a feed and starting delivery runs wait until
  every live member has it.
- **Rate limits are per replica**, and the list of recently refused requests
  is kept in each replica's memory.
- If a replica dies after claiming a range and before it submitted the run,
  another replica claims the range once the claim's five minutes have passed.

## Watching feeds

`GET /v1/feeds` lists each feed with its status, size and deliveries, and
`GET /v1/feeds/{name}` adds each delivery's position, lag, last run and error,
the feed's gaps, its tokens and its change log. `GET /v1/feeds/{name}/refusals`
lists the requests the server refused lately, with the reason. `/readyz` has a
`feeds` section, and the shipped alerts cover a stalled or held delivery, a
lagging one, a feed close to full, repeated failed runs, a failing feed store
and a feed that answers many ingest requests with a `5xx` or a refusal.
See [metrics and alerts](/faucet-stream/reference/feeds/#metrics).

The web console's **Feeds** page shows the same: every feed with what needs
attention first, a feed's ingest URL, last 24 hours, storage against its
limits, deliveries with their position and runs, stored events (payloads for
admins, never a sealed feed's), refusals, tokens and history. Operators create
and edit feeds, add deliveries, pause, resume and deliver now there; admins
replay, skip, recover and delete. See the
[web console](/faucet-stream/cookbook/web-console/#feeds).

The `faucet feed` command manages feeds straight in their stores, without a
server: see [the reference](/faucet-stream/reference/feeds/#faucet-feed).

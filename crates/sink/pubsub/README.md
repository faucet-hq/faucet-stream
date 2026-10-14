# faucet-sink-pubsub

Google Cloud Pub/Sub **sink** connector for
[faucet-stream](https://github.com/faucet-hq/faucet-stream). Publishes each
record as a Pub/Sub message with a configurable encoding, optional ordering
key, bounded concurrency, and per-message partial-failure routing to a DLQ.

## Configuration

```yaml
sink:
  kind: pubsub
  config:
    topic: orders                     # required (short id, not the full path)
    project_id: my-gcp-project        # required for real Pub/Sub
    # emulator_host: "localhost:8085" # or set PUBSUB_EMULATOR_HOST
    credentials: { type: application_default }
    value_format: json                # json | string | bytes  (default json)
    ordering_key: { type: field, name: customer_id }   # none | field | jsonpath
    attributes_field: __attributes    # optional: record field -> msg attributes
    batch_size: 100                   # records per publish batch (1..=1000; 0 is rejected — see below)
    concurrency: 4                     # bounded in-flight publishes
```

### Ordering key

- `{ type: none }` — no ordering (default).
- `{ type: field, name: <field> }` — a top-level field value.
- `{ type: jsonpath, path: a.b.c }` — a dot-path value.

When an ordering-key strategy is set, message ordering is enabled on the
publisher and messages with the same key are delivered in publish order. A
record missing the key (or with a null / container value) fails **per-record**
(DLQ-routable), never the whole batch.

### Attributes

If `attributes_field` names a record field holding a JSON object of scalars,
those key/value pairs become the message attributes and the field is stripped
from the published payload — the inverse of the source's `attributes_key`, so a
Pub/Sub → Pub/Sub pipeline round-trips attributes.

### `value_format`

`json` serializes the whole record; `string` requires a JSON string record
(raw UTF-8 bytes); `bytes` requires a base64 JSON string (decoded bytes).

### `batch_size`

Records per `Publish` request (the SDK publisher's bundle size), `1..=1000`, default `100`. `concurrency` is the number of publisher workers sending bundles in parallel. Pages larger than
`batch_size` are re-chunked into several publishes, and a chunk is also cut at
about 9 MB of message data so no request crosses Pub/Sub's 10 MB `Publish`
limit when payloads are large. The house `batch_size: 0`
"no batching" sentinel does **not** apply here — Pub/Sub caps a single
`Publish` at 1000 messages, so a whole-page request is not expressible, and `0`
(like any value above 1000) is rejected at config load.

## Delivery semantics — at-least-once

Pub/Sub is at-least-once and this sink does not implement idempotent writes, so
`delivery: exactly_once` is not supported. De-duplicate downstream on a
business key in the payload or an attribute if replays must converge — the
server assigns a new `message_id` to every publish, so a re-sent record never
shares its id. With an `ordering_key`, a failed message also fails every later
message of the same key in the page: those already enqueued alongside it reached
the topic ahead of it, and those in later publish chunks are not published at
all, so a DLQ replay or retry restores the key's order.
Every publish confirmation is bounded (120 s), and the client uses gRPC
keepalives and a per-RPC deadline, so a dropped connection fails the write
instead of hanging it. Per-record encode failures and
per-message publish rejections surface via `write_batch_partial`, so a DLQ
captures individual bad records instead of failing the whole page.

## Testing

Unit tests are fully offline. Integration tests (`tests/integration.rs`)
require the Pub/Sub emulator and are skipped unless `PUBSUB_EMULATOR_HOST` is
set:

```bash
gcloud beta emulators pubsub start --host-port=localhost:8085 &
export PUBSUB_EMULATOR_HOST=localhost:8085
cargo test -p faucet-sink-pubsub
```

License: MIT OR Apache-2.0.

## Batch atomicity

What a failed write leaves behind (#737): **per-row** — every publish reports its own outcome; there is no batch-level error. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## Capabilities

Declares `write_mode: append` only; no idempotent writes. `faucet list --json` prints the declaration.

## Timeouts, cancellation and retries

Every publish and its confirmation is awaited through the pipeline's
`CallContext` (#880): any caller deadline ends the wait, and a message not
confirmed by then fails as a per-row error. The 120 s per-message confirmation
bound is unchanged; the SDK bundles messages into RPCs itself, so waits are not
counted as round trips.

## How it is built

The sink runs on the messaging family driver (RFC 0014): `PubsubDriver`
implements `faucet_core::driver::MessageDriver` — encode a record (payload,
ordering key, attributes) and publish a page's messages (count- and
byte-bounded chunks, every confirmation awaited, later messages of a failed
ordering key failed or held back) — and `MessageSinkDriver` reports each row
and serves the plain, reported and per-row write paths. The driver awaits its
own confirmations through the call slot (`publish_is_one_call` is `false`), so
the round-trip count (one `publish` per chunk) is unchanged. The public type
and config are unchanged.

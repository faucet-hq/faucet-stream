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

Records per `Publish` request, `1..=1000`, default `100`. Pages larger than
`batch_size` are re-chunked into several publishes. The house `batch_size: 0`
"no batching" sentinel does **not** apply here — Pub/Sub caps a single
`Publish` at 1000 messages, so a whole-page request is not expressible, and `0`
(like any value above 1000) is rejected at config load.

## Delivery semantics — at-least-once

Pub/Sub is at-least-once and this sink does not implement idempotent writes, so
`delivery: exactly_once` is not supported. De-duplicate downstream on
`message_id` if replays must converge. Per-record encode failures and
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

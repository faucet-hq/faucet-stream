# faucet-source-pubsub

Google Cloud Pub/Sub **source** connector for
[faucet-stream](https://github.com/faucet-hq/faucet-stream). Streams
messages from a subscription, emits one record per message, and acks messages
only once the pipeline has durably written them.

## Configuration

```yaml
source:
  kind: pubsub
  config:
    subscription: orders-sub          # required (short id, not the full path)
    project_id: my-gcp-project        # required for real Pub/Sub
    # emulator_host: "localhost:8085" # or set PUBSUB_EMULATOR_HOST
    credentials: { type: application_default }
    value_format: json                # json | string | bytes  (default json)
    attributes_key: __attributes      # JSON key for the message attribute map
    max_messages_per_pull: 100        # 1..=1000 (default 100)
    idle_termination_secs: 30         # stop after N idle seconds
    max_messages: 100000              # or stop after N messages
    batch_size: 1000                  # records per page (0 = one page per drain)
    on_decode_error: fail             # fail | skip | raw  (default fail)
    ack_deadline_extension_secs: 60   # 10..=600, 0 = no renewal (default 60)
```

`on_decode_error` handles a payload that does not decode under `value_format`:
`fail` stops the run (the message stays unacked, so the next run fails at it
again), `skip` acks it with its page and emits nothing, `raw` emits it with the
payload base64-encoded under `data` and the error under `decode_error`.

At least one of `idle_termination_secs` / `max_messages` **must** be set so a
batch run terminates (mirrors the Kafka / Kinesis sources).

### Credentials

`{ type, config }` shape, same as every faucet connector:

- `{ type: application_default }` — ADC (env, gcloud, metadata server).
- `{ type: service_account_json_file, config: { path } }`
- `{ type: service_account_json_inline, config: { json } }` — pair with
  `${secret:…}` / `${env:…}`.
- `{ type: anonymous }` — for the emulator.

## Emitted record shape

```json
{
  "data": { ... },                 // decoded per value_format
  "__attributes": { "k": "v" },    // key configurable via attributes_key
  "message_id": "123456789",
  "ordering_key": "order-42",      // omitted when empty
  "publish_time_millis": 1716700000123
}
```

`value_format`: `json` parses the payload as JSON; `string` decodes UTF-8 into a
JSON string; `bytes` base64-encodes the raw payload.

## Delivery semantics — at-least-once

Messages are acked at **durable page boundaries**: a page's messages are acked
only after the pipeline has written that page to the sink and persisted its
bookmark. A crash between the sink write and the ack redelivers those messages
on the next run — never data loss, but duplicates are possible. Pair with an
upsert sink keyed on `message_id` when replays must converge. Acks go out as one
`Acknowledge` RPC per 500 messages.

While a message is held — its page being assembled or written — the source
renews its ack deadline to `ack_deadline_extension_secs` (every 5 s), so a page
that outlives the subscription's ack deadline is not redelivered into the same
run.

**Exactly-once delivery is not supported** — Pub/Sub offers no primitive that
composes with faucet's atomic-watermark model. The bookmark this source
persists is informational only (a cumulative count + last `message_id`); on
resume the subscription redelivers whatever was never acked.

## Testing

Unit tests are fully offline. Integration tests (`tests/integration.rs`)
require the Pub/Sub emulator and are skipped unless `PUBSUB_EMULATOR_HOST` is
set:

```bash
gcloud beta emulators pubsub start --host-port=localhost:8085 &
export PUBSUB_EMULATOR_HOST=localhost:8085
cargo test -p faucet-source-pubsub
```

License: MIT OR Apache-2.0.

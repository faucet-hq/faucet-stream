# faucet-source-sqs

AWS SQS **source** connector for
[faucet-stream](https://github.com/faucet-hq/faucet-stream): long-polls
`ReceiveMessage`, buffers up to `batch_size` messages, and emits them page by
page with bounded memory. It terminates on `idle_timeout_secs` and/or
`max_messages`.

```yaml
source:
  type: sqs
  config:
    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/events
    region: us-east-1
    idle_timeout_secs: 30      # at least one termination knob is required
    wait_time_seconds: 10
    batch_size: 1000
```

## Configuration

| Field | Default | Notes |
|-------|---------|-------|
| `queue_url` | — | Required. Full SQS queue URL. |
| `region` | SDK default chain | |
| `endpoint_url` | — | LocalStack / VPC endpoint override. |
| `credentials` | `{ type: default }` | `default` \| `profile` \| `access_key` \| `assume_role` \| `web_identity` — see `faucet-common-sqs`. |
| `idle_timeout_secs` / `max_messages` | — | **At least one is required** so a batch run terminates. |
| `wait_time_seconds` | `10` | Long-poll wait per `ReceiveMessage` (0–20). |
| `batch_size` | `1000` | Records per emitted page. `0` = one page for the whole drain. |
| `include_metadata` | `false` | Wrap each record as `{message_id, attributes, payload}` — the SQS `MessageId` for downstream deduplication of redeliveries, and the message attributes (requested only when this is on). |
| `visibility_extension_secs` | `60` | Visibility timeout renewed (every third of the window) on every message received but not yet deleted, so a slow page is not redelivered into the same run. `0` disables renewal (3–43200 otherwise). |

Each `ReceiveMessage` call requests up to 10 messages (the SQS API cap),
capped further so it never over-reads past `max_messages`. With
`wait_time_seconds: 0` an empty receive is followed by a short pause rather
than an immediate retry.

**FIFO queues** (`.fifo`) hand out nothing more from a message group while its
earlier messages are in flight, so the source emits — and, once written,
deletes — a page after every receive instead of waiting for `batch_size`
messages; a single-group queue drains completely.

## Record shape

Each message body is emitted as its **parsed JSON value** when the body is
valid JSON, otherwise as a JSON **string** of the raw body:

```json
{ "order_id": 42, "status": "shipped" }
```
```json
"a plain, non-JSON body"
```

## Delivery semantics

Each page's receipt handles are deleted (via `DeleteMessageBatch`) **after** the
page has been written downstream — the deletes for a page are issued once the
pipeline comes back for the next page, so nothing is removed from the queue
before it has been persisted. Delivery is therefore **at-least-once**: a sink
error, an abort, or a crash between the write and the delete leaves the messages
in the queue, and they are redelivered once the visibility window elapses.

Every page carries an informational `{queue, consumed}` bookmark so the pipeline
flushes a buffering sink (file, object-store, Parquet) **before** it resumes the
source — which is when that page is deleted. It is not a resume position: the
queue itself is the cursor. Key downstream consumers on a message field (e.g. an upsert sink)
when replays must converge.

While a message is held — its page being assembled or written — the source
renews its visibility timeout to `visibility_extension_secs`, so a slow page is
not redelivered into the same run. With renewal off (`0`), set the queue's
visibility timeout comfortably above the time it takes to assemble and write
one page.

## LocalStack

```yaml
config:
  endpoint_url: http://localhost:4566
  region: us-east-1
  credentials: { type: access_key, config: { access_key_id: test, secret_access_key: test } }
```

## License

MIT OR Apache-2.0

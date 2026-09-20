# faucet-sink-sqs

AWS SQS **sink** connector for
[faucet-stream](https://github.com/faucet-hq/faucet-stream): batched
`SendMessageBatch` writes with bounded request concurrency, per-entry
partial-failure retry, and optional FIFO routing.

```yaml
sink:
  type: sqs
  config:
    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/events
    region: us-east-1
    batch_size: 10
```

FIFO queue:

```yaml
sink:
  type: sqs
  config:
    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/events.fifo
    region: us-east-1
    message_group_id: orders
    message_deduplication_id_field: order_id   # a record field used as the dedup id
```

## Configuration

| Field | Default | Notes |
|-------|---------|-------|
| `queue_url` | — | Required. Full SQS queue URL. |
| `region` | SDK default chain | |
| `endpoint_url` | — | LocalStack / VPC endpoint override. |
| `credentials` | `{ type: default }` | `default` \| `profile` \| `access_key` \| `assume_role` \| `web_identity` — see `faucet-common-sqs`. |
| `message_group_id` | — | Applied to every message. Required by FIFO queues. |
| `message_deduplication_id_field` | — | Record field whose stringified value is the `MessageDeduplicationId`. Missing / non-scalar → per-record failure (DLQ-routable). |
| `batch_size` | `10` | Entries per `SendMessageBatch` (1–10, the API cap). The house `batch_size: 0` "no batching" sentinel does **not** apply — `0` is rejected at config load, since a whole-page request cannot exceed the 10-entry API cap. |
| `concurrency` | `4` | Bounded concurrent in-flight requests. |
| `retry` | `{}` | Per-record partial-failure retry, grouped: `max_attempts` (`5`), `initial_backoff_ms` (`100`), `max_backoff_ms` (`30000`). The flat `retry_max_attempts` / `retry_initial_backoff_ms` / `retry_max_backoff_ms` keys are still accepted (**deprecated** since #654) and are superseded wholesale when `retry:` is present. |

Each record is serialized to a JSON string as the message body. A body over
256 KiB (the SQS limit) fails per-record (never sent) rather than panicking.
Requests are re-chunked to both the 10-entry and 256 KiB request ceilings.

## Delivery semantics

**At-least-once.** A whole-request failure that is retried after the messages
actually landed can duplicate them. On a FIFO queue, set
`message_deduplication_id_field` (or enable content-based dedup on the queue)
so replays within the 5-minute dedup window converge. This is an append-only
sink — it advertises no idempotent-write or keyed-dedup capability, so the
pipeline correctly refuses `delivery: exactly_once`.

Partial failures come back per-record: with a DLQ configured, individual
rejected messages are routed to the DLQ while the rest of the page proceeds.

## LocalStack

```yaml
config:
  endpoint_url: http://localhost:4566
  region: us-east-1
  credentials: { type: access_key, config: { access_key_id: test, secret_access_key: test } }
```

## License

MIT OR Apache-2.0

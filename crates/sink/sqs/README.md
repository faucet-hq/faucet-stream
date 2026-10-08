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
| `message_group_id` | — | Applied to every message. Required (validated) for a FIFO queue — a `queue_url` ending in `.fifo`. |
| `message_deduplication_id_field` | — | Record field whose stringified value is the `MessageDeduplicationId`. Missing / non-scalar → per-record failure (DLQ-routable). |
| `batch_size` | `10` | Entries per `SendMessageBatch` (1–10, the API cap). The house `batch_size: 0` "no batching" sentinel does **not** apply — `0` is rejected at config load, since a whole-page request cannot exceed the 10-entry API cap. |
| `concurrency` | `4` | Bounded concurrent in-flight requests. A FIFO queue always sends one request at a time, so a group's messages arrive in order. |
| `retry` | `{}` | Per-record partial-failure retry, grouped: `max_attempts` (`5`), `initial_backoff_ms` (`100`), `max_backoff_ms` (`30000`). The flat `retry_max_attempts` / `retry_initial_backoff_ms` / `retry_max_backoff_ms` keys are still accepted (**deprecated** since #654) and are superseded wholesale when `retry:` is present. |

Each record is serialized to a JSON string as the message body. A body over
1 MiB (the SQS limit) fails per-record (never sent); a queue whose
`MaximumMessageSize` is lower rejects the entry per row. Requests are
re-chunked to both the 10-entry and 1 MiB request ceilings, and re-split to
256 KiB requests when a service still enforcing the older request limit
refuses a batch as too long.

Only transient request failures (transport errors, 5xx/429, throttling) are
retried; a missing queue or denied access fails at once. A request that fails
for good is reported on each of its entries, so the outcomes of the requests
that landed are kept. On a FIFO queue, an entry that fails is re-sent together
with every later entry of its group from the same request, and the later
entries of a group whose message failed for good are reported failed too, so a
retry or DLQ replay can restore the group's order.

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

## Batch atomicity

What a failed write leaves behind (#737): **best-effort** — SendMessageBatch requests run concurrently and retry per entry. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## License

MIT OR Apache-2.0

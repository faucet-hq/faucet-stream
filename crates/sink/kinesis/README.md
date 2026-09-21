# faucet-sink-kinesis

AWS Kinesis Data Streams **sink** connector for
[faucet-stream](https://github.com/faucet-hq/faucet-stream): batched
`PutRecords` writes with configurable partition-key derivation, bounded
concurrent in-flight requests, per-entry partial-failure retry, and
DLQ-routable per-record outcomes.

```yaml
sink:
  type: kinesis
  config:
    stream_name: events
    region: us-east-1
    partition_key: { type: field, name: user_id }
```

## Configuration

| Field | Default | Notes |
|-------|---------|-------|
| `stream_name` | — | Required. |
| `region` / `endpoint_url` / `credentials` | SDK defaults | Same shapes as the source — see `faucet-common-kinesis`. |
| `partition_key` | `{ type: random }` | See strategies below. |
| `explicit_hash_key` | `{ type: none }` | `field` / `jsonpath` override — must resolve to a decimal integer in `[0, 2^128)`. |
| `value_format` | `json` | `json` (serialize the record) \| `string` (record must be a JSON string) \| `bytes` (record must be a base64 string). |
| `batch_size` | `500` | Entries per `PutRecords` request (hard API cap: 500). The house `batch_size: 0` "no batching" sentinel does **not** apply — `0` is rejected at config load, since a whole-page request cannot exceed the 500-entry API cap. |
| `max_record_size_bytes` | `1048576` | Per-record cap (data + partition key; Kinesis hard limit 1 MiB). Oversized records fail per-record, never sent. |
| `max_request_bytes` | `5242880` | Per-request cap (Kinesis hard limit 5 MiB); batches re-chunk to it. |
| `concurrency` | `4` | Bounded in-flight `PutRecords` requests. |
| `retry` | `{}` | Per-entry partial-failure retry, grouped: `max_attempts` (`5`), `initial_backoff_ms` (`100`), `max_backoff_ms` (`30000`). The flat `retry_max_attempts` / `retry_initial_backoff_ms` / `retry_max_backoff_ms` keys are still accepted (**deprecated** since #654) and are superseded wholesale when `retry:` is present. |

## Partition-key strategies

| `type` | Behavior |
|--------|----------|
| `random` | UUID v4 per record — uniform spread, no ordering guarantees. |
| `field` | Top-level field value, stringified. Missing / null / container values fail **per-record** (DLQ-routable). |
| `jsonpath` | Dot path (`a.b.c`, object keys only), stringified. |
| `hash` | Dot-path value MD5-hashed — even spread over hot keys. |
| `static` | One constant key → one shard (rarely what you want). |

Keys must be 1–256 characters (per-record error otherwise). Records sharing a
partition key land on the same shard, preserving their relative order.

## Failure semantics

- **Per-entry `PutRecords` rejections** (throughput, internal errors) are
  retried with backoff up to `retry.max_attempts`; on exhaustion those records
  come back as `Err` rows from `write_batch_partial` — the pipeline's `dlq:`
  router quarantines them.
- **Whole-request failures** (network partition, auth) retry the same way; on
  exhaustion the batch fails outright and the pipeline's `on_batch_error`
  policy decides.
- **Delivery is at-least-once**: an ambiguous failure retried after a partial
  server-side write can double-write records. Key downstream consumers on an
  idempotency field when replays must converge.

## License

MIT OR Apache-2.0

# faucet-sink-nats

A [NATS](https://nats.io) sink for [`faucet-stream`](https://crates.io/crates/faucet-stream).

Publishes each record as a JSON message to a subject — fixed, or per-record via
a configurable `subject_field`. Core NATS publishes are flushed after every
`batch_size` records; core NATS only confirms the server received the bytes, so
a message no subscriber or stream takes is dropped by the server. Set
`jetstream: true` to publish through JetStream and await each message's
acknowledgement — a row no stream stores (or that a stream rejects) then fails
on its own and can be routed to a DLQ. Every publish batch is bounded by
`publish_timeout_secs`, and the client gives up reconnecting after 60 attempts,
so a server outage fails the run instead of hanging it.

Append-only: it does not override idempotency, upsert, or schema-evolution (the
trait defaults hold).

## Configuration

The shared connection surface (`servers` / `auth` / `tls` / `name` — see
[`faucet-common-nats`](https://crates.io/crates/faucet-common-nats)) is flattened
in alongside these fields:

| field           | type             | default | description                                                        |
|-----------------|------------------|---------|--------------------------------------------------------------------|
| `subject`       | `String`         | —       | default subject every record is published to. Required.            |
| `subject_field` | `Option<String>` | —       | top-level record field whose string value overrides `subject` per record. |
| `batch_size`    | `usize`          | `1000`  | records per publish batch: the client is flushed (or the JetStream acks awaited) after each batch. `0` = one batch per page. |
| `jetstream`     | `bool`           | `false` | publish through JetStream and await each acknowledgement; per-row outcomes (`write_batch_partial`, DLQ-routable). |
| `publish_timeout_secs` | `u64`     | `30`    | bound on one publish batch (publishes + flush, or the acks); a timeout fails the write as retriable. |

A record whose `subject_field` is missing or not a string fails on its own row.

## Example

```yaml
version: 1
pipeline:
  source:
    kind: jsonl
    config:
      path: events.jsonl
  sink:
    kind: nats
    config:
      servers: ["nats://127.0.0.1:4222"]
      subject: "events.ingested"
      batch_size: 1000
```

### Subject per record

```yaml
  sink:
    kind: nats
    config:
      servers: ["nats://127.0.0.1:4222"]
      subject: "events.default"    # fallback / default
      subject_field: "topic"        # each record's `topic` string is the subject
```

## Batch atomicity

What a failed write leaves behind (#737): **best-effort** — each record is its own publish. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## License

Licensed under either of Apache-2.0 or MIT at your option.

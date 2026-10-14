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
so a server outage fails the run instead of hanging it. A sink instance whose
client has given up stays failed — the CLI builds connectors afresh for each
run; a library caller that keeps one sink across an outage must build a new one.

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
| `publish_timeout_secs` | `u64`     | `30`    | bound on one publish batch (publishes + flush, or the acks); on a timeout the rows not yet confirmed fail with a `timeout`-class row error — a `resilience.poison` policy resubmits them, otherwise they go to the DLQ — and earlier batches keep their outcomes; a timed-out flush is retried by `resilience.retry`. |

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

## Capabilities

Declares `write_mode: append` only; no idempotent writes. `faucet list --json` prints the declaration.

## Timeouts, cancellation and retries

The connect, each publish group and `flush` run through the pipeline's `CallContext` (#880): the run's deadline ends them promptly, `publish_timeout_secs` still bounds each, and the rows of a group cut short fail per row (DLQ-routable). Each counts as one round trip (`connect`, `publish`, `flush`) in `faucet_sink_roundtrips_total`. The sink never re-publishes itself.

## How it is built

The sink runs on the messaging family driver (RFC 0014): `NatsDriver`
implements `faucet_core::driver::MessageDriver` — resolve the subject and
encode a record, publish a group (Core NATS: publishes then a flush;
JetStream: publishes then each acknowledgement) — and `MessageSinkDriver`
drives the `batch_size` groups, the `publish_timeout_secs` bound, which rows a
timeout or a cancel leaves unconfirmed, and the per-row outcomes. The public
type and config are unchanged.

## License

Licensed under either of Apache-2.0 or MIT at your option.

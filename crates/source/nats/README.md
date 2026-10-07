# faucet-source-nats

A [NATS](https://nats.io) source for [`faucet-stream`](https://crates.io/crates/faucet-stream).

Subscribes to a subject (core NATS, with `*`/`>` wildcards and optional queue
groups) or pulls from a durable JetStream consumer, drains until `max_messages`
or `idle_timeout_secs` fires, and yields each message payload as a JSON record —
by default valid JSON passes through and other UTF-8 text becomes a JSON string;
`value_format: bytes` base64-encodes binary payloads. A payload the format
cannot represent (binary under the default) fails the run rather than being
altered.

Core NATS is fire-and-forget at-least-once, so runs carry **no bookmark** and
are not resumable/exactly-once. In JetStream mode each page's messages are
**acked after the page is written and flushed**, giving at-least-once delivery:
every JetStream page carries an informational `{stream, consumer, consumed}`
bookmark so the pipeline flushes a buffering sink before the page is acked, and
held messages get in-progress acks every `progress_interval_secs` so a slow page
is not redelivered into the same run.

## Configuration

The shared connection surface (`servers` / `auth` / `tls` / `name` — see
[`faucet-common-nats`](https://crates.io/crates/faucet-common-nats)) is flattened
in alongside these fields:

| field               | type             | default | description                                                        |
|---------------------|------------------|---------|--------------------------------------------------------------------|
| `subject`           | `String`         | —       | subject to subscribe to (`*`/`>` wildcards). Required.             |
| `queue_group`       | `Option<String>` | —       | core-NATS queue group for load-balanced subscriptions.             |
| `jetstream_stream`  | `Option<String>` | —       | JetStream stream name (enables JetStream mode).                    |
| `jetstream_consumer`| `Option<String>` | —       | durable pull-consumer name; required with `jetstream_stream`.      |
| `max_messages`      | `Option<usize>`  | —       | stop after this many messages.                                     |
| `idle_timeout_secs` | `Option<u64>`    | —       | stop after this many seconds with no new message.                  |
| `batch_size`        | `usize`          | `1000`  | records per emitted page (`0` = one page for the whole run window).|
| `progress_interval_secs` | `u64`       | `10`    | JetStream only: in-progress ack (`+WPI`) every N s for every message pulled but not yet acked, so a slow page is not redelivered into the same run. Keep it below the consumer's `ack_wait`. `0` disables. |
| `value_format`      | `auto` \| `json` \| `string` \| `bytes` | `auto` | how a payload becomes a record: `auto` (JSON, else UTF-8 text), `json` (must parse), `string` (UTF-8 text), `bytes` (base64). Non-UTF-8 under `auto` / `string` fails the run. |

At least one of `max_messages` / `idle_timeout_secs` must be set so the run
terminates.

## Example (core NATS)

```yaml
version: 1
pipeline:
  source:
    kind: nats
    config:
      servers: ["nats://127.0.0.1:4222"]
      subject: "events.>"
      idle_timeout_secs: 5
      batch_size: 500
  sink:
    kind: stdout
    config: {}
```

## Example (JetStream durable consumer)

```yaml
pipeline:
  source:
    kind: nats
    config:
      servers: ["nats://127.0.0.1:4222"]
      subject: "orders.>"
      jetstream_stream: ORDERS
      jetstream_consumer: faucet-worker
      max_messages: 10000
      idle_timeout_secs: 10
```

## License

Licensed under either of Apache-2.0 or MIT at your option.

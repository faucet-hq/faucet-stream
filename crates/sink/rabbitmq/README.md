# faucet-sink-rabbitmq

A [RabbitMQ](https://www.rabbitmq.com) (AMQP 0.9.1) sink for
[`faucet-stream`](https://crates.io/crates/faucet-stream), built on the
pure-Rust [`lapin`](https://crates.io/crates/lapin) client.

Publishes each record as one message to an exchange (the default exchange
unless configured) with a static, per-field, or JSONPath-derived routing key.
Each chunk of `batch_size` messages is published back-to-back and then its
publisher confirms are awaited together, so throughput stays high while every
write is broker-acknowledged before it returns.

## Delivery and errors

- **Publisher confirms** (`confirm: true`, default): a write returns only after
  the broker has acknowledged every message.
- **`mandatory: true`**: a message no queue is bound to receive is returned by
  the broker. Through `write_batch_partial` it becomes that row's error, so the
  pipeline routes it to the `dlq:`; without a DLQ the batch fails. Requires
  `confirm: true`.
- Per-row problems — a routing key that resolves to nothing / null / a
  container / more than 255 bytes, a record that does not fit `value_format`, a
  broker `nack` — are row errors too. A channel-level failure (e.g. publishing
  to a missing exchange) fails the batch; the next write reconnects.

Append-only: it does not advertise idempotent writes, upsert, or schema
evolution.

## Configuration

The shared connection surface (see
[`faucet-common-rabbitmq`](https://crates.io/crates/faucet-common-rabbitmq)) is
flattened in alongside these fields:

| field                  | type                    | default | description |
|------------------------|-------------------------|---------|-------------|
| `exchange`             | `String`                | `""`    | exchange to publish to (`""` = default exchange: routing key = queue name) |
| `exchange_kind`        | `Option<…>`             | —       | `direct`/`fanout`/`topic`/`headers` — declare `exchange` (durable) before publishing |
| `routing_key`          | `Option<String>`        | —       | static routing key |
| `routing_key_field`    | `Option<String>`        | —       | top-level record field holding the routing key |
| `routing_key_jsonpath` | `Option<String>`        | —       | JSONPath whose first match is the routing key |
| `value_format`         | `json`/`string`/`bytes` | `json`  | body encoding (`string` needs string records, `bytes` base64 strings) |
| `persistent`           | `bool`                  | `true`  | delivery mode 2 (persisted on durable queues) |
| `mandatory`            | `bool`                  | `false` | return unroutable messages as row errors |
| `confirm`              | `bool`                  | `true`  | wait for publisher confirms |
| `batch_size`           | `usize`                 | `1000`  | messages published before awaiting their confirms (`0` = whole batch) |

Exactly one of `routing_key` / `routing_key_field` / `routing_key_jsonpath` is
required. Messages carry a `content_type` matching `value_format`.

## Example

```yaml
version: 1
pipeline:
  source:
    type: csv
    config:
      path: ./orders.csv
  sink:
    type: rabbitmq
    config:
      url: "amqp://guest:guest@127.0.0.1:5672/%2f"
      exchange: events
      exchange_kind: topic
      routing_key_field: event_type
      mandatory: true
  dlq:
    type: jsonl
    config:
      path: ./out/unroutable.jsonl
```

## Preflight (`faucet doctor`)

`check()` connects and passively checks the exchange — nothing is published. A
missing exchange is a skip when `exchange_kind` is set (it is declared on the
first write) and a failure otherwise.

## TLS

Enable the `tls` feature (CLI: `rabbitmq-tls`) and set `tls.enabled: true` or
use an `amqps://` url.

## License

Licensed under either of Apache-2.0 or MIT at your option.

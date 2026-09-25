# faucet-source-rabbitmq

A [RabbitMQ](https://www.rabbitmq.com) (AMQP 0.9.1) source for
[`faucet-stream`](https://crates.io/crates/faucet-stream), built on the
pure-Rust [`lapin`](https://crates.io/crates/lapin) client.

Consumes a queue — optionally declaring it and binding it to exchanges — drains
until `max_messages` or `idle_timeout_secs` fires, and yields each message body
as a record. Streams natively (`stream_pages`), holding at most `batch_size`
records per page.

## Delivery guarantee

- **`ack_mode: on_sink_confirm`** (default) — at-least-once. A page's
  deliveries are acknowledged with one `basic.ack(multiple)` only after the
  pipeline has written **and flushed** that page (every page carries a small
  `{queue, consumed}` bookmark so the pipeline flushes the sink before asking
  for the next one). A crash or failed write leaves them unacknowledged and the
  broker redelivers them — duplicates are possible, loss is not. Pair with a
  keyed-upsert sink (`write_mode: upsert`) for effectively-once results.
- **`ack_mode: auto`** — at-most-once (`no_ack`): fastest, but a crash loses
  in-flight messages.

The broker owns the queue position, so the source is not resumable from a
faucet bookmark and does not qualify for `delivery: exactly_once`.

Messages the broker pre-fetched beyond `max_messages` are never acknowledged —
closing the channel at the end of the run requeues them. A connection or
consumer lost mid-run (e.g. the queue is deleted) fails the run loudly.

## Configuration

The shared connection surface (`url` / `host` / `port` / `vhost` / `auth` /
`tls` / … — see [`faucet-common-rabbitmq`](https://crates.io/crates/faucet-common-rabbitmq))
is flattened in alongside these fields:

| field               | type                    | default           | description |
|---------------------|-------------------------|-------------------|-------------|
| `queue`             | `String`                | —                 | queue to consume. Required. |
| `declare_queue`     | `bool`                  | `true`            | declare the queue before consuming (idempotent); `false` requires it to exist |
| `queue_durable`     | `bool`                  | `true`            | durability of a declared queue (must match an existing queue) |
| `bindings`          | `[{exchange, routing_key, exchange_kind?}]` | `[]` | exchange → queue bindings declared before consuming; `exchange_kind` (`direct`/`fanout`/`topic`/`headers`) also declares the exchange |
| `prefetch`          | `Option<u16>`           | `batch_size`      | QoS window (`0` = unlimited). Under `on_sink_confirm` it must be `0` or ≥ `batch_size` |
| `ack_mode`          | `on_sink_confirm`/`auto`| `on_sink_confirm` | see above |
| `value_format`      | `json`/`string`/`bytes` | `json`            | body decoding (`bytes` → base64) |
| `on_decode_error`   | `fail`/`skip`           | `fail`            | `skip` rejects the message without requeue (→ the queue's dead-letter exchange, if any) |
| `include_metadata`  | `bool`                  | `false`           | wrap records as `{data, exchange, routing_key, delivery_tag, redelivered, headers, content_type, message_id, correlation_id, timestamp}` |
| `consumer_tag`      | `Option<String>`        | broker-generated  | consumer tag |
| `max_messages`      | `Option<usize>`         | —                 | stop after this many messages |
| `idle_timeout_secs` | `Option<u64>`           | —                 | stop after this many idle seconds |
| `batch_size`        | `usize`                 | `1000`            | records per page and per ack (`0` = one page for the run) |

At least one of `max_messages` / `idle_timeout_secs` must be set so the run
terminates. Redeclaring an existing queue or exchange with different
properties surfaces as a clear config error (`PRECONDITION_FAILED`).

## Example

```yaml
version: 1
pipeline:
  source:
    type: rabbitmq
    config:
      url: "amqp://guest:guest@127.0.0.1:5672/%2f"
      queue: orders
      bindings:
        - exchange: events
          exchange_kind: topic
          routing_key: "orders.*"
      idle_timeout_secs: 5
      batch_size: 500
  sink:
    type: jsonl
    config:
      path: ./out/orders.jsonl
```

## Preflight (`faucet doctor`)

`check()` connects and passively checks the queue — nothing is consumed. A
missing queue is a skip when `declare_queue: true` (it is declared on the first
run) and a failure otherwise.

## TLS

Enable the `tls` feature (CLI: `rabbitmq-tls`) and set `tls.enabled: true` or
use an `amqps://` url.

## License

Licensed under either of Apache-2.0 or MIT at your option.

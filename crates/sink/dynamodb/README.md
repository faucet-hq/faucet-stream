# faucet-sink-dynamodb

Amazon DynamoDB sink for [faucet-stream](https://github.com/faucet-hq/faucet-stream).

```yaml
sink:
  type: dynamodb
  config:
    table_name: orders
    region: us-east-1
    write_mode: upsert
    key: [pk, sk]
    delete_marker: { field: __op, values: [d] }
```

## Configuration

| Field | Default | Meaning |
|-------|---------|---------|
| `table_name` | — | Target table (must exist) |
| `region` | SDK chain | AWS region |
| `endpoint_url` | — | DynamoDB Local / LocalStack / VPC endpoint |
| `credentials` | `default` | `{ type, config }` — see `faucet-common-dynamodb` |
| `write_mode` | `append` | `append`, `upsert` or `delete` |
| `key` | `[]` | Required for `upsert` / `delete`; must equal the table's key schema |
| `delete_marker` | — | Upsert only: rows whose `field` matches one of `values` are deleted; the field is stripped from written items |
| `batch_size` | `25` | Items per `BatchWriteItem` (1–25; `0` = 25) |
| `concurrency` | `4` | Concurrent requests |
| `condition_expression` | — | Applied to every put/delete (e.g. `attribute_not_exists(pk)`) |
| `expression_attribute_names` / `expression_attribute_values` | `{}` | For `condition_expression` |
| `on_condition_failure` | `skip` | `skip` (treat as already applied) or `fail` (row error → DLQ) |
| `retry` | `{max_retries: 8, initial_backoff_ms: 100, max_backoff_ms: 10000}` | Throttle / unprocessed-item retry |

## Write modes

`PutItem` replaces a whole item, so `append` and `upsert` both write puts;
`delete` writes `DeleteItem`s with only the key attributes. Every mode dedups
the page by the table key, last write wins — `BatchWriteItem` rejects a request
that names one key twice, and sequential puts converge to the last anyway.
`overwrite` is not supported (no atomic table swap). `dedups_by_key()` is true
for `upsert` / `delete` with a `key`, so `delivery: exactly_once` is satisfied by
the keyed-upsert mechanism.

## Batching, retries and the DLQ

Requests carry at most 25 items and 16 MB; items over 400 KB are rejected
locally. `UnprocessedItems` are resent with jittered backoff until the retry
budget is spent; throttled or transient request failures retry the same way.

`write_batch_partial` reports per-row outcomes for the DLQ: non-object records,
rows missing a key attribute, oversized items, items DynamoDB rejects as invalid
(a `ValidationException` on a batch falls back to item-by-item writes so only the
bad rows fail), items still unprocessed after the budget, and — with
`on_condition_failure: fail` — failed conditions. A request-level failure (missing
table, access denied, retries exhausted) is the outer error.

## Conditional writes

`BatchWriteItem` cannot carry conditions, so with `condition_expression` set the
sink writes item by item (still `concurrency`-bounded). A failed condition is
skipped by default, which makes re-delivery idempotent with conditions like
`attribute_not_exists(pk)` or `attribute_not_exists(pk) OR #v < :v`.

## Preflight

`check()` runs `DescribeTable` and, for keyed modes, verifies `key` matches the
table key schema. Nothing is written.

## License

MIT OR Apache-2.0

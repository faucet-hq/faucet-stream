# faucet-source-dynamodb

Amazon DynamoDB source for [faucet-stream](https://github.com/faucet-hq/faucet-stream):
parallel **scan**, **query** by key condition, and **DynamoDB Streams CDC**.

```yaml
source:
  type: dynamodb
  config:
    table_name: orders
    region: us-east-1
    mode: scan          # scan | query | streams
    segments: 8         # parallel scan
```

## Configuration

| Field | Default | Meaning |
|-------|---------|---------|
| `table_name` | — | Table to read |
| `region` | SDK chain | AWS region |
| `endpoint_url` | — | DynamoDB Local / LocalStack / VPC endpoint (also used for Streams) |
| `credentials` | `default` | `{ type, config }` — see `faucet-common-dynamodb` |
| `mode` | `scan` | `scan`, `query` or `streams` |
| `segments` | `1` | Parallel scan `TotalSegments` (1–1000000), scan only |
| `max_concurrency` | `8` | Segments read at once |
| `index_name` | — | Secondary index to scan or query |
| `projection` | — | `ProjectionExpression` |
| `filter_expression` | — | `FilterExpression` |
| `key_condition_expression` | — | Required for `query`, rejected otherwise |
| `expression_attribute_names` | `{}` | `#name` → attribute |
| `expression_attribute_values` | `{}` | `:value` → plain JSON value |
| `consistent_read` | `false` | Strongly consistent reads (table / LSI) |
| `scan_index_forward` | `true` | Query order |
| `page_limit` | — | `Limit` per request |
| `stream_arn` | table's `LatestStreamArn` | Streams only |
| `start_position` | `trim_horizon` | `trim_horizon` or `latest`, for shards with no bookmark |
| `on_gap` | `fail` | `fail` or `resnapshot` (see below) |
| `poll_interval_ms` | `1000` | Per-shard wait when caught up (floored at 200) |
| `records_per_request` | `1000` | `GetRecords` `Limit` (1–1000) |
| `shard_concurrency` | `4` | Shards read at once — keep ≥ the number of open shards |
| `idle_termination_secs` / `max_messages` | — | Streams: at least one is required so a run terminates |
| `retry` | `{max_retries: 8, initial_backoff_ms: 100, max_backoff_ms: 10000}` | Throttle / transient retry |
| `batch_size` | `1000` | Records per page; `0` = one page |

Items are converted to plain JSON losslessly: numbers that are not exactly
representable (beyond `i64`/`u64` or an exact `f64`) stay decimal strings,
sets become arrays, binary becomes base64.

## Scan and query

Each segment pages with `LastEvaluatedKey`. Intermediate pages carry a
bookmark with every segment's cursor (`dynamodb:<table>` state key), so a
crashed run resumes each segment where it stopped; the final page resets the
cursors, so the next run reads the table again (a scan is a snapshot). Throttling
(`ProvisionedThroughputExceededException` and friends) backs off and retries;
consumed capacity is logged per segment.

### Cluster sharding

`mode: scan` is shardable: `enumerate_shards` returns one shard per segment
(`segments` when set above 1, otherwise the coordinator's target), and each
worker scans only its segment.

## Streams CDC

`mode: streams` reads the table's stream (enable it with `NEW_AND_OLD_IMAGES`).
Each change becomes a `cdc_unwrap`-compatible envelope:

```json
{ "op": "c|u|d", "before": {…} | null, "after": {…} | null, "key": {…},
  "document_key": {…}, "table": "orders", "event_id": "…", "event_name": "INSERT",
  "sequence_number": "…", "shard_id": "…", "ts_ms": 1716700000000,
  "size_bytes": 42, "stream_view_type": "NEW_AND_OLD_IMAGES", "user_identity": null }
```

TTL expirations arrive as `op: "d"` with `user_identity.principal_id =
"dynamodb.amazonaws.com"`. With `KEYS_ONLY` / `NEW_IMAGE` views `before` is
`null`, and `cdc_unwrap` falls back to `document_key` for deletes.

- **Ordering** — a child shard is read only after its parent is drained; when a
  shard closes the stream is re-described to pick up its children.
- **Bookmarks** — every page carries `{stream_arn, shards: {id: seq}, finished}`
  (state key `dynamodb-streams:<table>`). A shard opened but not yet read is
  recorded with an empty sequence and resumes from its trim horizon, so nothing
  between runs is skipped. A child of a shard that was read always starts at its
  trim horizon.
- **Gaps** — Streams keep 24 hours of changes. Resuming fails with the gap named
  when the stream was replaced, a bookmarked shard expired, a shard's parent
  expired unread, or a bookmarked position was trimmed. With
  `on_gap: resnapshot` the source instead scans the whole table (emitted as
  `op: "r"` envelopes, no bookmark until the scan completes) and then replays the
  retained stream from the trim horizon; on an upsert sink this converges.
- **`faucet mirror`** — `capture_resume_position` returns every current shard at
  its trim horizon, so a CDC run after the snapshot replays the retained window
  and converges.
- **Delivery** — at-least-once (`supports_exactly_once` is `false`, like the
  Kinesis source). Pair with an upsert sink keyed on the table key.

## Discovery and preflight

`discover()` lists tables (`ListTables` + `DescribeTable`): one descriptor per
table with its key attributes as the schema, `ItemCount` as the row estimate and
`{table_name}` as the config patch. `check()` runs `DescribeTable` (and
`DescribeStream` in streams mode) — nothing is read.

## License

MIT OR Apache-2.0

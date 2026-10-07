# faucet-source-kinesis

AWS Kinesis Data Streams **source** connector for
[faucet-stream](https://github.com/faucet-hq/faucet-stream): consumes
records from every (or selected) shard with bounded per-shard concurrency,
resumable per-shard sequence-number checkpoints, throttle-aware backoff, and
the standard `idle_termination_secs` / `max_messages` termination knobs
(mirroring the Kafka source).

```yaml
source:
  type: kinesis
  config:
    stream_name: events
    region: us-east-1
    start_position: { type: trim_horizon }
    idle_termination_secs: 30      # at least one termination knob is required
    value_format: json
```

## Configuration

| Field | Default | Notes |
|-------|---------|-------|
| `stream_name` | — | Required. |
| `region` | SDK default chain | |
| `endpoint_url` | — | LocalStack / VPC endpoint override. |
| `credentials` | `{ type: default }` | `default` \| `profile` \| `access_key` \| `assume_role` \| `web_identity` — see `faucet-common-kinesis`. |
| `start_position` | `{ type: trim_horizon }` | `trim_horizon` \| `latest` \| `at_timestamp { timestamp_secs }` \| `at_sequence_number { sequence }` \| `after_sequence_number { sequence }`. `at_timestamp` matches at **second** granularity. |
| `shard_ids` | `[]` (all) | Explicit shard allowlist. |
| `include_closed` | `false` | Also drain closed (post-resharding) shards inside the retention window that have no bookmark. A closed shard **with** a bookmark is always drained. A fully-drained closed shard counts as *done*, not idle. |
| `poll_interval_ms` | `1000` | Base wait between `GetRecords` per shard; floored at 200 ms (the 5 reads/sec/shard API budget). |
| `records_per_request` | `500` | `GetRecords` limit (1–10000). |
| `shard_concurrency` | `4` | Maximum concurrent `GetRecords` calls. Every eligible shard is read, in turn, even when there are more shards than this. |
| `idle_termination_secs` / `max_messages` | — | **At least one is required** so a batch run terminates. |
| `value_format` | `json` | `json` (parse; invalid JSON fails with shard+sequence context) \| `string` (strict UTF-8) \| `bytes` (base64). |
| `batch_size` | `1000` | Records per emitted page. `0` = one page per drain cycle. |

## Record shape

```json
{
  "data": { "your": "payload" },
  "partition_key": "user-42",
  "sequence_number": "49593…",
  "shard_id": "shardId-000000000003",
  "approximate_arrival_timestamp_ms": 1716700000123
}
```

## Resume & checkpoints

State key: `kinesis:<stream_name>`. The bookmark is a per-shard map
`{ "shards": { "<shard-id>": "<last-sequence>" } }`, attached cumulatively to
**every** page — any page's bookmark is a valid resume point. On restart a
bookmarked shard resumes at `AFTER_SEQUENCE_NUMBER`; unbookmarked shards use
`start_position`.

**Resharding:** after a split or merge, a closed parent shard that has a
bookmark is drained before its children start, so its unread tail is never
dropped and a key's older records (in the parent) are written before its newer
ones (in the children). A child of a parent that was read starts at
`TRIM_HORIZON` rather than `start_position`, so nothing written to it after the
reshard is skipped. With `include_closed: true` unbookmarked closed shards are
read too, also parents first.

**Delivery is at-least-once.** A restart between a sink write and the bookmark
persist re-delivers records at the boundary — key downstream on
`sequence_number` (e.g. an upsert sink) when replays must converge.

## Throttling

`ProvisionedThroughputExceededException` (the 2 MB/s/shard read cap) triggers
exponential backoff with per-shard jitter and is never surfaced as an error.
Expired iterators are transparently re-acquired from the last-seen sequence.
Other request failures retry up to 8 times before failing the stream.

## LocalStack

```yaml
config:
  endpoint_url: http://localhost:4566
  region: us-east-1
  credentials: { type: access_key, config: { access_key_id: test, secret_access_key: test } }
```

## Source lag

`Source::lag` (#733) reports **seconds**: the worst shard's `MillisBehindLatest` from this run's `GetRecords` calls — or, before any, from one single-record probing read per shard at the resume position. The pipeline polls it on the first page, at most every
15 s after, and when the run ends, exporting `faucet_source_lag_*{pipeline,row,connector}`;
`faucet status --probe` and `faucet doctor` ask it from the stored bookmark,
and `sla.max_lag_*` thresholds turn it into an SLA. A failing lag query is
logged once and reported as no lag — it never fails a run.

## License

MIT OR Apache-2.0

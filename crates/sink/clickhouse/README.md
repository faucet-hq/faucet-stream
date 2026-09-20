# faucet-sink-clickhouse

ClickHouse **sink** for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Writes each page over the ClickHouse
[HTTP interface](https://clickhouse.com/docs/en/interfaces/http) as a batched
`INSERT … FORMAT JSONEachRow` request (the statement travels in the `query` URL
parameter, the newline-delimited JSON rows in the request body). Records are
type-round-tripped through `JSONEachRow`, which maps cleanly onto ClickHouse
column types.

## Configuration

```yaml
sink:
  kind: clickhouse
  config:
    # Endpoint — either `url` OR `host` (+ optional `http_port` / `tls`).
    url: http://localhost:8123
    # host: localhost
    # http_port: 8123
    # tls: false
    database: default
    user: default          # optional; sent as X-ClickHouse-User
    password: ${env:CH_PASSWORD}   # optional; sent as X-ClickHouse-Key
    table: events          # may be schema-qualified, e.g. analytics.events
    batch_size: 1000       # records per INSERT; 0 = whole page in one request
    async_insert: false    # true → server-side async buffering (async_insert=1)
    wait_for_async_insert: true  # wait for the flush ack (keeps at-least-once durability)
```

The `table` (including a `db.table` qualifier) is identifier-quoted before use,
so it is safe against injection.

### Authentication

Username + password (ClickHouse native HTTP auth), sent as the
`X-ClickHouse-User` / `X-ClickHouse-Key` headers.

### Asynchronous inserts

Set `async_insert: true` to enable ClickHouse
[asynchronous inserts](https://clickhouse.com/docs/en/optimize/asynchronous-inserts),
where the server buffers rows and flushes them in the background — a large
throughput win for many small inserts. Keep `wait_for_async_insert: true` (the
default) so the request is only acknowledged once the batch is durably accepted,
preserving faucet's at-least-once contract (the bookmark advances only after the
write is confirmed).

## Staged bulk load (`staging`)

With the `staging` feature and a `staging:` block, each page is uploaded to an
object store and the ClickHouse **server** pulls it with the `s3()` / `gcs()`
table function — `INSERT INTO t SELECT * FROM s3('<https-url>', …, '<Format>')`
— instead of streaming rows in the request body. Far more efficient for large
batches, and the server does the fetching.

```yaml
sink:
  type: clickhouse
  config:
    url: "http://clickhouse:8123"
    table: events
    staging:
      location: "s3://my-bucket/clickhouse-stage/"  # s3:// or gs://
      format: jsonl                                  # jsonl | csv
      compression: none
      cleanup: always
      region: us-east-1        # for the derived s3() URL (omit for gs://)
      # endpoint: minio:9000   # S3-compatible override (path-style)
      access_key: "${env:CH_STAGE_KEY}"   # creds the server uses to READ the stage
      secret_key: "${env:CH_STAGE_SECRET}"
```

Build with the crate's `staging` feature (CLI: `--features sink-clickhouse-staging`,
included in `full`). The load SQL/URL generation is unit-tested; the server-side
`s3()`/`gcs()` fetch itself requires a live ClickHouse + object store and is not
exercised in CI (same as the other staged-load sinks). Azure staging is not
supported by this path — stage to `s3://` or `gs://`.

## Write modes

The sink is **append-only**. ClickHouse upsert semantics are engine-dependent —
use a [`ReplacingMergeTree`](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
(or `CollapsingMergeTree` / `AggregatingMergeTree`) table to deduplicate by key
at merge time. The sink never emulates upsert, so `write_mode` must be `append`.


## Auto-create (`create_table`)

`create_table` (**default `true`**, #580) creates the target table from the
first written page's inferred columns when it does not exist — a first-ever
sync cannot assume the destination is already there. Every inferred column is
created **nullable**: a column present in page 1 is not required forever, and a
`NOT NULL` inferred from one page fails page 2 the first time a record omits
the field (narrowing later is the `schema:` drift policy's job). The table is created `MergeTree ORDER BY tuple()` — faucet has no basis to pick a sort key, so define the table yourself and set `create_table: false` when the sort key matters.

Set `create_table: false` to require a pre-existing target; a missing one then
fails fast with the same error every table sink raises, naming both ways out.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your
option.

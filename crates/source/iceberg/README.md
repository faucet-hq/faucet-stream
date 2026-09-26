# faucet-source-iceberg

Apache **Iceberg** source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Reads an Iceberg table through the same catalogs the [Iceberg sink](https://crates.io/crates/faucet-sink-iceberg) writes to (REST, AWS Glue, SQL-backed, Hive Metastore), scanning data files with [`iceberg-rust`](https://crates.io/crates/iceberg) into Arrow batches — with column projection, a pushed-down row filter, time travel, and **incremental reads between snapshots**.

Reach for it to move lakehouse data *out* of Iceberg — into a warehouse, a service database, a search index — or to treat an append-only Iceberg table as a change feed without extra infrastructure.

## Highlights

- **Projection** — `columns` reads only the listed columns.
- **Predicate pushdown** — `filter` is typed against the table schema and pushed into the scan as an Iceberg expression, so non-matching data files and row groups are skipped and rows are filtered exactly.
- **Time travel** — read a pinned `snapshot_id`, or the snapshot that was current at `as_of_timestamp`. Each snapshot is read with its own schema.
- **Incremental snapshot reads** — `mode: incremental` reads only the data files added by `append` snapshots since the bookmarked snapshot, one bookmark per snapshot. `overwrite` / `delete` snapshots and rolled-back history are detected (`on_rewrite`), expired bookmarks too (`on_expired`).
- **Row-level deletes are never ignored** — Parquet position and equality delete files are applied during the scan. A delete file iceberg-rust cannot apply (e.g. a v3 deletion vector) makes the source refuse the table rather than return deleted rows.
- **Columnar fast path** — with the `arrow` feature, an `iceberg → parquet` (or any Arrow-capable sink) chain moves `RecordBatch`es end to end with no per-row `Value`.
- **Discovery** — `faucet discover` lists every namespace and table with its schema and row count.
- **Sharding** — clustered runs split a table by data file (hash of the file path).

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `catalog` | `CatalogConfig` | — *(required)* | Catalog type + connection settings — identical to the sink's [catalog block](https://crates.io/crates/faucet-sink-iceberg#catalog-config). |
| `table` | string | — *(required)* | `namespace.table`; multi-level namespaces are dot-separated (`lake.analytics.events`). |
| `columns` | `[string]` | `[]` (all) | Projection. |
| `filter` | string | none | Row filter pushed into the scan (grammar below). |
| `snapshot_id` | int | none | Time travel to this snapshot. `mode: full` only; exclusive with `as_of_timestamp`. |
| `as_of_timestamp` | string | none | Time travel to the snapshot current at this RFC 3339 instant. `mode: full` only. |
| `batch_size` | int | `1000` | Rows per page (Arrow batch size). `0` emits one page per snapshot read. |
| `concurrency` | int | `4` | Data files read concurrently (must be > 0). |
| `mode` | `full` \| `incremental` | `full` | `full` re-reads the table each run; `incremental` reads only appended data since the bookmark. |
| `on_rewrite` | `fail` \| `full_refresh` | `fail` | Incremental: an `overwrite` / `delete` snapshot since the bookmark, or a bookmark that is no longer an ancestor of the current snapshot. |
| `on_expired` | `fail` \| `full_refresh` | `fail` | Incremental: the bookmarked snapshot (or one after it) was expired from the metadata. |

`full_refresh` re-reads the current snapshot in full and moves the bookmark to it; rows already delivered are delivered again, so pair it with an upsert or overwrite sink.

### Filter grammar

```text
expr      := term ("or" term)*
term      := unary ("and" unary)*
unary     := "not" unary | "(" expr ")" | predicate
predicate := column ("=" | "==" | "!=" | "<>" | "<" | "<=" | ">" | ">=") literal
           | column ["not"] "in" "(" literal ("," literal)* ")"
           | column "is" ["not"] "null"
           | column ["not"] "starts_with" 'string'
literal   := integer | decimal | 'string' | "string" | true | false
```

Keywords are case-insensitive. Column names may be dot paths into structs (`address.city`) or back-quoted (`` `order id` ``). Literals are typed against the column: integers widen to `long` / `float` / `double` / `decimal` (rescaled, never rounded); dates, times, timestamps and UUIDs are written as strings (`dt = '2026-01-31'`, `ts >= '2026-01-31T00:00:00'`, `tz < '2026-01-31T00:00:00Z'`), and an integer compared with a timestamp column means epoch microseconds. A literal that cannot represent the column type (`int_col = 1.5`, a `decimal(10,2)` compared with `1.234`) is a config error, never a silent mismatch.

```yaml
filter: "status = 'active' and (amount >= 10.5 or vip is not null) and region in ('eu', 'us')"
```

## Incremental reads

The bookmark is the last processed snapshot id (`{"snapshot_id": 123}`), persisted under the state key `iceberg:<namespace>.<table>`:

1. **First run** (no bookmark): the current snapshot is read in full; the bookmark becomes its id.
2. **Later runs**: the snapshots between the bookmark and the current snapshot are walked through their parents. Each `append` snapshot's added data files are read — as of that snapshot — and its id is bookmarked once its rows are written. `replace` snapshots (compaction) change no data and are skipped.
3. A run with nothing new moves the bookmark forward without reading anything.

An `overwrite` / `delete` snapshot means rows changed that an append-only read would miss: the run fails (`on_rewrite: fail`) or re-reads the table (`full_refresh`). A bookmark that has been expired — or an intermediate snapshot that has — cannot be resolved to a set of appended files, so `on_expired` applies. Time travel and incremental mode are mutually exclusive.

```yaml
version: 1
name: lake-export
pipeline:
  source:
    type: iceberg
    config:
      catalog: { type: rest, uri: "http://localhost:8181", warehouse: "s3://warehouse/" }
      table: analytics.events
      columns: [id, ts, user_id, amount]
      filter: "amount > 0"
      mode: incremental
      on_rewrite: full_refresh
  sink:
    type: jsonl
    config: { path: ./events.jsonl, append: true }
  state:
    type: file
    config: { path: ./state }
```

## Time travel

```yaml
source:
  type: iceberg
  config:
    catalog: { type: glue, warehouse: "s3://lake/warehouse" }
    table: lake.orders
    as_of_timestamp: "2026-06-30T23:59:59Z"   # or: snapshot_id: 5872839458372
```

`as_of_timestamp` resolves the newest ancestor of the current snapshot committed at or before the instant. A snapshot that has been expired is an error naming it.

## Row-level deletes

iceberg-rust applies Parquet **position** and **equality** delete files during the scan, so deleted rows never reach the sink. Before reading, the source checks every delete file attached to each data file and refuses the read — naming both files — when one is not a Parquet delete file (for example a v3 deletion vector stored in Puffin) or is an equality delete without equality field ids.

## Sharding and discovery

`enumerate_shards(n)` plans `n` hash shards; each worker keeps the data files whose path hashes to its shard, so the shards partition the table's files exactly (incremental reads shard the same way). `discover()` walks every namespace (recursively where the catalog supports nested namespaces) and returns one descriptor per table: `name` `namespace.table`, a JSON Schema derived from the Iceberg schema, the current snapshot's `total-records` as the row estimate, and `config_patch: { table: <namespace.table> }`.

## Preflight (`faucet doctor`)

`check()` loads the table metadata (no data read), then verifies every `columns` entry exists and that `filter` types against the current schema.

## Library usage

```rust,ignore
use faucet_core::Source;
use faucet_source_iceberg::{IcebergSource, IcebergSourceConfig};

let config: IcebergSourceConfig = serde_json::from_value(serde_json::json!({
    "catalog": { "type": "rest", "uri": "http://localhost:8181" },
    "table": "analytics.events",
    "filter": "id > 100",
}))?;
let source = IcebergSource::new(config).await?;
let rows = source.fetch_all().await?;
```

`IcebergSource::with_catalog(config, catalog)` reuses an already-connected `Arc<dyn iceberg::Catalog>`.

## Lineage dataset URI

`iceberg://<catalog_type>/<namespace>.<table>` — the same URI the Iceberg sink reports, so a sink → source hop joins up in lineage.

## Feature flags

| Feature | Default? | Enables |
|---|---|---|
| `catalog-rest` | **yes** | REST catalog |
| `catalog-glue` | no | AWS Glue catalog (+ `storage-opendal`) |
| `catalog-sql` | no | SQL-backed catalog (+ `storage-opendal`) |
| `catalog-hms` | no | Hive Metastore catalog (+ `storage-opendal`) |
| `storage-opendal` | no (auto) | OpenDAL S3 / GCS / local warehouse storage for the non-REST catalogs |
| `arrow` | no | Columnar fast path (`supports_columnar` / `stream_batches`) |

The catalog features forward to [`faucet-common-iceberg`](https://crates.io/crates/faucet-common-iceberg), the same crate the sink uses, so both connectors gate catalogs identically.

## License

MIT OR Apache-2.0.

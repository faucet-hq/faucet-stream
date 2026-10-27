# Embedding faucet as a Rust library

The `faucet` CLI is a thin wrapper over the same library you can use directly.
Embedding gives you typed configs, compile-time connector selection, and the
ability to build a `Source` or `Sink` from your own code.

## Add the dependency

```toml
[dependencies]
faucet-stream = { version = "1.0", features = ["source-rest", "sink-bigquery"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Build and run a pipeline

```rust,ignore
use faucet_stream::source::rest::{RestStream, RestStreamConfig, Auth, PaginationStyle};
use faucet_stream::sink::bigquery::{BigQuerySink, BigQuerySinkConfig};
use faucet_stream::Pipeline;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(RestStreamConfig {
        base_url: "https://api.example.com".into(),
        path: "/v1/events".into(),
        auth: Auth::Bearer { token: std::env::var("API_TOKEN")? },
        ..Default::default()
    })?;

    let sink = BigQuerySink::new(/* BigQuerySinkConfig { .. } */).await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("moved {} records", result.records_written);
    Ok(())
}
```

> Exact field names and constructors are documented per crate on
> [docs.rs](https://docs.rs/faucet-stream) (rendered with all features, so every
> connector's API is visible). Treat the snippet above as the shape, not the
> literal field list.

## Applying transforms

`faucet_stream::TransformingSource` is the library entry point for attaching
transforms to any source. It wraps a `Box<dyn Source>` with a flat list of
`RecordTransform`s applied to every record emitted via `fetch_*` and
`stream_pages`.

```rust,ignore
use faucet_stream::{
    KeyCaseMode, KeyCollision, Labels, RecordTransform, Source, TransformingSource,
};

let inner: Box<dyn Source> = Box::new(my_source);
let source = TransformingSource::new(
    inner,
    vec![
        RecordTransform::Flatten { separator: "__".into() },
        RecordTransform::KeysCase {
            mode: KeyCaseMode::Snake,
            on_collision: KeyCollision::Error,
            suffix_keys: vec![],
        },
        RecordTransform::custom(|mut record| {
            if let serde_json::Value::Object(ref mut map) = record {
                map.insert("_ingested_at".into(), serde_json::json!("2026-05-28T00:00:00Z"));
            }
            record
        }),
    ],
    Labels::for_named("my-source"),
)?;
// `source` is now a `Source` that streams the inner source's pages with
// transforms applied per page — memory stays bounded by `batch_size` even on
// large result sets.
```

Transforms compile eagerly inside `new()` — an invalid regex in `RenameKeys`
surfaces immediately as `FaucetError::Transform`, not at first record.

`Labels::for_named(name)` is the convenient constructor for library callers
(the CLI uses its own `Labels` carrying the pipeline / row / run-id triple).
The wrapper emits `faucet_transform_records_in_total` /
`faucet_transform_records_out_total` (use the `out/in` ratio for filter drop
rate or explode fan-out), `faucet_transform_duration_seconds`, and
`faucet_transform_errors_total` per page through the standard observability
stack.

For configuration-driven users (the `faucet` binary), transforms are declared
in YAML — see the [transforms cookbook](../cookbook/transforms.md) for the
three-layer model and per-layer opt-out.

## Durable state and streaming

Wire a state store for resumable runs, and use the streaming entry point when you
want to control batching explicitly:

```rust,ignore
use std::sync::Arc;
use faucet_stream::{Pipeline, FileStateStore};

let state = Arc::new(FileStateStore::new("./state")?);
let result = Pipeline::new(&source, &sink)
    .with_state_store(state)
    .run()
    .await?;
```

The pipeline reads the bookmark before fetching and persists a new one only after
the sink confirms each page — so a crash never loses unwritten data.

## Buffered fetch vs. native streaming

`Pipeline::run` (and `run_stream`) always drives the source through
`Source::stream_pages` and writes each `StreamPage` as it arrives, so the **sink**
side is bounded at `O(batch_size)` regardless of which source you use. What differs is
the **source** side:

- Sources that **override `stream_pages`** (databases, CDC, object stores in
  JSONL/raw-text mode, Parquet, Kafka, …) read incrementally from their primitive —
  peak memory stays at `O(batch_size)` end to end.
- Sources that use the **default `stream_pages`** implement only `fetch_with_context`;
  the default buffers the whole result via `fetch_all` and then chunks it — correct, but
  peak memory is the full result set on the source side.

When you implement a custom `Source`, override `stream_pages` if your backend can page
natively; otherwise implement just `fetch_with_context` and inherit the buffered
default. The per-connector breakdown is in the
[connector catalog](../reference/connectors.md#streaming-native-vs-buffered).

## Why embed instead of shelling out to the CLI?

- **Typed configs** — config structs implement `serde` + `JsonSchema`, so you get
  compile-time checking and can generate UIs/forms from the schema.
- **Custom connectors** — implement the `Source` / `Sink` traits for systems we
  don't ship, and run them through the same `Pipeline`. See
  [authoring a connector](../extending/authoring-connectors.md).
- **One process** — no subprocess, no temp config files; integrate pipelines into
  an existing service, job runner, or test harness.

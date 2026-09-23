# Summary

[Introduction](./introduction.md)

# Getting Started

- [Installation](./getting-started/installation.md)
- [Your first pipeline](./getting-started/first-pipeline.md)
- [Try it locally (interactive demo)](./getting-started/try-it-locally.md)
- [Core concepts](./getting-started/concepts.md)
- [Learn the architecture (interactive)](./getting-started/learn.md)

# Tutorials

- [REST API → BigQuery (incremental)](./tutorials/rest-to-bigquery.md)
- [PostgreSQL CDC → JSONL](./tutorials/postgres-cdc.md)
- [Multi-pipeline DAGs with `matrix`](./tutorials/matrix-dag.md)
- [Embedding faucet as a Rust library](./tutorials/library.md)

# Cookbook

- [Shaping the data]()
  - [Record transforms](./cookbook/transforms.md)
  - [SQL transform](./cookbook/sql-transform.md)
  - [WASM transform (custom code)](./cookbook/wasm-transforms.md)
  - [Topology mode (tee / merge / join)](./cookbook/topology.md)
- [Reading from sources]()
  - [Pagination styles](./cookbook/pagination.md)
  - [Authentication](./cookbook/auth.md)
  - [Source discovery (auto-generate configs)](./cookbook/discover.md)
  - [Airtable (via REST source)](./cookbook/airtable.md)
- [Moving data reliably]()
  - [Incremental replication & state](./cookbook/state.md)
  - [Upsert / mirror tables](./cookbook/upsert.md)
  - [Replication (snapshot → CDC)](./cookbook/replication.md)
  - [Backfill (historical replay)](./cookbook/backfill.md)
  - [Parallel range partitioning](./cookbook/partitioning.md)
  - [Dead-letter queues](./cookbook/dlq.md)
  - [Resilience (retry / circuit breaker / poison-pill)](./cookbook/resilience.md)
  - [File formats](./cookbook/file-formats.md)
  - [Compression](./cookbook/compression.md)
- [Data quality & governance]()
  - [Data-quality checks](./cookbook/quality.md)
  - [Data contracts](./cookbook/contracts.md)
  - [PII detection & masking](./cookbook/masking.md)
  - [Schema drift](./cookbook/schema-drift.md)
- [Config & reuse]()
  - [Config composition](./cookbook/composition.md)
  - [Parameters & pipeline templates](./cookbook/templates.md)
  - [Template Hub: source × sink templates](./cookbook/template-hub.md)
  - [Secrets-manager interpolation](./cookbook/secrets.md)
  - [Adaptive batching](./cookbook/adaptive-batching.md)
  - [Throughput tuning](./cookbook/tuning.md)
  - [Testing pipelines](./cookbook/testing.md)
- [Observability & lineage]()
  - [SLA monitoring (freshness & volume)](./cookbook/sla.md)
  - [Notifications (Slack / PagerDuty / webhook)](./cookbook/notifications.md)
  - [Lineage (OpenLineage)](./cookbook/lineage.md)
  - [Dashboards & alerts](./cookbook/dashboards.md)
  - [Data Movement Catalog](./cookbook/catalog.md)

# Reference

- [Connector catalog](./reference/connectors.md)
- [Connector conformance & tiers](./reference/conformance.md)
- [Connector capability matrix](./reference/capability-matrix.md)
- [Choosing a connector](./reference/choosing.md)
- [CLI commands](./reference/cli.md)
- [Template Hub — source × sink matrix](./reference/template-hub-matrix.md)
- [Configuration file format](./reference/config.md)
- [Editor setup (autocomplete & validation)](./reference/editor-setup.md)
- [HTTP API (`faucet serve`)](./reference/http-api.md)
- [Triggers](./reference/triggers.md)

# Comparisons

- [How faucet-stream compares](./comparison/index.md)
- [Benchmarks (vs Meltano)](./comparison/benchmarks.md)
- [vs. Meltano (Singer)](./comparison/meltano.md)
- [vs. Airbyte](./comparison/airbyte.md)
- [vs. Singer](./comparison/singer.md)
- [vs. Redpanda Connect (Benthos)](./comparison/redpanda-connect.md)
- [vs. Vector](./comparison/vector.md)
- [vs. Fivetran](./comparison/fivetran.md)

# Operations

- [Deploying faucet](./operations/deploying.md)
- [Running faucet as a service](./cookbook/serve.md)
- [Web console (`serve-ui`)](./cookbook/web-console.md)
- [MCP server (agent tools)](./cookbook/mcp.md)
- [Scheduling](./cookbook/scheduling.md)
- [Event-driven triggers](./cookbook/triggers.md)
- [Orchestration (Airflow / Dagster + dbt)](./cookbook/orchestration.md)
- [Running a cluster](./cookbook/cluster.md)
- [Observability](./operations/observability.md)
- [Reliability testing](./operations/reliability-testing.md)
- [Performance tuning](./operations/tuning.md)
- [Troubleshooting with `faucet doctor`](./cookbook/troubleshooting.md)
- [Troubleshooting & FAQ](./operations/troubleshooting.md)

# Extending

- [Authoring a connector](./extending/authoring-connectors.md)
- [Connector protocol (FCP v0)](./spec/faucet-connector-spec-v0.md)
- [Connector marketplace](./extending/marketplace.md)

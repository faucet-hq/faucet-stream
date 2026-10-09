# Project structure

Where everything lives in the faucet-stream repository. For how the pieces fit
together at run time, see the [architecture guide](docs/architecture/README.md);
to start contributing, see [CONTRIBUTING.md](CONTRIBUTING.md).

The workspace has <!--COUNT:crates-->111<!--/COUNT--> crates: <!--COUNT:sources-->43<!--/COUNT-->
source connectors, <!--COUNT:sinks-->35<!--/COUNT--> sink connectors and
<!--COUNT:common-->23<!--/COUNT--> shared connector libraries, plus the core, auth, state,
lineage, transform and test crates below.

```
Cargo.toml                    — workspace manifest
crates/
  core/                       — faucet-core: shared types, traits, pipeline, transforms, config
  auth/                       — faucet-auth: shared OAuth2 / token-endpoint providers
  source/                     — source connectors (rest, graphql, xml, grpc, *-cdc, kafka, s3, azure-blob, redshift, clickhouse, pubsub, delta, databricks, iceberg, dynamodb, oracle, oracle-cdc, singer, duckdb, sqs, nats, rabbitmq, sftp, file, …)
  sink/                       — sink connectors (bigquery, iceberg, delta, databricks, dynamodb, oracle, postgres, parquet, kafka, redshift, clickhouse, pubsub, azure-blob, duckdb, sqs, nats, rabbitmq, sftp, singer, …)
  common/                     — shared connector libraries (bigquery, elasticsearch, gcs, kafka, snowflake, mssql, kinesis, spanner, delta, redshift, pubsub, clickhouse, azure, sqs, nats, rabbitmq, sftp, iceberg, dynamodb, databricks, oracle, singer, file)
  state/                      — Redis- and Postgres-backed StateStore backends
  lineage/                    — faucet-lineage: OpenLineage event emission
  transform-sql/              — faucet-transform-sql: embedded DuckDB SQL transform
  transform-wasm/             — faucet-transform-wasm: WebAssembly (wasmtime) per-record transform
  conformance/                — faucet-conformance: connector conformance harness + engine reliability suites
  interop-tests/              — unpublished: tests that need two connectors (round trips, fidelity)
faucet-stream/                — umbrella crate with feature-gated re-exports
cli/                          — faucet-cli: `faucet` binary, YAML/JSON pipeline runner
  examples/                   — ready-to-run pipeline YAMLs
  tests/                      — assert_cmd + wiremock + testcontainers integration tests
hub/                          — Template Hub: sink templates + example source templates
schemas/                      — committed JSON Schema for pipeline configs (editor validation)
examples/                     — repo-level examples: docker-compose infra stack + run index
  orchestration/              — ELT recipe: faucet (EL) + dbt (T) + Airflow/Dagster
benchmarks/                   — benchmark harness and configs
observability/                — Prometheus alert rules and Grafana dashboards
Dockerfile                    — multi-stage image build (name-based connector selection)
deploy/                       — container + Kubernetes assets
  helm/faucet-stream/         — Helm chart (serve Deployment and/or run Job/CronJob)
  helm/faucet-stream/ci/      — chart test values (`# expect:` lines; ci/fail/ = must be refused)
  helm/faucet-stream/examples/ — example values (everything.yaml: every serve feature on)
  helm/test-chart.sh          — renders the chart test values and checks their expectations
  otel/                       — log-shipping recipes (Alloy → Loki, OTel Collector → S3/GCS/Azure)
scripts/                      — helper and CI-check scripts (try-local.sh, build-image.sh, …)
docs/book/                    — mdBook documentation site (source under docs/book/src)
rfcs/                         — design RFCs
.github/workflows/            — ci.yml, release-plz.yml, docs.yml, docker-images.yml, …
.github/assets/               — brand assets: logo, wordmark, social-preview banner, favicon
```

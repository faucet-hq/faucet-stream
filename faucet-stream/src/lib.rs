#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-stream
//!
//! A declarative, config-driven data pipeline with pluggable source and sink
//! connectors.
//!
//! 📖 **Guide, tutorials & cookbook:** <https://faucet-hq.github.io/faucet-stream/>
//!
//! ## Feature flags
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `source-rest` *(default)* | REST API source with pagination, auth, transforms |
//! | `source-graphql` | GraphQL API source with cursor pagination |
//! | `source-xml` | XML/SOAP API source with XML-to-JSON conversion |
//! | `source-grpc` | gRPC source with dynamic protobuf messages |
//! | `source-postgres` | PostgreSQL query source |
//! | `source-postgres-cdc` | PostgreSQL CDC source (logical replication) |
//! | `source-mysql` | MySQL query source |
//! | `source-mssql` | Microsoft SQL Server query source |
//! | `source-sqlite` | SQLite query source |
//! | `source-duckdb` | DuckDB query source |
//! | `source-sqs` | AWS SQS source |
//! | `source-nats` | NATS source |
//! | `source-rabbitmq` | RabbitMQ (AMQP 0.9.1) queue source |
//! | `source-sftp` | SFTP source |

//! | `source-s3` | AWS S3 file source |
//! | `source-mongodb` | MongoDB query source |
//! | `source-mongodb-cdc` | MongoDB CDC source (Change Streams) |
//! | `source-mysql-cdc` | MySQL CDC source (binlog replication) |
//! | `source-redis` | Redis source (streams, lists, keys) |
//! | `source-webhook` | Webhook HTTP receiver source |
//! | `source-websocket` | WebSocket streaming source |
//! | `source-csv` | CSV file source |
//! | `source-elasticsearch` | Elasticsearch search/scroll source |
//! | `source-kafka` | Apache Kafka consumer source |
//! | `source-kinesis` | AWS Kinesis Data Streams source |
//! | `source-spanner` | Google Cloud Spanner query source |
//! | `source-parquet` | Apache Parquet file source (local, glob, S3) |
//! | `source-delta` | Apache Delta Lake source (local FS or S3/Azure/GCS, time travel) |
//! | `source-databricks` | Databricks SQL query source (Statement Execution API) |
//! | `source-iceberg` | Apache Iceberg table source (REST/Glue/SQL/HMS catalogs, time travel, incremental) |
//! | `source-dynamodb` | Amazon DynamoDB source (scan, query, Streams CDC) |
//! | `sink-bigquery` | Google BigQuery streaming insert sink |
//! | `sink-iceberg` | Apache Iceberg sink (append-only, REST/Glue/SQL/HMS catalogs) |
//! | `sink-postgres` | PostgreSQL sink (jsonb or auto-mapped columns) |
//! | `sink-jsonl` | JSON Lines file sink |
//! | `sink-snowflake` | Snowflake SQL REST API sink |
//! | `sink-mysql` | MySQL sink |
//! | `sink-mssql` | Microsoft SQL Server sink |
//! | `sink-sqlite` | SQLite sink |
//! | `sink-duckdb` | DuckDB sink |
//! | `sink-sqs` | AWS SQS sink |
//! | `sink-nats` | NATS sink |
//! | `sink-rabbitmq` | RabbitMQ (AMQP 0.9.1) publish sink |
//! | `sink-sftp` | SFTP sink |

//! | `sink-s3` | AWS S3 file sink |
//! | `sink-mongodb` | MongoDB insert sink |
//! | `sink-redis` | Redis sink (streams, lists, key-value) |
//! | `sink-csv` | CSV file sink |
//! | `sink-elasticsearch` | Elasticsearch bulk index sink |
//! | `sink-http` | HTTP POST sink |
//! | `sink-kafka` | Apache Kafka producer sink |
//! | `sink-kinesis` | AWS Kinesis Data Streams sink |
//! | `sink-spanner` | Google Cloud Spanner mutation sink |
//! | `sink-parquet` | Apache Parquet file sink (local, S3) |
//! | `encryption` | AES-256-GCM at-rest sealing for file state-store bookmarks and per-line JSONL/DLQ output |
//! | `sink-dynamodb` | Amazon DynamoDB sink (batched writes, upsert/delete) |
//! | `sink-databricks` | Databricks SQL warehouse sink (append/upsert/overwrite, exactly-once) |
//! | `sink-delta` | Apache Delta Lake sink (append-only; local FS or S3/Azure/GCS) |
//! | `kafka-schema-registry` | Schema Registry support for Kafka connectors |
//! | `source` | All source connectors |
//! | `sink` | All sink connectors |
//! | `full` | Every connector |

// Always re-export core types and traits.
pub use faucet_core::*;

// Explicit re-exports for the library-side transforms wrapper and observability
// labels so users can import them via the umbrella path.
pub use faucet_core::TransformingSource;
pub use faucet_core::observability::Labels;

// ── Shared auth providers ────────────────────────────────────────────────────
/// Single-flight OAuth2 / token-endpoint auth providers (enable the `auth`
/// feature). Share one across connectors via `with_auth_provider` or the CLI
/// `auth: { ref }` catalog.
#[cfg(feature = "auth")]
pub mod auth {
    pub use faucet_auth::*;
}

// ── Source connectors ────────────────────────────────────────────────────────

#[cfg(feature = "source-rest")]
pub mod source {
    pub mod rest {
        pub use faucet_source_rest::*;
    }

    #[cfg(feature = "source-graphql")]
    pub mod graphql {
        pub use faucet_source_graphql::*;
    }

    #[cfg(feature = "source-xml")]
    pub mod xml {
        pub use faucet_source_xml::*;
    }

    #[cfg(feature = "source-grpc")]
    pub mod grpc {
        pub use faucet_source_grpc::*;
    }

    #[cfg(feature = "source-postgres")]
    pub mod postgres {
        pub use faucet_source_postgres::*;
    }

    #[cfg(feature = "source-postgres-cdc")]
    pub mod postgres_cdc {
        pub use faucet_source_postgres_cdc::*;
    }

    #[cfg(feature = "source-mysql")]
    pub mod mysql {
        pub use faucet_source_mysql::*;
    }

    #[cfg(feature = "source-mssql")]
    pub mod mssql {
        pub use faucet_source_mssql::*;
    }

    #[cfg(feature = "source-sqlite")]
    pub mod sqlite {
        pub use faucet_source_sqlite::*;
    }

    #[cfg(feature = "source-duckdb")]
    pub mod duckdb {
        pub use faucet_source_duckdb::*;
    }

    #[cfg(feature = "source-sqs")]
    pub mod sqs {
        pub use faucet_source_sqs::*;
    }

    #[cfg(feature = "source-nats")]
    pub mod nats {
        pub use faucet_source_nats::*;
    }

    #[cfg(feature = "source-rabbitmq")]
    pub mod rabbitmq {
        pub use faucet_source_rabbitmq::*;
    }

    #[cfg(feature = "source-sftp")]
    pub mod sftp {
        pub use faucet_source_sftp::*;
    }

    #[cfg(feature = "source-s3")]
    pub mod s3 {
        pub use faucet_source_s3::*;
    }

    #[cfg(feature = "source-mongodb")]
    pub mod mongodb {
        pub use faucet_source_mongodb::*;
    }

    #[cfg(feature = "source-mongodb-cdc")]
    pub mod mongodb_cdc {
        pub use faucet_source_mongodb_cdc::*;
    }

    #[cfg(feature = "source-mysql-cdc")]
    pub mod mysql_cdc {
        pub use faucet_source_mysql_cdc::*;
    }

    #[cfg(feature = "source-redis")]
    pub mod redis {
        pub use faucet_source_redis::*;
    }

    #[cfg(feature = "source-webhook")]
    pub mod webhook {
        pub use faucet_source_webhook::*;
    }

    #[cfg(feature = "source-websocket")]
    pub mod websocket {
        pub use faucet_source_websocket::*;
    }

    #[cfg(feature = "source-csv")]
    pub mod csv {
        pub use faucet_source_csv::*;
    }

    #[cfg(feature = "source-elasticsearch")]
    pub mod elasticsearch {
        pub use faucet_source_elasticsearch::*;
    }

    #[cfg(feature = "source-kafka")]
    pub mod kafka {
        pub use faucet_source_kafka::*;
    }

    #[cfg(feature = "source-kinesis")]
    pub mod kinesis {
        pub use faucet_source_kinesis::*;
    }

    #[cfg(feature = "source-bigquery")]
    pub mod bigquery {
        pub use faucet_source_bigquery::*;
    }

    #[cfg(feature = "source-snowflake")]
    pub mod snowflake {
        pub use faucet_source_snowflake::*;
    }

    #[cfg(feature = "source-spanner")]
    pub mod spanner {
        pub use faucet_source_spanner::*;
    }

    #[cfg(feature = "source-parquet")]
    pub mod parquet {
        pub use faucet_source_parquet::*;
    }

    #[cfg(feature = "source-delta")]
    pub mod delta {
        pub use faucet_source_delta::*;
    }

    #[cfg(feature = "source-databricks")]
    pub mod databricks {
        pub use faucet_source_databricks::*;
    }

    #[cfg(feature = "source-iceberg")]
    pub mod iceberg {
        pub use faucet_source_iceberg::*;
    }

    #[cfg(feature = "source-dynamodb")]
    pub mod dynamodb {
        pub use faucet_source_dynamodb::*;
    }

    #[cfg(feature = "source-gcs")]
    pub mod gcs {
        pub use faucet_source_gcs::*;
    }

    #[cfg(feature = "source-mssql-cdc")]
    pub mod mssql_cdc {
        pub use faucet_source_mssql_cdc::*;
    }

    #[cfg(feature = "source-redshift")]
    pub mod redshift {
        pub use faucet_source_redshift::*;
    }

    #[cfg(feature = "source-pubsub")]
    pub mod pubsub {
        pub use faucet_source_pubsub::*;
    }

    #[cfg(feature = "source-clickhouse")]
    pub mod clickhouse {
        pub use faucet_source_clickhouse::*;
    }

    #[cfg(feature = "source-azure-blob")]
    pub mod azure_blob {
        pub use faucet_source_azure_blob::*;
    }

    #[cfg(feature = "source-singer")]
    pub mod singer {
        pub use faucet_source_singer::*;
    }
}

// Source modules available without source-rest (when only other sources are enabled).
#[cfg(not(feature = "source-rest"))]
pub mod source {
    #[cfg(feature = "source-graphql")]
    pub mod graphql {
        pub use faucet_source_graphql::*;
    }

    #[cfg(feature = "source-xml")]
    pub mod xml {
        pub use faucet_source_xml::*;
    }

    #[cfg(feature = "source-grpc")]
    pub mod grpc {
        pub use faucet_source_grpc::*;
    }

    #[cfg(feature = "source-postgres")]
    pub mod postgres {
        pub use faucet_source_postgres::*;
    }

    #[cfg(feature = "source-postgres-cdc")]
    pub mod postgres_cdc {
        pub use faucet_source_postgres_cdc::*;
    }

    #[cfg(feature = "source-mysql")]
    pub mod mysql {
        pub use faucet_source_mysql::*;
    }

    #[cfg(feature = "source-mssql")]
    pub mod mssql {
        pub use faucet_source_mssql::*;
    }

    #[cfg(feature = "source-sqlite")]
    pub mod sqlite {
        pub use faucet_source_sqlite::*;
    }

    #[cfg(feature = "source-duckdb")]
    pub mod duckdb {
        pub use faucet_source_duckdb::*;
    }

    #[cfg(feature = "source-sqs")]
    pub mod sqs {
        pub use faucet_source_sqs::*;
    }

    #[cfg(feature = "source-nats")]
    pub mod nats {
        pub use faucet_source_nats::*;
    }

    #[cfg(feature = "source-rabbitmq")]
    pub mod rabbitmq {
        pub use faucet_source_rabbitmq::*;
    }

    #[cfg(feature = "source-sftp")]
    pub mod sftp {
        pub use faucet_source_sftp::*;
    }

    #[cfg(feature = "source-s3")]
    pub mod s3 {
        pub use faucet_source_s3::*;
    }

    #[cfg(feature = "source-mongodb")]
    pub mod mongodb {
        pub use faucet_source_mongodb::*;
    }

    #[cfg(feature = "source-mongodb-cdc")]
    pub mod mongodb_cdc {
        pub use faucet_source_mongodb_cdc::*;
    }

    #[cfg(feature = "source-mysql-cdc")]
    pub mod mysql_cdc {
        pub use faucet_source_mysql_cdc::*;
    }

    #[cfg(feature = "source-redis")]
    pub mod redis {
        pub use faucet_source_redis::*;
    }

    #[cfg(feature = "source-webhook")]
    pub mod webhook {
        pub use faucet_source_webhook::*;
    }

    #[cfg(feature = "source-websocket")]
    pub mod websocket {
        pub use faucet_source_websocket::*;
    }

    #[cfg(feature = "source-csv")]
    pub mod csv {
        pub use faucet_source_csv::*;
    }

    #[cfg(feature = "source-elasticsearch")]
    pub mod elasticsearch {
        pub use faucet_source_elasticsearch::*;
    }

    #[cfg(feature = "source-kafka")]
    pub mod kafka {
        pub use faucet_source_kafka::*;
    }

    #[cfg(feature = "source-kinesis")]
    pub mod kinesis {
        pub use faucet_source_kinesis::*;
    }

    #[cfg(feature = "source-bigquery")]
    pub mod bigquery {
        pub use faucet_source_bigquery::*;
    }

    #[cfg(feature = "source-snowflake")]
    pub mod snowflake {
        pub use faucet_source_snowflake::*;
    }

    #[cfg(feature = "source-spanner")]
    pub mod spanner {
        pub use faucet_source_spanner::*;
    }

    #[cfg(feature = "source-parquet")]
    pub mod parquet {
        pub use faucet_source_parquet::*;
    }

    #[cfg(feature = "source-delta")]
    pub mod delta {
        pub use faucet_source_delta::*;
    }

    #[cfg(feature = "source-databricks")]
    pub mod databricks {
        pub use faucet_source_databricks::*;
    }

    #[cfg(feature = "source-iceberg")]
    pub mod iceberg {
        pub use faucet_source_iceberg::*;
    }

    #[cfg(feature = "source-dynamodb")]
    pub mod dynamodb {
        pub use faucet_source_dynamodb::*;
    }

    #[cfg(feature = "source-gcs")]
    pub mod gcs {
        pub use faucet_source_gcs::*;
    }

    #[cfg(feature = "source-mssql-cdc")]
    pub mod mssql_cdc {
        pub use faucet_source_mssql_cdc::*;
    }

    #[cfg(feature = "source-redshift")]
    pub mod redshift {
        pub use faucet_source_redshift::*;
    }

    #[cfg(feature = "source-pubsub")]
    pub mod pubsub {
        pub use faucet_source_pubsub::*;
    }

    #[cfg(feature = "source-clickhouse")]
    pub mod clickhouse {
        pub use faucet_source_clickhouse::*;
    }

    #[cfg(feature = "source-azure-blob")]
    pub mod azure_blob {
        pub use faucet_source_azure_blob::*;
    }

    #[cfg(feature = "source-singer")]
    pub mod singer {
        pub use faucet_source_singer::*;
    }
}

// Backwards-compatible flat re-exports for existing users who depend on
// `faucet-stream::{RestStream, Auth, ...}` without the `source::rest::` path.
#[cfg(feature = "source-rest")]
pub use faucet_source_rest::{
    Auth, DEFAULT_EXPIRY_RATIO, DEFAULT_TOKEN_ENDPOINT_EXPIRY_RATIO, PaginationStyle,
    ResponseValidator, RestStream, RestStreamConfig, fetch_oauth2_token, fetch_token_from_endpoint,
};

// ── Sink connectors ──────────────────────────────────────────────────────────

pub mod sink {
    #[cfg(feature = "sink-bigquery")]
    pub mod bigquery {
        pub use faucet_sink_bigquery::*;
    }

    #[cfg(feature = "sink-iceberg")]
    pub mod iceberg {
        pub use faucet_sink_iceberg::*;
    }

    #[cfg(feature = "sink-postgres")]
    pub mod postgres {
        pub use faucet_sink_postgres::*;
    }

    #[cfg(feature = "sink-jsonl")]
    pub mod jsonl {
        pub use faucet_sink_jsonl::*;
    }

    #[cfg(feature = "sink-snowflake")]
    pub mod snowflake {
        pub use faucet_sink_snowflake::*;
    }

    #[cfg(feature = "sink-mysql")]
    pub mod mysql {
        pub use faucet_sink_mysql::*;
    }

    #[cfg(feature = "sink-mssql")]
    pub mod mssql {
        pub use faucet_sink_mssql::*;
    }

    #[cfg(feature = "sink-sqlite")]
    pub mod sqlite {
        pub use faucet_sink_sqlite::*;
    }

    #[cfg(feature = "sink-duckdb")]
    pub mod duckdb {
        pub use faucet_sink_duckdb::*;
    }

    #[cfg(feature = "sink-sqs")]
    pub mod sqs {
        pub use faucet_sink_sqs::*;
    }

    #[cfg(feature = "sink-nats")]
    pub mod nats {
        pub use faucet_sink_nats::*;
    }

    #[cfg(feature = "sink-rabbitmq")]
    pub mod rabbitmq {
        pub use faucet_sink_rabbitmq::*;
    }

    #[cfg(feature = "sink-sftp")]
    pub mod sftp {
        pub use faucet_sink_sftp::*;
    }

    #[cfg(feature = "sink-s3")]
    pub mod s3 {
        pub use faucet_sink_s3::*;
    }

    #[cfg(feature = "sink-mongodb")]
    pub mod mongodb {
        pub use faucet_sink_mongodb::*;
    }

    #[cfg(feature = "sink-redis")]
    pub mod redis {
        pub use faucet_sink_redis::*;
    }

    #[cfg(feature = "sink-csv")]
    pub mod csv {
        pub use faucet_sink_csv::*;
    }

    #[cfg(feature = "sink-elasticsearch")]
    pub mod elasticsearch {
        pub use faucet_sink_elasticsearch::*;
    }

    #[cfg(feature = "sink-http")]
    pub mod http {
        pub use faucet_sink_http::*;
    }

    #[cfg(feature = "sink-stdout")]
    pub mod stdout {
        pub use faucet_sink_stdout::*;
    }

    #[cfg(feature = "sink-kafka")]
    pub mod kafka {
        pub use faucet_sink_kafka::*;
    }

    #[cfg(feature = "sink-kinesis")]
    pub mod kinesis {
        pub use faucet_sink_kinesis::*;
    }

    #[cfg(feature = "sink-spanner")]
    pub mod spanner {
        pub use faucet_sink_spanner::*;
    }

    #[cfg(feature = "sink-parquet")]
    pub mod parquet {
        pub use faucet_sink_parquet::*;
    }

    #[cfg(feature = "sink-delta")]
    pub mod delta {
        pub use faucet_sink_delta::*;
    }

    #[cfg(feature = "sink-gcs")]
    pub mod gcs {
        pub use faucet_sink_gcs::*;
    }

    #[cfg(feature = "sink-redshift")]
    pub mod redshift {
        pub use faucet_sink_redshift::*;
    }

    #[cfg(feature = "sink-pubsub")]
    pub mod pubsub {
        pub use faucet_sink_pubsub::*;
    }

    #[cfg(feature = "sink-clickhouse")]
    pub mod clickhouse {
        pub use faucet_sink_clickhouse::*;
    }

    #[cfg(feature = "sink-azure-blob")]
    pub mod azure_blob {
        pub use faucet_sink_azure_blob::*;
    }

    #[cfg(feature = "sink-dynamodb")]
    pub mod dynamodb {
        pub use faucet_sink_dynamodb::*;
    }

    #[cfg(feature = "sink-databricks")]
    pub mod databricks {
        pub use faucet_sink_databricks::*;
    }
}

// ── GCS common types ─────────────────────────────────────────────────────────

#[cfg(any(feature = "source-gcs", feature = "sink-gcs"))]
pub mod common_gcs {
    pub use faucet_common_gcs::*;
}

// ── Kafka common types ───────────────────────────────────────────────────────

#[cfg(any(feature = "source-kafka", feature = "sink-kafka"))]
pub mod common_kafka {
    pub use faucet_common_kafka::*;
}

/// Shared AWS Kinesis types (credentials enum, client builder), re-exported
/// for library callers when either Kinesis connector is enabled.
#[cfg(any(feature = "source-kinesis", feature = "sink-kinesis"))]
pub mod common_kinesis {
    pub use faucet_common_kinesis::*;
}

/// Shared Cloud Spanner types (credentials enum, connection block, value
/// conversion), re-exported for library callers when either Spanner
/// connector is enabled.
#[cfg(any(feature = "source-spanner", feature = "sink-spanner"))]
pub mod common_spanner {
    pub use faucet_common_spanner::*;
}

/// Shared Amazon Redshift types (credentials enum, connection block, pool
/// builder), re-exported when either Redshift connector is enabled.
#[cfg(any(feature = "source-redshift", feature = "sink-redshift"))]
pub mod common_redshift {
    pub use faucet_common_redshift::*;
}

/// Shared Google Cloud Pub/Sub types (credentials enum, connection block,
/// client builder), re-exported when either Pub/Sub connector is enabled.
#[cfg(any(feature = "source-pubsub", feature = "sink-pubsub"))]
pub mod common_pubsub {
    pub use faucet_common_pubsub::*;
}

/// Shared ClickHouse types (connection block, HTTP client builder), re-exported
/// when either ClickHouse connector is enabled.
#[cfg(any(feature = "source-clickhouse", feature = "sink-clickhouse"))]
pub mod common_clickhouse {
    pub use faucet_common_clickhouse::*;
}

/// Shared RabbitMQ types (connection/auth/TLS config, value formats, exchange
/// kinds), re-exported when either RabbitMQ connector is enabled.
#[cfg(any(feature = "source-rabbitmq", feature = "sink-rabbitmq"))]
pub mod common_rabbitmq {
    pub use faucet_common_rabbitmq::*;
}

/// Shared Azure Blob / ADLS Gen2 types (credentials enum, object-store builder),
/// re-exported when either Azure Blob connector is enabled.
#[cfg(any(feature = "source-azure-blob", feature = "sink-azure-blob"))]
pub mod common_azure {
    pub use faucet_common_azure::*;
}

/// Shared Amazon DynamoDB types (credentials enum, client builders, attribute
/// value conversion), re-exported when either DynamoDB connector is enabled.
#[cfg(any(feature = "source-dynamodb", feature = "sink-dynamodb"))]
pub mod common_dynamodb {
    pub use faucet_common_dynamodb::*;
}

/// Shared Databricks types (connection/auth config, SQL client), re-exported
/// when either Databricks connector is enabled.
#[cfg(any(feature = "source-databricks", feature = "sink-databricks"))]
pub mod common_databricks {
    pub use faucet_common_databricks::*;
}

// ── State-store backends ─────────────────────────────────────────────────────

pub mod state {
    #[cfg(feature = "state-redis")]
    pub mod redis {
        pub use faucet_state_redis::*;
    }

    #[cfg(feature = "state-postgres")]
    pub mod postgres {
        pub use faucet_state_postgres::*;
    }
}

// ── Lineage (OpenLineage emission) ───────────────────────────────────────────
/// OpenLineage event emission for pipeline runs (enable the `lineage` feature;
/// `lineage-kafka` adds the Kafka transport). The CLI wires this automatically
/// from a `lineage:` config block; library callers can build a
/// [`lineage::LineageEmitter`] directly.
#[cfg(feature = "lineage")]
pub use faucet_lineage as lineage;

// ── SQL transform (embedded DuckDB) ──────────────────────────────────────────
/// SQL-as-transform: run DuckDB SQL over each pipeline page (the `batch`
/// relation). Enable the `transform-sql` feature. The CLI wires this via the
/// `sql` transform; library callers build [`transform_sql::SqlTransform`] and
/// attach it with [`TransformingSource`].
#[cfg(feature = "transform-sql")]
pub use faucet_transform_sql as transform_sql;

// ── WASM transform (wasmtime) ────────────────────────────────────────────────
/// WebAssembly-as-transform: run a user-provided sandboxed `.wasm` module once
/// per record (issue #124). Enable the `transform-wasm` feature. The CLI wires
/// this via the `wasm` transform; library callers build
/// [`transform_wasm::WasmTransform`] and attach it with [`TransformingSource`].
#[cfg(feature = "transform-wasm")]
pub use faucet_transform_wasm as transform_wasm;

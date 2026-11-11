//! Every built-in connector that can authenticate with the identity of the
//! host it runs on answers which config fields select that, for each
//! credential variant, through the registry — and a connector whose client
//! builder refuses that identity reports the same fields its descriptor names.

use faucet_registry::{HostIdentity, sink_host_identity, source_host_identity};
use serde_json::{Value, json};

#[derive(Clone, Copy)]
enum Role {
    Source,
    Sink,
}

fn answer(role: Role, kind: &str, config: &Value) -> HostIdentity {
    match role {
        Role::Source => source_host_identity(kind, config),
        Role::Sink => sink_host_identity(kind, config),
    }
}

#[allow(dead_code)]
fn fields(role: Role, kind: &str, config: Value) -> Vec<String> {
    match answer(role, kind, &config) {
        HostIdentity::Fields(f) => f,
        other => panic!("{kind}: built-in connectors answer, got {other:?}"),
    }
}

#[allow(dead_code)]
fn region_from_env() -> bool {
    ["AWS_REGION", "AWS_DEFAULT_REGION"]
        .iter()
        .any(|k| std::env::var(k).is_ok_and(|v| !v.trim().is_empty()))
}

#[allow(dead_code)]
fn keys() -> Value {
    json!({"type": "access_key", "config": {"access_key_id": "a", "secret_access_key": "s"}})
}

#[allow(dead_code)]
fn aws_family(role: Role, kind: &str, extra: Value) {
    let with = |creds: Option<Value>, region: bool| {
        let mut c = extra.clone();
        if let Some(creds) = creds {
            c["credentials"] = creds;
        }
        if region {
            c["region"] = json!("us-east-1");
        }
        c
    };
    for variant in [
        None,
        Some(json!({"type": "default"})),
        Some(json!({"type": "web_identity"})),
        Some(json!({"type": "profile", "config": {"name": "p"}})),
        Some(json!({"type": "assume_role", "config": {"role_arn": "r"}})),
    ] {
        assert_eq!(
            fields(role, kind, with(variant.clone(), true)),
            vec!["credentials"],
            "{kind} {variant:?}"
        );
    }
    assert!(
        fields(role, kind, with(Some(keys()), true)).is_empty(),
        "{kind}"
    );
    assert_eq!(
        fields(role, kind, with(Some(keys()), false)).is_empty(),
        region_from_env(),
        "{kind}: a region comes from the config or the environment"
    );
}

#[test]
fn a_password_config_reports_no_host_identity() {
    #[cfg(feature = "source-redshift")]
    assert!(
        fields(
            Role::Source,
            "redshift",
            json!({"credentials": {"type": "password", "config": {"password": "p"}}})
        )
        .is_empty()
    );
    #[cfg(feature = "sink-redshift")]
    assert!(
        fields(
            Role::Sink,
            "redshift",
            json!({"credentials": {"type": "password", "config": {"password": "p"}},
                "write_strategy": "insert"})
        )
        .is_empty()
    );
}

#[test]
fn host_identity_follows_the_selected_credentials() {
    #[cfg(feature = "source-kinesis")]
    aws_family(Role::Source, "kinesis", json!({"stream_name": "s"}));
    #[cfg(feature = "sink-kinesis")]
    aws_family(Role::Sink, "kinesis", json!({"stream_name": "s"}));
    #[cfg(feature = "source-sqs")]
    aws_family(Role::Source, "sqs", json!({"queue_url": "q"}));
    #[cfg(feature = "sink-sqs")]
    aws_family(Role::Sink, "sqs", json!({"queue_url": "q"}));
    #[cfg(feature = "source-dynamodb")]
    aws_family(Role::Source, "dynamodb", json!({"table_name": "t"}));
    #[cfg(feature = "sink-dynamodb")]
    aws_family(Role::Sink, "dynamodb", json!({"table_name": "t"}));
    #[cfg(feature = "source-s3")]
    {
        aws_family(Role::Source, "s3", json!({"bucket": "b"}));
        let anon =
            json!({"bucket": "b", "region": "us-east-1", "credentials": {"type": "anonymous"}});
        assert!(fields(Role::Source, "s3", anon).is_empty());
    }
    #[cfg(feature = "sink-s3")]
    aws_family(Role::Sink, "s3", json!({"bucket": "b"}));
    #[cfg(feature = "source-parquet")]
    {
        let s3 = json!({"source": {"type": "s3", "bucket": "b", "region": "us-east-1"}});
        assert_eq!(
            fields(Role::Source, "parquet", s3),
            vec!["source.credentials"]
        );
        let local = json!({"source": {"type": "local_path", "path": "/tmp/x"}});
        assert!(fields(Role::Source, "parquet", local).is_empty());
    }
    #[cfg(feature = "sink-redshift")]
    {
        let staged = json!({"credentials": {"type": "password", "config": {"password": "p"}},
            "copy": {"staging_bucket": "b", "iam_role": "r", "region": "us-east-1"}});
        assert_eq!(
            fields(Role::Sink, "redshift", staged),
            vec!["copy.credentials"]
        );
        let iam = json!({"write_strategy": "insert", "credentials": {"type": "iam",
            "config": {"region": "us-east-1"}}});
        assert_eq!(
            fields(Role::Sink, "redshift", iam),
            vec!["credentials.config.aws_credentials"]
        );
    }
    #[cfg(feature = "source-gcs")]
    {
        assert_eq!(
            fields(Role::Source, "gcs", json!({"bucket": "b"})),
            vec!["auth"]
        );
        let key = json!({"bucket": "b", "auth": {"type": "service_account_json_inline", "config": {"json": "{}"}}});
        assert!(fields(Role::Source, "gcs", key).is_empty());
    }
    #[cfg(feature = "sink-bigquery")]
    {
        let adc = json!({"auth": {"type": "application_default"}});
        assert_eq!(fields(Role::Sink, "bigquery", adc), vec!["auth"]);
    }
    #[cfg(feature = "source-pubsub")]
    {
        assert_eq!(
            fields(Role::Source, "pubsub", json!({})),
            vec!["credentials"]
        );
        let emulated = json!({"emulator_host": "localhost:8085"});
        assert!(fields(Role::Source, "pubsub", emulated).is_empty());
    }
    #[cfg(feature = "sink-spanner")]
    {
        assert_eq!(fields(Role::Sink, "spanner", json!({})), vec!["auth"]);
        let emulated = json!({"emulator_host": "localhost:9010"});
        assert!(fields(Role::Sink, "spanner", emulated).is_empty());
    }
    #[cfg(feature = "source-azure-blob")]
    {
        assert_eq!(
            fields(Role::Source, "azure-blob", json!({"container": "c"})),
            vec!["auth"]
        );
        let key = json!({"container": "c", "auth": {"type": "account_key", "config": {"account_key": "k"}}});
        assert!(fields(Role::Source, "azure-blob", key).is_empty());
        let emulated = json!({"container": "c", "use_emulator": true});
        assert!(fields(Role::Source, "azure-blob", emulated).is_empty());
    }
    #[cfg(feature = "sink-clickhouse")]
    {
        let staged = json!({"staging": {"location": "s3://b/p/"}});
        assert_eq!(
            fields(Role::Sink, "clickhouse", staged),
            vec!["staging.credentials"]
        );
        assert!(fields(Role::Sink, "clickhouse", json!({})).is_empty());
    }
    #[cfg(feature = "sink-mssql")]
    {
        let staged = json!({"staging": {"location": "az://c/p/", "credentials": {
            "type": "sas_token", "config": {"sas_token": "sv=1"}}}});
        assert!(fields(Role::Sink, "mssql", staged).is_empty());
    }
    #[cfg(feature = "sink-snowflake")]
    {
        let bulk = json!({"bulk_load": {"stage": "S", "url": "s3://b/p/"}});
        assert_eq!(
            fields(Role::Sink, "snowflake", bulk),
            vec!["bulk_load.credentials"]
        );
    }
    #[cfg(feature = "source-delta")]
    {
        let s3 = json!({"table_uri": "s3://b/t"});
        assert_eq!(fields(Role::Source, "delta", s3), vec!["credentials"]);
        assert!(fields(Role::Source, "delta", json!({"table_uri": "/tmp/t"})).is_empty());
    }
    #[cfg(feature = "sink-iceberg")]
    {
        let sql = json!({"catalog": {"type": "sql", "warehouse": "s3://b/wh"}});
        assert_eq!(
            fields(Role::Sink, "iceberg", sql),
            vec!["catalog.properties"]
        );
    }
    #[cfg(feature = "source-kafka")]
    {
        let gssapi = json!({"extra_client_config": {"sasl.mechanism": "GSSAPI"}});
        assert_eq!(
            fields(Role::Source, "kafka", gssapi),
            vec!["extra_client_config.sasl.mechanism"]
        );
    }
    #[cfg(feature = "source-mongodb")]
    {
        let aws = json!({"connection_uri": "mongodb://db.example/?authMechanism=MONGODB-AWS"});
        assert_eq!(fields(Role::Source, "mongodb", aws), vec!["connection_uri"]);
    }
}

/// The connectors whose identity can come from what they learn at run time
/// say so, and every cloud kind answers.
#[test]
fn cloud_descriptors_answer_and_runtime_checks_are_declared() {
    let checks = |role: Role, kind: &str| match role {
        Role::Source => faucet_registry::source_checks_host_identity_at_runtime(kind),
        Role::Sink => faucet_registry::sink_checks_host_identity_at_runtime(kind),
    };
    #[cfg(feature = "source-delta")]
    assert!(checks(Role::Source, "delta"));
    #[cfg(feature = "sink-delta")]
    assert!(checks(Role::Sink, "delta"));
    #[cfg(feature = "source-iceberg")]
    assert!(checks(Role::Source, "iceberg"));
    #[cfg(feature = "sink-iceberg")]
    assert!(checks(Role::Sink, "iceberg"));
    #[cfg(feature = "source-s3")]
    assert!(!checks(Role::Source, "s3"));
    let _ = checks;
    for (role, kind) in [
        (Role::Source, "kinesis"),
        (Role::Sink, "kinesis"),
        (Role::Source, "sqs"),
        (Role::Sink, "sqs"),
        (Role::Source, "dynamodb"),
        (Role::Sink, "dynamodb"),
        (Role::Source, "s3"),
        (Role::Sink, "s3"),
        (Role::Source, "parquet"),
        (Role::Sink, "parquet"),
        (Role::Source, "redshift"),
        (Role::Sink, "redshift"),
        (Role::Source, "gcs"),
        (Role::Sink, "gcs"),
        (Role::Source, "bigquery"),
        (Role::Sink, "bigquery"),
        (Role::Source, "pubsub"),
        (Role::Sink, "pubsub"),
        (Role::Source, "spanner"),
        (Role::Sink, "spanner"),
        (Role::Source, "azure-blob"),
        (Role::Sink, "azure-blob"),
        (Role::Sink, "clickhouse"),
        (Role::Sink, "mssql"),
        (Role::Sink, "databricks"),
        (Role::Sink, "snowflake"),
        (Role::Source, "delta"),
        (Role::Sink, "delta"),
        (Role::Source, "iceberg"),
        (Role::Sink, "iceberg"),
        (Role::Source, "kafka"),
        (Role::Sink, "kafka"),
        (Role::Source, "mongodb"),
        (Role::Sink, "mongodb"),
        (Role::Source, "mongodb-cdc"),
    ] {
        let descriptor = match role {
            Role::Source => faucet_registry::source_descriptor(kind),
            Role::Sink => faucet_registry::sink_descriptor(kind),
        };
        if let Some(d) = descriptor {
            assert!(d.answers_host_identity(), "{kind} answers");
        }
    }
}

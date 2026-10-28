//! Every `aws_credentials` source the `iam` and `redshift_data_api` methods
//! accept signs its Data API requests with the key it must produce:
//! `default` (environment), `profile` (temporary shared files), `access_key`,
//! `assume_role` and `web_identity` (STS on a fake through
//! `AWS_ENDPOINT_URL_STS`). One test, because each phase rewrites the process
//! environment the default credential chain reads.
#![allow(clippy::disallowed_methods)]

use std::sync::{Arc, Mutex};

use faucet_common_redshift::data_api::{Effect, Statement};
use faucet_common_redshift::{DataApiClient, RedshiftAwsCredentials, RedshiftConnection};
use faucet_core::FaucetError;
use faucet_core::io::CallContext;
use serde_json::json;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const REGION: &str = "us-east-1";

fn set(vars: &[(&str, &str)]) {
    // SAFETY: this binary has one test; nothing reads the environment concurrently.
    unsafe {
        for (k, v) in vars {
            std::env::set_var(k, v);
        }
    }
}

fn unset(vars: &[&str]) {
    // SAFETY: as in `set`.
    unsafe {
        for k in vars {
            std::env::remove_var(k);
        }
    }
}

/// A Data API that finishes every statement at once and keeps each
/// request's `Authorization` header.
#[derive(Clone, Default)]
struct Finishing(Arc<Mutex<Vec<String>>>);

impl Respond for Finishing {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        if let Some(a) = req
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
        {
            self.0.lock().unwrap().push(a.to_string());
        }
        let target = req
            .headers
            .get("x-amz-target")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        let body = match target {
            "RedshiftData.ExecuteStatement" => json!({"Id": "s1"}),
            _ => json!({"Id": "s1", "Status": "FINISHED", "HasResultSet": false}),
        };
        ResponseTemplate::new(200)
            .insert_header("content-type", "application/x-amz-json-1.1")
            .set_body_json(body)
    }
}

/// STS answering `AssumeRole` and `AssumeRoleWithWebIdentity` with distinct
/// keys.
struct Sts;

impl Respond for Sts {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let form: std::collections::HashMap<String, String> =
            url::form_urlencoded::parse(&req.body)
                .into_owned()
                .collect();
        let action = form.get("Action").cloned().unwrap_or_default();
        let key = match action.as_str() {
            "AssumeRole" => {
                assert_eq!(form["RoleArn"], "arn:aws:iam::222222222222:role/loader");
                assert_eq!(form["RoleSessionName"], "faucet-it");
                assert_eq!(form["ExternalId"], "ext-1");
                "ASIAASSUMED"
            }
            "AssumeRoleWithWebIdentity" => {
                assert_eq!(form["RoleArn"], "arn:aws:iam::333333333333:role/web");
                "ASIAWEBIDENTITY"
            }
            other => panic!("unexpected STS action {other}"),
        };
        ResponseTemplate::new(200)
            .insert_header("content-type", "text/xml")
            .set_body_string(format!(
                "<{action}Response xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><{action}Result>\
                 <Credentials><AccessKeyId>{key}</AccessKeyId><SecretAccessKey>s</SecretAccessKey>\
                 <SessionToken>t</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>\
                 <AssumedRoleUser><AssumedRoleId>AROA:x</AssumedRoleId><Arn>arn:aws:sts::1:assumed-role/r/x</Arn></AssumedRoleUser>\
                 </{action}Result><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></{action}Response>"
            ))
    }
}

async fn signer(server: &MockServer, seen: &Finishing, creds: RedshiftAwsCredentials) -> String {
    let label = format!("{creds:?}");
    let mut conn: RedshiftConnection = serde_json::from_value(json!({
        "database": "dev",
        "credentials": {"type": "redshift_data_api", "config": {
            "workgroup_name": "wg", "region": REGION, "endpoint_url": server.uri()}}
    }))
    .unwrap();
    if let faucet_common_redshift::RedshiftCredentials::RedshiftDataApi {
        aws_credentials, ..
    } = &mut conn.credentials
    {
        *aws_credentials = creds;
    }
    let api = DataApiClient::from_connection(&conn, FaucetError::Source)
        .unwrap()
        .unwrap();
    api.execute(
        &CallContext::new(),
        Effect::Read,
        &Statement::new("SELECT 1"),
    )
    .await
    .unwrap_or_else(|e| panic!("{label}: {e}"));
    let auth = seen.0.lock().unwrap().last().cloned().unwrap_or_default();
    auth.split("Credential=")
        .nth(1)
        .and_then(|c| c.split('/').next())
        .unwrap_or_default()
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_credentials_source_signs_with_its_own_key() {
    let data_api = MockServer::start().await;
    let seen = Finishing::default();
    Mock::given(wiremock::matchers::method("POST"))
        .respond_with(seen.clone())
        .mount(&data_api)
        .await;
    let sts = MockServer::start().await;
    Mock::given(wiremock::matchers::method("POST"))
        .respond_with(Sts)
        .mount(&sts)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let none = dir.path().join("absent");
    set(&[
        ("AWS_ENDPOINT_URL_STS", sts.uri().as_str()),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("AWS_REGION", REGION),
        ("AWS_CONFIG_FILE", none.to_str().unwrap()),
        ("AWS_SHARED_CREDENTIALS_FILE", none.to_str().unwrap()),
        ("AWS_ACCESS_KEY_ID", "AKIDFROMENV"),
        ("AWS_SECRET_ACCESS_KEY", "envsecret"),
    ]);

    assert_eq!(
        signer(&data_api, &seen, RedshiftAwsCredentials::Default).await,
        "AKIDFROMENV"
    );
    assert_eq!(
        signer(
            &data_api,
            &seen,
            RedshiftAwsCredentials::AccessKey {
                access_key_id: "AKIDSTATIC".into(),
                secret_access_key: "s".into(),
                session_token: None,
            }
        )
        .await,
        "AKIDSTATIC"
    );
    assert_eq!(
        signer(
            &data_api,
            &seen,
            RedshiftAwsCredentials::AssumeRole {
                role_arn: "arn:aws:iam::222222222222:role/loader".into(),
                session_name: Some("faucet-it".into()),
                external_id: Some("ext-1".into()),
            }
        )
        .await,
        "ASIAASSUMED"
    );

    unset(&["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]);
    let config = dir.path().join("config");
    let credentials = dir.path().join("credentials");
    std::fs::write(
        &config,
        format!("[profile faucet-test]\nregion = {REGION}\n"),
    )
    .unwrap();
    std::fs::write(
        &credentials,
        "[faucet-test]\naws_access_key_id = AKIDPROFILE\naws_secret_access_key = p\n",
    )
    .unwrap();
    set(&[
        ("AWS_CONFIG_FILE", config.to_str().unwrap()),
        ("AWS_SHARED_CREDENTIALS_FILE", credentials.to_str().unwrap()),
    ]);
    assert_eq!(
        signer(
            &data_api,
            &seen,
            RedshiftAwsCredentials::Profile {
                name: "faucet-test".into()
            }
        )
        .await,
        "AKIDPROFILE"
    );
    set(&[
        ("AWS_CONFIG_FILE", none.to_str().unwrap()),
        ("AWS_SHARED_CREDENTIALS_FILE", none.to_str().unwrap()),
    ]);

    let token = dir.path().join("token");
    std::fs::write(&token, "eyJhbGciOiJub25lIn0.eyJzdWIiOiJmYXVjZXQifQ.").unwrap();
    set(&[
        ("AWS_WEB_IDENTITY_TOKEN_FILE", token.to_str().unwrap()),
        ("AWS_ROLE_ARN", "arn:aws:iam::333333333333:role/web"),
        ("AWS_ROLE_SESSION_NAME", "faucet-web"),
    ]);
    assert_eq!(
        signer(&data_api, &seen, RedshiftAwsCredentials::WebIdentity).await,
        "ASIAWEBIDENTITY"
    );
}

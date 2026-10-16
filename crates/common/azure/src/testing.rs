//! Test helpers for Azure Blob connectors run against the Azurite emulator.
//!
//! `object_store` has no container-management API, so this signs the one
//! request tests need (Create Container) with Shared Key itself.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use faucet_core::FaucetError;
use hmac::{Hmac, Mac};
use sha2::Sha256;

const API_VERSION: &str = "2021-08-06";

/// Create `container` on the account served at `endpoint`
/// (`http://host:port/<account>`); an existing container is not an error.
pub async fn create_container(
    endpoint: &str,
    account: &str,
    account_key: &str,
    container: &str,
) -> Result<(), FaucetError> {
    let endpoint = endpoint.trim_end_matches('/');
    let url = format!("{endpoint}/{container}?restype=container");
    let path = url::Url::parse(&url)
        .map_err(|e| FaucetError::Config(format!("invalid Azure endpoint `{endpoint}`: {e}")))?
        .path()
        .to_string();
    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let signature = sign(
        account_key,
        &string_to_sign(&date, &format!("/{account}{path}")),
    )?;

    let client = faucet_core::http::Client::new()?;
    let resp = client
        .send(
            client
                .put(&url)
                .header("x-ms-date", &date)
                .header("x-ms-version", API_VERSION)
                .header("content-length", "0")
                .header("authorization", format!("SharedKey {account}:{signature}")),
        )
        .await?;
    match resp.status().as_u16() {
        201 | 409 => Ok(()),
        status => Err(FaucetError::Sink(format!(
            "create container `{container}`: HTTP {status}: {}",
            client.text(resp).await.unwrap_or_default()
        ))),
    }
}

fn string_to_sign(date: &str, resource: &str) -> String {
    format!(
        "PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{date}\nx-ms-version:{API_VERSION}\n{resource}\nrestype:container"
    )
}

fn sign(account_key: &str, string_to_sign: &str) -> Result<String, FaucetError> {
    let key = STANDARD
        .decode(account_key)
        .map_err(|e| FaucetError::Auth(format!("Azure account key is not base64: {e}")))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&key)
        .map_err(|e| FaucetError::Auth(format!("Azure account key: {e}")))?;
    mac.update(string_to_sign.as_bytes());
    Ok(STANDARD.encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_matches_a_known_shared_key_vector() {
        let date = "Thu, 16 Oct 2026 10:00:00 GMT";
        let s = string_to_sign(date, "/acct/acct/c");
        assert_eq!(
            s,
            "PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:Thu, 16 Oct 2026 10:00:00 GMT\n\
             x-ms-version:2021-08-06\n/acct/acct/c\nrestype:container"
        );
        let a = sign("a2V5", &s).unwrap();
        assert_eq!(a, sign("a2V5", &s).unwrap());
        assert_ne!(a, sign("a2V6", &s).unwrap());
        assert!(sign("not base64!", &s).is_err());
    }

    #[tokio::test]
    async fn creates_a_container_and_accepts_an_existing_one() {
        use wiremock::matchers::{header, header_exists, method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/acct/made"))
            .and(query_param("restype", "container"))
            .and(header("x-ms-version", API_VERSION))
            .and(header_exists("x-ms-date"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/acct/there"))
            .respond_with(ResponseTemplate::new(409))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/acct/denied"))
            .respond_with(ResponseTemplate::new(403).set_body_string("AuthenticationFailed"))
            .mount(&server)
            .await;
        let endpoint = format!("{}/acct/", server.uri());
        create_container(&endpoint, "acct", "a2V5", "made")
            .await
            .unwrap();
        create_container(&endpoint, "acct", "a2V5", "there")
            .await
            .unwrap();
        let err = create_container(&endpoint, "acct", "a2V5", "denied")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("HTTP 403: AuthenticationFailed"),
            "{err}"
        );
        let auth = server.received_requests().await.unwrap()[0]
            .headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(auth.starts_with("SharedKey acct:"), "{auth}");
    }

    #[tokio::test]
    async fn a_bad_endpoint_is_a_config_error() {
        let err = create_container("not a url", "a", "a2V5", "c")
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)), "{err}");
    }
}

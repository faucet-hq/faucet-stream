#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-snowflake
//!
//! Shared configuration types and helpers for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! Snowflake source and sink connectors.
//!
//! - [`SnowflakeAuth`] — JWT key-pair or OAuth bearer authentication.
//! - [`authorization_header`] — produces the `Authorization` header value the
//!   Snowflake SQL REST API expects (`Bearer <jwt>` for `KeyPair`,
//!   `Bearer <token>` for `OAuth`).
//! - [`snowflake_token_type`] — the matching `X-Snowflake-Authorization-Token-Type`
//!   header value (`KEYPAIR_JWT` for `KeyPair`, `OAUTH` for `OAuth`).
//!
//! `SnowflakeAuth` derives `Serialize`, `Deserialize`, and `JsonSchema` so it
//! round-trips through YAML/JSON configs and CLI introspection. Its `Debug`
//! impl masks credentials as `"***"`.

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Authentication method for Snowflake.
///
/// Serializes as `{ type: <method>, config: { … } }` (adjacent tagging,
/// snake_case discriminators) — the consistent auth wire shape shared by
/// every faucet connector. `key_pair` is stateless (JWT minted locally);
/// `oauth` carries a bearer token (and can be supplied via a shared
/// `auth: { ref }` provider).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum SnowflakeAuth {
    /// JWT key-pair authentication.
    ///
    /// Uses an RSA private key (PEM-encoded) to generate JWT tokens for the
    /// Snowflake SQL REST API.
    KeyPair {
        /// The Snowflake user account name.
        user: String,
        /// PEM-encoded RSA private key.
        private_key_pem: String,
    },
    /// OAuth2 bearer token (e.g. from an external identity provider).
    #[serde(rename = "oauth")]
    OAuth { token: String },
}

impl std::fmt::Debug for SnowflakeAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyPair { user, .. } => f
                .debug_struct("KeyPair")
                .field("user", user)
                .field("private_key_pem", &"***")
                .finish(),
            Self::OAuth { .. } => f.debug_struct("OAuth").field("token", &"***").finish(),
        }
    }
}

/// Build the `Authorization` header value for a Snowflake SQL REST API request.
///
/// For `KeyPair`, generates a fresh JWT signed with the configured RSA key
/// (issuer/subject set to `{ACCOUNT}.{USER}`, see [`jwt_account`], 1-hour
/// expiry) and wraps it as `Bearer {jwt}`. For `OAuth`, wraps the token as
/// `Bearer {token}` — the SQL API pairs it with
/// `X-Snowflake-Authorization-Token-Type: OAUTH`.
///
/// `account` is the Snowflake account identifier from the source/sink config
/// (e.g. `"xy12345.us-east-1"`), the same value that names the host.
pub fn authorization_header(auth: &SnowflakeAuth, account: &str) -> Result<String, FaucetError> {
    match auth {
        SnowflakeAuth::KeyPair {
            user,
            private_key_pem,
        } => {
            let account_upper = jwt_account(account);
            let user_upper = user.to_uppercase();
            let qualified_user = format!("{account_upper}.{user_upper}");

            // Snowflake's key-pair JWT spec requires the issuer to carry the
            // SHA-256 fingerprint of the *public* key:
            //   iss = {ACCOUNT}.{USER}.SHA256:{base64(sha256(DER SPKI public key))}
            //   sub = {ACCOUNT}.{USER}
            // Without the fingerprint the server rejects the token with 401
            // (#78/#19).
            let fingerprint = public_key_fingerprint(private_key_pem)?;
            let issuer = format!("{qualified_user}.{fingerprint}");

            let now = jsonwebtoken::get_current_timestamp();
            let claims = serde_json::json!({
                "iss": issuer,
                "sub": qualified_user,
                "iat": now,
                "exp": now + 3600,
            });

            let key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key_pem.as_bytes())
                .map_err(|e| FaucetError::Auth(format!("invalid RSA key: {e}")))?;

            let token = jsonwebtoken::encode(
                &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
                &claims,
                &key,
            )
            .map_err(|e| FaucetError::Auth(format!("JWT generation failed: {e}")))?;

            Ok(format!("Bearer {token}"))
        }
        SnowflakeAuth::OAuth { token } => Ok(format!("Bearer {token}")),
    }
}

/// Check a Snowflake account identifier (`xy12345.us-east-1`, `org-acct`,
/// `acct.privatelink`): letters, digits, `_`, `-` and `.` only, so the
/// request URL built from it — `https://{account}.snowflakecomputing.com` —
/// always names a Snowflake host. Anything else (`/`, `:`, `?`, `#`, `@`,
/// whitespace) would move the request to another host, port or path.
pub fn validate_account(account: &str) -> Result<(), FaucetError> {
    let valid = !account.is_empty()
        && !account.starts_with(['.', '-'])
        && !account.ends_with(['.', '-'])
        && !account.contains("..")
        && account
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
    if valid {
        Ok(())
    } else {
        Err(FaucetError::Config(format!(
            "snowflake: `account` {account:?} is not an account identifier (letters, digits, \
             `_`, `-` and `.` only, e.g. `xy12345.us-east-1` or `myorg-myaccount`)"
        )))
    }
}

/// The account part of a key-pair JWT's `iss`/`sub` claims.
///
/// Snowflake wants the account locator without its region or cloud suffix
/// (`xy12345.us-east-1` → `XY12345`), and for a `.global` identifier only the
/// part before the first hyphen; the organization form (`org-acct`) passes
/// through. The result is upper-cased.
pub fn jwt_account(account: &str) -> String {
    let base = if account.to_ascii_lowercase().contains(".global") {
        account.split('-').next().unwrap_or(account)
    } else {
        account.split('.').next().unwrap_or(account)
    };
    base.to_uppercase()
}

/// TCP connect timeout of [`http_client`].
pub const HTTP_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Idle read timeout of [`http_client`]: longer than the ~45 s the SQL API
/// holds a submit before answering 202, so only a stalled connection trips it.
pub const HTTP_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// The HTTP client the Snowflake source and sink share: gzip-decoding (result
/// partitions after the first arrive gzip-encoded) with connect and idle-read
/// timeouts, so a half-open connection fails instead of hanging the run.
pub fn http_client() -> Result<reqwest::Client, FaucetError> {
    faucet_core::http::Client::builder()
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .read_timeout(HTTP_READ_TIMEOUT)
        .configure(|b| b.gzip(true))
        .build_reqwest()
        .map_err(|e| FaucetError::Config(format!("snowflake: cannot build HTTP client: {e}")))
}

/// Best-effort `POST {statements_url}/{handle}/cancel`, so an abandoned
/// statement stops running (and billing) on the warehouse. `statements_url`
/// is the `…/api/v2/statements` endpoint. A failure — a transport error or a
/// non-success status such as an expired token's `401` — is logged with the
/// status, never returned.
pub async fn cancel_statement(
    client: &reqwest::Client,
    statements_url: &str,
    handle: &str,
    auth: &str,
    token_type: &str,
) {
    let url = format!("{}/{handle}/cancel", statements_url.trim_end_matches('/'));
    let result = client
        .post(&url)
        .header("Authorization", auth)
        .header("Accept", "application/json")
        .header("X-Snowflake-Authorization-Token-Type", token_type)
        .send()
        .await;
    match result {
        Err(e) => {
            tracing::warn!(statement = handle, error = %e, "Snowflake statement cancel failed");
        }
        Ok(resp) if !resp.status().is_success() => {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            tracing::warn!(
                statement = handle,
                status,
                body = %cancel_body_excerpt(&body),
                "Snowflake refused the statement cancel; the statement may keep running"
            );
        }
        Ok(_) => {}
    }
}

/// The start of a refused cancel's response body, for the log line.
fn cancel_body_excerpt(body: &str) -> &str {
    const MAX: usize = 512;
    let body = body.trim();
    &body[..body.floor_char_boundary(MAX.min(body.len()))]
}

/// Cancels a still-running statement when the future polling it is dropped
/// (the run was cancelled or timed out). [`disarm`](Self::disarm) it once the
/// statement finished or was cancelled explicitly.
pub struct CancelOnDrop {
    client: reqwest::Client,
    statements_url: String,
    handle: String,
    auth: Option<(String, &'static str)>,
    armed: bool,
}

impl CancelOnDrop {
    /// Arm a guard for `handle` on the `…/api/v2/statements` endpoint.
    pub fn new(client: reqwest::Client, statements_url: String, handle: &str) -> Self {
        Self {
            client,
            statements_url,
            handle: handle.to_owned(),
            auth: None,
            armed: true,
        }
    }

    /// The latest `Authorization` header and token type, used by the cancel.
    pub fn set_auth(&mut self, auth: String, token_type: &'static str) {
        self.auth = Some((auth, token_type));
    }

    /// Do not cancel on drop.
    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (Some((auth, token_type)), Ok(rt)) =
            (self.auth.take(), tokio::runtime::Handle::try_current())
        else {
            return;
        };
        let client = self.client.clone();
        let url = std::mem::take(&mut self.statements_url);
        let handle = std::mem::take(&mut self.handle);
        rt.spawn(async move { cancel_statement(&client, &url, &handle, &auth, token_type).await });
    }
}

/// Compute the Snowflake public-key fingerprint (`SHA256:<base64>`) from a
/// PEM-encoded RSA private key.
///
/// The fingerprint is `base64(SHA-256(DER SubjectPublicKeyInfo))` of the
/// derived public key — exactly what
/// `openssl rsa -pubout -outform DER | openssl dgst -sha256 -binary | base64`
/// produces, and what Snowflake expects in the JWT `iss` claim.
fn public_key_fingerprint(private_key_pem: &str) -> Result<String, FaucetError> {
    use aws_lc_rs::signature::{KeyPair, RsaKeyPair};
    use base64::Engine as _;
    use sha2::{Digest, Sha256};

    let invalid =
        |e: &dyn std::fmt::Display| FaucetError::Auth(format!("invalid RSA private key: {e}"));
    // Accept either PKCS#8 (`BEGIN PRIVATE KEY`) or PKCS#1
    // (`BEGIN RSA PRIVATE KEY`) PEM — both are common for Snowflake keys.
    let (label, der) = pem_block(private_key_pem).ok_or_else(|| invalid(&"not a PEM key"))?;
    let pair = match label.as_str() {
        "PRIVATE KEY" => RsaKeyPair::from_pkcs8(&der),
        "RSA PRIVATE KEY" => RsaKeyPair::from_der(&der),
        other => return Err(invalid(&format!("unsupported PEM block `{other}`"))),
    }
    .map_err(|e| invalid(&e))?;

    let digest = Sha256::digest(rsa_spki_der(pair.public_key().as_ref()));
    let b64 = base64::engine::general_purpose::STANDARD.encode(digest);
    Ok(format!("SHA256:{b64}"))
}

/// The label and decoded body of the first PEM block in `pem`.
fn pem_block(pem: &str) -> Option<(String, Vec<u8>)> {
    use base64::Engine as _;
    let rest = pem.split_once("-----BEGIN ")?.1;
    let (label, rest) = rest.split_once("-----")?;
    let body = rest.split_once(&format!("-----END {label}-----"))?.0;
    let b64: String = body.chars().filter(|c| !c.is_whitespace()).collect();
    let der = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    Some((label.to_string(), der))
}

/// Wrap a PKCS#1 `RSAPublicKey` in a DER `SubjectPublicKeyInfo` (rsaEncryption).
fn rsa_spki_der(pkcs1: &[u8]) -> Vec<u8> {
    const RSA_ALGORITHM: [u8; 15] = [
        0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00,
    ];
    let mut bits = vec![0x03];
    bits.extend(der_length(pkcs1.len() + 1));
    bits.push(0x00);
    bits.extend_from_slice(pkcs1);
    let mut spki = vec![0x30];
    spki.extend(der_length(RSA_ALGORITHM.len() + bits.len()));
    spki.extend_from_slice(&RSA_ALGORITHM);
    spki.extend(bits);
    spki
}

fn der_length(len: usize) -> Vec<u8> {
    if len < 0x80 {
        return vec![len as u8];
    }
    let bytes: Vec<u8> = len
        .to_be_bytes()
        .into_iter()
        .skip_while(|b| *b == 0)
        .collect();
    let mut out = vec![0x80 | bytes.len() as u8];
    out.extend(bytes);
    out
}

/// The `X-Snowflake-Authorization-Token-Type` header value that pairs with the
/// `Authorization` header produced by [`authorization_header`].
pub fn snowflake_token_type(auth: &SnowflakeAuth) -> &'static str {
    match auth {
        SnowflakeAuth::KeyPair { .. } => "KEYPAIR_JWT",
        SnowflakeAuth::OAuth { .. } => "OAUTH",
    }
}

/// Map a [`faucet_core::Credential`] yielded by a shared [`faucet_core::AuthProvider`]
/// onto [`SnowflakeAuth`].
///
/// Snowflake supports OAuth bearer tokens via shared providers. Key-pair JWT
/// auth is stateless (the JWT is minted locally from the RSA key) and therefore
/// cannot be supplied by a provider; attempting to do so returns
/// [`FaucetError::Auth`].
///
/// | Credential | Mapping |
/// |---|---|
/// | `Bearer(token)` | `SnowflakeAuth::OAuth { token }` |
/// | `Token(token)` | `SnowflakeAuth::OAuth { token }` |
/// | `Basic` / `Header` | `FaucetError::Auth` |
pub fn credential_to_auth(cred: faucet_core::Credential) -> Result<SnowflakeAuth, FaucetError> {
    match cred {
        faucet_core::Credential::Bearer(token) | faucet_core::Credential::Token(token) => {
            Ok(SnowflakeAuth::OAuth { token })
        }
        other => Err(FaucetError::Auth(format!(
            "Snowflake auth provider must yield a bearer/token credential, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn account_identifiers_cannot_leave_snowflake() {
        for ok in [
            "xy12345",
            "xy12345.us-east-1",
            "myorg-my_acct",
            "acct.privatelink",
        ] {
            assert!(validate_account(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "internal-admin.svc.cluster.local:8443/admin/export?x=",
            "evil.example/x",
            "a@b",
            "a#",
            "a b",
            ".a",
            "a.",
            "a..b",
            "-a",
        ] {
            let err = validate_account(bad).unwrap_err();
            assert!(
                err.to_string().contains("not an account identifier"),
                "{bad}"
            );
        }
    }
    use super::*;

    #[test]
    fn debug_masks_key_pair_private_key() {
        let auth = SnowflakeAuth::KeyPair {
            user: "alice".into(),
            private_key_pem: "PRIVATE-KEY-DATA".into(),
        };
        let debug = format!("{auth:?}");
        assert!(debug.contains("alice"));
        assert!(debug.contains("***"));
        assert!(!debug.contains("PRIVATE-KEY-DATA"));
    }

    #[test]
    fn debug_masks_oauth_token() {
        let auth = SnowflakeAuth::OAuth {
            token: "my-token".into(),
        };
        let debug = format!("{auth:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("my-token"));
    }

    #[test]
    fn serde_round_trip_oauth() {
        let auth = SnowflakeAuth::OAuth { token: "t".into() };
        let json = serde_json::to_string(&auth).unwrap();
        assert_eq!(json, r#"{"type":"oauth","config":{"token":"t"}}"#);
        let parsed: SnowflakeAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, SnowflakeAuth::OAuth { .. }));
    }

    #[test]
    fn serde_round_trip_key_pair() {
        let json = r#"{"type":"key_pair","config":{"user":"u","private_key_pem":"k"}}"#;
        let parsed: SnowflakeAuth = serde_json::from_str(json).unwrap();
        match parsed {
            SnowflakeAuth::KeyPair {
                user,
                private_key_pem,
            } => {
                assert_eq!(user, "u");
                assert_eq!(private_key_pem, "k");
            }
            _ => panic!("expected KeyPair"),
        }
    }

    #[test]
    fn oauth_authorization_header_uses_bearer_scheme() {
        let auth = SnowflakeAuth::OAuth {
            token: "my-token".into(),
        };
        let header = authorization_header(&auth, "acct").unwrap();
        assert_eq!(header, "Bearer my-token");
    }

    #[test]
    fn jwt_account_strips_region_and_global_suffixes() {
        assert_eq!(jwt_account("xy12345.us-east-1"), "XY12345");
        assert_eq!(jwt_account("xy12345.us-east-2.aws"), "XY12345");
        assert_eq!(jwt_account("myorg-myacct"), "MYORG-MYACCT");
        assert_eq!(jwt_account("xy12345-abcdef.global"), "XY12345");
        assert_eq!(jwt_account("acct"), "ACCT");
        assert_eq!(jwt_account("XY12345-ABCDEF.GLOBAL"), "XY12345");
    }

    #[test]
    fn key_pair_jwt_claims_use_the_locator_without_region() {
        use base64::Engine as _;
        let auth = SnowflakeAuth::KeyPair {
            user: "u".into(),
            private_key_pem: TEST_RSA_PKCS8_PEM.into(),
        };
        let header = authorization_header(&auth, "xy12345.us-east-1").unwrap();
        let jwt = header.strip_prefix("Bearer ").unwrap();
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(jwt.split('.').nth(1).unwrap())
            .unwrap();
        let claims: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(claims["sub"], "XY12345.U");
    }

    #[test]
    fn key_pair_with_invalid_pem_surfaces_auth_error() {
        let auth = SnowflakeAuth::KeyPair {
            user: "u".into(),
            private_key_pem: "not-a-pem".into(),
        };
        let err = authorization_header(&auth, "acct").unwrap_err();
        match err {
            // The fingerprint step parses the key first, so an invalid PEM now
            // surfaces "invalid RSA private key" — still an Auth error.
            FaucetError::Auth(msg) => assert!(msg.contains("invalid RSA"), "{msg}"),
            other => panic!("expected Auth error, got {other:?}"),
        }
    }

    // Throwaway 2048-bit RSA test key (PKCS#8). Not used anywhere real.
    const TEST_RSA_PKCS8_PEM: &str = include_str!("../tests/fixtures/snowflake_test_pkcs8.pem");

    #[test]
    fn key_pair_jwt_iss_includes_public_key_fingerprint() {
        // Regression for #78/#19: `iss` must be {ACCOUNT}.{USER}.SHA256:<fp>
        // while `sub` stays {ACCOUNT}.{USER}.
        use base64::Engine as _;
        let auth = SnowflakeAuth::KeyPair {
            user: "u".into(),
            private_key_pem: TEST_RSA_PKCS8_PEM.into(),
        };
        let header = authorization_header(&auth, "acct").unwrap();
        let jwt = header.strip_prefix("Bearer ").expect("Bearer token");
        let payload_b64 = jwt.split('.').nth(1).expect("jwt payload segment");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload_b64)
            .expect("base64url payload");
        let claims: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(claims["sub"], "ACCT.U");
        assert_eq!(
            claims["iss"],
            "ACCT.U.SHA256:NiQ5G+9Hr4ZBmdBscIoTOgx2SM6aWPG0/Q9Y6NuFtpI="
        );
    }

    #[test]
    fn public_key_fingerprint_matches_openssl() {
        let fp = public_key_fingerprint(TEST_RSA_PKCS8_PEM).unwrap();
        assert_eq!(fp, "SHA256:NiQ5G+9Hr4ZBmdBscIoTOgx2SM6aWPG0/Q9Y6NuFtpI=");
    }

    const TEST_RSA_PKCS1_PEM: &str = include_str!("../tests/fixtures/snowflake_test_pkcs1.pem");

    #[test]
    fn pkcs1_and_pkcs8_forms_of_one_key_share_a_fingerprint() {
        let fp = public_key_fingerprint(TEST_RSA_PKCS1_PEM).unwrap();
        assert_eq!(fp, "SHA256:NiQ5G+9Hr4ZBmdBscIoTOgx2SM6aWPG0/Q9Y6NuFtpI=");
    }

    #[test]
    fn a_non_rsa_or_malformed_pem_is_an_auth_error() {
        for pem in [
            "not a pem",
            "-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----",
            "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----",
            "-----BEGIN PRIVATE KEY-----\n!!!\n-----END PRIVATE KEY-----",
        ] {
            let err = public_key_fingerprint(pem).unwrap_err();
            assert!(
                matches!(err, FaucetError::Auth(ref m) if m.starts_with("invalid RSA private key")),
                "{err}"
            );
        }
    }

    #[test]
    fn der_lengths_use_the_long_form_above_127() {
        assert_eq!(der_length(5), vec![5]);
        assert_eq!(der_length(200), vec![0x81, 200]);
        assert_eq!(der_length(270), vec![0x82, 0x01, 0x0e]);
    }

    #[test]
    fn a_guard_without_auth_or_runtime_does_nothing_on_drop() {
        let mut g = CancelOnDrop::new(http_client().unwrap(), "http://x".into(), "h");
        drop(CancelOnDrop::new(
            http_client().unwrap(),
            "http://x".into(),
            "h",
        ));
        g.set_auth("Bearer t".into(), "OAUTH");
        drop(g);
    }

    #[test]
    fn a_refused_cancel_logs_a_bounded_body_excerpt() {
        assert_eq!(cancel_body_excerpt("  expired token \n"), "expired token");
        let long = "é".repeat(400);
        let cut = cancel_body_excerpt(&long);
        assert!(cut.len() <= 512 && long.starts_with(cut));
    }

    #[tokio::test]
    async fn a_refused_cancel_is_sent_and_only_logged() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v2/statements/h1/cancel"))
            .and(header("Authorization", "Bearer old"))
            .respond_with(ResponseTemplate::new(401).set_body_string("token expired"))
            .expect(1)
            .mount(&server)
            .await;
        cancel_statement(
            &http_client().unwrap(),
            &format!("{}/api/v2/statements/", server.uri()),
            "h1",
            "Bearer old",
            "OAUTH",
        )
        .await;
        server.verify().await;
    }

    #[test]
    fn http_client_builds() {
        assert!(http_client().is_ok());
        assert!(HTTP_READ_TIMEOUT > std::time::Duration::from_secs(45));
    }

    #[test]
    fn token_type_matches_variant() {
        assert_eq!(
            snowflake_token_type(&SnowflakeAuth::OAuth { token: "t".into() }),
            "OAUTH"
        );
        assert_eq!(
            snowflake_token_type(&SnowflakeAuth::KeyPair {
                user: "u".into(),
                private_key_pem: "k".into()
            }),
            "KEYPAIR_JWT"
        );
    }
}

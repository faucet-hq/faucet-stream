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
/// `o_auth` carries a bearer token (and can be supplied via a shared
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

/// The account part of a key-pair JWT's `iss`/`sub` claims.
///
/// Snowflake wants the account locator without its region or cloud suffix
/// (`xy12345.us-east-1` → `XY12345`), and for a `.global` identifier only the
/// part before the first hyphen; the organization form (`org-acct`) passes
/// through. The result is upper-cased.
pub fn jwt_account(account: &str) -> String {
    let base = if account.contains(".global") {
        account.split('-').next().unwrap_or(account)
    } else {
        account.split('.').next().unwrap_or(account)
    };
    base.to_uppercase()
}

/// Compute the Snowflake public-key fingerprint (`SHA256:<base64>`) from a
/// PEM-encoded RSA private key.
///
/// The fingerprint is `base64(SHA-256(DER SubjectPublicKeyInfo))` of the
/// derived public key — exactly what
/// `openssl rsa -pubout -outform DER | openssl dgst -sha256 -binary | base64`
/// produces, and what Snowflake expects in the JWT `iss` claim.
fn public_key_fingerprint(private_key_pem: &str) -> Result<String, FaucetError> {
    use base64::Engine as _;
    use rsa::pkcs1::DecodeRsaPrivateKey;
    use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
    use rsa::{RsaPrivateKey, RsaPublicKey};
    use sha2::{Digest, Sha256};

    // Accept either PKCS#8 (`BEGIN PRIVATE KEY`) or PKCS#1
    // (`BEGIN RSA PRIVATE KEY`) PEM — both are common for Snowflake keys.
    let private = RsaPrivateKey::from_pkcs8_pem(private_key_pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(private_key_pem))
        .map_err(|e| FaucetError::Auth(format!("invalid RSA private key: {e}")))?;

    let public = RsaPublicKey::from(&private);
    let der = public
        .to_public_key_der()
        .map_err(|e| FaucetError::Auth(format!("failed to DER-encode public key: {e}")))?;

    let digest = Sha256::digest(der.as_bytes());
    let b64 = base64::engine::general_purpose::STANDARD.encode(digest);
    Ok(format!("SHA256:{b64}"))
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
    const TEST_RSA_PKCS8_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDDmeSF5jD5LMGw
INB1hExU2Ux9qEQ9DXNUeWxrDv7K3QHA+UkCbdUpHDZdFSbIr/bvwlNn16Hqhqi9
b8WywAzjagZNg0cReXuQ7nKIr5c9zYl2EJe+RZTo2z2LE21HrSKRhTAmlOk3XJ1N
xc7ahYcKyw8lchuTcZaYWaNTYvronOpHUAGS0XpT0y8Oggzp1DvZNYOeZbJCPZwf
mpGGCSilnODNYnwT02Pc4aXXBzJP7TP57+ve/ZzqvsKCBiNJUMLsjUZcGWnqQHnR
A+8B87ug7CyhhEiYnskp0d1ZlWT/kU7rIZv58KMbMJidAdizA47jRjelsWeoedRf
JmiA99ZhAgMBAAECggEAAOrybwxm82xZ1k05HSwLPaStXrOQ6mZrQZy2PQRbfrEt
xm2FAa1pQCGhQauNPIjS1EopoQWafWK3XPguyclr5g9Dy05P4Y2b3lC4GdsVDxWt
TPAD/kEOU09gCQyEyT7PODaTRMMTGw7ksA47C7xvp0XPouHXrkfsqHdXNFd1DO1Z
dBCzkX4dg4Y4ffh5tt/ILeSsNlmqqpUQmHQZ/X3JHkP9/+NpAe6i4k9QKsqmLDGD
7+br/snVYbECBgmN1QIofTSvnlmmRiKgoG9wbZLmGvCiW9xVjbY+ryJs/lsLoM7w
W1TUuOlk3apoIzQ7OIGznyZzE5RumdQq11rNKB7aaQKBgQDowsceEQz2kLb93f8J
QaBDcebqbbGTJE6+hq2k8D/GzvZAdBHGuEt7NiDAFKy/GItwzJSGGdjK24iRtZ7G
2gIloZShu+7mmxX6Ojuxun8EMRZKZzTedMJWQJMwA1Hk1fwzsEM0+9+yZdTcylP9
wYDMFKbvw+av7sDcySENNEhshQKBgQDXIVX+Zvlf2PoLkRx11mk1CBtPfjqPTMcs
QVjISwvkgGSi8ihq+mwsIWLXhOZX38+L4iGfdIgqSSnwqB/fgTbjwQsa0Dqkygt6
IBfb3QmWr7196c+xss5h8eUTFiCMWw/EAa9R+jkWH0cVpJVbyTK7cBJlaXxPcXx3
xprI10qnLQKBgBl/NKajgYME6Ta3+bb+3FpnAL+PUpNmt8WBJUZbFvFlPG5lCIl3
KLWPgVjpKt8oBiZOErr529ik4bnsZj8sJG4Q3CI3Xv0d4fNuK5nVbxJ7ehCea5ku
uxcNrdHlmzPxCNZ0qXgFW0TEiOPCuh6i8sPoQz0ifYOqKLBGy/sRThmtAoGAGTd9
Hv7vCD8kwCpYTa++UUsL+HtxXc7AIf3e7Etvr28lXLxJ5JBKEbowHdckMPS5HUp6
anh8ZYiB9AWhBs/coUHFjXUPCrXsNnqAkXMNZq5e5d18TPYKnwx9r4kOc6VQ6cbQ
yCkue9tat7y9DS8+VR5D6cM9oQpKbrfG+PfTdlkCgYBf/pUWO94VgZvpV5Ui7MHb
6ZoH11q0gIhmT72FQ+2Erw977qghzs1+C7HO4Q7kNfC8sA9uVS4WiA1EzE6QeJWt
+FklEinW+AR2azgC/+gEUBvZSWU1v4meYdAQcNEek8L4VtBuGc4ZwbVbho3hiLmx
68Y3qeoKxOyBKo6j2NiZzg==
-----END PRIVATE KEY-----
";

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

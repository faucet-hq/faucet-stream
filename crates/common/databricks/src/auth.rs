//! Databricks bearer authentication, shared by the source and sink.

use faucet_core::{AuthSpec, FaucetError, SharedAuthProvider};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Authentication for the Databricks SQL Statement Execution API.
///
/// Both variants send `Authorization: Bearer <token>` — Databricks accepts a
/// Personal Access Token (PAT) or an OAuth machine-to-machine (M2M) access
/// token in the same header. Uses the project-wide adjacently-tagged
/// `{ type, config }` shape, e.g. `auth: { type: pat, config: { token: … } }`.
/// A shared `auth: { ref: <name> }` provider that yields a `Bearer`/`Token`
/// credential maps onto [`DatabricksAuth::Token`] — that is how an OAuth M2M
/// service principal is wired: an `oauth2` client-credentials provider against
/// `{workspace_url}/oidc/v1/token` with scope `all-apis`, which refreshes the
/// token before it expires.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum DatabricksAuth {
    /// Databricks Personal Access Token.
    Pat {
        /// The token string (use `${env:…}` / `${vault:…}` to inject).
        token: String,
    },
    /// A pre-obtained OAuth (M2M) bearer token.
    Token {
        /// The bearer token string.
        token: String,
    },
}

impl std::fmt::Debug for DatabricksAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabricksAuth::Pat { .. } => f.debug_struct("Pat").field("token", &"***").finish(),
            DatabricksAuth::Token { .. } => f.debug_struct("Token").field("token", &"***").finish(),
        }
    }
}

impl DatabricksAuth {
    /// The `Authorization` header value (`Bearer <token>`).
    pub fn authorization_value(&self) -> String {
        match self {
            DatabricksAuth::Pat { token } | DatabricksAuth::Token { token } => {
                format!("Bearer {token}")
            }
        }
    }
}

/// Resolve the `Authorization` header value: a shared provider wins, else the
/// inline auth. A `{ ref }` with no provider attached is a typed auth error.
pub async fn resolve_authorization(
    auth: &AuthSpec<DatabricksAuth>,
    provider: Option<&SharedAuthProvider>,
) -> Result<String, FaucetError> {
    if let Some(p) = provider {
        let cred = p.credential().await?;
        return cred.authorization_value().ok_or_else(|| {
            FaucetError::Auth("databricks: shared provider yielded no bearer credential".into())
        });
    }
    match auth {
        AuthSpec::Inline(a) => Ok(a.authorization_value()),
        AuthSpec::Reference(r) => Err(FaucetError::Auth(format!(
            "databricks: auth references provider '{}' but none was supplied",
            r.name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{AuthReference, Credential};
    use std::sync::Arc;

    #[derive(Debug)]
    struct Fixed(Credential);

    #[faucet_core::async_trait]
    impl faucet_core::AuthProvider for Fixed {
        async fn credential(&self) -> Result<Credential, FaucetError> {
            Ok(self.0.clone())
        }
        fn provider_name(&self) -> &'static str {
            "fixed"
        }
    }

    #[test]
    fn auth_is_bearer_for_both_variants() {
        assert_eq!(
            DatabricksAuth::Pat {
                token: "abc".into()
            }
            .authorization_value(),
            "Bearer abc"
        );
        assert_eq!(
            DatabricksAuth::Token {
                token: "xyz".into()
            }
            .authorization_value(),
            "Bearer xyz"
        );
    }

    #[test]
    fn debug_masks_tokens() {
        let p = format!(
            "{:?}",
            DatabricksAuth::Pat {
                token: "secret1".into()
            }
        );
        let t = format!(
            "{:?}",
            DatabricksAuth::Token {
                token: "secret2".into()
            }
        );
        assert!(!p.contains("secret1") && p.contains("Pat"));
        assert!(!t.contains("secret2") && t.contains("Token"));
    }

    #[test]
    fn deserializes_adjacent_shape() {
        let a: DatabricksAuth =
            serde_json::from_value(serde_json::json!({"type": "pat", "config": {"token": "t"}}))
                .unwrap();
        assert_eq!(a.authorization_value(), "Bearer t");
    }

    #[tokio::test]
    async fn resolve_prefers_provider_then_inline_then_errors_on_dangling_ref() {
        let inline = AuthSpec::Inline(DatabricksAuth::Pat { token: "i".into() });
        assert_eq!(
            resolve_authorization(&inline, None).await.unwrap(),
            "Bearer i"
        );

        let p: SharedAuthProvider = Arc::new(Fixed(Credential::Bearer("shared".into())));
        assert_eq!(
            resolve_authorization(&inline, Some(&p)).await.unwrap(),
            "Bearer shared"
        );

        let basic: SharedAuthProvider = Arc::new(Fixed(Credential::Basic {
            username: "u".into(),
            password: "p".into(),
        }));
        let err = resolve_authorization(&inline, Some(&basic))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no bearer"), "{err}");

        let dangling: AuthSpec<DatabricksAuth> =
            AuthSpec::Reference(AuthReference { name: "idp".into() });
        let err = resolve_authorization(&dangling, None).await.unwrap_err();
        assert!(err.to_string().contains("idp"), "{err}");
    }
}

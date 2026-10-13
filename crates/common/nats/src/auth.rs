//! NATS authentication modes.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// NATS client authentication configuration.
///
/// Serializes with an adjacent `{ "type": <method>, "config": { … } }` tag in
/// snake_case, matching every other faucet connector's auth shape.
///
/// The [`std::fmt::Debug`] implementation is hand-written so secret material
/// (`token`, `password`, `nkey` seed) is never printed — only the variant name
/// and non-secret fields appear.
#[derive(Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum NatsAuth {
    /// No authentication (anonymous connection).
    #[default]
    None,
    /// Bearer/token authentication.
    Token {
        /// The authentication token.
        token: String,
    },
    /// Username + password authentication.
    UserPassword {
        /// The username.
        username: String,
        /// The password.
        password: String,
    },
    /// NKey (Ed25519 seed) authentication.
    NKey {
        /// The NKey seed (starts with `S`).
        nkey: String,
    },
    /// Credentials-file (`.creds`) authentication — a decentralized JWT +
    /// NKey seed bundle as produced by `nsc`.
    CredsFile {
        /// Path to the `.creds` file.
        #[schemars(extend("x-faucet-resource" = "path"))]
        path: PathBuf,
    },
}

impl std::fmt::Debug for NatsAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render secret material — only the variant and non-secret fields.
        match self {
            NatsAuth::None => f.write_str("None"),
            NatsAuth::Token { .. } => f
                .debug_struct("Token")
                .field("token", &"<redacted>")
                .finish(),
            NatsAuth::UserPassword { username, .. } => f
                .debug_struct("UserPassword")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            NatsAuth::NKey { .. } => f.debug_struct("NKey").field("nkey", &"<redacted>").finish(),
            NatsAuth::CredsFile { path } => {
                f.debug_struct("CredsFile").field("path", path).finish()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_is_none() {
        assert!(matches!(NatsAuth::default(), NatsAuth::None));
    }

    #[test]
    fn serde_round_trip_token() {
        let auth = NatsAuth::Token {
            token: "sekret".into(),
        };
        let v = serde_json::to_value(&auth).unwrap();
        assert_eq!(v["type"], "token");
        assert_eq!(v["config"]["token"], "sekret");
        let parsed: NatsAuth = serde_json::from_value(v).unwrap();
        assert!(matches!(parsed, NatsAuth::Token { token } if token == "sekret"));
    }

    #[test]
    fn serde_round_trip_user_password() {
        let v = json!({"type": "user_password", "config": {"username": "u", "password": "p"}});
        let parsed: NatsAuth = serde_json::from_value(v).unwrap();
        assert!(
            matches!(parsed, NatsAuth::UserPassword { username, password } if username == "u" && password == "p")
        );
    }

    #[test]
    fn serde_round_trip_creds_file() {
        let v = json!({"type": "creds_file", "config": {"path": "/tmp/x.creds"}});
        let parsed: NatsAuth = serde_json::from_value(v).unwrap();
        assert!(
            matches!(parsed, NatsAuth::CredsFile { path } if path == std::path::Path::new("/tmp/x.creds"))
        );
    }

    #[test]
    fn debug_redacts_token() {
        let auth = NatsAuth::Token {
            token: "super-secret-token".into(),
        };
        let dbg = format!("{auth:?}");
        assert!(
            !dbg.contains("super-secret-token"),
            "debug leaked token: {dbg}"
        );
        assert!(dbg.contains("redacted"));
    }

    #[test]
    fn debug_redacts_password_but_shows_username() {
        let auth = NatsAuth::UserPassword {
            username: "alice".into(),
            password: "hunter2".into(),
        };
        let dbg = format!("{auth:?}");
        assert!(dbg.contains("alice"));
        assert!(!dbg.contains("hunter2"), "debug leaked password: {dbg}");
    }

    #[test]
    fn debug_redacts_nkey() {
        let auth = NatsAuth::NKey {
            nkey: "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY".into(),
        };
        let dbg = format!("{auth:?}");
        assert!(!dbg.contains("SUACSSL3"), "debug leaked nkey: {dbg}");
    }

    #[test]
    fn schema_compiles() {
        let _ = schemars::schema_for!(NatsAuth);
    }
}

//! `GET /v1/whoami` — the caller's own principal, role and permissions (#698).
//! Every role may call it, so a client (the web console) can shape itself to
//! what the caller may do. The server stays the authority: this is a hint for
//! the UI, every route still enforces its own permission.

use crate::serve::rbac::{AuthContext, Permission, Role};
use axum::{Extension, Json};
use serde::Serialize;

/// `GET /v1/whoami` response body.
#[derive(Debug, Serialize)]
pub struct WhoAmI {
    pub principal: String,
    pub role: Role,
    pub permissions: Vec<Permission>,
}

impl WhoAmI {
    pub fn of(actor: &AuthContext) -> Self {
        Self {
            principal: actor.principal.clone(),
            role: actor.role,
            permissions: actor.role.permissions(),
        }
    }
}

/// `GET /v1/whoami` → 200.
pub async fn whoami(Extension(actor): Extension<AuthContext>) -> Json<WhoAmI> {
    Json(WhoAmI::of(&actor))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(role: Role) -> AuthContext {
        AuthContext {
            principal: "alice".into(),
            role,
            source_ip: None,
        }
    }

    #[tokio::test]
    async fn reports_the_callers_role_and_exactly_its_permissions() {
        let Json(me) = whoami(Extension(actor(Role::Operator))).await;
        assert_eq!(me.principal, "alice");
        assert_eq!(me.role, Role::Operator);
        assert!(me.permissions.contains(&Permission::RunWrite));
        assert!(!me.permissions.contains(&Permission::TemplateAdmin));
        let json = serde_json::to_value(WhoAmI::of(&actor(Role::Viewer))).unwrap();
        assert_eq!(json["role"], "viewer");
        assert!(
            json["permissions"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("template_read"))
        );
    }
}

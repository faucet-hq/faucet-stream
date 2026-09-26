//! Role-based access control for the `faucet serve` control plane (#205).
//!
//! The default single-`--auth-token` mode is one implicit `admin` principal. A
//! `--auth-config <file>` promotes serve to a multi-principal deployment: a list
//! of `{ name, token, role }` principals, each token mapped to a [`Role`] that
//! grants a fixed set of [`Permission`]s. Every `/v1` route declares the
//! permission it needs ([`required_permission`]); the auth middleware
//! (`serve::auth::require_auth`) resolves the bearer token to an
//! [`AuthContext`] and denies (`403`) any request whose role lacks the permission.
//!
//! Tokens are compared in constant time (via `serve::auth::constant_time_eq`)
//! and never appear in `{:?}` output — [`PrincipalSpec`]'s `Debug` masks them,
//! and the server registers every token with the redaction writer at startup.

use crate::error::{CliError, CliResult};
use axum::http::Method;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// A discrete capability a route requires. Roles grant a fixed set of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    /// Read run records / logs (`GET /v1/runs*`).
    RunRead,
    /// Submit / cancel / delete runs (`POST`/`DELETE /v1/runs*`).
    RunWrite,
    /// Read the connector/transform schema catalog (`GET /v1/schemas*`).
    SchemaRead,
    /// Run the preflight probe (`POST /v1/doctor`).
    Doctor,
    /// Fire an event-driven trigger (`POST`/`PUT /v1/triggers/{name}`).
    TriggerFire,
    /// Inspect a dead-letter-queue location (`POST /v1/dlq/inspect`) — read-only.
    DlqRead,
    /// Replay / discard dead-letter-queue envelopes
    /// (`POST /v1/dlq/replay`, `POST /v1/dlq/discard`).
    DlqManage,
    /// Read the Data Movement Catalog (`GET /v1/catalog/*`, #279) — read-only.
    CatalogRead,
    /// Read the pipeline template registry (`GET /v1/templates*`, #444) —
    /// read-only. Also required to *resolve* a template when triggering a run.
    TemplateRead,
    /// Manage the pipeline template registry's lifecycle (#444, #698): register
    /// templates and versions, launch, roll back, deprecate, assign channels,
    /// delete, sync and publish. Admin-only: a launch changes what every
    /// unpinned caller runs, so it is a release decision, not an operator's.
    /// Triggering a registered template is `RunWrite`.
    TemplateAdmin,
    /// Read the local sink output ledger (`GET /v1/local-outputs`, #587) —
    /// read-only, granted to every role from `viewer` up. It lists paths and
    /// sizes of the server's own output files, which anyone who can already read
    /// the catalog can see there too.
    LocalOutputRead,
    /// Delete local sink output files (`DELETE /v1/local-outputs/{id}`,
    /// `POST /v1/local-outputs/cleanup`, #587). Destructive, so `operator` up: a
    /// principal that can already submit runs can already write these files, and
    /// a `viewer` must never be able to delete data. Guarded further in the
    /// console — "clean all" needs an explicit confirm.
    LocalOutputManage,
    /// Undo a run (`POST /v1/runs/{id}/rollback`, #706) — admin-only: it
    /// rewrites destination rows and rewinds a bookmark, a release-grade
    /// decision rather than an operator's.
    Rollback,
    /// Read the audit log (`GET /v1/audit`) — admin-only.
    AuditRead,
    /// Hot-reload the server's `--default-config` (`POST /v1/reload`) — admin-only.
    Reload,
    /// Read the caller's own principal, role and permissions (`GET /v1/whoami`,
    /// #698) — every role, so a client can shape itself to what it may do.
    Identity,
    /// Plan a config without running it (`POST /v1/plan`, #283/#707): the
    /// resolved row, an offline sample pass, the schema delta and the
    /// downstream impact. Read-only — nothing is written, no connector runs
    /// against a destination — so every role from `viewer` up.
    Plan,
    /// Annotate a catalogued dataset with owners and declared consumers
    /// (`POST /v1/catalog/datasets/{id}/consumers`, #707). Changes shared
    /// metadata that impact reports name, so `operator` up; a viewer reads it.
    CatalogAnnotate,
}

impl Permission {
    /// Every permission, in declaration order.
    pub const ALL: [Permission; 18] = [
        Permission::RunRead,
        Permission::RunWrite,
        Permission::SchemaRead,
        Permission::Doctor,
        Permission::TriggerFire,
        Permission::DlqRead,
        Permission::DlqManage,
        Permission::CatalogRead,
        Permission::TemplateRead,
        Permission::TemplateAdmin,
        Permission::LocalOutputRead,
        Permission::LocalOutputManage,
        Permission::Rollback,
        Permission::AuditRead,
        Permission::Reload,
        Permission::Identity,
        Permission::Plan,
        Permission::CatalogAnnotate,
    ];
}

/// A named role. Roles are a fixed, built-in ladder — `viewer` ⊂ `operator` ⊂
/// `admin` — chosen so the common cases (read-only dashboard user, run
/// operator, full admin) need no custom permission wiring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Read-only: runs + logs + schemas.
    Viewer,
    /// Everything a viewer can do, plus submit/cancel/delete runs (including
    /// triggering registered templates), doctor, and firing triggers.
    Operator,
    /// Full access, including the template lifecycle and the audit log.
    Admin,
}

impl Role {
    /// Whether this role grants `perm`.
    pub fn grants(self, perm: Permission) -> bool {
        use Permission::*;
        match self {
            Role::Viewer => {
                matches!(
                    perm,
                    RunRead
                        | SchemaRead
                        | DlqRead
                        | CatalogRead
                        | TemplateRead
                        | LocalOutputRead
                        | Identity
                        | Plan
                )
            }
            Role::Operator => {
                matches!(
                    perm,
                    RunRead
                        | SchemaRead
                        | DlqRead
                        | CatalogRead
                        | TemplateRead
                        | RunWrite
                        | Doctor
                        | TriggerFire
                        | DlqManage
                        | LocalOutputRead
                        | LocalOutputManage
                        | Identity
                        | Plan
                        | CatalogAnnotate
                )
            }
            Role::Admin => true,
        }
    }

    /// Every permission this role grants, in declaration order.
    pub fn permissions(self) -> Vec<Permission> {
        Permission::ALL
            .into_iter()
            .filter(|p| self.grants(*p))
            .collect()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
            Role::Admin => "admin",
        }
    }
}

/// One principal entry in an `--auth-config` file: a human-readable `name`, its
/// bearer `token`, and the `role` it is granted.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalSpec {
    pub name: String,
    pub token: String,
    pub role: Role,
}

// Hand-written Debug so a `{:?}` of a spec (or the RbacConfig embedding it) never
// prints the bearer token in clear — mirrors `AuthMode`'s masking.
impl std::fmt::Debug for PrincipalSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrincipalSpec")
            .field("name", &self.name)
            .field("token", &"***")
            .field("role", &self.role)
            .finish()
    }
}

/// File shape for `--auth-config` (`{ principals: [ … ] }`), parsed from YAML or
/// JSON (YAML is a JSON superset, so one parser handles both).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthConfigFile {
    principals: Vec<PrincipalSpec>,
}

/// A validated RBAC configuration: a non-empty set of principals with unique
/// names and unique, non-empty tokens.
#[derive(Debug, Clone)]
pub struct RbacConfig {
    principals: Vec<PrincipalSpec>,
}

/// The resolved identity for one request, carried in the request extensions for
/// handlers (and the audit writer) to read. Holds no token.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub principal: String,
    pub role: Role,
    pub source_ip: Option<String>,
}

impl AuthContext {
    /// Actor for a trigger-originated (non-HTTP) submission — `trigger:<name>`,
    /// treated as an operator for audit attribution.
    pub fn trigger(name: &str) -> Self {
        Self {
            principal: format!("trigger:{name}"),
            role: Role::Operator,
            source_ip: None,
        }
    }

    /// Actor for an event the running pipeline itself raised — a data-flow
    /// policy's runtime backstop denying a page (#702). Attributed to
    /// `runtime`, never to the principal who submitted the run.
    pub fn runtime() -> Self {
        Self {
            principal: "runtime".to_string(),
            role: Role::Operator,
            source_ip: None,
        }
    }
}

impl RbacConfig {
    /// Load + validate an `--auth-config` file (YAML or JSON).
    pub fn from_file(path: &Path) -> CliResult<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            CliError::Serve(format!("reading --auth-config {}: {e}", path.display()))
        })?;
        let file: AuthConfigFile = serde_yaml::from_str(&text).map_err(|e| {
            CliError::Serve(format!("parsing --auth-config {}: {e}", path.display()))
        })?;
        Self::new(file.principals)
    }

    /// Build an in-memory config from the `--read-token` / `--write-token` /
    /// `--admin-token` trio (#608).
    ///
    /// The ergonomic form of the common split — dashboards read, operators
    /// write, one admin — with no file to author. Returns `None` when none of
    /// the three is set, so the caller falls through to the other auth modes.
    ///
    /// Any subset may be set: a deployment that only ever hands out a read
    /// token and an admin token should not have to invent an operator one.
    /// The same validation as a file config applies, so an empty or reused
    /// token is rejected at startup rather than silently granting the wrong
    /// role.
    pub fn from_token_trio(
        read: Option<&str>,
        write: Option<&str>,
        admin: Option<&str>,
    ) -> CliResult<Option<Self>> {
        let wanted = [
            ("read", read, Role::Viewer),
            ("write", write, Role::Operator),
            ("admin", admin, Role::Admin),
        ];
        let principals: Vec<PrincipalSpec> = wanted
            .iter()
            .filter_map(|(name, token, role)| {
                token.map(|t| PrincipalSpec {
                    name: (*name).to_string(),
                    token: t.to_string(),
                    role: *role,
                })
            })
            .collect();
        if principals.is_empty() {
            return Ok(None);
        }
        for p in &principals {
            if p.token.trim().is_empty() {
                return Err(CliError::Serve(format!(
                    "--{}-token must not be empty (omit the flag to not issue that token, \
                     or use --no-auth to disable authentication entirely)",
                    p.name
                )));
            }
        }
        Self::new(principals).map(Some)
    }

    /// Build from an already-parsed principal list, validating invariants.
    pub fn new(principals: Vec<PrincipalSpec>) -> CliResult<Self> {
        if principals.is_empty() {
            return Err(CliError::Serve(
                "--auth-config must define at least one principal".into(),
            ));
        }
        let mut seen_names = std::collections::HashSet::new();
        let mut seen_tokens = std::collections::HashSet::new();
        for p in &principals {
            if p.name.trim().is_empty() {
                return Err(CliError::Serve(
                    "--auth-config: every principal must have a non-empty name".into(),
                ));
            }
            if p.token.is_empty() {
                return Err(CliError::Serve(format!(
                    "--auth-config: principal '{}' has an empty token",
                    p.name
                )));
            }
            if !seen_names.insert(p.name.clone()) {
                return Err(CliError::Serve(format!(
                    "--auth-config: duplicate principal name '{}'",
                    p.name
                )));
            }
            if !seen_tokens.insert(p.token.clone()) {
                return Err(CliError::Serve(format!(
                    "--auth-config: principal '{}' reuses a token already assigned to another \
                     principal",
                    p.name
                )));
            }
        }
        Ok(Self { principals })
    }

    /// Resolve a bearer token to its principal in constant time. Every principal
    /// is compared (no early return) so the match position doesn't leak via
    /// timing; the matched role/name is returned after the full scan.
    pub fn authenticate(&self, token: &str) -> Option<AuthContext> {
        let mut matched: Option<(&str, Role)> = None;
        for p in &self.principals {
            if crate::serve::auth::constant_time_eq(token.as_bytes(), p.token.as_bytes()) {
                matched = Some((p.name.as_str(), p.role));
            }
        }
        matched.map(|(name, role)| AuthContext {
            principal: name.to_string(),
            role,
            source_ip: None,
        })
    }

    /// Every configured token, for redaction registration at startup.
    pub fn tokens(&self) -> impl Iterator<Item = &str> {
        self.principals.iter().map(|p| p.token.as_str())
    }
}

/// The permission a `(method, matched-route-template)` pair requires. `None`
/// means the route has no specific mapping and is therefore admin-only (fail
/// closed for any route added without an explicit entry here).
pub fn required_permission(method: &Method, matched_path: &str) -> Option<Permission> {
    use Permission::*;
    match (method, matched_path) {
        (&Method::POST, "/v1/runs") => Some(RunWrite),
        (&Method::GET, "/v1/runs") => Some(RunRead),
        (&Method::GET, "/v1/runs/{id}") => Some(RunRead),
        (&Method::DELETE, "/v1/runs/{id}") => Some(RunWrite),
        (&Method::POST, "/v1/runs/{id}/cancel") => Some(RunWrite),
        (&Method::GET, "/v1/runs/{id}/logs") => Some(RunRead),
        (&Method::GET, "/v1/schemas") => Some(SchemaRead),
        (&Method::GET, "/v1/schemas/{kind}/{name}") => Some(SchemaRead),
        (&Method::POST, "/v1/doctor") => Some(Doctor),
        (&Method::POST, "/v1/backfill") => Some(RunWrite),
        // Content verification (#701): a repair writes through the sink, so
        // the whole endpoint is `RunWrite` (operator+).
        (&Method::POST, "/v1/verify") => Some(RunWrite),
        // Plan (#283/#707) is a pure read: no sink is written, no run starts.
        (&Method::POST, "/v1/plan") => Some(Plan),
        (&Method::POST, "/v1/runs/{id}/rollback") => Some(Rollback),
        (&Method::POST, "/v1/dlq/inspect") => Some(DlqRead),
        (&Method::POST, "/v1/dlq/replay") => Some(DlqManage),
        (&Method::POST, "/v1/dlq/discard") => Some(DlqManage),
        (&Method::GET, "/v1/audit") => Some(AuditRead),
        (&Method::POST, "/v1/triggers/{name}") => Some(TriggerFire),
        (&Method::PUT, "/v1/triggers/{name}") => Some(TriggerFire),
        (&Method::GET, "/v1/catalog/datasets") => Some(CatalogRead),
        (&Method::GET, "/v1/catalog/datasets/{id}") => Some(CatalogRead),
        (&Method::GET, "/v1/catalog/lineage") => Some(CatalogRead),
        (&Method::POST, "/v1/catalog/datasets/{id}/consumers") => Some(CatalogAnnotate),
        // Local sink output retention (#587). Listing is a read scope; deleting
        // files is `LocalOutputManage`, so a `viewer` can see what local data
        // exists but can never remove it.
        (&Method::GET, "/v1/local-outputs") => Some(LocalOutputRead),
        (&Method::DELETE, "/v1/local-outputs/{id}") => Some(LocalOutputManage),
        (&Method::POST, "/v1/local-outputs/cleanup") => Some(LocalOutputManage),
        // Dataset preview (#586) is a *read* of an output's contents, so it rides
        // `LocalOutputRead` (viewer+) rather than earning a scope of its own: a
        // viewer can already read run logs, which carry record data, and the real
        // gate on this endpoint is the server-level `--preview-local-outputs`
        // opt-in — a permission split would suggest per-principal control the
        // fixed role ladder does not offer.
        (&Method::GET, "/v1/local-outputs/{id}/preview") => Some(LocalOutputRead),
        // Pipeline templates (#444). Triggering maps to `RunWrite` — it starts a
        // run, which is the privileged half; `operator` holds both scopes, so a
        // single check suffices and a `viewer` can browse but never trigger.
        (&Method::POST, "/v1/templates") => Some(TemplateAdmin),
        (&Method::GET, "/v1/templates") => Some(TemplateRead),
        (&Method::GET, "/v1/templates/matrix") => Some(TemplateRead),
        (&Method::GET, "/v1/templates/{id}") => Some(TemplateRead),
        (&Method::DELETE, "/v1/templates/{id}") => Some(TemplateAdmin),
        (&Method::POST, "/v1/templates/{id}/runs") => Some(RunWrite),
        (&Method::POST, "/v1/templates/{id}/tags") => Some(TemplateAdmin),
        (&Method::POST, "/v1/templates/{id}/launch") => Some(TemplateAdmin),
        (&Method::POST, "/v1/templates/{id}/rollback") => Some(TemplateAdmin),
        (&Method::POST, "/v1/templates/{id}/deprecate") => Some(TemplateAdmin),
        (&Method::POST, "/v1/templates/{id}/versions/{version}/deprecate") => Some(TemplateAdmin),
        (&Method::POST, "/v1/templates/sync") => Some(TemplateAdmin),
        (&Method::POST, "/v1/templates/{id}/publish") => Some(TemplateAdmin),
        (&Method::POST, "/v1/reload") => Some(Reload),
        (&Method::GET, "/v1/whoami") => Some(Identity),
        // MCP endpoint (#420): baseline access needs only a read scope (Viewer+);
        // the mutating `run_pipeline` tool is separately gated on RunWrite inside
        // the handler.
        (&Method::POST, "/mcp") => Some(SchemaRead),
        _ => None,
    }
}

/// A short, stable audit action label for a `(method, matched-route)` pair.
pub fn audit_action(method: &Method, matched_path: &str) -> &'static str {
    match (method, matched_path) {
        (&Method::POST, "/v1/runs") => "run.submit",
        (&Method::GET, "/v1/runs") => "run.list",
        (&Method::GET, "/v1/runs/{id}") => "run.get",
        (&Method::DELETE, "/v1/runs/{id}") => "run.delete",
        (&Method::POST, "/v1/runs/{id}/cancel") => "run.cancel",
        (&Method::GET, "/v1/runs/{id}/logs") => "run.logs",
        (&Method::GET, "/v1/schemas") => "schema.list",
        (&Method::GET, "/v1/schemas/{kind}/{name}") => "schema.get",
        (&Method::POST, "/v1/doctor") => "doctor",
        (&Method::POST, "/v1/backfill") => "backfill.submit",
        (&Method::POST, "/v1/verify") => "verify",
        (&Method::POST, "/v1/plan") => "plan",
        (&Method::POST, "/v1/runs/{id}/rollback") => "run.rollback",
        (&Method::POST, "/v1/dlq/inspect") => "dlq.inspect",
        (&Method::POST, "/v1/dlq/replay") => "dlq.replay",
        (&Method::POST, "/v1/dlq/discard") => "dlq.discard",
        (&Method::GET, "/v1/audit") => "audit.list",
        (&Method::POST | &Method::PUT, "/v1/triggers/{name}") => "trigger.fire",
        (&Method::GET, "/v1/catalog/datasets") => "catalog.list",
        (&Method::GET, "/v1/catalog/datasets/{id}") => "catalog.get",
        (&Method::GET, "/v1/catalog/lineage") => "catalog.lineage",
        (&Method::POST, "/v1/catalog/datasets/{id}/consumers") => "catalog.annotate",
        (&Method::GET, "/v1/local-outputs") => "local_output.list",
        (&Method::DELETE, "/v1/local-outputs/{id}") => "local_output.delete",
        (&Method::POST, "/v1/local-outputs/cleanup") => "local_output.cleanup",
        (&Method::GET, "/v1/local-outputs/{id}/preview") => "local_output.preview",
        (&Method::POST, "/v1/templates") => "template.register",
        (&Method::GET, "/v1/templates") => "template.list",
        (&Method::GET, "/v1/templates/{id}") => "template.get",
        (&Method::DELETE, "/v1/templates/{id}") => "template.delete",
        (&Method::POST, "/v1/templates/{id}/runs") => "template.run",
        (&Method::POST, "/v1/templates/{id}/tags") => "template.promote",
        (&Method::POST, "/v1/templates/{id}/launch") => "template.launch",
        (&Method::POST, "/v1/templates/{id}/rollback") => "template.rollback",
        (&Method::POST, "/v1/templates/{id}/deprecate") => "template.deprecate",
        (&Method::POST, "/v1/templates/{id}/versions/{version}/deprecate") => {
            "template.version_deprecate"
        }
        (&Method::POST, "/v1/templates/sync") => "template.sync",
        (&Method::POST, "/v1/templates/{id}/publish") => "template.publish",
        (&Method::POST, "/v1/reload") => "config.reload",
        (&Method::GET, "/v1/whoami") => "whoami",
        (&Method::POST, "/mcp") => "mcp",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, token: &str, role: Role) -> PrincipalSpec {
        PrincipalSpec {
            name: name.into(),
            token: token.into(),
            role,
        }
    }

    #[test]
    fn role_permission_ladder() {
        use Permission::*;
        // Viewer: reads only.
        assert!(Role::Viewer.grants(RunRead));
        assert!(Role::Viewer.grants(SchemaRead));
        assert!(Role::Viewer.grants(DlqRead));
        assert!(Role::Viewer.grants(TemplateRead));
        assert!(!Role::Viewer.grants(RunWrite));
        assert!(!Role::Viewer.grants(Doctor));
        assert!(!Role::Viewer.grants(DlqManage));
        assert!(!Role::Viewer.grants(AuditRead));
        assert!(!Role::Viewer.grants(TemplateAdmin));
        // Operator: reads + writes + doctor + triggers + dlq management, but not
        // audit and not the template lifecycle (#698).
        assert!(Role::Operator.grants(RunWrite));
        assert!(Role::Operator.grants(Doctor));
        assert!(Role::Operator.grants(TriggerFire));
        assert!(Role::Operator.grants(DlqRead));
        assert!(Role::Operator.grants(DlqManage));
        assert!(!Role::Operator.grants(TemplateAdmin));
        assert!(!Role::Operator.grants(AuditRead));
        // Admin: everything.
        for p in [
            RunRead,
            RunWrite,
            SchemaRead,
            Doctor,
            TriggerFire,
            DlqRead,
            DlqManage,
            AuditRead,
            TemplateRead,
            TemplateAdmin,
        ] {
            assert!(Role::Admin.grants(p));
        }
    }

    #[test]
    fn authenticate_resolves_token_to_principal() {
        let cfg = RbacConfig::new(vec![
            spec("alice", "tok-a", Role::Admin),
            spec("bob", "tok-b", Role::Viewer),
        ])
        .unwrap();
        let a = cfg.authenticate("tok-a").unwrap();
        assert_eq!(a.principal, "alice");
        assert_eq!(a.role, Role::Admin);
        let b = cfg.authenticate("tok-b").unwrap();
        assert_eq!(b.role, Role::Viewer);
        assert!(cfg.authenticate("nope").is_none());
    }

    #[test]
    fn rejects_empty_duplicate_and_blank() {
        assert!(RbacConfig::new(vec![]).is_err());
        assert!(RbacConfig::new(vec![spec("", "t", Role::Admin)]).is_err());
        assert!(RbacConfig::new(vec![spec("a", "", Role::Admin)]).is_err());
        // Duplicate name.
        assert!(
            RbacConfig::new(vec![
                spec("a", "t1", Role::Admin),
                spec("a", "t2", Role::Viewer),
            ])
            .is_err()
        );
        // Duplicate token.
        assert!(
            RbacConfig::new(vec![
                spec("a", "dup", Role::Admin),
                spec("b", "dup", Role::Viewer),
            ])
            .is_err()
        );
    }

    #[test]
    fn debug_masks_token() {
        let s = format!("{:?}", spec("alice", "supersecret", Role::Admin));
        assert!(!s.contains("supersecret"), "token leaked: {s}");
        assert!(s.contains("***"));
    }

    #[test]
    fn trigger_actor_is_operator() {
        let ctx = AuthContext::trigger("nightly");
        assert_eq!(ctx.principal, "trigger:nightly");
        assert_eq!(ctx.role, Role::Operator);
        assert!(ctx.source_ip.is_none());
    }

    #[test]
    fn tokens_iterates_all_principals() {
        let cfg = RbacConfig::new(vec![
            spec("a", "t1", Role::Admin),
            spec("b", "t2", Role::Viewer),
        ])
        .unwrap();
        let toks: Vec<&str> = cfg.tokens().collect();
        assert_eq!(toks, vec!["t1", "t2"]);
    }

    #[test]
    fn required_permission_covers_all_routes() {
        use Permission::*;
        for (m, path, want) in [
            (Method::GET, "/v1/runs/{id}", RunRead),
            (Method::DELETE, "/v1/runs/{id}", RunWrite),
            (Method::POST, "/v1/runs/{id}/cancel", RunWrite),
            (Method::GET, "/v1/runs/{id}/logs", RunRead),
            (Method::GET, "/v1/schemas", SchemaRead),
            (Method::GET, "/v1/schemas/{kind}/{name}", SchemaRead),
            (Method::POST, "/v1/doctor", Doctor),
            (Method::POST, "/v1/triggers/{name}", TriggerFire),
            (Method::PUT, "/v1/triggers/{name}", TriggerFire),
            (Method::POST, "/v1/backfill", RunWrite),
            (Method::POST, "/v1/dlq/inspect", DlqRead),
            (Method::POST, "/v1/dlq/replay", DlqManage),
            (Method::POST, "/v1/dlq/discard", DlqManage),
            (Method::GET, "/v1/catalog/datasets", CatalogRead),
            (Method::GET, "/v1/catalog/datasets/{id}", CatalogRead),
            (Method::GET, "/v1/catalog/lineage", CatalogRead),
            (Method::GET, "/v1/local-outputs", LocalOutputRead),
            (Method::DELETE, "/v1/local-outputs/{id}", LocalOutputManage),
            (Method::POST, "/v1/local-outputs/cleanup", LocalOutputManage),
            (
                Method::GET,
                "/v1/local-outputs/{id}/preview",
                LocalOutputRead,
            ),
            (Method::POST, "/v1/verify", RunWrite),
            (Method::POST, "/v1/plan", Plan),
            (
                Method::POST,
                "/v1/catalog/datasets/{id}/consumers",
                CatalogAnnotate,
            ),
            (Method::POST, "/v1/runs/{id}/rollback", Rollback),
            (Method::POST, "/v1/templates", TemplateAdmin),
            (Method::GET, "/v1/templates", TemplateRead),
            (Method::GET, "/v1/templates/{id}", TemplateRead),
            (Method::DELETE, "/v1/templates/{id}", TemplateAdmin),
            (Method::POST, "/v1/templates/{id}/runs", RunWrite),
            (Method::POST, "/v1/templates/{id}/tags", TemplateAdmin),
            (Method::POST, "/v1/templates/{id}/launch", TemplateAdmin),
            (Method::POST, "/v1/templates/{id}/rollback", TemplateAdmin),
            (Method::POST, "/v1/templates/{id}/deprecate", TemplateAdmin),
            (
                Method::POST,
                "/v1/templates/{id}/versions/{version}/deprecate",
                TemplateAdmin,
            ),
            (Method::POST, "/v1/templates/sync", TemplateAdmin),
            (Method::POST, "/v1/templates/{id}/publish", TemplateAdmin),
            (Method::POST, "/v1/reload", Reload),
            (Method::GET, "/v1/whoami", Identity),
        ] {
            assert_eq!(required_permission(&m, path), Some(want), "{m} {path}");
        }
        // Reload is admin-only.
        assert!(!Role::Viewer.grants(Permission::Reload));
        assert!(!Role::Operator.grants(Permission::Reload));
        assert!(Role::Admin.grants(Permission::Reload));
        // Plan is a read (viewer+); annotating shared metadata is operator+.
        assert!(Role::Viewer.grants(Permission::Plan));
        assert!(!Role::Viewer.grants(Permission::CatalogAnnotate));
        assert!(Role::Operator.grants(Permission::CatalogAnnotate));
        // Every role can read the catalog; a viewer still can't write runs.
        assert!(Role::Viewer.grants(Permission::CatalogRead));
        assert!(Role::Operator.grants(Permission::CatalogRead));
        assert!(Role::Admin.grants(Permission::CatalogRead));
        // A viewer can see what local data exists but must never delete it —
        // "delete now" / "purge" / "clean all" are operator-and-up (#587/#588).
        assert!(Role::Viewer.grants(Permission::LocalOutputRead));
        assert!(!Role::Viewer.grants(Permission::LocalOutputManage));
        assert!(Role::Operator.grants(Permission::LocalOutputManage));
        assert!(Role::Admin.grants(Permission::LocalOutputManage));
    }

    #[test]
    fn local_output_routes_have_distinct_audit_actions() {
        // Every destructive control-plane action must be attributable in the
        // audit log, and two actions must never share a label.
        let actions = [
            audit_action(&Method::GET, "/v1/local-outputs"),
            audit_action(&Method::DELETE, "/v1/local-outputs/{id}"),
            audit_action(&Method::POST, "/v1/local-outputs/cleanup"),
            audit_action(&Method::GET, "/v1/local-outputs/{id}/preview"),
        ];
        assert_eq!(
            actions,
            [
                "local_output.list",
                "local_output.delete",
                "local_output.cleanup",
                "local_output.preview"
            ]
        );
        assert!(actions.iter().all(|a| *a != "unknown"));
    }

    #[test]
    fn role_and_permission_serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&Role::Operator).unwrap(),
            "\"operator\""
        );
        assert_eq!(
            serde_json::to_string(&Permission::AuditRead).unwrap(),
            "\"audit_read\""
        );
    }

    #[test]
    fn template_lifecycle_is_admin_only_and_triggering_is_operator() {
        use Permission::*;
        for role in [Role::Viewer, Role::Operator] {
            assert!(!role.grants(TemplateAdmin), "{role:?}");
        }
        assert!(Role::Admin.grants(TemplateAdmin));
        assert!(!Role::Viewer.grants(RunWrite));
        assert!(Role::Operator.grants(RunWrite));
        assert!(Role::Operator.grants(TemplateRead));
    }

    #[test]
    fn every_role_can_read_its_own_identity_and_lists_exactly_what_it_grants() {
        for role in [Role::Viewer, Role::Operator, Role::Admin] {
            assert!(role.grants(Permission::Identity), "{role:?}");
            let listed = role.permissions();
            for p in Permission::ALL {
                assert_eq!(listed.contains(&p), role.grants(p), "{role:?} {p:?}");
            }
        }
        assert_eq!(Role::Admin.permissions().len(), Permission::ALL.len());
        assert!(
            !Role::Viewer
                .permissions()
                .contains(&Permission::TemplateAdmin)
        );
    }

    #[test]
    fn required_permission_maps_routes() {
        assert_eq!(
            required_permission(&Method::POST, "/v1/runs"),
            Some(Permission::RunWrite)
        );
        assert_eq!(
            required_permission(&Method::GET, "/v1/runs"),
            Some(Permission::RunRead)
        );
        assert_eq!(
            required_permission(&Method::GET, "/v1/audit"),
            Some(Permission::AuditRead)
        );
        // Unmapped → admin-only (None).
        assert_eq!(required_permission(&Method::GET, "/v1/unknown"), None);
    }

    #[test]
    fn parses_yaml_and_json() {
        let yaml = "principals:\n  - name: alice\n    token: tok-a\n    role: admin\n";
        let cfg: AuthConfigFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.principals.len(), 1);
        let json = r#"{"principals":[{"name":"bob","token":"tok-b","role":"viewer"}]}"#;
        let cfg: AuthConfigFile = serde_yaml::from_str(json).unwrap();
        assert_eq!(cfg.principals[0].role, Role::Viewer);
    }

    #[test]
    fn audit_action_labels() {
        assert_eq!(audit_action(&Method::POST, "/v1/runs"), "run.submit");
        assert_eq!(
            audit_action(&Method::POST, "/v1/runs/{id}/cancel"),
            "run.cancel"
        );
        assert_eq!(
            audit_action(&Method::POST, "/v1/templates"),
            "template.register"
        );
        assert_eq!(
            audit_action(&Method::POST, "/v1/templates/{id}/runs"),
            "template.run"
        );
        assert_eq!(audit_action(&Method::GET, "/v1/whatever"), "unknown");
    }
}

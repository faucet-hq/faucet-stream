use super::*;

#[test]
#[serial_test::serial(env)]
fn an_auth_config_file_resolves_and_protects_its_references() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.yaml");
    let tok = dir.path().join("tok");
    std::fs::write(&tok, "tok-from-file").unwrap();
    // SAFETY: a variable only this test reads.
    unsafe { std::env::set_var("FAUCET_RBAC_UNIT_TOKEN", "tok-from-env") };
    std::fs::write(
            &path,
            format!(
                "principals:\n  - {{ name: a, token: \"${{secret:FAUCET_RBAC_UNIT_TOKEN}}\", role: admin }}\n  - {{ name: b, token: \"${{file:{}}}\", role: viewer }}\n",
                tok.display()
            ),
        )
        .unwrap();
    let cfg = RbacConfig::from_file(&path).unwrap();
    assert_eq!(cfg.authenticate("tok-from-env").unwrap().principal, "a");
    assert_eq!(
        cfg.authenticate("tok-from-file").unwrap().role,
        Role::Viewer
    );
    let refs = crate::serve::load::server_secret_refs(&serde_json::json!([
        "${env:FAUCET_RBAC_UNIT_TOKEN}",
        format!("${{file:{}}}", tok.display())
    ]));
    assert_eq!(refs.len(), 2, "{refs:?}");

    assert!(RbacConfig::from_file(&dir.path().join("missing.yaml")).is_err());
    std::fs::write(&path, "principals: [").unwrap();
    assert!(RbacConfig::from_file(&path).is_err());
    std::fs::write(
        &path,
        "principals:\n  - { name: a, token: \"${vault:kv/x#t}\", role: admin }\n",
    )
    .unwrap();
    let err = RbacConfig::from_file(&path).unwrap_err().to_string();
    let expected = if cfg!(feature = "secrets-vault") {
        "auth-config"
    } else {
        "vault"
    };
    assert!(err.contains(expected), "{err}");

    std::fs::write(&path, "principals: 5\n").unwrap();
    let err = RbacConfig::from_file(&path).unwrap_err().to_string();
    assert!(err.contains("parsing --auth-config"), "{err}");
}

fn spec(name: &str, token: &str, role: Role) -> PrincipalSpec {
    PrincipalSpec {
        name: name.into(),
        token: token.into(),
        role,
        tenant: None,
    }
}

#[test]
fn tenant_scoped_principals_reach_only_their_tenant() {
    use TenantScopeDecision::*;
    let d = |m: Method, mp: &str, p: &str| tenant_scope_decision(&m, mp, p, "acme");
    assert_eq!(path_tenant("/v1/tenants/acme/runs"), Some("acme"));
    assert_eq!(path_tenant("/v1/tenants/acme"), Some("acme"));
    assert_eq!(path_tenant("/v1/tenants/"), None);
    assert_eq!(path_tenant("/v1/runs"), None);
    assert_eq!(
        d(
            Method::POST,
            "/v1/tenants/{tenant}/runs",
            "/v1/tenants/acme/runs"
        ),
        Allow
    );
    assert_eq!(
        d(Method::GET, "/v1/tenants/{tenant}", "/v1/tenants/acme"),
        Allow
    );
    assert_eq!(
        d(
            Method::POST,
            "/v1/tenants/{tenant}/runs",
            "/v1/tenants/other/runs"
        ),
        NotFound
    );
    assert_eq!(
        d(Method::DELETE, "/v1/tenants/{tenant}", "/v1/tenants/acme"),
        Deny
    );
    assert_eq!(
        d(Method::PATCH, "/v1/tenants/{tenant}", "/v1/tenants/acme"),
        Deny
    );
    assert_eq!(d(Method::POST, "/v1/runs", "/v1/runs"), Allow);
    assert_eq!(d(Method::GET, "/v1/usage", "/v1/usage"), Allow);
    assert_eq!(d(Method::GET, "/v1/overview", "/v1/overview"), Allow);
    assert_eq!(
        d(Method::GET, "/v1/templates/{id}", "/v1/templates/x"),
        Allow
    );
    assert_eq!(d(Method::POST, "/v1/templates", "/v1/templates"), Deny);
    assert_eq!(d(Method::POST, "/v1/tenants", "/v1/tenants"), Deny);
    assert_eq!(d(Method::GET, "/v1/audit", "/v1/audit"), Deny);
    assert_eq!(
        d(
            Method::POST,
            "/v1/templates/{id}/fanout",
            "/v1/templates/x/fanout"
        ),
        Deny
    );
}

#[test]
fn a_principal_tenant_must_be_a_slug_and_reaches_its_context() {
    let mut p = spec("a", "tok", Role::Operator);
    p.tenant = Some("Bad Tenant".into());
    assert!(RbacConfig::new(vec![p]).is_err());
    let mut p = spec("a", "tok", Role::Operator);
    p.tenant = Some("acme".into());
    assert!(format!("{p:?}").contains("acme"));
    let cfg = RbacConfig::new(vec![p]).unwrap();
    assert_eq!(
        cfg.authenticate("tok").unwrap().tenant.as_deref(),
        Some("acme")
    );
}

#[test]
fn tenant_filters_and_visibility() {
    let scoped = AuthContext {
        principal: "p".into(),
        role: Role::Operator,
        source_ip: None,
        tenant: Some("acme".into()),
    };
    assert_eq!(scoped.tenant_filter(None).unwrap(), Some("acme".into()));
    assert_eq!(
        scoped.tenant_filter(Some("acme".into())).unwrap(),
        Some("acme".into())
    );
    assert!(scoped.tenant_filter(Some("other".into())).is_err());
    assert!(scoped.sees_tenant(Some("acme")));
    assert!(!scoped.sees_tenant(Some("other")));
    assert!(!scoped.sees_tenant(None));
    let global = AuthContext {
        tenant: None,
        ..scoped
    };
    assert_eq!(global.tenant_filter(None).unwrap(), None);
    assert_eq!(
        global.tenant_filter(Some("x".into())).unwrap(),
        Some("x".into())
    );
    assert!(global.sees_tenant(None));
    assert!(global.sees_tenant(Some("x")));
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
    for reserved in ["runtime", "system:x", "trigger:t", "sync:origin"] {
        let err = RbacConfig::new(vec![spec(reserved, "t", Role::Operator)])
            .unwrap_err()
            .to_string();
        assert!(err.contains("reserved"), "{reserved}: {err}");
    }
    RbacConfig::new(vec![spec("ci:deploy", "t", Role::Operator)])
        .expect("other names with a colon stay valid");
    assert!(is_server_identity("system:tenants"));
    assert!(!is_server_identity("ci:deploy"));
    assert!(!is_server_identity("runtime-ops"));
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
        (Method::GET, "/v1/usage", UsageRead),
        (Method::GET, "/v1/overview", RunRead),
        (Method::POST, "/v1/changes", ChangeRequest),
        (Method::GET, "/v1/changes", ChangeRead),
        (Method::GET, "/v1/changes/{id}", ChangeRead),
        (Method::POST, "/v1/changes/{id}/approve", ChangeApprove),
        (Method::POST, "/v1/changes/{id}/reject", ChangeApprove),
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
        (Method::GET, "/v1/templates/{id}/rows", TemplateRead),
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
        (
            Method::POST,
            "/v1/templates/{id}/versions/{version}/test",
            TemplateAdmin,
        ),
        (
            Method::GET,
            "/v1/templates/{id}/versions/{version}/tests",
            TemplateRead,
        ),
        (Method::POST, "/v1/templates/sync", TemplateAdmin),
        (Method::POST, "/v1/templates/{id}/publish", TemplateAdmin),
        (Method::POST, "/v1/reload", Reload),
        (Method::GET, "/v1/whoami", Identity),
        (Method::GET, "/v1/status", StatusRead),
        (Method::POST, "/v1/status", StatusRead),
        (Method::GET, "/v1/mirror/{name}", StatusRead),
        (Method::POST, "/v1/mirror/{name}", StatusRead),
        (Method::GET, "/v1/state/{pipeline}/{row}", StateAdmin),
        (Method::PUT, "/v1/state/{pipeline}/{row}", StateAdmin),
        (Method::DELETE, "/v1/state/{pipeline}/{row}", StateAdmin),
    ] {
        assert_eq!(required_permission(&m, path), Some(want), "{m} {path}");
    }
    // Status is a read (viewer+); pipeline state is admin-only (#732/#735).
    assert!(Role::Viewer.grants(Permission::StatusRead));
    assert!(Role::Operator.grants(Permission::StatusRead));
    assert!(!Role::Viewer.grants(Permission::StateAdmin));
    assert!(!Role::Operator.grants(Permission::StateAdmin));
    assert!(Role::Admin.grants(Permission::StateAdmin));
    assert_eq!(audit_action(&Method::POST, "/v1/status"), "status");
    assert_eq!(
        audit_action(&Method::GET, "/v1/mirror/{name}"),
        "mirror.status"
    );
    assert_eq!(
        audit_action(&Method::GET, "/v1/state/{pipeline}/{row}"),
        "state.get"
    );
    assert_eq!(
        audit_action(&Method::PUT, "/v1/state/{pipeline}/{row}"),
        "state.set"
    );
    assert_eq!(
        audit_action(&Method::DELETE, "/v1/state/{pipeline}/{row}"),
        "state.reset"
    );
    // Reload is admin-only.
    assert!(!Role::Viewer.grants(Permission::Reload));
    assert!(!Role::Operator.grants(Permission::Reload));
    assert!(Role::Admin.grants(Permission::Reload));
    // Plan loads the submitted config on the server: operator+, like
    // doctor; annotating shared metadata is operator+ too.
    assert!(!Role::Viewer.grants(Permission::Plan));
    assert!(Role::Operator.grants(Permission::Plan));
    assert!(!Role::Viewer.grants(Permission::CatalogAnnotate));
    assert!(Role::Operator.grants(Permission::CatalogAnnotate));
    // Usage accounting is a priced read of the catalog's volumes: viewer+.
    assert!(Role::Viewer.grants(Permission::UsageRead));
    assert!(Role::Operator.grants(Permission::UsageRead));
    assert!(Role::Admin.grants(Permission::UsageRead));
    assert_eq!(audit_action(&Method::GET, "/v1/usage"), "usage.list");
    assert_eq!(audit_action(&Method::GET, "/v1/overview"), "overview.get");
    // Change requests (#703): everyone reads; proposing and approving are
    // operator scopes (the approvals policy narrows approving further).
    assert!(Role::Viewer.grants(Permission::ChangeRead));
    assert!(!Role::Viewer.grants(Permission::ChangeRequest));
    assert!(!Role::Viewer.grants(Permission::ChangeApprove));
    assert!(Role::Operator.grants(Permission::ChangeRequest));
    assert!(Role::Operator.grants(Permission::ChangeApprove));
    assert_eq!(
        audit_action(&Method::POST, "/v1/changes/{id}/approve"),
        "change.approve"
    );
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
    let cfg: AuthConfigFile = crate::yaml::from_str(yaml).unwrap();
    assert_eq!(cfg.principals.len(), 1);
    let json = r#"{"principals":[{"name":"bob","token":"tok-b","role":"viewer"}]}"#;
    let cfg: AuthConfigFile = crate::yaml::from_str(json).unwrap();
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

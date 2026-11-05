use super::*;
use crate::serve::test_support::test_state;

const SERVER: &str = r#"
version: 1
classifications:
  - label: pii.email
    fields: [email]
rules:
  - name: pii-to-eu-only
    when: { label: pii.email }
    require: { residency: [eu] }
    mask: [hash]
"#;

fn record(scope: &str, version: u32, text: &str) -> PolicyVersionRecord {
    let spec: PolicySpec = crate::yaml::from_str(text).unwrap();
    PolicyVersionRecord {
        scope: scope.into(),
        version,
        spec: serde_json::to_value(spec).unwrap(),
        created_at: Utc::now(),
        created_by: "alice".into(),
        approved_by: Some("bob".into()),
        reason: None,
        change_id: Some(format!("c{version}")),
        restores: None,
    }
}

#[test]
fn locate_finds_the_quoted_name() {
    let text = "a: 1\nrules:\n  - name: dup\n";
    assert_eq!(locate(text, "rule 'dup' is declared twice"), Some(3));
    assert_eq!(locate(text, "nothing quoted"), None);
    assert_eq!(locate(text, "rule `missing`"), None);
}

#[test]
fn parse_draft_reports_the_line() {
    let problems = parse_draft("version: 1\nrules:\n  - name: x\n    bogus: 1\n").unwrap_err();
    assert_eq!(problems.len(), 1);
    assert!(problems[0].line.is_some());
    assert!(parse_draft("  ").unwrap_err()[0].message.contains("empty"));
    assert_eq!(parse_draft(SERVER).unwrap().rules.len(), 1);
}

#[test]
fn line_diff_marks_changes() {
    let d = line_diff("a\nb\nc\n", "a\nc\nd\n");
    let ops: String = d.iter().map(|l| l.op).collect();
    assert_eq!(ops, " - +");
    assert_eq!(d[3].text, "d");
    assert!(line_diff("", "x").iter().all(|l| l.op == '+'));
    assert!(line_diff("x", "").iter().all(|l| l.op == '-'));
}

#[test]
fn violation_text_round_trip() {
    let text = "Policy `pii-to-eu-only` violated on column `email`: rule `pii-to-eu-only`: \
                column `email` — label `pii.email` requires sink `residency` in [eu]; sink \
                `backup` (s3) has `us`";
    let d = parse_violation_text(text);
    assert_eq!(d["rule"], "pii-to-eu-only");
    assert_eq!(d["column"], "email");
    assert_eq!(d["label"], "pii.email");
    assert_eq!(d["sink"], "residency");
    assert_eq!(
        parse_violation_text("nothing").as_object().unwrap().len(),
        0
    );
    let denied = "Policy `no-stdout` violated on column `email`: rule `no-stdout`: column \
                  `email` — label `pii.email` may not reach sink `out` (stdout)";
    let d = parse_violation_text(denied);
    assert_eq!(d["sink"], "out");
    assert_eq!(d["sink_kind"], "stdout");
}

#[test]
fn requires_and_outline() {
    let spec = parse_draft(SERVER).unwrap();
    let o = outline(&spec);
    assert_eq!(o[0].name, "pii-to-eu-only");
    assert_eq!(o[0].requires, "sink residency in [eu], or masked by hash");
    assert_eq!(o[0].on_runtime, "fail");
    let mut deny = spec.rules[0].clone();
    deny.deny = true;
    deny.mask.clear();
    assert_eq!(requires_text(&deny), "never (deny)");
    deny.mask = vec!["redact".into()];
    assert_eq!(requires_text(&deny), "only masked by redact");
}

#[test]
fn refusal_detail_carries_the_first_violation() {
    let v = faucet_core::Violation {
        rule: "r".into(),
        label: "l".into(),
        column: "c".into(),
        sink: "s".into(),
        sink_kind: "k".into(),
        kind: faucet_core::ViolationKind::Denied,
        conservative: false,
        satisfied_by_mask: Vec::new(),
    };
    let d: Value = serde_json::from_str(&refusal_detail("submit", Some(&v), "refused")).unwrap();
    assert_eq!(d["rule"], "r");
    assert_eq!(d["sink_kind"], "k");
    let d: Value = serde_json::from_str(&refusal_detail("runtime", None, "failed")).unwrap();
    assert!(d.get("rule").is_none());
    assert_eq!(d["phase"], "runtime");
}

#[test]
fn sink_attributes_and_schema_paths() {
    let doc = json!({
        "pipeline": {
            "sink": {"attributes": {"residency": "eu"}},
            "sinks": {"a": {"attributes": {"tier": "gold"}}, "b": {}}
        },
        "sink": {"attributes": {"region": "x"}}
    });
    assert_eq!(sink_attribute_maps(&doc).len(), 3);
    let mut paths = Vec::new();
    schema_paths(
        &json!({"properties": {"a": {"properties": {"b": {}}}, "c": {}}}),
        "",
        &mut paths,
    );
    assert_eq!(paths, vec!["a", "a.b", "c"]);
}

#[tokio::test]
async fn effective_merges_and_pins() {
    let state = test_state();
    let h = state.history();
    assert_eq!(effective(&state, None, None).await.unwrap().0, None);
    assert!(
        h.policy_version_insert(&record(SERVER_SCOPE, 1, SERVER))
            .await
            .unwrap()
    );
    // A second insert of the same version is refused, not overwritten.
    assert!(
        !h.policy_version_insert(&record(SERVER_SCOPE, 1, SERVER))
            .await
            .unwrap()
    );
    let tenant = r#"
version: 1
classifications:
  - label: acme.secret
    fields: [secret]
rules:
  - name: acme-secret
    when: { label: acme.secret }
    deny: true
"#;
    h.policy_version_insert(&record(&tenant_scope("acme"), 1, tenant))
        .await
        .unwrap();
    let (spec, pin) = effective(&state, Some("acme"), None).await.unwrap();
    assert_eq!(spec.unwrap().rules.len(), 2);
    assert_eq!(
        pin,
        PolicyPin {
            server: Some(1),
            tenant: Some(1)
        }
    );
    // A newer server version does not reach a run pinned to v1.
    let v2 = SERVER.replace("[eu]", "[eu, uk]");
    h.policy_version_insert(&record(SERVER_SCOPE, 2, &v2))
        .await
        .unwrap();
    let (spec, pin) = effective(
        &state,
        None,
        Some(&PolicyPin {
            server: Some(1),
            tenant: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(pin.server, Some(1));
    assert_eq!(spec.unwrap().rules[0].require["residency"], vec!["eu"]);
    assert_eq!(
        effective(&state, None, None).await.unwrap().1.server,
        Some(2)
    );
    // A pin to a version that is gone fails the load rather than run unchecked.
    let gone = effective(
        &state,
        None,
        Some(&PolicyPin {
            server: Some(9),
            tenant: None,
        }),
    )
    .await;
    assert!(matches!(gone, Err(ServeError::Unprocessable { .. })));
    // The layer view.
    let layer = server_layer(&state).await.unwrap();
    assert_eq!(layer.source, PolicySource::Server);
    assert_eq!(layer.version.unwrap().version, 2);
}

#[tokio::test]
async fn a_file_policy_wins_and_ignores_pins() {
    let state = test_state();
    let spec = parse_draft(SERVER).unwrap();
    state.set_policy(std::sync::Arc::new(spec));
    state.set_policy_file(PolicyFile {
        path: "p.yaml".into(),
        sha256: "x".into(),
        loaded_at: Utc::now(),
    });
    let (eff, pin) = effective(
        &state,
        None,
        Some(&PolicyPin {
            server: Some(4),
            tenant: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(eff.unwrap().rules.len(), 1);
    assert_eq!(pin.server, None);
    assert_eq!(
        server_layer(&state).await.unwrap().source,
        PolicySource::File
    );
}

#[tokio::test]
async fn tenant_drafts_may_only_add() {
    let state = test_state();
    state
        .history()
        .policy_version_insert(&record(SERVER_SCOPE, 1, SERVER))
        .await
        .unwrap();
    let layer = Layer::Tenant("acme".into());
    // Reusing a server rule name is refused.
    let clash = SERVER.replace(
        "pii.email\n    fields: [email]",
        "pii.other\n    fields: [x]",
    );
    let draft = parse_draft(&clash).unwrap();
    let problems = check_draft(&state, &layer, &draft, &clash).await.unwrap();
    assert!(problems[0].message.contains("tenant rules can only add"));
    // A tenant rule over a server label is fine (the merge classifies it).
    let ok = "version: 1\nrules:\n  - name: acme-email\n    when: { label: pii.email }\n    deny: true\n";
    let draft = parse_draft(ok).unwrap();
    assert!(
        check_draft(&state, &layer, &draft, ok)
            .await
            .unwrap()
            .is_empty()
    );
    // A label nobody classifies is refused, with a line.
    let bad = "version: 1\nrules:\n  - name: acme-x\n    when: { label: nope }\n    deny: true\n";
    let draft = parse_draft(bad).unwrap();
    let problems = check_draft(&state, &layer, &draft, bad).await.unwrap();
    assert!(problems[0].message.contains("nope"));
    assert_eq!(problems[0].line, Some(4));
    // A server draft is validated on its own.
    let empty_rule = "version: 1\nclassifications: []\nrules:\n  - name: ''\n    when: { label: a }\n    deny: true\n";
    let draft = parse_draft(empty_rule).unwrap();
    assert!(
        !check_draft(&state, &Layer::Server, &draft, empty_rule)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn refusals_read_back_from_the_audit_log() {
    let state = test_state();
    state
        .history()
        .policy_version_insert(&record(SERVER_SCOPE, 1, SERVER))
        .await
        .unwrap();
    let mut actor = crate::serve::rbac::AuthContext::system("t");
    actor.tenant = Some("acme".into());
    crate::serve::audit::record_detail(
        &state,
        &actor,
        "policy.denied",
        None,
        Some("fp".into()),
        Some("pipeline:p".into()),
        json!({"phase": "submit", "outcome": "refused", "rule": "pii-to-eu-only", "column": "email"}).to_string(),
        "denied",
    )
    .await;
    let runtime = crate::serve::rbac::AuthContext::runtime();
    crate::serve::audit::write(
        &state,
        &runtime,
        "policy.denied",
        Some("r1".into()),
        None,
        "denied",
    )
    .await;
    crate::serve::audit::write_target_detail(
        &state,
        &actor,
        "policy.denied",
        "template params".into(),
        "x".into(),
        "denied",
    )
    .await;
    let all = refusals(&state, None, None, 100).await.unwrap();
    assert_eq!(all.len(), 2);
    let submit = all.iter().find(|r| r.phase == "submit").unwrap();
    assert_eq!(submit.rule.as_deref(), Some("pii-to-eu-only"));
    assert_eq!(submit.layer.as_deref(), Some("server"));
    assert_eq!(submit.outcome, "refused");
    let run = all.iter().find(|r| r.phase == "runtime").unwrap();
    assert_eq!(run.outcome, "failed");
    assert_eq!(
        refusals(&state, Some("acme"), None, 100)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(refusal_counts(&state, Some("acme")).await["submit"], 1);
}

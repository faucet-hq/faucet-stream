use super::*;
use crate::serve::test_support::test_state;

#[test]
fn same_policy_compares_canonical_content() {
    let a: PolicySpec = crate::yaml::from_str(SERVER).unwrap();
    let relaid = "# a comment\nversion: 1\nrules:\n- name: pii-to-eu-only\n  mask: [hash]\n  \
                  require: {residency: [eu]}\n  when: {label: pii.email}\nclassifications:\n- \
                  {label: pii.email, fields: [email]}\n";
    let b: PolicySpec = crate::yaml::from_str(relaid).unwrap();
    assert!(same_policy(&a, &b), "layout and comments are not a change");
    let c: PolicySpec = crate::yaml::from_str(&SERVER.replace("[eu]", "[eu, us]")).unwrap();
    assert!(!same_policy(&a, &c));
}

#[tokio::test]
async fn a_policy_file_seeds_an_empty_store_and_then_only_reports_drift() {
    use crate::serve::config::{HistoryBackendSpec, PolicyStore};
    use crate::serve::server::apply_policy_file;
    use crate::serve::test_support::{state_from, test_config};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policy.yaml");
    std::fs::write(&path, SERVER).unwrap();
    let spec: Arc<PolicySpec> = Arc::new(crate::yaml::from_str(SERVER).unwrap());
    let mut config = test_config();
    config.history = HistoryBackendSpec::Sqlite("sqlite:persistent.db".into());

    // An empty store: the file becomes version 1, imported by `system`.
    let state = state_from(&config);
    apply_policy_file(&state, PolicyStore::Server, spec.clone(), &path)
        .await
        .unwrap();
    let layer = server_layer(&state).await.unwrap();
    assert_eq!(layer.source, PolicySource::Server);
    let v1 = layer.version.unwrap();
    assert_eq!((v1.version, v1.created_by.as_str()), (1, SYSTEM_ACTOR));
    assert!(v1.reason.unwrap().contains("imported from --policy file"));
    let drift = file_drift(&state).await.unwrap().unwrap();
    assert!(!drift.differs && drift.yaml.is_none());

    // The same file again: the stored version stays, nothing is imported.
    apply_policy_file(&state, PolicyStore::Server, spec, &path)
        .await
        .unwrap();
    assert_eq!(
        state
            .history()
            .policy_versions(SERVER_SCOPE, 0)
            .await
            .unwrap()
            .len(),
        1
    );

    // A file that differs is reported, not applied.
    let wider = SERVER.replace("[eu]", "[eu, us]");
    std::fs::write(&path, &wider).unwrap();
    let wider_spec: Arc<PolicySpec> = Arc::new(crate::yaml::from_str(&wider).unwrap());
    apply_policy_file(&state, PolicyStore::Server, wider_spec.clone(), &path)
        .await
        .unwrap();
    assert_eq!(
        server_layer(&state).await.unwrap().version.unwrap().version,
        1
    );
    let drift = file_drift(&state).await.unwrap().unwrap();
    assert!(drift.differs);
    assert!(drift.yaml.unwrap().contains("us"));

    // `--policy-store file`, or nowhere to keep versions: the file is the policy.
    let file_store = state_from(&config);
    apply_policy_file(&file_store, PolicyStore::File, wider_spec.clone(), &path)
        .await
        .unwrap();
    assert_eq!(
        server_layer(&file_store).await.unwrap().source,
        PolicySource::File
    );
    assert!(file_drift(&file_store).await.unwrap().is_none());
    let memory = test_state();
    apply_policy_file(&memory, PolicyStore::Server, wider_spec, &path)
        .await
        .unwrap();
    assert_eq!(
        server_layer(&memory).await.unwrap().source,
        PolicySource::File
    );
}

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
fn parse_draft_all_reports_subtractive_keys_and_keeps_parsing() {
    let text = "version: 1\nrules:\n  - name: r\n    when: { label: a }\n    deny: true\ndisable: [x, y]\n";
    let tenant = Layer::Tenant("acme".into());
    let (spec, problems) = parse_draft_all(text, &tenant);
    assert_eq!(spec.unwrap().rules[0].name, "r");
    assert_eq!(problems.len(), 1);
    assert_eq!(problems[0].line, Some(6));
    assert_eq!(
        problems[0].message,
        "A tenant policy cannot disable a server rule. Remove `disable`; ask the server operator about `x`, `y`."
    );
    let (_, p) = parse_draft_all("version: 1\nremove: r\n", &tenant);
    assert!(p[0].message.ends_with("about `r`."));
    let (_, p) = parse_draft_all("version: 1\noverride: 3\n", &tenant);
    assert!(p[0].message.ends_with("Remove `override`."));
    let (_, p) = parse_draft_all("version: 1\ndisable: [r]\n", &Layer::Server);
    assert!(p[0].message.contains("layers only add"));
    // The rest still fails to parse: both problems are reported.
    let (spec, p) = parse_draft_all("version: 1\nbogus: 1\ndisable: [r]\n", &tenant);
    assert!(spec.is_none());
    assert_eq!(p.len(), 2);
    // No subtractive key: the same as parse_draft.
    let (spec, p) = parse_draft_all(SERVER, &tenant);
    assert!(spec.is_some() && p.is_empty());
    let (spec, p) = parse_draft_all("version: [", &tenant);
    assert!(spec.is_none() && !p.is_empty());
    let (spec, p) = parse_draft_all("- a\n", &tenant);
    assert!(spec.is_none() && !p.is_empty());
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
    assert!(problems[0].message.contains("Tenant rules can only add"));
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

/// A pipeline whose contract sends `email` to a `residency: us` sink.
const US: &str = r#"
version: 1
name: us
pipeline:
  source: { type: csv, config: { path: in.csv } }
  contract: { version: "1", fields: [ { name: email, type: string } ] }
  sink: { type: jsonl, attributes: { residency: us }, config: { path: out.jsonl } }
"#;

/// The same pipeline carrying its own copy of the server rule: merging it
/// with the server layer is a conflict.
fn own_policy() -> String {
    format!("{US}policy:{}", SERVER.replace('\n', "\n  "))
}

/// A matrix row naming a source template nobody declared: it loads but does
/// not expand.
const UNEXPANDABLE: &str = r#"
version: 1
name: broken
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
matrix:
  - id: a
    source: { ref: missing }
"#;

const TOPOLOGY: &str = r#"
version: 1
name: topo
pipeline:
  contract: { version: "1", fields: [ { name: email, type: string } ] }
  sources:
    src: { type: csv, config: { path: in.csv } }
  sinks:
    out: { type: jsonl, attributes: { residency: us }, config: { path: out.jsonl } }
  nodes:
    read: { kind: source, ref: src }
    write: { kind: sink, ref: out }
  edges:
    - { from: read, to: write }
"#;

async fn stored_run(
    state: &ServerState,
    id: &str,
    body: Option<String>,
    format: crate::serve::load::ConfigFormat,
) {
    let mut rec = crate::serve::history::RunRecord::queued(
        id.into(),
        None,
        Default::default(),
        None,
        Utc::now(),
    );
    rec.config_body = body;
    rec.config_format = Some(format);
    state.history().upsert(&rec).await.unwrap();
}

#[tokio::test]
async fn impact_compares_runs_and_templates_before_and_after() {
    use crate::serve::load::ConfigFormat;
    let state = test_state();
    let server: PolicySpec = crate::yaml::from_str(SERVER).unwrap();
    let us_json = serde_json::to_string(&crate::yaml::from_str::<Value>(US).unwrap()).unwrap();
    stored_run(&state, "us", Some(US.into()), ConfigFormat::Yaml).await;
    stored_run(&state, "us-json", Some(us_json), ConfigFormat::Json).await;
    stored_run(&state, "own", Some(own_policy()), ConfigFormat::Yaml).await;
    stored_run(
        &state,
        "broken",
        Some(UNEXPANDABLE.into()),
        ConfigFormat::Yaml,
    )
    .await;
    stored_run(&state, "topo", Some(TOPOLOGY.into()), ConfigFormat::Yaml).await;
    stored_run(&state, "garbled", Some(": [".into()), ConfigFormat::Yaml).await;
    stored_run(&state, "no-body", None, ConfigFormat::Yaml).await;
    #[cfg(feature = "templates")]
    for (id, body) in [("tpl-us", US), ("tpl-plain", UNEXPANDABLE)] {
        state
            .history()
            .template_register(&crate::serve::history::templates::TemplateDraft {
                id: crate::serve::history::templates::TemplateId::parse(id).unwrap(),
                kind: crate::hub::TemplateKind::Pipeline,
                name: None,
                description: None,
                body: body.into(),
                format: ConfigFormat::Yaml,
                params: Default::default(),
                created_by: None,
            })
            .await
            .unwrap();
    }

    // No policy before, the server's after.
    let first = impact(&state, None, None, &server).await.unwrap();
    assert_eq!(
        first.runs_checked, 5,
        "the garbled and bodiless runs are skipped"
    );
    let refused: Vec<&str> = first.newly_refused.iter().map(|e| e.id.as_str()).collect();
    assert!(
        refused.contains(&"us") && refused.contains(&"us-json"),
        "{refused:?}"
    );
    let us = first.newly_refused.iter().find(|e| e.id == "us").unwrap();
    assert_eq!(us.rule.as_deref(), Some("pii-to-eu-only"));
    assert_eq!(us.column.as_deref(), Some("email"));
    assert!(
        first.unchanged >= 1,
        "the run with its own conflicting copy"
    );
    #[cfg(feature = "templates")]
    {
        assert_eq!(first.templates_checked, 2);
        assert_eq!(
            first.templates_runtime_only, 1,
            "no contract: run time only"
        );
        assert!(first.newly_refused.iter().any(|e| e.kind == "template"));
    }

    // Allowing `us` residency lifts the refusals.
    let wider: PolicySpec = crate::yaml::from_str(&SERVER.replace("[eu]", "[eu, us]")).unwrap();
    let second = impact(&state, None, Some(&server), &wider).await.unwrap();
    assert!(second.newly_allowed.iter().any(|e| e.id == "us"));
    assert!(second.newly_refused.is_empty());
}

#[tokio::test]
async fn conflicting_layers_and_unreadable_versions_fail_closed() {
    let state = test_state();
    state
        .history()
        .policy_version_insert(&record(SERVER_SCOPE, 1, SERVER))
        .await
        .unwrap();
    state
        .history()
        .policy_version_insert(&record(&tenant_scope("acme"), 1, SERVER))
        .await
        .unwrap();
    assert!(matches!(
        effective(&state, Some("acme"), None).await,
        Err(ServeError::Unprocessable { .. })
    ));
    let clash: PolicySpec = crate::yaml::from_str(SERVER).unwrap();
    assert!(matches!(
        draft_impact(&state, &Layer::Tenant("globex".into()), clash).await,
        Err(ServeError::Unprocessable { .. })
    ));

    let mut unreadable = record(SERVER_SCOPE, 2, SERVER);
    unreadable.spec = serde_json::json!("not a policy");
    state
        .history()
        .policy_version_insert(&unreadable)
        .await
        .unwrap();
    assert!(matches!(
        server_layer(&state).await,
        Err(ServeError::Internal(_))
    ));
}

#[cfg(feature = "templates")]
async fn register(state: &ServerState, id: &str, kind: crate::hub::TemplateKind, body: &str) {
    state
        .history()
        .template_register(&crate::serve::history::templates::TemplateDraft {
            id: crate::serve::history::templates::TemplateId::parse(id).unwrap(),
            kind,
            name: None,
            description: None,
            body: body.into(),
            format: crate::serve::load::ConfigFormat::Yaml,
            params: Default::default(),
            created_by: None,
        })
        .await
        .unwrap();
}

#[cfg(feature = "templates")]
#[tokio::test]
async fn templates_are_checked_statically() {
    use crate::hub::TemplateKind;
    let state = test_state();
    register(&state, "us", TemplateKind::Pipeline, US).await;
    register(&state, "own", TemplateKind::Pipeline, &own_policy()).await;
    register(&state, "topo", TemplateKind::Pipeline, TOPOLOGY).await;
    register(&state, "garbled", TemplateKind::Pipeline, ": [").await;
    register(
        &state,
        "src",
        TemplateKind::SourceTemplate,
        "kind: source-template\n",
    )
    .await;

    // Nothing in force and no policy of its own: nothing to check against.
    let none = check_template(&state, None, "us", None).await.unwrap();
    assert_eq!(none["loaded"], false);
    assert!(matches!(
        check_template(&state, None, "nope", None).await,
        Err(ServeError::Unprocessable { .. } | ServeError::NotFound)
    ));
    assert!(matches!(
        check_template(&state, None, "src", None).await,
        Err(ServeError::Unprocessable { .. })
    ));
    assert!(matches!(
        check_template(&state, None, "garbled", None).await,
        Err(ServeError::Unprocessable { .. })
    ));

    state
        .history()
        .policy_version_insert(&record(SERVER_SCOPE, 1, SERVER))
        .await
        .unwrap();
    let report = check_template(&state, None, "us", Some(1)).await.unwrap();
    assert_eq!(report["template"], "us");
    assert_eq!(report["version"], 1);
    assert!(check_template(&state, None, "topo", None).await.is_ok());
    assert!(matches!(
        check_template(&state, None, "own", None).await,
        Err(ServeError::Unprocessable { .. })
    ));

    // The source template and the template that does not load are skipped.
    let server: PolicySpec = crate::yaml::from_str(SERVER).unwrap();
    let impact = impact(&state, None, None, &server).await.unwrap();
    assert_eq!(impact.templates_checked, 3);
    // Sink attributes are read from the templates that parse.
    assert_eq!(sink_attributes(&state).await["sinks"], 3);
}

#[tokio::test]
async fn own_policies_merge_and_tenant_drafts_stand_alone() {
    use crate::serve::load::ConfigFormat;
    let state = test_state();
    // A pipeline policy that adds its own rule merges with the layer.
    let own = format!(
        "{US}policy:\n  version: 1\n  rules:\n    - name: own-rule\n      when: {{ label: pii.email }}\n      deny: true\n"
    );
    stored_run(&state, "own", Some(own), ConfigFormat::Yaml).await;
    let server: PolicySpec = crate::yaml::from_str(SERVER).unwrap();
    let out = impact(&state, None, Some(&server), &server).await.unwrap();
    assert_eq!(out.runs_checked, 1);
    assert_eq!(out.unchanged, 1);
    // No server policy: a tenant draft is its own effective policy.
    let draft: PolicySpec = crate::yaml::from_str(SERVER).unwrap();
    assert!(
        draft_impact(&state, &Layer::Tenant("acme".into()), draft)
            .await
            .is_ok()
    );
    // A policy that does not compile labels nothing.
    let broken: PolicySpec = crate::yaml::from_str(
        "version: 1\nclassifications:\n  - label: a\n    field_pattern: \"(\"\n",
    )
    .unwrap();
    assert!(columns_seen(&state, &broken).await.is_empty());
}

#[tokio::test]
async fn drafts_are_compiled_on_every_layer() {
    let state = test_state();
    let bad_regex = "version: 1\nclassifications:\n  - label: a\n    field_pattern: \"(\"\n";
    let draft = parse_draft(bad_regex).unwrap();
    assert_eq!(
        check_draft(&state, &Layer::Server, &draft, bad_regex)
            .await
            .unwrap()
            .len(),
        1
    );
    // A tenant draft with no server policy stands alone.
    let tenant = Layer::Tenant("acme".into());
    assert_eq!(
        check_draft(&state, &tenant, &draft, bad_regex)
            .await
            .unwrap()
            .len(),
        1
    );
    let fine = "version: 1\nclassifications:\n  - label: a\n    fields: [a]\nrules:\n  - name: r\n    when: { label: a }\n    deny: true\n";
    let draft = parse_draft(fine).unwrap();
    assert!(
        check_draft(&state, &tenant, &draft, fine)
            .await
            .unwrap()
            .is_empty()
    );
    let disabled = parse_draft("version: 1\ndisable: [r]\n").unwrap_err();
    assert!(
        disabled[0].message.contains("cannot disable"),
        "{disabled:?}"
    );
}

#[tokio::test]
async fn a_run_admitted_without_a_policy_stays_unchecked_on_reload() {
    let state = test_state();
    // Admitted while no policy was stored: its pin names no version.
    let (_, pin) = effective(&state, Some("acme"), None).await.unwrap();
    assert!(pin.is_empty());
    // A policy stored meanwhile does not reach it (#1051 #8)…
    state
        .history()
        .policy_version_insert(&record(SERVER_SCOPE, 1, SERVER))
        .await
        .unwrap();
    let (spec, _) = effective(&state, Some("acme"), Some(&pin)).await.unwrap();
    assert_eq!(spec, None);
    // …while an unpinned run gets the newest.
    assert!(effective(&state, None, None).await.unwrap().0.is_some());
}

#[tokio::test]
async fn a_degraded_history_refuses_rather_than_running_unchecked() {
    let (state, down) = crate::serve::test_support::flaky_state();
    state
        .history()
        .policy_version_insert(&record(SERVER_SCOPE, 1, SERVER))
        .await
        .unwrap();
    let (spec, pin) = effective(&state, None, None).await.unwrap();
    assert!(spec.is_some());
    down.store(true, std::sync::atomic::Ordering::Release);
    for pinned in [None, Some(&pin)] {
        let err = effective(&state, None, pinned).await.unwrap_err();
        assert!(matches!(err, ServeError::Unavailable(_)), "{err:?}");
    }
    assert!(state.history().degraded());
    assert!(
        state
            .history()
            .policy_version_insert(&record(SERVER_SCOPE, 2, SERVER))
            .await
            .is_err()
    );
}

#[cfg(feature = "tenants")]
#[tokio::test]
async fn a_server_draft_must_keep_each_tenant_merge_valid() {
    let state = test_state();
    let now = Utc::now();
    state
        .history()
        .tenant_upsert(&crate::serve::history::tenants::TenantRecord {
            id: "acme".into(),
            name: None,
            labels: Default::default(),
            limits: Default::default(),
            notifications: Vec::new(),
            notifications_sealed: None,
            suspended: false,
            created_at: now,
            updated_at: now,
            created_by: "t".into(),
        })
        .await
        .unwrap();
    // acme's rule governs a label only the server classifies.
    let tenant = "version: 1\nrules:\n  - name: acme-email\n    when: { label: pii.email }\n    deny: true\n";
    state
        .history()
        .policy_version_insert(&record(&tenant_scope("acme"), 1, tenant))
        .await
        .unwrap();
    // A server draft that drops the classification breaks acme's merge.
    let dropped = "version: 1\nclassifications:\n  - label: pii.phone\n    fields: [phone]\nrules:\n  - name: phones\n    when: { label: pii.phone }\n    deny: true\n";
    let draft = parse_draft(dropped).unwrap();
    let problems = check_draft(&state, &Layer::Server, &draft, dropped)
        .await
        .unwrap();
    assert!(
        problems.iter().any(|p| p
            .message
            .contains("tenant acme's policy would no longer validate")),
        "{problems:?}"
    );
    // Keeping it passes.
    let draft = parse_draft(SERVER).unwrap();
    assert!(
        check_draft(&state, &Layer::Server, &draft, SERVER)
            .await
            .unwrap()
            .is_empty()
    );
}

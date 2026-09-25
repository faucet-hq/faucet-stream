//! `faucet template sync` / `faucet template publish` end-to-end (RFC 0006 /
//! #589), driven in-process through `run_command` so the command layer —
//! argument parsing, report rendering, the exit-code contract — is exercised
//! against a real SQLite registry and a mock GitHub origin.
#![cfg(all(feature = "templates-sync", feature = "serve-history-sqlite"))]

use clap::Parser as _;
use faucet_cli::cli::Cli;
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const BODY: &str = "version: 1\nname: nightly\npipeline:\n  source: {type: rest, config: {base_url: \"https://x\", path: /e}}\n  sink: {type: stdout, config: {}}\n";

async fn mount_repo(server: &MockServer, files: &[(&str, &str)]) {
    let entries: Vec<Value> = files
        .iter()
        .map(|(n, _)| {
            json!({
                "name": n, "type": "file", "sha": "abc",
                "url": format!("{}/repos/acme/tpl/contents/templates/{n}?ref=main", server.uri()),
            })
        })
        .collect();
    Mock::given(method("GET"))
        .and(path("/repos/acme/tpl/contents/templates"))
        .and(query_param("ref", "main"))
        .respond_with(ResponseTemplate::new(200).set_body_json(entries))
        .mount(server)
        .await;
    for (n, body) in files {
        Mock::given(method("GET"))
            .and(path(format!("/repos/acme/tpl/contents/templates/{n}")))
            .and(header("Accept", "application/vnd.github.raw+json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(*body))
            .mount(server)
            .await;
    }
}

fn write_sync_file(dir: &std::path::Path, api: &str, prune: &str) -> String {
    let p = dir.join("sync.yaml");
    std::fs::write(
        &p,
        format!(
            "version: 1\norigins:\n  - name: gh\n    prefix: plat-\n    launch: follow\n    prune: {prune}\n    source:\n      type: github\n      config: {{ repo: acme/tpl, path: templates, api_base: \"{api}\" }}\n"
        ),
    )
    .unwrap();
    p.to_string_lossy().into_owned()
}

async fn run(args: &[&str]) -> Result<(), faucet_cli::error::CliError> {
    let mut argv = vec!["faucet"];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).expect("argv parses");
    // The whole-CLI future is large; pin it on the heap rather than moving it
    // across the test thread's stack.
    Box::pin(faucet_cli::run_command(cli)).await
}

#[tokio::test]
async fn sync_dry_run_then_pull_then_publish() {
    let gh = MockServer::start().await;
    mount_repo(
        &gh,
        &[
            ("nightly.yaml", BODY),
            ("nightly.faucet.yaml", "launch: true\ndescription: Nightly"),
            ("Bad-Name.yaml", BODY),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let store = format!("sqlite:{}", dir.path().join("t.db").display());
    let sync = write_sync_file(dir.path(), &gh.uri(), "deprecate");

    // Dry run: succeeds, registers nothing.
    run(&[
        "template",
        "sync",
        "--store",
        &store,
        "--config",
        &sync,
        "--dry-run",
        "--no-env-file",
    ])
    .await
    .expect("dry run");
    let registry = faucet_cli::templates::resolve_store_url(&store)
        .await
        .unwrap();
    assert!(
        faucet_cli::templates::list_with_state(&registry)
            .await
            .unwrap()
            .is_empty()
    );

    // Real pull (JSON output path). `Bad-Name` (uppercase) is skipped, not fatal.
    run(&[
        "template",
        "sync",
        "--store",
        &store,
        "--config",
        &sync,
        "--origin",
        "gh",
        "--json",
        "--no-env-file",
    ])
    .await
    .expect("pull");
    let state = faucet_cli::templates::template_state(&registry, "plat-nightly")
        .await
        .unwrap();
    assert_eq!(state.stable, Some(1), "sidecar launch under `follow`");

    // Idempotent re-pull.
    run(&[
        "template",
        "sync",
        "--store",
        &store,
        "--config",
        &sync,
        "--no-env-file",
    ])
    .await
    .expect("re-pull");
    assert_eq!(
        faucet_cli::templates::template_state(&registry, "plat-nightly")
            .await
            .unwrap()
            .versions,
        vec![1]
    );

    // Publish v1 back (create: 404 then PUT without sha), human + JSON output.
    Mock::given(method("GET"))
        .and(path("/repos/acme/tpl/contents/templates/nightly.yaml"))
        .and(header("Accept", "application/vnd.github+json"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&gh)
        .await;
    Mock::given(method("PUT"))
        .and(path("/repos/acme/tpl/contents/templates/nightly.yaml"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"content": {}})))
        .mount(&gh)
        .await;
    run(&[
        "template",
        "publish",
        "plat-nightly",
        "--store",
        &store,
        "--config",
        &sync,
        "--origin",
        "gh",
        "--no-env-file",
    ])
    .await
    .expect("publish");
    run(&[
        "template",
        "publish",
        "plat-nightly",
        "--store",
        &store,
        "--config",
        &sync,
        "--origin",
        "gh",
        "--version",
        "1",
        "--json",
        "--no-env-file",
    ])
    .await
    .expect("publish --json");
    let puts = gh
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method == "PUT")
        .count();
    assert_eq!(puts, 2);

    // Publishing outside the origin's namespace is an error.
    let err = run(&[
        "template",
        "publish",
        "other",
        "--store",
        &store,
        "--config",
        &sync,
        "--origin",
        "gh",
        "--no-env-file",
    ])
    .await
    .unwrap_err();
    assert!(err.to_string().contains("outside origin"), "{err}");
}

/// The exit contract: an unreadable origin, or any template that failed to
/// apply, makes the command fail — a partial pull must not look green.
#[tokio::test]
async fn sync_fails_when_an_origin_is_unreadable_or_a_template_does_not_apply() {
    let gh = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let store = format!("sqlite:{}", dir.path().join("t.db").display());
    let sync = write_sync_file(dir.path(), &gh.uri(), "keep");

    // Nothing mounted → 404 on the listing → origin error.
    let err = run(&[
        "template",
        "sync",
        "--store",
        &store,
        "--config",
        &sync,
        "--no-env-file",
    ])
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("1 origin(s) unreadable"), "{err}");

    // A template whose body fails validation → applied-failure → non-zero.
    mount_repo(
        &gh,
        &[(
            "broken.yaml",
            "version: 1\npipeline: {source: {type: nope, config: {}}, sink: {type: stdout, config: {}}}\n",
        )],
    )
    .await;
    let err = run(&[
        "template",
        "sync",
        "--store",
        &store,
        "--config",
        &sync,
        "--json",
        "--no-env-file",
    ])
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("1 template(s) failed to apply"), "{err}");

    // An unknown `--origin` is a config error before any network call.
    let err = run(&[
        "template",
        "sync",
        "--store",
        &store,
        "--config",
        &sync,
        "--origin",
        "nope",
        "--no-env-file",
    ])
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("no origin named 'nope'"), "{err}");
}

#[tokio::test]
async fn schema_templates_sync_prints_the_sync_file_schema() {
    run(&["schema", "templates-sync"]).await.expect("schema");
}

/// #697: a catalog origin (`paths:`) that later deprecates the version this
/// registry already holds retires that version here too, and the template is
/// not mistaken for one that left the catalog.
#[tokio::test]
async fn a_version_the_catalog_deprecates_is_retired_in_the_registry() {
    let gh = MockServer::start().await;
    let mount = |deprecated: bool| {
        let gh = &gh;
        async move {
            gh.reset().await;
            Mock::given(method("GET"))
                .and(path("/repos/acme/hub/contents/source-templates"))
                .and(query_param("ref", "main"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
                    "name": "erp.yaml", "type": "file", "sha": "abc",
                    "url": format!("{}/repos/acme/hub/contents/source-templates/erp.yaml?ref=main", gh.uri()),
                }])))
                .mount(gh)
                .await;
            Mock::given(method("GET"))
                .and(path("/repos/acme/hub/contents/source-templates/erp.yaml"))
                .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
                .mount(gh)
                .await;
            let version = if deprecated {
                json!({"version": 1, "commit": "a", "deprecated": true, "reason": "drops invoices"})
            } else {
                json!({"version": 1, "commit": "a"})
            };
            Mock::given(method("GET"))
                .and(path("/repos/acme/hub/contents/index.json"))
                .respond_with(ResponseTemplate::new(200).set_body_string(
                    json!({"commit": "a", "sources": [{"id": "erp", "versions": [version]}], "sinks": []})
                        .to_string(),
                ))
                .mount(gh)
                .await;
        }
    };
    let dir = tempfile::tempdir().unwrap();
    let store = format!("sqlite:{}", dir.path().join("t.db").display());
    let sync = dir.path().join("sync.yaml");
    std::fs::write(
        &sync,
        format!(
            "version: 1\norigins:\n  - name: hub\n    prefix: plat-\n    launch: always\n    prune: deprecate\n    source:\n      type: github\n      config: {{ repo: acme/hub, paths: [source-templates], api_base: \"{}\" }}\n",
            gh.uri()
        ),
    )
    .unwrap();
    let sync = sync.to_string_lossy().into_owned();
    let args: Vec<String> = [
        "template",
        "sync",
        "--store",
        &store,
        "--config",
        &sync,
        "--no-env-file",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let pull = || {
        let args = args.clone();
        async move {
            let argv: Vec<&str> = args.iter().map(String::as_str).collect();
            run(&argv).await
        }
    };
    let registry = faucet_cli::templates::resolve_store_url(&store)
        .await
        .unwrap();

    mount(false).await;
    pull().await.expect("first pull registers v1");
    let st = faucet_cli::templates::template_state(&registry, "plat-erp")
        .await
        .unwrap();
    assert_eq!((st.stable, st.deprecated_versions.len()), (Some(1), 0));

    mount(true).await;
    pull().await.expect("the catalog now deprecates v1");
    let st = faucet_cli::templates::template_state(&registry, "plat-erp")
        .await
        .unwrap();
    let d = st.version_deprecation(1).expect("v1 retired here too");
    assert_eq!(
        d.record.reason.as_deref(),
        Some("catalog v1 is deprecated: drops invoices")
    );
    assert_eq!(d.record.deprecated_by.as_deref(), Some("sync:hub"));
    assert_eq!(
        st.status,
        faucet_cli::serve::history::templates::TemplateStatus::Launched,
        "still in the catalog, so not retired as a whole"
    );

    // Pulling again changes nothing.
    pull().await.expect("idempotent");
    assert_eq!(
        faucet_cli::templates::template_state(&registry, "plat-erp")
            .await
            .unwrap()
            .deprecated_versions
            .len(),
        1
    );
}

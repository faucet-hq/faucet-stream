//! `faucet hub import openapi` end to end (#899): a synthetic spec served by
//! wiremock is imported in-process, and the generated template must pass
//! `hub lint`, `hub check` against two sinks, `validate`, its own draft
//! `template test` suite, and a real run against the spec's example
//! responses. The output is deterministic and matches the committed skill
//! example byte for byte.
#![cfg(all(
    feature = "source-rest",
    feature = "sink-sqlite",
    feature = "sink-postgres",
    feature = "hub-remote"
))]

use std::path::{Path, PathBuf};

use clap::Parser as _;
use faucet_cli::cli::Cli;
use faucet_cli::hub::import::{ImportOptions, ImportRequest, import_openapi};
use faucet_cli::template_tests::{SuiteFile, Target};
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn on_big_stack<F>(f: impl FnOnce() -> F + Send + 'static)
where
    F: std::future::Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f())
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run(args: &[&str]) -> Result<(), faucet_cli::error::CliError> {
    let mut argv = vec!["faucet"];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).expect("argv parses");
    Box::pin(faucet_cli::run_command(cli)).await
}

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn example_dir() -> PathBuf {
    repo().join("skills/faucet-templates/examples")
}

fn spec_text() -> String {
    std::fs::read_to_string(example_dir().join("openapi/example-store.yaml")).unwrap()
}

fn hub_with_sinks(dir: &Path) -> PathBuf {
    let hub = dir.join("hub");
    let sinks = hub.join("sink-templates/faucet-hq");
    std::fs::create_dir_all(&sinks).unwrap();
    for s in ["sqlite", "postgres"] {
        std::fs::copy(
            repo().join(format!("hub/sink-templates/faucet-hq/{s}.yaml")),
            sinks.join(format!("{s}.yaml")),
        )
        .unwrap();
    }
    hub
}

fn request(spec: &str, out: &Path) -> ImportRequest {
    ImportRequest {
        spec: spec.to_string(),
        options: ImportOptions {
            owner: "acme".into(),
            test_sink: "sink-templates/faucet-hq/sqlite.yaml".into(),
            ..Default::default()
        },
        out: out.to_path_buf(),
        dry_run: false,
        force: false,
        allow_remote_refs: false,
    }
}

fn ok_json(body: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(body)
}

async fn mount_api(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/v1/customers"))
        .and(header("authorization", "Bearer tok"))
        .and(query_param_is_missing("cursor"))
        .respond_with(ok_json(json!({
            "data": [{"id": 1, "name": "Ada", "address": {"city": "Paris"}, "updated_at": "2026-01-02T00:00:00Z"}],
            "next_cursor": "c2"
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/customers"))
        .and(query_param("cursor", "c2"))
        .respond_with(ok_json(json!({
            "data": [{"id": 2, "name": "Bo", "address": null, "updated_at": "2026-01-03T00:00:00Z"}],
            "next_cursor": null
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/customers/1/orders"))
        .and(query_param("page", "1"))
        .respond_with(ok_json(
            json!([{"id": 10, "total": 5.5, "lines": [{"sku": "a"}]}]),
        ))
        .mount(server)
        .await;
    for p in ["/v1/customers/1/orders", "/v1/customers/2/orders"] {
        Mock::given(method("GET"))
            .and(path(p))
            .respond_with(ok_json(json!([])))
            .mount(server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/v1/events"))
        .respond_with(ok_json(json!({
            "items": [{"id": "e1", "type": "customer.created", "created_at": "2026-01-01T00:00:00Z"}],
            "total": 1
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/products"))
        .and(query_param_is_missing("page"))
        .respond_with(
            ok_json(json!([{"sku": "a", "name": "Apple", "tags": ["fruit"]}])).insert_header(
                "link",
                format!("<{}/v1/products?page=2>; rel=\"next\"", server.uri()).as_str(),
            ),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/products"))
        .and(query_param("page", "2"))
        .respond_with(ok_json(json!([{"sku": "b", "name": "Bean", "tags": []}])))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/invoices"))
        .and(query_param_is_missing("p"))
        .respond_with(ok_json(json!({
            "results": [{"id": "i1", "amount": 1.0, "modified_at": "2026-01-01T00:00:00Z"}],
            "links": {"next": format!("{}/v1/invoices?p=2", server.uri())}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/invoices"))
        .and(query_param("p", "2"))
        .respond_with(ok_json(json!({
            "results": [{"id": "i2", "amount": 2.0, "modified_at": "2026-01-02T00:00:00Z"}],
            "links": {}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/notes"))
        .respond_with(ok_json(
            json!({"data": [{"id": "n1", "body": "hi"}], "next_cursor": "z"}),
        ))
        .mount(server)
        .await;
}

async fn count(db: &Path, table: &str) -> i64 {
    use sqlx::Connection as _;
    let mut c = sqlx::SqliteConnection::connect(&format!("sqlite:{}", db.display()))
        .await
        .unwrap();
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"{table}\""))
        .fetch_one(&mut c)
        .await
        .unwrap()
}

#[test]
fn a_served_spec_imports_into_a_template_that_passes_every_hub_gate() {
    on_big_stack(|| async {
        let tmp = tempfile::tempdir().unwrap();
        let hub = hub_with_sinks(tmp.path());
        let hub_s = hub.to_str().unwrap().to_string();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/spec/openapi.yaml"))
            .respond_with(ResponseTemplate::new(200).set_body_string(spec_text()))
            .mount(&server)
            .await;
        let url = format!("{}/spec/openapi.yaml", server.uri());
        run(&[
            "hub", "import", "openapi", &url, "--owner", "acme", "--out", &hub_s,
        ])
        .await
        .expect("import succeeds");
        let template = hub.join("source-templates/acme/example-store.yaml");
        let suite = hub.join("tests/acme/example-store/suite.yaml");
        assert!(template.is_file() && suite.is_file());

        run(&["hub", "lint", "--hub", &hub_s])
            .await
            .expect("lint passes");
        for sink in ["faucet-hq/sqlite", "faucet-hq/postgres"] {
            run(&[
                "hub",
                "check",
                "--hub",
                &hub_s,
                "--source",
                "acme/example-store",
                "--sink",
                sink,
            ])
            .await
            .unwrap_or_else(|e| panic!("hub check {sink}: {e}"));
        }
        run(&[
            "validate",
            "--hub",
            &hub_s,
            "--source",
            "acme/example-store",
            "--sink",
            "faucet-hq/sqlite",
        ])
        .await
        .expect("validate passes");

        let file = SuiteFile::from_path(&suite).unwrap();
        let body = std::fs::read_to_string(hub.join(&file.template)).unwrap();
        let sink_body = std::fs::read_to_string(hub.join(file.sink.as_deref().unwrap())).unwrap();
        let outcome = faucet_cli::template_tests::run(
            &file,
            Target::Document {
                body,
                sink_body: Some(sink_body),
                overlay: None,
            },
            None,
        )
        .await
        .unwrap();
        assert!(outcome.cases.len() >= 4, "{} case(s)", outcome.cases.len());
        assert_eq!(outcome.failed(), 0, "suite failed");

        mount_api(&server).await;
        let db = tmp.path().join("store.db");
        let api_root = format!("api_root={}/v1", server.uri());
        let sqlite_path = format!("sqlite_path={}", db.display());
        run(&[
            "run",
            "--hub",
            &hub_s,
            "--source",
            "acme/example-store",
            "--sink",
            "faucet-hq/sqlite",
            "--param",
            &api_root,
            "--param",
            "api_token=tok",
            "--param",
            &sqlite_path,
        ])
        .await
        .expect("a run against the example responses succeeds");
        for (table, n) in [
            ("customers", 2),
            ("orders", 1),
            ("events", 1),
            ("products", 2),
            ("invoices", 2),
            ("notes", 1),
        ] {
            assert_eq!(count(&db, table).await, n, "{table}");
        }
    });
}

#[test]
fn output_is_deterministic_and_matches_the_committed_example() {
    on_big_stack(|| async {
        let tmp = tempfile::tempdir().unwrap();
        let spec = example_dir().join("openapi/example-store.yaml");
        let spec = spec.to_str().unwrap();
        let first = import_openapi(&request(spec, tmp.path())).await.unwrap();
        assert_eq!(first.written.len(), 2);
        let again = import_openapi(&request(spec, tmp.path())).await.unwrap();
        assert!(
            again.written.is_empty(),
            "an unchanged re-import writes nothing"
        );
        assert_eq!(first.template_yaml, again.template_yaml);
        let norm = |s: &str| {
            s.lines()
                .filter(|l| !l.starts_with("requires_faucet:"))
                .collect::<Vec<_>>()
                .join("\n")
        };
        for (got, rel) in [
            (
                &first.template_yaml,
                "source-templates/acme/example-store.yaml",
            ),
            (&first.suite_yaml, "tests/acme/example-store/suite.yaml"),
        ] {
            let want = std::fs::read_to_string(example_dir().join(rel)).unwrap();
            assert_eq!(
                norm(got),
                norm(&want),
                "{rel} is stale — regenerate it with `faucet hub import openapi \
                 skills/faucet-templates/examples/openapi/example-store.yaml --owner acme \
                 --out skills/faucet-templates/examples --force`"
            );
        }
        let decisions: Vec<String> = first
            .imported
            .report
            .decisions
            .iter()
            .map(|d| format!("{}:{}", d.stream.clone().unwrap_or_default(), d.field))
            .collect();
        assert_eq!(
            decisions,
            vec![
                "/customers/{customer_id}/orders:parent_key".to_string(),
                "notes:pagination".to_string()
            ]
        );

        let doc: serde_json::Value = serde_json::from_str(&first.to_json().unwrap()).unwrap();
        assert_eq!(doc["template"]["name"], json!("example-store"));
        assert_eq!(doc["report"]["decisions"][1]["field"], json!("pagination"));
        assert_eq!(doc["report"]["skipped"].as_array().unwrap().len(), 2);
        assert_eq!(
            doc["suite"]["sink"],
            json!("sink-templates/faucet-hq/sqlite.yaml")
        );
        let report = faucet_cli::hub::import::render_report(&first);
        assert!(report.contains("decision: notes: pagination"), "{report}");
        assert!(report.contains("wrote "), "{report}");

        let edited = tmp.path().join("edited.yaml");
        std::fs::write(&edited, spec_text().replace("/notes:", "/memos:")).unwrap();
        let mut req = request(edited.to_str().unwrap(), tmp.path());
        req.options.name = Some("example-store".into());
        let refused = import_openapi(&req).await.unwrap_err().to_string();
        assert!(
            refused.contains("--force") && refused.contains("+ - name: memos"),
            "{refused}"
        );
        req.force = true;
        let forced = import_openapi(&req).await.unwrap();
        assert_eq!(forced.diffs.len(), 1);
        assert!(faucet_cli::hub::import::render_report(&forced).contains("changed "));
        req.dry_run = true;
        let dry = import_openapi(&req).await.unwrap();
        assert!(dry.written.is_empty());
        assert!(faucet_cli::hub::import::render_report(&dry).contains("nothing written"));
        run(&[
            "hub",
            "import",
            "openapi",
            edited.to_str().unwrap(),
            "--owner",
            "acme",
            "--name",
            "example-store",
            "--dry-run",
            "--format",
            "json",
        ])
        .await
        .unwrap();
        run(&[
            "hub",
            "import",
            "openapi",
            edited.to_str().unwrap(),
            "--owner",
            "acme",
            "--name",
            "example-store",
            "--dry-run",
        ])
        .await
        .unwrap();
    });
}

#[test]
fn references_and_bad_documents_are_handled() {
    on_big_stack(|| async {
        let tmp = tempfile::tempdir().unwrap();
        let server = MockServer::start().await;
        let root = r##"
openapi: 3.1.0
info: { title: Parts }
paths:
  /things:
    get:
      responses:
        "200":
          description: ok
          content:
            application/json:
              schema: { type: array, items: { $ref: "parts/thing.yaml#/Thing" } }
"##;
        Mock::given(method("GET"))
            .and(path("/api/openapi.yaml"))
            .respond_with(ResponseTemplate::new(200).set_body_string(root))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/parts/thing.yaml"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "Thing: { type: object, properties: { id: { type: string }, at: { $ref: '#/Stamp' } } }\nStamp: { type: string }\n",
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/remote.yaml"))
            .respond_with(ResponseTemplate::new(200).set_body_string(root.replace(
                "parts/thing.yaml#/Thing",
                "https://elsewhere.example.com/thing.yaml",
            )))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/escape.yaml"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                root.replace("parts/thing.yaml#/Thing", "/\\\\evil.example.com/x.yaml"),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/docs"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("<!doctype html><html><body>API docs</body></html>"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/gone"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let ok = import_openapi(&request(
            &format!("{}/api/openapi.yaml", server.uri()),
            tmp.path(),
        ))
        .await
        .unwrap();
        assert_eq!(
            ok.imported.template.streams[0].primary_keys,
            vec!["id".to_string()]
        );
        assert!(ok.imported.template.params["api_root"].required);
        assert_eq!(ok.imported.report.spec_version, "3.1.0");

        let remote = import_openapi(&request(
            &format!("{}/api/remote.yaml", server.uri()),
            tmp.path(),
        ))
        .await
        .unwrap_err()
        .to_string();
        assert!(remote.contains("--allow-remote-refs"), "{remote}");
        let escape = import_openapi(&request(
            &format!("{}/api/escape.yaml", server.uri()),
            tmp.path(),
        ))
        .await
        .unwrap_err()
        .to_string();
        assert!(escape.contains("malformed"), "{escape}");
        let html = import_openapi(&request(&format!("{}/docs", server.uri()), tmp.path()))
            .await
            .unwrap_err()
            .to_string();
        assert!(html.contains("HTML page"), "{html}");
        let gone = import_openapi(&request(&format!("{}/gone", server.uri()), tmp.path()))
            .await
            .unwrap_err()
            .to_string();
        assert!(gone.contains("fetching spec"), "{gone}");
        let mut bad = request("unused", tmp.path());
        bad.options.owner = "Not A Slug".into();
        assert!(
            import_openapi(&bad)
                .await
                .unwrap_err()
                .to_string()
                .contains("--owner")
        );
        bad.options.owner = "acme".into();
        bad.options.name = Some("Bad Name".into());
        assert!(
            import_openapi(&bad)
                .await
                .unwrap_err()
                .to_string()
                .contains("--name")
        );

        let many = tmp.path().join("many");
        std::fs::create_dir_all(&many).unwrap();
        let mut props = serde_json::Map::new();
        for i in 0..=faucet_cli::hub::import::MAX_DOCUMENTS {
            std::fs::write(many.join(format!("p{i}.yaml")), "type: string\n").unwrap();
            props.insert(format!("p{i}"), json!({"$ref": format!("p{i}.yaml")}));
        }
        let spec = json!({"openapi": "3.0.0", "info": {"title": "Many"}, "paths": {"/x": {"get": {"responses": {"200": {
            "content": {"application/json": {"schema": {"type": "array", "items": {"properties": props}}}}}}}}}});
        let path = many.join("api.json");
        std::fs::write(&path, spec.to_string()).unwrap();
        let e = import_openapi(&request(path.to_str().unwrap(), tmp.path()))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("more than"), "{e}");
        let huge = tmp.path().join("huge.yaml");
        std::fs::File::create(&huge)
            .unwrap()
            .set_len(faucet_cli::hub::import::MAX_SPEC_BYTES + 1)
            .unwrap();
        let e = import_openapi(&request(huge.to_str().unwrap(), tmp.path()))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("limit"), "{e}");
    });
}

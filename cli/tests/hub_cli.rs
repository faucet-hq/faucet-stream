//! Template Hub CLI end-to-end (#571), driven in-process through
//! `run_command`: a fixture hub with a CSV source template and a JSONL sink
//! template is composed, checked, listed, linted, validated, and **run** —
//! the composed pipeline writes one file per stream, with the stream name in
//! the path and the state key `{source}::{stream}`.

use clap::Parser as _;
use faucet_cli::cli::Cli;
use std::path::Path;

async fn run(args: &[&str]) -> Result<(), faucet_cli::error::CliError> {
    let mut argv = vec!["faucet"];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).expect("argv parses");
    Box::pin(faucet_cli::run_command(cli)).await
}

fn fixture(dir: &Path) -> (String, String) {
    let hub = dir.join("hub");
    std::fs::create_dir_all(hub.join("source-templates")).unwrap();
    std::fs::create_dir_all(hub.join("sink-templates")).unwrap();
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::write(
        data.join("orders.csv"),
        "id,amount,region\n1,10,eu\n2,20,us\n",
    )
    .unwrap();
    std::fs::write(data.join("customers.csv"), "id,name\n7,Acme\n").unwrap();
    std::fs::write(
        hub.join("source-templates/shop.yaml"),
        format!(
            r#"
kind: source-template
name: shop
description: A shop's exports
tags: [demo]
params:
  data_dir: {{ type: string, default: "{data}" }}
source:
  type: csv
  config: {{ path: "${{param.data_dir}}/orders.csv", has_headers: true }}
transforms:
  - {{ type: keys_case, config: {{ mode: snake }} }}
streams:
  - name: orders
    primary_keys: [id]
    write: [overwrite, upsert]
  - name: customers
    source: {{ config: {{ path: "${{param.data_dir}}/customers.csv" }} }}
    write: overwrite
"#,
            data = data.display()
        ),
    )
    .unwrap();
    std::fs::write(
        hub.join("sink-templates/files.yaml"),
        format!(
            r#"
kind: sink-template
name: files
description: JSON Lines files
params:
  out_dir: {{ type: string, default: "{out}" }}
sink:
  type: jsonl
  config: {{ append: false }}
per_stream:
  path: "${{param.out_dir}}/${{source}}/${{stream}}.jsonl"
write_mode_aliases:
  overwrite: append
"#,
            out = dir.join("out").display()
        ),
    )
    .unwrap();
    // A second sink with no alias: `overwrite` streams cannot land on it.
    std::fs::write(
        hub.join("sink-templates/plain.yaml"),
        r#"
kind: sink-template
name: plain
description: Append-only files
sink:
  type: jsonl
  config: {}
per_stream:
  path: "./plain/${stream}.jsonl"
"#,
    )
    .unwrap();
    (
        hub.to_string_lossy().into_owned(),
        dir.join("out").to_string_lossy().into_owned(),
    )
}

#[tokio::test]
async fn compose_check_list_lint_validate_and_run_a_pairing() {
    let dir = tempfile::tempdir().unwrap();
    let (hub, out) = fixture(dir.path());

    // Compose to a file and validate that file the ordinary way.
    let composed = dir.path().join("composed.yaml");
    run(&[
        "hub",
        "compose",
        "--source",
        "shop",
        "--sink",
        "files",
        "--hub",
        &hub,
        "--out",
        composed.to_str().unwrap(),
    ])
    .await
    .expect("compose");
    let text = std::fs::read_to_string(&composed).unwrap();
    assert!(
        text.contains("name: shop") && text.contains("orders.jsonl"),
        "{text}"
    );
    run(&[
        "validate",
        composed.to_str().unwrap(),
        "--no-env-file",
        "--no-secrets",
    ])
    .await
    .expect("validate composed file");
    run(&[
        "hub", "compose", "--source", "shop", "--sink", "files", "--hub", &hub, "--json",
    ])
    .await
    .expect("compose --json");

    // Check, list, matrix, lint.
    run(&[
        "hub", "check", "--source", "shop", "--sink", "files", "--hub", &hub,
    ])
    .await
    .expect("check");
    run(&[
        "hub", "check", "--source", "shop", "--sink", "files", "--hub", &hub, "--json",
    ])
    .await
    .expect("check --json");
    let err = run(&[
        "hub", "check", "--source", "shop", "--sink", "plain", "--hub", &hub,
    ])
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("2 stream(s) of 'shop' have no write mode sink 'plain' supports"),
        "{err}"
    );
    run(&["hub", "list", "--hub", &hub]).await.expect("list");
    run(&["hub", "list", "--hub", &hub, "--json"])
        .await
        .expect("list --json");
    for fmt in ["table", "markdown", "json"] {
        run(&["hub", "matrix", "--hub", &hub, "--format", fmt])
            .await
            .expect("matrix");
    }
    let md = dir.path().join("matrix.md");
    run(&[
        "hub",
        "matrix",
        "--hub",
        &hub,
        "--format",
        "markdown",
        "--out",
        md.to_str().unwrap(),
    ])
    .await
    .unwrap();
    let md = std::fs::read_to_string(md).unwrap();
    assert!(md.contains("| [shop](#shop) | ✓ | — |"), "{md}");
    run(&["hub", "lint", "--hub", &hub])
        .await
        .expect("lint catalog");
    let one = format!("{hub}/source-templates/shop.yaml");
    run(&["hub", "lint", &one]).await.expect("lint one file");
    run(&["hub", "lint", &one, "--json"])
        .await
        .expect("lint one file --json");

    // Validate the pairing directly (placeholder binding, and --show-composed).
    run(&[
        "validate",
        "--source",
        "shop",
        "--sink",
        "files",
        "--hub",
        &hub,
        "--no-env-file",
        "--no-secrets",
    ])
    .await
    .expect("validate --source/--sink");
    run(&[
        "validate",
        "--source",
        "shop",
        "--sink",
        "files",
        "--hub",
        &hub,
        "--no-env-file",
        "--show-composed",
    ])
    .await
    .expect("validate --show-composed");
    run(&[
        "validate",
        "--source",
        "shop",
        "--sink",
        "files",
        "--hub",
        &hub,
        "--no-env-file",
        "--no-secrets",
        "--json",
    ])
    .await
    .expect("validate --json");

    // Run it: one file per stream under <out>/<source>/.
    run(&[
        "run",
        "--source",
        "shop",
        "--sink",
        "files",
        "--hub",
        &hub,
        "--no-env-file",
        "--quiet",
    ])
    .await
    .expect("run --source/--sink");
    let orders =
        std::fs::read_to_string(Path::new(&out).join("shop/orders.jsonl")).expect("orders written");
    assert_eq!(orders.lines().count(), 2, "{orders}");
    assert!(orders.contains("\"region\":\"eu\""), "{orders}");
    let customers = std::fs::read_to_string(Path::new(&out).join("shop/customers.jsonl"))
        .expect("customers written");
    assert_eq!(customers.lines().count(), 1);
    // A --param override reaches the composed config.
    let other = dir.path().join("other");
    run(&[
        "run",
        "--source",
        "shop",
        "--sink",
        "files",
        "--hub",
        &hub,
        "--no-env-file",
        "--quiet",
        "--param",
        &format!("out_dir={}", other.display()),
    ])
    .await
    .expect("run with --param");
    assert!(other.join("shop/orders.jsonl").is_file());
}

#[tokio::test]
async fn hub_templates_handed_to_run_or_validate_directly_are_redirected() {
    let dir = tempfile::tempdir().unwrap();
    let (hub, _) = fixture(dir.path());
    let tpl = format!("{hub}/source-templates/shop.yaml");
    let err = run(&["run", &tpl, "--no-env-file"])
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("is a hub source-template")
            && err.contains("--source <source-template> --sink <sink-template>"),
        "{err}"
    );
    let err = run(&["validate", &tpl, "--no-env-file", "--no-secrets"])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("is a hub source-template"), "{err}");
    // Unknown ids name the catalog; an incompatible pairing fails at compose.
    let err = run(&[
        "run",
        "--source",
        "nope",
        "--sink",
        "files",
        "--hub",
        &hub,
        "--no-env-file",
    ])
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("no hub template 'nope'") && err.contains("known: shop"),
        "{err}"
    );
    let err = run(&[
        "run",
        "--source",
        "shop",
        "--sink",
        "plain",
        "--hub",
        &hub,
        "--no-env-file",
    ])
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("cannot compose with sink-template 'plain'"),
        "{err}"
    );
    // Lint on a non-template file is a clear error.
    let plain = dir.path().join("plain.yaml");
    std::fs::write(&plain, "version: 1\n").unwrap();
    let err = run(&["hub", "lint", plain.to_str().unwrap()])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("not a hub template"), "{err}");
    // Lint findings exit non-zero.
    let bad = format!("{hub}/source-templates/bad.yaml");
    std::fs::write(
        &bad,
        "kind: source-template\nname: bad\nsource: {type: csv, config: {path: x.csv, api_key: literal-key}}\nstreams: [{name: t}]\n",
    )
    .unwrap();
    let err = run(&["hub", "lint", &bad]).await.unwrap_err().to_string();
    assert!(err.contains("1 template(s) with findings"), "{err}");
    let err = run(&["hub", "lint", "--hub", &hub, "--json"])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("with findings"), "{err}");
}

#[tokio::test]
async fn schema_targets_print() {
    run(&["schema", "source-template"])
        .await
        .expect("schema source-template");
    run(&["schema", "sink-template"])
        .await
        .expect("schema sink-template");
}

/// `--source` without `--sink` (and vice versa) is a clap-level error; so is
/// mixing `--source` with a config path.
#[test]
fn hub_flags_are_mutually_required_and_exclusive_with_a_config_path() {
    assert!(Cli::try_parse_from(["faucet", "run", "--source", "a"]).is_err());
    assert!(Cli::try_parse_from(["faucet", "run", "--sink", "b"]).is_err());
    assert!(
        Cli::try_parse_from(["faucet", "run", "cfg.yaml", "--source", "a", "--sink", "b"]).is_err()
    );
    assert!(
        Cli::try_parse_from([
            "faucet",
            "run",
            "--from-env",
            "--source",
            "a",
            "--sink",
            "b"
        ])
        .is_err()
    );
    assert!(Cli::try_parse_from(["faucet", "run", "--source", "a", "--sink", "b"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "faucet", "validate", "cfg.yaml", "--source", "a", "--sink", "b"
        ])
        .is_err()
    );
    assert!(Cli::try_parse_from(["faucet", "validate", "--source", "a", "--sink", "b"]).is_ok());
}

/// #676: the shipped `faucet-hq/example-csv` × `faucet-hq/sqlite` pairing asks
/// for `overwrite` on every stream. Against a fresh database file the first
/// run creates each table, and the second replaces it — the row counts match
/// the CSVs both times instead of failing on a missing target or doubling.
#[cfg(all(feature = "source-csv", feature = "sink-sqlite"))]
#[tokio::test]
async fn shipped_example_pairing_runs_twice_against_a_fresh_database() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let hub = repo.join("hub");
    let data = hub.join("examples/data");
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("fresh.db");
    let csv_rows = |name: &str| {
        std::fs::read_to_string(data.join(name))
            .unwrap()
            .lines()
            .skip(1)
            .filter(|l| !l.trim().is_empty())
            .count() as i64
    };
    for pass in 1..=2 {
        run(&[
            "run",
            "--source",
            "faucet-hq/example-csv",
            "--sink",
            "faucet-hq/sqlite",
            "--hub",
            hub.to_str().unwrap(),
            "--no-env-file",
            "--quiet",
            "--param",
            &format!("data_dir={}", data.display()),
            "--param",
            &format!("sqlite_path={}", db.display()),
        ])
        .await
        .unwrap_or_else(|e| panic!("pass {pass}: {e}"));
        let pool = sqlx::SqlitePool::connect(&format!("sqlite:{}", db.display()))
            .await
            .unwrap();
        for (table, file) in [("orders", "orders.csv"), ("customers", "customers.csv")] {
            let n: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(n, csv_rows(file), "pass {pass}: {table}");
        }
        pool.close().await;
    }
}

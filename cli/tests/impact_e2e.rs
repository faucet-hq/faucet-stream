//! End-to-end change impact analysis (#707) over the CLI: two chained
//! pipelines A → B recorded into a SQLite catalog, `catalog.datasets:`
//! annotations + `faucet catalog annotate`, then `faucet plan --impact`
//! reporting the downstream breakage, the contract version, owners and
//! consumers.
#![cfg(all(
    feature = "catalog",
    feature = "serve-history-sqlite",
    feature = "source-csv",
    feature = "sink-csv",
    feature = "sink-jsonl"
))]

use assert_cmd::Command;
use predicates::str::contains;
use serde_json::Value;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

fn faucet() -> Command {
    Command::cargo_bin("faucet").unwrap()
}

fn catalog_block(dir: &Path, extra: &str) -> String {
    format!(
        "catalog:\n  url: \"sqlite:{}\"\n  sample_records: 10\n{extra}",
        dir.join("cat.db").display()
    )
}

fn config_a(dir: &Path, transforms: &str, annotations: &str) -> String {
    format!(
        "version: 1\nname: a\n{}pipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n{transforms}  sink: {{ type: csv, config: {{ path: \"{}\" }} }}\n",
        catalog_block(dir, annotations),
        dir.join("in.csv").display(),
        dir.join("a.csv").display(),
    )
}

fn config_b(dir: &Path) -> String {
    format!(
        "version: 1\nname: b\n{}pipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  transforms:\n    - type: rename_field\n      config: {{ fields: {{ email: contact }} }}\n  contract:\n    version: \"7\"\n    fields:\n      - {{ name: id, type: string }}\n      - {{ name: contact, type: string }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        catalog_block(dir, ""),
        dir.join("a.csv").display(),
        dir.join("b.jsonl").display(),
    )
}

fn json_out(cmd: &mut Command) -> (Option<i32>, Value, String) {
    let out = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let v: Value = serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("{e}: {}\n{stderr}", String::from_utf8_lossy(&out.stdout)));
    (out.status.code(), v, stderr)
}

#[test]
fn plan_impact_walks_the_catalog_and_names_contracts_owners_and_consumers() {
    let dir = TempDir::new().unwrap();
    let d = dir.path();
    fs::write(
        d.join("in.csv"),
        "id,email,amount\n1,a@x.io,10\n2,b@x.io,20\n",
    )
    .unwrap();
    // A annotates its own sink (a.csv) through the config block.
    let annotations = format!(
        "  datasets:\n    - dataset: \"file://{}\"\n      owners: [team-a]\n      consumers:\n        - {{ name: raw-export, kind: export, columns: [amount] }}\n",
        d.join("a.csv").display()
    );
    let a_path = d.join("a.yaml");
    fs::write(&a_path, config_a(d, "", &annotations)).unwrap();
    let b_path = d.join("b.yaml");
    fs::write(&b_path, config_b(d)).unwrap();

    // Before any run: no lineage, and the report says so (exit 0 — a preview).
    let (code, plan, _) = json_out(faucet().args(["plan", "--impact", "--json"]).arg(&a_path));
    assert_eq!(code, Some(0));
    assert!(plan["impact"]["dataset"].is_null(), "{plan}");
    assert!(
        plan["impact"]["notes"][0]
            .as_str()
            .unwrap()
            .contains("no recorded run")
    );

    faucet().args(["run"]).arg(&a_path).assert().success();
    faucet().args(["run"]).arg(&b_path).assert().success();

    // The config-block annotation landed on a.csv.
    let (_, datasets, _) = json_out(
        faucet()
            .args(["catalog", "datasets", "--json", "--config"])
            .arg(&a_path),
    );
    let list = datasets["datasets"].as_array().unwrap();
    let a_csv = list
        .iter()
        .find(|x| x["uri"].as_str().unwrap().ends_with("a.csv"))
        .expect("a.csv catalogued");
    assert_eq!(a_csv["owners"], serde_json::json!(["team-a"]));
    assert_eq!(a_csv["consumers"][0]["name"], "raw-export");
    assert_eq!(a_csv["consumers"][0]["registered_by"], "config");
    let b_id = list
        .iter()
        .find(|x| x["uri"].as_str().unwrap().ends_with("b.jsonl"))
        .expect("b.jsonl catalogued")["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Annotate B's sink over the CLI (id prefix resolution included).
    faucet()
        .args(["catalog", "annotate"])
        .arg(&b_id[..8])
        .args(["--config"])
        .arg(&a_path)
        .args([
            "--owner",
            "team-b",
            "--consumer",
            "contacts-dashboard=dashboard",
            "--contact",
            "#bi",
            "--columns",
            "contact",
        ])
        .assert()
        .success()
        .stdout(contains("owners   team-b"))
        .stdout(contains("contacts-dashboard (dashboard) → #bi"));
    faucet()
        .args(["catalog", "show"])
        .arg(&b_id)
        .args(["--config"])
        .arg(&a_path)
        .assert()
        .success()
        .stdout(contains("owners: team-b"))
        .stdout(contains("reads: contact"));
    // Nothing to annotate → a typed refusal, and a malformed consumer flag too.
    faucet()
        .args(["catalog", "annotate"])
        .arg(&b_id)
        .args(["--config"])
        .arg(&a_path)
        .assert()
        .failure()
        .stderr(contains("nothing to annotate"));
    faucet()
        .args(["catalog", "annotate"])
        .arg(&b_id)
        .args(["--config"])
        .arg(&a_path)
        .args(["--consumer", "=dashboard"])
        .assert()
        .failure()
        .stderr(contains("name must not be empty"));

    // Plan A dropping `email` (the column B renames and promises in its
    // contract): breaking at B, contract v7 named, owner + dashboard listed;
    // A's own consumer reads only `amount`, so it is not affected.
    let sample = d.join("sample.jsonl");
    fs::write(&sample, "{\"id\": \"1\", \"amount\": \"10\"}\n").unwrap();
    let (code, plan, _) = json_out(
        faucet()
            .args(["plan", "--impact", "--json", "--sample"])
            .arg(&sample)
            .arg(&a_path),
    );
    assert_eq!(code, Some(0));
    let impact = &plan["impact"];
    assert_eq!(impact["planned_from"], "sample");
    assert_eq!(impact["severity"], "breaking", "{impact}");
    assert_eq!(impact["delta"]["removed"], serde_json::json!(["email"]));
    let affected = impact["affected"].as_array().unwrap();
    assert_eq!(affected.len(), 2, "{impact}");
    assert_eq!(affected[0]["depth"], 0);
    assert!(affected[0]["uri"].as_str().unwrap().ends_with("a.csv"));
    assert_eq!(affected[0]["owners"], serde_json::json!(["team-a"]));
    assert!(affected[0]["consumers"].as_array().unwrap().is_empty());
    let b_hit = &affected[1];
    assert_eq!(b_hit["depth"], 1);
    assert_eq!(b_hit["pipeline"], "b");
    assert_eq!(b_hit["severity"], "breaking");
    assert_eq!(b_hit["columns"][0]["column"], "contact");
    assert_eq!(b_hit["columns"][0]["reads"], serde_json::json!(["email"]));
    assert_eq!(b_hit["contract"]["version"], "7");
    assert_eq!(b_hit["contract"]["fields"], serde_json::json!(["contact"]));
    assert_eq!(b_hit["owners"], serde_json::json!(["team-b"]));
    assert_eq!(b_hit["consumers"][0]["name"], "contacts-dashboard");
    assert_eq!(b_hit["consumers"][0]["severity"], "breaking");
    assert_eq!(impact["owners"], serde_json::json!(["team-a", "team-b"]));

    // Human rendering.
    faucet()
        .args(["plan", "--impact", "--sample"])
        .arg(&sample)
        .arg(&a_path)
        .assert()
        .success()
        .stdout(contains("impact: breaking"))
        .stdout(contains("[breaking] depth 1"))
        .stdout(contains("contact reads email (removed)"))
        .stdout(contains("contract v7 of b / row-0 declares: contact"))
        .stdout(contains("owners to notify: team-a, team-b"));

    // Without a sample, the planned schema comes from the catalog's source
    // schema through A's chain: a rename in A is reported as a rename.
    fs::write(
        &a_path,
        config_a(
            d,
            "  transforms:\n    - type: rename_field\n      config: { fields: { email: mail } }\n",
            "",
        ),
    )
    .unwrap();
    let (_, plan, _) = json_out(faucet().args(["plan", "--impact", "--json"]).arg(&a_path));
    let impact = &plan["impact"];
    assert_eq!(impact["planned_from"], "lineage", "{impact}");
    assert_eq!(
        impact["delta"]["renamed"],
        serde_json::json!([{ "from": "email", "to": "mail" }])
    );
    assert_eq!(impact["severity"], "breaking");

    // `--impact` needs a catalog block.
    let bare = d.join("bare.yaml");
    fs::write(
        &bare,
        format!(
            "version: 1\nname: a\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: csv, config: {{ path: \"{}\" }} }}\n",
            d.join("in.csv").display(),
            d.join("a.csv").display()
        ),
    )
    .unwrap();
    faucet()
        .args(["plan", "--impact"])
        .arg(&bare)
        .assert()
        .failure()
        .stderr(contains("needs a `catalog:` block"));
}

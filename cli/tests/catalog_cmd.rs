//! Drives the `faucet catalog` subcommand bodies directly (#279) — the
//! commands are clap-free by convention exactly so integration tests can call
//! them in-process (spawned-binary tests are skipped under coverage). Seeds a
//! real SQLite catalog store by running a csv→jsonl pipeline twice (with a
//! schema change), then exercises datasets / show / lineage in both human and
//! `--json` modes plus every user-facing error path.
#![cfg(all(
    feature = "catalog",
    feature = "serve-history-sqlite",
    feature = "source-csv",
    feature = "sink-jsonl"
))]

use faucet_cli::cli::{
    CatalogArgs, CatalogCommand, CatalogConfigArgs, CatalogDatasetsArgs, CatalogLineageArgs,
    CatalogShowArgs,
};
use faucet_cli::commands::catalog as catalog_cmd;
use faucet_cli::serve::history::catalog::CatalogListFilter;
use std::path::{Path, PathBuf};

fn config_yaml(dir: &Path) -> String {
    format!(
        "version: 1\nname: catalog-cmd\ncatalog:\n  url: \"sqlite:{dir}/cat.db\"\n  sample_records: 10\nprofiling: {{ min_history: 2 }}\npipeline:\n  source: {{ type: csv, config: {{ path: \"{dir}/in.csv\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{dir}/out.jsonl\" }} }}\n  state: {{ type: file, config: {{ path: \"{dir}/state\" }} }}\n",
        dir = dir.display()
    )
}

fn common(config: Option<PathBuf>, json: bool) -> CatalogConfigArgs {
    CatalogConfigArgs {
        config,
        env_file: None,
        no_env_file: true,
        profile: None,
        json,
    }
}

/// Seed the store: two runs, the second with an added column.
async fn seed(dir: &Path) -> PathBuf {
    let yaml = config_yaml(dir);
    let config_path = dir.join("faucet.yaml");
    std::fs::write(&config_path, &yaml).unwrap();

    std::fs::write(dir.join("in.csv"), "id,name\n1,alice\n2,bob\n").unwrap();
    faucet_cli::run_from_yaml_str(&yaml).await.expect("run 1");
    std::fs::write(
        dir.join("in.csv"),
        "id,name,email\n1,alice,a@x.io\n2,bob,b@x.io\n",
    )
    .unwrap();
    faucet_cli::run_from_yaml_str(&yaml).await.expect("run 2");
    config_path
}

/// The seeded source dataset's id, read back through the store.
async fn source_dataset_id(dir: &Path) -> String {
    let handle = faucet_cli::catalog::connect_from_spec(&faucet_cli::catalog::CatalogSpec {
        url: format!("sqlite:{}/cat.db", dir.display()),
        sample_records: 10,
        datasets: Vec::new(),
    })
    .await
    .unwrap();
    let page = handle
        .store
        .catalog_list_datasets(&CatalogListFilter {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.datasets.len(), 2, "source + sink datasets seeded");
    page.datasets
        .iter()
        .find(|d| d.kind == "csv")
        .expect("csv dataset")
        .id
        .clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_command_datasets_show_lineage_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let config = seed(dir.path()).await;
    let id = source_dataset_id(dir.path()).await;

    // datasets — human, json, and filtered variants all succeed.
    for (json, kind, q) in [
        (false, None, None),
        (true, None, None),
        (false, Some("csv".to_string()), None),
        (false, None, Some("out.jsonl".to_string())),
    ] {
        catalog_cmd::run(CatalogArgs {
            command: CatalogCommand::Datasets(CatalogDatasetsArgs {
                common: common(Some(config.clone()), json),
                kind,
                q,
                limit: 50,
            }),
        })
        .await
        .expect("datasets");
    }

    // show — full id, unique prefix, human + json.
    for (shown, json) in [(id.clone(), false), (id[..8].to_string(), true)] {
        catalog_cmd::run(CatalogArgs {
            command: CatalogCommand::Show(CatalogShowArgs {
                id: shown,
                common: common(Some(config.clone()), json),
            }),
        })
        .await
        .expect("show");
    }

    // show — the sink dataset carries the column profile section (#708), in
    // both renderings; the store agrees.
    let handle = faucet_cli::catalog::connect_from_spec(&faucet_cli::catalog::CatalogSpec {
        url: format!("sqlite:{}/cat.db", dir.path().display()),
        sample_records: 10,
        datasets: Vec::new(),
    })
    .await
    .unwrap();
    let sink_id = handle
        .store
        .catalog_list_datasets(&CatalogListFilter {
            kind: Some("jsonl".into()),
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap()
        .datasets[0]
        .id
        .clone();
    let detail = handle
        .store
        .catalog_get_dataset(&sink_id)
        .await
        .unwrap()
        .unwrap();
    let profile = detail.profile.expect("profile on the sink dataset");
    assert_eq!(profile.history.len(), 2);
    assert!(profile.latest.profile.columns.contains_key("email"));
    for json in [false, true] {
        catalog_cmd::run(CatalogArgs {
            command: CatalogCommand::Show(CatalogShowArgs {
                id: sink_id.clone(),
                common: common(Some(config.clone()), json),
            }),
        })
        .await
        .expect("show sink");
    }

    // show — unknown id is a clear config error, not a panic.
    let err = catalog_cmd::run(CatalogArgs {
        command: CatalogCommand::Show(CatalogShowArgs {
            id: "ffffffffffffffff".into(),
            common: common(Some(config.clone()), false),
        }),
    })
    .await
    .unwrap_err();
    assert!(err.to_string().contains("no catalogued dataset"), "{err}");

    // show — an empty prefix matches both datasets → ambiguous error.
    let err = catalog_cmd::run(CatalogArgs {
        command: CatalogCommand::Show(CatalogShowArgs {
            id: "".into(),
            common: common(Some(config.clone()), false),
        }),
    })
    .await
    .unwrap_err();
    assert!(err.to_string().contains("ambiguous"), "{err}");

    // lineage — whole graph (human + json) and the rooted slice.
    for (json, root) in [(false, None), (true, None), (false, Some(id.clone()))] {
        catalog_cmd::run(CatalogArgs {
            command: CatalogCommand::Lineage(CatalogLineageArgs {
                common: common(Some(config.clone()), json),
                root,
                depth: 3,
            }),
        })
        .await
        .expect("lineage");
    }

    // lineage — an unknown root yields the empty-graph message, not an error.
    catalog_cmd::run(CatalogArgs {
        command: CatalogCommand::Lineage(CatalogLineageArgs {
            common: common(Some(config.clone()), false),
            root: Some("ffffffffffffffff".into()),
            depth: 3,
        }),
    })
    .await
    .expect("empty rooted lineage");
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_command_errors_without_a_catalog_block() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("plain.yaml");
    std::fs::write(
        &config,
        "version: 1\npipeline:\n  source: { type: csv, config: { path: ./in.csv } }\n  sink: { type: jsonl, config: { path: ./out.jsonl } }\n",
    )
    .unwrap();
    let err = catalog_cmd::run(CatalogArgs {
        command: CatalogCommand::Datasets(CatalogDatasetsArgs {
            common: common(Some(config), false),
            kind: None,
            q: None,
            limit: 10,
        }),
    })
    .await
    .unwrap_err();
    assert!(err.to_string().contains("no `catalog:` block"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_command_on_an_empty_store_prints_the_empty_message() {
    // A memory store that no run has written to: datasets + lineage both
    // succeed with their "empty" renderings.
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("faucet.yaml");
    std::fs::write(
        &config,
        "version: 1\ncatalog: { url: memory }\npipeline:\n  source: { type: csv, config: { path: ./in.csv } }\n  sink: { type: jsonl, config: { path: ./out.jsonl } }\n",
    )
    .unwrap();
    catalog_cmd::run(CatalogArgs {
        command: CatalogCommand::Datasets(CatalogDatasetsArgs {
            common: common(Some(config.clone()), false),
            kind: None,
            q: None,
            limit: 10,
        }),
    })
    .await
    .expect("empty datasets");
    catalog_cmd::run(CatalogArgs {
        command: CatalogCommand::Lineage(CatalogLineageArgs {
            common: common(Some(config), false),
            root: None,
            depth: 3,
        }),
    })
    .await
    .expect("empty lineage");
}

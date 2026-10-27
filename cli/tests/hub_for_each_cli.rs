//! `for_each:` streams (#952) through the hub CLI, in-process: `hub check`
//! expands with placeholder items, `hub compose` needs the list, `validate`
//! and `run --source/--sink --param` expand with the caller's items, `hub
//! rows` lists the rendered rows and selects by rendered name, and a sink
//! whose destination ignores the item is reported as a collision (#950).

use clap::Parser as _;
use faucet_cli::cli::Cli;
use std::path::Path;

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

fn fixture(dir: &Path) -> String {
    let hub = dir.join("hub");
    std::fs::create_dir_all(hub.join("source-templates")).unwrap();
    std::fs::create_dir_all(hub.join("sink-templates")).unwrap();
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::write(data.join("orders.csv"), "id\n1\n2\n").unwrap();
    std::fs::write(data.join("customers.csv"), "id\n7\n").unwrap();
    std::fs::write(
        hub.join("source-templates/files.yaml"),
        format!(
            r#"
kind: source-template
name: files
description: One stream per chosen CSV file
params:
  data_dir: {{ type: string, default: "{data}" }}
  tables: {{ type: list, required: true, items: {{ type: string }} }}
source:
  type: csv
  config:
    path: "${{param.data_dir}}/orders.csv"
streams:
  - for_each: ${{param.tables}}
    as: t
    name: "csv_${{t}}"
    source: {{ config: {{ path: "${{param.data_dir}}/${{t}}.csv" }} }}
"#,
            data = data.display()
        ),
    )
    .unwrap();
    std::fs::write(
        hub.join("source-templates/named.yaml"),
        format!(
            r#"
kind: source-template
name: named
description: Repeated streams named by a param
params:
  prefix: {{ type: string, required: true }}
  tables: {{ type: list, items: {{ type: string }} }}
source:
  type: csv
  config:
    path: "{data}/orders.csv"
streams:
  - for_each: ${{param.tables}}
    as: t
    name: "${{param.prefix}}_${{t}}"
    source: {{ config: {{ path: "{data}/${{t}}.csv" }} }}
"#,
            data = data.display()
        ),
    )
    .unwrap();
    std::fs::write(
        hub.join("sink-templates/jsonl.yaml"),
        format!(
            "kind: sink-template\nname: jsonl\ndescription: files\nparams:\n  out_dir: {{ type: string, default: \"{}\" }}\nsink:\n  type: jsonl\n  config: {{}}\nper_stream:\n  path: \"${{param.out_dir}}/${{stream}}.jsonl\"\n",
            dir.join("out").display()
        ),
    )
    .unwrap();
    std::fs::write(
        hub.join("sink-templates/single.yaml"),
        format!(
            "kind: sink-template\nname: single\ndescription: one file\nsink:\n  type: jsonl\n  config: {{}}\nper_stream:\n  path: \"{}\"\n",
            dir.join("one.jsonl").display()
        ),
    )
    .unwrap();
    hub.display().to_string()
}

#[test]
fn for_each_streams_through_the_hub_cli() {
    let dir = tempfile::tempdir().unwrap();
    let hub = fixture(dir.path());
    let root = dir.path().to_path_buf();
    on_big_stack(move || async move {
        let h = hub.as_str();
        run(&[
            "hub", "check", "--source", "files", "--sink", "jsonl", "--hub", h,
        ])
        .await
        .expect("placeholder items check");
        let err = run(&[
            "hub", "check", "--source", "files", "--sink", "single", "--hub", h,
        ])
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("same destination for every item"), "{err}");

        let err = run(&[
            "hub", "compose", "--source", "files", "--sink", "jsonl", "--hub", h,
        ])
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("pass each with `--param"), "{err}");
        let err = run(&[
            "hub",
            "compose",
            "--source",
            "named",
            "--sink",
            "jsonl",
            "--hub",
            h,
            "--param",
            "tables=orders",
        ])
        .await
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("names its repeated streams with param(s) prefix"),
            "{err}"
        );
        let named = root.join("named.yaml");
        run(&[
            "hub",
            "compose",
            "--source",
            "named",
            "--sink",
            "jsonl",
            "--hub",
            h,
            "--param",
            "tables=orders",
            "--param",
            "prefix=eu",
            "--out",
            named.to_str().unwrap(),
        ])
        .await
        .unwrap();
        let text = std::fs::read_to_string(&named).unwrap();
        assert!(
            text.contains("id: eu_orders") && !text.contains("param_"),
            "{text}"
        );
        let out = root.join("composed.yaml");
        let o = out.to_str().unwrap();
        run(&[
            "hub",
            "compose",
            "--source",
            "files",
            "--sink",
            "jsonl",
            "--hub",
            h,
            "--param",
            "tables=orders,customers",
            "--out",
            o,
        ])
        .await
        .unwrap();
        let text = std::fs::read_to_string(&out).unwrap();
        assert!(
            text.contains("id: csv_orders") && text.contains("id: csv_customers"),
            "{text}"
        );

        run(&[
            "validate", "--source", "files", "--sink", "jsonl", "--hub", h,
        ])
        .await
        .expect("validates with placeholder items");
        run(&[
            "validate",
            "--source",
            "files",
            "--sink",
            "jsonl",
            "--hub",
            h,
            "--param",
            r#"tables=["orders"]"#,
        ])
        .await
        .unwrap();

        run(&[
            "hub",
            "rows",
            "files",
            "--sink",
            "jsonl",
            "--hub",
            h,
            "--param",
            "tables=orders,customers",
            "--select",
            "csv_customers",
            "--json",
        ])
        .await
        .expect("rows with a selection by rendered name");
        run(&["hub", "rows", "files", "--hub", h])
            .await
            .expect("placeholder rows");

        run(&[
            "run",
            "--source",
            "files",
            "--sink",
            "jsonl",
            "--hub",
            h,
            "--param",
            "tables=orders,customers",
        ])
        .await
        .unwrap();
        let orders = std::fs::read_to_string(root.join("out/csv_orders.jsonl")).unwrap();
        assert_eq!(orders.lines().count(), 2, "{orders}");
        let customers = std::fs::read_to_string(root.join("out/csv_customers.jsonl")).unwrap();
        assert_eq!(customers.lines().count(), 1, "{customers}");

        let err = run(&[
            "run",
            "--source",
            "files",
            "--sink",
            "jsonl",
            "--hub",
            h,
            "--param",
            "tables=a..b",
        ])
        .await
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("is not a stream name") || err.contains("`..`"),
            "{err}"
        );
    });
}

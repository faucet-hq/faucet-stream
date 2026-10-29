//! Every `StdoutFormat` written to the real process stdout of a child process.
use faucet_core::Sink;
use faucet_sink_stdout::{StdoutFormat, StdoutSink, StdoutSinkConfig};
use serde_json::json;
use std::process::Command;

const CHILD: &str = "FAUCET_STDOUT_FORMAT_CHILD";

fn records() -> Vec<serde_json::Value> {
    vec![
        json!({"id": 1, "name": "a,b", "tags": ["x"]}),
        json!({"id": 2, "name": "say \"hi\"", "tags": []}),
    ]
}

fn child_output(format: &str) -> String {
    let out = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_writes_to_real_stdout",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, format)
        .output()
        .expect("spawn child");
    assert!(
        out.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8(out.stdout).unwrap();
    let start = text.find("<<BEGIN>>\n").expect("begin marker") + "<<BEGIN>>\n".len();
    let end = text.find("<<END>>").expect("end marker");
    text[start..end].to_string()
}

#[tokio::test]
async fn child_writes_to_real_stdout() {
    let Ok(format) = std::env::var(CHILD) else {
        return;
    };
    let format: StdoutFormat = serde_json::from_value(json!(format)).unwrap();
    let sink = StdoutSink::new(StdoutSinkConfig::new().format(format));
    use std::io::Write;
    println!("<<BEGIN>>");
    std::io::stdout().flush().unwrap();
    assert_eq!(sink.write_batch(&records()).await.unwrap(), 2);
    sink.flush().await.unwrap();
    print!("<<END>>");
    std::io::stdout().flush().unwrap();
}

#[test]
fn csv_quotes_per_rfc_4180_on_real_stdout() {
    assert_eq!(
        child_output("csv"),
        "1,\"a,b\",\"[\"\"x\"\"]\"\n2,\"say \"\"hi\"\"\",[]\n"
    );
}

#[test]
fn pretty_json_round_trips_on_real_stdout() {
    let out = child_output("pretty_json");
    assert!(out.contains("\n  \"id\": 1"), "not indented: {out}");
    let parsed: Vec<serde_json::Value> = serde_json::Deserializer::from_str(&out)
        .into_iter()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(parsed, records());
}

#[test]
fn tsv_and_json_lines_on_real_stdout() {
    assert_eq!(child_output("tsv"), "1\ta,b\t[\"x\"]\n2\tsay \"hi\"\t[]\n");
    let lines: Vec<serde_json::Value> = child_output("json_lines")
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines, records());
}

//! The message model and redactor moved to `faucet-common-singer`; these
//! paths are this crate's public API and must keep resolving.

use faucet_source_singer::message::{SingerMessage as ModMessage, parse_line as mod_parse};
use faucet_source_singer::process::Redactor;
use faucet_source_singer::{SingerMessage, parse_line};

#[test]
fn moved_items_resolve_at_their_original_paths() {
    let line = r#"{"type":"RECORD","stream":"s","record":{"id":1}}"#;
    let a: SingerMessage = parse_line(line).unwrap();
    let b: ModMessage = mod_parse(line).unwrap();
    assert!(matches!(a, SingerMessage::Record { ref stream, .. } if stream == "s"));
    assert!(matches!(b, ModMessage::Record { .. }));
    let r = Redactor::from_config(&serde_json::json!({"password": "hunter2"}));
    assert!(!r.redact("pw=hunter2").contains("hunter2"));
}

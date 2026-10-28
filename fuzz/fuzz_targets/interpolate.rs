#![no_main]

use std::collections::HashMap;

use chrono::{DateTime, FixedOffset};
use faucet_cli::interpolate::{
    contains_unescaped, interpolate_record, interpolate_record_file_path, interpolate_record_path,
    interpolate_record_xml, iter_directives, resolve_now, unescape,
};
use libfuzzer_sys::fuzz_target;
use serde_json::json;

// Only the record-time and `${now.*}` passes: load-time `${file:}` / `${env:}`
// read the host, which a fuzzer must not drive.
fuzz_target!(|data: &str| {
    // Text after the first NUL is the record value tokens resolve to.
    let (input, value) = data.split_once('\0').unwrap_or((data, "v"));
    let ctx = HashMap::from([(
        "p".to_owned(),
        json!({ "v": value, "a": [value, 1], "n": null, "o": { "k": value } }),
    )]);

    for _ in iter_directives(input) {}
    let _ = unescape(input);

    let record = interpolate_record(input, &ctx);
    if !contains_unescaped(input, "${") {
        assert_eq!(
            record.as_deref().ok(),
            Some(input),
            "text without a token changed"
        );
    }
    let _ = interpolate_record_path(input, &ctx);
    let _ = interpolate_record_xml(input, &ctx);
    if let Ok(path) = interpolate_record_file_path(input, &ctx) {
        let single = input
            .strip_prefix("${")
            .and_then(|rest| rest.strip_suffix('}'))
            .is_some_and(|body| !body.is_empty() && !body.contains('}'));
        if single {
            assert!(!path.contains('\0'));
            assert!(
                !path.split(['/', '\\']).any(|c| c == ".."),
                "{path:?} climbs out"
            );
            assert!(!path.starts_with(['/', '\\', '~']), "{path:?} is anchored");
        }
    }

    let clock: DateTime<FixedOffset> =
        DateTime::parse_from_rfc3339("2026-10-28T12:34:56.789+05:30").unwrap();
    let _ = resolve_now(input, clock);
});

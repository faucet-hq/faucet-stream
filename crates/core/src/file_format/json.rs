//! JSON-shaped file formats: JSON Lines, a single JSON array, and raw text.
//!
//! These need no extra dependency, so they are always compiled — which also
//! makes them the formats a connector can fall back to when no feature is on.

use crate::error::FaucetError;
use serde_json::Value;

/// The key a [`RawText`](super::FileFormat::RawText) body lands under.
pub const RAW_TEXT_FIELD: &str = "text";

/// One JSON value per line.
///
/// Blank lines are skipped rather than reported: a trailing newline is the
/// normal end of an NDJSON file, and a writer that emits `\r\n` should not fail
/// a reader.
pub fn decode_lines(bytes: &[u8]) -> Result<Vec<Value>, FaucetError> {
    let text = as_utf8(bytes, "json_lines")?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v = serde_json::from_str(line)
            .map_err(|e| FaucetError::Source(format!("json_lines: line {}: {e}", i + 1)))?;
        check_numbers_exact(line.as_bytes())
            .map_err(|e| FaucetError::Source(format!("json_lines: line {}: {e}", i + 1)))?;
        out.push(v);
    }
    Ok(out)
}

/// A single JSON array (or a single object, which becomes one record).
pub fn decode_array(bytes: &[u8]) -> Result<Vec<Value>, FaucetError> {
    let v: Value = serde_json::from_slice(bytes)
        .map_err(|e| FaucetError::Source(format!("json_array: {e}")))?;
    check_numbers_exact(bytes).map_err(|e| FaucetError::Source(format!("json_array: {e}")))?;
    Ok(match v {
        Value::Array(a) => a,
        other => vec![other],
    })
}

/// Refuse a number literal the parsed value cannot hold exactly (CORE-69).
///
/// Without serde_json's `arbitrary_precision`, an integer beyond `u64` or a
/// decimal with more than ~17 significant digits is stored as the nearest
/// `f64`, silently changing an id or an amount. Each literal is compared, as a
/// decimal value, with what parsing it yields; a literal of at most 15
/// significant digits always survives and is skipped cheaply. Runs only on
/// input that already parsed, so it never has to report a syntax error.
fn check_numbers_exact(bytes: &[u8]) -> Result<(), String> {
    match unfaithful_literals(bytes).first() {
        Some(r) => check_literal(std::str::from_utf8(&bytes[r.clone()]).unwrap_or_default()),
        None => Ok(()),
    }
}

/// Byte ranges of the number literals in JSON `bytes` (outside strings) that
/// parse to a value which does not spell them again — see
/// [`literal_is_faithful`]. A literal of at most 15 significant digits is
/// always faithful and skipped cheaply.
pub(crate) fn unfaithful_literals(bytes: &[u8]) -> Vec<std::ops::Range<usize>> {
    if !may_hold_long_literal(bytes) {
        return Vec::new();
    }
    scan_literals(bytes)
}

const fn numeric_bytes() -> [bool; 256] {
    let mut t = [false; 256];
    let set = b"0123456789-+.eE";
    let mut i = 0;
    while i < set.len() {
        t[set[i] as usize] = true;
        i += 1;
    }
    t
}

const NUMERIC: [bool; 256] = numeric_bytes();

/// Whether `bytes` holds a run of number-literal bytes with more than 15
/// digits, in or out of a string. `false` means [`scan_literals`] finds
/// nothing; such a run is at least 16 bytes long, so only every 16th byte is
/// looked at.
pub(crate) fn may_hold_long_literal(bytes: &[u8]) -> bool {
    const STEP: usize = 16;
    let mut i = STEP - 1;
    while i < bytes.len() {
        if !NUMERIC[bytes[i] as usize] {
            i += STEP;
            continue;
        }
        let mut start = i;
        while start > 0 && NUMERIC[bytes[start - 1] as usize] {
            start -= 1;
        }
        let mut end = i + 1;
        while end < bytes.len() && NUMERIC[bytes[end] as usize] {
            end += 1;
        }
        if bytes[start..end]
            .iter()
            .filter(|b| b.is_ascii_digit())
            .count()
            > 15
        {
            return true;
        }
        i = end + STEP;
    }
    false
}

fn scan_literals(bytes: &[u8]) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut in_string = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            match b {
                b'\\' => i += 1,
                b'"' => in_string = false,
                _ => {}
            }
            i += 1;
            continue;
        }
        if b == b'"' {
            in_string = true;
            i += 1;
            continue;
        }
        if b == b'-' || b.is_ascii_digit() {
            let start = i;
            while i < bytes.len()
                && matches!(bytes[i], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
            {
                i += 1;
            }
            let token = std::str::from_utf8(&bytes[start..i]).unwrap_or_default();
            if token.bytes().filter(u8::is_ascii_digit).count() > 15
                && let Ok(parsed) = serde_json::from_str::<serde_json::Number>(token)
                && !literal_is_faithful(token, &parsed)
            {
                out.push(start..i);
            }
            continue;
        }
        i += 1;
    }
    out
}

fn check_literal(token: &str) -> Result<(), String> {
    let Ok(parsed) = serde_json::from_str::<serde_json::Number>(token) else {
        return Ok(());
    };
    let back = parsed.to_string();
    if literal_is_faithful(token, &parsed) {
        Ok(())
    } else {
        Err(format!(
            "number {token} cannot be represented exactly (it would be read as {back}); \
             quote it as a string in the source data to keep every digit"
        ))
    }
}

/// Whether `token` names the value it parsed to. An integer literal must stay
/// an integer. A decimal literal of `n` significant digits is faithful when the
/// parsed double, rounded to `n` significant digits, spells it again — so the
/// `%.17g` rendering `0.10000000000000001` is accepted like `0.1`, while a
/// literal whose digits the double cannot carry is refused.
fn literal_is_faithful(token: &str, parsed: &serde_json::Number) -> bool {
    let literal = canonical_decimal(token);
    let is_integer = !token.contains(['.', 'e', 'E']);
    match parsed.as_f64() {
        Some(f) if parsed.is_f64() => {
            if is_integer {
                return false;
            }
            let digits = literal.as_ref().map_or(1, |(_, d, _)| d.len().max(1));
            canonical_decimal(&format!("{:.*e}", digits - 1, f)) == literal
        }
        _ => canonical_decimal(&parsed.to_string()) == literal,
    }
}

/// `(negative, significant digits, decimal-point position)` with leading and
/// trailing zeros stripped, so `1.50`, `15e-1` and `0.15e1` compare equal.
fn canonical_decimal(s: &str) -> Option<(bool, String, i64)> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (mantissa, exp) = match body.find(['e', 'E']) {
        Some(k) => (&body[..k], body[k + 1..].parse::<i64>().ok()?),
        None => (body, 0),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits: String = format!("{int}{frac}");
    let mut point = (int.len() as i64).checked_add(exp)?;
    let trimmed = digits.trim_start_matches('0');
    point -= (digits.len() - trimmed.len()) as i64;
    let trimmed = trimmed.trim_end_matches('0');
    if trimmed.is_empty() {
        return Some((false, String::new(), 0));
    }
    Some((neg, trimmed.to_string(), point))
}

/// The whole body as one record.
pub fn decode_raw_text(bytes: &[u8]) -> Result<Vec<Value>, FaucetError> {
    let text = as_utf8(bytes, "raw_text")?;
    Ok(vec![serde_json::json!({ RAW_TEXT_FIELD: text })])
}

/// One JSON value per line, newline-terminated.
pub fn encode_lines(records: &[Value]) -> Result<Vec<u8>, FaucetError> {
    let mut out = Vec::new();
    for (i, r) in records.iter().enumerate() {
        serde_json::to_writer(&mut out, r)
            .map_err(|e| FaucetError::Sink(format!("json_lines: {e}")))?;
        out.push(b'\n');
        if i == 0 {
            reserve_like_first(&mut out, records.len());
        }
    }
    Ok(out)
}

/// A single JSON array.
pub fn encode_array(records: &[Value]) -> Result<Vec<u8>, FaucetError> {
    serde_json::to_vec(records).map_err(|e| FaucetError::Sink(format!("json_array: {e}")))
}

/// The inverse of [`decode_raw_text`]: each record's `text` field, one per line.
///
/// A record without a `text` field is written as its JSON form rather than
/// skipped — dropping a record because it does not match the expected shape is
/// the silent-data-loss failure this project treats as the worst class of bug.
pub fn encode_raw_text(records: &[Value]) -> Result<Vec<u8>, FaucetError> {
    let mut out = Vec::new();
    for (i, r) in records.iter().enumerate() {
        match r.get(RAW_TEXT_FIELD) {
            Some(Value::String(s)) => out.extend_from_slice(s.as_bytes()),
            _ => out.extend_from_slice(super::cell_str(r).as_bytes()),
        }
        out.push(b'\n');
        if i == 0 {
            reserve_like_first(&mut out, records.len());
        }
    }
    Ok(out)
}

const PAGE_PRESIZE_CAP: usize = 8 * 1024 * 1024;

fn reserve_like_first(out: &mut Vec<u8>, records: usize) {
    let rest = out.len().saturating_mul(records.saturating_sub(1));
    out.reserve(rest.min(PAGE_PRESIZE_CAP));
}

fn as_utf8<'a>(bytes: &'a [u8], what: &str) -> Result<&'a str, FaucetError> {
    std::str::from_utf8(bytes)
        .map_err(|e| FaucetError::Source(format!("{what}: body is not UTF-8: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numbers_that_would_round_are_refused() {
        // Exact: big u64, i64::MIN, short decimals, strings that look like numbers.
        let ok = br#"[{"a": 18446744073709551615, "b": -9223372036854775808, "c": 0.1,
            "d": 1.50, "e": "123456789012345678901234567890", "f": 1.0000000000000000e-5,
            "g": 0.0000000000000000}]"#;
        assert_eq!(decode_array(ok).unwrap().len(), 1);
        let err = decode_array(br#"[{"id": 18446744073709551616}]"#).unwrap_err();
        assert!(err.to_string().contains("18446744073709551616"), "{err}");
        let err = decode_lines(b"{\"x\":\"\\\"\"}\n{\"amt\": 0.12345678901234567890}\n")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("line 2") && err.contains("0.12345678901234567890"),
            "{err}"
        );
    }

    #[test]
    fn seventeen_digit_float_renderings_are_accepted() {
        let ok = br#"[{"a": 0.10000000000000001, "b": 3.1415926535897931,
            "c": 2.2250738585072014e-308, "d": -0.30000000000000004,
            "e": 0.1000000000000000055511151231257827}]"#;
        let rec = &decode_array(ok).unwrap()[0];
        assert_eq!(rec["a"], json!(0.1));
        assert_eq!(rec["b"], json!(std::f64::consts::PI));
        let err = decode_array(br#"[{"amt": 1234567890123456.7}]"#).unwrap_err();
        assert!(err.to_string().contains("1234567890123456.7"), "{err}");
        assert!(!literal_is_faithful(
            "18446744073709551616",
            &serde_json::from_str("18446744073709551616").unwrap()
        ));
    }

    #[test]
    fn canonical_decimal_normalizes_spellings() {
        assert_eq!(canonical_decimal("1.50"), canonical_decimal("15e-1"));
        assert_eq!(canonical_decimal("0.15e1"), canonical_decimal("1.5"));
        assert_eq!(canonical_decimal("-0.0"), canonical_decimal("0"));
        assert_ne!(canonical_decimal("-1"), canonical_decimal("1"));
        assert_eq!(canonical_decimal("1e99999999999999999999"), None);
        assert!(check_literal("1e99999").is_ok());
    }

    #[test]
    fn json_lines_round_trips_and_tolerates_blank_lines() {
        let recs = vec![json!({"a": 1}), json!({"a": 2})];
        let bytes = encode_lines(&recs).expect("encode");
        assert_eq!(bytes, b"{\"a\":1}\n{\"a\":2}\n");
        assert_eq!(decode_lines(&bytes).expect("decode"), recs);
        // A trailing newline and a \r\n writer must both read back cleanly.
        assert_eq!(
            decode_lines(b"{\"a\":1}\r\n\n{\"a\":2}\n").expect("decode"),
            recs
        );
    }

    #[test]
    fn a_bad_line_names_its_line_number() {
        let err = decode_lines(b"{\"a\":1}\nnot json\n").expect_err("bad line");
        assert!(err.to_string().contains("line 2"), "{err}");
    }

    #[test]
    fn json_array_accepts_an_array_or_a_bare_object() {
        assert_eq!(
            decode_array(br#"[{"a":1},{"a":2}]"#).expect("array"),
            vec![json!({"a": 1}), json!({"a": 2})]
        );
        assert_eq!(
            decode_array(br#"{"a":1}"#).expect("object"),
            vec![json!({"a": 1})]
        );
        assert_eq!(
            encode_array(&[json!({"a": 1})]).expect("encode"),
            br#"[{"a":1}]"#
        );
    }

    #[test]
    fn raw_text_round_trips_through_the_text_field() {
        let recs = decode_raw_text(b"hello\nworld").expect("decode");
        assert_eq!(recs, vec![json!({"text": "hello\nworld"})]);
        assert_eq!(encode_raw_text(&recs).expect("encode"), b"hello\nworld\n");
    }

    #[test]
    fn raw_text_encode_never_drops_a_record_of_the_wrong_shape() {
        let out = encode_raw_text(&[json!({"other": 1})]).expect("encode");
        assert_eq!(out, b"{\"other\":1}\n");
    }

    #[test]
    fn non_utf8_is_reported_not_replaced() {
        let err = decode_lines(&[0xff, 0xfe]).expect_err("invalid utf-8");
        assert!(err.to_string().contains("not UTF-8"), "{err}");
    }

    #[test]
    fn line_pages_are_presized_without_changing_their_bytes() {
        let records: Vec<Value> = (0..50)
            .map(|i| serde_json::json!({"i": i, "text": "t".repeat(i), "o": {"k": [i]}}))
            .collect();
        let mut old = Vec::new();
        for r in &records {
            serde_json::to_writer(&mut old, r).unwrap();
            old.push(b'\n');
        }
        assert_eq!(encode_lines(&records).unwrap(), old);
        assert!(encode_lines(&[]).unwrap().is_empty());

        let texts = vec![
            serde_json::json!({"text": "first"}),
            serde_json::json!({"other": 1}),
            serde_json::json!("bare"),
            serde_json::json!({"text": 5}),
        ];
        let old: Vec<u8> = texts
            .iter()
            .flat_map(|r| {
                let line = match r.get(RAW_TEXT_FIELD) {
                    Some(Value::String(s)) => s.clone(),
                    _ => super::super::cell_text(r),
                };
                format!("{line}\n").into_bytes()
            })
            .collect();
        assert_eq!(encode_raw_text(&texts).unwrap(), old);

        let mut out = vec![0u8; 3 * 1024 * 1024];
        reserve_like_first(&mut out, 1_000_000);
        assert!(out.capacity() < 3 * 1024 * 1024 + 2 * PAGE_PRESIZE_CAP);
        let mut one = vec![0u8; 10];
        reserve_like_first(&mut one, 1);
        assert_eq!(one.len(), 10);
    }

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    fn random_number(rng: &mut Rng) -> String {
        const EDGES: [&str; 12] = [
            "9007199254740992",
            "9007199254740993",
            "9223372036854775807",
            "9223372036854775808",
            "-9223372036854775808",
            "-9223372036854775809",
            "18446744073709551615",
            "18446744073709551616",
            "0.10000000000000001",
            "1.7976931348623157e308",
            "123456789012345.6",
            "1234567890123456e-3",
        ];
        if rng.below(4) == 0 {
            return EDGES[rng.below(EDGES.len())].to_string();
        }
        let mut out = String::new();
        if rng.below(3) == 0 {
            out.push('-');
        }
        let int = 1 + rng.below(22);
        for i in 0..int {
            let d = if i == 0 && int > 1 {
                1 + rng.below(9)
            } else {
                rng.below(10)
            };
            out.push(char::from(b'0' + d as u8));
        }
        if rng.below(3) == 0 {
            out.push('.');
            for _ in 0..1 + rng.below(20) {
                out.push(char::from(b'0' + rng.below(10) as u8));
            }
        }
        if rng.below(4) == 0 {
            out.push(['e', 'E'][rng.below(2)]);
            out.push_str(["", "+", "-"][rng.below(3)]);
            out.push_str(&rng.below(30).to_string());
        }
        out
    }

    fn random_string(rng: &mut Rng) -> String {
        const PARTS: [&str; 12] = [
            "abc",
            "12345678901234567890",
            "\\\"",
            "\\\\",
            "\\u00e9",
            "é",
            " ",
            "2026-11-24T10:00:00.123456Z",
            "-1.5e10",
            "{[,:]}",
            "\\n",
            "99999999999999999",
        ];
        let mut out = String::from("\"");
        for _ in 0..rng.below(5) {
            out.push_str(PARTS[rng.below(PARTS.len())]);
        }
        out.push('"');
        out
    }

    fn random_value(rng: &mut Rng, depth: usize) -> String {
        const WS: [&str; 5] = ["", " ", "\t", "\r\n", "  "];
        let ws = WS[rng.below(WS.len())];
        let pick = if depth > 3 {
            rng.below(4)
        } else {
            rng.below(6)
        };
        let body = match pick {
            0 => random_number(rng),
            1 => random_string(rng),
            2 => ["true", "false", "null"][rng.below(3)].to_string(),
            3 => random_number(rng),
            4 => {
                let items: Vec<String> = (0..rng.below(5))
                    .map(|_| random_value(rng, depth + 1))
                    .collect();
                format!("[{}]", items.join(","))
            }
            _ => {
                let items: Vec<String> = (0..rng.below(5))
                    .map(|_| format!("{}:{}", random_string(rng), random_value(rng, depth + 1)))
                    .collect();
                format!("{{{}}}", items.join(","))
            }
        };
        format!("{ws}{body}{ws}")
    }

    fn random_soup(rng: &mut Rng) -> String {
        const BITS: [&str; 14] = [
            "{", "}", "[", "]", ",", ":", "\"", "\\", " ", "x", "-", "e", ".", "tru",
        ];
        let mut out = String::new();
        for _ in 0..rng.below(12) {
            match rng.below(4) {
                0 => out.push_str(&random_number(rng)),
                1 => out.push_str(&random_string(rng)),
                _ => out.push_str(BITS[rng.below(BITS.len())]),
            }
        }
        out
    }

    fn corpus() -> Vec<String> {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut out = Vec::new();
        for _ in 0..20_000 {
            out.push(random_value(&mut rng, 0));
        }
        for _ in 0..20_000 {
            out.push(random_soup(&mut rng));
        }
        for pad in 0..40 {
            for digits in 13..20 {
                let n = "9".repeat(digits);
                let lead = " ".repeat(pad);
                out.push(format!("{lead}{n}"));
                out.push(format!("{lead}-{n}"));
                out.push(format!("{lead}[1,{n}]"));
                out.push(format!("{lead}{{\"k\":0.{n}}}"));
                out.push(format!("{lead}\"{n}\""));
                out.push(format!("{lead}{{{n}:1}}"));
                out.push(format!("{lead}[{n} x]"));
            }
        }
        out
    }

    fn reference_from_str(text: &str) -> Result<Value, serde_json::Error> {
        let value: Value = serde_json::from_str(text)?;
        let lossy = scan_literals(text.as_bytes());
        if lossy.is_empty() {
            return Ok(value);
        }
        let mut rewritten = String::with_capacity(text.len() + 2 * lossy.len());
        let mut last = 0;
        for r in &lossy {
            rewritten.push_str(&text[last..r.start]);
            rewritten.push('"');
            rewritten.push_str(&text[r.clone()]);
            rewritten.push('"');
            last = r.end;
        }
        rewritten.push_str(&text[last..]);
        serde_json::from_str(&rewritten)
    }

    #[test]
    fn the_pre_filter_never_clears_an_input_the_full_scan_flags() {
        let mut flagged = 0;
        let mut cleared = 0;
        for text in corpus() {
            let full = scan_literals(text.as_bytes());
            assert_eq!(unfaithful_literals(text.as_bytes()), full, "{text}");
            if !full.is_empty() {
                flagged += 1;
                assert!(may_hold_long_literal(text.as_bytes()), "{text}");
            }
            if !may_hold_long_literal(text.as_bytes()) {
                cleared += 1;
            }
        }
        assert!(flagged > 1_000, "{flagged}");
        assert!(cleared > 1_000, "{cleared}");
    }

    #[test]
    fn parsing_matches_the_unfiltered_scan_over_a_generated_corpus() {
        let (mut ok, mut bad) = (0, 0);
        for text in corpus() {
            let new = crate::fidelity::from_str(&text);
            let old = reference_from_str(&text);
            match (new, old) {
                (Ok(a), Ok(b)) => {
                    ok += 1;
                    assert_eq!(a, b, "{text}");
                }
                (Err(a), Err(b)) => {
                    bad += 1;
                    assert_eq!(a.to_string(), b.to_string(), "{text}");
                }
                (a, b) => panic!("{text}: {a:?} vs {b:?}"),
            }
            let lines = decode_lines(text.as_bytes()).map_err(|e| e.to_string());
            let array = decode_array(text.as_bytes()).map_err(|e| e.to_string());
            if scan_literals(text.as_bytes()).is_empty() {
                if let Err(e) = &lines {
                    assert!(!e.contains("cannot be represented"), "{text}: {e}");
                }
                if let Err(e) = &array {
                    assert!(!e.contains("cannot be represented"), "{text}: {e}");
                }
            } else if serde_json::from_str::<Value>(&text).is_ok() {
                assert!(array.is_err(), "{text}");
            }
        }
        assert!(ok > 5_000, "{ok}");
        assert!(bad > 5_000, "{bad}");
    }

    #[test]
    fn the_pre_filter_reads_runs_at_every_alignment() {
        assert!(!may_hold_long_literal(b""));
        assert!(!may_hold_long_literal(b"123456789012345"));
        assert!(may_hold_long_literal(b"1234567890123456"));
        assert!(!may_hold_long_literal(b"1234567-8901.2345e+"));
        assert!(may_hold_long_literal(b"\"id\":\"1234567890123456\""));
        let quiet = "2026-11-24T10:00:00.123456Z 550e8400-e29b-41d4-a716-446655440000 ".repeat(8);
        assert!(!may_hold_long_literal(quiet.as_bytes()));
    }

}

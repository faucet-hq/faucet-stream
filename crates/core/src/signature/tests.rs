use super::*;
use base64::Engine;
use std::collections::HashMap;

const BODY: &[u8] = br#"{"event":"created","id":7}"#;

#[test]
fn a_signed_header_value_verifies_under_the_same_settings() {
    let now = 1_700_000_000;
    for spec in [
        SignatureSpec::new("X-Sig", "k1"),
        SignatureSpec::new("X-Sig", "k1").prefix("sha256="),
        SignatureSpec::new("X-Sig", "k1")
            .algorithm(HmacAlgorithm::Sha512)
            .encoding(DigestEncoding::Base64),
        SignatureSpec::new("X-Sig", "k1").timestamp_header("X-Ts"),
    ] {
        let check = spec.check();
        let ts = check.timestamp_header.map(|_| now);
        let value = check.sign(BODY, ts).unwrap();
        let stamp = now.to_string();
        let headers = |h: &str| match h {
            "X-Sig" => Some(value.as_str()),
            "X-Ts" => Some(stamp.as_str()),
            _ => None,
        };
        assert!(check.verify(headers, BODY, now), "{spec:?}");
        assert!(!check.verify(headers, b"another body", now), "{spec:?}");
    }
}

fn mac(algorithm: HmacAlgorithm, secret: &[u8], signed: &[u8]) -> Vec<u8> {
    match algorithm {
        HmacAlgorithm::Sha256 => {
            let mut m = <hmac::Hmac<sha2::Sha256> as KeyInit>::new_from_slice(secret).unwrap();
            m.update(signed);
            m.finalize().into_bytes().to_vec()
        }
        HmacAlgorithm::Sha512 => {
            let mut m = <hmac::Hmac<sha2::Sha512> as KeyInit>::new_from_slice(secret).unwrap();
            m.update(signed);
            m.finalize().into_bytes().to_vec()
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn verify(spec: &SignatureSpec, h: &HashMap<String, String>, body: &[u8], now: u64) -> bool {
    spec.check()
        .verify(|name| h.get(name).map(String::as_str), body, now)
}

#[test]
fn every_algorithm_and_encoding_verifies_and_rejects_tampering() {
    for algorithm in [HmacAlgorithm::Sha256, HmacAlgorithm::Sha512] {
        for encoding in [DigestEncoding::Hex, DigestEncoding::Base64] {
            let spec = SignatureSpec::new("x-signature", "topsecret")
                .algorithm(algorithm)
                .encoding(encoding);
            let digest = mac(algorithm, b"topsecret", BODY);
            let value = match encoding {
                DigestEncoding::Hex => hex(&digest),
                DigestEncoding::Base64 => b64(&digest),
            };
            let h = headers(&[("x-signature", &value)]);
            assert!(verify(&spec, &h, BODY, 0), "{algorithm:?}/{encoding:?}");
            assert!(!verify(&spec, &h, b"tampered", 0));
            assert!(!verify(&spec, &HashMap::new(), BODY, 0));
        }
    }
}

#[test]
fn hex_is_case_insensitive_and_base64_is_not() {
    let digest = mac(HmacAlgorithm::Sha256, b"k", BODY);
    let spec = SignatureSpec::new("x-signature", "k");
    let upper = hex(&digest).to_ascii_uppercase();
    assert!(verify(
        &spec,
        &headers(&[("x-signature", &format!("  {upper} "))]),
        BODY,
        0
    ));
    let spec = spec.encoding(DigestEncoding::Base64);
    let swapped: String = b64(&digest)
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() {
                c.to_ascii_uppercase()
            } else {
                c.to_ascii_lowercase()
            }
        })
        .collect();
    assert!(!verify(
        &spec,
        &headers(&[("x-signature", &swapped)]),
        BODY,
        0
    ));
}

#[test]
fn a_wrong_secret_is_rejected() {
    let value = hex(&mac(HmacAlgorithm::Sha256, b"other", BODY));
    let spec = SignatureSpec::new("x-signature", "topsecret");
    assert!(!verify(
        &spec,
        &headers(&[("x-signature", &value)]),
        BODY,
        0
    ));
}

#[test]
fn the_prefix_is_required_when_configured() {
    let value = hex(&mac(HmacAlgorithm::Sha256, b"k", BODY));
    let spec = SignatureSpec::new("x-signature-256", "k").prefix("sha256=");
    let with = headers(&[("x-signature-256", &format!("sha256={value}"))]);
    assert!(verify(&spec, &with, BODY, 0));
    let without = headers(&[("x-signature-256", &value)]);
    assert!(!verify(&spec, &without, BODY, 0));
    let other = headers(&[("x-signature-256", &format!("v1={value}"))]);
    assert!(!verify(&spec, &other, BODY, 0));
}

#[test]
fn a_signed_timestamp_is_accepted_inside_the_tolerance_only() {
    let now = 1_800_000_000u64;
    let spec = SignatureSpec::new("x-signature", "k")
        .timestamp_header("x-timestamp")
        .tolerance_secs(60);
    let signed_at = |ts: u64| {
        let mut content = ts.to_string().into_bytes();
        content.push(b'.');
        content.extend_from_slice(BODY);
        headers(&[
            (
                "x-signature",
                &hex(&mac(HmacAlgorithm::Sha256, b"k", &content)),
            ),
            ("x-timestamp", &ts.to_string()),
        ])
    };
    assert!(verify(&spec, &signed_at(now), BODY, now));
    assert!(verify(&spec, &signed_at(now - 60), BODY, now));
    assert!(verify(&spec, &signed_at(now + 60), BODY, now));
    assert!(!verify(&spec, &signed_at(now - 61), BODY, now));
    assert!(!verify(&spec, &signed_at(now + 61), BODY, now));
}

#[test]
fn a_timestamp_that_was_not_signed_or_cannot_be_read_is_rejected() {
    let now = 1_800_000_000u64;
    let spec = SignatureSpec::new("x-signature", "k").timestamp_header("x-timestamp");
    let body_only = hex(&mac(HmacAlgorithm::Sha256, b"k", BODY));
    let unsigned_ts = headers(&[
        ("x-signature", &body_only),
        ("x-timestamp", &now.to_string()),
    ]);
    assert!(!verify(&spec, &unsigned_ts, BODY, now));
    let unreadable = headers(&[("x-signature", &body_only), ("x-timestamp", "soon")]);
    assert!(!verify(&spec, &unreadable, BODY, now));
    let missing = headers(&[("x-signature", &body_only)]);
    assert!(!verify(&spec, &missing, BODY, now));
}

#[test]
fn signed_request_names_the_request_inside_its_window() {
    let spec = SignatureSpec::new("x-signature", "k")
        .timestamp_header("x-timestamp")
        .tolerance_secs(300);
    let h = headers(&[("x-signature", " ABCDEF "), ("x-timestamp", " 1000 ")]);
    let got = spec
        .check()
        .signed_request(|n| h.get(n).map(String::as_str), 1200);
    assert_eq!(
        got,
        Some(SignedRequest {
            signature: "abcdef".into(),
            timestamp: 1000,
            oldest_accepted: 900,
        })
    );
    let base64 = spec.clone().encoding(DigestEncoding::Base64);
    let got = base64
        .check()
        .signed_request(|n| h.get(n).map(String::as_str), 100);
    assert_eq!(
        got,
        Some(SignedRequest {
            signature: "ABCDEF".into(),
            timestamp: 1000,
            oldest_accepted: 0,
        })
    );
    let plain = SignatureSpec::new("x-signature", "k");
    assert_eq!(
        plain
            .check()
            .signed_request(|n| h.get(n).map(String::as_str), 1200),
        None
    );
    let no_sig = headers(&[("x-timestamp", "1000")]);
    assert_eq!(
        spec.check()
            .signed_request(|n| no_sig.get(n).map(String::as_str), 1200),
        None
    );
}

#[test]
fn token_matches_compares_raw_and_bearer_forms() {
    assert!(token_matches(Some("sekret"), "sekret"));
    assert!(token_matches(Some("Bearer sekret"), "sekret"));
    assert!(!token_matches(Some("sekreT"), "sekret"));
    assert!(!token_matches(Some("Bearer sekreT"), "sekret"));
    assert!(!token_matches(Some("sekre"), "sekret"));
    assert!(!token_matches(Some("sekret-and-more"), "sekret"));
    assert!(!token_matches(Some("bearer sekret"), "sekret"));
    assert!(!token_matches(Some(""), "sekret"));
    assert!(!token_matches(None, "sekret"));
}

#[test]
fn secrets_equal_handles_equal_unequal_and_different_lengths() {
    assert!(secrets_equal(b"abc", b"abc"));
    assert!(secrets_equal(b"", b""));
    assert!(!secrets_equal(b"abc", b"abd"));
    assert!(!secrets_equal(b"abc", b"ab"));
    assert!(!secrets_equal(b"", b"a"));
}

#[test]
fn the_spec_reads_its_defaults_and_refuses_unknown_fields() {
    let spec: SignatureSpec =
        serde_json::from_value(serde_json::json!({"header": "x-signature", "secret": "k"}))
            .unwrap();
    assert_eq!(spec, SignatureSpec::new("x-signature", "k"));
    assert!(spec.is_complete());
    assert!(!SignatureSpec::new("", "k").is_complete());
    assert!(!SignatureSpec::new("x-signature", "").is_complete());
    let text = serde_json::to_value(&spec).unwrap();
    assert_eq!(
        text,
        serde_json::json!({
            "header": "x-signature",
            "secret": "k",
            "algorithm": "sha256",
            "encoding": "hex",
            "tolerance_secs": 300,
        })
    );
    assert!(
        serde_json::from_value::<SignatureSpec>(
            serde_json::json!({"header": "h", "secret": "k", "extra": 1})
        )
        .is_err()
    );
}

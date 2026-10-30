use super::*;

fn provider() -> OAuth1Provider {
    OAuth1Provider::from_config(&serde_json::json!({
        "consumer_key": "ck",
        "consumer_secret": "cs",
        "token": "tk",
        "token_secret": "ts",
        "realm": "ACCT123",
    }))
    .unwrap()
}

#[test]
fn nonces_are_random_across_providers() {
    let a = random_nonce(0);
    let b = random_nonce(0);
    assert_ne!(a, b, "two fresh providers must not share a first nonce");
    assert!(a.starts_with('0') && a.len() == 33, "{a}");
    assert!(a.chars().all(|c| c.is_ascii_alphanumeric()), "{a}");
}

#[test]
fn the_url_query_is_signed_like_the_query_map() {
    let p = provider();
    let mut q = BTreeMap::new();
    q.insert("page".to_string(), "2".to_string());
    q.insert("since".to_string(), "2024-01-01 00:00".to_string());
    let from_map = p.authorization_header("GET", "https://api.example.com/r", &q, "n", 1);
    let from_url = p.authorization_header(
        "GET",
        "https://api.example.com/r?since=2024-01-01+00%3A00&page=2#frag",
        &BTreeMap::new(),
        "n",
        1,
    );
    assert_eq!(from_map, from_url);
    let unsigned =
        p.authorization_header("GET", "https://api.example.com/r", &BTreeMap::new(), "n", 1);
    assert_ne!(
        from_url, unsigned,
        "the URL's query must change the signature"
    );
    let repeated = p.authorization_header(
        "GET",
        "https://api.example.com/r?k=a&k=b",
        &BTreeMap::new(),
        "n",
        1,
    );
    let single = p.authorization_header(
        "GET",
        "https://api.example.com/r?k=b",
        &BTreeMap::new(),
        "n",
        1,
    );
    assert_ne!(repeated, single, "every repeated key is signed");
}

#[test]
fn header_is_stable_for_fixed_nonce_and_timestamp() {
    let p = provider();
    let mut q = BTreeMap::new();
    q.insert("limit".to_string(), "10".to_string());
    let h1 = p.authorization_header(
        "GET",
        "https://api.example.com/records",
        &q,
        "nonce1",
        1700000000,
    );
    let h2 = p.authorization_header(
        "GET",
        "https://api.example.com/records",
        &q,
        "nonce1",
        1700000000,
    );
    assert_eq!(h1, h2, "same inputs → same signature");
    assert!(h1.starts_with("OAuth "));
    assert!(h1.contains("realm=\"ACCT123\""));
    assert!(h1.contains("oauth_signature_method=\"HMAC-SHA256\""));
    assert!(h1.contains("oauth_consumer_key=\"ck\""));
    assert!(h1.contains("oauth_signature=\""));
}

#[test]
fn signature_changes_with_method_and_query() {
    let p = provider();
    let q = BTreeMap::new();
    let get = p.authorization_header("GET", "https://api.example.com/x", &q, "n", 1);
    let post = p.authorization_header("POST", "https://api.example.com/x", &q, "n", 1);
    assert_ne!(get, post, "method is part of the base string");
    let mut q2 = BTreeMap::new();
    q2.insert("a".to_string(), "b".to_string());
    let with_q = p.authorization_header("GET", "https://api.example.com/x", &q2, "n", 1);
    assert_ne!(get, with_q, "query params are part of the base string");
}

#[test]
fn query_string_on_url_is_stripped_from_base_url() {
    // A url carrying `?a=b` must not double-count: the base URL excludes the
    // query, and callers pass query params via the map.
    let p = provider();
    let mut q = BTreeMap::new();
    q.insert("a".to_string(), "b".to_string());
    let with_qs = p.authorization_header("GET", "https://api.example.com/x?a=b", &q, "n", 1);
    let clean = p.authorization_header("GET", "https://api.example.com/x", &q, "n", 1);
    assert_eq!(with_qs, clean);
}

#[tokio::test]
async fn sign_request_returns_authorization_header() {
    let p = provider();
    let cred = p
        .sign_request("GET", "https://api.example.com/x", &BTreeMap::new())
        .await
        .unwrap()
        .expect("oauth1 signs the request");
    match cred {
        Credential::Header { name, value } => {
            assert_eq!(name, "Authorization");
            assert!(value.starts_with("OAuth "));
        }
        other => panic!("expected a Header credential, got {other:?}"),
    }
}

#[tokio::test]
async fn credential_errors_directing_to_sign_request() {
    assert!(provider().credential().await.is_err());
}

#[test]
fn rejects_missing_fields_and_bad_signature_method() {
    assert!(OAuth1Provider::from_config(&serde_json::json!({"consumer_key": "x"})).is_err());
    assert!(
        OAuth1Provider::from_config(&serde_json::json!({
            "consumer_key": "ck", "consumer_secret": "cs", "token": "tk", "token_secret": "ts",
            "signature_method": "HMAC-SHA1"
        }))
        .is_err()
    );
}

#[test]
fn debug_does_not_leak_secrets() {
    let p = OAuth1Provider::from_config(&serde_json::json!({
        "consumer_key": "ck",
        "consumer_secret": "CONSUMER-SECRET-VALUE",
        "token": "ACCESS-TOKEN-VALUE",
        "token_secret": "TOKEN-SECRET-VALUE",
    }))
    .unwrap();
    let s = format!("{p:?}");
    assert!(!s.contains("CONSUMER-SECRET-VALUE"), "{s}");
    assert!(!s.contains("TOKEN-SECRET-VALUE"), "{s}");
    assert!(!s.contains("ACCESS-TOKEN-VALUE"), "{s}");
    assert!(s.contains("ck"));
}

#[test]
fn base_uri_is_normalized_per_rfc_5849() {
    let p = provider();
    let q = BTreeMap::new();
    let canonical = p.authorization_header("GET", "https://api.example.com/v1/x", &q, "n", 1);
    for variant in [
        "https://API.Example.COM/v1/x",
        "HTTPS://api.example.com:443/v1/x",
        "https://api.example.com/v1/x#frag",
    ] {
        assert_eq!(
            p.authorization_header("GET", variant, &q, "n", 1),
            canonical,
            "{variant}"
        );
    }
    let other_port = p.authorization_header("GET", "https://api.example.com:8443/v1/x", &q, "n", 1);
    assert_ne!(other_port, canonical);
    let path_case = p.authorization_header("GET", "https://api.example.com/V1/x", &q, "n", 1);
    assert_ne!(path_case, canonical, "the path keeps its case");
}

#[test]
fn normalized_base_uri_keeps_non_default_port() {
    let u = reqwest::Url::parse("http://Host.Example:8080/a/b?c=d").unwrap();
    assert_eq!(
        normalized_base_uri(&u).unwrap(),
        "http://host.example:8080/a/b"
    );
    let u = reqwest::Url::parse("http://host.example:80/").unwrap();
    assert_eq!(normalized_base_uri(&u).unwrap(), "http://host.example/");
}

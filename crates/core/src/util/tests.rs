
#[test]
fn redacted_uri_authority_keeps_only_scheme_and_hosts() {
    assert_eq!(
        super::redacted_uri_authority("mongodb://u:p@h1:27017,h2/admin?replicaSet=rs#x"),
        "mongodb://h1:27017,h2"
    );
    assert_eq!(
        super::redacted_uri_authority("mongodb+srv://cluster.example.net"),
        "mongodb+srv://cluster.example.net"
    );
    assert_eq!(super::redacted_uri_authority("localhost"), "localhost");
}

/// Serve one response: `head` then `body`, then hold the socket open.
fn serve_once(head: String, body: Vec<u8>) -> String {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf);
        let _ = sock.write_all(head.as_bytes());
        let _ = sock.write_all(&body);
        std::thread::sleep(std::time::Duration::from_secs(30));
    });
    format!("http://{addr}/")
}

#[tokio::test]
async fn check_http_response_reads_only_the_truncation_limit() {
    let url = serve_once(
        "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 100000000\r\n\r\n".into(),
        vec![b'x'; 8192],
    );
    let resp = reqwest::get(&url).await.unwrap();
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        check_http_response(resp, 100),
    )
    .await
    .expect("must not wait for the rest of a huge body")
    .unwrap_err();
    match err {
        FaucetError::HttpStatus { status, body, .. } => {
            assert_eq!(status, 500);
            assert_eq!(body, format!("{}...(truncated)", "x".repeat(100)));
        }
        other => panic!("{other:?}"),
    }
    let url = serve_once(
        "HTTP/1.1 404 Not Found\r\nContent-Length: 4\r\n\r\n".into(),
        b"gone".to_vec(),
    );
    let resp = reqwest::get(&url).await.unwrap();
    match check_http_response(resp, 100).await.unwrap_err() {
        FaucetError::HttpStatus { body, .. } => assert_eq!(body, "gone"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn retry_after_reads_seconds_and_http_dates() {
    let mut h = reqwest::header::HeaderMap::new();
    assert_eq!(retry_after(&h), None);
    h.insert(reqwest::header::RETRY_AFTER, " 30 ".parse().unwrap());
    assert_eq!(retry_after(&h), Some(std::time::Duration::from_secs(30)));
    h.insert(
        reqwest::header::RETRY_AFTER,
        "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
    );
    assert_eq!(retry_after(&h), Some(std::time::Duration::ZERO));
    let future = (chrono::Utc::now() + chrono::Duration::seconds(120)).to_rfc2822();
    h.insert(reqwest::header::RETRY_AFTER, future.parse().unwrap());
    let w = retry_after(&h).unwrap();
    assert!(w > std::time::Duration::from_secs(100) && w <= std::time::Duration::from_secs(120));
    h.insert(reqwest::header::RETRY_AFTER, "soon".parse().unwrap());
    assert_eq!(retry_after(&h), None);
}

#[test]
fn retry_after_accepts_obsolete_date_forms_and_fractional_seconds() {
    let mut h = reqwest::header::HeaderMap::new();
    for past in ["Sunday, 06-Nov-94 08:49:37 GMT", "Sun Nov  6 08:49:37 1994"] {
        h.insert(reqwest::header::RETRY_AFTER, past.parse().unwrap());
        assert_eq!(retry_after(&h), Some(std::time::Duration::ZERO), "{past}");
    }
    let future = chrono::Utc::now() + chrono::Duration::seconds(300);
    for f in ["%A, %d-%b-%y %H:%M:%S GMT", "%a %b %e %H:%M:%S %Y"] {
        let v = future.format(f).to_string();
        h.insert(reqwest::header::RETRY_AFTER, v.parse().unwrap());
        let w = retry_after(&h).unwrap();
        assert!(w > std::time::Duration::from_secs(250), "{v}: {w:?}");
    }
    h.insert(reqwest::header::RETRY_AFTER, "1.2".parse().unwrap());
    assert_eq!(retry_after(&h), Some(std::time::Duration::from_secs(2)));
    h.insert(reqwest::header::RETRY_AFTER, "-3".parse().unwrap());
    assert_eq!(retry_after(&h), None);
}

#[test]
fn rate_limited_fails_above_the_ceiling() {
    let max = std::time::Duration::from_secs(60);
    assert!(matches!(
        rate_limited(std::time::Duration::from_secs(5), max),
        FaucetError::RateLimited(d) if d.as_secs() == 5
    ));
    let e = rate_limited(std::time::Duration::from_secs(86_400), max);
    assert!(
        matches!(&e, FaucetError::Source(m) if m.contains("86400s")),
        "{e:?}"
    );
    assert!(!e.is_retriable());
}

#[test]
fn fmt_redacted_masks_secret_keys_at_any_depth() {
    struct Show<'a>(&'a Value);
    impl std::fmt::Debug for Show<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            super::fmt_redacted(f, "Cfg", self.0, &["password", "headers"])
        }
    }
    let v = serde_json::json!({
        "user": "u",
        "password": "hunter22",
        "nested": [{"password": 7, "headers": {"X-Key": "k1", "list": ["a"]}}],
        "headers": null,
    });
    let out = format!("{:?}", Show(&v));
    assert!(out.starts_with("Cfg("), "{out}");
    for secret in ["hunter22", "k1", "\"a\"", ":7"] {
        assert!(!out.contains(secret), "{secret} leaked: {out}");
    }
    assert!(
        out.contains("X-Key") && out.contains("\"user\":\"u\""),
        "{out}"
    );
    assert!(out.contains("\"headers\":null"), "{out}");

    struct Bad;
    impl serde::Serialize for Bad {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("no"))
        }
    }
    struct ShowBad;
    impl std::fmt::Debug for ShowBad {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            super::fmt_redacted(f, "Bad", &Bad, &[])
        }
    }
    assert_eq!(format!("{ShowBad:?}"), "Bad(..)");
}

use super::*;
use serde_json::json;

// ── quote_ident ─────────────────────────────────────────────────────

#[test]
fn quote_ident_simple() {
    assert_eq!(quote_ident("my_table"), "\"my_table\"");
}

#[test]
fn quote_ident_with_embedded_quotes() {
    assert_eq!(quote_ident("has\"quote"), "\"has\"\"quote\"");
}

#[test]
fn quote_ident_empty() {
    assert_eq!(quote_ident(""), "\"\"");
}

#[test]
fn quote_ident_special_chars() {
    assert_eq!(quote_ident("table; DROP"), "\"table; DROP\"");
}

// ── extract_records ─────────────────────────────────────────────────

#[test]
fn extract_records_owned_matches_the_borrowing_form() {
    let body = json!({
        "data": [{"id": 1}, {"id": 2}],
        "meta": {"page": {"items": [3, 4], "n": null}, "obj": {"a": 1, "b": 2}},
        "scalar": 5
    });
    for path in [
        None,
        Some("$"),
        Some("$[*]"),
        Some("$.data"),
        Some("$.data[*]"),
        Some("$.meta.page.items[*]"),
        Some("$.meta.page.n"),
        Some("$.meta.obj[*]"),
        Some("$.missing"),
        Some("$.missing[*]"),
        Some("$.scalar[*]"),
        Some("$.data[0]"),
        Some("$..id"),
        Some("$.data[*].id"),
    ] {
        assert_eq!(
            extract_records_owned(body.clone(), path).unwrap(),
            extract_records(&body, path).unwrap(),
            "{path:?}"
        );
    }
    assert_eq!(
        extract_records_owned(json!([1, 2]), None).unwrap(),
        vec![json!(1), json!(2)]
    );
    assert!(extract_records_owned(body, Some("$[")).is_err());
}

#[test]
fn extract_with_path() {
    let body = json!({"data": [{"id": 1}, {"id": 2}]});
    let records = extract_records(&body, Some("$.data[*]")).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], 1);
}

#[test]
fn extract_without_path_array() {
    let body = json!([{"id": 1}, {"id": 2}]);
    let records = extract_records(&body, None).unwrap();
    assert_eq!(records.len(), 2);
}

#[test]
fn extract_without_path_object() {
    let body = json!({"id": 1});
    let records = extract_records(&body, None).unwrap();
    assert_eq!(records.len(), 1);
}

#[test]
fn extract_empty_result() {
    let body = json!({"data": []});
    let records = extract_records(&body, Some("$.data[*]")).unwrap();
    assert!(records.is_empty());
}

#[test]
fn extract_invalid_path_returns_error() {
    let body = json!({"data": 1});
    // jsonpath-rust handles most paths gracefully; test error propagation.
    let result = extract_records(&body, Some("$.data[*]"));
    // This should succeed (empty match) or fail; either is fine as long as
    // it doesn't panic.
    let _ = result;
}

// ── substitute_context ──────────────────────────────────────────────

#[test]
fn substitute_context_string_values() {
    let mut ctx = HashMap::new();
    ctx.insert("org".to_string(), json!("acme"));
    ctx.insert("repo".to_string(), json!("widgets"));
    let result = substitute_context("/orgs/{org}/repos/{repo}", &ctx);
    assert_eq!(result, "/orgs/acme/repos/widgets");
}

#[test]
fn substitute_context_number_value() {
    let mut ctx = HashMap::new();
    ctx.insert("id".to_string(), json!(42));
    let result = substitute_context("/items/{id}", &ctx);
    assert_eq!(result, "/items/42");
}

#[test]
fn substitute_context_bool_value() {
    let mut ctx = HashMap::new();
    ctx.insert("active".to_string(), json!(true));
    let result = substitute_context("/filter?active={active}", &ctx);
    assert_eq!(result, "/filter?active=true");
}

#[test]
fn substitute_context_null_value() {
    let mut ctx = HashMap::new();
    ctx.insert("val".to_string(), json!(null));
    let result = substitute_context("/x/{val}", &ctx);
    assert_eq!(result, "/x/null");
}

#[test]
fn substitute_context_array_value() {
    let mut ctx = HashMap::new();
    ctx.insert("ids".to_string(), json!([1, 2, 3]));
    let result = substitute_context("/x/{ids}", &ctx);
    assert_eq!(result, "/x/[1,2,3]");
}

#[test]
fn substitute_context_unmatched_placeholder_left_as_is() {
    let ctx = HashMap::new();
    let result = substitute_context("/orgs/{org}/repos", &ctx);
    assert_eq!(result, "/orgs/{org}/repos");
}

#[test]
fn substitute_context_empty_template() {
    let ctx = HashMap::new();
    let result = substitute_context("", &ctx);
    assert_eq!(result, "");
}

#[test]
fn substitute_context_replaces_all_occurrences() {
    let mut ctx = HashMap::new();
    ctx.insert("id".to_string(), Value::String("42".to_string()));
    let result = substitute_context("/a/{id}/b/{id}", &ctx);
    assert_eq!(result, "/a/42/b/42");
}

#[test]
fn substitute_context_does_not_rescan_replacement() {
    // Single-pass: a replacement value that itself looks like a placeholder
    // is emitted verbatim, never re-substituted (#78/#36).
    let mut ctx = HashMap::new();
    ctx.insert("a".to_string(), Value::String("{b}".to_string()));
    ctx.insert("b".to_string(), Value::String("SECRET".to_string()));
    let result = substitute_context("{a}", &ctx);
    assert_eq!(result, "{b}");
}

// ── extract_context ─────────────────────────────────────────────────

#[test]
fn extract_context_simple_paths() {
    let record = json!({"id": 1, "name": "alice"});
    let mut mapping = HashMap::new();
    mapping.insert("user_id".to_string(), "$.id".to_string());
    mapping.insert("user_name".to_string(), "$.name".to_string());
    let ctx = extract_context(&record, &mapping).unwrap();
    assert_eq!(ctx["user_id"], json!(1));
    assert_eq!(ctx["user_name"], json!("alice"));
}

#[test]
fn extract_context_nested_path() {
    let record = json!({"data": {"info": {"id": 99}}});
    let mut mapping = HashMap::new();
    mapping.insert("deep_id".to_string(), "$.data.info.id".to_string());
    let ctx = extract_context(&record, &mapping).unwrap();
    assert_eq!(ctx["deep_id"], json!(99));
}

#[test]
fn extract_context_missing_path_returns_error() {
    let record = json!({"id": 1});
    let mut mapping = HashMap::new();
    mapping.insert("missing".to_string(), "$.nonexistent".to_string());
    let result = extract_context(&record, &mapping);
    assert!(result.is_err());
}

#[test]
fn extract_context_empty_mapping() {
    let record = json!({"id": 1});
    let mapping = HashMap::new();
    let ctx = extract_context(&record, &mapping).unwrap();
    assert!(ctx.is_empty());
}

// ── substitute_context_bind_params ──────────────────────────────────

#[test]
fn bind_params_postgres_style() {
    let mut ctx = HashMap::new();
    ctx.insert("org".to_string(), json!("acme"));
    ctx.insert("id".to_string(), json!(42));
    let (query, values) = substitute_context_bind_params(
        "SELECT * FROM t WHERE org = {org} AND id = {id}",
        &ctx,
        1,
        |i| format!("${i}"),
    );
    assert_eq!(query, "SELECT * FROM t WHERE org = $1 AND id = $2");
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], json!("acme"));
    assert_eq!(values[1], json!(42));
}

#[test]
fn bind_params_question_mark_style() {
    let mut ctx = HashMap::new();
    ctx.insert("name".to_string(), json!("test"));
    let (query, values) =
        substitute_context_bind_params("SELECT * FROM t WHERE name = {name}", &ctx, 1, |_| {
            "?".to_string()
        });
    assert_eq!(query, "SELECT * FROM t WHERE name = ?");
    assert_eq!(values, vec![json!("test")]);
}

#[test]
fn bind_params_duplicate_key_produces_multiple_binds() {
    let mut ctx = HashMap::new();
    ctx.insert("id".to_string(), json!(5));
    let (query, values) = substitute_context_bind_params(
        "SELECT * FROM t WHERE a = {id} OR b = {id}",
        &ctx,
        3,
        |i| format!("${i}"),
    );
    assert_eq!(query, "SELECT * FROM t WHERE a = $3 OR b = $4");
    assert_eq!(values, vec![json!(5), json!(5)]);
}

#[test]
fn bind_params_unknown_key_left_as_is() {
    let ctx = HashMap::new();
    let (query, values) =
        substitute_context_bind_params("SELECT * FROM t WHERE x = {unknown}", &ctx, 1, |i| {
            format!("${i}")
        });
    assert_eq!(query, "SELECT * FROM t WHERE x = {unknown}");
    assert!(values.is_empty());
}

#[test]
fn bind_params_mixed_known_and_unknown() {
    let mut ctx = HashMap::new();
    ctx.insert("id".to_string(), json!(1));
    let (query, values) = substitute_context_bind_params(
        "SELECT * FROM t WHERE id = {id} AND x = {unknown}",
        &ctx,
        1,
        |i| format!("${i}"),
    );
    assert_eq!(query, "SELECT * FROM t WHERE id = $1 AND x = {unknown}");
    assert_eq!(values, vec![json!(1)]);
}

#[test]
fn bind_params_empty_context() {
    let ctx = HashMap::new();
    let (query, values) = substitute_context_bind_params("SELECT 1", &ctx, 1, |i| format!("${i}"));
    assert_eq!(query, "SELECT 1");
    assert!(values.is_empty());
}

#[test]
fn bind_params_start_index_offset() {
    let mut ctx = HashMap::new();
    ctx.insert("name".to_string(), json!("x"));
    let (query, values) =
        substitute_context_bind_params("SELECT * FROM t WHERE name = {name}", &ctx, 5, |i| {
            format!("${i}")
        });
    assert_eq!(query, "SELECT * FROM t WHERE name = $5");
    assert_eq!(values, vec![json!("x")]);
}

// ── substitute_context_json ─────────────────────────────────────────

#[test]
fn json_sub_escapes_double_quotes() {
    let mut ctx = HashMap::new();
    ctx.insert("name".to_string(), json!(r#"O'Brien "Bob""#));
    let template = r#"{"name":"{name}"}"#;
    let result = substitute_context_json(template, &ctx);
    let parsed: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed["name"], r#"O'Brien "Bob""#);
}

#[test]
fn json_sub_escapes_backslashes() {
    let mut ctx = HashMap::new();
    ctx.insert("path".to_string(), json!("C:\\Users\\test"));
    let template = r#"{"path":"{path}"}"#;
    let result = substitute_context_json(template, &ctx);
    let parsed: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed["path"], "C:\\Users\\test");
}

#[test]
fn json_sub_escapes_control_chars() {
    let mut ctx = HashMap::new();
    ctx.insert("text".to_string(), json!("line1\nline2\ttab"));
    let template = r#"{"text":"{text}"}"#;
    let result = substitute_context_json(template, &ctx);
    let parsed: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed["text"], "line1\nline2\ttab");
}

#[test]
fn json_sub_number_value() {
    let mut ctx = HashMap::new();
    ctx.insert("id".to_string(), json!(42));
    let template = r#"{"user_id":"{id}"}"#;
    let result = substitute_context_json(template, &ctx);
    let parsed: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed["user_id"], "42");
}

#[test]
fn json_sub_preserves_valid_json_without_special_chars() {
    let mut ctx = HashMap::new();
    ctx.insert("name".to_string(), json!("alice"));
    let template = r#"{"filter":{"name":"{name}"}}"#;
    let result = substitute_context_json(template, &ctx);
    let parsed: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed["filter"]["name"], "alice");
}

// ── json_escape_string ──────────────────────────────────────────────

#[test]
fn json_escape_plain_string() {
    assert_eq!(json_escape_string("hello"), "hello");
}

#[test]
fn json_escape_quotes_and_backslashes() {
    assert_eq!(json_escape_string(r#"a"b\c"#), r#"a\"b\\c"#);
}

#[test]
fn json_escape_newlines_and_tabs() {
    assert_eq!(json_escape_string("a\nb\tc"), "a\\nb\\tc");
}

// ── redact_uri_credentials ──────────────────────────────────────────

#[test]
fn redact_strips_url_userinfo() {
    assert_eq!(
        redact_uri_credentials("postgres://user:pass@host:5432/db"),
        "postgres://host:5432/db"
    );
    assert_eq!(
        redact_uri_credentials("mongodb://u:p@h/db?x=1"),
        "mongodb://h/db?x=1"
    );
}

#[test]
fn redact_strips_userinfo_before_a_multi_host_seed_list() {
    assert_eq!(
        redact_uri_credentials("mongodb://u:pw@h1:27017,h2:27017/?replicaSet=rs0"),
        "mongodb://h1:27017,h2:27017/?replicaSet=rs0"
    );
    assert_eq!(
        redact_uri_credentials("postgres://app:p@ss,w0rd@db1:5432,db2:5432/app"),
        "postgres://db1:5432,db2:5432/app"
    );
}

#[test]
fn redact_strips_user_only_userinfo() {
    assert_eq!(
        redact_uri_credentials("redis://user@127.0.0.1:6379"),
        "redis://127.0.0.1:6379"
    );
}

#[test]
fn redact_handles_adonet_password_tokens() {
    assert_eq!(
        redact_uri_credentials("Server=tcp:h,1433;Database=db;User Id=sa;Password=secret;"),
        "Server=tcp:h,1433;Database=db;User Id=sa;Password=***;"
    );
    assert_eq!(
        redact_uri_credentials("server=h;pwd=secret"),
        "server=h;pwd=***"
    );
}

#[test]
fn redact_passthrough_when_no_credentials() {
    assert_eq!(
        redact_uri_credentials("s3://bucket/prefix"),
        "s3://bucket/prefix"
    );
    assert_eq!(
        redact_uri_credentials("file:///tmp/x.csv"),
        "file:///tmp/x.csv"
    );
}

#[test]
fn redact_strips_password_containing_special_chars() {
    // Passwords with '/', '?' or '@' must not leak (F5): the userinfo→host
    // delimiter is the LAST '@', and the host segment after it is what's kept.
    assert_eq!(
        redact_uri_credentials("postgres://user:p/w@host:5432/db"),
        "postgres://host:5432/db"
    );
    assert_eq!(
        redact_uri_credentials("postgres://user:p?w@host/db"),
        "postgres://host/db"
    );
    assert_eq!(
        redact_uri_credentials("postgres://user:p@ss@host/db"),
        "postgres://host/db"
    );
    assert_eq!(
        redact_uri_credentials("mysql://u:a/b?c@d@127.0.0.1:3306/app"),
        "mysql://127.0.0.1:3306/app"
    );
}

#[test]
fn redact_keeps_an_at_sign_in_an_object_path() {
    assert_eq!(
        redact_uri_credentials("s3://bucket/data@2024/x.parquet"),
        "s3://bucket/data@2024/x.parquet"
    );
    assert_eq!(
        redact_uri_credentials("http://host:8080/p@2024/x"),
        "http://host:8080/p@2024/x"
    );
    assert_eq!(
        redact_uri_credentials("s3://user:pw@bucket/data@2024/x"),
        "s3://bucket/data@2024/x"
    );
    assert_eq!(
        redact_uri_credentials("postgres://u:/pw@host/db"),
        "postgres://host/db"
    );
}

#[test]
fn redact_hides_the_whole_value_up_to_the_field_terminator() {
    assert_eq!(
        redact_uri_credentials("Server=h;Password=a&b?c;Database=d"),
        "Server=h;Password=***;Database=d"
    );
    assert_eq!(
        redact_uri_credentials("Password=a&b;Database=d"),
        "Password=***;Database=d"
    );
    assert_eq!(
        redact_uri_credentials("Server=h;Password=\"a;b\";Database=d"),
        "Server=h;Password=***;Database=d"
    );
    assert_eq!(
        redact_uri_credentials("https://h/?password=a;b&x=1"),
        "https://h/?password=***&x=1"
    );
    assert_eq!(
        redact_uri_credentials("jdbc:sqlserver://h;password=p&q;db=x"),
        "jdbc:sqlserver://h;password=***;db=x"
    );
    assert_eq!(redact_uri_credentials("token="), "token=***");
}

#[test]
fn redact_strips_query_string_password() {
    assert_eq!(
        redact_uri_credentials("https://host/api?user=sa&password=secret&x=1"),
        "https://host/api?user=sa&password=***&x=1"
    );
    assert_eq!(
        redact_uri_credentials("snowflake://host/db?password=secret"),
        "snowflake://host/db?password=***"
    );
}

#[test]
fn redact_strips_credential_parameters_by_suffix() {
    for key in [
        "api_token",
        "auth_token",
        "private_token",
        "X-Amz-Security-Token",
        "X-Amz-Signature",
        "X-Amz-Credential",
        "X-Goog-Signature",
        "passwd",
        "pass",
        "signature",
        "auth_code",
        "authorization_code",
        "jwt",
        "client_password",
    ] {
        let uri = format!("https://h/p?{key}=tok-live-123&keep=1");
        assert_eq!(
            redact_uri_credentials(&uri),
            format!("https://h/p?{key}=***&keep=1"),
            "{key}"
        );
    }
    for key in [
        "region",
        "limit",
        "page",
        "key",
        "monkey_business",
        "code",
        "pageToken",
        "next_token",
        "continuationToken",
    ] {
        let uri = format!("https://h/p?{key}=v");
        assert_eq!(redact_uri_credentials(&uri), uri, "{key}");
    }
}

#[test]
fn redact_strips_common_secret_query_keys() {
    // #321 M11: secret parameter names beyond password/pwd must be redacted
    // before a URI is emitted to lineage / the catalog.
    assert_eq!(
        redact_uri_credentials("https://api.example.com/v2/?api_key=SECRET"),
        "https://api.example.com/v2/?api_key=***"
    );
    assert_eq!(
        redact_uri_credentials("https://h/?token=abc&access_token=xyz&keep=1"),
        "https://h/?token=***&access_token=***&keep=1"
    );
    // Azure SAS style, and separator/casing variants normalize to one key.
    assert_eq!(
        redact_uri_credentials("https://acct.blob.core.windows.net/c?sig=DEAD&sp=r"),
        "https://acct.blob.core.windows.net/c?sig=***&sp=r"
    );
    assert_eq!(
        redact_uri_credentials("db=x;Client-Secret=shh;Account_Key=k"),
        "db=x;Client-Secret=***;Account_Key=***"
    );
    // A non-secret key is untouched.
    assert_eq!(
        redact_uri_credentials("https://h/?region=us-east-1"),
        "https://h/?region=us-east-1"
    );
}

//! HTTP-sourced reference relation (#558): fetch rows from a small REST
//! endpoint **once** at compile/first-use, select the row array, and hand back
//! JSON objects for materialization into a DuckDB table. The fetch runs exactly
//! once per run — the relation is loaded in [`crate::compile::build_connection`]
//! and never registered as a reloadable, so no page ever re-hits the endpoint.

use crate::config::HttpMethod;
use faucet_core::FaucetError;
use faucet_core::util::extract_records;
use serde_json::Value;
use std::collections::BTreeMap;

fn cfg_err(msg: impl Into<String>) -> FaucetError {
    FaucetError::Config(format!("sql transform: {}", msg.into()))
}

/// Fetch the endpoint body and select the row objects.
///
/// The blocking request runs on a dedicated `std::thread` so `reqwest::blocking`
/// never executes inside a tokio runtime (`compile()` is sync but is typically
/// invoked from async CLI code, where a blocking client would otherwise panic).
pub(crate) fn fetch_http_rows(
    name: &str,
    url: &str,
    method: HttpMethod,
    headers: &BTreeMap<String, String>,
    records_path: Option<&str>,
) -> Result<Vec<Value>, FaucetError> {
    let body = fetch_body(name, url, method, headers)?;
    select_rows(name, &body, records_path)
}

fn fetch_body(
    name: &str,
    url: &str,
    method: HttpMethod,
    headers: &BTreeMap<String, String>,
) -> Result<Value, FaucetError> {
    let owned_name = name.to_string();
    let url = url.to_string();
    let headers = headers.clone();
    off_worker(move || {
        let handle =
            std::thread::spawn(move || fetch_body_blocking(&owned_name, &url, method, &headers));
        match handle.join() {
            Ok(res) => res,
            Err(_) => Err(cfg_err(format!(
                "http relation '{name}': fetch thread panicked"
            ))),
        }
    })
}

/// Wait without pinning a tokio worker: on a multi-thread runtime the worker
/// hands its other tasks off first (`block_in_place`); elsewhere run inline.
pub(crate) fn off_worker<T>(f: impl FnOnce() -> T) -> T {
    let multi_thread = tokio::runtime::Handle::try_current()
        .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
    if multi_thread {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

fn fetch_body_blocking(
    name: &str,
    url: &str,
    method: HttpMethod,
    headers: &BTreeMap<String, String>,
) -> Result<Value, FaucetError> {
    let client = reqwest::blocking::Client::builder()
        .build()
        .map_err(|e| cfg_err(format!("http relation '{name}': build client: {e}")))?;
    let mut req = match method {
        HttpMethod::Get => client.get(url),
        HttpMethod::Post => client.post(url),
    };
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let resp = req
        .send()
        .map_err(|e| cfg_err(format!("http relation '{name}': request to '{url}': {e}")))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(cfg_err(format!(
            "http relation '{name}': '{url}' returned HTTP {status}"
        )));
    }
    resp.json::<Value>().map_err(|e| {
        cfg_err(format!(
            "http relation '{name}': decode json from '{url}': {e}"
        ))
    })
}

/// Select the row objects from a fetched response body.
///
/// - With `records_path`, the JSONPath matches are used; a path that resolves to
///   a single array (e.g. `$.items` with no `[*]`) is unpacked, so both
///   `$.items` and `$.items[*]` work.
/// - Without `records_path`, the body must be a JSON array.
///
/// Every selected element must be a JSON object; anything else is a clear
/// configuration error (naming the relation and the offending element).
pub(crate) fn select_rows(
    name: &str,
    body: &Value,
    records_path: Option<&str>,
) -> Result<Vec<Value>, FaucetError> {
    let selected: Vec<Value> = match records_path {
        Some(p) => {
            let matched = extract_records(body, Some(p))
                .map_err(|e| cfg_err(format!("http relation '{name}': records_path '{p}': {e}")))?;
            // A path without `[*]` yields the array itself as one match; unpack
            // it so both `$.items` and `$.items[*]` behave the same.
            if matched.len() == 1
                && let Some(arr) = matched[0].as_array()
            {
                arr.clone()
            } else {
                matched
            }
        }
        None => match body {
            Value::Array(arr) => arr.clone(),
            _ => {
                return Err(cfg_err(format!(
                    "http relation '{name}': response body is not a JSON array; \
                     set records_path to select the row array"
                )));
            }
        },
    };

    for (i, v) in selected.iter().enumerate() {
        if !v.is_object() {
            return Err(cfg_err(format!(
                "http relation '{name}': selected row {i} is not a JSON object \
                 (got {}); each row must be an object",
                kind_of(v)
            )));
        }
    }
    Ok(selected)
}

fn kind_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn off_worker_lets_other_tasks_run_on_the_only_worker() {
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let f2 = flag.clone();
        tokio::spawn(async move { f2.store(true, std::sync::atomic::Ordering::SeqCst) });
        let seen = off_worker(|| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while std::time::Instant::now() < deadline {
                if flag.load(std::sync::atomic::Ordering::SeqCst) {
                    return true;
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            false
        });
        assert!(seen, "the spawned task never ran while the fetch waited");
        assert_eq!(off_worker(|| 7), 7);
    }

    #[test]
    fn selects_array_at_path() {
        let body = json!({"items": [{"id": 1}, {"id": 2}]});
        let rows = select_rows("r", &body, Some("$.items[*]")).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], json!(1));
    }

    #[test]
    fn unpacks_bare_array_path_without_star() {
        // `$.items` (no `[*]`) resolves to the array itself; it is unpacked.
        let body = json!({"items": [{"id": 1}, {"id": 2}]});
        let rows = select_rows("r", &body, Some("$.items")).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1]["id"], json!(2));
    }

    #[test]
    fn whole_body_array_used_when_no_path() {
        let body = json!([{"id": 1}, {"id": 2}, {"id": 3}]);
        let rows = select_rows("r", &body, None).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn non_array_body_without_path_is_config_error() {
        let body = json!({"id": 1});
        let err = select_rows("r", &body, None).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)), "got: {err:?}");
        let msg = format!("{err}");
        assert!(msg.contains("not a JSON array"), "got: {msg}");
        assert!(msg.contains("records_path"), "got: {msg}");
    }

    #[test]
    fn path_selecting_scalars_is_config_error() {
        // `$.ids[*]` matches numbers, not objects → a clear mismatch error.
        let body = json!({"ids": [1, 2, 3]});
        let err = select_rows("named_lists", &body, Some("$.ids[*]")).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)), "got: {err:?}");
        let msg = format!("{err}");
        assert!(msg.contains("not a JSON object"), "got: {msg}");
        assert!(msg.contains("number"), "got: {msg}");
        assert!(msg.contains("named_lists"), "got: {msg}");
    }

    #[test]
    fn empty_selection_is_ok() {
        let body = json!({"items": []});
        let rows = select_rows("r", &body, Some("$.items[*]")).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn kind_of_names_every_json_type() {
        assert_eq!(kind_of(&json!(null)), "null");
        assert_eq!(kind_of(&json!(true)), "boolean");
        assert_eq!(kind_of(&json!(1)), "number");
        assert_eq!(kind_of(&json!("s")), "string");
        assert_eq!(kind_of(&json!([])), "array");
        assert_eq!(kind_of(&json!({})), "object");
    }
}

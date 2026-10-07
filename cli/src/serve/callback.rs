//! Per-run completion callbacks (#481).
//!
//! A caller that submits a run over HTTP usually owns a job record and wants to
//! be told when the run finishes, at an endpoint that varies per job. The
//! config-declared `notifications:` block cannot express that — it is static per
//! pipeline — so the callback destination rides the *submission* instead
//! (`POST /v1/runs`, `POST /v1/templates/{id}/runs`) and is stored on the run
//! record.
//!
//! ## Delivery guarantee: at-most-once
//!
//! The callback fires from the in-process terminal transitions — [`fire`] is
//! called by `runner::finalize`, by `runner::maybe_finalize_parent` (the sharded
//! parent), and by the pending-cancel handler. It is **not** fired by the
//! recovery sweeps that live inside the history backend (lease-expiry orphan
//! recovery, reclaim-poison, the sharded-parent completion sweep), because those
//! run inside the SQL layer and never see this dispatcher.
//!
//! So: if the instance owning a run dies and the run is later failed by lease
//! recovery, **no callback is delivered**. A caller must therefore treat a
//! missing callback as "unknown", not as "still running", and reconcile against
//! `GET /v1/runs/{id}`, which is always authoritative. This is stated in the
//! HTTP API reference too — it is the one thing that will hang an integration
//! that assumes otherwise.
//!
//! ## Egress
//!
//! [`CallbackSpec::validate`] restricts the scheme to http/https and refuses
//! link-local / cloud-metadata targets, which is the concrete SSRF risk called
//! out in the serve cookbook. `--callback-allow-host` narrows it further to an
//! explicit allowlist. Note the broader posture is unchanged: a caller who can
//! submit a run can already point a `rest` source anywhere, so this guard closes
//! the metadata hole rather than pretending to be a general egress control.

use crate::serve::history::{RunRecord, RunStatus};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

/// Per-attempt HTTP timeout for a callback POST.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
/// Total attempts (1 initial + retries) before giving up.
const MAX_ATTEMPTS: u32 = 3;
/// Base backoff between attempts.
const RETRY_BASE: Duration = Duration::from_millis(250);

/// Top-level keys the callback body always carries. `extra_fields` may not
/// shadow any of them — the same fail-fast rule the notify webhook uses, for the
/// same reason: a typo'd `status` key would otherwise let a submission spoof the
/// very signal the receiver keys off.
pub const RESERVED_BODY_KEYS: &[&str] = &[
    "event",
    "run_id",
    "status",
    "name",
    "labels",
    "submitted_at",
    "started_at",
    "finished_at",
    "elapsed_secs",
    "records_written",
    "error",
    "attempt",
];

/// A caller-supplied completion callback, carried on the run record.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CallbackSpec {
    /// Destination URL. `http`/`https` only.
    pub url: String,
    /// HTTP method — defaults to `POST`.
    #[serde(default = "default_method")]
    pub method: String,
    /// Extra headers sent with the request.
    ///
    /// These are **credentials** in practice, so a clustered server refuses them
    /// (see [`CallbackSpec::reject_secrets_in_cluster`]): a clustered submit
    /// persists the run record — including this map — into the shared history
    /// database for a peer to execute, which would store the value in clear
    /// text.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// Static fields merged into the callback body (e.g. the caller's own job
    /// id). A key colliding with a faucet-emitted field is rejected at submit
    /// time — see [`RESERVED_BODY_KEYS`].
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra_fields: BTreeMap<String, Value>,
    /// Terminal statuses to fire on. Empty (the default) means **all** of them,
    /// which is what a job-status callback wants — a caller that only subscribes
    /// to success will hang on a cancelled run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on: Vec<RunStatus>,
}

fn default_method() -> String {
    "POST".to_string()
}

impl CallbackSpec {
    /// Structural + egress validation, run at submit time so a bad callback is a
    /// `422` on the submission rather than a silent no-op an hour later when the
    /// run finishes.
    pub fn validate(&self, allow_hosts: &[String]) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err("callback.url must be non-empty".into());
        }
        if self.method.trim().is_empty() {
            return Err("callback.method must be non-empty".into());
        }
        if reqwest::Method::from_bytes(self.method.as_bytes()).is_err() {
            return Err(format!("callback.method `{}` is not valid", self.method));
        }
        for key in self.extra_fields.keys() {
            if RESERVED_BODY_KEYS.contains(&key.as_str()) {
                return Err(format!(
                    "callback.extra_fields.{key} collides with a field faucet emits — \
                     reserved keys are: {}",
                    RESERVED_BODY_KEYS.join(", ")
                ));
            }
        }
        for st in &self.on {
            if !st.is_terminal() {
                return Err(format!(
                    "callback.on contains non-terminal status `{}` — a callback \
                     only fires on a terminal state (completed, failed, cancelled)",
                    st.as_str()
                ));
            }
        }

        let url = reqwest::Url::parse(&self.url)
            .map_err(|e| format!("callback.url is not a valid URL: {e}"))?;
        match url.scheme() {
            "http" | "https" => {}
            other => {
                return Err(format!(
                    "callback.url scheme `{other}` is not allowed (http or https only)"
                ));
            }
        }
        let host = url
            .host_str()
            .ok_or_else(|| "callback.url has no host".to_string())?;

        if !allow_hosts.is_empty() {
            if !allow_hosts.iter().any(|h| h == host) {
                return Err(format!(
                    "callback.url host `{host}` is not in the server's callback allowlist \
                     ({}) — set --callback-allow-host to permit it",
                    allow_hosts.join(", ")
                ));
            }
            // An explicitly allowlisted host is trusted, metadata range or not.
            return Ok(());
        }

        if is_link_local(host) {
            return Err(format!(
                "callback.url host `{host}` is a link-local / cloud-metadata address, \
                 which the server refuses to call. Add it to --callback-allow-host if \
                 this is genuinely intended"
            ));
        }
        Ok(())
    }

    /// A clustered submit persists the run record for a peer to execute, so
    /// caller-supplied credentials would land in the shared history database in
    /// clear text. Mirrors the guard the template registry already applies to
    /// secret params.
    pub fn reject_secrets_in_cluster(&self, clustered: bool) -> Result<(), String> {
        if clustered && !self.headers.is_empty() {
            return Err(
                "this callback carries `headers`, and a clustered server persists the run \
                 record so a peer can execute it — which would store those values in the \
                 shared run-history database in clear text. Authenticate the callback \
                 without a request header (e.g. a capability token embedded in a \
                 single-use URL path), or submit to a non-clustered server"
                    .into(),
            );
        }
        Ok(())
    }

    /// Whether this spec subscribes to `status`.
    pub fn fires_on(&self, status: RunStatus) -> bool {
        status.is_terminal() && (self.on.is_empty() || self.on.contains(&status))
    }
}

/// The server's `--callback-allow-host` list, consulted again at delivery.
static ALLOW_HOSTS: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();

/// Record the server's callback allowlist (once, at startup).
pub fn set_allow_hosts(hosts: Vec<String>) {
    let _ = ALLOW_HOSTS.set(hosts);
}

fn allowlisted(host: &str) -> bool {
    ALLOW_HOSTS
        .get()
        .is_some_and(|hosts| hosts.iter().any(|h| h == host))
}

/// Whether an address is link-local (IPv4 `169.254.0.0/16`, IPv6
/// `fe80::/10`) — the range cloud instance-metadata services live on —
/// including an IPv4-mapped IPv6 form of one.
fn is_link_local_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => v4.is_link_local(),
        std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.is_link_local(),
            // `Ipv6Addr::is_unicast_link_local` is unstable; check fe80::/10.
            None => (v6.segments()[0] & 0xffc0) == 0xfe80,
        },
    }
}

/// Whether `host` is a link-local address or a well-known metadata name.
fn is_link_local(host: &str) -> bool {
    // Strip an IPv6 literal's brackets if the caller passed them.
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    match bare.parse::<std::net::IpAddr>() {
        Ok(ip) => is_link_local_ip(ip),
        // Not an IP literal. The well-known metadata hostnames resolve into the
        // link-local range, so refuse them by name too rather than relying on
        // resolution at request time.
        Err(_) => matches!(
            bare,
            "metadata" | "metadata.google.internal" | "metadata.goog" | "instance-data"
        ),
    }
}

/// The body delivered to the callback URL.
fn payload(rec: &RunRecord, spec: &CallbackSpec) -> Value {
    let mut body = serde_json::json!({
        "event": format!("run.{}", rec.status.as_str()),
        "run_id": rec.run_id,
        "status": rec.status.as_str(),
        "name": rec.name.clone().map_or(Value::Null, Value::String),
        "labels": rec.labels.iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect::<serde_json::Map<String, Value>>(),
        "submitted_at": rec.submitted_at.to_rfc3339(),
        "started_at": rec.started_at.map_or(Value::Null, |t| Value::String(t.to_rfc3339())),
        "finished_at": rec.finished_at.map_or(Value::Null, |t| Value::String(t.to_rfc3339())),
        "elapsed_secs": rec.elapsed_secs
            .and_then(serde_json::Number::from_f64)
            .map_or(Value::Null, Value::Number),
        "records_written": rec.records_written,
        // Redacted: an error string frequently embeds the material that produced
        // it (a request URL with an API key in a query param), and a callback
        // leaves the trust boundary entirely.
        "error": rec.error.as_deref()
            .map(|e| Value::String(crate::secrets::registry::redact(e).into_owned()))
            .unwrap_or(Value::Null),
        "attempt": rec.attempt,
    });
    if let Some(map) = body.as_object_mut() {
        for (k, v) in &spec.extra_fields {
            map.insert(k.clone(), v.clone());
        }
    }
    body
}

/// Deliver the completion callback for `rec`, if it has one that subscribes to
/// its terminal status. Never propagates an error: a callback is best-effort and
/// must not affect the recorded outcome of a run that already finished.
pub async fn fire(rec: &RunRecord) {
    let Some(spec) = rec.callback.as_ref() else {
        return;
    };
    if !spec.fires_on(rec.status) {
        return;
    }
    let body = payload(rec, spec);
    match deliver(spec, &body).await {
        Ok(()) => tracing::debug!(run_id = %rec.run_id, "callback delivered"),
        Err(e) => tracing::warn!(
            run_id = %rec.run_id,
            // The URL can itself embed a token, so redact before logging.
            url = %crate::secrets::registry::redact(&spec.url),
            error = %e,
            "callback delivery failed; the run outcome is unaffected \
             (reconcile via GET /v1/runs/<id>)"
        ),
    }
}

/// Resolve the callback host and refuse it when any address is link-local
/// (#789 SERVE-41): a DNS name or a redirector can point anywhere, so the
/// submit-time check of the host text is not enough. Returns the addresses
/// to pin the connection to, so a second lookup cannot answer differently.
async fn checked_addrs(url: &reqwest::Url) -> Result<Vec<std::net::SocketAddr>, String> {
    let host = url.host_str().ok_or("callback.url has no host")?;
    let port = url.port_or_known_default().unwrap_or(443);
    if allowlisted(host) {
        return Ok(Vec::new());
    }
    if is_link_local(host) {
        return Err(format!("callback host `{host}` is link-local; refused"));
    }
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    if bare.parse::<std::net::IpAddr>().is_ok() {
        return Ok(Vec::new());
    }
    let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host((bare, port))
        .await
        .map_err(|e| format!("resolving callback host `{host}`: {e}"))?
        .collect();
    if let Some(bad) = addrs.iter().find(|a| is_link_local_ip(a.ip())) {
        return Err(format!(
            "callback host `{host}` resolves to the link-local address {}; refused",
            bad.ip()
        ));
    }
    Ok(addrs)
}

/// One bounded, retried delivery attempt sequence.
async fn deliver(spec: &CallbackSpec, body: &Value) -> Result<(), String> {
    let url =
        reqwest::Url::parse(&spec.url).map_err(|e| format!("callback.url is not valid: {e}"))?;
    let pinned = checked_addrs(&url).await?;
    let mut builder = reqwest::Client::builder()
        .timeout(ATTEMPT_TIMEOUT)
        // A redirect would leave the checked host.
        .redirect(reqwest::redirect::Policy::none());
    if let Some(host) = url.host_str()
        && !pinned.is_empty()
    {
        builder = builder.resolve_to_addrs(host, &pinned);
    }
    let client = builder
        .build()
        .map_err(|e| format!("building callback client: {e}"))?;
    let method = reqwest::Method::from_bytes(spec.method.as_bytes())
        .map_err(|_| format!("invalid method `{}`", spec.method))?;

    let mut last = String::new();
    for attempt in 1..=MAX_ATTEMPTS {
        let mut req = client
            .request(method.clone(), &spec.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(body);
        for (k, v) in &spec.headers {
            req = req.header(k, v);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                let status = resp.status();
                last = format!("HTTP {status}");
                // 4xx other than 408/429 will not succeed on retry.
                if status.is_client_error()
                    && status != reqwest::StatusCode::REQUEST_TIMEOUT
                    && status != reqwest::StatusCode::TOO_MANY_REQUESTS
                {
                    return Err(last);
                }
            }
            Err(e) => last = format!("request failed: {e}"),
        }
        if attempt < MAX_ATTEMPTS {
            tokio::time::sleep(RETRY_BASE * 2u32.pow(attempt - 1)).await;
        }
    }
    Err(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(url: &str) -> CallbackSpec {
        CallbackSpec {
            url: url.into(),
            method: "POST".into(),
            headers: BTreeMap::new(),
            extra_fields: BTreeMap::new(),
            on: Vec::new(),
        }
    }

    #[test]
    fn mapped_ipv6_metadata_addresses_are_refused() {
        for u in [
            "http://[::ffff:169.254.169.254]/latest",
            "http://[::ffff:a9fe:a9fe]/x",
            "http://[fe80::1]/x",
        ] {
            let err = spec(u).validate(&[]).expect_err(u);
            assert!(err.contains("link-local"), "{err}");
        }
        assert!(!is_link_local("[::ffff:10.0.0.1]"));
    }

    #[tokio::test]
    async fn delivery_resolves_and_checks_every_address() {
        let ok = checked_addrs(&reqwest::Url::parse("http://localhost:9/x").unwrap())
            .await
            .unwrap();
        assert!(!ok.is_empty());
        assert!(ok.iter().all(|a| a.port() == 9));
        let literal = checked_addrs(&reqwest::Url::parse("http://127.0.0.1:9/").unwrap())
            .await
            .unwrap();
        assert!(literal.is_empty(), "an IP literal needs no pinning");
        let err = checked_addrs(&reqwest::Url::parse("http://169.254.169.254/").unwrap())
            .await
            .unwrap_err();
        assert!(err.contains("link-local"), "{err}");
        let err = checked_addrs(&reqwest::Url::parse("http://no-such-host.invalid/").unwrap())
            .await
            .unwrap_err();
        assert!(err.contains("resolving"), "{err}");
    }

    #[test]
    fn accepts_a_plain_https_url() {
        assert!(spec("https://caller.example/hook").validate(&[]).is_ok());
    }

    #[test]
    fn rejects_non_http_schemes() {
        for u in ["file:///etc/passwd", "gopher://x/", "ftp://x/"] {
            let err = spec(u).validate(&[]).expect_err("scheme must be refused");
            assert!(err.contains("scheme"), "{err}");
        }
    }

    #[test]
    fn rejects_link_local_and_metadata_targets() {
        // The concrete SSRF risk documented for serve: cloud instance metadata.
        for u in [
            "http://169.254.169.254/latest/meta-data/",
            "http://metadata.google.internal/computeMetadata/v1/",
            "http://[fe80::1]/",
        ] {
            let err = spec(u).validate(&[]).expect_err("must be refused");
            assert!(err.contains("link-local"), "{err}");
        }
    }

    #[test]
    fn loopback_is_allowed_without_an_allowlist() {
        // Deliberate: the guard closes the metadata hole, it is not a general
        // egress control, and a local receiver is a legitimate deployment.
        assert!(spec("http://127.0.0.1:8080/cb").validate(&[]).is_ok());
    }

    #[test]
    fn allowlist_restricts_to_named_hosts() {
        let allow = vec!["caller.example".to_string()];
        assert!(spec("https://caller.example/hook").validate(&allow).is_ok());
        let err = spec("https://elsewhere.example/hook")
            .validate(&allow)
            .expect_err("must be refused");
        assert!(err.contains("allowlist"), "{err}");
    }

    #[test]
    fn allowlist_overrides_the_link_local_refusal() {
        let allow = vec!["169.254.169.254".to_string()];
        assert!(
            spec("http://169.254.169.254/x").validate(&allow).is_ok(),
            "an explicitly allowlisted host is trusted"
        );
    }

    #[test]
    fn rejects_reserved_extra_field_keys() {
        for key in RESERVED_BODY_KEYS {
            let mut s = spec("https://x.example/h");
            s.extra_fields
                .insert((*key).to_string(), Value::String("x".into()));
            let err = s.validate(&[]).expect_err("reserved key must be refused");
            assert!(err.contains(key), "{err}");
        }
    }

    #[test]
    fn rejects_a_non_terminal_on_filter() {
        let mut s = spec("https://x.example/h");
        s.on = vec![RunStatus::Running];
        let err = s.validate(&[]).expect_err("must be refused");
        assert!(err.contains("non-terminal"), "{err}");
    }

    #[test]
    fn rejects_bad_method_and_empty_url() {
        let mut s = spec("https://x.example/h");
        s.method = "NOT A METHOD".into();
        assert!(s.validate(&[]).is_err());
        assert!(spec("   ").validate(&[]).is_err());
    }

    #[test]
    fn cluster_guard_refuses_caller_supplied_headers() {
        let mut s = spec("https://x.example/h");
        s.headers
            .insert("Authorization".into(), "Bearer t".to_string());
        // Non-clustered: fine, nothing is persisted for a peer.
        assert!(s.reject_secrets_in_cluster(false).is_ok());
        let err = s
            .reject_secrets_in_cluster(true)
            .expect_err("clustered must refuse");
        assert!(err.contains("shared run-history"), "{err}");
        // Without headers a clustered submit is fine.
        assert!(
            spec("https://x.example/h")
                .reject_secrets_in_cluster(true)
                .is_ok()
        );
    }

    #[test]
    fn fires_on_respects_the_filter_and_terminality() {
        let mut s = spec("https://x.example/h");
        // Empty filter = every terminal status.
        assert!(s.fires_on(RunStatus::Completed));
        assert!(s.fires_on(RunStatus::Failed));
        assert!(s.fires_on(RunStatus::Cancelled));
        assert!(!s.fires_on(RunStatus::Running));
        assert!(!s.fires_on(RunStatus::Queued));

        s.on = vec![RunStatus::Failed];
        assert!(s.fires_on(RunStatus::Failed));
        assert!(!s.fires_on(RunStatus::Completed));
    }

    #[test]
    fn payload_carries_run_identity_and_merges_extra_fields() {
        let mut rec = RunRecord::queued(
            "run-7".into(),
            Some("orders".into()),
            BTreeMap::from([("env".to_string(), "prod".to_string())]),
            None,
            chrono::Utc::now(),
        );
        rec.status = RunStatus::Completed;
        rec.records_written = 42;
        rec.finished_at = Some(chrono::Utc::now());
        rec.elapsed_secs = Some(1.5);

        let mut s = spec("https://x.example/h");
        s.extra_fields
            .insert("job_id".into(), Value::String("abc".into()));

        let body = payload(&rec, &s);
        assert_eq!(body["event"], "run.completed");
        assert_eq!(body["run_id"], "run-7");
        assert_eq!(body["status"], "completed");
        assert_eq!(body["name"], "orders");
        assert_eq!(body["labels"]["env"], "prod");
        assert_eq!(body["records_written"], 42);
        assert_eq!(body["elapsed_secs"], 1.5);
        assert!(body["error"].is_null());
        assert_eq!(body["job_id"], "abc");
    }

    #[test]
    fn payload_emits_null_for_absent_optional_fields() {
        let rec = RunRecord::queued("r".into(), None, BTreeMap::new(), None, chrono::Utc::now());
        let body = payload(&rec, &spec("https://x.example/h"));
        for k in ["name", "started_at", "finished_at", "elapsed_secs", "error"] {
            assert!(body.get(k).is_some(), "{k} key must exist");
            assert!(body[k].is_null(), "{k} must be null");
        }
    }
}

//! HTTP delivery for each channel (#280).
//!
//! Thin I/O shims: take a rendered JSON body + the channel config, POST it, and
//! map the HTTP outcome to `Result<(), String>` (the `Err` string is logged,
//! never surfaced to the pipeline). All retry / timeout / dedupe policy lives in
//! [`crate::notify::dispatch`]; these functions do exactly one request each.

use super::render::{self, PdAction};
use super::spec::{PagerdutyConfig, SlackConfig, WebhookConfig};
use crate::notify::event::NotifyEvent;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;

/// Default PagerDuty Events API v2 endpoint (global).
pub const PAGERDUTY_ENDPOINT: &str = "https://events.pagerduty.com/v2/enqueue";

/// Deliver a Slack message.
pub async fn send_slack(
    client: &Client,
    cfg: &SlackConfig,
    event: &NotifyEvent,
) -> Result<(), String> {
    let body = render::slack(cfg, event);
    post_json(client, &cfg.webhook_url, &body, &[]).await
}

/// Deliver a PagerDuty trigger or resolve.
pub async fn send_pagerduty(
    client: &Client,
    cfg: &PagerdutyConfig,
    event: &NotifyEvent,
    action: PdAction,
    dedup_key: &str,
) -> Result<(), String> {
    let body = render::pagerduty(cfg, event, action, dedup_key);
    let url = cfg.endpoint.as_deref().unwrap_or(PAGERDUTY_ENDPOINT);
    post_json(client, url, &body, &[]).await
}

/// Deliver a generic webhook, optionally HMAC-signed.
pub async fn send_webhook(
    client: &Client,
    cfg: &WebhookConfig,
    event: &NotifyEvent,
) -> Result<(), String> {
    let body = render::webhook(cfg, event);
    let raw = serde_json::to_vec(&body).map_err(|e| format!("serializing webhook body: {e}"))?;

    let mut headers: Vec<(String, String)> = cfg
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if let Some(secret) = &cfg.hmac_secret {
        headers.push((
            cfg.signature_header.clone(),
            hmac_sha256_hex(secret.as_bytes(), &raw),
        ));
    }

    let method = reqwest::Method::from_bytes(cfg.method.as_bytes())
        .map_err(|_| format!("invalid HTTP method `{}`", cfg.method))?;
    let mut req = client
        .request(method, &cfg.url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(raw);
    for (k, v) in &headers {
        req = req.header(k, v);
    }
    let resp = req.send().await.map_err(request_failed)?;
    check_status(resp).await
}

/// POST a JSON body with optional extra headers.
async fn post_json(
    client: &Client,
    url: &str,
    body: &Value,
    headers: &[(String, String)],
) -> Result<(), String> {
    let mut req = client.post(url).json(body);
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let resp = req.send().await.map_err(request_failed)?;
    check_status(resp).await
}

/// A transport error, without the request URL: a channel URL (a Slack
/// incoming webhook) is itself a credential, and this text is logged.
fn request_failed(e: reqwest::Error) -> String {
    format!("request failed: {}", e.without_url())
}

/// A 2xx is success; anything else is an error carrying the status + a short
/// body excerpt (helps diagnose a bad Slack/PD token without leaking much).
async fn check_status(resp: reqwest::Response) -> Result<(), String> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    let excerpt: String = body.chars().take(200).collect();
    Err(format!("HTTP {status}: {excerpt}"))
}

/// Lowercase-hex HMAC-SHA256 of `data` under `key`.
fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    let bytes = mac.finalize().into_bytes();
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_transport_error_does_not_carry_the_webhook_url() {
        let cfg = crate::notify::spec::SlackConfig {
            webhook_url: "http://127.0.0.1:1/services/T000/B000/SECRETTOKEN".into(),
            channel: None,
            username: None,
        };
        let err = send_slack(
            &Client::new(),
            &cfg,
            &crate::notify::NotifyEvent::run_failure("p", "", "s", "m"),
        )
        .await
        .unwrap_err();
        assert!(err.starts_with("request failed"), "{err}");
        assert!(!err.contains("SECRETTOKEN"), "{err}");
        assert!(!err.contains("127.0.0.1"), "{err}");
    }

    #[test]
    fn hmac_matches_known_vector() {
        // RFC 4231 test case 1: key = 0x0b*20, data = "Hi There".
        let key = [0x0bu8; 20];
        let got = hmac_sha256_hex(&key, b"Hi There");
        assert_eq!(
            got,
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn hmac_is_deterministic() {
        let a = hmac_sha256_hex(b"secret", b"payload");
        let b = hmac_sha256_hex(b"secret", b"payload");
        assert_eq!(a, b);
        assert_ne!(a, hmac_sha256_hex(b"secret", b"other"));
        assert_eq!(a.len(), 64); // 32 bytes hex
    }
}

//! Listeners a config opens (fields annotated `x-faucet-resource: listen`,
//! such as a `webhook` source's `listen_addr`), held to the `--allow-listen`
//! ranges of the policy a body loads under.

use crate::expand::ExpandedNode;
use crate::serve::error::ServeError;
use faucet_core::resolve::{ReferencePolicy, ResourceKind, annotated_fields};
use serde_json::Value;

/// One kind's listen fields: the top-level field, its schema default, and
/// whether the kind can require a shared secret (`auth_token` / `signature`).
#[derive(Debug, Default)]
struct KindListeners {
    fields: Vec<(String, Option<String>)>,
    authenticates: bool,
}

fn kind_listeners(is_source: bool, kind: &str) -> KindListeners {
    let mut out = KindListeners::default();
    if let Some(schema) = crate::registry::cached_connector_schema(is_source, kind) {
        let props = schema.get("properties");
        out.fields = annotated_fields(&schema)
            .into_iter()
            .filter(|(path, k)| *k == ResourceKind::Listen && !path.contains(['.', '[', '{']))
            .map(|(path, _)| {
                let default = props
                    .and_then(|p| p.get(&path))
                    .and_then(|f| f.get("default"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                (path, default)
            })
            .collect();
        out.authenticates =
            props.is_some_and(|p| p.get("auth_token").is_some() || p.get("signature").is_some());
    }
    out
}

/// The connectors of the expanded rows: `(is_source, kind, config)`.
fn connectors(nodes: &[ExpandedNode]) -> impl Iterator<Item = (bool, &str, &Value)> {
    nodes.iter().flat_map(|n| {
        let mut v = vec![
            (true, n.source.kind.as_str(), &n.source.config),
            (false, n.sink.kind.as_str(), &n.sink.config),
        ];
        if let Some(d) = &n.dlq {
            v.push((false, d.sink.kind.as_str(), &d.sink.config));
        }
        v
    })
}

/// Refuse a listener the rows open outside `policy`'s ranges — the address
/// a config omits is the schema default the connector binds — and one off
/// loopback that a connector able to require a shared secret opens without
/// one (`auth_token` / `signature`).
pub fn screen(nodes: &[ExpandedNode], policy: &ReferencePolicy) -> Result<(), ServeError> {
    if policy.listen_ranges().is_none() {
        return Ok(());
    }
    for (is_source, kind, config) in connectors(nodes) {
        let k = kind_listeners(is_source, kind);
        for (field, default) in &k.fields {
            let Some(addr) = config
                .get(field)
                .and_then(Value::as_str)
                .or(default.as_deref())
            else {
                continue;
            };
            policy.check_listen(addr).map_err(|r| {
                ServeError::Forbidden(format!("`{kind}` `{field}` {}: {}", r.subject, r.reason))
            })?;
            let off_loopback = addr
                .trim()
                .parse::<std::net::SocketAddr>()
                .is_ok_and(|a| !a.ip().is_loopback());
            if off_loopback && k.authenticates && !holds_secret(config) {
                return Err(ServeError::Forbidden(format!(
                    "`{kind}` `{field}` {addr} is reachable from the network; set `auth_token` \
                     or `signature`"
                )));
            }
        }
    }
    Ok(())
}

/// Whether a listener config names a non-empty `auth_token` or a
/// `signature` with a non-empty `secret`.
fn holds_secret(config: &Value) -> bool {
    let token = config
        .get("auth_token")
        .and_then(Value::as_str)
        .is_some_and(|t| !t.trim().is_empty());
    let signature = config
        .get("signature")
        .and_then(|s| s.get("secret"))
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty());
    token || signature
}

/// Whether the config opens a listener: one of its rows' connectors (or, for
/// a topology, any connector `type` in the document) declares a listen field.
pub fn opens_listener(doc: Option<&Value>, nodes: &[ExpandedNode]) -> bool {
    fn walk(v: &Value) -> bool {
        match v {
            Value::Object(map) => {
                let declares = map.get("type").and_then(Value::as_str).is_some_and(|t| {
                    !kind_listeners(true, t).fields.is_empty()
                        || !kind_listeners(false, t).fields.is_empty()
                });
                declares || map.values().any(walk)
            }
            Value::Array(items) => items.iter().any(walk),
            _ => false,
        }
    }
    connectors(nodes).any(|(s, k, _)| !kind_listeners(s, k).fields.is_empty())
        || doc.is_some_and(walk)
}

#[cfg(all(test, feature = "source-webhook", feature = "sink-jsonl"))]
mod tests {
    use super::*;

    fn nodes(source: Value) -> Vec<ExpandedNode> {
        let cfg = crate::config::PipelineConfig::from_value(serde_json::json!({
            "version": 1,
            "pipeline": { "source": { "type": "webhook", "config": source },
                          "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } } }
        }))
        .unwrap();
        crate::expand::expand(&cfg).unwrap()
    }

    fn caller() -> ReferencePolicy {
        crate::serve::references::ServeReferencePolicy::default()
            .reserve_ports([29150])
            .for_origin(
                crate::serve::load::BodyOrigin::Caller {
                    allow_subprocess: false,
                },
                None,
            )
    }

    fn refused(source: Value, policy: &ReferencePolicy, needle: &str) {
        let err = screen(&nodes(source.clone()), policy).unwrap_err();
        assert!(
            matches!(&err, ServeError::Forbidden(m) if m.contains(needle)),
            "{source}: {err:?}"
        );
    }

    #[test]
    fn caller_webhook_binds_need_the_listen_allow_list() {
        let p = caller();
        screen(
            &nodes(serde_json::json!({ "listen_addr": "127.0.0.1:29100" })),
            &p,
        )
        .unwrap();
        screen(
            &nodes(serde_json::json!({ "listen_addr": "[::1]:29199" })),
            &p,
        )
        .unwrap();
        // The default 8080 is outside the range; the refusal names it.
        refused(serde_json::json!({}), &p, "127.0.0.1:29100-29199");
        refused(
            serde_json::json!({ "listen_addr": "0.0.0.0:29100" }),
            &p,
            "outside",
        );
        refused(
            serde_json::json!({ "listen_addr": "localhost:29100" }),
            &p,
            "literal",
        );
        refused(
            serde_json::json!({ "listen_addr": "127.0.0.1:0" }),
            &p,
            "port 0",
        );
        refused(
            serde_json::json!({ "listen_addr": "127.0.0.1:29150" }),
            &p,
            "port 29150",
        );

        let wide = ReferencePolicy::deny_all()
            .with_listen([faucet_core::resolve::ListenRange::parse("[::]:9000").unwrap()]);
        refused(
            serde_json::json!({ "listen_addr": "0.0.0.0:9000" }),
            &wide,
            "auth_token",
        );
        screen(
            &nodes(serde_json::json!({ "listen_addr": "0.0.0.0:9000", "auth_token": "t" })),
            &wide,
        )
        .unwrap();
        screen(
            &nodes(serde_json::json!({
                "listen_addr": "0.0.0.0:9000",
                "signature": { "header": "x-sig", "secret": "s" }
            })),
            &wide,
        )
        .unwrap();
        for blank in ["", "  "] {
            refused(
                serde_json::json!({ "listen_addr": "0.0.0.0:9000", "auth_token": blank }),
                &wide,
                "auth_token",
            );
        }
        assert!(!holds_secret(
            &serde_json::json!({ "signature": { "header": "x-sig", "secret": "" } })
        ));
        assert!(!holds_secret(&serde_json::json!({ "auth_token": null })));

        let tenant = crate::serve::references::ServeReferencePolicy::default().for_origin(
            crate::serve::load::BodyOrigin::Caller {
                allow_subprocess: false,
            },
            Some("acme"),
        );
        refused(
            serde_json::json!({ "listen_addr": "127.0.0.1:29100" }),
            &tenant,
            "not allowed",
        );
        // An operator's body binds where it says.
        screen(&nodes(serde_json::json!({})), &ReferencePolicy::open()).unwrap();
    }

    #[test]
    fn a_config_opens_a_listener_when_a_connector_declares_one() {
        assert!(opens_listener(None, &nodes(serde_json::json!({}))));
        let topology = serde_json::json!({ "pipeline": { "nodes": {
            "in": { "kind": "source", "type": "webhook" } } } });
        assert!(opens_listener(Some(&topology), &[]));
        let none = serde_json::json!({ "pipeline": { "source": { "type": "jsonl" } } });
        assert!(!opens_listener(Some(&none), &[]));
    }
}

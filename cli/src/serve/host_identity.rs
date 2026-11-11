//! Connectors that authenticate with the server's own identity: its
//! environment, instance or workload role, application-default credentials,
//! local credential files, OS authentication.
//!
//! Each connector answers from its typed config through its descriptor
//! ([`faucet_core::ConnectorDescriptor::with_host_identity`], read with
//! [`crate::registry::source_host_identity`] /
//! [`crate::registry::sink_host_identity`]). A config a caller submitted may
//! use the server's identity only when `faucet serve` runs with
//! `--allow-host-credentials`, and never for a tenant
//! ([`faucet_core::resolve::ReferencePolicy::allows_host_credentials`]).
//! The check runs on the expanded config — every row's source, sink and DLQ
//! sink, every topology node and a mirror's snapshot source — so a matrix row
//! that supplies its own keys is judged after it is merged.

use crate::config::PipelineConfig;
use crate::expand::ExpandedNode;
use crate::registry::HostIdentity;
use crate::serve::error::ServeError;
use faucet_core::resolve::ReferencePolicy;
use serde_json::Value;

/// The `faucet serve` flag that lets a caller's config use the server's
/// identity.
pub const FLAG: &str = "--allow-host-credentials";

/// The refusal while a cluster member does not check the server's identity
/// when it builds a client.
pub const MEMBER_REFUSAL: &str = "host credentials unavailable: a cluster member does not check \
     whether a submitted config's connectors use the server's own identity when it builds them; \
     finish the rolling upgrade, then retry";

/// Whether a connector reads (a source) or writes (a sink).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Source,
    Sink,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::Source => "source",
            Role::Sink => "sink",
        }
    }

    /// The registry's answer for a `kind` connector in this role.
    pub fn host_identity(self, kind: &str, config: &Value) -> HostIdentity {
        match self {
            Role::Source => crate::registry::source_host_identity(kind, config),
            Role::Sink => crate::registry::sink_host_identity(kind, config),
        }
    }

    fn checks_at_runtime(self, kind: &str) -> bool {
        match self {
            Role::Source => crate::registry::source_checks_host_identity_at_runtime(kind),
            Role::Sink => crate::registry::sink_checks_host_identity_at_runtime(kind),
        }
    }
}

/// One connector a config builds.
#[derive(Debug, Clone)]
struct Connector {
    at: String,
    role: Role,
    kind: String,
    config: Value,
}

/// Every connector a loaded config builds: each expanded row's source, sink
/// and DLQ sink, each topology node, the pipeline DLQ and a mirror's snapshot
/// source.
fn connectors(cfg: &PipelineConfig, nodes: &[ExpandedNode]) -> Vec<Connector> {
    let mut out = Vec::new();
    let mut push = |at: String, role: Role, spec: &crate::config::ConnectorSpec| {
        out.push(Connector {
            at,
            role,
            kind: spec.kind.clone(),
            config: spec.config.clone(),
        });
    };
    for node in nodes {
        push(
            format!("row `{}` source", node.id),
            Role::Source,
            &node.source,
        );
        push(format!("row `{}` sink", node.id), Role::Sink, &node.sink);
        if let Some(dlq) = &node.dlq {
            push(format!("row `{}` dlq sink", node.id), Role::Sink, &dlq.sink);
        }
    }
    if let Some(dlq) = &cfg.pipeline.dlq {
        push("pipeline dlq sink".into(), Role::Sink, &dlq.sink);
    }
    if let Some(m) = &cfg.replication {
        push(
            "mirror snapshot source".into(),
            Role::Source,
            &m.snapshot.source,
        );
    }
    if crate::topology::is_topology(cfg) {
        let found = [
            (Role::Source, crate::topology::source_nodes(cfg)),
            (Role::Sink, crate::topology::sink_nodes(cfg)),
        ];
        for (role, list) in found {
            for (id, kind, config) in list.unwrap_or_default() {
                out.push(Connector {
                    at: format!("node `{id}`"),
                    role,
                    kind,
                    config,
                });
            }
        }
    }
    out
}

/// One connector that does, or may, use the server's identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Use {
    /// Where it is (`row \`r\` sink`, `node \`n\``).
    pub at: String,
    pub role: Role,
    pub kind: String,
    pub identity: HostIdentity,
}

impl Use {
    fn describe(&self) -> String {
        let role = self.role.as_str();
        match &self.identity {
            HostIdentity::Unknown => format!(
                "{} `{}` (a custom connector that does not say whether it does — register it \
                 with `register_{role}_described` and a descriptor built `with_host_identity`)",
                self.at, self.kind
            ),
            identity => {
                let fields = identity
                    .fields()
                    .iter()
                    .map(|f| format!("`{f}`"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} `{}` ({fields})", self.at, self.kind)
            }
        }
    }
}

/// Every connector of the loaded config that does, or may, authenticate
/// with the server's own identity.
pub fn uses(cfg: &PipelineConfig, nodes: &[ExpandedNode]) -> Vec<Use> {
    let mut out: Vec<Use> = Vec::new();
    for c in connectors(cfg, nodes) {
        let identity = c.role.host_identity(&c.kind, &c.config);
        if !identity.uses_host_identity() {
            continue;
        }
        let u = Use {
            at: c.at,
            role: c.role,
            kind: c.kind,
            identity,
        };
        if !out.contains(&u) {
            out.push(u);
        }
    }
    out
}

/// Refuse (403) a caller's config that would use the server's identity
/// unless `policy` allows it. The refusal names the connectors and fields,
/// never a value.
pub fn refuse(
    cfg: &PipelineConfig,
    nodes: &[ExpandedNode],
    policy: &ReferencePolicy,
) -> Result<(), ServeError> {
    if policy.allows_host_credentials() {
        return Ok(());
    }
    let found = uses(cfg, nodes);
    if found.is_empty() {
        return Ok(());
    }
    let list = found
        .iter()
        .map(Use::describe)
        .collect::<Vec<_>>()
        .join("; ");
    Err(ServeError::Forbidden(format!(
        "this config authenticates with the server's own identity (its environment, instance or \
         workload role, or local credential files), which a submitted config may not use: \
         {list}. Name credentials in the config, or the operator can start `faucet serve` with \
         `{FLAG}` (never available to a tenant)"
    )))
}

/// Whether the loaded config builds a connector that also checks the
/// server's identity when it builds a client
/// ([`faucet_core::ConnectorDescriptor::with_runtime_host_identity`]).
pub fn checked_at_runtime(cfg: &PipelineConfig, nodes: &[ExpandedNode]) -> bool {
    connectors(cfg, nodes)
        .iter()
        .any(|c| c.role.checks_at_runtime(&c.kind))
}

/// The caller reference policy for a principal's tenant, whatever origin its
/// body is re-loaded with.
pub fn caller_policy(
    references: &crate::serve::references::ServeReferencePolicy,
    tenant: Option<&str>,
) -> ReferencePolicy {
    references.for_origin(
        crate::serve::load::BodyOrigin::Caller {
            allow_subprocess: false,
        },
        tenant,
    )
}

/// Whether a run must wait for every cluster member to advertise
/// [`crate::serve::history::CAP_HOST_CREDENTIALS`]: a caller's body, refused
/// the server's identity, that builds a connector checked only when it
/// builds a client (`runtime_checked`).
pub fn members_must_check(
    caller_body: bool,
    tenant: Option<&str>,
    references: &crate::serve::references::ServeReferencePolicy,
    runtime_checked: bool,
) -> bool {
    runtime_checked && caller_body && !caller_policy(references, tenant).allows_host_credentials()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_connectors_use_no_host_identity() {
        let cfg: PipelineConfig = crate::yaml::from_str(
            "version: 1\npipeline:\n  source: {type: csv, config: {path: in.csv}}\n  sink: {type: jsonl, config: {path: out.jsonl}}\n",
        )
        .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert!(uses(&cfg, &nodes).is_empty());
        assert!(!checked_at_runtime(&cfg, &nodes));
        assert!(refuse(&cfg, &nodes, &ReferencePolicy::deny_all()).is_ok());
    }

    #[test]
    fn only_refused_caller_runs_with_runtime_checks_wait_for_members() {
        use crate::serve::references::ServeReferencePolicy;
        let off = ServeReferencePolicy::default();
        let on = ServeReferencePolicy::from_args(&crate::cli::ReferenceArgs {
            allow_host_credentials: true,
            ..Default::default()
        })
        .unwrap();
        assert!(members_must_check(true, None, &off, true));
        assert!(!members_must_check(true, None, &off, false));
        assert!(!members_must_check(false, None, &off, true));
        assert!(!members_must_check(true, None, &on, true));
        assert!(members_must_check(true, Some("acme"), &on, true));
    }

    #[test]
    fn a_use_names_its_fields_or_how_to_answer() {
        let u = Use {
            at: "row `a` sink".into(),
            role: Role::Sink,
            kind: "s3".into(),
            identity: HostIdentity::Fields(vec![
                "credentials".into(),
                "staging.credentials".into(),
            ]),
        };
        assert_eq!(
            u.describe(),
            "row `a` sink `s3` (`credentials`, `staging.credentials`)"
        );
        let u = Use {
            identity: HostIdentity::Unknown,
            role: Role::Source,
            ..u
        };
        let text = u.describe();
        assert!(text.contains("register_source_described"), "{text}");
        assert!(text.contains("with_host_identity"), "{text}");
    }
}

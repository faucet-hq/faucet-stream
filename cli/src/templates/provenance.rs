//! Whose a registered template is. A version a template sync registered
//! (`created_by: sync:<origin>`) under an `<owner>/` namespace is that
//! owner's — a community template — unless its origin trusts the owner or
//! the id (`trust:`), or it is the `faucet-hq` namespace of the official
//! catalog. Anything an operator registered (HTTP, CLI, MCP) is the
//! operator's. Derived from what is stored, so templates synced by earlier
//! releases are covered and a `trust:` edit takes effect without a re-sync.

/// What one sync origin trusts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginTrust {
    pub name: String,
    /// The official catalog (`faucet-hq/template-hub` at `main`).
    pub official_repo: bool,
    pub trust: Vec<String>,
}

/// The community owner of template `id` whose version was created by
/// `created_by`, or `None` when it is the operator's or a trusted owner's. A
/// synced namespaced version whose origin is no longer configured is
/// community (nothing vouches for it).
pub fn community_owner(
    id: &str,
    created_by: Option<&str>,
    origins: &[OriginTrust],
) -> Option<String> {
    let origin = created_by?.strip_prefix("sync:")?;
    let (owner, _) = id.split_once('/')?;
    match origins.iter().find(|o| o.name == origin) {
        Some(o) if o.official_repo && owner == crate::hub::spec::OFFICIAL_OWNER => None,
        Some(o) if o.trust.iter().any(|t| t == "*" || t == owner || t == id) => None,
        _ => Some(owner.to_string()),
    }
}

/// The trust of every origin in a sync file.
#[cfg(feature = "templates-sync")]
pub fn origin_trust(file: &crate::templates::sync::SyncFile) -> Vec<OriginTrust> {
    file.origins
        .iter()
        .map(|o| OriginTrust {
            name: o.name.clone(),
            official_repo: crate::templates::sync::is_official_origin(o),
            trust: o.trust.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "templates-sync")]
    #[test]
    fn origin_trust_reads_each_origin_of_a_sync_file() {
        let file: crate::templates::sync::SyncFile = crate::yaml::from_str(
            "version: 1\norigins:\n  - name: hub\n    source:\n      type: github\n      config:\n        repo: faucet-hq/template-hub\n        ref: main\n    prefix: \"\"\n    trust: [acme]\n",
        )
        .unwrap();
        let t = origin_trust(&file);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].name, "hub");
        assert!(t[0].official_repo);
        assert_eq!(t[0].trust, ["acme"]);
    }

    #[test]
    fn whose_a_template_is() {
        let origins = [
            OriginTrust {
                name: "hub".into(),
                official_repo: true,
                trust: vec!["acme".into()],
            },
            OriginTrust {
                name: "fork".into(),
                official_repo: false,
                trust: vec!["beta/one".into()],
            },
        ];
        let owner = |id, by| community_owner(id, by, &origins);
        assert_eq!(owner("faucet-hq/csv", Some("sync:hub")), None, "official");
        assert_eq!(
            owner("faucet-hq/csv", Some("sync:fork")).as_deref(),
            Some("faucet-hq"),
            "the namespace only counts in the official catalog"
        );
        assert_eq!(owner("acme/x", Some("sync:hub")), None, "a trusted owner");
        assert_eq!(owner("beta/one", Some("sync:fork")), None, "a trusted id");
        assert_eq!(
            owner("beta/two", Some("sync:fork")).as_deref(),
            Some("beta")
        );
        assert_eq!(
            owner("beta/one", Some("sync:gone")).as_deref(),
            Some("beta"),
            "an origin no longer configured vouches for nothing"
        );
        assert_eq!(
            owner("beta/one", Some("admin")),
            None,
            "registered by a principal"
        );
        assert_eq!(owner("plain", Some("sync:fork")), None, "not namespaced");
        assert_eq!(owner("beta/one", None), None);
    }
}

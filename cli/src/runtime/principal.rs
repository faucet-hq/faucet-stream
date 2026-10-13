//! Who an operation runs for, and how far its config is trusted.

/// Where an operation's config came from, which decides how it is screened
/// when it is loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// A CLI command on the operator's machine: the operator's own files.
    Local,
    /// The server's operator or an admin wrote it (a registered template), or
    /// it was already admitted once (a stored run re-loaded on claim).
    Trusted,
    /// The request carried it. Subprocess connectors run only when the server
    /// allows them.
    Caller { allow_subprocess: bool },
    /// A file the server reads (the triggers file): trusted, and its SQL
    /// connectors keep host access.
    ServerFile,
}

/// The identity an operation runs as: who (for the audit log), for which
/// tenant (its scope, limits and state namespace), and from which [`Origin`].
#[derive(Debug, Clone)]
pub struct Principal {
    identity: String,
    tenant: Option<String>,
    origin: Origin,
    caller_body: bool,
    #[cfg(feature = "serve")]
    actor: Option<crate::serve::rbac::AuthContext>,
}

impl Principal {
    /// The operator running a CLI command.
    pub fn local() -> Self {
        Self {
            identity: "local".to_string(),
            tenant: None,
            origin: Origin::Local,
            caller_body: false,
            #[cfg(feature = "serve")]
            actor: None,
        }
    }

    /// Who the operation is attributed to.
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// The tenant the operation runs for, if any.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    /// How the operation's config is screened when it is loaded.
    pub fn origin(&self) -> Origin {
        self.origin
    }

    /// Whether the config came from a caller, so its HTTP clients run under
    /// the caller reference policy (#881) — also when an already admitted
    /// body is re-loaded as [`Origin::Trusted`].
    pub fn caller_body(&self) -> bool {
        self.caller_body
    }

    /// The reference policy the operation's runtime HTTP clients check their
    /// requests and redirects against: the caller policy for a body a caller
    /// supplied, none otherwise.
    #[cfg(feature = "serve")]
    pub fn reference_policy(
        &self,
    ) -> Option<std::sync::Arc<faucet_core::resolve::ReferencePolicy>> {
        self.caller_body.then(|| {
            std::sync::Arc::new(crate::serve::references::installed().for_origin(
                crate::serve::load::BodyOrigin::Caller {
                    allow_subprocess: false,
                },
                self.tenant.as_deref(),
            ))
        })
    }

    /// No runtime reference policy outside `faucet serve`.
    #[cfg(not(feature = "serve"))]
    pub fn reference_policy(
        &self,
    ) -> Option<std::sync::Arc<faucet_core::resolve::ReferencePolicy>> {
        None
    }
}

#[cfg(feature = "serve")]
impl Principal {
    /// A request's principal: `actor`, for its own tenant, with a body of
    /// `origin`.
    pub fn new(actor: crate::serve::rbac::AuthContext, origin: Origin) -> Self {
        Self {
            identity: actor.principal.clone(),
            tenant: actor.tenant.clone(),
            origin,
            caller_body: matches!(origin, Origin::Caller { .. }),
            actor: Some(actor),
        }
    }

    /// `actor` with a config the request carried.
    pub fn caller(
        state: &crate::serve::state::ServerState,
        actor: crate::serve::rbac::AuthContext,
    ) -> Self {
        Self::new(actor, Origin::from(state.caller_origin()))
    }

    /// `actor` with a body the server's operator wrote (a template) when
    /// `trusted`, else one the request carried.
    pub fn for_body(
        state: &crate::serve::state::ServerState,
        actor: crate::serve::rbac::AuthContext,
        trusted: bool,
    ) -> Self {
        Self::new(actor, Origin::from(state.origin(trusted)))
    }

    /// The principal a stored run re-executes as: its own tenant, trusted
    /// (it was screened when it was submitted), under the caller reference
    /// policy when a caller supplied it.
    pub fn stored(
        actor: crate::serve::rbac::AuthContext,
        rec: &crate::serve::history::RunRecord,
    ) -> Self {
        let origin = Origin::from(crate::serve::runner::stored_origin(rec));
        let mut p = Self::new(actor, origin);
        p.tenant = rec.tenant.clone();
        p.caller_body = rec.caller_config;
        p
    }

    /// Run for `tenant` instead of the actor's own (an unscoped admin acting
    /// on a tenant's behalf). The audit identity stays the actor's.
    pub fn on_behalf_of(mut self, tenant: Option<String>) -> Self {
        if tenant.is_some() {
            self.tenant = tenant;
        }
        self
    }

    /// The identity audit records and RBAC checks see.
    pub fn actor(&self) -> crate::serve::rbac::AuthContext {
        self.actor
            .clone()
            .unwrap_or_else(|| crate::serve::rbac::AuthContext::system(&self.identity))
    }

    /// How the loader screens this principal's body.
    pub fn body_origin(&self) -> crate::serve::load::BodyOrigin {
        use crate::serve::load::BodyOrigin;
        match self.origin {
            Origin::Trusted => BodyOrigin::Trusted,
            Origin::Caller { allow_subprocess } => BodyOrigin::Caller { allow_subprocess },
            Origin::Local | Origin::ServerFile => BodyOrigin::ServerFile,
        }
    }
}

#[cfg(feature = "serve")]
impl From<crate::serve::load::BodyOrigin> for Origin {
    fn from(o: crate::serve::load::BodyOrigin) -> Self {
        use crate::serve::load::BodyOrigin;
        match o {
            BodyOrigin::Trusted => Origin::Trusted,
            BodyOrigin::Caller { allow_subprocess } => Origin::Caller { allow_subprocess },
            BodyOrigin::ServerFile => Origin::ServerFile,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_local_principal_is_trusted_and_unscoped() {
        let p = Principal::local();
        assert_eq!(p.identity(), "local");
        assert_eq!(p.tenant(), None);
        assert_eq!(p.origin(), Origin::Local);
        assert!(!p.caller_body());
        assert!(p.reference_policy().is_none());
    }

    #[cfg(feature = "serve")]
    #[test]
    fn a_caller_principal_carries_the_reference_policy_and_its_tenant() {
        use crate::serve::load::BodyOrigin;
        let mut actor = crate::serve::rbac::AuthContext::system("t");
        actor.tenant = Some("acme".into());
        let p = Principal::new(
            actor,
            Origin::Caller {
                allow_subprocess: false,
            },
        );
        assert_eq!(p.tenant(), Some("acme"));
        assert!(p.caller_body());
        assert!(p.reference_policy().is_some());
        assert_eq!(
            p.body_origin(),
            BodyOrigin::Caller {
                allow_subprocess: false
            }
        );
        let other = p.clone().on_behalf_of(None);
        assert_eq!(other.tenant(), Some("acme"));
        let other = p.on_behalf_of(Some("beta".into()));
        assert_eq!(other.tenant(), Some("beta"));
        assert_eq!(other.actor().tenant.as_deref(), Some("acme"));
    }

    #[cfg(feature = "serve")]
    #[test]
    fn origins_map_both_ways() {
        use crate::serve::load::BodyOrigin;
        for o in [
            BodyOrigin::Trusted,
            BodyOrigin::ServerFile,
            BodyOrigin::Caller {
                allow_subprocess: true,
            },
        ] {
            let p = Principal::new(
                crate::serve::rbac::AuthContext::system("t"),
                Origin::from(o),
            );
            assert_eq!(p.body_origin(), o);
        }
        let local = Principal::local();
        assert_eq!(local.body_origin(), BodyOrigin::ServerFile);
        assert_eq!(local.actor().principal, "system:local");
    }

    #[cfg(feature = "serve")]
    #[test]
    fn a_stored_run_reloads_trusted_but_keeps_its_callers_policy() {
        let mut rec = crate::serve::history::RunRecord::queued(
            "r".into(),
            None,
            Default::default(),
            None,
            chrono::Utc::now(),
        );
        rec.tenant = Some("acme".into());
        rec.caller_config = true;
        let p = Principal::stored(crate::serve::rbac::AuthContext::runtime(), &rec);
        assert_eq!(p.origin(), Origin::Trusted);
        assert_eq!(p.tenant(), Some("acme"));
        assert!(p.reference_policy().is_some());
        rec.host_access = true;
        rec.caller_config = false;
        let p = Principal::stored(crate::serve::rbac::AuthContext::runtime(), &rec);
        assert_eq!(p.origin(), Origin::ServerFile);
        assert!(p.reference_policy().is_none());
    }
}

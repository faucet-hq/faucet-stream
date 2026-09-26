//! Who may approve what (#703): the `approvals:` block of `--auth-config`.
//!
//! ```yaml
//! principals: [...]
//! approvals:
//!   expire_secs: 86400          # a pending request lapses after this (default 24h)
//!   rules:
//!     - kinds: [run]            # empty / omitted = every kind
//!       roles: [operator, admin]
//!       min_approvers: 1
//!       self_approve: false
//!     - kinds: [template_register, template_launch]
//!       principals: [alice]     # named approvers, on top of / instead of roles
//!       roles: [admin]
//!       min_approvers: 2
//! ```
//!
//! The first rule whose `kinds` covers a request's kind applies; with no rule
//! the default is **admins only, one approval, no self-approval**. A server
//! without RBAC (`--auth-token` / `--no-auth`) has a single principal, so its
//! policy is [`ApprovalPolicy::permissive`] — the same admin-only rule with
//! self-approval allowed, otherwise nothing could ever be approved there.

use super::ChangeKind;
use crate::serve::rbac::{AuthContext, Role};
use faucet_core::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default expiry of a pending request.
pub const DEFAULT_EXPIRE_SECS: u64 = 86_400;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApprovalPolicy {
    /// Seconds a pending request stays approvable. Default 86400.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_secs: Option<u64>,
    /// Ordered rules; the first whose `kinds` covers the request applies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<ApprovalRule>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRule {
    /// Change kinds this rule governs. Empty = every kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kinds: Vec<ChangeKind>,
    /// Roles that may approve. Empty (with no `principals`) = `admin`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<Role>,
    /// Named principals that may approve, whatever their role.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub principals: Vec<String>,
    /// Distinct approvals a request needs before it executes. Default 1.
    #[serde(default = "one")]
    pub min_approvers: u32,
    /// Whether the requester's own approval counts. Default `false`.
    #[serde(default)]
    pub self_approve: bool,
}

fn one() -> u32 {
    1
}

/// The rule in force for one request.
#[derive(Debug, Clone, PartialEq)]
pub struct EffectiveRule {
    pub roles: Vec<Role>,
    pub principals: Vec<String>,
    pub min_approvers: u32,
    pub self_approve: bool,
}

impl Default for EffectiveRule {
    fn default() -> Self {
        Self {
            roles: vec![Role::Admin],
            principals: Vec::new(),
            min_approvers: 1,
            self_approve: false,
        }
    }
}

impl ApprovalPolicy {
    /// The policy of a server with one principal (`--auth-token`, `--no-auth`,
    /// or the token trio's admin): admins approve, and — since requester and
    /// approver are the same principal — their own request.
    pub fn permissive() -> Self {
        Self {
            expire_secs: None,
            rules: vec![ApprovalRule {
                kinds: Vec::new(),
                roles: vec![Role::Admin],
                principals: Vec::new(),
                min_approvers: 1,
                self_approve: true,
            }],
        }
    }

    /// Fail-fast validation at server start.
    pub fn validate(&self) -> Result<(), String> {
        if self.expire_secs == Some(0) {
            return Err("approvals.expire_secs must be greater than 0".into());
        }
        for (i, r) in self.rules.iter().enumerate() {
            if r.min_approvers == 0 {
                return Err(format!(
                    "approvals.rules[{i}].min_approvers must be at least 1"
                ));
            }
            if r.principals.iter().any(|p| p.trim().is_empty()) {
                return Err(format!(
                    "approvals.rules[{i}].principals contains an empty name"
                ));
            }
        }
        Ok(())
    }

    /// Seconds a pending request stays approvable.
    pub fn expire_secs(&self) -> u64 {
        self.expire_secs.unwrap_or(DEFAULT_EXPIRE_SECS)
    }

    /// The rule for `kind`.
    pub fn effective(&self, kind: ChangeKind) -> EffectiveRule {
        let rule = self
            .rules
            .iter()
            .find(|r| r.kinds.is_empty() || r.kinds.contains(&kind));
        match rule {
            None => EffectiveRule::default(),
            Some(r) => EffectiveRule {
                roles: if r.roles.is_empty() && r.principals.is_empty() {
                    vec![Role::Admin]
                } else {
                    r.roles.clone()
                },
                principals: r.principals.clone(),
                min_approvers: r.min_approvers.max(1),
                self_approve: r.self_approve,
            },
        }
    }

    /// Whether `actor` may approve (or reject) a `kind` request made by
    /// `requester`. `Err` carries the reason, phrased for a 403.
    pub fn may_approve(
        &self,
        kind: ChangeKind,
        actor: &AuthContext,
        requester: &str,
    ) -> Result<(), String> {
        let rule = self.effective(kind);
        let named = rule.principals.iter().any(|p| p == &actor.principal);
        let by_role = rule.roles.contains(&actor.role);
        if !named && !by_role {
            let who = if rule.principals.is_empty() {
                format!(
                    "role {}",
                    rule.roles
                        .iter()
                        .map(|r| r.as_str())
                        .collect::<Vec<_>>()
                        .join(" / ")
                )
            } else {
                format!(
                    "{} or role {}",
                    rule.principals.join(", "),
                    rule.roles
                        .iter()
                        .map(|r| r.as_str())
                        .collect::<Vec<_>>()
                        .join(" / ")
                )
            };
            return Err(format!(
                "a {} change may be approved by {who}; '{}' is {}",
                kind.as_str(),
                actor.principal,
                actor.role.as_str()
            ));
        }
        if actor.principal == requester && !rule.self_approve {
            return Err(format!(
                "'{}' requested this change and may not approve it (self-approval is off for {} changes)",
                actor.principal,
                kind.as_str()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(principal: &str, role: Role) -> AuthContext {
        AuthContext {
            principal: principal.into(),
            role,
            source_ip: None,
            tenant: None,
        }
    }

    #[test]
    fn default_rule_is_admins_only_without_self_approval() {
        let p = ApprovalPolicy::default();
        let r = p.effective(ChangeKind::Run);
        assert_eq!(r, EffectiveRule::default());
        assert!(
            p.may_approve(ChangeKind::Run, &ctx("root", Role::Admin), "bob")
                .is_ok()
        );
        let err = p
            .may_approve(ChangeKind::Run, &ctx("op", Role::Operator), "bob")
            .unwrap_err();
        assert!(err.contains("role admin"), "{err}");
        let err = p
            .may_approve(ChangeKind::Run, &ctx("root", Role::Admin), "root")
            .unwrap_err();
        assert!(err.contains("self-approval"), "{err}");
        assert_eq!(p.expire_secs(), DEFAULT_EXPIRE_SECS);
        assert!(
            ApprovalPolicy::permissive()
                .may_approve(ChangeKind::Run, &ctx("token", Role::Admin), "token")
                .is_ok()
        );
    }

    #[test]
    fn first_matching_rule_wins_and_principals_bypass_roles() {
        let p: ApprovalPolicy = serde_yaml::from_str(
            "expire_secs: 60\nrules:\n  - kinds: [run]\n    roles: [operator]\n    self_approve: true\n  - principals: [alice]\n    min_approvers: 2\n",
        )
        .unwrap();
        p.validate().unwrap();
        assert_eq!(p.expire_secs(), 60);
        let run = p.effective(ChangeKind::Run);
        assert_eq!(run.roles, vec![Role::Operator]);
        assert!(run.self_approve);
        assert!(
            p.may_approve(ChangeKind::Run, &ctx("op", Role::Operator), "op")
                .is_ok()
        );
        // An admin is not an operator: the rule names roles exactly.
        assert!(
            p.may_approve(ChangeKind::Run, &ctx("root", Role::Admin), "op")
                .is_err()
        );
        let launch = p.effective(ChangeKind::TemplateLaunch);
        assert_eq!(launch.min_approvers, 2);
        assert!(launch.roles.is_empty());
        assert!(
            p.may_approve(
                ChangeKind::TemplateLaunch,
                &ctx("alice", Role::Viewer),
                "bob"
            )
            .is_ok(),
            "a named principal approves whatever their role"
        );
        let err = p
            .may_approve(ChangeKind::TemplateLaunch, &ctx("root", Role::Admin), "bob")
            .unwrap_err();
        assert!(err.contains("alice"), "{err}");
    }

    #[test]
    fn validate_refuses_zero_quorum_and_empty_names() {
        let bad: ApprovalPolicy = serde_yaml::from_str("rules:\n  - min_approvers: 0\n").unwrap();
        assert!(bad.validate().unwrap_err().contains("min_approvers"));
        let bad: ApprovalPolicy = serde_yaml::from_str("rules:\n  - principals: [' ']\n").unwrap();
        assert!(bad.validate().unwrap_err().contains("empty name"));
        let bad: ApprovalPolicy = serde_yaml::from_str("expire_secs: 0\n").unwrap();
        assert!(bad.validate().is_err());
        assert!(serde_yaml::from_str::<ApprovalPolicy>("nope: 1\n").is_err());
    }
}

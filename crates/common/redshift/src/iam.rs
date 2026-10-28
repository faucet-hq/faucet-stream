//! Temporary database credentials for `credentials: { type: iam }`.
//!
//! A provisioned cluster's come from `redshift:GetClusterCredentials`, a
//! serverless workgroup's from `redshift-serverless:GetCredentials`. Either is
//! a user and password valid until an expiration; [`crate::RedshiftPool`]
//! fetches a new pair before that and hands it to the pool, so every new
//! connection logs in with credentials that are still valid. Open connections
//! are unaffected: the credentials are only checked at login.

use std::time::{Duration, Instant, SystemTime};

use faucet_core::FaucetError;
use faucet_core::io::CallContext;
use tokio::sync::OnceCell;

use crate::config::{RedshiftAwsCredentials, RedshiftConnection, RedshiftCredentials};

/// The shortest and longest lifetime both credentials APIs accept.
pub const DURATION_RANGE: std::ops::RangeInclusive<u32> = 900..=3600;

/// How long before expiry new credentials are fetched, at most.
pub const REFRESH_MARGIN: Duration = Duration::from_secs(60);

/// A user and password and when they stop being accepted.
#[derive(Clone)]
pub struct TempCredentials {
    /// The database user to log in as (`IAM:<db_user>` on a provisioned
    /// cluster).
    pub user: String,
    /// The temporary password.
    pub password: String,
    /// When the password expires.
    pub expires_at: SystemTime,
}

impl std::fmt::Debug for TempCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TempCredentials")
            .field("user", &self.user)
            .field("password", &"***")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// The shortest and longest wait before retrying a failed refresh while the
/// current credentials are still valid; also the shortest time between two
/// fetches.
pub const RETRY_RANGE: std::ops::RangeInclusive<Duration> =
    Duration::from_secs(1)..=Duration::from_secs(60);

/// When credentials fetched at `fetched` and valid until `expires_at` should
/// be replaced: [`REFRESH_MARGIN`] before expiry, or halfway through a
/// lifetime shorter than twice the margin, but never sooner than the start of
/// [`RETRY_RANGE`] after `fetched` (credentials that look expired already, as
/// with a skewed clock, are not refetched on every use).
pub fn refresh_at(fetched: Instant, now: SystemTime, expires_at: SystemTime) -> Instant {
    let lifetime = expires_at.duration_since(now).unwrap_or_default();
    (fetched + lifetime - REFRESH_MARGIN.min(lifetime / 2)).max(fetched + *RETRY_RANGE.start())
}

/// When to retry after a refresh failed at `failed` while the current
/// credentials are valid until `expires_at`: a quarter of the remaining
/// lifetime, within [`RETRY_RANGE`], and no later than expiry.
pub fn retry_at(failed: Instant, now: SystemTime, expires_at: SystemTime) -> Instant {
    let remaining = expires_at.duration_since(now).unwrap_or_default();
    let wait = (remaining / 4)
        .clamp(*RETRY_RANGE.start(), *RETRY_RANGE.end())
        .min(remaining);
    failed + wait
}

#[derive(Debug, Clone)]
enum Target {
    Cluster {
        id: String,
        db_user: String,
        auto_create: bool,
        db_groups: Vec<String>,
    },
    Workgroup {
        name: String,
    },
}

enum Client {
    Cluster(aws_sdk_redshift::Client),
    Workgroup(aws_sdk_redshiftserverless::Client),
}

/// Fetches temporary credentials for one connection.
pub struct IamCredentials {
    target: Target,
    database: String,
    region: Option<String>,
    duration_secs: Option<u32>,
    aws_credentials: RedshiftAwsCredentials,
    endpoint_url: Option<String>,
    client: OnceCell<Client>,
}

impl std::fmt::Debug for IamCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IamCredentials")
            .field("target", &self.target)
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

impl IamCredentials {
    /// The provider for an `iam` connection, validated; `None` for any other
    /// credentials method.
    pub fn from_connection(conn: &RedshiftConnection) -> Result<Option<Self>, FaucetError> {
        let RedshiftCredentials::Iam {
            region,
            cluster_identifier,
            db_user,
            workgroup_name,
            auto_create,
            db_groups,
            duration_secs,
            aws_credentials,
            endpoint_url,
        } = &conn.credentials
        else {
            return Ok(None);
        };
        fn present(v: &Option<String>) -> Option<&str> {
            v.as_deref().map(str::trim).filter(|s| !s.is_empty())
        }
        let target = match (present(cluster_identifier), present(workgroup_name)) {
            (Some(id), None) => {
                let user = present(db_user).unwrap_or(conn.user.trim());
                if user.is_empty() {
                    return Err(FaucetError::Config(
                        "redshift iam: a cluster needs `db_user` (or the connection's `user`)"
                            .into(),
                    ));
                }
                Target::Cluster {
                    id: id.to_string(),
                    db_user: user.to_string(),
                    auto_create: *auto_create,
                    db_groups: db_groups.clone(),
                }
            }
            (None, Some(name)) => {
                if present(db_user).is_some() || *auto_create || !db_groups.is_empty() {
                    return Err(FaucetError::Config(
                        "redshift iam: `db_user`, `auto_create` and `db_groups` apply to a \
                         provisioned `cluster_identifier`; a serverless workgroup derives the \
                         user from the IAM identity"
                            .into(),
                    ));
                }
                Target::Workgroup {
                    name: name.to_string(),
                }
            }
            _ => {
                return Err(FaucetError::Config(
                    "redshift iam: set exactly one of `cluster_identifier` (provisioned) or \
                     `workgroup_name` (serverless)"
                        .into(),
                ));
            }
        };
        if let Some(d) = duration_secs
            && !DURATION_RANGE.contains(d)
        {
            return Err(FaucetError::Config(format!(
                "redshift iam: `duration_secs` must be between {} and {} (got {d})",
                DURATION_RANGE.start(),
                DURATION_RANGE.end()
            )));
        }
        Ok(Some(Self {
            target,
            database: conn.database.clone(),
            region: region.clone(),
            duration_secs: *duration_secs,
            aws_credentials: aws_credentials.clone(),
            endpoint_url: endpoint_url.clone(),
            client: OnceCell::new(),
        }))
    }

    async fn client(&self) -> &Client {
        self.client
            .get_or_init(|| async {
                let sdk =
                    crate::aws::sdk_config(self.region.as_deref(), &self.aws_credentials).await;
                match &self.target {
                    Target::Cluster { .. } => {
                        let mut b = aws_sdk_redshift::config::Builder::from(&sdk);
                        if let Some(e) = &self.endpoint_url {
                            b = b.endpoint_url(e);
                        }
                        Client::Cluster(aws_sdk_redshift::Client::from_conf(b.build()))
                    }
                    Target::Workgroup { .. } => {
                        let mut b = aws_sdk_redshiftserverless::config::Builder::from(&sdk);
                        if let Some(e) = &self.endpoint_url {
                            b = b.endpoint_url(e);
                        }
                        Client::Workgroup(aws_sdk_redshiftserverless::Client::from_conf(b.build()))
                    }
                }
            })
            .await
    }

    /// Fetch a new user and password, as the call `credentials`.
    pub async fn fetch(&self, calls: &CallContext) -> Result<TempCredentials, FaucetError> {
        let client = self.client().await;
        calls
            .call("credentials", || async {
                let (user, password, expires) = match (client, &self.target) {
                    (
                        Client::Cluster(c),
                        Target::Cluster {
                            id,
                            db_user,
                            auto_create,
                            db_groups,
                        },
                    ) => {
                        let out = c
                            .get_cluster_credentials()
                            .cluster_identifier(id)
                            .db_user(db_user)
                            .db_name(&self.database)
                            .auto_create(*auto_create)
                            .set_db_groups((!db_groups.is_empty()).then(|| db_groups.clone()))
                            .set_duration_seconds(self.duration_secs.map(|d| d as i32))
                            .send()
                            .await
                            .map_err(|e| {
                                FaucetError::Auth(format!(
                                    "redshift iam: GetClusterCredentials failed: {}",
                                    crate::aws::describe(&e)
                                ))
                            })?;
                        (
                            out.db_user().map(str::to_string),
                            out.db_password().map(str::to_string),
                            out.expiration().copied(),
                        )
                    }
                    (Client::Workgroup(c), Target::Workgroup { name }) => {
                        let out = c
                            .get_credentials()
                            .workgroup_name(name)
                            .db_name(&self.database)
                            .set_duration_seconds(self.duration_secs.map(|d| d as i32))
                            .send()
                            .await
                            .map_err(|e| {
                                FaucetError::Auth(format!(
                                    "redshift iam: GetCredentials failed: {}",
                                    crate::aws::describe(&e)
                                ))
                            })?;
                        (
                            out.db_user().map(str::to_string),
                            out.db_password().map(str::to_string),
                            out.expiration().copied(),
                        )
                    }
                    _ => unreachable!("the client is built for its target"),
                };
                credentials_from(user, password, expires)
            })
            .await
    }
}

/// The credentials a response carries; every field is required.
fn credentials_from(
    user: Option<String>,
    password: Option<String>,
    expires: Option<aws_sdk_redshift::primitives::DateTime>,
) -> Result<TempCredentials, FaucetError> {
    let missing = |what: &str| {
        FaucetError::Auth(format!(
            "redshift iam: the credentials response has no {what}"
        ))
    };
    let user = user
        .filter(|u| !u.is_empty())
        .ok_or_else(|| missing("user"))?;
    let password = password
        .filter(|p| !p.is_empty())
        .ok_or_else(|| missing("password"))?;
    let expires = expires.ok_or_else(|| missing("expiration"))?;
    let expires_at = SystemTime::try_from(expires)
        .map_err(|_| FaucetError::Auth("redshift iam: the expiration is out of range".into()))?;
    Ok(TempCredentials {
        user,
        password,
        expires_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(config: serde_json::Value) -> RedshiftConnection {
        serde_json::from_value(serde_json::json!({
            "host": "h", "database": "dev", "user": "fallback",
            "credentials": {"type": "iam", "config": config}
        }))
        .unwrap()
    }

    #[test]
    fn a_cluster_takes_db_user_or_the_connection_user() {
        let p =
            IamCredentials::from_connection(&conn(serde_json::json!({"cluster_identifier": "c"})))
                .unwrap()
                .unwrap();
        assert!(matches!(&p.target, Target::Cluster { db_user, .. } if db_user == "fallback"));
        assert!(format!("{p:?}").contains("fallback"));
        let p = IamCredentials::from_connection(&conn(
            serde_json::json!({"cluster_identifier": "c", "db_user": "analyst", "auto_create": true, "db_groups": ["g"]}),
        ))
        .unwrap()
        .unwrap();
        assert!(
            matches!(&p.target, Target::Cluster { db_user, auto_create: true, db_groups, .. } if db_user == "analyst" && db_groups == &["g".to_string()])
        );
        let p = IamCredentials::from_connection(&conn(serde_json::json!({"workgroup_name": "wg"})))
            .unwrap()
            .unwrap();
        assert!(matches!(&p.target, Target::Workgroup { name } if name == "wg"));
        assert!(
            IamCredentials::from_connection(&RedshiftConnection::new("h", "d", "u", "p"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn invalid_iam_settings_are_refused() {
        let mut no_user = conn(serde_json::json!({"cluster_identifier": "c"}));
        no_user.user = String::new();
        for (c, needle) in [
            (no_user, "db_user"),
            (conn(serde_json::json!({})), "exactly one"),
            (
                conn(serde_json::json!({"cluster_identifier": "c", "workgroup_name": "w"})),
                "exactly one",
            ),
            (
                conn(serde_json::json!({"workgroup_name": "w", "db_user": "u"})),
                "serverless",
            ),
            (
                conn(serde_json::json!({"cluster_identifier": "c", "duration_secs": 60})),
                "duration_secs",
            ),
        ] {
            let e = IamCredentials::from_connection(&c).unwrap_err().to_string();
            assert!(e.contains(needle), "{e}");
        }
    }

    #[test]
    fn refresh_comes_a_margin_before_expiry_or_halfway() {
        let now = SystemTime::now();
        let fetched = Instant::now();
        assert_eq!(
            refresh_at(fetched, now, now + Duration::from_secs(900)),
            fetched + Duration::from_secs(840)
        );
        assert_eq!(
            refresh_at(fetched, now, now + Duration::from_secs(10)),
            fetched + Duration::from_secs(5)
        );
        assert_eq!(
            refresh_at(fetched, now, now - Duration::from_secs(1)),
            fetched + Duration::from_secs(1),
            "already expired by the local clock: not refetched on every use"
        );
        assert_eq!(
            refresh_at(fetched, now, now + Duration::from_secs(1)),
            fetched + Duration::from_secs(1)
        );
    }

    #[test]
    fn a_failed_refresh_backs_off_within_the_remaining_lifetime() {
        let failed = Instant::now();
        let now = SystemTime::now();
        let at = |secs: u64| retry_at(failed, now, now + Duration::from_secs(secs)) - failed;
        assert_eq!(at(60), Duration::from_secs(15));
        assert_eq!(at(3600), Duration::from_secs(60));
        assert_eq!(at(2), Duration::from_secs(1));
        assert_eq!(
            retry_at(failed, now, now + Duration::from_millis(400)) - failed,
            Duration::from_millis(400)
        );
        assert_eq!(retry_at(failed, now, now - Duration::from_secs(1)), failed);
    }

    #[test]
    fn a_response_needs_every_field() {
        let exp = aws_sdk_redshift::primitives::DateTime::from_secs(2_000_000_000);
        let ok = credentials_from(Some("IAM:u".into()), Some("pw".into()), Some(exp)).unwrap();
        assert_eq!(ok.user, "IAM:u");
        assert!(!format!("{ok:?}").contains("pw\""));
        for (u, p, e, needle) in [
            (None, Some("p"), Some(exp), "user"),
            (Some("u"), Some(""), Some(exp), "password"),
            (Some("u"), Some("p"), None, "expiration"),
        ] {
            let err = credentials_from(u.map(Into::into), p.map(Into::into), e)
                .unwrap_err()
                .to_string();
            assert!(err.contains(needle), "{err}");
        }
    }
}

//! Whether an object-store option map (the string keys `object_store` and
//! the table-format libraries built on it read: `aws_access_key_id`,
//! `google_service_account`, `azure_storage_account_key`, …) makes the
//! client authenticate with the identity of the host it runs on.
//! Re-exported from [`crate::resolve`].
//!
//! Keys are matched the way those libraries parse them: any case, with or
//! without the store prefix (`aws_` / `google_` / `azure_storage_` /
//! `azure_`).

/// The object store an option map configures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StoreKind {
    /// S3 and S3-compatible stores.
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage / ADLS Gen2.
    Azure,
}

impl StoreKind {
    /// The store a location URL names (`s3://`, `s3a://`, `gs://`, `az://`,
    /// `abfss://`, an `https://` Azure blob or DFS host, …), `None` for
    /// anything else (a local path, `memory://`).
    pub fn from_url(url: &str) -> Option<Self> {
        let lower = url.trim().to_ascii_lowercase();
        let (scheme, rest) = lower.split_once("://")?;
        match scheme {
            "s3" | "s3a" => Some(Self::S3),
            "gs" | "gcs" => Some(Self::Gcs),
            "az" | "adl" | "azure" | "abfs" | "abfss" => Some(Self::Azure),
            "http" | "https" => {
                let host = rest.split(['/', '?', '#']).next().unwrap_or_default();
                if host.ends_with(".amazonaws.com") || host.ends_with(".r2.cloudflarestorage.com") {
                    Some(Self::S3)
                } else if host.contains(".blob.core.") || host.contains(".dfs.core.") {
                    Some(Self::Azure)
                } else if host == "storage.googleapis.com" {
                    Some(Self::Gcs)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

fn normalized(key: &str, kind: StoreKind) -> String {
    let lower = key.trim().to_ascii_lowercase();
    let prefixes: &[&str] = match kind {
        StoreKind::S3 => &["aws_"],
        StoreKind::Gcs => &["google_"],
        StoreKind::Azure => &["azure_storage_", "azure_"],
    };
    prefixes
        .iter()
        .find_map(|p| lower.strip_prefix(p))
        .map_or_else(|| lower.clone(), str::to_string)
}

fn truthy(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "on" | "yes" | "y"
    )
}

/// The key of `options` that makes a `kind` client authenticate with the
/// host's identity, or `"credentials"` when the map names no credential of
/// its own (the client would fall back to its default chain). `None` when
/// the map names its own credentials and nothing that reaches the host's.
///
/// A key naming a role, a profile, the instance metadata service, a
/// container or workload-identity credential, or the Azure CLI counts even
/// beside static keys: the library then resolves it through the host.
pub fn store_options_host_identity<I, K, V>(kind: StoreKind, options: I) -> Option<String>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut set = std::collections::BTreeMap::new();
    for (k, v) in options {
        let (k, v) = (k.as_ref(), v.as_ref());
        if !v.trim().is_empty() {
            set.insert(normalized(k, kind), (k.to_string(), v.to_string()));
        }
    }
    let has = |names: &[&str]| names.iter().any(|n| set.contains_key(*n));
    let on = |name: &str| set.get(name).is_some_and(|(_, v)| truthy(v));
    let host_keys: &[&str] = match kind {
        StoreKind::S3 => &[
            "role_arn",
            "iam_role_arn",
            "s3_assume_role_arn",
            "profile",
            "web_identity_token_file",
            "container_credentials_relative_uri",
            "container_credentials_full_uri",
            "container_authorization_token_file",
            "metadata_endpoint",
            "imdsv1_fallback",
        ],
        StoreKind::Gcs => &[],
        StoreKind::Azure => &[
            "msi_endpoint",
            "identity_endpoint",
            "object_id",
            "msi_resource_id",
            "federated_token_file",
        ],
    };
    if let Some(found) = host_keys.iter().find(|n| set.contains_key(**n)) {
        return Some(set[*found].0.clone());
    }
    if kind == StoreKind::Azure && on("use_azure_cli") {
        return Some(set["use_azure_cli"].0.clone());
    }
    let own = on("skip_signature")
        || match kind {
            StoreKind::S3 => has(&["access_key_id"]) && has(&["secret_access_key"]),
            StoreKind::Gcs => has(&[
                "service_account",
                "service_account_path",
                "service_account_key",
                "application_credentials",
            ]),
            StoreKind::Azure => {
                has(&[
                    "account_key",
                    "access_key",
                    "master_key",
                    "sas_key",
                    "sas_token",
                    "token",
                    "bearer_token",
                    "fabric_session_token",
                ]) || on("use_emulator")
                    || (has(&["client_id"])
                        && has(&["client_secret"])
                        && has(&["tenant_id", "authority_id"]))
            }
        };
    (!own).then(|| "credentials".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host(kind: StoreKind, pairs: &[(&str, &str)]) -> Option<String> {
        store_options_host_identity(kind, pairs.iter().copied())
    }

    #[test]
    fn stores_are_named_by_their_url() {
        for (url, kind) in [
            ("s3://b/p", Some(StoreKind::S3)),
            ("S3A://b", Some(StoreKind::S3)),
            ("gs://b/p", Some(StoreKind::Gcs)),
            ("az://c/p", Some(StoreKind::Azure)),
            ("abfss://c@a.dfs.core.windows.net/p", Some(StoreKind::Azure)),
            ("https://a.blob.core.windows.net/c", Some(StoreKind::Azure)),
            (
                "https://b.s3.us-east-1.amazonaws.com/k",
                Some(StoreKind::S3),
            ),
            ("https://storage.googleapis.com/b", Some(StoreKind::Gcs)),
            ("https://minio.example/b", None),
            ("/data/table", None),
            ("memory://", None),
        ] {
            assert_eq!(StoreKind::from_url(url), kind, "{url}");
        }
    }

    #[test]
    fn static_keys_are_the_configs_own_credentials() {
        assert_eq!(host(StoreKind::S3, &[]), Some("credentials".into()));
        assert_eq!(
            host(StoreKind::S3, &[("AWS_ACCESS_KEY_ID", "a")]),
            Some("credentials".into()),
            "an id without a secret falls through to the default chain"
        );
        assert_eq!(
            host(
                StoreKind::S3,
                &[("aws_access_key_id", "a"), ("aws_secret_access_key", " ")]
            ),
            Some("credentials".into())
        );
        assert_eq!(
            host(
                StoreKind::S3,
                &[("access_key_id", "a"), ("SECRET_ACCESS_KEY", "s")]
            ),
            None
        );
        assert_eq!(host(StoreKind::S3, &[("aws_skip_signature", "true")]), None);
        for key in [
            "aws_role_arn",
            "AWS_IAM_ROLE_ARN",
            "aws_s3_assume_role_arn",
            "aws_profile",
            "web_identity_token_file",
            "aws_container_credentials_full_uri",
            "aws_metadata_endpoint",
        ] {
            assert_eq!(
                host(
                    StoreKind::S3,
                    &[
                        ("aws_access_key_id", "a"),
                        ("aws_secret_access_key", "s"),
                        (key, "x")
                    ]
                ),
                Some(key.to_string()),
                "{key}"
            );
        }
        assert_eq!(host(StoreKind::Gcs, &[]), Some("credentials".into()));
        for key in [
            "google_service_account",
            "service_account_key",
            "application_credentials",
        ] {
            assert_eq!(host(StoreKind::Gcs, &[(key, "k")]), None, "{key}");
        }
        assert_eq!(
            host(StoreKind::Gcs, &[("google_skip_signature", "1")]),
            None
        );
        assert_eq!(
            host(StoreKind::Azure, &[("account_name", "a")]),
            Some("credentials".into())
        );
        for key in [
            "azure_storage_account_key",
            "access_key",
            "azure_storage_sas_token",
            "bearer_token",
        ] {
            assert_eq!(host(StoreKind::Azure, &[(key, "k")]), None, "{key}");
        }
        assert_eq!(
            host(StoreKind::Azure, &[("azure_storage_use_emulator", "true")]),
            None
        );
        assert_eq!(
            host(
                StoreKind::Azure,
                &[
                    ("azure_client_id", "c"),
                    ("azure_client_secret", "s"),
                    ("azure_tenant_id", "t")
                ]
            ),
            None
        );
        assert_eq!(
            host(StoreKind::Azure, &[("azure_client_id", "c")]),
            Some("credentials".into())
        );
        assert_eq!(
            host(
                StoreKind::Azure,
                &[("account_key", "k"), ("azure_use_azure_cli", "true")]
            ),
            Some("azure_use_azure_cli".into())
        );
        assert_eq!(
            host(
                StoreKind::Azure,
                &[("account_key", "k"), ("azure_federated_token_file", "/t")]
            ),
            Some("azure_federated_token_file".into())
        );
    }
}

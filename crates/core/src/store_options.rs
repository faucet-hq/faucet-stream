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
    /// The store a location URL names, read the way `object_store` reads it
    /// (`s3://`, `s3a://`, `gs://`, `az://`, `abfss://`, an `https://` Azure
    /// blob, DFS or Fabric host, an `https://` S3 or R2 host), `None` for
    /// anything else (a local path, `memory://`, a plain HTTP store).
    pub fn from_url(url: &str) -> Option<Self> {
        let parsed = url::Url::parse(url.trim()).ok()?;
        match parsed.scheme() {
            "s3" | "s3a" => Some(Self::S3),
            "gs" | "gcs" => Some(Self::Gcs),
            "az" | "adl" | "azure" | "abfs" | "abfss" => Some(Self::Azure),
            "https" => {
                let host = parsed.host_str()?;
                if host.ends_with("dfs.core.windows.net")
                    || host.ends_with("blob.core.windows.net")
                    || host.ends_with("dfs.fabric.microsoft.com")
                    || host.ends_with("blob.fabric.microsoft.com")
                {
                    Some(Self::Azure)
                } else if host.ends_with("amazonaws.com")
                    || host.ends_with("r2.cloudflarestorage.com")
                {
                    Some(Self::S3)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

fn normalized(key: &str, kind: StoreKind) -> String {
    stripped(&key.trim().to_ascii_lowercase(), kind)
}

/// `lower` without its store prefix, as the store's option parser reads it.
fn stripped(lower: &str, kind: StoreKind) -> String {
    let prefixes: &[&str] = match kind {
        StoreKind::S3 => &["aws_"],
        StoreKind::Gcs => &["google_"],
        StoreKind::Azure => &["azure_storage_", "azure_"],
    };
    prefixes
        .iter()
        .find_map(|p| lower.strip_prefix(p))
        .unwrap_or(lower)
        .to_string()
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
/// beside static keys: the library then resolves it through the host. So
/// does one of two keys the store reads as the same option (`skip_signature`
/// beside `aws_skip_signature`, or two spellings differing only in case):
/// which one the library applies last is not ours to know.
pub fn store_options_host_identity<I, K, V>(kind: StoreKind, options: I) -> Option<String>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut set = std::collections::BTreeMap::new();
    let mut exact = std::collections::BTreeSet::new();
    let mut read: std::collections::BTreeMap<String, (String, String)> = Default::default();
    let mut collision = None;
    for (k, v) in options {
        let (k, v) = (k.as_ref(), v.as_ref());
        let lower = k.to_ascii_lowercase();
        if read
            .insert(stripped(&lower, kind), (k.to_string(), v.to_string()))
            .is_some()
        {
            collision.get_or_insert_with(|| k.to_string());
        }
        if !v.trim().is_empty() {
            exact.insert(lower);
            set.insert(normalized(k, kind), (k.to_string(), v.to_string()));
        }
    }
    if collision.is_some() {
        return collision;
    }
    let given = |names: &[&str]| names.iter().any(|n| exact.contains(*n));
    let on = |name: &str| set.get(name).is_some_and(|(_, v)| truthy(v));
    let read_on = |name: &str| read.get(name).is_some_and(|(_, v)| truthy(v));
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
    let own = match kind {
        StoreKind::S3 => {
            given(&["aws_skip_signature", "skip_signature"]) && read_on("skip_signature")
                || given(&["aws_access_key_id", "access_key_id"])
                    && given(&["aws_secret_access_key", "secret_access_key"])
        }
        StoreKind::Gcs => {
            given(&["google_skip_signature", "skip_signature"]) && read_on("skip_signature")
                || given(&[
                    "google_service_account",
                    "service_account",
                    "google_service_account_path",
                    "service_account_path",
                    "google_service_account_key",
                    "service_account_key",
                    "google_application_credentials",
                    "application_credentials",
                ])
        }
        StoreKind::Azure => {
            given(&["azure_skip_signature", "skip_signature"]) && read_on("skip_signature")
                || given(&[
                    "azure_storage_account_key",
                    "azure_storage_access_key",
                    "azure_storage_master_key",
                    "master_key",
                    "account_key",
                    "access_key",
                    "azure_storage_sas_key",
                    "azure_storage_sas_token",
                    "sas_key",
                    "sas_token",
                    "azure_storage_token",
                    "bearer_token",
                    "token",
                    "azure_fabric_session_token",
                    "fabric_session_token",
                ])
                || given(&["azure_storage_use_emulator", "use_emulator"]) && read_on("use_emulator")
                || given(&["azure_storage_client_id", "azure_client_id", "client_id"])
                    && given(&[
                        "azure_storage_client_secret",
                        "azure_client_secret",
                        "client_secret",
                    ])
                    && given(&[
                        "azure_storage_tenant_id",
                        "azure_storage_authority_id",
                        "azure_tenant_id",
                        "azure_authority_id",
                        "tenant_id",
                        "authority_id",
                    ])
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
            ("https://storage.googleapis.com/b", None),
            ("http://b.s3.amazonaws.com/k", None),
            (
                "https://ws.dfs.fabric.microsoft.com/f",
                Some(StoreKind::Azure),
            ),
            (
                "https://ws.blob.fabric.microsoft.com/f",
                Some(StoreKind::Azure),
            ),
            (
                "https://acct.r2.cloudflarestorage.com/b",
                Some(StoreKind::S3),
            ),
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

    #[test]
    fn percent_encoded_store_hosts_are_recognised() {
        for (url, kind) in [
            (
                "https://acct.blob.core.windows%2Enet/container/p",
                Some(StoreKind::Azure),
            ),
            (
                "https://acct.blob.core.windows\u{3002}net/container/p",
                Some(StoreKind::Azure),
            ),
            ("https://b.s3.amazonaws%2ecom/k", Some(StoreKind::S3)),
            (
                "https://ACCT.DFS.CORE.WINDOWS.NET/f",
                Some(StoreKind::Azure),
            ),
            (
                "https://user@acct.blob.core.windows.net:443/c",
                Some(StoreKind::Azure),
            ),
            (
                "\u{1}https://acct.blob.core.windows.net/c",
                Some(StoreKind::Azure),
            ),
            ("https://minio%2Eexample/b", None),
        ] {
            assert_eq!(StoreKind::from_url(url), kind, "{url}");
        }
    }

    #[test]
    fn colliding_store_option_keys_are_refused_for_callers() {
        assert_eq!(
            host(
                StoreKind::S3,
                &[
                    ("aws_skip_signature", "false"),
                    (" skip_signature", "true"),
                    ("skip_signature ", "true")
                ]
            ),
            Some("credentials".into()),
            "padded spellings are not read by the store"
        );
        assert_eq!(
            host(
                StoreKind::S3,
                &[("aws_skip_signature", "false"), ("skip_signature", "true")]
            ),
            Some("skip_signature".into())
        );
        assert_eq!(
            host(
                StoreKind::S3,
                &[("SKIP_SIGNATURE", "false"), ("skip_signature", "true")]
            ),
            Some("skip_signature".into())
        );
        assert_eq!(
            host(
                StoreKind::Azure,
                &[
                    ("azure_storage_use_emulator", "true"),
                    ("use_emulator", "false")
                ]
            ),
            Some("use_emulator".into())
        );
        assert_eq!(
            host(
                StoreKind::S3,
                &[
                    ("aws_access_key_id", "a"),
                    ("access_key_id", "b"),
                    ("aws_secret_access_key", "s")
                ]
            ),
            Some("access_key_id".into())
        );
        assert_eq!(
            host(
                StoreKind::S3,
                &[("aws_access_key_id", "a"), ("aws_secret_access_key", "s")]
            ),
            None
        );
    }

    #[test]
    fn only_key_spellings_the_store_reads_count_as_its_own_credentials() {
        assert_eq!(
            host(
                StoreKind::S3,
                &[(" AWS_ACCESS_KEY_ID", "a"), ("AWS_SECRET_ACCESS_KEY ", "s")]
            ),
            Some("credentials".into())
        );
        assert_eq!(
            host(
                StoreKind::S3,
                &[("AWS_ACCESS_KEY_ID", "a"), ("AWS_SECRET_ACCESS_KEY", "s")]
            ),
            None
        );
        assert_eq!(
            host(StoreKind::Azure, &[("azure_account_key", "k")]),
            Some("credentials".into())
        );
        assert_eq!(
            host(StoreKind::Gcs, &[("gcp_service_account", "k")]),
            Some("credentials".into())
        );
        assert_eq!(
            host(StoreKind::S3, &[("skip_signature", "false")]),
            Some("credentials".into())
        );
    }
}

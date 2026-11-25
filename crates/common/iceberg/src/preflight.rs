//! A check that a table's storage accepts the credentials faucet will use,
//! run before the first write or read so a missing credential fails early and
//! names its cause.
//!
//! The check reads the table's current metadata file once (`HEAD`), through
//! the table's own `FileIO`: the same credential path a write takes. Only a
//! failure the store or the signer reports as a credential failure can refuse
//! the run; anything else (a timeout, a flaky network) is a warning, so a
//! deployment that signs with the host's own identity is never refused by a
//! slow probe.

use std::collections::HashMap;

use faucet_core::FaucetError;
use iceberg::table::Table;

use crate::config::{
    AccessDelegation, WarehouseScheme, storage_names_credentials, warehouse_scheme,
};

/// What the user configured for the table's storage.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct StorageExpectation<'a> {
    /// `catalog.properties` as written.
    pub user_props: &'a HashMap<String, String>,
    /// The catalog's `access_delegation` setting.
    pub access_delegation: Option<AccessDelegation>,
}

impl<'a> StorageExpectation<'a> {
    /// An expectation from `catalog.properties` and `access_delegation`.
    pub fn new(
        user_props: &'a HashMap<String, String>,
        access_delegation: Option<AccessDelegation>,
    ) -> Self {
        Self {
            user_props,
            access_delegation,
        }
    }
}

/// The outcome of [`check_table_storage`].
#[derive(Debug)]
#[non_exhaustive]
pub enum StorageVerdict {
    /// The storage answered, or the check does not apply.
    Ok,
    /// The probe failed in a way that does not prove a credential problem.
    Warn(String),
    /// The storage refused the credentials and the config explains why.
    Refuse(FaucetError),
}

/// What a probe failure says about the credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CredentialFailure {
    /// The store answered 403.
    PermissionDenied,
    /// No credential could be loaded, or the config is invalid.
    Unusable,
}

/// The credential failure in `err`'s source chain, if any: an `opendal`
/// `PermissionDenied` / `ConfigInvalid`, or a signer that could not load a
/// credential (`reqsign` `CredentialInvalid`, which opendal reports as
/// `Unexpected` "signing http request").
fn credential_failure(err: &(dyn std::error::Error + 'static)) -> Option<CredentialFailure> {
    let mut next = Some(err);
    let mut found = None;
    while let Some(e) = next {
        if let Some(o) = e.downcast_ref::<opendal::Error>() {
            match o.kind() {
                opendal::ErrorKind::PermissionDenied => {
                    return Some(CredentialFailure::PermissionDenied);
                }
                opendal::ErrorKind::ConfigInvalid => found = Some(CredentialFailure::Unusable),
                _ => {}
            }
        }
        if let Some(r) = e.downcast_ref::<reqsign_core::Error>()
            && r.kind() == reqsign_core::ErrorKind::CredentialInvalid
        {
            found = Some(CredentialFailure::Unusable);
        }
        next = e.source();
    }
    found
}

fn is_object_store(scheme: &str) -> Option<WarehouseScheme> {
    match warehouse_scheme(scheme) {
        s @ (WarehouseScheme::S3(_) | WarehouseScheme::Gcs) => Some(s),
        WarehouseScheme::Unsupported(s) if s == "s3n" => Some(WarehouseScheme::S3("s3n")),
        _ => None,
    }
}

fn set(props: &HashMap<String, String>, k: &str) -> bool {
    props.get(k).is_some_and(|v| !v.trim().is_empty())
}

/// Static S3 keys written by the user, together with a session token the user
/// did not write.
fn mixes_static_keys_with_a_vended_token(
    storage: &HashMap<String, String>,
    user: &HashMap<String, String>,
) -> bool {
    (set(user, "s3.access-key-id") || set(user, "s3.secret-access-key"))
        && set(storage, "s3.session-token")
        && user.get("s3.session-token") != storage.get("s3.session-token")
}

/// `err` and every source the text does not show yet.
fn chain_text(err: &(dyn std::error::Error + 'static)) -> String {
    let mut text = err.to_string();
    let mut next = err.source();
    while let Some(e) = next {
        let part = e.to_string();
        if !text.contains(&part) {
            text.push_str(": ");
            text.push_str(&part);
        }
        next = e.source();
    }
    text
}

/// The verdict for a probe of `table_name`'s storage. Pure.
fn verdict(
    table_name: &str,
    scheme: &WarehouseScheme,
    storage: &HashMap<String, String>,
    expect: &StorageExpectation<'_>,
    probe: Result<(), &(dyn std::error::Error + 'static)>,
) -> StorageVerdict {
    let Err(err) = probe else {
        return StorageVerdict::Ok;
    };
    let cause = chain_text(err);
    let warn = || {
        StorageVerdict::Warn(format!(
            "iceberg: reading the metadata file of table '{table_name}' failed ({cause}); the \
             first write will show whether the storage works"
        ))
    };
    let Some(failure) = credential_failure(err) else {
        return warn();
    };
    if failure == CredentialFailure::PermissionDenied
        && mixes_static_keys_with_a_vended_token(storage, expect.user_props)
    {
        return StorageVerdict::Refuse(FaucetError::Config(format!(
            "iceberg: catalog.properties sets s3.access-key-id / s3.secret-access-key, and the \
             catalog hands out a session token for table '{table_name}'. The store refused that \
             mix ({cause}). Remove the static keys from catalog.properties to use the catalog's \
             credentials, or turn credential vending off for this warehouse in the catalog."
        )));
    }
    if storage_names_credentials(scheme, storage) {
        return warn();
    }
    let returned = match expect.access_delegation {
        Some(_) => "the catalog returned no storage credentials although access_delegation: \
                    vended_credentials asks for them"
            .to_string(),
        None => "the catalog returned none in the table config".to_string(),
    };
    let keys = match scheme {
        WarehouseScheme::Gcs => "gcs.credentials-json",
        _ => "s3.access-key-id and s3.secret-access-key",
    };
    StorageVerdict::Refuse(FaucetError::Config(format!(
        "iceberg: no usable storage credentials for table '{table_name}': {returned}, \
         catalog.properties names none, and reading the table's metadata file failed ({cause}). \
         Fix one of: (1) have the catalog hand out credentials (a REST catalog that does so only \
         on request needs catalog.access_delegation: vended_credentials); (2) set {keys} in \
         catalog.properties. If this host has its own identity (an instance or workload role), \
         check that it can read the table location."
    )))
}

/// The table's name as `namespace.table`.
pub fn table_name(table: &Table) -> String {
    let id = table.identifier();
    let mut parts: Vec<String> = id.namespace().clone().inner();
    parts.push(id.name().to_string());
    parts.join(".")
}

/// Probe `table`'s storage with the credentials its `FileIO` holds.
///
/// Returns [`StorageVerdict::Ok`] without I/O for a table on a store the
/// check does not cover (anything but `s3://`, `s3a://`, `s3n://`, `gs://`) or
/// a handle with no metadata location. Run it on the task of the run: the
/// storage is built on first use and follows that task's credential rules.
pub async fn check_table_storage(table: &Table, expect: &StorageExpectation<'_>) -> StorageVerdict {
    let Some(location) = table.metadata_location() else {
        return StorageVerdict::Ok;
    };
    let Some(scheme) = is_object_store(table.metadata().location()) else {
        return StorageVerdict::Ok;
    };
    let storage = table.file_io().config().props().clone();
    let probe = table.file_io().exists(location).await;
    verdict(
        &table_name(table),
        &scheme,
        &storage,
        expect,
        probe
            .as_ref()
            .map(|_| ())
            .map_err(|e| e as &(dyn std::error::Error + 'static)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn opendal_err(kind: opendal::ErrorKind) -> iceberg::Error {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Failure in doing io operation",
        )
        .with_source(opendal::Error::new(kind, "store says no"))
    }

    fn signer_err() -> iceberg::Error {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Failure in doing io operation",
        )
        .with_source(
            opendal::Error::new(opendal::ErrorKind::Unexpected, "signing http request").set_source(
                reqsign_core::Error::credential_invalid("failed to load signing credential"),
            ),
        )
    }

    #[test]
    fn credential_failures_are_found_in_the_source_chain() {
        let cases = [
            (
                opendal_err(opendal::ErrorKind::PermissionDenied),
                Some(CredentialFailure::PermissionDenied),
            ),
            (
                opendal_err(opendal::ErrorKind::ConfigInvalid),
                Some(CredentialFailure::Unusable),
            ),
            (signer_err(), Some(CredentialFailure::Unusable)),
            (opendal_err(opendal::ErrorKind::Unexpected), None),
            (opendal_err(opendal::ErrorKind::RateLimited), None),
            (
                iceberg::Error::new(iceberg::ErrorKind::Unexpected, "timed out"),
                None,
            ),
        ];
        for (err, want) in cases {
            assert_eq!(credential_failure(&err), want, "{err}");
        }
    }

    fn run(
        scheme: &str,
        storage: &[(&str, &str)],
        user: &[(&str, &str)],
        delegation: Option<AccessDelegation>,
        err: Option<iceberg::Error>,
    ) -> StorageVerdict {
        let user = props(user);
        let expect = StorageExpectation::new(&user, delegation);
        let scheme = is_object_store(scheme).expect("object store");
        verdict(
            "ns.t",
            &scheme,
            &props(storage),
            &expect,
            match &err {
                Some(e) => Err(e as &(dyn std::error::Error + 'static)),
                None => Ok(()),
            },
        )
    }

    const KEYS: [(&str, &str); 2] = [("s3.access-key-id", "K"), ("s3.secret-access-key", "S")];
    const VENDED: [(&str, &str); 3] = [
        ("s3.access-key-id", "VK"),
        ("s3.secret-access-key", "VS"),
        ("s3.session-token", "VT"),
    ];

    #[test]
    fn the_verdict_refuses_only_explained_credential_failures() {
        assert!(matches!(
            run("s3://b/t", &[], &[], None, None),
            StorageVerdict::Ok
        ));
        let refused = |v: StorageVerdict| match v {
            StorageVerdict::Refuse(e) => e.to_string(),
            other => panic!("expected a refusal, got {other:?}"),
        };
        let warned = |v: StorageVerdict| match v {
            StorageVerdict::Warn(w) => w,
            other => panic!("expected a warning, got {other:?}"),
        };

        let none = refused(run("s3://b/t", &[], &[], None, Some(signer_err())));
        assert!(none.contains("the catalog returned none"), "{none}");
        assert!(
            none.contains("access_delegation: vended_credentials"),
            "{none}"
        );
        assert!(
            none.contains("s3.access-key-id and s3.secret-access-key"),
            "{none}"
        );
        let asked = refused(run(
            "s3a://b/t",
            &[],
            &[],
            Some(AccessDelegation::VendedCredentials),
            Some(opendal_err(opendal::ErrorKind::PermissionDenied)),
        ));
        assert!(asked.contains("although access_delegation"), "{asked}");
        let gcs = refused(run(
            "gs://b/t",
            &[],
            &[],
            None,
            Some(opendal_err(opendal::ErrorKind::ConfigInvalid)),
        ));
        assert!(gcs.contains("gcs.credentials-json"), "{gcs}");

        let mut mixed = KEYS.to_vec();
        mixed.push(("s3.session-token", "VT"));
        let mix = refused(run(
            "s3://b/t",
            &mixed,
            &KEYS,
            None,
            Some(opendal_err(opendal::ErrorKind::PermissionDenied)),
        ));
        assert!(mix.contains("Remove the static keys"), "{mix}");
        warned(run("s3://b/t", &mixed, &KEYS, None, Some(signer_err())));

        let named = warned(run(
            "s3://b/t",
            &VENDED,
            &[],
            None,
            Some(opendal_err(opendal::ErrorKind::PermissionDenied)),
        ));
        assert!(named.contains("first write will show"), "{named}");
        warned(run(
            "s3://b/t",
            &[],
            &[],
            None,
            Some(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "timed out",
            )),
        ));
        warned(run(
            "s3n://b/t",
            &[],
            &[],
            None,
            Some(opendal_err(opendal::ErrorKind::Unexpected)),
        ));
    }

    #[test]
    fn only_object_store_tables_are_probed() {
        for s in ["s3://b", "s3a://b", "s3n://b", "gs://b"] {
            assert!(is_object_store(s).is_some(), "{s}");
        }
        for s in ["file:///tmp/w", "/tmp/w", "abfss://c@a/w", "oss://b"] {
            assert!(is_object_store(s).is_none(), "{s}");
        }
    }

    #[tokio::test]
    async fn a_table_without_a_metadata_location_or_on_local_disk_is_not_probed() {
        use iceberg::io::FileIO;
        use iceberg::spec::{NestedField, PrimitiveType, Schema, TableMetadataBuilder, Type};
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let build = |location: &str, metadata_location: Option<&str>| {
            let metadata = TableMetadataBuilder::from_table_creation(
                iceberg::TableCreation::builder()
                    .name("t".to_string())
                    .location(location.to_string())
                    .schema(schema.clone())
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            let builder = Table::builder()
                .identifier(iceberg::TableIdent::from_strs(["ns", "t"]).unwrap())
                .metadata(metadata)
                .file_io(FileIO::new_with_fs())
                .runtime(iceberg::Runtime::try_current().unwrap());
            match metadata_location {
                Some(l) => builder.metadata_location(l.to_string()).build().unwrap(),
                None => builder.build().unwrap(),
            }
        };
        let user = HashMap::new();
        let expect = StorageExpectation::new(&user, None);
        let unlocated = build("s3://b/ns/t", None);
        assert_eq!(table_name(&unlocated), "ns.t");
        assert!(matches!(
            check_table_storage(&unlocated, &expect).await,
            StorageVerdict::Ok
        ));
        let local = build("/tmp/wh/ns/t", Some("/tmp/wh/ns/t/metadata/v1.json"));
        assert!(matches!(
            check_table_storage(&local, &expect).await,
            StorageVerdict::Ok
        ));
    }
}

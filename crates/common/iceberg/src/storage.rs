//! Taking fresh storage credentials for a table that is already loaded.
//!
//! A catalog that vends storage credentials hands them out with the table
//! load, and they expire. A connector that keeps working on one table reloads
//! it and takes only the `FileIO` of the reload: the metadata it planned or
//! wrote with stays, so a catalog that lags behind can never move the schema
//! or the snapshot under it.

use std::collections::HashMap;

use iceberg::ErrorKind;
use iceberg::table::Table;

/// `metadata_of`'s metadata and metadata location with `storage_of`'s `FileIO`.
///
/// The rebuilt handle starts with an empty manifest cache, has no KMS client
/// and runs on the current tokio runtime, as the catalog's own handles do.
pub fn with_storage_of(metadata_of: &Table, storage_of: &Table) -> Result<Table, iceberg::Error> {
    let builder = Table::builder()
        .identifier(metadata_of.identifier().clone())
        .metadata(metadata_of.metadata_ref())
        .file_io(storage_of.file_io().clone())
        .runtime(iceberg::Runtime::try_current()?);
    match metadata_of.metadata_location() {
        Some(location) => builder.metadata_location(location.to_string()).build(),
        None => builder.build(),
    }
}

/// What a reload for fresh storage credentials came to.
#[derive(Debug)]
pub enum StorageReload {
    /// `table` with the reload's `FileIO`.
    Fresh(Table),
    /// The catalog now holds another table under this name (its UUID differs).
    Replaced,
    /// The table or its namespace is gone from the catalog.
    Gone,
    /// The reload failed for another reason; the text says why.
    Failed(String),
}

/// Judge `loaded` (a fresh `load_table` of `table`'s identifier) and, when it
/// is the same table, rebuild `table` on its storage.
pub fn storage_reload(table: &Table, loaded: Result<Table, iceberg::Error>) -> StorageReload {
    match loaded {
        Ok(fresh) if fresh.metadata().uuid() != table.metadata().uuid() => StorageReload::Replaced,
        Ok(fresh) => match with_storage_of(table, &fresh) {
            Ok(refreshed) => StorageReload::Fresh(refreshed),
            Err(e) => StorageReload::Failed(e.to_string()),
        },
        Err(e)
            if matches!(
                e.kind(),
                ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
            ) =>
        {
            StorageReload::Gone
        }
        Err(e) => StorageReload::Failed(e.to_string()),
    }
}

/// When the vended credentials in `storage` (a table's storage properties)
/// stop working, in milliseconds since the Unix epoch: the earliest of the
/// expiry properties a catalog sent with them, if it sent any.
pub fn credential_expiry_ms(storage: &HashMap<String, String>) -> Option<i64> {
    storage
        .iter()
        .filter(|(k, _)| {
            matches!(
                k.as_str(),
                "s3.session-token-expires-at-ms" | "gcs.oauth2.token-expires-at"
            ) || k.starts_with("adls.sas-token-expires-at-ms")
        })
        .filter_map(|(_, v)| v.trim().parse::<i64>().ok())
        .min()
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

    #[test]
    fn the_earliest_known_expiry_wins_and_text_that_is_no_number_is_ignored() {
        assert_eq!(
            credential_expiry_ms(&props(&[("s3.session-token", "t")])),
            None
        );
        assert_eq!(
            credential_expiry_ms(&props(&[
                ("s3.session-token-expires-at-ms", " 2000 "),
                ("gcs.oauth2.token-expires-at", "1500"),
                ("adls.sas-token-expires-at-ms.account", "1800"),
                ("s3.other", "1"),
            ])),
            Some(1500)
        );
        assert_eq!(
            credential_expiry_ms(&props(&[("s3.session-token-expires-at-ms", "soon")])),
            None
        );
    }
}

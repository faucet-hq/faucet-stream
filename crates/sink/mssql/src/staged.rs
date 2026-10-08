//! Staged bulk load (#528): stage a page to Azure Blob / ADLS, then have SQL
//! Server / Synapse pull it with `COPY INTO … FROM '<url>' WITH (FILE_TYPE=…)`.
//!
//! Pure SQL/URL generators (unit-tested); the execution shim in [`crate::sink`]
//! uploads via `faucet_core::staging::StageUploader` and runs the statement over
//! the tiberius pool. Only enabled with the `staging` feature.
//!
//! **Azure only.** `COPY INTO` reads Azure Blob / ADLS Gen2 (not S3/GCS), so a
//! MSSQL staging `location` must be `az://…`. `COPY INTO` maps CSV fields by
//! *position*, so the statement names the target columns in the staged CSV's
//! own header order (the union of the page's keys); a key with no column
//! fails the load rather than landing in the wrong column.

use faucet_core::FaucetError;
use faucet_core::staging::{StageUploader, StagedFile, StagingFormat, StagingScheme};
use serde_json::Value;

/// Stage one page to `uploader`'s object store and build the `COPY INTO`
/// statement for it. The upload + URL derivation + SQL build are all here (and
/// unit-tested against an in-memory store); the caller runs the returned SQL
/// over the tiberius pool. `table_quoted` is the bracket-quoted target;
/// `scope`/`run_id`/`seq` shape the staged object key.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_staged_copy_sql(
    uploader: &StageUploader,
    table_quoted: &str,
    scope: &str,
    run_id: &str,
    seq: usize,
    records: &[Value],
    staging: &crate::config::MssqlStagingConfig,
) -> Result<(StagedFile, String), FaucetError> {
    let loc = uploader.location().clone();
    let staged = uploader
        .stage_page(&staging.spec, scope, run_id, seq, records, None)
        .await?;
    let url = staged_azure_url(
        loc.scheme,
        &loc.bucket,
        &staged.key,
        staging.storage_account.as_deref(),
        staging.endpoint.as_deref(),
    )?;
    // The staged CSV carries a header row → skip it with FIRSTROW = 2.
    let sql = mssql_copy_into_sql(
        table_quoted,
        &csv_columns(records),
        &url,
        staging.spec.format,
        staging.sas_token.as_deref(),
        2,
    )?;
    Ok((staged, sql))
}

/// Map a staging file format to the `COPY INTO` `FILE_TYPE`. Only CSV is
/// produced by the shared serializer (Parquet needs Arrow).
pub fn mssql_file_type(format: StagingFormat) -> Result<&'static str, FaucetError> {
    match format {
        StagingFormat::Csv => Ok("CSV"),
        StagingFormat::Jsonl => Err(FaucetError::Config(
            "mssql staging: `COPY INTO` has no JSONL file type — use `format: csv`".into(),
        )),
        StagingFormat::Parquet => Err(FaucetError::Config(
            "mssql staging: parquet is not produced by the shared serializer — use `format: csv`"
                .into(),
        )),
    }
}

/// Derive the HTTPS Azure Blob URL `COPY INTO` fetches, from a staged object's
/// container + key. Needs the storage `account` (the `az://` URI does not carry
/// it) unless an explicit `endpoint` base (host, no scheme) is given.
pub fn staged_azure_url(
    scheme: StagingScheme,
    container: &str,
    key: &str,
    account: Option<&str>,
    endpoint: Option<&str>,
) -> Result<String, FaucetError> {
    if !matches!(scheme, StagingScheme::Azure) {
        return Err(FaucetError::Config(
            "mssql staging: `COPY INTO` reads Azure Blob / ADLS only — stage to `az://…`".into(),
        ));
    }
    if let Some(ep) = endpoint {
        let ep = ep
            .trim_end_matches('/')
            .trim_start_matches("https://")
            .trim_start_matches("http://");
        return Ok(format!("https://{ep}/{container}/{key}"));
    }
    let account = account.filter(|a| !a.is_empty()).ok_or_else(|| {
        FaucetError::Config(
            "mssql staging: azure needs `storage_account` (or `endpoint`) to build the COPY INTO URL"
                .into(),
        )
    })?;
    Ok(format!(
        "https://{account}.blob.core.windows.net/{container}/{key}"
    ))
}

/// The staged CSV's header: the union of the records' top-level keys in
/// first-seen order — the order `faucet_core::staging` writes them.
pub(crate) fn csv_columns(records: &[Value]) -> Vec<String> {
    let mut columns: Vec<String> = Vec::new();
    for key in records
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|m| m.keys())
    {
        if !columns.contains(key) {
            columns.push(key.clone());
        }
    }
    columns
}

/// Build the `COPY INTO <table> FROM '<url>' WITH (…)` statement for one staged
/// object. `table` must already be identifier-quoted by the caller. `first_row`
/// is the 1-based first data row (2 when the CSV carries a header). A SAS token,
/// when given, is passed as a Shared Access Signature credential; otherwise the
/// server's managed identity / configured access is used. String literals are
/// single-quote-escaped.
pub fn mssql_copy_into_sql(
    quoted_table: &str,
    columns: &[String],
    url: &str,
    format: StagingFormat,
    sas_token: Option<&str>,
    first_row: usize,
) -> Result<String, FaucetError> {
    let file_type = mssql_file_type(format)?;
    let mut opts = vec![
        format!("FILE_TYPE = '{file_type}'"),
        format!("FIRSTROW = {first_row}"),
    ];
    if let Some(sas) = sas_token {
        opts.push(format!(
            "CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = '{}')",
            sql_quote(sas)
        ));
    }
    let column_list = if columns.is_empty() {
        String::new()
    } else {
        let quoted = columns
            .iter()
            .map(|c| faucet_common_mssql::quote_ident_mssql(c))
            .collect::<Result<Vec<_>, _>>()?;
        format!(" ({})", quoted.join(", "))
    };
    Ok(format!(
        "COPY INTO {quoted_table}{column_list} FROM '{}' WITH (\n    {}\n)",
        sql_quote(url),
        opts.join(",\n    ")
    ))
}

/// Escape single quotes for a T-SQL string literal (`''`).
fn sql_quote(s: &str) -> String {
    s.replace('\'', "''")
}

/// Per-sink run id for staged-object keys, so parts from different runs never
/// collide in the staging prefix. Nanosecond wall-clock is unique enough per
/// process; a monotonic sequence disambiguates within a run.
pub(crate) fn new_stage_run_id() -> String {
    format!(
        "run-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_run_id_is_prefixed_and_unique() {
        let a = new_stage_run_id();
        assert!(a.starts_with("run-"));
        assert!(a.len() > 4);
    }

    #[test]
    fn file_type_mapping() {
        assert_eq!(mssql_file_type(StagingFormat::Csv).unwrap(), "CSV");
        assert!(mssql_file_type(StagingFormat::Jsonl).is_err());
        assert!(mssql_file_type(StagingFormat::Parquet).is_err());
    }

    #[test]
    fn azure_url_from_account() {
        assert_eq!(
            staged_azure_url(
                StagingScheme::Azure,
                "cont",
                "p/part-00001.csv",
                Some("acct"),
                None
            )
            .unwrap(),
            "https://acct.blob.core.windows.net/cont/p/part-00001.csv"
        );
    }

    #[test]
    fn azure_url_endpoint_override() {
        assert_eq!(
            staged_azure_url(
                StagingScheme::Azure,
                "cont",
                "k",
                None,
                Some("https://azurite:10000/devacct")
            )
            .unwrap(),
            "https://azurite:10000/devacct/cont/k"
        );
    }

    #[test]
    fn azure_url_requires_account() {
        assert!(staged_azure_url(StagingScheme::Azure, "c", "k", None, None).is_err());
    }

    #[test]
    fn non_azure_scheme_rejected() {
        assert!(staged_azure_url(StagingScheme::S3, "c", "k", Some("a"), None).is_err());
        assert!(staged_azure_url(StagingScheme::Gcs, "c", "k", Some("a"), None).is_err());
    }

    #[test]
    fn copy_into_with_sas() {
        let sql = mssql_copy_into_sql(
            "[dbo].[events]",
            &[],
            "https://acct.blob.core.windows.net/c/part-00001.csv",
            StagingFormat::Csv,
            Some("sv=2022&sig=ab'c"),
            2,
        )
        .unwrap();
        assert!(sql.starts_with("COPY INTO [dbo].[events] FROM 'https://acct.blob.core.windows.net/c/part-00001.csv' WITH ("));
        assert!(sql.contains("FILE_TYPE = 'CSV'"));
        assert!(sql.contains("FIRSTROW = 2"));
        assert!(sql.contains("IDENTITY = 'Shared Access Signature'"));
        assert!(sql.contains("SECRET = 'sv=2022&sig=ab''c'")); // '' escaped
    }

    #[test]
    fn copy_into_without_credential() {
        let sql = mssql_copy_into_sql(
            "[t]",
            &["b".to_string(), "a".to_string()],
            "https://a.blob.core.windows.net/c/k.csv",
            StagingFormat::Csv,
            None,
            2,
        )
        .unwrap();
        assert!(!sql.contains("CREDENTIAL"));
        assert!(sql.starts_with("COPY INTO [t] ([b], [a]) FROM"), "{sql}");
        assert!(sql.contains("FILE_TYPE = 'CSV'"));
    }

    #[test]
    fn copy_into_rejects_non_csv() {
        assert!(mssql_copy_into_sql("[t]", &[], "u", StagingFormat::Jsonl, None, 2).is_err());
    }

    // Covers the staged upload + Azure URL + COPY INTO build against an
    // in-memory object store (only the tiberius execution stays untested).
    #[tokio::test]
    async fn build_staged_copy_sql_uploads_and_builds() {
        use faucet_core::staging::{StageUploader, StagingLocation};
        use std::sync::Arc;

        let store = Arc::new(object_store::memory::InMemory::new());
        let loc = StagingLocation::parse("az://container/stage").unwrap();
        let uploader = StageUploader::new(store, loc);
        let staging: crate::config::MssqlStagingConfig =
            serde_json::from_value(serde_json::json!({
                "location": "az://container/stage",
                "format": "csv",
                "storage_account": "acct",
                "sas_token": "sv=2022&sig=abc",
            }))
            .unwrap();
        let recs = vec![
            serde_json::json!({"id": 1}),
            serde_json::json!({"id": 2, "name": "x"}),
            serde_json::json!(3),
        ];
        assert_eq!(
            csv_columns(&recs),
            vec!["id".to_string(), "name".to_string()]
        );
        let recs = &recs[..2];

        let (staged, sql) = build_staged_copy_sql(
            &uploader,
            "[dbo].[events]",
            "dbo.events",
            "run-1",
            0,
            recs,
            &staging,
        )
        .await
        .unwrap();

        assert_eq!(staged.rows, 2);
        assert!(sql.starts_with(
            "COPY INTO [dbo].[events] ([id], [name]) FROM 'https://acct.blob.core.windows.net/container/"
        ));
        assert!(sql.contains("FILE_TYPE = 'CSV'"));
        assert!(sql.contains("FIRSTROW = 2"));
        assert!(sql.contains("Shared Access Signature"));
    }
}

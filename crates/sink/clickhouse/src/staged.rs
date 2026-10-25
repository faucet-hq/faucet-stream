//! Staged bulk load (#528): stage a page to an object store, then have the
//! ClickHouse **server** pull it with the `s3()` / `gcs()` table function —
//! `INSERT INTO <t> SELECT * FROM s3('<https-url>', '<key>', '<secret>', '<Format>')`.
//!
//! Everything here is a **pure** SQL/URL generator (unit-tested); the execution
//! shim in [`crate::sink`] uploads via `faucet_core::staging::StageUploader` and
//! sends the generated statement over the same HTTP interface as an ordinary
//! insert. Only enabled with the `staging` feature.

use faucet_core::FaucetError;
use faucet_core::staging::{StagingFormat, StagingScheme};

/// Map a staging file format to the ClickHouse `FORMAT` name.
pub fn clickhouse_format(format: StagingFormat) -> Result<&'static str, FaucetError> {
    match format {
        StagingFormat::Jsonl => Ok("JSONEachRow"),
        StagingFormat::Csv => Ok("CSVWithNames"),
        // Parquet needs Arrow serialization (not produced by the shared
        // `serialize_records`); the ClickHouse staged path sticks to text formats.
        StagingFormat::Parquet => Err(FaucetError::Config(
            "clickhouse staging: `format: parquet` is not supported — use jsonl or csv".into(),
        )),
    }
}

/// Derive the HTTPS URL the ClickHouse server fetches, from a staged object's
/// `scheme` / `bucket` / `key`. S3 uses the virtual-hosted endpoint (with an
/// optional region); GCS uses the interoperability XML endpoint. An explicit
/// `endpoint` override (host or host+path base) wins and renders path-style —
/// for S3-compatible stores / MinIO; its `http://` or `https://` scheme is
/// kept (`https://` when it has none). Each key segment is percent-encoded,
/// so URL and glob characters in a table name or prefix stay literal.
pub fn staged_https_url(
    scheme: StagingScheme,
    bucket: &str,
    key: &str,
    region: Option<&str>,
    endpoint: Option<&str>,
) -> Result<String, FaucetError> {
    let key = encode_key(key);
    if let Some(ep) = endpoint {
        let ep = ep.trim_end_matches('/');
        let (scheme, host) = match ep.split_once("://") {
            Some((s, rest)) if s.eq_ignore_ascii_case("http") => ("http", rest),
            Some((s, rest)) if s.eq_ignore_ascii_case("https") => ("https", rest),
            Some((s, _)) => {
                return Err(FaucetError::Config(format!(
                    "clickhouse staging: endpoint scheme `{s}://` is not supported — use \
                     http:// or https://"
                )));
            }
            None => ("https", ep),
        };
        return Ok(format!("{scheme}://{host}/{bucket}/{key}"));
    }
    match scheme {
        StagingScheme::S3 => Ok(match region {
            Some(r) if !r.is_empty() => format!("https://{bucket}.s3.{r}.amazonaws.com/{key}"),
            _ => format!("https://{bucket}.s3.amazonaws.com/{key}"),
        }),
        StagingScheme::Gcs => Ok(format!("https://storage.googleapis.com/{bucket}/{key}")),
        StagingScheme::Azure => Err(FaucetError::Config(
            "clickhouse staging: azure blob is not supported by the s3()/gcs() path — \
             stage to s3:// or gs://"
                .into(),
        )),
    }
}

/// Percent-encode every byte of each `/`-separated key segment except the
/// RFC 3986 unreserved characters.
fn encode_key(key: &str) -> String {
    key.split('/')
        .map(|segment| {
            segment
                .bytes()
                .map(|b| match b {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                        (b as char).to_string()
                    }
                    _ => format!("%{b:02X}"),
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Build the `INSERT INTO <table> SELECT * FROM <fn>('<url>'[, '<key>','<secret>'], '<Format>')`
/// statement for one staged object. `table` must already be identifier-quoted
/// by the caller (`quote_table`). Credentials are optional (omitted → the
/// server's own IAM / configured access is used). URL and credentials are
/// single-quote-escaped for the SQL string literal.
pub fn clickhouse_stage_insert_sql(
    quoted_table: &str,
    scheme: StagingScheme,
    url: &str,
    creds: Option<(&str, &str)>,
    format: StagingFormat,
) -> Result<String, FaucetError> {
    let func = match scheme {
        StagingScheme::Gcs => "gcs",
        _ => "s3",
    };
    let fmt = clickhouse_format(format)?;
    let url = sql_quote(url);
    let fmt_q = sql_quote(fmt);
    let args = match creds {
        Some((k, s)) => format!("'{url}', '{}', '{}', '{fmt_q}'", sql_quote(k), sql_quote(s)),
        None => format!("'{url}', '{fmt_q}'"),
    };
    Ok(format!(
        "INSERT INTO {quoted_table} SELECT * FROM {func}({args})"
    ))
}

/// Escape single quotes and backslashes for a ClickHouse SQL string literal.
fn sql_quote(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Per-sink run id for staged-object keys, so parts from different runs never
/// collide in the staging prefix.
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
    fn stage_run_id_is_prefixed() {
        assert!(new_stage_run_id().starts_with("run-"));
    }

    #[test]
    fn format_mapping() {
        assert_eq!(
            clickhouse_format(StagingFormat::Jsonl).unwrap(),
            "JSONEachRow"
        );
        assert_eq!(
            clickhouse_format(StagingFormat::Csv).unwrap(),
            "CSVWithNames"
        );
        assert!(clickhouse_format(StagingFormat::Parquet).is_err());
    }

    #[test]
    fn s3_url_derivation() {
        assert_eq!(
            staged_https_url(StagingScheme::S3, "buk", "p/part-00001.jsonl", None, None).unwrap(),
            "https://buk.s3.amazonaws.com/p/part-00001.jsonl"
        );
        assert_eq!(
            staged_https_url(StagingScheme::S3, "buk", "k", Some("us-west-2"), None).unwrap(),
            "https://buk.s3.us-west-2.amazonaws.com/k"
        );
    }

    #[test]
    fn gcs_url_derivation() {
        assert_eq!(
            staged_https_url(StagingScheme::Gcs, "buk", "k", None, None).unwrap(),
            "https://storage.googleapis.com/buk/k"
        );
    }

    #[test]
    fn endpoint_override_is_path_style() {
        assert_eq!(
            staged_https_url(
                StagingScheme::S3,
                "buk",
                "k",
                None,
                Some("https://minio.local:9000")
            )
            .unwrap(),
            "https://minio.local:9000/buk/k"
        );
        let url = |ep| staged_https_url(StagingScheme::S3, "buk", "k", None, Some(ep)).unwrap();
        assert_eq!(url("http://minio:9000/"), "http://minio:9000/buk/k");
        assert_eq!(url("HTTP://minio:9000"), "http://minio:9000/buk/k");
        assert_eq!(url("minio:9000"), "https://minio:9000/buk/k");
        assert!(
            staged_https_url(StagingScheme::S3, "b", "k", None, Some("ftp://h"))
                .unwrap_err()
                .to_string()
                .contains("ftp://")
        );
    }

    #[test]
    fn key_segments_are_percent_encoded() {
        assert_eq!(
            staged_https_url(
                StagingScheme::S3,
                "buk",
                "p x/t#1?/{a,b}*%.jsonl",
                None,
                None
            )
            .unwrap(),
            "https://buk.s3.amazonaws.com/p%20x/t%231%3F/%7Ba%2Cb%7D%2A%25.jsonl"
        );
        assert_eq!(encode_key("a-b_c.d~/é"), "a-b_c.d~/%C3%A9");
    }

    #[test]
    fn azure_unsupported() {
        assert!(staged_https_url(StagingScheme::Azure, "b", "k", None, None).is_err());
    }

    #[test]
    fn insert_sql_with_and_without_creds() {
        let with = clickhouse_stage_insert_sql(
            "\"db\".\"t\"",
            StagingScheme::S3,
            "https://buk.s3.amazonaws.com/p/part-00001.jsonl",
            Some(("AKIA", "sec")),
            StagingFormat::Jsonl,
        )
        .unwrap();
        assert_eq!(
            with,
            "INSERT INTO \"db\".\"t\" SELECT * FROM s3('https://buk.s3.amazonaws.com/p/part-00001.jsonl', 'AKIA', 'sec', 'JSONEachRow')"
        );
        let without = clickhouse_stage_insert_sql(
            "\"t\"",
            StagingScheme::S3,
            "https://buk.s3.amazonaws.com/k",
            None,
            StagingFormat::Csv,
        )
        .unwrap();
        assert_eq!(
            without,
            "INSERT INTO \"t\" SELECT * FROM s3('https://buk.s3.amazonaws.com/k', 'CSVWithNames')"
        );
    }

    #[test]
    fn gcs_uses_gcs_function() {
        let sql = clickhouse_stage_insert_sql(
            "\"t\"",
            StagingScheme::Gcs,
            "https://storage.googleapis.com/buk/k",
            Some(("hmac", "sk")),
            StagingFormat::Jsonl,
        )
        .unwrap();
        assert!(sql.starts_with("INSERT INTO \"t\" SELECT * FROM gcs("));
    }

    #[test]
    fn credentials_are_escaped() {
        let sql = clickhouse_stage_insert_sql(
            "\"t\"",
            StagingScheme::S3,
            "https://buk.s3.amazonaws.com/k",
            Some(("ab'c", "x")),
            StagingFormat::Jsonl,
        )
        .unwrap();
        assert!(sql.contains("'ab\\'c'"));
    }
}

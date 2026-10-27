//! Shared MSSQL connection configuration and pure parsing/quoting helpers.
//!
//! No I/O lives here — connecting and pooling are in [`crate::pool`]. This
//! module only holds the serde config types and the pure logic the source and
//! sink both need (URL parsing, identifier quoting, the parameter ceiling).

use std::path::PathBuf;

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// MSSQL's hard ceiling on bind parameters per request. A multi-row `INSERT`
/// binds `rows * columns` parameters, so the sink auto-splits a batch into
/// multiple statements whenever it would exceed this.
pub const PARAM_LIMIT: usize = 2100;

/// Shared connection configuration for the MSSQL source and sink.
///
/// Exactly one of [`connection_url`](Self::connection_url) or
/// [`connection_string`](Self::connection_string) must be set. The
/// `connection_url` form is parsed by faucet (host/port/database/credentials)
/// and the [`tls`](Self::tls) block governs encryption. The `connection_string`
/// form is an ADO.NET-style string handed straight to `tiberius`, with the
/// `tls` block applied on top.
///
/// `max_connections` and `statement_timeout_secs` are intentionally *not* here —
/// they default differently for the source (10 / 300) and sink (5 / 300), so
/// each end owns them and passes them to [`crate::pool::build_pool`] /
/// [`crate::pool::with_statement_timeout`].
#[derive(Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct MssqlConnectionConfig {
    /// `mssql://user:pass@host:1433/database` URL form. Mutually exclusive with
    /// [`connection_string`](Self::connection_string).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-faucet-resource" = "url"))]
    pub connection_url: Option<String>,
    /// ADO.NET-style connection string handed straight to `tiberius`, e.g.
    /// `Server=tcp:host,1433;Database=db;User Id=sa;Password=...;`. Mutually
    /// exclusive with [`connection_url`](Self::connection_url).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-faucet-resource" = "url"))]
    pub connection_string: Option<String>,
    /// TLS / encryption settings. Defaults to [`MssqlTlsMode::Prefer`].
    #[serde(default)]
    pub tls: MssqlTls,
}

/// TLS configuration for an MSSQL connection.
///
/// Matches the YAML shape `tls: { type: <mode>, ca_cert_path: <path> }`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq)]
pub struct MssqlTls {
    /// Encryption mode. Defaults to [`MssqlTlsMode::Prefer`].
    #[serde(rename = "type", default)]
    pub mode: MssqlTlsMode,
    /// Optional path to a CA certificate (PEM/DER) to trust for server
    /// validation. Ignored when `mode` is [`MssqlTlsMode::Disable`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-faucet-resource" = "path"))]
    pub ca_cert_path: Option<PathBuf>,
}

/// MSSQL encryption mode.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MssqlTlsMode {
    /// The default; encrypts the connection and **fails** against a server
    /// that offers no encryption (there is no plaintext fallback — tiberius
    /// cannot negotiate one without panicking). Same as `require`.
    #[default]
    Prefer,
    /// Require encryption; fail if the server does not offer it. Maps to
    /// `EncryptionLevel::Required`.
    Require,
    /// Encrypt and accept the server certificate without validating its chain
    /// (self-signed dev servers); `ca_cert_path` cannot be combined with it.
    /// **Insecure against MITM — never use in production.**
    TrustServerCertificate,
    /// No transport encryption. Maps to `EncryptionLevel::NotSupported`.
    Disable,
}

/// Parsed parts of a `mssql://` connection URL.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ConnectionParts {
    pub host: String,
    pub port: u16,
    pub database: Option<String>,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for ConnectionParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionParts")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("username", &self.username)
            .field("password", &"***")
            .finish()
    }
}

impl MssqlConnectionConfig {
    /// Validate that exactly one of `connection_url` / `connection_string` is
    /// set.
    pub fn validate(&self) -> Result<(), FaucetError> {
        match (&self.connection_url, &self.connection_string) {
            (Some(_), Some(_)) => Err(FaucetError::Config(
                "MSSQL config sets both `connection_url` and `connection_string`; set exactly one"
                    .into(),
            )),
            (None, None) => Err(FaucetError::Config(
                "MSSQL config requires either `connection_url` or `connection_string`".into(),
            )),
            _ => Ok(()),
        }
    }

    /// The server and database this config connects to, as a state-key-safe
    /// label: `<host>_<port>_<database>` (`_` when no database is named), or
    /// `mssql` when neither form parses.
    pub fn scope_label(&self) -> String {
        let (host, port, database) = if let Some(url) = &self.connection_url {
            match parse_connection_url(url) {
                Ok(p) => (p.host, p.port, p.database),
                Err(_) => return "mssql".to_string(),
            }
        } else if let Some(cs) = &self.connection_string {
            match ado_scope(cs) {
                Some(parts) => parts,
                None => return "mssql".to_string(),
            }
        } else {
            return "mssql".to_string();
        };
        let raw = format!(
            "{}_{port}_{}",
            host.trim(),
            database.as_deref().map(str::trim).unwrap_or("_")
        );
        key_safe(&raw)
    }
}

/// Server host, port and database named by an ADO.NET connection string.
fn ado_scope(raw: &str) -> Option<(String, u16, Option<String>)> {
    let mut server = None;
    let mut database = None;
    for pair in raw.split(';') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        let value = value.trim().trim_matches(|c| c == '\'' || c == '"');
        match key.trim().to_ascii_lowercase().as_str() {
            "server" | "data source" | "address" | "addr" | "network address" => {
                server = Some(value.to_string());
            }
            "database" | "initial catalog" => database = Some(value.to_string()),
            _ => {}
        }
    }
    let server = server.filter(|s| !s.is_empty())?;
    let server = server.strip_prefix("tcp:").unwrap_or(&server);
    let (host, port) = match server.split_once(',') {
        Some((host, port)) => (host, port.trim().parse().ok()?),
        None => (server, 1433),
    };
    Some((host.trim().to_string(), port, database.filter(|d| !d.is_empty())))
}

/// `raw` with every character outside `[A-Za-z0-9_.-]` replaced by `_`, and
/// leading / trailing `_` trimmed.
fn key_safe(raw: &str) -> String {
    let label: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    label.trim_matches('_').to_string()
}

/// Parse a `mssql://user:password@host:port/database` URL into its parts.
///
/// The port defaults to 1433. The database is the first path segment (optional).
/// Credentials are percent-decoded. Returns [`FaucetError::Config`] on a
/// malformed URL or a missing host.
pub(crate) fn parse_connection_url(raw: &str) -> Result<ConnectionParts, FaucetError> {
    let url = url::Url::parse(raw)
        .map_err(|e| FaucetError::Config(format!("invalid MSSQL connection_url: {e}")))?;

    if url.scheme() != "mssql" && url.scheme() != "sqlserver" {
        return Err(FaucetError::Config(format!(
            "MSSQL connection_url scheme must be `mssql://`, got `{}://`",
            url.scheme()
        )));
    }

    let host = url
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| FaucetError::Config("MSSQL connection_url is missing a host".into()))?
        .to_string();

    let port = url.port().unwrap_or(1433);

    let database = {
        let seg = url.path().trim_start_matches('/');
        if seg.is_empty() {
            None
        } else {
            Some(
                percent_decode(seg)
                    .map_err(|e| FaucetError::Config(format!("invalid database in URL: {e}")))?,
            )
        }
    };

    let username = percent_decode(url.username())
        .map_err(|e| FaucetError::Config(format!("invalid username in URL: {e}")))?;
    let password = percent_decode(url.password().unwrap_or(""))
        .map_err(|e| FaucetError::Config(format!("invalid password in URL: {e}")))?;

    Ok(ConnectionParts {
        host,
        port,
        database,
        username,
        password,
    })
}

fn percent_decode(s: &str) -> Result<String, std::str::Utf8Error> {
    percent_encoding::percent_decode_str(s)
        .decode_utf8()
        .map(|c| c.into_owned())
}

/// Quote an MSSQL identifier with bracket quoting (`[name]`), doubling any
/// interior `]` per T-SQL rules. Rejects identifiers containing a NUL byte.
///
/// MSSQL idiom is `[brackets]`; this is why the source/sink do not reuse
/// `faucet_core::util::quote_ident` (double-quote quoting).
pub fn quote_ident_mssql(name: &str) -> Result<String, FaucetError> {
    if name.contains('\0') {
        return Err(FaucetError::Config(format!(
            "invalid MSSQL identifier (contains NUL): {name:?}"
        )));
    }
    Ok(format!("[{}]", name.replace(']', "]]")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url_cfg(url: &str) -> MssqlConnectionConfig {
        MssqlConnectionConfig {
            connection_url: Some(url.into()),
            ..Default::default()
        }
    }

    fn ado_cfg(cs: &str) -> MssqlConnectionConfig {
        MssqlConnectionConfig {
            connection_string: Some(cs.into()),
            ..Default::default()
        }
    }

    #[test]
    fn scope_label_names_host_port_and_database() {
        assert_eq!(
            url_cfg("mssql://sa:pw@db.example.com:1444/sales").scope_label(),
            "db.example.com_1444_sales"
        );
        assert_eq!(
            url_cfg("mssql://sa:pw@db.example.com/sales").scope_label(),
            "db.example.com_1433_sales"
        );
        assert_eq!(
            url_cfg("mssql://sa:pw@db.example.com").scope_label(),
            "db.example.com_1433"
        );
        assert_ne!(
            url_cfg("mssql://sa:pw@h/a").scope_label(),
            url_cfg("mssql://sa:pw@h/b").scope_label()
        );
    }

    #[test]
    fn scope_label_reads_an_ado_connection_string() {
        assert_eq!(
            ado_cfg("Server=tcp:sql.local,1450;Database=Orders;User Id=sa;Password=x;")
                .scope_label(),
            "sql.local_1450_Orders"
        );
        assert_eq!(
            ado_cfg("Data Source='sql.local';Initial Catalog=hr").scope_label(),
            "sql.local_1433_hr"
        );
        assert_eq!(ado_cfg("Server=a\\inst;").scope_label(), "a_inst_1433");
        assert_ne!(
            ado_cfg("Server=a;Database=x").scope_label(),
            ado_cfg("Server=b;Database=x").scope_label()
        );
    }

    #[test]
    fn scope_label_falls_back_when_nothing_parses() {
        assert_eq!(url_cfg("not a url").scope_label(), "mssql");
        assert_eq!(ado_cfg("User Id=sa;garbage").scope_label(), "mssql");
        assert_eq!(ado_cfg("Server=h,notaport").scope_label(), "mssql");
        assert_eq!(MssqlConnectionConfig::default().scope_label(), "mssql");
    }

    #[test]
    fn validate_accepts_exactly_one() {
        let url_only = MssqlConnectionConfig {
            connection_url: Some("mssql://sa:pw@host/db".into()),
            ..Default::default()
        };
        assert!(url_only.validate().is_ok());

        let str_only = MssqlConnectionConfig {
            connection_string: Some("Server=host;Database=db".into()),
            ..Default::default()
        };
        assert!(str_only.validate().is_ok());
    }

    #[test]
    fn validate_rejects_both_and_neither() {
        let both = MssqlConnectionConfig {
            connection_url: Some("mssql://sa:pw@host/db".into()),
            connection_string: Some("Server=host".into()),
            ..Default::default()
        };
        assert!(both.validate().is_err());

        let neither = MssqlConnectionConfig::default();
        assert!(neither.validate().is_err());
    }

    #[test]
    fn parse_url_extracts_all_parts() {
        let parts = parse_connection_url("mssql://sa:s3cret@db.example.com:1433/sales").unwrap();
        assert_eq!(parts.host, "db.example.com");
        assert_eq!(parts.port, 1433);
        assert_eq!(parts.database.as_deref(), Some("sales"));
        assert_eq!(parts.username, "sa");
        assert_eq!(parts.password, "s3cret");
    }

    #[test]
    fn parse_url_defaults_port_and_optional_database() {
        let parts = parse_connection_url("mssql://sa:pw@localhost").unwrap();
        assert_eq!(parts.port, 1433);
        assert_eq!(parts.database, None);
    }

    #[test]
    fn parse_url_percent_decodes_credentials() {
        // password "p@ss:w/rd" percent-encoded
        let parts = parse_connection_url("mssql://us%65r:p%40ss%3Aw%2Frd@host/db").unwrap();
        assert_eq!(parts.username, "user");
        assert_eq!(parts.password, "p@ss:w/rd");
    }

    #[test]
    fn parse_url_rejects_wrong_scheme_and_missing_host() {
        assert!(parse_connection_url("postgres://sa:pw@host/db").is_err());
        assert!(parse_connection_url("not a url").is_err());
    }

    #[test]
    fn quote_ident_brackets_and_doubles_closing_bracket() {
        assert_eq!(quote_ident_mssql("events").unwrap(), "[events]");
        assert_eq!(quote_ident_mssql("dbo.events").unwrap(), "[dbo.events]");
        assert_eq!(quote_ident_mssql("we[i]rd").unwrap(), "[we[i]]rd]");
        assert!(quote_ident_mssql("bad\0name").is_err());
    }
}

#[cfg(test)]
mod debug_redaction_tests {
    use super::*;

    #[test]
    fn connection_parts_debug_masks_the_password() {
        let parts = parse_connection_url("mssql://sa:S3CRET-1@h:1433/db").unwrap();
        let dbg = format!("{parts:?}");
        assert!(!dbg.contains("S3CRET") && dbg.contains("***"), "{dbg}");
    }
}

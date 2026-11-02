//! Connect with the settings a `postgres://` URL names, and only those.
//!
//! libpq-style clients (sqlx among them) fill whatever a connection URL leaves
//! out from `PG*` environment variables and the password file: `PGPASSWORD`,
//! `~/.pgpass` / `PGPASSFILE`, `PGUSER`, `PGHOST` / `PGHOSTADDR`, `PGPORT`,
//! `PGDATABASE`, `PGSSLROOTCERT`, `PGSSLCERT`, `PGSSLKEY`, `PGAPPNAME`,
//! `PGOPTIONS`. That is what a command-line run wants, so it stays the default.
//! A process that runs configs written by someone else (`faucet serve`) turns
//! on [`set_config_only`]: then [`config_only`] says which settings a connector
//! must pin so nothing comes from the environment, refusing what it cannot pin.

use crate::error::FaucetError;
use std::sync::atomic::{AtomicBool, Ordering};

/// Environment variables a client reads for a client certificate or key.
const CLIENT_FILE_VARS: [&str; 2] = ["PGSSLCERT", "PGSSLKEY"];

static CONFIG_ONLY: AtomicBool = AtomicBool::new(false);

/// Make every PostgreSQL connection the process opens from now on take its
/// settings from its configuration alone (see the module docs). Off by
/// default; `faucet serve` turns it on at startup.
pub fn set_config_only(on: bool) {
    CONFIG_ONLY.store(on, Ordering::SeqCst);
}

/// Whether [`set_config_only`] is on.
pub fn config_only_enabled() -> bool {
    CONFIG_ONLY.load(Ordering::SeqCst)
}

/// Settings a connector pins after parsing a URL so nothing comes from the
/// environment. Apply each `true` field to the parsed options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LibpqPins {
    /// The URL names no password: set an empty one (no password is sent, and
    /// neither `PGPASSWORD` nor the password file is read).
    pub empty_password: bool,
    /// The URL names no host and the environment does: use `localhost`.
    pub default_host: bool,
    /// The URL names no port and the environment does: use `5432`.
    pub default_port: bool,
    /// The URL names no database and the environment does: use the user name
    /// (what the server picks when none is sent).
    pub database_from_user: bool,
    /// The URL names no `application_name` and the environment does: use
    /// `faucet`.
    pub default_application_name: bool,
}

/// What a connector pins on the options it parsed from `url` so they hold
/// only what the URL says. `env` reads one environment variable (pass
/// `|k| std::env::var(k).ok()`). Refuses, naming `connector`, a URL that
/// leaves out a setting the environment would supply and that cannot be
/// unset on the parsed options: the user (`PGUSER`), the client certificate
/// and key (`PGSSLCERT` / `PGSSLKEY`), the root certificate (`PGSSLROOTCERT`)
/// and server options (`PGOPTIONS`). A value that is not a URL pins nothing
/// (the client's own parser rejects it).
pub fn config_only(
    connector: &str,
    url: &str,
    env: impl Fn(&str) -> Option<String>,
) -> Result<LibpqPins, FaucetError> {
    let Ok(parsed) = url::Url::parse(url.trim()) else {
        return Ok(LibpqPins::default());
    };
    let query: Vec<(String, String)> = parsed.query_pairs().into_owned().collect();
    let has = |keys: &[&str]| query.iter().any(|(k, _)| keys.contains(&k.as_str()));
    let set = |name: &str| env(name).is_some_and(|v| !v.is_empty());

    if parsed.username().is_empty() && !has(&["user"]) && set("PGUSER") {
        return Err(FaucetError::Config(format!(
            "{connector}: connection_url names no user; add one to the URL (the PGUSER \
             environment variable is not used)"
        )));
    }
    let names_cert = has(&["sslcert", "ssl-cert"]);
    let names_key = has(&["sslkey", "ssl-key"]);
    if !(names_cert && names_key) {
        refuse_env_client_files(connector, &env)?;
    }
    if !has(&["sslrootcert", "ssl-root-cert", "ssl-ca"]) {
        refuse_env_vars(connector, &["PGSSLROOTCERT"], &env)?;
    }
    let names_options = query
        .iter()
        .any(|(k, _)| k == "options" || k.starts_with("options["));
    if !names_options {
        refuse_env_vars(connector, &["PGOPTIONS"], &env)?;
    }
    let names_host = parsed.host_str().is_some_and(|h| !h.is_empty()) || has(&["host", "hostaddr"]);
    let names_database = !parsed.path().trim_start_matches('/').is_empty() || has(&["dbname"]);
    Ok(LibpqPins {
        empty_password: parsed.password().is_none() && !has(&["password"]),
        default_host: !names_host && (set("PGHOST") || set("PGHOSTADDR")),
        default_port: parsed.port().is_none() && !has(&["port"]) && set("PGPORT"),
        database_from_user: !names_database && set("PGDATABASE"),
        default_application_name: !has(&["application_name"]) && set("PGAPPNAME"),
    })
}

/// Refuse, naming `connector`, when the environment names a client
/// certificate or key (`PGSSLCERT` / `PGSSLKEY`): a client would present it
/// to whichever server the config names. For connectors whose config sets
/// every other setting itself.
pub fn refuse_env_client_files(
    connector: &str,
    env: impl Fn(&str) -> Option<String>,
) -> Result<(), FaucetError> {
    refuse_env_vars(connector, &CLIENT_FILE_VARS, env)
}

/// Refuse, naming `connector`, when any of `names` is set (non-empty) in the
/// environment: a connection setting the config should name instead.
pub fn refuse_env_vars(
    connector: &str,
    names: &[&str],
    env: impl Fn(&str) -> Option<String>,
) -> Result<(), FaucetError> {
    match names
        .iter()
        .find(|name| env(name).is_some_and(|v| !v.is_empty()))
    {
        Some(name) => Err(FaucetError::Config(format!(
            "{connector}: the {name} environment variable is set; name the setting in the \
             connection settings, or unset it"
        ))),
        None => Ok(()),
    }
}

/// sqlx connect options for `url`. With [`set_config_only`] off this is the
/// URL parsed as sqlx parses it (environment and password file included);
/// with it on, only what the URL names (see [`config_only`]). `parse_err`
/// words a URL sqlx cannot parse.
#[cfg(feature = "libpq-sqlx")]
pub fn connect_options(
    connector: &str,
    url: &str,
    parse_err: impl FnOnce(sqlx::Error) -> FaucetError,
) -> Result<sqlx::postgres::PgConnectOptions, FaucetError> {
    connect_options_with(
        connector,
        url,
        config_only_enabled(),
        |k| std::env::var(k).ok(),
        parse_err,
    )
}

/// [`connect_options`] with the switch and the environment injected.
#[cfg(feature = "libpq-sqlx")]
pub fn connect_options_with(
    connector: &str,
    url: &str,
    config_only_on: bool,
    env: impl Fn(&str) -> Option<String>,
    parse_err: impl FnOnce(sqlx::Error) -> FaucetError,
) -> Result<sqlx::postgres::PgConnectOptions, FaucetError> {
    let pins = if config_only_on {
        config_only(connector, url, env)?
    } else {
        LibpqPins::default()
    };
    let mut opts: sqlx::postgres::PgConnectOptions = url.parse().map_err(parse_err)?;
    if pins.empty_password {
        opts = opts.password("");
    }
    if pins.default_host {
        opts = opts.host("localhost");
    }
    if pins.default_port {
        opts = opts.port(5432);
    }
    if pins.database_from_user {
        let user = opts.get_username().to_owned();
        opts = opts.database(&user);
    }
    if pins.default_application_name {
        opts = opts.application_name("faucet");
    }
    Ok(opts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_of(vars: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |k| {
            vars.iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn the_switch_is_off_until_turned_on() {
        assert!(!config_only_enabled());
        set_config_only(true);
        assert!(config_only_enabled());
        set_config_only(false);
        assert!(!config_only_enabled());
    }

    #[test]
    fn a_url_without_a_password_pins_an_empty_one() {
        let pins = config_only("pg", "postgres://u@h/db", env_of(&[("PGPASSWORD", "x")])).unwrap();
        assert!(pins.empty_password);
        let pins = config_only("pg", "postgres://u:p@h/db", env_of(&[])).unwrap();
        assert!(!pins.empty_password);
        let pins = config_only("pg", "postgres://u@h/db?password=p", env_of(&[])).unwrap();
        assert!(!pins.empty_password);
        let pins = config_only("pg", "postgres://u:@h/db", env_of(&[])).unwrap();
        assert!(pins.empty_password);
    }

    #[test]
    fn a_url_without_a_user_is_refused_while_pguser_is_set() {
        let err = config_only("pg", "postgres://h/db", env_of(&[("PGUSER", "svc")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("pg: connection_url names no user"), "{err}");
        assert!(config_only("pg", "postgres://h/db", env_of(&[])).is_ok());
        assert!(config_only("pg", "postgres://h/db?user=a", env_of(&[("PGUSER", "svc")])).is_ok());
        assert!(config_only("pg", "postgres://a@h/db", env_of(&[("PGUSER", "svc")])).is_ok());
    }

    #[test]
    fn host_port_database_and_application_name_pin_only_when_the_environment_sets_them() {
        let env = env_of(&[
            ("PGHOST", "db.internal"),
            ("PGPORT", "6543"),
            ("PGDATABASE", "other"),
            ("PGAPPNAME", "app"),
            ("PGSSLMODE", "verify-full"),
        ]);
        let pins = config_only("pg", "postgres:///?user=u&password=p", &env).unwrap();
        assert!(pins.default_host && pins.default_port);
        assert!(pins.database_from_user && pins.default_application_name);
        let pins = config_only(
            "pg",
            "postgres://u:p@h:1/db?sslmode=require&application_name=a",
            &env,
        )
        .unwrap();
        assert_eq!(
            pins,
            LibpqPins {
                empty_password: false,
                ..LibpqPins::default()
            }
        );
        let pins = config_only(
            "pg",
            "postgres:///?user=u&password=p&host=h&port=1&dbname=d",
            &env,
        )
        .unwrap();
        assert!(!pins.default_host && !pins.default_port && !pins.database_from_user);
        let pins = config_only("pg", "postgres:///?user=u&password=p", env_of(&[])).unwrap();
        assert_eq!(
            pins,
            LibpqPins {
                empty_password: false,
                ..LibpqPins::default()
            }
        );
    }

    #[test]
    fn an_environment_client_certificate_is_refused_unless_the_url_names_one() {
        let env = env_of(&[("PGSSLKEY", "/k.pem")]);
        let err = config_only("pg", "postgres://u:p@h/db", &env)
            .unwrap_err()
            .to_string();
        assert!(err.contains("PGSSLKEY"), "{err}");
        assert!(config_only("pg", "postgres://u:p@h/db?sslcert=/c&sslkey=/k", &env).is_ok());
        assert!(refuse_env_client_files("rs", env_of(&[("PGSSLCERT", "/c")])).is_err());
        assert!(refuse_env_client_files("rs", env_of(&[("PGSSLCERT", "")])).is_ok());
    }

    #[test]
    fn an_environment_root_certificate_or_server_options_are_refused_unless_the_url_names_them() {
        let env = env_of(&[("PGSSLROOTCERT", "/ca.pem")]);
        let err = config_only("pg", "postgres://u:p@h/db", &env)
            .unwrap_err()
            .to_string();
        assert!(err.contains("PGSSLROOTCERT"), "{err}");
        assert!(config_only("pg", "postgres://u:p@h/db?sslrootcert=/r", &env).is_ok());
        assert!(config_only("pg", "postgres://u:p@h/db?ssl-ca=/r", &env).is_ok());
        let env = env_of(&[("PGOPTIONS", "-c search_path=x")]);
        let err = config_only("pg", "postgres://u:p@h/db", &env)
            .unwrap_err()
            .to_string();
        assert!(err.contains("PGOPTIONS"), "{err}");
        assert!(config_only("pg", "postgres://u:p@h/db?options=-c%20a%3Db", &env).is_ok());
        assert!(config_only("pg", "postgres://u:p@h/db?options[a]=b", &env).is_ok());
    }

    #[test]
    fn a_value_that_is_not_a_url_pins_nothing() {
        assert_eq!(
            config_only("pg", "host=h user=u", env_of(&[("PGUSER", "svc")])).unwrap(),
            LibpqPins::default()
        );
    }

    #[cfg(feature = "libpq-sqlx")]
    mod options {
        use super::*;

        fn parse_err(e: sqlx::Error) -> FaucetError {
            FaucetError::Config(e.to_string())
        }

        #[test]
        fn with_the_switch_on_nothing_comes_from_the_environment() {
            let env = env_of(&[
                ("PGPASSWORD", "dummy-secret"),
                ("PGHOST", "db.internal"),
                ("PGPORT", "6543"),
                ("PGDATABASE", "other"),
                ("PGAPPNAME", "app"),
            ]);
            let opts =
                connect_options_with("pg", "postgres:///?user=u", true, &env, parse_err).unwrap();
            assert_eq!(opts.get_host(), "localhost");
            assert_eq!(opts.get_port(), 5432);
            assert_eq!(opts.get_username(), "u");
            assert_eq!(opts.get_database(), Some("u"));
            assert_eq!(opts.get_application_name(), Some("faucet"));
            let opts =
                connect_options_with("pg", "postgres://u:p@h:1/db", true, &env, parse_err).unwrap();
            assert_eq!((opts.get_host(), opts.get_port()), ("h", 1));
            assert_eq!(opts.get_database(), Some("db"));
        }

        #[test]
        fn with_the_switch_off_the_options_are_what_sqlx_parses() {
            let env = env_of(&[("PGUSER", "svc"), ("PGOPTIONS", "-c a=b")]);
            for url in [
                "postgres:///?user=u",
                "postgres://u:p@h:1/db",
                "postgres://h/db",
            ] {
                let ours = connect_options_with("pg", url, false, &env, parse_err).unwrap();
                let plain: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
                assert_eq!(format!("{ours:?}"), format!("{plain:?}"), "{url}");
            }
            let err = connect_options_with("pg", "postgres://h:x/db", false, &env, parse_err)
                .unwrap_err()
                .to_string();
            assert!(!err.is_empty());
        }
    }
}

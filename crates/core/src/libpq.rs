//! Connect with the settings a `postgres://` URL names, and only those.
//!
//! libpq-style clients (sqlx among them) fill whatever a connection URL leaves
//! out from `PG*` environment variables and the password file: `PGPASSWORD`,
//! `~/.pgpass` / `PGPASSFILE`, `PGUSER`, `PGHOST`, `PGPORT`, `PGSSLMODE`,
//! `PGSSLCERT`, `PGSSLKEY`. A connector must not pick those up: the URL comes
//! from a config, and the process environment belongs to whoever runs it.
//! [`config_only`] reads a URL (and the environment, injected) and says which
//! settings a connector must pin to their built-in defaults, refusing what it
//! cannot pin.

use crate::error::FaucetError;

/// Environment variables a client reads for a client certificate or key.
const CLIENT_CERT_VARS: [&str; 2] = ["PGSSLCERT", "PGSSLKEY"];

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
    /// The URL names no `sslmode` and the environment does: use `prefer`.
    pub default_ssl_mode: bool,
}

/// What a connector pins on the options it parsed from `url` so they hold
/// only what the URL says. `env` reads one environment variable (pass
/// `|k| std::env::var(k).ok()`). Refuses, naming `connector`, a URL that
/// names no user while `PGUSER` is set, and one that names no client
/// certificate / key while `PGSSLCERT` / `PGSSLKEY` is set — those cannot be
/// unset on the parsed options. A value that is not a URL pins nothing (the
/// client's own parser rejects it).
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
        refuse_env_client_cert(connector, &env)?;
    }
    let names_host = parsed.host_str().is_some_and(|h| !h.is_empty()) || has(&["host", "hostaddr"]);
    Ok(LibpqPins {
        empty_password: parsed.password().is_none() && !has(&["password"]),
        default_host: !names_host && (set("PGHOST") || set("PGHOSTADDR")),
        default_port: parsed.port().is_none() && !has(&["port"]) && set("PGPORT"),
        default_ssl_mode: !has(&["sslmode", "ssl-mode"]) && set("PGSSLMODE"),
    })
}

/// Refuse, naming `connector`, when the environment names a client
/// certificate or key (`PGSSLCERT` / `PGSSLKEY`): a client would present it
/// to whichever server the config names. For connectors whose config sets
/// every other setting itself.
pub fn refuse_env_client_cert(
    connector: &str,
    env: impl Fn(&str) -> Option<String>,
) -> Result<(), FaucetError> {
    match CLIENT_CERT_VARS
        .iter()
        .find(|name| env(name).is_some_and(|v| !v.is_empty()))
    {
        Some(name) => Err(FaucetError::Config(format!(
            "{connector}: the {name} environment variable is set; name the client \
             certificate and key in the connection settings, or unset it"
        ))),
        None => Ok(()),
    }
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
    fn host_port_and_ssl_mode_pin_to_defaults_only_when_the_environment_sets_them() {
        let env = env_of(&[
            ("PGHOST", "db.internal"),
            ("PGPORT", "6543"),
            ("PGSSLMODE", "disable"),
        ]);
        let pins = config_only("pg", "postgres:///db?user=u&password=p", &env).unwrap();
        assert!(pins.default_host && pins.default_port && pins.default_ssl_mode);
        let pins = config_only("pg", "postgres://u:p@h:1/db?sslmode=require", &env).unwrap();
        assert!(!pins.default_host && !pins.default_port && !pins.default_ssl_mode);
        let pins = config_only(
            "pg",
            "postgres:///db?user=u&password=p&host=h&port=1&ssl-mode=require",
            &env,
        )
        .unwrap();
        assert!(!pins.default_host && !pins.default_port && !pins.default_ssl_mode);
        let pins = config_only("pg", "postgres:///db?user=u&password=p", env_of(&[])).unwrap();
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
        assert!(refuse_env_client_cert("rs", env_of(&[("PGSSLCERT", "/c")])).is_err());
        assert!(refuse_env_client_cert("rs", env_of(&[("PGSSLCERT", "")])).is_ok());
    }

    #[test]
    fn a_value_that_is_not_a_url_pins_nothing() {
        assert_eq!(
            config_only("pg", "host=h user=u", env_of(&[("PGUSER", "svc")])).unwrap(),
            LibpqPins::default()
        );
    }
}

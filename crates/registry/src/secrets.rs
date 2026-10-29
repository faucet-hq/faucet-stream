//! The secret registrar: where the registry reports secret values it finds
//! while building a connector, so a host can scrub them from its output.
//!
//! Building some connectors surfaces credentials the host never resolved
//! itself, so it cannot know to redact them: a `google_service_account`
//! provider's inline key material, or the secret-looking values of a Singer
//! target's `target_config` / `env`. The registry hands each such value to the
//! registrar installed with [`install`]. With none installed the values are
//! not reported anywhere — nothing is stored, and the registry never logs
//! them itself.
//!
//! The `faucet` CLI installs its redaction registry here; an embedder that
//! scrubs secrets from its logs installs its own:
//!
//! ```
//! faucet_registry::secrets::install(Box::new(|secret: &str| {
//!     // add `secret` to the host's redaction set
//!     let _ = secret;
//! }));
//! ```
//!
//! For scrubbing text `faucet-core` itself emits (DLQ envelopes, errors) see
//! [`faucet_core::redact`].

use std::sync::OnceLock;

/// A registrar: called once per secret value the registry finds.
pub type SecretRegistrar = Box<dyn Fn(&str) + Send + Sync>;

fn hook() -> &'static OnceLock<SecretRegistrar> {
    static HOOK: OnceLock<SecretRegistrar> = OnceLock::new();
    &HOOK
}

/// Install the process-wide registrar. The **first** call wins; later calls
/// are ignored and return `false`.
pub fn install(registrar: SecretRegistrar) -> bool {
    hook().set(registrar).is_ok()
}

/// Whether a registrar has been installed.
pub fn is_installed() -> bool {
    hook().get().is_some()
}

/// Hand `secret` to the installed registrar, if any.
pub(crate) fn report(secret: &str) {
    if let Some(f) = hook().get() {
        f(secret);
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Mutex;

    static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());

    /// Install a registrar that records every reported value (idempotent).
    pub(crate) fn install() {
        super::install(Box::new(|s: &str| {
            SEEN.lock().unwrap().push(s.to_owned());
        }));
        assert!(super::is_installed());
    }

    /// Whether `secret` was reported.
    pub(crate) fn reported(secret: &str) -> bool {
        SEEN.lock().unwrap().iter().any(|s| s == secret)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn install_is_first_wins() {
        super::test_support::install();
        assert!(!super::install(Box::new(|_: &str| {})));
        super::report("first-wins-secret-value");
        assert!(super::test_support::reported("first-wins-secret-value"));
    }
}

//! Version-selector check for a bundle's companion `*_select` fields. The
//! registry's own parser lives behind the `serve` feature; a bundle is read
//! by every build (a pipeline config, a hub document), so the check is here.

use crate::error::{CliError, CliResult};

/// Every channel name a selector may use.
pub const CHANNELS: &[&str] = &[
    "stable", "previous", "newest", "dev", "test", "staging", "pre-prod", "canary", "prod",
];

fn normalize(raw: &str) -> String {
    raw.trim()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Accept a version number or a channel name; refuse `latest` and anything
/// else with the same wording the registry uses.
pub fn check(raw: &str) -> CliResult<()> {
    if raw.trim().parse::<u32>().is_ok() {
        return Ok(());
    }
    let n = normalize(raw);
    if n == "latest" {
        return Err(CliError::Config(
            "tests: `latest` is not a version channel — use `stable` (the launched version) or \
             `newest` (the highest version number)"
                .into(),
        ));
    }
    if CHANNELS.iter().any(|c| normalize(c) == n) {
        Ok(())
    } else {
        Err(CliError::Config(format!(
            "tests: unknown version selector '{raw}' — use a version number or one of: {}",
            CHANNELS.join(", ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_and_channels_pass_latest_and_typos_do_not() {
        check("3").unwrap();
        check("pre_prod").unwrap();
        check("Stable").unwrap();
        assert!(check("latest").unwrap_err().to_string().contains("newest"));
        assert!(check("prd").unwrap_err().to_string().contains("unknown"));
    }

    #[cfg(feature = "serve")]
    #[test]
    fn the_list_matches_the_registry_channels() {
        use crate::serve::history::templates::VersionChannel;
        let names: Vec<&str> = VersionChannel::ALL.iter().map(|c| c.as_str()).collect();
        assert_eq!(names, CHANNELS);
    }
}

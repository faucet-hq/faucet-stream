//! Connector SDK scaffolding (#209): pure templates for `faucet new connector`.
//!
//! Emits a ready-to-build `faucet-source-<name>` / `faucet-sink-<name>` crate
//! that follows every repo convention — the standard module layout,
//! `JsonSchema`-deriving config, `config_schema()` / `connector_name()`
//! overrides, the `#![cfg_attr(docsrs, feature(doc_cfg))]` crate-root line, the
//! `[package.metadata.docs.rs]` block, system-name-first crates.io keywords, a
//! README, and a passing unit test — so a third-party author starts from a
//! compiling, publish-ready crate instead of hand-assembling one.
//!
//! Everything here is a pure `&self -> String` / `-> Vec<GeneratedFile>`
//! function so it is fully unit-testable without touching the filesystem; the
//! [`crate::commands::new`] command writes the returned files to disk.

/// Which half of a connector pair to scaffold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorKind {
    Source,
    Sink,
}

impl ConnectorKind {
    /// `"source"` / `"sink"` — the crate-name infix and role word.
    pub fn as_str(self) -> &'static str {
        match self {
            ConnectorKind::Source => "source",
            ConnectorKind::Sink => "sink",
        }
    }

    /// Parse from the `--kind` argument.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "source" => Ok(ConnectorKind::Source),
            "sink" => Ok(ConnectorKind::Sink),
            other => Err(format!(
                "unknown connector kind `{other}` (expected `source` or `sink`)"
            )),
        }
    }
}

/// A single generated file: a path relative to the new crate's parent
/// directory, and its full contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedFile {
    pub path: String,
    pub contents: String,
}

/// A parsed, validated scaffold request.
#[derive(Debug, Clone)]
pub struct ConnectorScaffold {
    /// System name, e.g. `acme` (validated: `^[a-z][a-z0-9-]*$`).
    pub name: String,
    pub kind: ConnectorKind,
    /// Also emit a `faucet-common-<name>` crate for shared source/sink config.
    pub with_common: bool,
}

impl ConnectorScaffold {
    /// Validate the connector name and build a scaffold request.
    pub fn new(name: &str, kind: ConnectorKind, with_common: bool) -> Result<Self, String> {
        validate_name(name)?;
        Ok(Self {
            name: name.to_owned(),
            kind,
            with_common,
        })
    }

    /// `faucet-source-acme` / `faucet-sink-acme`.
    pub fn crate_name(&self) -> String {
        format!("faucet-{}-{}", self.kind.as_str(), self.name)
    }

    /// `faucet-common-acme`.
    pub fn common_crate_name(&self) -> String {
        format!("faucet-common-{}", self.name)
    }

    /// PascalCase system name, e.g. `acme-widgets` → `AcmeWidgets`.
    pub fn type_prefix(&self) -> String {
        to_pascal(&self.name)
    }

    /// The connector struct name, e.g. `AcmeSource` / `AcmeSink`.
    pub fn connector_type(&self) -> String {
        format!(
            "{}{}",
            self.type_prefix(),
            match self.kind {
                ConnectorKind::Source => "Source",
                ConnectorKind::Sink => "Sink",
            }
        )
    }

    /// The config struct name, e.g. `AcmeSourceConfig`.
    pub fn config_type(&self) -> String {
        format!("{}Config", self.connector_type())
    }

    /// All files to write, each path relative to the output parent directory.
    pub fn files(&self) -> Vec<GeneratedFile> {
        let base = self.crate_name();
        let impl_file = match self.kind {
            ConnectorKind::Source => "stream.rs",
            ConnectorKind::Sink => "sink.rs",
        };
        let mut files = vec![
            GeneratedFile {
                path: format!("{base}/Cargo.toml"),
                contents: self.cargo_toml(),
            },
            GeneratedFile {
                path: format!("{base}/README.md"),
                contents: self.readme(),
            },
            GeneratedFile {
                path: format!("{base}/src/lib.rs"),
                contents: self.lib_rs(),
            },
            GeneratedFile {
                path: format!("{base}/src/config.rs"),
                contents: self.config_rs(),
            },
            GeneratedFile {
                path: format!("{base}/src/{impl_file}"),
                contents: self.impl_rs(),
            },
            GeneratedFile {
                path: format!("{base}/tests/conformance.rs"),
                contents: self.conformance_rs(),
            },
        ];
        if self.with_common {
            let cbase = self.common_crate_name();
            files.push(GeneratedFile {
                path: format!("{cbase}/Cargo.toml"),
                contents: self.common_cargo_toml(),
            });
            files.push(GeneratedFile {
                path: format!("{cbase}/src/lib.rs"),
                contents: self.common_lib_rs(),
            });
        }
        files
    }

    fn cargo_toml(&self) -> String {
        let name = &self.name;
        let crate_name = self.crate_name();
        let role = self.kind.as_str();
        let common_dep = if self.with_common {
            format!(
                "{cn} = {{ path = \"../{cn}\", version = \"1.0.0\" }}\n",
                cn = self.common_crate_name()
            )
        } else {
            String::new()
        };
        format!(
            r#"[package]
name = "{crate_name}"
version = "1.0.0"
edition = "2024"
rust-version = "1.96"
license = "MIT OR Apache-2.0"
repository = "https://github.com/your-org/{crate_name}"
description = "{name} {role} connector for the faucet-stream ecosystem"
readme = "README.md"
# System name first so the crate ranks on crates.io for `{name}`.
keywords = ["{name}", "etl", "pipeline", "connector", "data"]
categories = ["database", "asynchronous"]

[dependencies]
# faucet-core carries the Source/Sink traits and re-exports async_trait +
# serde_json. serde + schemars are pulled directly for the derive macros
# (matching every built-in connector crate).
faucet-core = "1"
{common_dep}serde = {{ version = "1", features = ["derive"] }}
schemars = "1"

[dev-dependencies]
# The connector-SDK conformance battery `tests/conformance.rs` runs.
faucet-conformance = "1"
tokio = {{ version = "1", features = ["macros", "rt-multi-thread"] }}

# Renders the complete feature-gated API (with per-item feature badges) on
# docs.rs — mirror this in every connector crate.
[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
"#
        )
    }

    fn lib_rs(&self) -> String {
        let crate_name = self.crate_name();
        let name = &self.name;
        let role = self.kind.as_str();
        let config_ty = self.config_type();
        let conn_ty = self.connector_type();
        let (trait_reexport, impl_mod) = match self.kind {
            ConnectorKind::Source => ("Source", "stream"),
            ConnectorKind::Sink => ("Sink", "sink"),
        };
        format!(
            r#"#![cfg_attr(docsrs, feature(doc_cfg))]

//! # {crate_name}
//!
//! {name} {role} connector for the [faucet-stream](https://docs.rs/faucet-stream)
//! ecosystem. Generated by `faucet new connector`.

pub mod config;
pub mod {impl_mod};

pub use faucet_core::{{FaucetError, {trait_reexport}}};

pub use config::{config_ty};
pub use {impl_mod}::{conn_ty};
"#
        )
    }

    fn config_rs(&self) -> String {
        let config_ty = self.config_type();
        let name = &self.name;
        format!(
            r#"//! Configuration for the {name} connector.
//!
//! No I/O or protocol logic lives here — just the serde/schemars-deriving
//! config struct (and any sub-enums it needs).

use faucet_core::JsonSchema;
use serde::{{Deserialize, Serialize}};

/// Config for the {name} connector, deserialized from the `config:` block of a
/// `faucet.yaml` pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct {config_ty} {{
    /// TODO: replace with your connector's real settings (endpoint, table,
    /// bucket, credentials-via-`${{env:VAR}}`, …). This placeholder keeps the
    /// generated crate compiling and testable out of the box.
    #[serde(default)]
    pub example_setting: Option<String>,
}}
"#
        )
    }

    /// `tests/conformance.rs`: the `faucet-conformance` checks a fresh
    /// scaffold already passes, with the ones to add as the connector grows.
    fn conformance_rs(&self) -> String {
        let name = &self.name;
        let krate = self.crate_name().replace('-', "_");
        let config_ty = self.config_type();
        let conn_ty = self.connector_type();
        match self.kind {
            ConnectorKind::Source => format!(
                r#"//! The `faucet-conformance` battery for the {name} source — run it with
//! `cargo test --test conformance`.
//!
//! Add checks as the connector grows: `assert_bounded_memory` once
//! `stream_pages` pages natively, `assert_bookmark_roundtrip` once it is
//! resumable, `assert_errors_not_panics` against a config that must fail.

use faucet_conformance::{{
    assert_config_schema_valid, assert_connector_name_nonempty, assert_preflight_check_wellformed,
}};
use faucet_core::check::CheckContext;
use {krate}::{{{config_ty}, {conn_ty}}};

fn source() -> {conn_ty} {{
    {conn_ty}::new({config_ty} {{ example_setting: None }})
}}

#[test]
fn conformance_config_schema_valid() {{
    assert_config_schema_valid(&source());
}}

#[test]
fn conformance_connector_name_nonempty() {{
    assert_connector_name_nonempty(&source());
}}

#[tokio::test]
async fn conformance_preflight_check_wellformed() {{
    assert_preflight_check_wellformed(&source(), &CheckContext::default()).await;
}}
"#
            ),
            ConnectorKind::Sink => format!(
                r#"//! The `faucet-conformance` battery for the {name} sink — run it with
//! `cargo test --test conformance`.
//!
//! Add checks as the connector grows: `assert_capabilities_truthful` once the
//! sink writes somewhere a test can count, `assert_write_modes_truthful` once
//! it supports upsert/delete, `assert_idempotent_replay` once it supports
//! exactly-once writes.

use faucet_conformance::{{
    assert_batch_atomicity_declared, assert_config_schema_valid_value,
    assert_connector_name_nonempty_value, assert_sink_preflight_check_wellformed,
}};
use faucet_core::check::CheckContext;
use faucet_core::Sink;
use {krate}::{{{config_ty}, {conn_ty}}};

fn sink() -> {conn_ty} {{
    {conn_ty}::new({config_ty} {{ example_setting: None }})
}}

#[test]
fn conformance_config_schema_valid() {{
    let sink = sink();
    assert_config_schema_valid_value(&sink.config_schema(), sink.connector_name());
}}

#[test]
fn conformance_connector_name_nonempty() {{
    let sink = sink();
    assert_connector_name_nonempty_value(sink.connector_name(), sink.connector_name());
    assert_batch_atomicity_declared(&sink);
}}

#[tokio::test]
async fn conformance_preflight_check_wellformed() {{
    assert_sink_preflight_check_wellformed(&sink(), &CheckContext::default()).await;
}}
"#
            ),
        }
    }

    fn impl_rs(&self) -> String {
        match self.kind {
            ConnectorKind::Source => self.source_impl(),
            ConnectorKind::Sink => self.sink_impl(),
        }
    }

    fn source_impl(&self) -> String {
        let name = &self.name;
        let config_ty = self.config_type();
        let conn_ty = self.connector_type();
        format!(
            r#"//! The {name} source — the one module that performs I/O.

use crate::config::{config_ty};
use faucet_core::{{async_trait, serde_json::Value, FaucetError, Source}};
use std::collections::HashMap;

/// {name} source connector.
pub struct {conn_ty} {{
    #[allow(dead_code)]
    config: {config_ty},
}}

impl {conn_ty} {{
    /// Construct the source from its config. Store reusable clients/pools here;
    /// never recreate them per fetch (see the faucet performance guidelines).
    pub fn new(config: {config_ty}) -> Self {{
        Self {{ config }}
    }}
}}

#[async_trait]
impl Source for {conn_ty} {{
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {{
        // TODO: fetch real records from your source. The passthrough below
        // returns an empty page so the generated crate compiles and tests green.
        Ok(Vec::new())
    }}

    fn config_schema(&self) -> Value {{
        faucet_core::serde_json::to_value(faucet_core::schema_for!({config_ty}))
            .unwrap_or(Value::Null)
    }}

    fn connector_name(&self) -> &'static str {{
        "{name}"
    }}
}}

#[cfg(test)]
mod tests {{
    use super::*;

    #[tokio::test]
    async fn fetches_without_error() {{
        let source = {conn_ty}::new({config_ty} {{ example_setting: None }});
        let records = source.fetch_all().await.expect("fetch");
        assert!(records.is_empty());
        assert_eq!(source.connector_name(), "{name}");
    }}
}}
"#
        )
    }

    fn sink_impl(&self) -> String {
        let name = &self.name;
        let config_ty = self.config_type();
        let conn_ty = self.connector_type();
        format!(
            r#"//! The {name} sink — the one module that performs I/O.

use crate::config::{config_ty};
use faucet_core::{{async_trait, serde_json::Value, FaucetError, Sink}};

/// {name} sink connector.
pub struct {conn_ty} {{
    #[allow(dead_code)]
    config: {config_ty},
}}

impl {conn_ty} {{
    /// Construct the sink from its config. Store reusable clients/pools here.
    pub fn new(config: {config_ty}) -> Self {{
        Self {{ config }}
    }}
}}

#[async_trait]
impl Sink for {conn_ty} {{
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {{
        // TODO: write `records` to your destination (prefer a bulk/batch API).
        // The passthrough below just counts them so the crate tests green.
        Ok(records.len())
    }}

    fn config_schema(&self) -> Value {{
        faucet_core::serde_json::to_value(faucet_core::schema_for!({config_ty}))
            .unwrap_or(Value::Null)
    }}

    fn connector_name(&self) -> &'static str {{
        "{name}"
    }}
}}

#[cfg(test)]
mod tests {{
    use super::*;
    use faucet_core::serde_json::json;

    #[tokio::test]
    async fn writes_batch() {{
        let sink = {conn_ty}::new({config_ty} {{ example_setting: None }});
        let n = sink.write_batch(&[json!({{"a": 1}})]).await.expect("write");
        assert_eq!(n, 1);
        assert_eq!(sink.connector_name(), "{name}");
    }}
}}
"#
        )
    }

    fn readme(&self) -> String {
        let crate_name = self.crate_name();
        let name = &self.name;
        let role = self.kind.as_str();
        let type_kw = if self.kind == ConnectorKind::Source {
            "source"
        } else {
            "sink"
        };
        format!(
            r#"# {crate_name}

{name} {role} connector for the [faucet-stream](https://github.com/faucet-hq/faucet-stream)
ecosystem. Generated by `faucet new connector`.

## Usage from a pipeline config

```yaml
pipeline:
  {type_kw}:
    type: {name}
    config:
      example_setting: replace-me
```

To use it from the `faucet` CLI, build a custom binary that registers it — see
[Custom binaries with third-party connectors](https://github.com/faucet-hq/faucet-stream/blob/main/cli/README.md#custom-binaries-with-third-party-connectors).

## Next steps

1. Replace the fields in `src/config.rs` with your connector's real settings.
2. Implement the I/O in `src/{impl_file}` (reuse clients/pools created in `new()`).
3. Flesh out the tests, then publish with `cargo publish`.
"#,
            impl_file = match self.kind {
                ConnectorKind::Source => "stream.rs",
                ConnectorKind::Sink => "sink.rs",
            }
        )
    }

    fn common_cargo_toml(&self) -> String {
        let common = self.common_crate_name();
        let name = &self.name;
        format!(
            r#"[package]
name = "{common}"
version = "1.0.0"
edition = "2024"
rust-version = "1.96"
license = "MIT OR Apache-2.0"
repository = "https://github.com/your-org/{common}"
description = "Shared config types for the {name} faucet-stream source/sink pair"
readme = "README.md"
keywords = ["{name}", "etl", "pipeline", "connector", "data"]
categories = ["database", "asynchronous"]

[dependencies]
faucet-core = "1"
serde = {{ version = "1", features = ["derive"] }}
schemars = "1"

[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
"#
        )
    }

    fn common_lib_rs(&self) -> String {
        let name = &self.name;
        format!(
            r#"#![cfg_attr(docsrs, feature(doc_cfg))]

//! Shared config types for the {name} source/sink pair.
//!
//! Put auth enums, value-format enums, TLS settings, and any other types both
//! the source and the sink crates need here; re-export them from each so
//! end-user imports don't change.

use faucet_core::JsonSchema;
use serde::{{Deserialize, Serialize}};

/// Shared connection settings for {name}.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct {prefix}Connection {{
    /// TODO: shared connection fields (endpoint, credentials, …).
    #[serde(default)]
    pub endpoint: Option<String>,
}}
"#,
            prefix = self.type_prefix()
        )
    }
}

/// Validate a connector system name: lowercase, starts with a letter, then
/// letters/digits/hyphens (the `faucet-<kind>-<name>` convention).
pub fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("connector name must not be empty".to_owned());
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_lowercase() {
        return Err(format!(
            "connector name `{name}` must start with a lowercase ASCII letter"
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(format!(
            "connector name `{name}` may only contain lowercase letters, digits, and hyphens"
        ));
    }
    if name.ends_with('-') || name.contains("--") {
        return Err(format!(
            "connector name `{name}` has a stray/doubled hyphen"
        ));
    }
    Ok(())
}

/// PascalCase a hyphenated system name: `acme-widgets` → `AcmeWidgets`.
fn to_pascal(name: &str) -> String {
    name.split('-')
        .filter(|s| !s.is_empty())
        .map(|word| {
            let mut c = word.chars();
            match c.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + c.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_names() {
        assert!(validate_name("acme").is_ok());
        assert!(validate_name("acme-widgets").is_ok());
        assert!(validate_name("s3").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("Acme").is_err());
        assert!(validate_name("1acme").is_err());
        assert!(validate_name("acme_widgets").is_err());
        assert!(validate_name("acme-").is_err());
        assert!(validate_name("acme--x").is_err());
    }

    #[test]
    fn pascal_case() {
        assert_eq!(to_pascal("acme"), "Acme");
        assert_eq!(to_pascal("acme-widgets"), "AcmeWidgets");
        assert_eq!(to_pascal("s3"), "S3");
    }

    #[test]
    fn source_scaffold_shape() {
        let s = ConnectorScaffold::new("acme", ConnectorKind::Source, false).unwrap();
        assert_eq!(s.crate_name(), "faucet-source-acme");
        assert_eq!(s.connector_type(), "AcmeSource");
        assert_eq!(s.config_type(), "AcmeSourceConfig");
        let files = s.files();
        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"faucet-source-acme/Cargo.toml"));
        assert!(paths.contains(&"faucet-source-acme/src/lib.rs"));
        assert!(paths.contains(&"faucet-source-acme/src/config.rs"));
        assert!(paths.contains(&"faucet-source-acme/src/stream.rs"));
        assert!(paths.contains(&"faucet-source-acme/README.md"));
        // No common crate unless requested.
        assert!(!paths.iter().any(|p| p.contains("faucet-common-acme")));
    }

    #[test]
    fn cargo_toml_follows_conventions() {
        let s = ConnectorScaffold::new("acme", ConnectorKind::Source, false).unwrap();
        let cargo = s
            .files()
            .into_iter()
            .find(|f| f.path.ends_with("Cargo.toml"))
            .unwrap()
            .contents;
        assert!(cargo.contains("version = \"1.0.0\""), "must start at 1.0.0");
        assert!(
            cargo.contains("keywords = [\"acme\""),
            "system name keyword first"
        );
        assert!(cargo.contains("[package.metadata.docs.rs]"));
        assert!(cargo.contains("all-features = true"));
        assert!(cargo.contains("faucet-core = \"1\""));
    }

    /// #824: the scaffold wires the conformance battery a third-party crate
    /// runs with `cargo test` (`faucet conformance` scores built-ins only).
    #[test]
    fn conformance_battery_is_wired_for_both_kinds() {
        for (kind, conn, imports) in [
            (
                ConnectorKind::Source,
                "AcmeSource",
                "assert_preflight_check_wellformed",
            ),
            (
                ConnectorKind::Sink,
                "AcmeSink",
                "assert_sink_preflight_check_wellformed",
            ),
        ] {
            let s = ConnectorScaffold::new("acme", kind, false).unwrap();
            let files = s.files();
            let file = |suffix: &str| {
                files
                    .iter()
                    .find(|f| f.path.ends_with(suffix))
                    .unwrap_or_else(|| panic!("{suffix} generated"))
                    .contents
                    .clone()
            };
            let crate_name = s.crate_name();
            let conformance = file("tests/conformance.rs");
            assert!(
                files
                    .iter()
                    .any(|f| f.path == format!("{crate_name}/tests/conformance.rs"))
            );
            assert!(conformance.contains(imports), "{conformance}");
            assert!(
                conformance.contains(&format!(
                    "use {}::{{{conn}Config, {conn}}};",
                    crate_name.replace('-', "_")
                )),
                "{conformance}"
            );
            assert!(conformance.contains("example_setting: None"));
            assert!(conformance.contains("cargo test --test conformance"));
            assert!(file("Cargo.toml").contains("faucet-conformance = \"1\""));
        }
    }

    #[test]
    fn lib_rs_has_docsrs_line_and_reexports() {
        let s = ConnectorScaffold::new("acme", ConnectorKind::Sink, false).unwrap();
        let lib = s
            .files()
            .into_iter()
            .find(|f| f.path.ends_with("src/lib.rs"))
            .unwrap()
            .contents;
        assert!(lib.starts_with("#![cfg_attr(docsrs, feature(doc_cfg))]"));
        assert!(lib.contains("pub use sink::AcmeSink"));
        assert!(lib.contains("pub mod sink;"));
    }

    #[test]
    fn sink_impl_implements_trait() {
        let s = ConnectorScaffold::new("acme", ConnectorKind::Sink, false).unwrap();
        let sink = s
            .files()
            .into_iter()
            .find(|f| f.path.ends_with("sink.rs"))
            .unwrap()
            .contents;
        assert!(sink.contains("impl Sink for AcmeSink"));
        assert!(sink.contains("async fn write_batch"));
        assert!(sink.contains("fn connector_name(&self) -> &'static str"));
    }

    #[test]
    fn common_crate_emitted_when_requested() {
        let s = ConnectorScaffold::new("acme", ConnectorKind::Source, true).unwrap();
        let paths: Vec<String> = s.files().into_iter().map(|f| f.path).collect();
        assert!(paths.iter().any(|p| p == "faucet-common-acme/Cargo.toml"));
        assert!(paths.iter().any(|p| p == "faucet-common-acme/src/lib.rs"));
    }
}

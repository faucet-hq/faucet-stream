//! `faucet new connector` — scaffold a ready-to-build connector crate (#209).

use crate::cli::{NewArgs, NewConnectorArgs, NewTarget};
use crate::error::{CliError, CliResult};
use crate::scaffold::{ConnectorKind, ConnectorScaffold};

/// Execute the `new` command.
pub async fn run(args: NewArgs) -> CliResult<()> {
    match args.target {
        NewTarget::Connector(a) => run_connector(a).await,
    }
}

/// What to do after scaffolding: build, then the conformance battery the
/// scaffold wired into `tests/conformance.rs`. `faucet conformance` only scores
/// connectors compiled into a faucet binary, so it is named for built-ins only
/// (#824).
fn next_steps(scaffold: &ConnectorScaffold, root: &std::path::Path) -> String {
    let dir = root.join(scaffold.crate_name());
    let (impl_file, grow) = match scaffold.kind {
        ConnectorKind::Source => (
            "stream.rs",
            "native streaming (assert_bounded_memory) + resumable bookmarks \
             (assert_bookmark_roundtrip)",
        ),
        ConnectorKind::Sink => (
            "sink.rs",
            "durable writes (assert_capabilities_truthful), then upsert \
             (assert_write_modes_truthful) / exactly-once (assert_idempotent_replay)",
        ),
    };
    format!(
        "\nNext:\n  cd {dir}\n  cargo test          # the generated passthrough compiles & tests green\n  \
         # then implement the TODOs in src/config.rs and src/{impl_file}\n\n\
         Conformance: tests/conformance.rs runs the faucet-conformance battery.\n  \
         cargo test --test conformance\n\
         Extend it as the connector grows: {grow}.\n\
         (`faucet conformance {name}` scores connectors compiled into a faucet binary —\n \
         it applies once this connector is built into one.)",
        dir = dir.display(),
        name = scaffold.name,
    )
}

/// Scaffold a connector crate.
async fn run_connector(args: NewConnectorArgs) -> CliResult<()> {
    let kind = ConnectorKind::parse(&args.kind).map_err(CliError::Config)?;
    let scaffold =
        ConnectorScaffold::new(&args.name, kind, args.common).map_err(CliError::Config)?;
    let files = scaffold.files();
    let root = &args.output;

    // Fail before writing anything if any target already exists (unless --force).
    if !args.force {
        for f in &files {
            let path = root.join(&f.path);
            if path.exists() {
                return Err(CliError::ScaffoldExists { path });
            }
        }
    }

    for f in &files {
        let path = root.join(&f.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, &f.contents)?;
    }

    println!(
        "Scaffolded {} ({} files):",
        scaffold.crate_name(),
        files.len()
    );
    for f in &files {
        println!("  {}", root.join(&f.path).display());
    }
    println!("{}", next_steps(&scaffold, root));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #824: the printed command works for a third-party crate; `faucet
    /// conformance` is only offered as the built-in path.
    #[test]
    fn next_steps_point_at_the_wired_conformance_battery() {
        let root = std::path::Path::new("/work");
        for kind in [ConnectorKind::Source, ConnectorKind::Sink] {
            let s = ConnectorScaffold::new("acme", kind, false).unwrap();
            let text = next_steps(&s, root);
            assert!(
                text.contains(&format!("cd /work/{}", s.crate_name())),
                "{text}"
            );
            assert!(text.contains("cargo test --test conformance"), "{text}");
            assert!(text.contains("tests/conformance.rs"), "{text}");
            assert!(
                !text.contains(&format!("faucet conformance {}`", s.crate_name())),
                "{text}"
            );
            assert!(text.contains("compiled into a faucet binary"), "{text}");
        }
        let src = next_steps(
            &ConnectorScaffold::new("acme", ConnectorKind::Source, false).unwrap(),
            root,
        );
        assert!(src.contains("src/stream.rs") && src.contains("assert_bounded_memory"));
        let sink = next_steps(
            &ConnectorScaffold::new("acme", ConnectorKind::Sink, false).unwrap(),
            root,
        );
        assert!(sink.contains("src/sink.rs") && sink.contains("assert_capabilities_truthful"));
    }

    #[tokio::test]
    async fn scaffolds_a_source_crate_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let args = NewConnectorArgs {
            name: "acme".into(),
            kind: "source".into(),
            common: false,
            output: dir.path().to_path_buf(),
            force: false,
        };
        run_connector(args).await.expect("scaffold succeeds");
        let base = dir.path().join("faucet-source-acme");
        assert!(base.join("Cargo.toml").is_file());
        assert!(base.join("src/lib.rs").is_file());
        assert!(base.join("src/config.rs").is_file());
        assert!(base.join("src/stream.rs").is_file());
        assert!(base.join("README.md").is_file());
        let lib = std::fs::read_to_string(base.join("src/lib.rs")).unwrap();
        assert!(lib.contains("pub use stream::AcmeSource;"));
    }

    #[tokio::test]
    async fn refuses_to_overwrite_without_force() {
        let dir = tempfile::tempdir().unwrap();
        let mk = || NewConnectorArgs {
            name: "acme".into(),
            kind: "sink".into(),
            common: false,
            output: dir.path().to_path_buf(),
            force: false,
        };
        run_connector(mk()).await.expect("first scaffold");
        let err = run_connector(mk()).await.expect_err("second must refuse");
        assert!(matches!(err, CliError::ScaffoldExists { .. }));
    }

    #[tokio::test]
    async fn force_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let mk = |force| NewConnectorArgs {
            name: "acme".into(),
            kind: "sink".into(),
            common: true,
            output: dir.path().to_path_buf(),
            force,
        };
        run_connector(mk(false)).await.expect("first scaffold");
        run_connector(mk(true)).await.expect("force overwrite");
        assert!(dir.path().join("faucet-common-acme/Cargo.toml").is_file());
    }

    #[tokio::test]
    async fn rejects_bad_kind() {
        let dir = tempfile::tempdir().unwrap();
        let args = NewConnectorArgs {
            name: "acme".into(),
            kind: "middleware".into(),
            common: false,
            output: dir.path().to_path_buf(),
            force: false,
        };
        assert!(matches!(
            run_connector(args).await,
            Err(CliError::Config(_))
        ));
    }

    #[tokio::test]
    async fn rejects_bad_name() {
        let dir = tempfile::tempdir().unwrap();
        let args = NewConnectorArgs {
            name: "Acme_Bad".into(),
            kind: "source".into(),
            common: false,
            output: dir.path().to_path_buf(),
            force: false,
        };
        assert!(matches!(
            run_connector(args).await,
            Err(CliError::Config(_))
        ));
    }
}

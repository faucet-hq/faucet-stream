//! Under a reference policy with path roots, every listed file must resolve
//! under a root; a link below the root that leads outside it is refused.

use faucet_core::Source;
use faucet_core::resolve::ReferencePolicy;
use faucet_source_file::{FileSource, FileSourceConfig};
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(unix)]
#[tokio::test]
async fn a_symlink_under_the_root_to_outside_is_refused_for_callers() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    std::fs::create_dir(root.join("in")).unwrap();
    std::fs::create_dir(root.join("out")).unwrap();
    std::fs::write(root.join("in/a.jsonl"), "{\"id\":1}\n").unwrap();
    std::fs::write(root.join("out/secret.jsonl"), "{\"id\":2}\n").unwrap();
    std::os::unix::fs::symlink(root.join("out/secret.jsonl"), root.join("in/b.jsonl")).unwrap();
    let path = root.join("in").display().to_string();
    let policy = Arc::new(ReferencePolicy::deny_all().with_path_roots(vec![root.join("in")]));

    let src = FileSource::new(FileSourceConfig::new(&path)).unwrap();
    let err = faucet_core::http::with_reference_policy(
        Arc::clone(&policy),
        src.fetch_with_context(&HashMap::new()),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("outside the allowed path roots"),
        "{err}"
    );

    std::fs::remove_file(root.join("in/b.jsonl")).unwrap();
    std::os::unix::fs::symlink(root.join("in/a.jsonl"), root.join("in/b.jsonl")).unwrap();
    let src = FileSource::new(FileSourceConfig::new(&path)).unwrap();
    let rows =
        faucet_core::http::with_reference_policy(policy, src.fetch_with_context(&HashMap::new()))
            .await
            .unwrap();
    assert_eq!(rows.len(), 2, "a link that stays under the root is read");

    std::fs::remove_file(root.join("in/b.jsonl")).unwrap();
    std::os::unix::fs::symlink(root.join("out/secret.jsonl"), root.join("in/b.jsonl")).unwrap();
    let src = FileSource::new(FileSourceConfig::new(&path)).unwrap();
    assert_eq!(
        src.fetch_with_context(&HashMap::new()).await.unwrap().len(),
        2,
        "configs the host trusts keep following links"
    );
}

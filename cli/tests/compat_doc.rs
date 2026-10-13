//! `docs/book/src/reference/compatibility.md` is generated from the
//! `faucet_core::compat` table (#883). Regenerate with
//! `cargo test -p faucet-cli --test compat_doc -- --ignored regenerate`.

use std::path::PathBuf;

fn doc_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../docs/book/src/reference/compatibility.md")
}

#[test]
fn compatibility_page_is_fresh() {
    let committed = std::fs::read_to_string(doc_path()).expect("compatibility.md exists");
    assert_eq!(
        committed,
        faucet_core::compat::render_markdown(),
        "docs/book/src/reference/compatibility.md is stale — run \
         `cargo test -p faucet-cli --test compat_doc -- --ignored regenerate`"
    );
}

#[test]
#[ignore]
fn regenerate() {
    std::fs::write(doc_path(), faucet_core::compat::render_markdown()).expect("write");
}

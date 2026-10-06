//! The published container image must not start an unauthenticated control
//! plane by default (#789 SUPPLY-02).

use std::path::Path;

fn dockerfile() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../Dockerfile");
    std::fs::read_to_string(path).expect("Dockerfile at the repo root")
}

#[test]
fn the_default_command_keeps_serve_auth_on() {
    let text = dockerfile();
    let cmd: Vec<&str> = text
        .lines()
        .filter(|l| l.trim_start().starts_with("CMD "))
        .collect();
    assert_eq!(cmd, vec![r#"CMD ["serve"]"#], "{cmd:?}");
    let no_auth: Vec<&str> = text
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .filter(|l| l.contains("--no-auth"))
        .collect();
    assert!(no_auth.is_empty(), "{no_auth:?}");
}

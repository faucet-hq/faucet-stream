//! A remote hub snapshot carries the catalog's `index.json`, so an unpinned
//! locator resolves to the launched (`stable`) version rather than the newest
//! body, and the cache never drops what a concurrent run may be reading
//! (#789 CLI-19, CLI-102, CLI-104).
#![cfg(feature = "hub-remote")]

use faucet_cli::hub::remote::{CACHE_ENV, OFFLINE_ENV, fetch_cached, fetch_file_at};
use faucet_cli::hub::{HubLocation, catalog, locate};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const STABLE_BODY: &str = "kind: source-template\nname: acme\n# v1\n";
const NEWEST_BODY: &str = "kind: source-template\nname: acme\n# v2\n";

fn loc() -> HubLocation {
    HubLocation::parse("github:acme/hub").unwrap()
}

async fn mock(server: &MockServer, sha: &str, stable_commit: &str) {
    let base = server.uri();
    Mock::given(method("GET"))
        .and(path("/repos/acme/hub/commits/main"))
        .respond_with(ResponseTemplate::new(200).set_body_string(sha))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos/acme/hub/contents/source-templates"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
            "name": "acme.yaml", "type": "file", "path": "source-templates/acme.yaml",
            "download_url": format!("{base}/raw/acme.yaml")
        }])))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/raw/acme.yaml"))
        .respond_with(ResponseTemplate::new(200).set_body_string(NEWEST_BODY))
        .mount(server)
        .await;
    let index = json!({
        "commit": "e".repeat(40),
        "sources": [{"id": "acme", "name": "acme", "newest": 2, "stable": 1,
            "versions": [{"version": 1, "commit": stable_commit}, {"version": 2, "commit": "e".repeat(40)}]}],
        "sinks": []
    });
    Mock::given(method("GET"))
        .and(path("/repos/acme/hub/contents/index.json"))
        .and(header("Accept", "application/vnd.github.raw+json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(index.to_string()))
        .mount(server)
        .await;
}

#[tokio::test]
#[serial_test::serial(hub_env)]
async fn an_unpinned_remote_template_is_the_launched_version() {
    let server = MockServer::start().await;
    let stable_commit = "d".repeat(40);
    mock(&server, "0123456789abcdef", &stable_commit).await;
    let cache = tempfile::tempdir().unwrap();
    // SAFETY: serial(hub_env) — no other test reads these meanwhile.
    unsafe {
        std::env::remove_var(OFFLINE_ENV);
        std::env::set_var(CACHE_ENV, cache.path());
    }

    let snapshot = fetch_cached(&loc(), cache.path(), &server.uri())
        .await
        .expect("fetch");
    assert!(snapshot.join("index.json").is_file());

    // The launched version sits at an older commit: fetch it through the
    // mock and seed the pinned cache exactly where `locate` will look.
    let pinned = fetch_file_at(
        &loc(),
        cache.path(),
        &server.uri(),
        &stable_commit,
        "source-templates/acme.yaml",
    )
    .await;
    assert!(pinned.is_err(), "the mock has no route for the old commit");
    let seeded = cache
        .path()
        .join(loc().cache_key())
        .join("files")
        .join(&stable_commit)
        .join("source-templates/acme.yaml");
    std::fs::create_dir_all(seeded.parent().unwrap()).unwrap();
    std::fs::write(&seeded, STABLE_BODY).unwrap();

    let unpinned = locate("acme", &snapshot, catalog::SOURCE_DIR).await;
    let newest = locate("acme@newest", &snapshot, catalog::SOURCE_DIR).await;
    let bad = fetch_file_at(
        &loc(),
        cache.path(),
        &server.uri(),
        "/etc/passwd",
        "source-templates/acme.yaml",
    )
    .await;

    // A newer head keeps the previous snapshot for runs still reading it.
    server.reset().await;
    mock(&server, "fedcba9876543210", &stable_commit).await;
    let next = fetch_cached(&loc(), cache.path(), &server.uri())
        .await
        .expect("refetch");
    unsafe { std::env::remove_var(CACHE_ENV) };

    assert_eq!(
        std::fs::read_to_string(unpinned.expect("locate")).unwrap(),
        STABLE_BODY
    );
    assert_eq!(
        std::fs::read_to_string(newest.expect("locate newest")).unwrap(),
        NEWEST_BODY
    );
    assert!(
        bad.unwrap_err().to_string().contains("40-character"),
        "an index commit that is not a commit id is refused"
    );
    assert!(next.ends_with("fedcba9876543210"));
    assert!(
        snapshot.exists(),
        "the replaced snapshot is kept a generation"
    );
    assert!(seeded.exists(), "pinned versions survive a refetch");
}

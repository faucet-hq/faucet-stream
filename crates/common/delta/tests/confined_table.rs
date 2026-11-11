//! A caller run reads and writes a local Delta table only inside its
//! `table_uri`; a trusted run's store is the plain one.

use std::sync::Arc;

use deltalake::logstore::object_store::path::Path;
use deltalake::logstore::object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use faucet_common_delta::DeltaConnection;
use faucet_core::resolve::ReferencePolicy;
use futures::StreamExt;

fn fs_path(p: &std::path::Path) -> Path {
    Path::parse(p.to_string_lossy().trim_start_matches('/')).unwrap()
}

#[tokio::test]
async fn a_caller_run_stays_inside_its_table() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let table = root.join("t");
    std::fs::create_dir(&table).unwrap();
    std::fs::create_dir(root.join("other")).unwrap();
    std::fs::write(root.join("other/x.parquet"), b"PAR1").unwrap();
    let conn: DeltaConnection =
        serde_json::from_value(serde_json::json!({ "table_uri": table.display().to_string() }))
            .unwrap();
    deltalake::operations::create::CreateBuilder::new()
        .with_location(conn.location_string().unwrap())
        .with_columns([deltalake::kernel::StructField::new(
            "id",
            deltalake::kernel::DataType::LONG,
            true,
        )])
        .await
        .unwrap();

    let trusted = conn.open().await.unwrap();
    assert!(
        !trusted
            .log_store()
            .root_object_store(None)
            .to_string()
            .contains("Confined")
    );

    let policy = Arc::new(ReferencePolicy::deny_all().with_path_roots(vec![root.clone()]));
    let caller = faucet_core::http::with_reference_policy(policy, conn.open())
        .await
        .unwrap();
    let store: Arc<dyn ObjectStore> = caller.log_store().root_object_store(None);
    let inside = |rel: &str| fs_path(&table.join(rel));
    let outside = fs_path(&root.join("other/x.parquet"));
    let refused = |e: deltalake::logstore::object_store::Error| {
        let e = e.to_string();
        assert!(e.contains("outside the Delta table"), "{e}");
    };

    assert!(
        store
            .get(&inside("_delta_log/00000000000000000000.json"))
            .await
            .is_ok()
    );
    refused(store.get(&outside).await.unwrap_err());
    refused(store.head(&outside).await.unwrap_err());
    refused(store.get_range(&outside, 0..2).await.unwrap_err());
    refused(
        store
            .put(&outside, PutPayload::from_static(b"x"))
            .await
            .unwrap_err(),
    );
    store
        .put(&inside("data.txt"), PutPayload::from_static(b"x"))
        .await
        .unwrap();
    refused(store.copy(&inside("data.txt"), &outside).await.unwrap_err());
    refused(
        store
            .rename(&inside("data.txt"), &outside)
            .await
            .unwrap_err(),
    );
    store
        .copy(&inside("data.txt"), &inside("copy.txt"))
        .await
        .unwrap();
    store
        .rename(&inside("copy.txt"), &inside("moved.txt"))
        .await
        .unwrap();
    refused(store.delete(&outside).await.unwrap_err());
    store.delete(&inside("moved.txt")).await.unwrap();

    let under_table: Vec<_> = store.list(Some(&inside(""))).collect().await;
    assert!(under_table.iter().all(Result::is_ok));
    let outside_dir = fs_path(&root.join("other"));
    let listed: Vec<_> = store.list(Some(&outside_dir)).collect().await;
    assert!(listed.iter().any(Result::is_err));
    assert!(store.list_with_delimiter(Some(&outside_dir)).await.is_err());
    assert!(store.list_with_delimiter(Some(&inside(""))).await.is_ok());
    assert!(std::fs::read(root.join("other/x.parquet")).unwrap() == b"PAR1");
}

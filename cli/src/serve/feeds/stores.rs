//! The feed stores a server defines (#1034). A feed names one; `default`
//! follows the run history: a `feeds.db` beside a SQLite history file (with
//! its own lock file), or the history's PostgreSQL database through the
//! store's own connection pool. A store is opened when it is first used.

use crate::serve::config::{FeedSettings, HistoryBackendSpec, ServeConfig};
use crate::serve::history::feeds::DEFAULT_STORE;
use faucet_common_feed::FeedStore;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Where a named store lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreTarget {
    /// A SQLite file; one server per file.
    Sqlite(PathBuf),
    /// A PostgreSQL database.
    Postgres(String),
}

impl StoreTarget {
    /// Parse `sqlite:<path>` or `postgres://…`.
    pub fn parse(url: &str) -> Result<Self, String> {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            return Ok(Self::Postgres(url.to_string()));
        }
        match crate::serve::load::sqlite_file(url) {
            Some(path) => Ok(Self::Sqlite(path)),
            None => Err(format!(
                "feed store '{url}': use sqlite:<path> or postgres://…"
            )),
        }
    }

    /// Whether several servers may share it.
    pub fn cluster_safe(&self) -> bool {
        matches!(self, Self::Postgres(_))
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Sqlite(_) => "sqlite",
            Self::Postgres(_) => "postgres",
        }
    }
}

/// Why the server has no `default` store.
pub const NO_DEFAULT_STORE: &str = "feeds need a run history that survives a restart: with \
     --history memory a feed's definition would be lost while its events stayed. Start the \
     server with a SQLite or PostgreSQL history";

/// The `default` store's target for a history backend.
pub fn default_target(history: &HistoryBackendSpec) -> Option<StoreTarget> {
    match history {
        HistoryBackendSpec::Memory => None,
        HistoryBackendSpec::Postgres(url) => Some(StoreTarget::Postgres(url.clone())),
        HistoryBackendSpec::Sqlite(url) => {
            let file = crate::serve::load::sqlite_file(url)?;
            let dir = file.parent().filter(|p| !p.as_os_str().is_empty());
            Some(StoreTarget::Sqlite(
                dir.unwrap_or_else(|| Path::new(".")).join("feeds.db"),
            ))
        }
    }
}

/// Every store the settings define, by name. `Err` names the first problem.
pub fn targets(
    settings: &FeedSettings,
    history: &HistoryBackendSpec,
) -> Result<BTreeMap<String, StoreTarget>, String> {
    let mut out = BTreeMap::new();
    if let Some(t) = default_target(history) {
        out.insert(DEFAULT_STORE.to_string(), t);
    }
    for (name, url) in &settings.stores {
        crate::serve::history::tenants::validate_slug(name, "feed store name", 64)?;
        out.insert(name.clone(), StoreTarget::parse(url)?);
    }
    Ok(out)
}

/// The startup refusals of a configuration that could lose or split feeds.
pub fn check_config(
    settings: &FeedSettings,
    history: &HistoryBackendSpec,
    cluster: bool,
) -> Result<(), String> {
    let all = targets(settings, history)?;
    if settings.path.is_some() && !all.contains_key(DEFAULT_STORE) {
        return Err(format!("--feeds: {NO_DEFAULT_STORE}"));
    }
    if cluster {
        let unsafe_named = settings
            .stores
            .keys()
            .find(|n| all.get(*n).is_some_and(|t| !t.cluster_safe()));
        if let Some(name) = unsafe_named {
            return Err(not_cluster_safe(name));
        }
        if settings.path.is_some() && all.get(DEFAULT_STORE).is_some_and(|t| !t.cluster_safe()) {
            return Err(not_cluster_safe(DEFAULT_STORE));
        }
    }
    Ok(())
}

fn not_cluster_safe(name: &str) -> String {
    format!(
        "feed store '{name}' is a SQLite file, which only one server may use; a cluster needs \
         a PostgreSQL feed store (a PostgreSQL --history, or --feed-store {name}=postgres://…)"
    )
}

/// One store's slot: opened once, by whoever asks first.
type Cell = Arc<tokio::sync::OnceCell<Arc<dyn FeedStore>>>;

/// The stores of one server: targets by name, opened on first use.
pub struct Stores {
    targets: BTreeMap<String, StoreTarget>,
    cluster: bool,
    cells: Mutex<BTreeMap<String, Cell>>,
    opened: Mutex<BTreeMap<String, Arc<dyn FeedStore>>>,
    locks: Mutex<Vec<std::fs::File>>,
}

impl std::fmt::Debug for Stores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stores")
            .field("targets", &self.targets.keys().collect::<Vec<_>>())
            .field("cluster", &self.cluster)
            .finish()
    }
}

impl Stores {
    /// The stores `config` defines. An invalid setting leaves that store out
    /// (startup validation refuses it first).
    pub fn from_config(config: &ServeConfig) -> Self {
        Self::new(
            targets(&config.feeds, &config.history).unwrap_or_default(),
            config.cluster.enabled,
        )
    }

    pub fn new(targets: BTreeMap<String, StoreTarget>, cluster: bool) -> Self {
        Self {
            targets,
            cluster,
            cells: Mutex::new(BTreeMap::new()),
            opened: Mutex::new(BTreeMap::new()),
            locks: Mutex::new(Vec::new()),
        }
    }

    /// The names a feed may use.
    pub fn names(&self) -> Vec<String> {
        self.targets.keys().cloned().collect()
    }

    pub fn target(&self, name: &str) -> Option<&StoreTarget> {
        self.targets.get(name)
    }

    /// Why `name` cannot be used, before anything is opened.
    pub fn refusal(&self, name: &str) -> Option<String> {
        match self.targets.get(name) {
            None if name == DEFAULT_STORE => Some(NO_DEFAULT_STORE.to_string()),
            None => Some(format!(
                "feed store '{name}' is not defined on this server (defined: {})",
                self.names().join(", ")
            )),
            Some(t) if self.cluster && !t.cluster_safe() => Some(not_cluster_safe(name)),
            Some(_) => None,
        }
    }

    /// The stores opened so far.
    pub fn opened(&self) -> Vec<(String, Arc<dyn FeedStore>)> {
        self.opened
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(n, s)| (n.clone(), s.clone()))
            .collect()
    }

    /// Use `store` as the store named `name` (tests, and a host that builds
    /// its own).
    pub async fn install(&self, name: &str, store: Arc<dyn FeedStore>) {
        let cell = Cell::default();
        let _ = cell.set(store.clone());
        self.cells
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.to_string(), cell);
        self.opened
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.to_string(), store);
    }

    /// The store named `name`, opened on first use; callers asking for the
    /// same store share one open, and another store's open never waits on
    /// it. `Err` is user-facing.
    pub async fn get(&self, name: &str) -> Result<Arc<dyn FeedStore>, String> {
        let cell = self
            .cells
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(name.to_string())
            .or_default()
            .clone();
        cell.get_or_try_init(|| self.open_named(name))
            .await
            .cloned()
    }

    async fn open_named(&self, name: &str) -> Result<Arc<dyn FeedStore>, String> {
        if let Some(why) = self.refusal(name) {
            return Err(why);
        }
        let target = self
            .targets
            .get(name)
            .ok_or_else(|| format!("feed store '{name}' is not defined on this server"))?;
        let store = self.open_target(name, target).await?;
        if self.cluster && !store.capabilities().cluster_safe {
            return Err(not_cluster_safe(name));
        }
        self.opened
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.to_string(), store.clone());
        Ok(store)
    }

    async fn open_target(
        &self,
        name: &str,
        target: &StoreTarget,
    ) -> Result<Arc<dyn FeedStore>, String> {
        match target {
            StoreTarget::Sqlite(path) => {
                let lock = claim_file(path)?;
                let store = faucet_common_feed::SqliteFeedStore::open(
                    path,
                    &faucet_common_feed::SqliteOptions::new(),
                )
                .await
                .map_err(|e| format!("opening feed store '{name}' ({}): {e}", path.display()))?;
                self.locks
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(lock);
                tracing::info!(store = name, path = %path.display(), "feed store opened");
                Ok(Arc::new(store))
            }
            #[cfg(feature = "serve-history-postgres")]
            StoreTarget::Postgres(url) => {
                let store = faucet_common_feed::PostgresFeedStore::connect(
                    url,
                    &faucet_common_feed::PostgresOptions::new(),
                )
                .await
                .map_err(|e| {
                    crate::secrets::registry::redact_message(&format!(
                        "opening feed store '{name}': {e}"
                    ))
                })?;
                tracing::info!(store = name, "feed store opened (postgres)");
                Ok(Arc::new(store))
            }
            #[cfg(not(feature = "serve-history-postgres"))]
            StoreTarget::Postgres(_) => Err(format!(
                "feed store '{name}' is a PostgreSQL database, which needs a build with the \
                 `serve-history-postgres` feature"
            )),
        }
    }
}

/// The files of a SQLite store that no submitted config may touch.
pub fn protected_files(path: &Path) -> Vec<PathBuf> {
    ["", "-wal", "-shm", "-journal", ".lock"]
        .iter()
        .map(|suffix| PathBuf::from(format!("{}{suffix}", path.display())))
        .collect()
}

/// Create the store's directory (owner-only) and take `<file>.lock` for the
/// life of the server, so a second server on the same file is refused.
fn claim_file(path: &Path) -> Result<std::fs::File, String> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let unwritable =
        |e: std::io::Error| format!("cannot keep the feed store in {}: {e}", dir.display());
    if !dir.exists() {
        std::fs::create_dir_all(dir).map_err(unwritable)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
                .map_err(unwritable)?;
        }
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(format!("{}.lock", path.display()))
        .map_err(unwritable)?;
    match lock.try_lock() {
        Ok(()) => Ok(lock),
        Err(std::fs::TryLockError::WouldBlock) => Err(format!(
            "another faucet server is using the feed store {}; a SQLite feed store takes one \
             server",
            path.display()
        )),
        Err(std::fs::TryLockError::Error(e)) => Err(unwritable(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(stores: &[(&str, &str)], file: bool) -> FeedSettings {
        FeedSettings {
            path: file.then(|| PathBuf::from("feeds.yaml")),
            stores: stores
                .iter()
                .map(|(n, u)| (n.to_string(), u.to_string()))
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn the_default_store_follows_the_history() {
        assert_eq!(default_target(&HistoryBackendSpec::Memory), None);
        assert_eq!(
            default_target(&HistoryBackendSpec::Sqlite("sqlite::memory:".into())),
            None
        );
        assert_eq!(
            default_target(&HistoryBackendSpec::Sqlite(
                "sqlite:/data/history.db".into()
            )),
            Some(StoreTarget::Sqlite("/data/feeds.db".into()))
        );
        assert_eq!(
            default_target(&HistoryBackendSpec::Sqlite("sqlite:runs.db".into())),
            Some(StoreTarget::Sqlite("./feeds.db".into()))
        );
        let pg = HistoryBackendSpec::Postgres("postgres://h/db".into());
        assert_eq!(
            default_target(&pg),
            Some(StoreTarget::Postgres("postgres://h/db".into()))
        );
        let t = default_target(&pg).unwrap();
        assert!(t.cluster_safe());
        assert_eq!(t.kind(), "postgres");
        let s = StoreTarget::parse("sqlite:/x/f.db").unwrap();
        assert!(!s.cluster_safe());
        assert_eq!(s.kind(), "sqlite");
        assert!(StoreTarget::parse("postgresql://h/db").is_ok());
        assert!(
            StoreTarget::parse("mysql://h/db")
                .unwrap_err()
                .contains("use sqlite:")
        );
    }

    #[test]
    fn startup_refuses_what_could_lose_or_split_feeds() {
        let mem = HistoryBackendSpec::Memory;
        let lite = HistoryBackendSpec::Sqlite("sqlite:/data/history.db".into());
        let pg = HistoryBackendSpec::Postgres("postgres://h/db".into());
        check_config(&settings(&[], false), &mem, false).unwrap();
        assert!(
            check_config(&settings(&[], true), &mem, false)
                .unwrap_err()
                .contains("--history memory")
        );
        check_config(&settings(&[("big", "sqlite:/b.db")], true), &lite, false).unwrap();
        // A cluster on a SQLite history starts as before while it has no feeds.
        check_config(&settings(&[], false), &lite, true).unwrap();
        assert!(
            check_config(&settings(&[], true), &lite, true)
                .unwrap_err()
                .contains("feed store 'default' is a SQLite file")
        );
        assert!(
            check_config(&settings(&[("big", "sqlite:/b.db")], false), &pg, true)
                .unwrap_err()
                .contains("feed store 'big'")
        );
        check_config(&settings(&[("big", "postgres://o/db")], true), &pg, true).unwrap();
        assert!(
            check_config(
                &settings(&[("Bad Name", "sqlite:/b.db")], false),
                &lite,
                false
            )
            .unwrap_err()
            .contains("feed store name")
        );
        assert!(check_config(&settings(&[("x", "nope")], false), &lite, false).is_err());
    }

    #[tokio::test]
    async fn a_store_opens_on_first_use_under_its_own_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("feeds.db");
        let targets: BTreeMap<String, StoreTarget> =
            [(DEFAULT_STORE.to_string(), StoreTarget::Sqlite(path.clone()))].into();
        let stores = Stores::new(targets.clone(), false);
        assert_eq!(stores.names(), vec![DEFAULT_STORE]);
        assert!(stores.opened().is_empty());
        assert!(!path.exists(), "nothing is created before first use");
        let a = stores.get(DEFAULT_STORE).await.unwrap();
        assert!(path.exists());
        assert_eq!(a.capabilities().backend, "sqlite");
        let again = stores.get(DEFAULT_STORE).await.unwrap();
        assert!(Arc::ptr_eq(&a, &again));
        assert_eq!(stores.opened().len(), 1);
        assert!(format!("{stores:?}").contains("default"));

        // A second server on the same file is refused.
        let other = Stores::new(targets.clone(), false);
        let err = other.get(DEFAULT_STORE).await.err().unwrap();
        assert!(err.contains("another faucet server"), "{err}");

        assert!(
            stores
                .get("big")
                .await
                .err()
                .unwrap()
                .contains("is not defined on this server (defined: default)")
        );
        let none = Stores::new(BTreeMap::new(), false);
        assert_eq!(
            none.refusal(DEFAULT_STORE).as_deref(),
            Some(NO_DEFAULT_STORE)
        );
        let clustered = Stores::new(targets, true);
        assert!(
            clustered
                .get(DEFAULT_STORE)
                .await
                .err()
                .unwrap()
                .contains("only one server may use")
        );
        assert!(clustered.target(DEFAULT_STORE).is_some());
        assert_eq!(protected_files(&path).len(), 5);

        let installed = Stores::new(BTreeMap::new(), true);
        installed.install("mine", a.clone()).await;
        assert!(Arc::ptr_eq(&installed.get("mine").await.unwrap(), &a));
        assert_eq!(installed.opened().len(), 1);
    }

    #[cfg(not(feature = "serve-history-postgres"))]
    #[tokio::test]
    async fn a_postgres_store_needs_the_postgres_build() {
        let stores = Stores::new(
            [(
                "pg".to_string(),
                StoreTarget::Postgres("postgres://h/db".into()),
            )]
            .into(),
            false,
        );
        assert!(
            stores
                .get("pg")
                .await
                .err()
                .unwrap()
                .contains("serve-history-postgres")
        );
    }
}

//! Schedules for `faucet serve` (#1044): run a registered template on a cron
//! schedule. Separate from triggers (which react to events outside faucet):
//! a schedule fires on time alone.
//!
//! Schedules come from two places, merged by name:
//! - the `--schedules <file>` (read-only at runtime; edit the file and
//!   restart to change one), and
//! - the API / console (`/v1/schedules`), stored in the run history so every
//!   cluster member ticks the same set.
//!
//! [`runner`] is the tick loop, [`fire`] submits one tick, [`status`] is the
//! shared record of recent ticks, [`service`] holds the API's rules
//! (permissions, tenant scoping, limits, approval) and [`validate`] the
//! definition checks every path shares.

pub mod fire;
pub mod metrics;
pub mod runner;
pub mod secrets;
pub mod service;
pub mod status;
pub mod validate;

use crate::error::{CliError, CliResult};
use crate::serve::history::schedules::ScheduleSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Where a schedule is defined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// Made through the API or the console; stored in the run history.
    Console,
    /// Listed in the `--schedules` file; read-only at runtime.
    File,
}

/// The `--schedules` file.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulesFile {
    /// Schema version; must be `1`.
    pub version: u32,
    /// The schedules. Names are unique.
    #[serde(default)]
    pub schedules: Vec<ScheduleSpec>,
}

/// Validate a parsed file. `Err` is user-facing.
pub fn check_file(file: &SchedulesFile) -> Result<(), String> {
    if file.version != 1 {
        return Err(format!(
            "schedules: unsupported version {} (expected 1)",
            file.version
        ));
    }
    let mut names = std::collections::HashSet::new();
    for s in &file.schedules {
        validate::validate(s).map_err(|e| format!("schedules: '{}': {e}", s.name))?;
        if !names.insert(s.name.as_str()) {
            return Err(format!("schedules: duplicate schedule name '{}'", s.name));
        }
    }
    Ok(())
}

/// Load + validate a `--schedules` file (YAML or JSON).
pub async fn load_file(path: &std::path::Path) -> CliResult<SchedulesFile> {
    let text = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| CliError::Serve(format!("reading schedules file {}: {e}", path.display())))?;
    crate::serve::load::protect_file(path);
    let is_json = path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("json"));
    let file: SchedulesFile = if is_json {
        serde_json::from_str(&text)
            .map_err(|e| CliError::Serve(format!("parsing schedules JSON: {e}")))?
    } else {
        crate::yaml::from_str(&text)
            .map_err(|e| CliError::Serve(format!("parsing schedules YAML: {e}")))?
    };
    check_file(&file).map_err(CliError::Serve)?;
    Ok(file)
}

/// The schedules runtime a server carries: the file's schedules, the
/// tenant guardrail, and the signal that wakes the loop after a local change.
pub struct SchedulesRuntime {
    file: Vec<ScheduleSpec>,
    tenant_min_interval: Duration,
    changed: Arc<tokio::sync::Notify>,
}

impl Default for SchedulesRuntime {
    fn default() -> Self {
        Self::new(Vec::new(), crate::serve::config::DEFAULT_TENANT_SCHEDULE_MIN_INTERVAL)
    }
}

impl SchedulesRuntime {
    pub fn new(file: Vec<ScheduleSpec>, tenant_min_interval: Duration) -> Self {
        Self {
            file,
            tenant_min_interval,
            changed: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// The `--schedules` file's schedules.
    pub fn file(&self) -> &[ScheduleSpec] {
        &self.file
    }

    pub fn file_schedule(&self, name: &str) -> Option<&ScheduleSpec> {
        self.file.iter().find(|s| s.name == name)
    }

    /// The shortest gap between a tenant-owned schedule's ticks.
    pub fn tenant_min_interval(&self) -> Duration {
        self.tenant_min_interval
    }

    /// The signal the loop waits on.
    pub fn changed(&self) -> Arc<tokio::sync::Notify> {
        self.changed.clone()
    }

    /// Wake the loop: a schedule changed on this server.
    pub fn notify(&self) {
        self.changed.notify_one();
    }
}

/// Start the tick loop.
pub fn spawn(
    state: crate::serve::state::ServerState,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(runner::run(state, shutdown))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_file_loads_and_every_problem_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let ok = dir.path().join("s.yaml");
        std::fs::write(
            &ok,
            "version: 1\nschedules:\n  - name: nightly\n    cron: '0 2 * * *'\n    timezone: Europe/Berlin\n    template: { id: acme/billing }\n",
        )
        .unwrap();
        let f = load_file(&ok).await.unwrap();
        assert_eq!(f.schedules[0].timezone, "Europe/Berlin");

        let json = dir.path().join("s.json");
        std::fs::write(&json, r#"{"version": 1, "schedules": []}"#).unwrap();
        assert!(load_file(&json).await.unwrap().schedules.is_empty());

        let cases = [
            ("version: 2\nschedules: []\n", "unsupported version 2"),
            ("version: 1\nschedules:\n  - {name: a, cron: 'x', template: {id: t}}\n", "'a'"),
            (
                "version: 1\nschedules:\n  - {name: a, cron: '0 * * * *', template: {id: t}}\n  - {name: a, cron: '0 * * * *', template: {id: t}}\n",
                "duplicate schedule name 'a'",
            ),
            ("version: 1\nschedule: []\n", "parsing schedules YAML"),
        ];
        for (text, want) in cases {
            let p = dir.path().join("bad.yaml");
            std::fs::write(&p, text).unwrap();
            let err = load_file(&p).await.unwrap_err().to_string();
            assert!(err.contains(want), "{want}: {err}");
        }
        let bad_json = dir.path().join("bad.json");
        std::fs::write(&bad_json, "{").unwrap();
        assert!(load_file(&bad_json).await.unwrap_err().to_string().contains("parsing schedules JSON"));
        assert!(
            load_file(&dir.path().join("missing.yaml"))
                .await
                .unwrap_err()
                .to_string()
                .contains("reading schedules file")
        );
    }

    #[test]
    fn the_runtime_exposes_the_file_and_the_guardrail() {
        let spec: ScheduleSpec = serde_json::from_value(serde_json::json!({
            "name": "n", "cron": "0 * * * *", "template": {"id": "t"}
        }))
        .unwrap();
        let rt = SchedulesRuntime::new(vec![spec], Duration::from_secs(60));
        assert_eq!(rt.file().len(), 1);
        assert!(rt.file_schedule("n").is_some());
        assert!(rt.file_schedule("x").is_none());
        assert_eq!(rt.tenant_min_interval(), Duration::from_secs(60));
        rt.notify();
        let d = SchedulesRuntime::default();
        assert!(d.file().is_empty());
        assert_eq!(
            d.tenant_min_interval(),
            crate::serve::config::DEFAULT_TENANT_SCHEDULE_MIN_INTERVAL
        );
    }
}

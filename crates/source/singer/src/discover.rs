//! Singer catalog discovery: run `<tap> --config <tmp> --discover` and return
//! the catalog it prints. Used by `faucet init --source singer --discover`.

use std::process::Stdio;

use faucet_core::{FaucetError, Value};
use tokio::process::Command;

use crate::config::SingerSourceConfig;
use crate::process::write_temp;

/// Run the tap in discovery mode and parse its stdout as a Singer catalog.
///
/// Spawns `<executable> --config <tmp> --discover <args…>`, materializing
/// `tap_config` to a private temp file. A non-zero exit or non-JSON output is a
/// [`FaucetError::Source`] (with the tap's stderr on failure).
pub async fn discover(config: &SingerSourceConfig) -> Result<Value, FaucetError> {
    let config_file = write_temp("config", &config.tap_config)?;

    let mut command = Command::new(&config.executable);
    command
        .arg("--config")
        .arg(config_file.path())
        .arg("--discover")
        .args(&config.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(env) = config.inherit_env.from_process() {
        command.env_clear().envs(env);
    }
    // The child is killed if the deadline drops the future (API-45).
    command.kill_on_drop(true);
    let redactor = crate::process::Redactor::from_config(&config.tap_config);
    let spawn_err = |e: std::io::Error| {
        FaucetError::Source(format!(
            "failed to spawn tap '{}' for discovery: {e}",
            redactor.redact(&config.executable)
        ))
    };
    let child = faucet_common_singer::spawn_command(&mut command).map_err(spawn_err)?;
    let run = child.wait_with_output();
    let output = match config.idle_timeout_secs {
        Some(secs) => tokio::time::timeout(std::time::Duration::from_secs(secs), run)
            .await
            .map_err(|_| {
                FaucetError::Source(format!(
                    "tap discovery did not finish within {secs}s (idle_timeout_secs)"
                ))
            })?,
        None => run.await,
    }
    .map_err(|e| FaucetError::Source(format!("tap discovery failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // A tap echoing its own credentials must not put them in the error.
        let stderr = faucet_core::redact::redact(&redactor.redact(stderr.trim()));
        return Err(FaucetError::Source(format!(
            "tap discovery exited with status {}; stderr:\n{stderr}",
            output.status
        )));
    }

    serde_json::from_slice(&output.stdout)
        .map_err(|e| FaucetError::Source(format!("tap discovery output is not valid JSON: {e}")))
}

/// Result of [`select_streams`]: the rewritten catalog plus what was selected
/// and any warnings the caller should surface to the user.
#[derive(Debug, Clone)]
pub struct StreamSelection {
    /// The catalog with selection metadata applied.
    pub catalog: Value,
    /// Stream ids marked `selected` (the target plus any inferred parents).
    pub selected: Vec<String>,
    /// Human-facing warnings (target missing, uninferrable parents, …).
    pub warnings: Vec<String>,
}

/// Mark `target` — and any **parent** streams inferable from the catalog — as
/// `selected`, returning the rewritten catalog.
///
/// Most database / Meltano-SDK taps sync **nothing** unless a stream is
/// explicitly selected in the catalog, so a passed-through unselected catalog is
/// a silent no-op. Parent-keyed taps (e.g. `tap-github`'s `issues` needs
/// `repositories`) additionally require the parent stream selected even though
/// faucet only emits the configured child stream.
///
/// Parent relationships are inferred, best-effort, from a `parent_stream` /
/// `parent` field on the stream object or its stream-level (`breadcrumb: []`)
/// metadata — the places taps that expose the relationship put it. When no such
/// hint exists but the catalog has other streams, a warning is emitted so the
/// user knows a parent may need manual selection.
pub fn select_streams(catalog: &Value, target: &str) -> StreamSelection {
    let mut catalog = catalog.clone();
    let mut selected: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    let ids = catalog_stream_ids(&catalog);
    if !ids.iter().any(|s| s == target) {
        warnings.push(format!(
            "stream '{target}' not found in the discovered catalog; available: {}",
            if ids.is_empty() {
                "(none)".to_string()
            } else {
                ids.join(", ")
            }
        ));
        return StreamSelection {
            catalog,
            selected,
            warnings,
        };
    }

    // Transitive closure of parents, starting from the target.
    let mut to_select = vec![target.to_string()];
    let mut i = 0;
    while i < to_select.len() {
        let cur = to_select[i].clone();
        if let Some(parent) = parent_stream_id(&catalog, &cur)
            && !to_select.contains(&parent)
        {
            if ids.iter().any(|s| s == &parent) {
                to_select.push(parent);
            } else {
                warnings.push(format!(
                    "stream '{cur}' references parent '{parent}', which is not in the \
                     catalog — select the parent manually"
                ));
            }
        }
        i += 1;
    }

    if let Some(streams) = catalog.get_mut("streams").and_then(Value::as_array_mut) {
        for stream in streams.iter_mut() {
            if let Some(id) = stream_id(stream)
                && to_select.contains(&id)
            {
                mark_selected(stream);
                if !selected.contains(&id) {
                    selected.push(id);
                }
            }
        }
    }

    // We selected only the target, the catalog has other streams, and none of
    // them expose a parent relationship we could follow — warn that a
    // parent-child tap may need manual selection.
    if selected.len() == 1 && ids.len() > 1 && !catalog_exposes_parent_metadata(&catalog) {
        warnings.push(format!(
            "selected only '{target}'. If this tap has parent/child streams, the catalog does \
             not express the relationship — a required parent may need manual selection. \
             Available streams: {}",
            ids.join(", ")
        ));
    }

    StreamSelection {
        catalog,
        selected,
        warnings,
    }
}

/// A stream's id: `tap_stream_id`, falling back to `stream`.
fn stream_id(stream: &Value) -> Option<String> {
    stream
        .get("tap_stream_id")
        .or_else(|| stream.get("stream"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The stream-level (`breadcrumb: []`) metadata object of a stream, if present.
fn stream_level_metadata(stream: &Value) -> Option<&Value> {
    stream
        .get("metadata")
        .and_then(Value::as_array)?
        .iter()
        .find(|m| {
            m.get("breadcrumb")
                .and_then(Value::as_array)
                .map(|b| b.is_empty())
                .unwrap_or(false)
        })
        .and_then(|m| m.get("metadata"))
}

/// Best-effort parent-stream id for `id`, from a `parent_stream` / `parent`
/// field on the stream object or its stream-level metadata.
fn parent_stream_id(catalog: &Value, id: &str) -> Option<String> {
    let stream = catalog
        .get("streams")
        .and_then(Value::as_array)?
        .iter()
        .find(|s| stream_id(s).as_deref() == Some(id))?;

    for key in ["parent_stream", "parent"] {
        if let Some(p) = stream.get(key).and_then(Value::as_str) {
            return Some(p.to_string());
        }
        if let Some(p) = stream_level_metadata(stream)
            .and_then(|m| m.get(key))
            .and_then(Value::as_str)
        {
            return Some(p.to_string());
        }
    }
    None
}

/// Whether any stream in the catalog exposes a parent relationship.
fn catalog_exposes_parent_metadata(catalog: &Value) -> bool {
    catalog
        .get("streams")
        .and_then(Value::as_array)
        .map(|streams| {
            streams
                .iter()
                .filter_map(stream_id)
                .any(|id| parent_stream_id(catalog, &id).is_some())
        })
        .unwrap_or(false)
}

/// Mark a single stream `selected`, both stream-level (legacy taps read
/// `stream.selected`) and in its `breadcrumb: []` metadata (Meltano-SDK taps
/// read the metadata), creating the metadata entry if absent.
fn mark_selected(stream: &mut Value) {
    if let Some(obj) = stream.as_object_mut() {
        obj.insert("selected".to_string(), Value::Bool(true));

        let metadata = obj
            .entry("metadata")
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Some(arr) = metadata.as_array_mut() {
            let root = arr.iter_mut().find(|m| {
                m.get("breadcrumb")
                    .and_then(Value::as_array)
                    .map(|b| b.is_empty())
                    .unwrap_or(false)
            });
            match root {
                Some(entry) => {
                    if let Some(md) = entry.get_mut("metadata").and_then(Value::as_object_mut) {
                        md.insert("selected".to_string(), Value::Bool(true));
                    } else if let Some(eobj) = entry.as_object_mut() {
                        eobj.insert(
                            "metadata".to_string(),
                            serde_json::json!({ "selected": true }),
                        );
                    }
                }
                None => arr.push(serde_json::json!({
                    "breadcrumb": [],
                    "metadata": { "selected": true }
                })),
            }
        }
    }
}

/// Extract the stream ids from a Singer catalog (`tap_stream_id`, falling back
/// to `stream`), in catalog order.
pub fn catalog_stream_ids(catalog: &Value) -> Vec<String> {
    catalog
        .get("streams")
        .and_then(Value::as_array)
        .map(|streams| {
            streams
                .iter()
                .filter_map(|s| {
                    s.get("tap_stream_id")
                        .or_else(|| s.get("stream"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_stream_ids_in_order() {
        let catalog = json!({
            "streams": [
                { "tap_stream_id": "users", "stream": "users" },
                { "stream": "orders" },
                { "tap_stream_id": "audit_log" }
            ]
        });
        assert_eq!(
            catalog_stream_ids(&catalog),
            vec!["users", "orders", "audit_log"]
        );
        assert!(catalog_stream_ids(&json!({})).is_empty());
    }

    /// Whether a stream is marked selected via its `breadcrumb: []` metadata.
    fn is_selected(catalog: &Value, id: &str) -> bool {
        catalog
            .get("streams")
            .and_then(Value::as_array)
            .and_then(|streams| streams.iter().find(|s| stream_id(s).as_deref() == Some(id)))
            .and_then(stream_level_metadata)
            .and_then(|m| m.get("selected"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    #[test]
    fn selects_target_and_creates_metadata_when_absent() {
        // A bare stream with no metadata array at all.
        let catalog = json!({ "streams": [ { "tap_stream_id": "users", "schema": {} } ] });
        let sel = select_streams(&catalog, "users");
        assert_eq!(sel.selected, vec!["users"]);
        assert!(is_selected(&sel.catalog, "users"));
        // Legacy stream-level flag is set too.
        assert_eq!(sel.catalog["streams"][0]["selected"], json!(true));
        assert!(sel.warnings.is_empty(), "single-stream tap: no warning");
    }

    #[test]
    fn includes_parent_stream_referenced_in_metadata() {
        let catalog = json!({
            "streams": [
                { "tap_stream_id": "repositories", "schema": {},
                  "metadata": [{ "breadcrumb": [], "metadata": {} }] },
                { "tap_stream_id": "issues", "schema": {},
                  "metadata": [{ "breadcrumb": [], "metadata": { "parent_stream": "repositories" } }] }
            ]
        });
        let sel = select_streams(&catalog, "issues");
        assert!(sel.selected.contains(&"issues".to_string()));
        assert!(
            sel.selected.contains(&"repositories".to_string()),
            "parent must be auto-selected: {:?}",
            sel.selected
        );
        assert!(is_selected(&sel.catalog, "issues"));
        assert!(is_selected(&sel.catalog, "repositories"));
        // Parent relationship was inferable → no manual-selection warning.
        assert!(sel.warnings.is_empty(), "warnings: {:?}", sel.warnings);
    }

    #[test]
    fn follows_parent_field_on_stream_object() {
        let catalog = json!({
            "streams": [
                { "tap_stream_id": "accounts", "schema": {} },
                { "tap_stream_id": "contacts", "parent": "accounts", "schema": {} }
            ]
        });
        let sel = select_streams(&catalog, "contacts");
        assert!(sel.selected.contains(&"accounts".to_string()));
    }

    #[test]
    fn warns_when_target_missing() {
        let catalog = json!({ "streams": [ { "tap_stream_id": "users" } ] });
        let sel = select_streams(&catalog, "nope");
        assert!(sel.selected.is_empty());
        assert_eq!(sel.warnings.len(), 1);
        assert!(sel.warnings[0].contains("not found"));
        assert!(sel.warnings[0].contains("users"));
    }

    #[test]
    fn warns_when_multiple_streams_but_no_parent_metadata() {
        let catalog = json!({
            "streams": [
                { "tap_stream_id": "issues", "schema": {} },
                { "tap_stream_id": "repositories", "schema": {} }
            ]
        });
        let sel = select_streams(&catalog, "issues");
        assert_eq!(sel.selected, vec!["issues"]);
        assert_eq!(sel.warnings.len(), 1);
        assert!(sel.warnings[0].contains("manual selection"));
        assert!(sel.warnings[0].contains("repositories"));
    }

    #[test]
    fn warns_when_referenced_parent_absent() {
        let catalog = json!({
            "streams": [
                { "tap_stream_id": "issues", "parent": "repositories", "schema": {} }
            ]
        });
        let sel = select_streams(&catalog, "issues");
        assert_eq!(sel.selected, vec!["issues"]);
        assert!(
            sel.warnings
                .iter()
                .any(|w| w.contains("not in the catalog")),
            "warnings: {:?}",
            sel.warnings
        );
    }
}

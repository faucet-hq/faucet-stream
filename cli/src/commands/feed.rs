//! `faucet feed` (#1034): manage feeds straight in their stores, the way
//! `faucet template` manages templates. `--store` names the run history a
//! server keeps its feed definitions in; the events are in the feed store
//! that history implies (or a `--feed-store`). Every verb goes through the
//! rules the HTTP API uses (`serve::feeds::service`); nothing here starts
//! delivery runs: a server does.

use crate::cli::{FeedArgs, FeedCommand, FeedTokenCommand};
use crate::error::{CliError, CliResult};
use crate::serve::feeds::service::{self, Position};
use crate::serve::feeds::{FeedsRuntime, ingest, spec, stores};
use crate::serve::history::feeds::FeedSpec;
use crate::serve::rbac::{AuthContext, Role};
use crate::serve::state::ServerState;
use clap::Parser;
use serde::Serialize;
use std::io::Read;

/// Who a `faucet feed` change is made by.
const PRINCIPAL: &str = "cli";

fn failed(e: crate::serve::error::ServeError) -> CliError {
    CliError::Serve(e.api_error().error.message)
}

/// The server-shaped state the feed rules run against: the history at
/// `--store`, the feed stores it implies and the vault key, with no
/// scheduler.
async fn open(args: &FeedArgs) -> CliResult<ServerState> {
    let store = args.store.as_deref().ok_or_else(|| {
        CliError::Config(
            "pass --store <URL> (or FAUCET_FEED_STORE): the run history the server keeps its \
             feeds in, e.g. sqlite:/var/lib/faucet/history.db or postgres://…"
                .into(),
        )
    })?;
    let mut argv: Vec<String> = ["faucet", "serve", "--no-auth", "--no-env-file", "--history"]
        .map(String::from)
        .to_vec();
    argv.push(store.to_string());
    for s in &args.feed_store {
        argv.extend(["--feed-store".to_string(), s.clone()]);
    }
    if let Some(key) = &args.vault_key {
        argv.extend(["--vault-key".to_string(), key.clone()]);
    }
    let cli = crate::cli::Cli::try_parse_from(argv).map_err(|e| CliError::Config(e.to_string()))?;
    let crate::cli::Command::Serve(serve) = cli.command else {
        return Err(CliError::Config(
            "could not build the store settings".into(),
        ));
    };
    let config = crate::serve::config::ServeConfig::from_args(*serve)?;
    let history = crate::serve::history::connect(
        &config.history,
        config.idempotency_retention,
        config.lease_ttl,
        &uuid::Uuid::new_v4().to_string(),
    )
    .await
    .map_err(|e| CliError::Serve(format!("opening --store: {e}")))?;
    let state = ServerState::new(
        &config,
        None,
        tokio_util::sync::CancellationToken::new(),
        history,
        crate::serve::logs::LogHub::new(),
        None,
        #[cfg(feature = "triggers")]
        crate::serve::triggers::health::TriggersHandle::empty(),
    );
    #[cfg(feature = "encryption")]
    if let Some(v) = &config.vault {
        let vault =
            crate::serve::vault::Vault::new(&v.key, &v.previous).map_err(CliError::Serve)?;
        state.set_vault(Some(std::sync::Arc::new(vault)));
    }
    state.set_feeds(
        FeedsRuntime::new(
            config.feeds.clone(),
            spec::LoadedFile::default(),
            stores::Stores::from_config(&config),
        )
        .offline(),
    );
    Ok(state)
}

fn actor() -> AuthContext {
    AuthContext {
        principal: PRINCIPAL.into(),
        role: Role::Admin,
        source_ip: None,
        tenant: None,
    }
}

fn print<T: Serialize>(json: bool, value: &T, human: impl FnOnce() -> String) -> CliResult<()> {
    if json {
        let text = serde_json::to_string_pretty(value)
            .map_err(|e| CliError::Serve(format!("serializing output: {e}")))?;
        println!("{text}");
    } else {
        println!("{}", human());
    }
    Ok(())
}

async fn read_spec(path: &std::path::Path) -> CliResult<FeedSpec> {
    let text = if path.as_os_str() == "-" {
        let mut text = String::new();
        std::io::stdin()
            .read_to_string(&mut text)
            .map_err(|e| CliError::Config(format!("reading stdin: {e}")))?;
        text
    } else {
        tokio::fs::read_to_string(path)
            .await
            .map_err(|e| CliError::Config(format!("reading {}: {e}", path.display())))?
    };
    let json = path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("json"));
    if json {
        serde_json::from_str(&text).map_err(|e| CliError::Config(format!("feed definition: {e}")))
    } else {
        crate::yaml::from_str(&text).map_err(|e| CliError::Config(format!("feed definition: {e}")))
    }
}

fn feed_line(f: &service::FeedView) -> String {
    let pending = f.deliveries.iter().filter_map(|d| d.lag).max().unwrap_or(0);
    format!(
        "{:<28} {:<10} events {:<8} pending {:<8} deliveries {}",
        service::label_of(f.tenant.as_deref(), &f.name),
        f.status,
        f.watermark.unwrap_or(0),
        pending,
        f.deliveries.len()
    )
}

fn feed_text(d: &service::FeedDetail) -> String {
    let f = &d.feed;
    let mut out = vec![
        format!(
            "feed {} ({})",
            service::label_of(f.tenant.as_deref(), &f.name),
            f.status
        ),
        format!("  store {} · push to {}", f.store, f.ingest_path),
        format!(
            "  events {}..{} · {} stored byte(s) · {} in the last 24h",
            f.oldest.unwrap_or(0),
            f.watermark.unwrap_or(0),
            f.stored_bytes.unwrap_or(0),
            f.events_24h.unwrap_or(0)
        ),
    ];
    for dl in &f.deliveries {
        out.push(format!(
            "  delivery {:<20} {:<14} next event {} · {} pending{}",
            dl.name,
            dl.status,
            dl.position.unwrap_or(0),
            dl.lag.unwrap_or(0),
            dl.last_error
                .as_ref()
                .map(|e| format!(" · {e}"))
                .unwrap_or_default()
        ));
    }
    for g in &d.gaps {
        out.push(format!(
            "  gap {}..{} ({}) by {}",
            g.from, g.to, g.reason, g.by
        ));
    }
    for t in &d.tokens {
        let state = if t.revoked_at.is_some() {
            "revoked"
        } else {
            "active"
        };
        out.push(format!("  token {} {} {} {state}", t.id, t.name, t.prefix));
    }
    out.join("\n")
}

/// Run `faucet feed …`.
pub async fn run(args: FeedArgs) -> CliResult<()> {
    let state = open(&args).await?;
    let (who, json) = (actor(), args.json);
    let tenant = args.tenant.as_deref();
    match &args.command {
        FeedCommand::List => {
            let feeds = service::list(&state, &who, tenant).await.map_err(failed)?;
            print(json, &feeds, || {
                if feeds.is_empty() {
                    "no feeds".to_string()
                } else {
                    feeds.iter().map(feed_line).collect::<Vec<_>>().join("\n")
                }
            })
        }
        FeedCommand::Show { name } => {
            let d = service::get(&state, &who, tenant, name)
                .await
                .map_err(failed)?;
            print(json, &d, || feed_text(&d))
        }
        FeedCommand::Create { file } => {
            let spec = read_spec(file).await?;
            let made = service::create(&state, &who, tenant, spec)
                .await
                .map_err(failed)?;
            print(json, &made, || format!("created feed {}", made.name))
        }
        FeedCommand::Apply { file } => {
            let spec = read_spec(file).await?;
            let name = spec.name.clone();
            let exists = match service::find(&state, tenant, &name).await {
                Ok(_) => true,
                Err(crate::serve::error::ServeError::NotFound) => false,
                Err(e) => return Err(failed(e)),
            };
            let view = if exists {
                service::update(&state, &who, tenant, &name, spec).await
            } else {
                service::create(&state, &who, tenant, spec).await
            }
            .map_err(failed)?;
            print(json, &view, || {
                format!(
                    "{} feed {name} (revision {})",
                    if exists { "updated" } else { "created" },
                    view.revision
                )
            })
        }
        FeedCommand::Delete { name } => {
            service::delete(&state, &who, tenant, name)
                .await
                .map_err(failed)?;
            print(json, &serde_json::json!({"deleted": name}), || {
                format!("deleted feed {name}")
            })
        }
        FeedCommand::Pause {
            name,
            delivery,
            reason,
        } => {
            service::pause(&state, &who, tenant, name, delivery, reason.clone())
                .await
                .map_err(failed)?;
            print(json, &serde_json::json!({"paused": delivery}), || {
                format!("paused {name}/{delivery}")
            })
        }
        FeedCommand::Resume { name, delivery } => {
            service::resume(&state, &who, tenant, name, delivery)
                .await
                .map_err(failed)?;
            print(json, &serde_json::json!({"resumed": delivery}), || {
                format!("resumed {name}/{delivery}")
            })
        }
        FeedCommand::DeliverNow { name, delivery } => {
            service::deliver_now(&state, &who, tenant, name, delivery)
                .await
                .map_err(failed)?;
            print(json, &serde_json::json!({"deliver_now": delivery}), || {
                format!("{name}/{delivery}: a server starts its next run without waiting")
            })
        }
        FeedCommand::Replay {
            name,
            delivery,
            to_seq,
            to_time,
        } => {
            let from = match (to_seq, to_time) {
                (Some(seq), None) => Position::Seq(*seq),
                (None, Some(time)) => Position::Time(
                    chrono::DateTime::parse_from_rfc3339(time)
                        .map_err(|e| CliError::Config(format!("--to-time '{time}': {e}")))?
                        .with_timezone(&chrono::Utc),
                ),
                _ => {
                    return Err(CliError::Config(
                        "a replay names where it starts: exactly one of --to-seq and --to-time"
                            .into(),
                    ));
                }
            };
            let moved = service::replay(&state, &who, tenant, name, delivery, from)
                .await
                .map_err(failed)?;
            print(json, &moved, || {
                format!(
                    "{name}/{delivery}: moved back from event {} to {}",
                    moved.from, moved.to
                )
            })
        }
        FeedCommand::Skip {
            name,
            delivery,
            to,
            reason,
        } => {
            let moved = service::skip(&state, &who, tenant, name, delivery, *to, reason)
                .await
                .map_err(failed)?;
            print(json, &moved, || {
                format!(
                    "{name}/{delivery}: skipped events {}..{}",
                    moved.from,
                    moved.to.saturating_sub(1)
                )
            })
        }
        FeedCommand::Recover { name, delivery } => {
            let moved = service::recover(&state, &who, tenant, name, delivery)
                .await
                .map_err(failed)?;
            print(json, &moved, || {
                format!("{name}/{delivery}: continues from event {}", moved.to)
            })
        }
        FeedCommand::Events {
            name,
            before,
            from,
            limit,
        } => {
            let page = service::events(&state, &who, tenant, name, *before, *from, *limit)
                .await
                .map_err(failed)?;
            print(json, &page, || {
                page.events
                    .iter()
                    .map(|e| {
                        format!(
                            "#{:<10} {} {} {:<24} {} B",
                            e.seq,
                            e.id,
                            e.received_at
                                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                            e.content_type,
                            e.size
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
        }
        FeedCommand::Push {
            name,
            file,
            content_type,
            dedupe_key,
        } => {
            let body = match file.as_deref().filter(|p| p.as_os_str() != "-") {
                Some(path) => tokio::fs::read(path)
                    .await
                    .map_err(|e| CliError::Config(format!("reading {}: {e}", path.display())))?,
                None => {
                    let mut body = Vec::new();
                    std::io::stdin()
                        .read_to_end(&mut body)
                        .map_err(|e| CliError::Config(format!("reading stdin: {e}")))?;
                    body
                }
            };
            let found = service::find(&state, tenant, name).await.map_err(failed)?;
            let ack = ingest::push_local(
                &state,
                &found.feed,
                content_type,
                &body,
                dedupe_key.as_deref(),
            )
            .await
            .map_err(|r| CliError::Serve(r.message))?;
            let out = serde_json::json!({
                "ids": ack.ids(),
                "first_seq": ack.first_seq.map(|s| s.get()),
                "last_seq": ack.last_seq.map(|s| s.get()),
                "duplicates": ack.duplicates,
            });
            print(json, &out, || {
                format!(
                    "stored {} event(s){}",
                    ack.stored(),
                    match ack.duplicates {
                        0 => String::new(),
                        n => format!(", {n} already stored"),
                    }
                )
            })
        }
        FeedCommand::Token(token) => match token {
            FeedTokenCommand::Create {
                name,
                token_name,
                expires_at,
            } => {
                let expires = expires_at
                    .as_deref()
                    .map(|t| {
                        chrono::DateTime::parse_from_rfc3339(t)
                            .map(|t| t.with_timezone(&chrono::Utc))
                            .map_err(|e| CliError::Config(format!("--expires-at '{t}': {e}")))
                    })
                    .transpose()?;
                let (view, text) =
                    service::token_create(&state, &who, tenant, name, token_name, expires)
                        .await
                        .map_err(failed)?;
                let mut out =
                    serde_json::to_value(&view).map_err(|e| CliError::Serve(e.to_string()))?;
                out["token"] = text.clone().into();
                print(json, &out, || {
                    format!(
                        "token {} ({}): {text}\nIt is shown once; store it now.",
                        view.name, view.id
                    )
                })
            }
            FeedTokenCommand::List { name } => {
                let tokens = service::token_list(&state, &who, tenant, name)
                    .await
                    .map_err(failed)?;
                print(json, &tokens, || {
                    tokens
                        .iter()
                        .map(|t| {
                            format!(
                                "{} {:<20} {} {}",
                                t.id,
                                t.name,
                                t.prefix,
                                if t.revoked_at.is_some() {
                                    "revoked"
                                } else {
                                    "active"
                                }
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                })
            }
            FeedTokenCommand::Revoke { name, id } => {
                let view = service::token_revoke(&state, &who, tenant, name, id)
                    .await
                    .map_err(failed)?;
                print(json, &view, || format!("revoked token {}", view.id))
            }
        },
    }
}

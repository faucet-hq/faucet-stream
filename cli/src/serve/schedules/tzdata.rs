//! Schedules across a tzdata change during a rolling upgrade (#1116).
//!
//! Replicas on different tzdata releases can put one schedule slot at two UTC
//! instants. A replica on older tzdata that cannot hold itself (a release
//! before the `tzdata:` capability, on 2025b) fires at its own instant, so a
//! newer replica holds the tick and runs it only when no such run appeared
//! within `max delta + GRACE` of it. Between two capability-aware releases
//! the older replica holds instead.

use chrono::{DateTime, Duration, Utc};
use faucet_core::time::{TzdataVersion, Zone, changed_since_2025b};

use crate::serve::history::{InstanceRecord, ListFilter, RunRecord, labels::LabelFilter};
use crate::serve::schedules::fire::{LABEL_SCHEDULE, LABEL_TICK};
use crate::serve::state::ServerState;

/// How long after its tick plus the largest offset change a held tick waits
/// for an older replica's run before it runs anyway.
pub const GRACE: Duration = Duration::minutes(5);

/// Runs scanned per held tick.
const SCAN: usize = 200;

/// A replica that may put a tick at another instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Peer {
    /// A release on tzdata 2025b, which does not hold: `who` names it.
    Before { who: String },
    /// A replica on newer tzdata: this one holds every tick in a named zone.
    Newer { who: String, version: TzdataVersion },
}

impl Peer {
    pub fn who(&self) -> &str {
        match self {
            Peer::Before { who } | Peer::Newer { who, .. } => who,
        }
    }

    /// Whether a tick at `tick` in `zone` may fire at another instant there.
    pub fn affects(&self, zone: Zone, tick: DateTime<Utc>) -> bool {
        if zone.is_fixed() {
            return false;
        }
        match self {
            Peer::Before { .. } => changed_since_2025b::contains(zone.name(), tick.timestamp()),
            Peer::Newer { .. } => true,
        }
    }

    /// Whether `run` was started by a replica of this kind.
    fn started(&self, run: &RunRecord, ours: TzdataVersion) -> bool {
        let stamp = run
            .tzdata
            .as_deref()
            .and_then(|v| TzdataVersion::parse(v).ok());
        match self {
            Peer::Before { .. } => run.tzdata.is_none(),
            Peer::Newer { .. } => stamp.is_some_and(|v| v > ours),
        }
    }
}

/// The peer the live members show, if any: a member without the capability
/// (or with one that does not parse) is on 2025b; a newer one makes this
/// replica the one that holds. Members on older capability-aware releases
/// hold themselves.
pub fn peer_among(members: &[InstanceRecord], ours: TzdataVersion) -> Option<Peer> {
    let mut newer = None;
    for m in members {
        match m.tzdata() {
            None => {
                return Some(Peer::Before {
                    who: format!("member {} (no tzdata capability)", m.instance_id),
                });
            }
            Some(v) if v > ours && newer.is_none() => {
                newer = Some(Peer::Newer {
                    who: format!("member {} (tzdata {v})", m.instance_id),
                    version: v,
                });
            }
            Some(_) => {}
        }
    }
    newer
}

/// Whether the live members disagree on tzdata, or one does not say.
pub fn mixed(members: &[InstanceRecord]) -> bool {
    let mut seen = std::collections::BTreeSet::new();
    for m in members {
        match m.tzdata() {
            None => return true,
            Some(v) => {
                seen.insert(v);
            }
        }
    }
    seen.len() > 1
}

/// A run of this schedule started within `window` by a replica that does not
/// advertise tzdata: an older peer that never heartbeats.
pub fn peer_among_runs(runs: &[RunRecord], since: DateTime<Utc>) -> Option<Peer> {
    runs.iter()
        .find(|r| r.tzdata.is_none() && r.submitted_at >= since)
        .map(|r| Peer::Before {
            who: format!("the replica that started run {}", r.run_id),
        })
}

/// What to do with a due tick while `peer` may be live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// The peer ran this slot: record the tick, start nothing.
    Delegated { run_id: String },
    /// Wait for the peer's run until `until`.
    Hold { until: DateTime<Utc> },
    /// Nobody ran it in time: run it now, late.
    FireLate,
}

/// Decide a due `tick`. `runs` are this schedule's recent runs; `ours_at` is
/// whether an instant is one of this replica's own ticks.
pub fn decide(
    peer: &Peer,
    tick: DateTime<Utc>,
    now: DateTime<Utc>,
    runs: &[RunRecord],
    ours: TzdataVersion,
    ours_at: impl Fn(DateTime<Utc>) -> bool,
) -> Decision {
    let delta = Duration::seconds(changed_since_2025b::max_delta_secs());
    let tick_of = |r: &RunRecord| {
        r.labels
            .get(LABEL_TICK)
            .and_then(|t| t.parse::<DateTime<Utc>>().ok())
    };
    let peer_runs: Vec<(&RunRecord, DateTime<Utc>)> = runs
        .iter()
        .filter(|r| peer.started(r, ours))
        .filter_map(|r| tick_of(r).map(|t| (r, t)))
        .collect();
    let exact = peer_runs.iter().find(|(_, t)| *t == tick);
    let shifted = || {
        peer_runs
            .iter()
            .find(|(_, t)| (*t - tick).num_seconds().abs() == delta.num_seconds() && !ours_at(*t))
    };
    if let Some((run, _)) = exact.or_else(shifted) {
        return Decision::Delegated {
            run_id: run.run_id.clone(),
        };
    }
    let until = tick + delta + GRACE;
    if now < until {
        Decision::Hold { until }
    } else {
        Decision::FireLate
    }
}

/// This schedule's runs submitted since `since`, newest first.
pub async fn recent_runs(
    state: &ServerState,
    name: &str,
    since: DateTime<Utc>,
) -> Result<Vec<RunRecord>, String> {
    let filter = ListFilter {
        labels: vec![LabelFilter {
            key: LABEL_SCHEDULE.into(),
            value: Some(name.to_string()),
        }],
        since: Some(since),
        limit: SCAN,
        ..Default::default()
    };
    state
        .history()
        .list(&filter)
        .await
        .map(|p| p.runs)
        .map_err(|e| e.to_string())
}

/// The peer that makes `tick` of schedule `name` (in `zone`, every `period`)
/// one to hold, if any. Only a SQL history can be shared.
pub async fn peer_for(
    state: &ServerState,
    name: &str,
    zone: Zone,
    period: Duration,
    tick: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Option<(Peer, Vec<RunRecord>)>, String> {
    if !state.persistent_history() || zone.is_fixed() {
        return Ok(None);
    }
    let ours = TzdataVersion::current();
    let members = state
        .history()
        .live_instances(state.lease_ttl())
        .await
        .map_err(|e| e.to_string())?;
    crate::serve::schedules::metrics::tzdata_mixed(mixed(&members));
    let delta = Duration::seconds(changed_since_2025b::max_delta_secs());
    let lookback = (period * 2).max(Duration::hours(1));
    let since = (now - lookback).min(tick - delta);
    let runs = recent_runs(state, name, since).await?;
    let peer = peer_among(&members, ours).or_else(|| peer_among_runs(&runs, now - lookback));
    Ok(peer.filter(|p| p.affects(zone, tick)).map(|p| (p, runs)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn ours() -> TzdataVersion {
        TzdataVersion::current()
    }

    fn member(id: &str, caps: Option<&[&str]>) -> InstanceRecord {
        InstanceRecord {
            instance_id: id.into(),
            started_at: at("2026-11-01T00:00:00Z"),
            last_heartbeat: at("2026-11-01T00:00:00Z"),
            listen: None,
            max_concurrent: 4,
            in_flight: 0,
            state_format: 1,
            pools: None,
            capabilities: caps.map(|c| c.iter().map(|s| s.to_string()).collect()),
            clock_offset_ms: None,
        }
    }

    fn run(id: &str, tick: &str, tzdata: Option<&str>) -> RunRecord {
        let mut labels = BTreeMap::new();
        labels.insert(LABEL_SCHEDULE.to_string(), "s".to_string());
        labels.insert(LABEL_TICK.to_string(), tick.to_string());
        let mut r = RunRecord::queued(id.into(), None, labels, None, at(tick));
        r.tzdata = tzdata.map(str::to_string);
        r
    }

    #[test]
    fn members_without_the_capability_are_on_2025b() {
        let cap = format!("tzdata:{}", ours());
        let current = member("a", Some(&[cap.as_str()]));
        assert_eq!(peer_among(std::slice::from_ref(&current), ours()), None);
        assert!(!mixed(std::slice::from_ref(&current)));
        let old = member("b", None);
        let p = peer_among(&[current.clone(), old.clone()], ours()).unwrap();
        assert!(matches!(p, Peer::Before { .. }), "{p:?}");
        assert!(p.who().contains("member b"));
        assert!(mixed(&[current.clone(), old]));
        let newer = member("c", Some(&["tzdata:2999a"]));
        assert!(matches!(
            peer_among(&[current.clone(), newer.clone()], ours()),
            Some(Peer::Newer { .. })
        ));
        assert!(mixed(&[current.clone(), newer]));
        let older_aware = member("d", Some(&["tzdata:2026a"]));
        assert_eq!(
            peer_among(&[current, older_aware], ours()),
            None,
            "it holds itself"
        );
    }

    #[test]
    fn a_run_without_a_tzdata_stamp_is_an_older_peer() {
        let runs = [
            run("new", "2026-11-24T16:00:00Z", Some("2026f")),
            run("old", "2026-11-24T17:00:00Z", None),
        ];
        let p = peer_among_runs(&runs, at("2026-11-24T00:00:00Z")).unwrap();
        assert!(p.who().contains("run old"));
        assert!(peer_among_runs(&runs, at("2026-11-25T00:00:00Z")).is_none());
    }

    #[test]
    fn only_ticks_in_changed_periods_are_held_against_2025b() {
        let before = Peer::Before { who: "x".into() };
        let vancouver = Zone::parse("America/Vancouver").unwrap();
        assert!(before.affects(vancouver, at("2026-11-24T16:00:00Z")));
        assert!(!before.affects(vancouver, at("2026-07-01T16:00:00Z")));
        assert!(!before.affects(
            Zone::parse("Europe/London").unwrap(),
            at("2026-11-24T16:00:00Z")
        ));
        assert!(!before.affects(Zone::UTC, at("2026-11-24T16:00:00Z")));
        let newer = Peer::Newer {
            who: "y".into(),
            version: TzdataVersion::parse("2999a").unwrap(),
        };
        assert!(newer.affects(
            Zone::parse("Europe/London").unwrap(),
            at("2026-07-01T16:00:00Z")
        ));
        assert!(!newer.affects(
            Zone::parse("Etc/GMT+5").unwrap(),
            at("2026-07-01T16:00:00Z")
        ));
    }

    #[test]
    fn a_held_tick_is_delegated_held_or_fired_late() {
        let before = Peer::Before { who: "x".into() };
        let tick = at("2026-11-24T16:00:00Z");
        let never_ours = |_: DateTime<Utc>| false;
        let old_run = [run("old", "2026-11-24T17:00:00Z", None)];
        assert_eq!(
            decide(&before, tick, tick, &old_run, ours(), never_ours),
            Decision::Delegated {
                run_id: "old".into()
            }
        );
        let exact = [run("old", "2026-11-24T16:00:00Z", None)];
        assert!(matches!(
            decide(&before, tick, tick, &exact, ours(), |_| true),
            Decision::Delegated { .. }
        ));
        assert_eq!(
            decide(&before, tick, tick, &old_run, ours(), |t| t
                == at("2026-11-24T17:00:00Z")),
            Decision::Hold {
                until: at("2026-11-24T17:05:00Z")
            },
            "an old run at our own next tick is that tick's run"
        );
        let mine = [run(
            "mine",
            "2026-11-24T17:00:00Z",
            Some(faucet_core::time::TZDATA_VERSION),
        )];
        assert_eq!(
            decide(
                &before,
                tick,
                at("2026-11-24T16:30:00Z"),
                &mine,
                ours(),
                never_ours
            ),
            Decision::Hold {
                until: at("2026-11-24T17:05:00Z")
            }
        );
        assert_eq!(
            decide(
                &before,
                tick,
                at("2026-11-24T17:05:00Z"),
                &[],
                ours(),
                never_ours
            ),
            Decision::FireLate
        );
        let newer = Peer::Newer {
            who: "y".into(),
            version: TzdataVersion::parse("2999a").unwrap(),
        };
        let newer_run = [run("n", "2026-11-24T16:00:00Z", Some("2999a"))];
        assert!(matches!(
            decide(&newer, tick, tick, &newer_run, ours(), never_ours),
            Decision::Delegated { .. }
        ));
    }
}

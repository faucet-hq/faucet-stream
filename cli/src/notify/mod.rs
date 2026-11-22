//! Notification / incident-routing layer (#280).
//!
//! A declarative top-level `notifications:` block fans pipeline lifecycle and
//! health events out to Slack / PagerDuty / a generic signed webhook. It works
//! from every runtime (`run` / `schedule` / `serve` / `replicate`) because the
//! emit sites live in the shared executor + SLA pass; the scheduler adds a
//! `scheduler_stuck` signal.
//!
//! **Notifications never fail or block a run.** A run's events are delivered
//! on a background task ([`queue`]), in order per pipeline row; a channel
//! outage is retried a little, then logged + counted
//! (`faucet_notifications_dropped_total`) and swallowed — the same
//! log-and-continue contract as lineage and SLA. A process calls
//! [`queue::flush`] before it exits so pending deliveries are not lost.
//!
//! Module layout (mirrors `sla/` / `schedule/`):
//! - [`spec`] — serde config types + validation (`faucet schema notifications`).
//! - [`event`] — the channel-agnostic [`NotifyEvent`] + its constructors.
//! - [`render`] — pure per-channel payload rendering.
//! - [`channels`] — the one-request-each HTTP shims.
//! - [`dispatch`] — the [`Notifier`]: match → coalesce → deliver → resolve.
//! - [`metrics`] — the `faucet_notifications_*` surface.
//!
//! Channel secrets (Slack webhook URL, PD routing key, webhook HMAC secret)
//! should be supplied via `${env:...}` / `${file:...}` / `${secret:...}`, which
//! are resolved over the raw document at load time and registered for log
//! redaction.

pub mod channels;
pub mod dispatch;
pub mod event;
pub mod metrics;
pub mod queue;
pub mod render;
pub mod spec;

pub use dispatch::{DeliveryOutcome, Notifier};
pub use event::{NotifyEvent, RunContext};
pub use queue::{dispatch, flush};
pub use spec::{
    ChannelSpec, EventKind, NotificationSpec, PagerdutyConfig, RESERVED_BODY_KEYS, Severity,
    SlackConfig, WebhookConfig, validate_all,
};

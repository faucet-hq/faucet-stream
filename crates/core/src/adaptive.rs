//! Adaptive batch sizing — an AIMD controller that auto-tunes the effective
//! write batch size per pipeline row from observed sink latency + error rate.
//! Pure logic (no I/O); `run_stream` feeds it observations and emits metrics.
//! See `docs/superpowers/specs/2026-05-31-adaptive-batch-sizing-design.md`.

use crate::error::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Duration;

fn default_controller() -> String {
    "aimd".to_string()
}
fn default_min() -> usize {
    100
}
fn default_max() -> usize {
    50_000
}
fn default_increase_step() -> usize {
    250
}
fn default_decrease_factor() -> f64 {
    0.5
}
fn default_cooldown_batches() -> usize {
    5
}
fn default_latency_window() -> usize {
    10
}
fn default_error_threshold() -> f64 {
    0.01
}
fn default_true() -> bool {
    true
}
fn default_log_every() -> usize {
    50
}

/// Configuration for the adaptive batch-size controller. Lives under
/// `execution.adaptive_batch_size`. Default `enabled = false` (opt-in); when
/// disabled the pipeline writes each page exactly as before.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AdaptiveBatchConfig {
    /// Master switch. Default `false`.
    #[serde(default)]
    pub enabled: bool,
    /// Controller algorithm. Only `"aimd"` is implemented in v1.
    #[serde(default = "default_controller")]
    pub controller: String,
    /// Lower bound on the effective batch size.
    #[serde(default = "default_min")]
    pub min: usize,
    /// Upper bound. Under within-page reslicing the effective ceiling is
    /// `min(max, page_len)`; values above the source page size are inert.
    #[serde(default = "default_max")]
    pub max: usize,
    /// Additive growth per clean+fast batch.
    #[serde(default = "default_increase_step")]
    pub increase_step: usize,
    /// Multiplicative shrink factor on error / high latency (0 < f < 1).
    #[serde(default = "default_decrease_factor")]
    pub decrease_factor: f64,
    /// Batches to wait after a shrink before allowing growth.
    #[serde(default = "default_cooldown_batches")]
    pub cooldown_batches: usize,
    /// Optional latency target in ms. `None` = react to errors only.
    #[serde(default)]
    pub target_latency_ms: Option<u64>,
    /// Rolling window size for the p50 batch-write latency.
    #[serde(default = "default_latency_window")]
    pub latency_window: usize,
    /// Per-batch error rate above which the controller shrinks.
    #[serde(default = "default_error_threshold")]
    pub error_threshold: f64,
    /// Never grow the effective batch past the source page size. Only `true`
    /// is supported (the default) — `false` is rejected by [`validate`], since
    /// cross-page buffering would have to hold records across source pages and
    /// break the pipeline's O(batch_size) memory guarantee.
    ///
    /// [`validate`]: AdaptiveBatchConfig::validate
    #[serde(default = "default_true")]
    pub respect_source_max: bool,
    /// Emit a `tracing::info!` summary every N adjustments.
    #[serde(default = "default_log_every")]
    pub log_every: usize,
}

impl AdaptiveBatchConfig {
    /// Fail-fast validation, surfaced as `FaucetError::Config` at config load.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.controller != "aimd" {
            return Err(FaucetError::Config(format!(
                "adaptive_batch_size.controller '{}' is not supported (only 'aimd')",
                self.controller
            )));
        }
        if self.min < 1 {
            return Err(FaucetError::Config(
                "adaptive_batch_size.min must be >= 1".into(),
            ));
        }
        if self.min > self.max {
            return Err(FaucetError::Config(format!(
                "adaptive_batch_size.min ({}) must be <= max ({})",
                self.min, self.max
            )));
        }
        if self.max > crate::MAX_BATCH_SIZE {
            return Err(FaucetError::Config(format!(
                "adaptive_batch_size.max ({}) must be <= {} (MAX_BATCH_SIZE)",
                self.max,
                crate::MAX_BATCH_SIZE
            )));
        }
        if self.increase_step > crate::MAX_BATCH_SIZE {
            return Err(FaucetError::Config(format!(
                "adaptive_batch_size.increase_step ({}) must be <= {} (MAX_BATCH_SIZE)",
                self.increase_step,
                crate::MAX_BATCH_SIZE
            )));
        }
        if !(self.decrease_factor > 0.0 && self.decrease_factor < 1.0) {
            return Err(FaucetError::Config(
                "adaptive_batch_size.decrease_factor must be in (0, 1)".into(),
            ));
        }
        if self.increase_step < 1 {
            return Err(FaucetError::Config(
                "adaptive_batch_size.increase_step must be >= 1".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.error_threshold) {
            return Err(FaucetError::Config(
                "adaptive_batch_size.error_threshold must be in [0, 1]".into(),
            ));
        }
        if self.latency_window < 1 {
            return Err(FaucetError::Config(
                "adaptive_batch_size.latency_window must be >= 1".into(),
            ));
        }
        if let Some(t) = self.target_latency_ms
            && t == 0
        {
            return Err(FaucetError::Config(
                "adaptive_batch_size.target_latency_ms must be > 0 when set".into(),
            ));
        }
        if !self.respect_source_max {
            return Err(FaucetError::Config(
                "adaptive_batch_size.respect_source_max=false is not supported \
                 (cross-page buffering would violate the O(batch_size) memory \
                 guarantee); remove the field or set it to true"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// Direction of a batch-size adjustment (metric label).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjustDirection {
    Up,
    Down,
}
impl AdjustDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            AdjustDirection::Up => "up",
            AdjustDirection::Down => "down",
        }
    }
}

/// Why an adjustment happened (metric label).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjustReason {
    Success,
    Error,
    Latency,
}
impl AdjustReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            AdjustReason::Success => "success",
            AdjustReason::Error => "error",
            AdjustReason::Latency => "latency",
        }
    }
}

/// One observed sub-batch write outcome fed to the controller.
#[derive(Debug, Clone, Copy)]
pub struct Observation {
    pub batch_len: usize,
    pub errors: usize,
    pub latency: Duration,
}

/// A size change the controller decided on.
#[derive(Debug, Clone, Copy)]
pub struct Adjustment {
    pub new_size: usize,
    pub direction: AdjustDirection,
    pub reason: AdjustReason,
}

/// Additive-increase / multiplicative-decrease controller. Pure + deterministic.
pub struct AimdController {
    min: usize,
    max: usize,
    increase_step: usize,
    decrease_factor: f64,
    cooldown_batches: usize,
    target_latency_ms: Option<u64>,
    latency_window: usize,
    error_threshold: f64,
    log_every: usize,

    current: usize,
    cooldown: usize,
    latencies: VecDeque<u64>,
    floor_warned: bool,
    adjustments: u64,
}

impl AimdController {
    /// Build a controller. `initial` is clamped into `[min, max]` (cold-start =
    /// the source's actual page size when `run_stream` passes the first page len).
    pub fn new(cfg: &AdaptiveBatchConfig, initial: usize) -> Self {
        Self {
            min: cfg.min,
            max: cfg.max,
            increase_step: cfg.increase_step,
            decrease_factor: cfg.decrease_factor,
            cooldown_batches: cfg.cooldown_batches,
            target_latency_ms: cfg.target_latency_ms,
            latency_window: cfg.latency_window.max(1),
            error_threshold: cfg.error_threshold,
            log_every: cfg.log_every,
            current: initial.clamp(cfg.min, cfg.max),
            cooldown: 0,
            latencies: VecDeque::new(),
            floor_warned: false,
            adjustments: 0,
        }
    }

    pub fn current(&self) -> usize {
        self.current
    }
    pub fn cooldown_active(&self) -> bool {
        self.cooldown > 0
    }

    /// p50 of the rolling latency window (ms), if any samples present.
    pub fn p50_latency_ms(&self) -> Option<u64> {
        if self.latencies.is_empty() {
            return None;
        }
        let mut v: Vec<u64> = self.latencies.iter().copied().collect();
        v.sort_unstable();
        Some(v[v.len() / 2])
    }

    /// Feed one sub-batch observation; returns `Some(Adjustment)` when the size
    /// changed. First-match-wins: error shrink → cooldown → latency target →
    /// success growth.
    pub fn observe(&mut self, obs: Observation) -> Option<Adjustment> {
        // 1. Error shrink (always, even during cooldown).
        if obs.batch_len > 0 {
            let rate = obs.errors as f64 / obs.batch_len as f64;
            if rate > self.error_threshold {
                return self.shrink(AdjustReason::Error);
            }
        }
        // 2. Cooldown blocks growth.
        if self.cooldown > 0 {
            self.cooldown -= 1;
            return None;
        }
        // 3. Latency target.
        if let Some(target) = self.target_latency_ms {
            self.latencies.push_back(obs.latency.as_millis() as u64);
            while self.latencies.len() > self.latency_window {
                self.latencies.pop_front();
            }
            let p50 = self.p50_latency_ms().unwrap_or(0) as f64;
            let t = target as f64;
            if p50 > t * 1.2 {
                return self.shrink(AdjustReason::Latency);
            } else if p50 < t * 0.5 {
                return self.grow(AdjustReason::Latency);
            }
            return None;
        }
        // 4. Success growth.
        self.grow(AdjustReason::Success)
    }

    fn shrink(&mut self, reason: AdjustReason) -> Option<Adjustment> {
        let new = ((self.current as f64 * self.decrease_factor).floor() as usize).max(self.min);
        self.cooldown = self.cooldown_batches;
        if new == self.current {
            if reason == AdjustReason::Error && self.current == self.min && !self.floor_warned {
                tracing::warn!(
                    batch_size = self.current,
                    "adaptive batch size at floor (min) and still seeing errors; \
                     consider lowering `min` or investigating the sink"
                );
                self.floor_warned = true;
            }
            return None;
        }
        self.current = new;
        self.latencies.clear();
        self.bump_log(AdjustDirection::Down, reason);
        Some(Adjustment {
            new_size: new,
            direction: AdjustDirection::Down,
            reason,
        })
    }

    fn grow(&mut self, reason: AdjustReason) -> Option<Adjustment> {
        // `saturating_add` so a controller built directly with a large
        // `increase_step` (bypassing `validate`) can't overflow `usize`.
        let new = self
            .current
            .saturating_add(self.increase_step)
            .min(self.max);
        if new == self.current {
            return None;
        }
        self.current = new;
        self.latencies.clear();
        self.bump_log(AdjustDirection::Up, reason);
        Some(Adjustment {
            new_size: new,
            direction: AdjustDirection::Up,
            reason,
        })
    }

    fn bump_log(&mut self, direction: AdjustDirection, reason: AdjustReason) {
        self.adjustments += 1;
        if self.log_every > 0 && self.adjustments.is_multiple_of(self.log_every as u64) {
            tracing::info!(
                current = self.current,
                direction = direction.as_str(),
                reason = reason.as_str(),
                adjustments = self.adjustments,
                "adaptive batch size adjusted"
            );
        }
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    fn valid() -> AdaptiveBatchConfig {
        serde_json::from_value(serde_json::json!({"enabled": true})).unwrap()
    }

    #[test]
    fn defaults_are_sane_and_valid() {
        let c = valid();
        assert_eq!(c.controller, "aimd");
        assert_eq!(c.min, 100);
        assert_eq!(c.max, 50_000);
        assert!(c.respect_source_max);
        assert!(c.target_latency_ms.is_none());
        c.validate().unwrap();
    }

    #[test]
    fn rejects_respect_source_max_false() {
        // `false` was a frozen no-op (it warned, then behaved as `true`).
        // Cross-page buffering would violate the O(batch_size) memory
        // guarantee, so the knob is rejected rather than silently ignored.
        let mut c = valid();
        c.respect_source_max = false;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_unknown_controller() {
        let mut c = valid();
        c.controller = "pid".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_min_gt_max_and_zero_min() {
        let mut c = valid();
        c.min = 10;
        c.max = 5;
        assert!(c.validate().is_err());
        let mut c = valid();
        c.min = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_max_and_increase_step_above_max_batch_size() {
        let mut c = valid();
        c.max = crate::MAX_BATCH_SIZE + 1;
        assert!(c.validate().is_err());
        let mut c = valid();
        c.increase_step = crate::MAX_BATCH_SIZE + 1;
        assert!(c.validate().is_err());
        // The ceiling itself is accepted.
        let mut c = valid();
        c.max = crate::MAX_BATCH_SIZE;
        c.validate().unwrap();
    }

    #[test]
    fn rejects_out_of_range_factors() {
        let mut c = valid();
        c.decrease_factor = 1.5;
        assert!(c.validate().is_err());
        let mut c = valid();
        c.error_threshold = 2.0;
        assert!(c.validate().is_err());
        let mut c = valid();
        c.increase_step = 0;
        assert!(c.validate().is_err());
        let mut c = valid();
        c.target_latency_ms = Some(0);
        assert!(c.validate().is_err());
        // decrease_factor is an *exclusive* (0, 1) range — both bounds invalid.
        let mut c = valid();
        c.decrease_factor = 0.0;
        assert!(c.validate().is_err());
        let mut c = valid();
        c.decrease_factor = 1.0;
        assert!(c.validate().is_err());
        // latency_window must be >= 1.
        let mut c = valid();
        c.latency_window = 0;
        assert!(c.validate().is_err());
    }
}

#[cfg(test)]
mod controller_tests {
    use super::*;
    use std::time::Duration;

    fn cfg() -> AdaptiveBatchConfig {
        serde_json::from_value(serde_json::json!({
            "enabled": true, "min": 100, "max": 1000,
            "increase_step": 100, "decrease_factor": 0.5,
            "cooldown_batches": 2, "error_threshold": 0.1
        }))
        .unwrap()
    }

    fn ok(len: usize) -> Observation {
        Observation {
            batch_len: len,
            errors: 0,
            latency: Duration::from_millis(1),
        }
    }

    #[test]
    fn cold_start_clamps_initial_to_bounds() {
        let c = AimdController::new(&cfg(), 50); // below min
        assert_eq!(c.current(), 100);
        let c = AimdController::new(&cfg(), 99_999); // above max
        assert_eq!(c.current(), 1000);
        let c = AimdController::new(&cfg(), 500);
        assert_eq!(c.current(), 500);
    }

    #[test]
    fn grow_saturates_instead_of_overflowing_usize() {
        // A controller built directly (bypassing `validate`) with a huge
        // increase_step must not overflow `usize` on growth — it saturates and
        // clamps to `max`. Guards the `current + increase_step` arithmetic.
        let cfg: AdaptiveBatchConfig = serde_json::from_value(serde_json::json!({
            "enabled": true, "min": 1, "max": usize::MAX,
            "increase_step": usize::MAX, "decrease_factor": 0.5
        }))
        .unwrap();
        let mut c = AimdController::new(&cfg, 1);
        let adj = c.observe(ok(1)).expect("growth should occur");
        assert_eq!(adj.new_size, usize::MAX);
        assert_eq!(c.current(), usize::MAX);
    }

    #[test]
    fn grows_additively_on_success_up_to_max() {
        let mut c = AimdController::new(&cfg(), 800);
        let a = c.observe(ok(800)).unwrap();
        assert_eq!(a.new_size, 900);
        assert_eq!(a.direction, AdjustDirection::Up);
        assert_eq!(a.reason, AdjustReason::Success);
        c.observe(ok(900)); // -> 1000 (max)
        assert_eq!(c.current(), 1000);
        // At max: no further growth, no adjustment.
        assert!(c.observe(ok(1000)).is_none());
        assert_eq!(c.current(), 1000);
    }

    #[test]
    fn shrinks_multiplicatively_on_error_and_arms_cooldown() {
        let mut c = AimdController::new(&cfg(), 800);
        let a = c
            .observe(Observation {
                batch_len: 100,
                errors: 20,
                latency: Duration::from_millis(1),
            })
            .unwrap();
        assert_eq!(a.new_size, 400); // floor(800*0.5)
        assert_eq!(a.direction, AdjustDirection::Down);
        assert_eq!(a.reason, AdjustReason::Error);
        assert!(c.cooldown_active());
        // Cooldown blocks growth for cooldown_batches observations.
        assert!(c.observe(ok(400)).is_none());
        assert!(c.observe(ok(400)).is_none());
        // After cooldown drains, growth resumes.
        let a = c.observe(ok(400)).unwrap();
        assert_eq!(a.new_size, 500);
    }

    #[test]
    fn does_not_shrink_below_min_and_warns_once() {
        let mut c = AimdController::new(&cfg(), 100); // == min
        let bad = Observation {
            batch_len: 100,
            errors: 100,
            latency: Duration::from_millis(1),
        };
        // Already at min: shrink is a no-op (no adjustment), cooldown still arms.
        assert!(c.observe(bad).is_none());
        assert_eq!(c.current(), 100);
    }

    #[test]
    fn latency_target_shrinks_when_slow_grows_when_fast() {
        let mut c: AimdController = AimdController::new(
            &serde_json::from_value(serde_json::json!({
                "enabled": true, "min": 100, "max": 1000, "increase_step": 100,
                "decrease_factor": 0.5, "cooldown_batches": 0,
                "target_latency_ms": 500, "latency_window": 1
            }))
            .unwrap(),
            800,
        );
        // p50 = 700ms > 500*1.2=600 -> shrink
        let a = c
            .observe(Observation {
                batch_len: 800,
                errors: 0,
                latency: Duration::from_millis(700),
            })
            .unwrap();
        assert_eq!(a.reason, AdjustReason::Latency);
        assert_eq!(a.direction, AdjustDirection::Down);
        assert_eq!(c.current(), 400);
        // p50 = 100ms < 500*0.5=250 -> grow
        let a = c
            .observe(Observation {
                batch_len: 400,
                errors: 0,
                latency: Duration::from_millis(100),
            })
            .unwrap();
        assert_eq!(a.direction, AdjustDirection::Up);
        assert_eq!(a.reason, AdjustReason::Latency);
        assert_eq!(c.current(), 500);
        // p50 = 500ms in deadband [250,600] -> no change
        assert!(
            c.observe(Observation {
                batch_len: 500,
                errors: 0,
                latency: Duration::from_millis(500)
            })
            .is_none()
        );
    }

    #[test]
    fn error_during_cooldown_reshrinks_and_rearms() {
        // The spec's distinctive invariant: error shrink fires BEFORE the
        // cooldown gate, so a mid-cooldown error shrinks again + re-arms.
        let mut c = AimdController::new(&cfg(), 800);
        let bad = Observation {
            batch_len: 100,
            errors: 50,
            latency: Duration::from_millis(1),
        };
        let a = c.observe(bad).unwrap(); // 800 -> 400, cooldown armed (2)
        assert_eq!(a.new_size, 400);
        assert!(c.cooldown_active());
        // Still in cooldown, but another error shrinks again (400 -> 200) and re-arms.
        let a = c.observe(bad).unwrap();
        assert_eq!(a.new_size, 200);
        assert_eq!(a.reason, AdjustReason::Error);
        assert!(c.cooldown_active());
    }

    #[test]
    fn p50_uses_median_of_multi_sample_window() {
        let mut c = AimdController::new(
            &serde_json::from_value(serde_json::json!({
                "enabled": true, "min": 100, "max": 1000, "increase_step": 100,
                "decrease_factor": 0.5, "cooldown_batches": 0,
                "target_latency_ms": 500, "latency_window": 5
            }))
            .unwrap(),
            800,
        );
        // Feed five fast samples (10ms): p50 = 10ms < 250 -> grows each time.
        for _ in 0..5 {
            c.observe(Observation {
                batch_len: 800,
                errors: 0,
                latency: Duration::from_millis(10),
            });
        }
        assert_eq!(c.p50_latency_ms(), Some(10));
        // One slow sample doesn't move the median of a 5-window enough to flip it
        // (sorted [10,10,10,10,900] -> p50 = 10), so still in grow/deadband territory.
        c.observe(Observation {
            batch_len: 800,
            errors: 0,
            latency: Duration::from_millis(900),
        });
        assert_eq!(c.p50_latency_ms(), Some(10));
    }

    #[test]
    fn an_adjustment_starts_a_fresh_latency_window() {
        let cfg: AdaptiveBatchConfig = serde_json::from_value(serde_json::json!({
            "enabled": true, "min": 1, "max": 1000, "target_latency_ms": 100,
            "latency_window": 5, "cooldown_batches": 0
        }))
        .unwrap();
        let mut c = AimdController::new(&cfg, 1000);
        let obs = |ms| Observation {
            batch_len: 10,
            errors: 0,
            latency: std::time::Duration::from_millis(ms),
        };
        assert!(c.observe(obs(500)).is_some());
        assert_eq!(c.current(), 500);
        assert!(
            c.observe(obs(100)).is_none(),
            "the stale slow sample is gone"
        );
        assert_eq!(c.current(), 500);
    }
}

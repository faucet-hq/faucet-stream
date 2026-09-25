//! Bounded-memory sketches behind column profiling (#708). Each is a few KB
//! per column regardless of row count, so a 10M-row run profiles in the same
//! memory as a 10-row one.
//!
//! - [`HyperLogLog`] — distinct-count estimate (2¹² registers, ~1.6 % error).
//! - [`TopK`] — most-frequent values (Misra-Gries / Space-Saving counters).
//! - [`Reservoir`] — a uniform sample for approximate quantiles (Algorithm R
//!   with a deterministic xorshift generator, so a profile is reproducible).
//! - [`Welford`] — streaming mean / variance / min / max.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// 64-bit hash of a value, stable for the process (SipHash with fixed keys).
pub fn hash64<T: Hash + ?Sized>(v: &T) -> u64 {
    let mut h = std::hash::DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

/// HyperLogLog distinct-count estimator with 2¹² registers (4 KiB).
#[derive(Debug, Clone)]
pub struct HyperLogLog {
    registers: Vec<u8>,
}

const HLL_P: u32 = 12;
const HLL_M: usize = 1 << HLL_P;

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    pub fn new() -> Self {
        Self {
            registers: vec![0; HLL_M],
        }
    }

    /// Observe one already-hashed value.
    pub fn insert_hash(&mut self, h: u64) {
        let idx = (h >> (64 - HLL_P)) as usize;
        let rest = h << HLL_P;
        // Leading-zero rank of the remaining bits, 1-based; a zero remainder
        // saturates at the width of the remainder.
        let rank = if rest == 0 {
            (64 - HLL_P) as u8 + 1
        } else {
            rest.leading_zeros() as u8 + 1
        };
        if rank > self.registers[idx] {
            self.registers[idx] = rank;
        }
    }

    /// Observe one value.
    pub fn insert<T: Hash + ?Sized>(&mut self, v: &T) {
        self.insert_hash(hash64(v));
    }

    /// Merge another sketch (register-wise max).
    pub fn merge(&mut self, other: &HyperLogLog) {
        for (a, b) in self.registers.iter_mut().zip(&other.registers) {
            if *b > *a {
                *a = *b;
            }
        }
    }

    /// The distinct-count estimate (rounded).
    pub fn estimate(&self) -> u64 {
        let m = HLL_M as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let sum: f64 = self.registers.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
        let raw = alpha * m * m / sum;
        let zeros = self.registers.iter().filter(|&&r| r == 0).count();
        let est = if raw <= 2.5 * m && zeros > 0 {
            // Linear counting for the small range.
            m * (m / zeros as f64).ln()
        } else {
            raw
        };
        est.round().max(0.0) as u64
    }
}

/// Space-Saving top-k: at most `capacity` counters, each value's count is an
/// upper bound whose error is bounded by `error`.
#[derive(Debug, Clone)]
pub struct TopK {
    capacity: usize,
    counters: HashMap<String, (u64, u64)>,
}

impl TopK {
    /// A sketch that keeps `k` values with `4·k` counters (a common overhead
    /// that keeps the top-`k` set exact in practice for skewed data).
    pub fn new(k: usize) -> Self {
        Self {
            capacity: (k * 4).max(1),
            counters: HashMap::new(),
        }
    }

    pub fn insert(&mut self, value: &str) {
        if let Some(c) = self.counters.get_mut(value) {
            c.0 += 1;
            return;
        }
        if self.counters.len() < self.capacity {
            self.counters.insert(value.to_string(), (1, 0));
            return;
        }
        // Evict the minimum counter and inherit its count as the error bound.
        let (victim, min) = self
            .counters
            .iter()
            .map(|(k, (c, _))| (k.clone(), *c))
            .min_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)))
            .expect("capacity >= 1");
        self.counters.remove(&victim);
        self.counters.insert(value.to_string(), (min + 1, min));
    }

    /// The `k` most frequent values as `(value, count, error)`, count
    /// descending then value ascending for determinism.
    pub fn top(&self, k: usize) -> Vec<(String, u64, u64)> {
        let mut all: Vec<(String, u64, u64)> = self
            .counters
            .iter()
            .map(|(v, (c, e))| (v.clone(), *c, *e))
            .collect();
        all.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        all.truncate(k);
        all
    }
}

/// Uniform reservoir sample (Algorithm R) with a deterministic generator.
#[derive(Debug, Clone)]
pub struct Reservoir {
    capacity: usize,
    seen: u64,
    values: Vec<f64>,
    rng: u64,
}

impl Reservoir {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            seen: 0,
            values: Vec::new(),
            rng: 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn insert(&mut self, v: f64) {
        self.seen += 1;
        if self.values.len() < self.capacity {
            self.values.push(v);
            return;
        }
        let j = self.next_u64() % self.seen;
        if (j as usize) < self.capacity {
            self.values[j as usize] = v;
        }
    }

    /// Approximate quantile over the sample; `None` while empty.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.values.is_empty() {
            return None;
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(f64::total_cmp);
        Some(crate::anomaly::quantile(&sorted, q))
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Welford streaming moments plus exact min / max.
#[derive(Debug, Clone, Default)]
pub struct Welford {
    pub count: u64,
    mean: f64,
    m2: f64,
    pub min: f64,
    pub max: f64,
}

impl Welford {
    pub fn insert(&mut self, x: f64) {
        if self.count == 0 {
            self.min = x;
            self.max = x;
        } else {
            self.min = self.min.min(x);
            self.max = self.max.max(x);
        }
        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (x - self.mean);
    }

    pub fn mean(&self) -> Option<f64> {
        (self.count > 0).then_some(self.mean)
    }

    /// Population standard deviation.
    pub fn stddev(&self) -> Option<f64> {
        (self.count > 0).then(|| (self.m2 / self.count as f64).sqrt())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hll_estimates_within_tolerance() {
        for &n in &[1u64, 10, 100, 1_000, 50_000] {
            let mut h = HyperLogLog::new();
            for i in 0..n {
                h.insert(&format!("value-{i}"));
            }
            let est = h.estimate() as f64;
            let err = (est - n as f64).abs() / n as f64;
            assert!(err < 0.05, "n {n} est {est} err {err}");
        }
        assert_eq!(HyperLogLog::new().estimate(), 0);
    }

    #[test]
    fn hll_merge_is_register_max_and_duplicates_do_not_count() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        for i in 0..1000 {
            a.insert(&i);
            a.insert(&i);
            b.insert(&(i + 500));
        }
        a.merge(&b);
        let est = a.estimate() as f64;
        assert!((est - 1500.0).abs() / 1500.0 < 0.05, "{est}");
        let mut z = HyperLogLog::new();
        z.insert_hash(0);
        assert_eq!(z.registers.iter().filter(|&&r| r > 0).count(), 1);
    }

    #[test]
    fn topk_keeps_the_heavy_hitters_with_error_bounds() {
        let mut t = TopK::new(2);
        for _ in 0..100 {
            t.insert("a");
        }
        for _ in 0..50 {
            t.insert("b");
        }
        for i in 0..40 {
            t.insert(&format!("noise-{i}"));
        }
        let top = t.top(2);
        assert_eq!(top[0].0, "a");
        assert_eq!(top[0].1, 100);
        assert_eq!(top[1].0, "b");
        assert!(top[1].1 >= 50);
        assert!(t.counters.len() <= 8);
        // Evicted noise inherits the minimum as its error bound.
        assert!(t.counters.values().any(|(_, e)| *e > 0));
    }

    #[test]
    fn topk_orders_ties_by_value() {
        let mut t = TopK::new(3);
        for v in ["z", "y", "x"] {
            t.insert(v);
        }
        let names: Vec<_> = t.top(3).into_iter().map(|(v, _, _)| v).collect();
        assert_eq!(names, vec!["x", "y", "z"]);
        assert!(TopK::new(0).top(1).is_empty());
    }

    #[test]
    fn reservoir_is_uniform_enough_and_deterministic() {
        let mut r = Reservoir::new(200);
        for i in 0..10_000 {
            r.insert(i as f64);
        }
        assert_eq!(r.len(), 200);
        let median = r.quantile(0.5).unwrap();
        assert!((3500.0..=6500.0).contains(&median), "{median}");
        let mut r2 = Reservoir::new(200);
        for i in 0..10_000 {
            r2.insert(i as f64);
        }
        assert_eq!(r.quantile(0.9), r2.quantile(0.9));
        assert!(Reservoir::new(4).quantile(0.5).is_none());
        assert!(Reservoir::new(4).is_empty());
    }

    #[test]
    fn welford_matches_closed_form() {
        let mut w = Welford::default();
        assert!(w.mean().is_none());
        assert!(w.stddev().is_none());
        for x in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            w.insert(x);
        }
        assert_eq!(w.count, 8);
        assert_eq!(w.mean(), Some(5.0));
        assert!((w.stddev().unwrap() - 2.0).abs() < 1e-12);
        assert_eq!(w.min, 2.0);
        assert_eq!(w.max, 9.0);
    }
}

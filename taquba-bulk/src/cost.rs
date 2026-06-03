//! Generic per-item cost accumulator.
//!
//! [`CostReport`] is an unopinionated collection of named `f64` counters.
//! Pipelines report whatever units make sense for their workload (LLM token
//! counts, paid-API call units, compute-seconds, dollars) via
//! [`crate::BulkCtx::record_cost`], and the bulk runner rolls the per-item
//! reports up into a batch-level total surfaced on
//! [`crate::ProgressSnapshot`] and [`crate::BulkReport`].
//! [`crate::BulkCtx::memoized_with_cached_cost`] stores value/cost pairs
//! when counters should be replayed on memo hits.
//!
//! The same type plays two roles: an interior-mutable accumulator while a
//! step runs (so `record_cost` takes `&self`), and a serializable value
//! that is carried in the per-item result envelope and merges into the
//! batch rollup.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};

/// A collection of named cost counters, accumulated as a pipeline runs and
/// rolled up across a batch.
///
/// Metric names are arbitrary strings; amounts add into a running total per
/// name. A [`BTreeMap`] backs the counters so serialization and
/// [`CostReport::entries`] are deterministically ordered.
#[derive(Debug, Default)]
pub struct CostReport {
    counters: Mutex<BTreeMap<String, f64>>,
}

impl CostReport {
    /// An empty report.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `amount` to the counter named `metric`, creating it at `amount`
    /// if it doesn't exist yet.
    pub fn record(&self, metric: impl Into<String>, amount: f64) {
        let mut counters = self.counters.lock().unwrap();
        *counters.entry(metric.into()).or_insert(0.0) += amount;
    }

    /// The current total for `metric`, or `0.0` if it has never been
    /// recorded.
    pub fn get(&self, metric: &str) -> f64 {
        self.counters
            .lock()
            .unwrap()
            .get(metric)
            .copied()
            .unwrap_or(0.0)
    }

    /// Add every counter from `other` into this report.
    pub fn merge(&self, other: &CostReport) {
        let snapshot = other.counters.lock().unwrap().clone();
        let mut counters = self.counters.lock().unwrap();
        for (metric, amount) in snapshot {
            *counters.entry(metric).or_insert(0.0) += amount;
        }
    }

    /// Snapshot the counters as `(metric, total)` pairs in name order.
    pub fn entries(&self) -> Vec<(String, f64)> {
        self.counters
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// `true` if no counter has ever been recorded.
    pub fn is_empty(&self) -> bool {
        self.counters.lock().unwrap().is_empty()
    }
}

impl Clone for CostReport {
    fn clone(&self) -> Self {
        Self {
            counters: Mutex::new(self.counters.lock().unwrap().clone()),
        }
    }
}

impl Serialize for CostReport {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.counters.lock().unwrap().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CostReport {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let counters = BTreeMap::<String, f64>::deserialize(deserializer)?;
        Ok(Self {
            counters: Mutex::new(counters),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_accumulates_per_metric() {
        let report = CostReport::new();
        report.record("tokens", 100.0);
        report.record("tokens", 50.0);
        report.record("calls", 1.0);
        assert_eq!(report.get("tokens"), 150.0);
        assert_eq!(report.get("calls"), 1.0);
    }

    #[test]
    fn get_returns_zero_for_unknown_metric() {
        let report = CostReport::new();
        assert_eq!(report.get("missing"), 0.0);
    }

    #[test]
    fn merge_sums_overlapping_and_disjoint_metrics() {
        let a = CostReport::new();
        a.record("tokens", 100.0);
        a.record("usd", 1.0);
        let b = CostReport::new();
        b.record("tokens", 25.0);
        b.record("calls", 3.0);

        a.merge(&b);
        assert_eq!(a.get("tokens"), 125.0);
        assert_eq!(a.get("usd"), 1.0);
        assert_eq!(a.get("calls"), 3.0);
    }

    #[test]
    fn entries_are_name_ordered() {
        let report = CostReport::new();
        report.record("zeta", 1.0);
        report.record("alpha", 2.0);
        assert_eq!(
            report.entries(),
            vec![("alpha".to_string(), 2.0), ("zeta".to_string(), 1.0)],
        );
    }

    #[test]
    fn serde_round_trips_through_rmp() {
        let report = CostReport::new();
        report.record("tokens", 42.0);
        report.record("usd", 0.5);
        let bytes = rmp_serde::to_vec_named(&report).unwrap();
        let restored: CostReport = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(restored.entries(), report.entries());
    }

    #[test]
    fn clone_is_an_independent_snapshot() {
        let original = CostReport::new();
        original.record("tokens", 10.0);
        let snapshot = original.clone();
        original.record("tokens", 5.0);
        assert_eq!(snapshot.get("tokens"), 10.0);
        assert_eq!(original.get("tokens"), 15.0);
    }
}

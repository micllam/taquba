//! An in-memory lower bound on the times of the live keys of a
//! time-ordered key space, so a scan of the key space returns without a
//! read until a key is due and starts at the bound.

use std::sync::atomic::{AtomicU64, Ordering};

/// A lower bound on the times of the live keys of one key space whose
/// keys lead with the time of their event.
///
/// Invariant: the bound does not exceed the time of any live key whose
/// writer calls [`lower`](Self::lower) after the commit of the key. A
/// scan raises the bound on the
/// evidence of its read alone: every key before the new value is read
/// and removed. The bound is process state and starts at zero, so the
/// first scan after an open reads from the front of the key space.
#[derive(Debug)]
pub(crate) struct TimeBound {
    bound: AtomicU64,
}

impl TimeBound {
    pub(crate) const fn new() -> Self {
        Self {
            bound: AtomicU64::new(0),
        }
    }

    /// Lowers the bound to `at_ms` for a key at that time, after the
    /// commit that writes the key. A scan that ends between a call
    /// before the commit and the commit raises the bound past the key.
    pub(crate) fn lower(&self, at_ms: u64) {
        self.bound.fetch_min(at_ms, Ordering::SeqCst);
    }

    /// Begins a scan at `now_ms`, or returns `None` when no key can be
    /// due. A key is due when its time is `retention_ms` or more before
    /// `now_ms`.
    pub(crate) fn begin(&self, now_ms: u64, retention_ms: u64) -> Option<Scan<'_>> {
        if !is_due(self.bound.load(Ordering::SeqCst), now_ms, retention_ms) {
            return None;
        }
        // A `lower` call during the scan lowers the bound below the
        // time the scan leaves.
        let from = self.bound.swap(u64::MAX, Ordering::SeqCst);
        Some(Scan {
            bound: &self.bound,
            now_ms,
            retention_ms,
            from,
            retained: u64::MAX,
            completed: false,
        })
    }
}

fn is_due(at_ms: u64, now_ms: u64, retention_ms: u64) -> bool {
    at_ms.saturating_add(retention_ms) <= now_ms
}

/// One scan of the key space from the bound. A completed scan leaves
/// the bound at the earliest time it retained, or at the maximum when
/// it retained no key. A scan dropped before [`complete`](Self::complete)
/// leaves the bound where the scan began.
#[derive(Debug)]
pub(crate) struct Scan<'a> {
    bound: &'a AtomicU64,
    now_ms: u64,
    retention_ms: u64,
    /// The bound at the start of the scan.
    from: u64,
    /// The earliest time of a key the scan read and left in the key
    /// space.
    retained: u64,
    completed: bool,
}

impl Scan<'_> {
    /// The time the scan starts at.
    pub(crate) fn from(&self) -> u64 {
        self.from
    }

    /// Whether a key at `at_ms` is due.
    pub(crate) fn due(&self, at_ms: u64) -> bool {
        is_due(at_ms, self.now_ms, self.retention_ms)
    }

    /// Records a key at `at_ms` that the scan leaves in the key space:
    /// the first key that is not due, or a key kept.
    pub(crate) fn retain(&mut self, at_ms: u64) {
        self.retained = self.retained.min(at_ms);
    }

    /// Ends the scan with every key before the earliest retained time
    /// removed.
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for Scan<'_> {
    fn drop(&mut self) {
        let left = if self.completed {
            self.retained
        } else {
            self.from
        };
        self.bound.fetch_min(left, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_returns_none_until_the_bound_is_due() {
        let bound = TimeBound::new();
        bound.begin(0, 0).unwrap().complete();
        assert!(bound.begin(5_000, 1_000).is_none());

        bound.lower(1_000);
        assert!(bound.begin(1_999, 1_000).is_none());
        let scan = bound.begin(2_000, 1_000).unwrap();
        assert_eq!(scan.from(), 1_000);
        assert!(scan.due(1_000));
        assert!(!scan.due(1_001));
    }

    #[test]
    fn a_completed_scan_leaves_the_bound_at_the_earliest_retained_time() {
        let bound = TimeBound::new();
        let mut scan = bound.begin(2_000, 0).unwrap();
        scan.retain(1_500);
        scan.retain(1_200);
        scan.complete();
        assert!(bound.begin(1_199, 0).is_none());
        assert_eq!(bound.begin(1_200, 0).unwrap().from(), 1_200);
    }

    #[test]
    fn a_dropped_scan_leaves_the_bound_where_it_began() {
        let bound = TimeBound::new();
        bound.begin(0, 0).unwrap().complete();
        bound.lower(1_000);
        let mut scan = bound.begin(2_000, 0).unwrap();
        scan.retain(1_500);
        drop(scan);
        assert_eq!(bound.begin(2_000, 0).unwrap().from(), 1_000);
    }

    #[test]
    fn a_lower_call_during_a_scan_is_kept_by_the_completion() {
        let bound = TimeBound::new();
        let mut scan = bound.begin(2_000, 0).unwrap();
        bound.lower(1_500);
        scan.retain(1_800);
        scan.complete();
        assert_eq!(bound.begin(2_000, 0).unwrap().from(), 1_500);
    }
}

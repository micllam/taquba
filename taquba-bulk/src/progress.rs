//! Batch-level progress and the final run report.

use std::time::{Duration, Instant};

use crate::cost::CostReport;

/// A point-in-time view of a bulk run's progress. Returned by
/// [`Bulk::progress`](crate::Bulk::progress) and suitable for a status line
/// or a polling UI.
#[derive(Debug, Clone)]
pub struct ProgressSnapshot {
    /// Number of items expected to complete (set once submission finishes;
    /// `0` while items are still being submitted).
    pub total: usize,
    /// Items that have reached any terminal state.
    pub completed: usize,
    /// Items that terminated successfully.
    pub succeeded: usize,
    /// Items that terminated failed.
    pub failed: usize,
    /// Items that terminated cancelled.
    pub cancelled: usize,
    /// Wall-clock time since the run started.
    pub elapsed: Duration,
    /// Completed items per second over the elapsed window.
    pub rate_per_sec: f64,
    /// Estimated time to finish the remaining items at the current rate, or
    /// `None` when the total is unknown or the rate is zero.
    pub time_remaining: Option<Duration>,
    /// Cost counters rolled up across completed items.
    pub cost: CostReport,
}

/// The outcome of a finished (or drained) bulk run, returned by
/// [`Bulk::run`](crate::Bulk::run).
#[derive(Debug, Clone)]
pub struct BulkReport {
    /// Number of items that were expected to complete.
    pub total: usize,
    /// Items that terminated successfully.
    pub succeeded: usize,
    /// Items that terminated failed.
    pub failed: usize,
    /// Items that terminated cancelled.
    pub cancelled: usize,
    /// Wall-clock duration of the run.
    pub elapsed: Duration,
    /// Cost counters rolled up across all completed items.
    pub cost: CostReport,
    /// Run identifiers of the items that failed. Because their memo entries
    /// are retained, re-submitting these ids resumes from the last cached
    /// step rather than recomputing from scratch.
    pub failed_run_ids: Vec<String>,
}

/// Internal, mutex-guarded counters updated by the terminal hook and read by
/// [`ProgressSnapshot`] / [`BulkReport`].
#[derive(Debug)]
pub(crate) struct ProgressState {
    pub total: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub cost: CostReport,
    pub failed_run_ids: Vec<String>,
    started_at: Instant,
}

impl ProgressState {
    pub(crate) fn new() -> Self {
        Self {
            total: 0,
            succeeded: 0,
            failed: 0,
            cancelled: 0,
            cost: CostReport::new(),
            failed_run_ids: Vec::new(),
            started_at: Instant::now(),
        }
    }

    pub(crate) fn completed(&self) -> usize {
        self.succeeded + self.failed + self.cancelled
    }

    /// True once submission has set a total and every expected item has
    /// reached a terminal state.
    pub(crate) fn is_done(&self) -> bool {
        self.total > 0 && self.completed() >= self.total
    }

    pub(crate) fn snapshot(&self) -> ProgressSnapshot {
        let elapsed = self.started_at.elapsed();
        let completed = self.completed();
        let secs = elapsed.as_secs_f64();
        let rate_per_sec = if secs > 0.0 {
            completed as f64 / secs
        } else {
            0.0
        };
        let remaining = self.total.saturating_sub(completed);
        let time_remaining = if rate_per_sec > 0.0 && remaining > 0 {
            Some(Duration::from_secs_f64(remaining as f64 / rate_per_sec))
        } else {
            None
        };
        ProgressSnapshot {
            total: self.total,
            completed,
            succeeded: self.succeeded,
            failed: self.failed,
            cancelled: self.cancelled,
            elapsed,
            rate_per_sec,
            time_remaining,
            cost: self.cost.clone(),
        }
    }

    pub(crate) fn to_report(&self) -> BulkReport {
        BulkReport {
            total: self.total,
            succeeded: self.succeeded,
            failed: self.failed,
            cancelled: self.cancelled,
            elapsed: self.started_at.elapsed(),
            cost: self.cost.clone(),
            failed_run_ids: self.failed_run_ids.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_sums_terminal_buckets() {
        let mut st = ProgressState::new();
        st.succeeded = 3;
        st.failed = 2;
        st.cancelled = 1;
        assert_eq!(st.completed(), 6);
    }

    #[test]
    fn is_done_requires_a_total() {
        let mut st = ProgressState::new();
        st.succeeded = 5;
        assert!(!st.is_done(), "no total set yet");
        st.total = 5;
        assert!(st.is_done());
    }

    #[test]
    fn snapshot_reports_no_time_remaining_before_progress() {
        let mut st = ProgressState::new();
        st.total = 10;
        let snap = st.snapshot();
        assert_eq!(snap.total, 10);
        assert_eq!(snap.completed, 0);
        assert!(snap.time_remaining.is_none());
    }
}

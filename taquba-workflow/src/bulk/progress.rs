//! Batch-level progress and the final run report.

use std::time::{Duration, Instant};

use crate::bulk::cost::CostReport;

/// The durable state of a batch, read from its manifest and member
/// records by [`Batch::status`](crate::bulk::Batch::status). An item
/// counts by its last recorded outcome; an item of the manifest with no
/// member record was not submitted.
#[derive(Debug, Clone)]
pub struct BatchStatus {
    /// The batch id.
    pub batch_id: String,
    /// Number of items in the batch's manifest.
    pub total: usize,
    /// Items submitted and not yet terminated.
    pub pending: usize,
    /// Items whose last recorded outcome is a success.
    pub succeeded: usize,
    /// Items whose last recorded outcome is a failure.
    pub failed: usize,
    /// Items whose last recorded outcome is a cancellation.
    pub cancelled: usize,
    /// Cost counters rolled up across the succeeded and failed items.
    pub cost: CostReport,
    /// Keys of the items whose last recorded outcome is a failure.
    pub failed_keys: Vec<String>,
}

/// A point-in-time view of a batch execution's progress. Returned by
/// [`Batch::progress`](crate::bulk::Batch::progress) and suitable for a
/// status line or a polling UI.
#[derive(Debug, Clone)]
pub struct ProgressSnapshot {
    /// Number of items in the batch.
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
/// [`Batch::run`](crate::bulk::Batch::run).
#[derive(Debug, Clone)]
pub struct BatchReport {
    /// The batch this report describes.
    pub batch_id: String,
    /// Number of items that were expected to complete.
    pub total: usize,
    /// Items that terminated successfully.
    pub succeeded: usize,
    /// Items that terminated failed.
    pub failed: usize,
    /// Items that terminated cancelled.
    pub cancelled: usize,
    /// Wall-clock duration of the batch execution.
    pub elapsed: Duration,
    /// Cost counters rolled up across all completed items.
    pub cost: CostReport,
    /// Keys of the items that failed. A later `Batch::run` of the same
    /// batch runs them again and skips the items that succeeded.
    pub failed_keys: Vec<String>,
}

/// Internal, mutex-guarded counters of a batch being run, updated as its
/// items terminate and read by [`ProgressSnapshot`] / [`BatchReport`].
#[derive(Debug)]
pub(crate) struct ProgressState {
    pub total: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub cost: CostReport,
    pub failed_keys: Vec<String>,
    started_at: Instant,
}

impl ProgressState {
    pub(crate) fn new(total: usize) -> Self {
        Self {
            total,
            succeeded: 0,
            failed: 0,
            cancelled: 0,
            cost: CostReport::new(),
            failed_keys: Vec::new(),
            started_at: Instant::now(),
        }
    }

    pub(crate) fn completed(&self) -> usize {
        self.succeeded + self.failed + self.cancelled
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

    pub(crate) fn to_report(&self, batch_id: &str) -> BatchReport {
        BatchReport {
            batch_id: batch_id.to_string(),
            total: self.total,
            succeeded: self.succeeded,
            failed: self.failed,
            cancelled: self.cancelled,
            elapsed: self.started_at.elapsed(),
            cost: self.cost.clone(),
            failed_keys: self.failed_keys.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_sums_terminal_buckets() {
        let mut st = ProgressState::new(6);
        st.succeeded = 3;
        st.failed = 2;
        st.cancelled = 1;
        assert_eq!(st.completed(), 6);
    }

    #[test]
    fn snapshot_reports_no_time_remaining_before_progress() {
        let st = ProgressState::new(10);
        let snap = st.snapshot();
        assert_eq!(snap.total, 10);
        assert_eq!(snap.completed, 0);
        assert!(snap.time_remaining.is_none());
    }
}

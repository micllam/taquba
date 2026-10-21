//! The public types of the signal wait: a scheduled job waits for a signal, and
//! a signal to the job's id wakes it before its `run_at` or is rejected.

/// Outcome of [`Queue::signal`](crate::Queue::signal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalOutcome {
    /// The job was waiting for a signal and is now pending, with the payload on
    /// its record.
    Delivered,
    /// The job is absent or is not waiting for a signal. Nothing was changed.
    NotWaiting,
}

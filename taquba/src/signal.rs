//! The public types of the signal wait: a scheduled job waits at a signal key,
//! and a signal to the key wakes it before its `run_at` or is buffered for the
//! next job that waits at the key.

/// Outcome of [`Queue::signal`](crate::Queue::signal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalOutcome {
    /// A scheduled job was waiting at the key and is now pending, with the
    /// payload on its record.
    Delivered,
    /// No job was waiting at the key. The signal is buffered at the key until
    /// the next job enqueued with the key consumes it or
    /// [`Queue::clear_signal`](crate::Queue::clear_signal) discards it.
    Buffered,
}

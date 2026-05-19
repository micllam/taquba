//! Time source for taquba.
//!
//! Every state transition that records a timestamp (`enqueued_at`,
//! `completed_at`, `failed_at`, `lease_expires_at`) and every comparison
//! against a stored timestamp (retention cutoffs, scheduled-job
//! promotion) reads the current time through a [`Clock`]. Production
//! callers leave [`OpenOptions::clock`](crate::OpenOptions::clock) at
//! its default [`SystemClock`]; tests can substitute [`MockClock`] to
//! advance time deterministically without `std::thread::sleep` or
//! `tokio::time::sleep`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A source of "current time" in milliseconds since the UNIX epoch.
///
/// Carried by [`Queue`](crate::Queue) (and threaded into its background
/// reaper / scheduler tasks) so the same wall-clock semantics apply
/// everywhere. Implementors must be cheap to call and thread-safe
/// (the value is read from many tokio tasks concurrently).
pub trait Clock: Send + Sync + 'static {
    /// Return the current time as milliseconds since the UNIX epoch.
    fn now_ms(&self) -> u64;
}

/// Reads `SystemTime::now()` and converts to milliseconds since the
/// UNIX epoch. The default for
/// [`OpenOptions::clock`](crate::OpenOptions::clock).
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before epoch")
            .as_millis() as u64
    }
}

/// A clock whose value the caller controls. Useful in tests where time
/// has to move past a retention window or a scheduled-job `run_at`
/// without waiting on the real clock.
///
/// `MockClock` is cheaply cloneable: every clone shares the same
/// underlying value, so advancing one advances all of them. Pass a
/// clone into `OpenOptions::clock` and keep the original around to
/// call [`advance`](Self::advance) / [`set`](Self::set) from the test
/// body.
///
/// ```
/// use std::time::Duration;
/// use taquba::{Clock, MockClock};
///
/// let clock = MockClock::new(1_700_000_000_000);
/// assert_eq!(clock.now_ms(), 1_700_000_000_000);
/// clock.advance(Duration::from_secs(60));
/// assert_eq!(clock.now_ms(), 1_700_000_060_000);
/// ```
#[derive(Debug, Clone, Default)]
pub struct MockClock(Arc<AtomicU64>);

impl MockClock {
    /// Create a clock whose initial value is `initial_ms`.
    pub fn new(initial_ms: u64) -> Self {
        Self(Arc::new(AtomicU64::new(initial_ms)))
    }

    /// Advance the clock by `by`. Subsequent [`now_ms`](Clock::now_ms)
    /// reads, including from any task that received a clone of this
    /// clock, return the new value.
    pub fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_millis() as u64, Ordering::SeqCst);
    }

    /// Set the clock to an absolute millisecond value.
    pub fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

pub(crate) fn default_clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

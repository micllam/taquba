//! The lease capability passed to job handlers.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::lease_registry::LeaseRegistry;

/// Margin added on top of a requested remaining duration, so a delivery
/// that runs to its declared bound still has lease left to settle in.
pub(crate) const SETTLEMENT_MARGIN: Duration = Duration::from_secs(5);

/// A handle on one claim's lease, passed to [`crate::Worker::process`]
/// by the worker loop.
///
/// The handle extends the lease without exposing the claim or the
/// queue, so a handler can prevent lease expiry during a long delivery
/// but cannot settle its own job. It is cheap to clone; clones refer to
/// the same lease.
///
/// [`LeaseHandle::detached`] builds a handle bound to no queue, whose
/// calls succeed without effect, so handler types stay constructible in
/// unit tests.
#[derive(Clone)]
pub struct LeaseHandle {
    inner: Option<Inner>,
}

#[derive(Clone)]
struct Inner {
    registry: LeaseRegistry,
    clock: Arc<dyn Clock>,
    queue: String,
    id: String,
    token: u64,
    cancel: Option<CancellationToken>,
}

impl LeaseHandle {
    pub(crate) fn new(
        registry: LeaseRegistry,
        clock: Arc<dyn Clock>,
        queue: String,
        id: String,
        token: u64,
        cancel: Option<CancellationToken>,
    ) -> Self {
        Self {
            inner: Some(Inner {
                registry,
                clock,
                queue,
                id,
                token,
                cancel,
            }),
        }
    }

    /// A handle bound to no lease. Every call succeeds without effect.
    /// For constructing handler-facing types in unit tests.
    pub fn detached() -> Self {
        Self { inner: None }
    }

    /// Ensure the lease lasts at least `remaining` from now, extending
    /// it if it would end sooner. An internal margin is added on top of
    /// `remaining`, so a delivery that runs to the requested bound
    /// still has lease left to settle in. The lease is never shortened.
    ///
    /// Call this at progress points of a long-running delivery. For a
    /// single slow call, give the call a timeout and pass that timeout
    /// here before issuing it, so the lease covers the call.
    ///
    /// Fails with [`Error::ClaimLost`] once the claim has ended or the
    /// reaper has begun re-queuing the expired lease; stop working on
    /// the delivery then, since another claim may already own the job.
    /// Fails with [`Error::CancelRequested`] once cancellation of the
    /// job has been requested, leaving the lease to expire.
    pub fn ensure_at_least(&self, remaining: Duration) -> Result<()> {
        let Some(inner) = &self.inner else {
            return Ok(());
        };
        if inner.cancel.as_ref().is_some_and(|t| t.is_cancelled()) {
            return Err(Error::CancelRequested);
        }
        let needed = inner.clock.now_ms()
            + remaining.as_millis() as u64
            + SETTLEMENT_MARGIN.as_millis() as u64;
        match inner.registry.current(&inner.queue, &inner.id) {
            Some((expires_at, token)) if token == inner.token => {
                if expires_at >= needed {
                    return Ok(());
                }
            }
            _ => return Err(Error::ClaimLost),
        }
        if !inner
            .registry
            .renew(&inner.queue, &inner.id, inner.token, needed)
        {
            return Err(Error::ClaimLost);
        }
        crate::obs::renewed(&inner.queue);
        debug!(queue = %inner.queue, job_id = %inner.id, new_expiry = needed, "lease extended");
        Ok(())
    }
}

impl std::fmt::Debug for LeaseHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Some(inner) => f
                .debug_struct("LeaseHandle")
                .field("queue", &inner.queue)
                .field("id", &inner.id)
                .finish_non_exhaustive(),
            None => f.write_str("LeaseHandle::detached"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;

    #[test]
    fn a_detached_handle_succeeds_without_effect() {
        let handle = LeaseHandle::detached();
        assert!(handle.ensure_at_least(Duration::from_secs(600)).is_ok());
    }

    #[test]
    fn ensure_at_least_extends_only_when_short() {
        let registry = LeaseRegistry::new();
        let clock = MockClock::new(1_000_000);
        registry.insert("q", "a", 1_030_000, 7);
        let handle = LeaseHandle::new(
            registry.clone(),
            Arc::new(clock.clone()),
            "q".into(),
            "a".into(),
            7,
            None,
        );

        // Covered: 10s + margin fits inside the 30s lease.
        handle.ensure_at_least(Duration::from_secs(10)).unwrap();
        assert!(registry.contains("q", "a", 1_030_000));

        // Short: extended to now + remaining + margin.
        handle.ensure_at_least(Duration::from_secs(60)).unwrap();
        let expected = 1_000_000 + 60_000 + SETTLEMENT_MARGIN.as_millis() as u64;
        assert!(registry.contains("q", "a", expected));
    }

    #[test]
    fn ensure_at_least_fails_once_the_claim_ended() {
        let registry = LeaseRegistry::new();
        let clock = MockClock::new(1_000_000);
        registry.insert("q", "a", 1_030_000, 7);
        let handle = LeaseHandle::new(
            registry.clone(),
            Arc::new(clock.clone()),
            "q".into(),
            "a".into(),
            7,
            None,
        );

        registry.remove("q", "a", 7);
        assert!(matches!(
            handle.ensure_at_least(Duration::from_secs(10)),
            Err(Error::ClaimLost)
        ));

        // A re-claim of the same job is a different token.
        registry.insert("q", "a", 1_030_000, 8);
        assert!(matches!(
            handle.ensure_at_least(Duration::from_secs(10)),
            Err(Error::ClaimLost)
        ));
    }

    #[test]
    fn ensure_at_least_is_refused_once_cancellation_is_requested() {
        let registry = LeaseRegistry::new();
        let clock = MockClock::new(1_000_000);
        registry.insert("q", "a", 1_030_000, 7);
        let cancel = CancellationToken::new();
        let handle = LeaseHandle::new(
            registry.clone(),
            Arc::new(clock.clone()),
            "q".into(),
            "a".into(),
            7,
            Some(cancel.clone()),
        );

        handle.ensure_at_least(Duration::from_secs(60)).unwrap();

        cancel.cancel();
        let before = registry.current("q", "a");
        assert!(matches!(
            handle.ensure_at_least(Duration::from_secs(600)),
            Err(Error::CancelRequested)
        ));
        assert_eq!(registry.current("q", "a"), before);
    }
}

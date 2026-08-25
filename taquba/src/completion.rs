//! In-process registry of tasks waiting for a job's terminal transition.
//!
//! A waiter registers its job id before it reads the job's state, so a
//! transition that commits after the read reaches it through the
//! registry; one that commits before the read is visible in the
//! read. Every terminal transition settles its job's waiters after its
//! commit, with the outcome the transition wrote, so a waiter learns
//! the outcome without a second read. The outcome is built only when
//! the job has waiters, so a transition without one costs a map lookup.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::WaitOutcome;

#[derive(Default)]
pub(crate) struct CompletionWaiters {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    next_key: u64,
    waiters: HashMap<String, Vec<(u64, oneshot::Sender<WaitOutcome>)>>,
}

/// One registered waiter. Dropping it withdraws the registration.
pub(crate) struct Registration {
    registry: Arc<CompletionWaiters>,
    id: String,
    key: u64,
    receiver: oneshot::Receiver<WaitOutcome>,
}

impl CompletionWaiters {
    /// Register a waiter for `id`. Synchronous, so a caller can register
    /// before its first await.
    pub(crate) fn register(self: &Arc<Self>, id: &str) -> Registration {
        let (sender, receiver) = oneshot::channel();
        let key = {
            let mut inner = self.inner.lock().expect("completion waiters lock");
            let key = inner.next_key;
            inner.next_key += 1;
            inner
                .waiters
                .entry(id.to_string())
                .or_default()
                .push((key, sender));
            key
        };
        Registration {
            registry: self.clone(),
            id: id.to_string(),
            key,
            receiver,
        }
    }

    /// Whether any task is waiting on `id`.
    pub(crate) fn has_waiters(&self, id: &str) -> bool {
        self.inner
            .lock()
            .expect("completion waiters lock")
            .waiters
            .contains_key(id)
    }

    /// Deliver the terminal outcome of `id` to its waiters, if any.
    pub(crate) fn settle(&self, id: &str, outcome: impl FnOnce() -> WaitOutcome) {
        let waiters = {
            let mut inner = self.inner.lock().expect("completion waiters lock");
            inner.waiters.remove(id)
        };
        let Some(waiters) = waiters else {
            return;
        };
        let outcome = outcome();
        for (_, sender) in waiters {
            let _ = sender.send(outcome.clone());
        }
    }

    #[cfg(test)]
    pub(crate) fn inner_is_empty(&self) -> bool {
        self.inner
            .lock()
            .expect("completion waiters lock")
            .waiters
            .is_empty()
    }

    fn withdraw(&self, id: &str, key: u64) {
        let mut inner = self.inner.lock().expect("completion waiters lock");
        if let Some(waiters) = inner.waiters.get_mut(id) {
            waiters.retain(|(k, _)| *k != key);
            if waiters.is_empty() {
                inner.waiters.remove(id);
            }
        }
    }
}

impl Registration {
    /// The outcome, if it was delivered already.
    pub(crate) fn try_outcome(&mut self) -> Option<WaitOutcome> {
        self.receiver.try_recv().ok()
    }

    /// The channel the outcome arrives on.
    pub(crate) fn receiver(&mut self) -> &mut oneshot::Receiver<WaitOutcome> {
        &mut self.receiver
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.registry.withdraw(&self.id, self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_settled_outcome_reaches_every_waiter_of_that_job_only() {
        let registry = Arc::new(CompletionWaiters::default());
        let mut a = registry.register("a");
        let mut b = registry.register("a");
        let mut other = registry.register("b");
        registry.settle("a", || WaitOutcome::Cancelled);
        assert!(matches!(a.try_outcome(), Some(WaitOutcome::Cancelled)));
        assert!(matches!(b.try_outcome(), Some(WaitOutcome::Cancelled)));
        assert!(other.try_outcome().is_none());
    }

    #[tokio::test]
    async fn a_dropped_registration_is_withdrawn_and_builds_no_outcome() {
        let registry = Arc::new(CompletionWaiters::default());
        drop(registry.register("a"));
        let mut built = false;
        registry.settle("a", || {
            built = true;
            WaitOutcome::Cancelled
        });
        assert!(!built);
        assert!(registry.inner_is_empty());
    }
}

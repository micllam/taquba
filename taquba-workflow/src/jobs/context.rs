use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{EffectsHandle, Memo, Step};
use taquba::LeaseHandle;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::jobs::handle::JobHandle;
use crate::jobs::job::Job;
use crate::jobs::runner::{Inner, SubmitOptions};

/// Type-erased application state shared with every job handler.
#[derive(Default)]
pub(crate) struct State {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl State {
    pub(crate) fn insert<T: Any + Send + Sync>(&mut self, value: T) {
        self.map.insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub(crate) fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
    }
}

/// The per-call context handed to [`Job::run`].
///
/// Provides access to application state registered on the
/// [`JobRunner`](crate::jobs::JobRunner), the job's identity and attempt count,
/// the delivery's lease and cancellation token, the job's memo and staged
/// KV effects, and follow-up submission. It holds no domain-specific
/// clients (HTTP, LLM, etc.); those belong to the application's
/// registered state or to layers built on top.
pub struct JobContext<'a> {
    inner: Arc<Inner>,
    step: &'a Step,
}

impl<'a> JobContext<'a> {
    pub(crate) fn new(inner: Arc<Inner>, step: &'a Step) -> Self {
        Self { inner, step }
    }

    /// Borrow a piece of application state by type.
    ///
    /// State is registered on the runner via
    /// [`JobRunnerBuilder::state`](crate::jobs::JobRunnerBuilder::state).
    ///
    /// # Panics
    ///
    /// Panics if no value of type `T` was registered. Use
    /// [`try_state`](Self::try_state) for a non-panicking lookup.
    pub fn state<T: Any + Send + Sync>(&self) -> &T {
        self.try_state().unwrap_or_else(|| {
            panic!(
                "no application state of type `{}` registered on the JobRunner",
                std::any::type_name::<T>()
            )
        })
    }

    /// Borrow a piece of application state by type, returning `None` if no
    /// value of type `T` was registered.
    pub fn try_state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.inner.state.get::<T>()
    }

    /// The identifier of the job currently executing, equal to
    /// [`JobHandle::id`] of its handle.
    pub fn id(&self) -> &str {
        &self.step.run_id
    }

    /// How many delivery attempts have been made for this job, including the
    /// current one. `1` on the first attempt.
    pub fn attempt(&self) -> u32 {
        self.step.attempts
    }

    /// The cooperative cancellation token for this job.
    ///
    /// `select!` on [`CancellationToken::cancelled`] to short-circuit when
    /// the job is cancelled. Cancellation is cooperative: a handler that
    /// ignores the token runs to completion.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.step.cancel_token
    }

    /// The lease handle for this delivery.
    ///
    /// A long-running handler calls
    /// [`LeaseHandle::ensure_at_least`] at progress points (or once,
    /// with a slow call's timeout, before issuing it) so the lease
    /// covers the remaining work and the job is not re-queued while it
    /// still runs.
    pub fn lease(&self) -> &LeaseHandle {
        &self.step.lease
    }

    /// The job's durable memo, scoped to this job. A handler records the
    /// results of expensive calls in it, and a retried attempt reads them
    /// back without repeating the calls; see [`Memo::memoized`].
    pub fn memo(&self) -> &Memo {
        &self.step.memo
    }

    /// The job's staged application KV effects, applied atomically with
    /// the job's successful completion; a failing attempt applies nothing.
    pub fn effects(&self) -> &EffectsHandle {
        &self.step.effects
    }

    /// Read a committed value from Taquba's caller KV namespace. Effects
    /// staged by this job are not visible until it completes.
    pub async fn kv_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.step.kv.get(key).await?.map(|bytes| bytes.to_vec()))
    }

    /// Submit a follow-up job from within a handler.
    ///
    /// Used for chaining (one job triggers the next) or fan-out (call this
    /// in a loop to submit N independent children). Returns a [`JobHandle`]
    /// to the newly submitted job. The child job is independent: it is not
    /// awaited as part of the current job and survives the current job's
    /// completion.
    pub async fn submit<J: Job>(&self, job: J) -> Result<JobHandle<J>> {
        self.inner.submit(job, SubmitOptions::default()).await
    }
}

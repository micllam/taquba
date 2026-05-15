use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use taquba::Queue;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::handle::JobHandle;
use crate::job::Job;
use crate::runner::{SubmitOptions, Submitter};

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
/// [`JobRunner`](crate::JobRunner), the underlying queue, the job's identity
/// and attempt count, and a cooperative cancellation token. It deliberately
/// carries no domain-specific clients (HTTP, LLM, etc.); those belong to the
/// application's registered state or to specialized layers built on top.
pub struct JobContext<'a> {
    submitter: &'a Submitter,
    job_id: &'a str,
    attempt: u32,
    cancel_token: CancellationToken,
}

impl<'a> JobContext<'a> {
    pub(crate) fn new(
        submitter: &'a Submitter,
        job_id: &'a str,
        attempt: u32,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            submitter,
            job_id,
            attempt,
            cancel_token,
        }
    }

    /// Borrow a piece of application state by type.
    ///
    /// State is registered on the runner via
    /// [`JobRunnerBuilder::state`](crate::JobRunnerBuilder::state).
    ///
    /// # Panics
    ///
    /// Panics if no value of type `T` was registered. Use
    /// [`try_state`](Self::try_state) for a non-panicking lookup.
    pub fn state<T: Any + Send + Sync>(&self) -> &'a T {
        self.try_state().unwrap_or_else(|| {
            panic!(
                "no application state of type `{}` registered on the JobRunner",
                std::any::type_name::<T>()
            )
        })
    }

    /// Borrow a piece of application state by type, returning `None` if no
    /// value of type `T` was registered.
    pub fn try_state<T: Any + Send + Sync>(&self) -> Option<&'a T> {
        self.submitter.state().get::<T>()
    }

    /// The ULID of the job currently executing.
    pub fn job_id(&self) -> &str {
        self.job_id
    }

    /// How many delivery attempts have been made for this job, including the
    /// current one. `1` on the first attempt.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The cooperative cancellation token for this job.
    ///
    /// `select!` on [`CancellationToken::cancelled`] to short-circuit when
    /// [`taquba::Queue::cancel`] is called for this job. Cancellation is
    /// cooperative: a handler that ignores the token simply runs to
    /// completion.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// The underlying taquba queue, for direct queue operations.
    pub fn queue(&self) -> &'a Arc<Queue> {
        self.submitter.queue()
    }

    /// Submit a follow-up job from within a handler.
    ///
    /// Useful for chaining (one job triggers the next) or fan-out (call this
    /// in a loop to spawn N independent children). Returns a [`JobHandle`]
    /// to the newly submitted job. The child job is independent: it is not
    /// awaited as part of the current job and survives the current job's
    /// completion.
    pub async fn submit<J: Job>(&self, job: J) -> Result<JobHandle<J>> {
        self.submitter.submit(job, SubmitOptions::default()).await
    }
}

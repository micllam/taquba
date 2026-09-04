use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::{RunOutcome, RunStatus, RunTermination, StepErrorKind, TerminalStatus};
use thiserror::Error;

use crate::jobs::job::Job;
use crate::jobs::runner::Inner;
use crate::{Error, Result};

/// The logical failure outcome of a job that did not succeed.
///
/// Distinct from [`Error`](enum@Error), which is an infrastructure
/// failure: a `JobError` means the job terminated unsuccessfully. The
/// concrete `Job::Error` value is not persisted, so this holds its
/// [`Display`](std::fmt::Display) message and classification.
#[derive(Debug, Clone, Error)]
#[error("job failed ({kind:?}): {message}")]
pub struct JobError {
    /// Whether the failure was classified transient (the job exhausted its
    /// attempts) or permanent (dead-lettered on the failing attempt).
    /// Transient for a job cancelled or terminated outside its handler.
    pub kind: StepErrorKind,
    /// The failure message.
    pub message: String,
}

/// The error produced by awaiting a [`JobHandle`] directly (via `.await`).
///
/// Flattens the two failure modes (infrastructure errors and the job's own
/// logical failure) into one type so `handle.await?` yields the job's
/// `Output` directly.
#[derive(Debug, Error)]
pub enum JoinError {
    /// An infrastructure error occurred while submitting, waiting or reading
    /// the outcome.
    #[error(transparent)]
    Infra(#[from] Error),
    /// The job ran to a terminal state but did not succeed.
    #[error(transparent)]
    Job(#[from] JobError),
}

/// A handle to a submitted job.
///
/// Returned by [`JobRunner::submit`](crate::jobs::JobRunner::submit). Await it
/// directly for the typed result, or use [`join`](Self::join),
/// [`fetch_result`](Self::fetch_result) and [`status`](Self::status) for
/// more control.
///
/// Awaiting is [`WorkflowRuntime::wait`](crate::WorkflowRuntime::wait)
/// on the job's run, which relies on Taquba's in-process completion
/// notification, so a handle is awaited in the same process that runs
/// the job. The outcome is durable regardless:
/// [`fetch_result`](Self::fetch_result) reads it back from object
/// storage after a restart.
pub struct JobHandle<J: Job> {
    id: String,
    inner: Arc<Inner>,
    newly_submitted: bool,
    _marker: PhantomData<fn() -> J>,
}

impl<J: Job> Clone for JobHandle<J> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            inner: self.inner.clone(),
            newly_submitted: self.newly_submitted,
            _marker: PhantomData,
        }
    }
}

impl<J: Job> JobHandle<J> {
    pub(crate) fn new(id: String, inner: Arc<Inner>, newly_submitted: bool) -> Self {
        Self {
            id,
            inner,
            newly_submitted,
            _marker: PhantomData,
        }
    }

    /// The job's identifier: a ULID, or the digest of the job's
    /// [`idempotency_key`](Job::idempotency_key) when it has one, so a
    /// submission that matched an earlier job returns that job's id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// True if the call that produced this handle submitted a new job;
    /// false if the call matched an in-flight or completed submission with
    /// the same [`Job::idempotency_key`](crate::jobs::Job::idempotency_key) and
    /// payload.
    ///
    /// For submissions without an `idempotency_key`, the value is always
    /// `true`. The value reflects the call that returned this handle and
    /// does not update as the job progresses; clones preserve it.
    pub fn newly_submitted(&self) -> bool {
        self.newly_submitted
    }

    /// The job's status, read from its durable state. A terminated job
    /// reports [`RunState::Terminated`](crate::RunState::Terminated)
    /// until [`JobRunnerBuilder::retention`](crate::jobs::JobRunnerBuilder::retention)
    /// removes its terminal record. Use
    /// [`fetch_result`](Self::fetch_result) to read a terminal outcome.
    pub async fn status(&self) -> Result<Option<RunStatus>> {
        self.inner.runtime.status(&self.id).await
    }

    /// Read the job's persisted result without waiting.
    ///
    /// Returns `None` when no run result record exists for this job: it
    /// is still pending or in flight, it terminated without a worker
    /// recording a result (a lease expiry dead-lettered it, or it was
    /// cancelled while pending), or the record was removed by retention.
    ///
    /// Reads from object storage, so it works across process restarts.
    pub async fn fetch_result(&self) -> Result<Option<std::result::Result<J::Output, JobError>>> {
        match self.inner.recorded_result(&self.id).await? {
            None => Ok(None),
            Some(result) => decode_end::<J>(result.termination, Some(result.outcome)).map(Some),
        }
    }

    /// Wait for the job to reach a terminal state and return its outcome.
    ///
    /// Waits indefinitely. Use [`join_timeout`](Self::join_timeout) to bound
    /// the wait.
    pub async fn join(&self) -> Result<std::result::Result<J::Output, JobError>> {
        let end = self.inner.runtime.wait(&self.id).await?;
        decode_end::<J>(end.termination, end.outcome)
    }

    /// Wait up to `timeout` for the job to reach a terminal state.
    ///
    /// Returns `Ok(None)` if the timeout elapses first. On completion
    /// the run result record is decoded; a job that reached a terminal
    /// state without one (a lease expiry dead-lettered it, or it was
    /// cancelled) is reported as a transient [`JobError`] from the
    /// termination the runtime retains, or with a generic message when
    /// it retains none.
    ///
    /// Returns [`Error::RunNotFound`] if the runtime has no record of
    /// the job.
    pub async fn join_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<std::result::Result<J::Output, JobError>>> {
        match self.inner.runtime.wait_timeout(&self.id, timeout).await? {
            None => Ok(None),
            Some(end) => decode_end::<J>(end.termination, end.outcome).map(Some),
        }
    }
}

/// The typed result of a terminated job: the decoded output of a
/// succeeded job, or the [`JobError`] of one that failed, was cancelled
/// or terminated without recording an outcome.
pub(crate) fn decode_end<J: Job>(
    termination: RunTermination,
    outcome: Option<RunOutcome>,
) -> Result<std::result::Result<J::Output, JobError>> {
    if let Some(outcome) = outcome
        && outcome.status == TerminalStatus::Succeeded
    {
        let output = outcome.result.unwrap_or_default();
        return Ok(Ok(rmp_serde::from_slice(&output)?));
    }
    let message = termination.error.unwrap_or_else(|| {
        match termination.status {
            TerminalStatus::Cancelled => "job cancelled",
            _ => "job terminated without recording an outcome",
        }
        .to_string()
    });
    Ok(Err(JobError {
        kind: termination.error_kind.unwrap_or(StepErrorKind::Transient),
        message,
    }))
}

impl<J: Job> IntoFuture for JobHandle<J> {
    type Output = std::result::Result<J::Output, JoinError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            match self.join().await {
                Ok(Ok(output)) => Ok(output),
                Ok(Err(job_error)) => Err(JoinError::Job(job_error)),
                Err(infra) => Err(JoinError::Infra(infra)),
            }
        })
    }
}

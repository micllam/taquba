use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::RunStatus;
use taquba::WaitOutcome;
use thiserror::Error;

use crate::jobs::error::{Error, Result};
use crate::jobs::job::{ErrorKind, Job};
use crate::jobs::runner::Inner;
use crate::outcome::{StoredErrorKind, StoredOutcome, read_outcome};

// `join` waits in chunks of this length; `wait_for_completion` needs a finite
// timeout, so an unbounded join loops over bounded waits.
const JOIN_CHUNK: Duration = Duration::from_secs(3600);

/// The logical failure outcome of a job that ran and did not succeed.
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
    pub kind: ErrorKind,
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
/// Awaiting is in-process: it relies on Taquba's in-process completion
/// notification, so a handle is awaited in the same process that runs the
/// job. The outcome is durable regardless: [`fetch_result`](Self::fetch_result)
/// reads it back from object storage after a restart.
pub struct JobHandle<J: Job> {
    id: String,
    /// The queue job backing the delivery, when known. A duplicate
    /// submission recognised only from the durable run record has none;
    /// `join_timeout` then polls the outcome record.
    queue_job_id: Option<String>,
    inner: Arc<Inner>,
    newly_submitted: bool,
    _marker: PhantomData<fn() -> J>,
}

impl<J: Job> Clone for JobHandle<J> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            queue_job_id: self.queue_job_id.clone(),
            inner: self.inner.clone(),
            newly_submitted: self.newly_submitted,
            _marker: PhantomData,
        }
    }
}

impl<J: Job> JobHandle<J> {
    pub(crate) fn new(
        id: String,
        queue_job_id: Option<String>,
        inner: Arc<Inner>,
        newly_submitted: bool,
    ) -> Self {
        Self {
            id,
            queue_job_id,
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

    /// The job's in-process status, or `None` once it has terminated or if
    /// this runtime never observed it. Use
    /// [`fetch_result`](Self::fetch_result) to read a terminal outcome.
    pub async fn status(&self) -> Option<RunStatus> {
        self.inner.runtime().status(&self.id).await
    }

    /// Read the job's persisted outcome without waiting.
    ///
    /// Returns `None` when no outcome record exists for this job: it is
    /// still pending or in flight, it terminated without a handler
    /// recording an outcome (a lease expiry dead-lettered it, or it was
    /// cancelled), or the record was removed by retention.
    ///
    /// Reads from object storage, so it works across process restarts.
    pub async fn fetch_result(&self) -> Result<Option<std::result::Result<J::Output, JobError>>> {
        match read_outcome(&self.inner.run_memo(&self.id)).await? {
            None => Ok(None),
            Some(record) => Ok(Some(decode_outcome::<J>(record.outcome)?)),
        }
    }

    /// Wait for the job to reach a terminal state and return its outcome.
    ///
    /// Waits indefinitely. Use [`join_timeout`](Self::join_timeout) to bound
    /// the wait.
    pub async fn join(&self) -> Result<std::result::Result<J::Output, JobError>> {
        loop {
            if let Some(outcome) = self.join_timeout(JOIN_CHUNK).await? {
                return Ok(outcome);
            }
        }
    }

    /// Wait up to `timeout` for the job to reach a terminal state.
    ///
    /// Returns `Ok(None)` if the timeout elapses first. On completion the
    /// outcome record is returned; if the job reached a terminal state
    /// without one (a lease expiry dead-lettered it, or it was cancelled),
    /// the outcome is synthesized from the queue record as a transient
    /// [`JobError`].
    ///
    /// Returns [`Error::JobNotFound`] if the queue has no record of the job
    /// and no outcome record exists.
    pub async fn join_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<std::result::Result<J::Output, JobError>>> {
        let Some(queue_job_id) = &self.queue_job_id else {
            return self.poll_outcome(timeout).await;
        };
        match self
            .inner
            .queue()
            .wait_for_completion(queue_job_id, timeout)
            .await?
        {
            WaitOutcome::TimedOut => Ok(None),
            WaitOutcome::NotFound => match self.fetch_result().await? {
                Some(outcome) => Ok(Some(outcome)),
                None => Err(Error::JobNotFound(self.id.clone())),
            },
            terminal @ (WaitOutcome::Done(_) | WaitOutcome::Dead(_) | WaitOutcome::Cancelled) => {
                if let Some(outcome) = self.fetch_result().await? {
                    return Ok(Some(outcome));
                }
                let message = match terminal {
                    WaitOutcome::Done(record) | WaitOutcome::Dead(record) => record.last_error,
                    _ => None,
                }
                .unwrap_or_else(|| "job terminated without recording an outcome".to_string());
                Ok(Some(Err(JobError {
                    kind: ErrorKind::Transient,
                    message,
                })))
            }
        }
    }

    /// Wait for the outcome record by polling, for a handle without a
    /// queue job id.
    async fn poll_outcome(
        &self,
        timeout: Duration,
    ) -> Result<Option<std::result::Result<J::Output, JobError>>> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(outcome) = self.fetch_result().await? {
                return Ok(Some(outcome));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(self.inner.poll_interval()).await;
        }
    }
}

fn decode_outcome<J: Job>(
    outcome: StoredOutcome,
) -> Result<std::result::Result<J::Output, JobError>> {
    match outcome {
        StoredOutcome::Success { output } => Ok(Ok(rmp_serde::from_slice(&output)?)),
        StoredOutcome::Failure { kind, message } => Ok(Err(JobError {
            kind: match kind {
                StoredErrorKind::Transient => ErrorKind::Transient,
                StoredErrorKind::Permanent => ErrorKind::Permanent,
            },
            message,
        })),
    }
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

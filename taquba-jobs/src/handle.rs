use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use taquba::{JobStatus, WaitOutcome};
use thiserror::Error;

use crate::error::{Error, Result};
use crate::job::{ErrorKind, Job};
use crate::result_store::StoredOutcome;
use crate::runner::Submitter;

// `join` waits in chunks of this length; `wait_for_completion` needs a finite
// timeout, so an unbounded join just loops over bounded waits.
const JOIN_CHUNK: Duration = Duration::from_secs(3600);

/// The logical failure outcome of a job that ran and did not succeed.
///
/// Distinct from [`crate::Error`]: an [`crate::Error`] is an infrastructure
/// failure (queue, object store), whereas a `JobError` means the job terminated
/// unsuccessfully. The concrete `Job::Error` value is not persisted, so this
/// carries its [`Display`](std::fmt::Display) message and classification.
#[derive(Debug, Clone, Error)]
#[error("job failed ({kind:?}): {message}")]
pub struct JobError {
    /// Whether the underlying failure was classified transient (the job
    /// exhausted its retries) or permanent (dead-lettered immediately).
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
    /// An infrastructure error occurred while submitting, waiting, or reading
    /// the result.
    #[error(transparent)]
    Infra(#[from] Error),
    /// The job ran to a terminal state but did not succeed.
    #[error(transparent)]
    Job(#[from] JobError),
}

/// A handle to a submitted job.
///
/// Returned by [`JobRunner::submit`](crate::JobRunner::submit). Await it
/// directly for the typed result, or use [`join`](Self::join) /
/// [`fetch_result`](Self::fetch_result) / [`status`](Self::status) for more
/// control.
///
/// Awaiting is in-process: it relies on taquba's in-process completion
/// notification, so a handle is awaited in the same process that runs the
/// job. The *result* is durable regardless: [`fetch_result`](Self::fetch_result)
/// reads it back from object storage even after a restart.
pub struct JobHandle<J: Job> {
    id: String,
    submitter: Submitter,
    newly_submitted: bool,
    _marker: PhantomData<fn() -> J>,
}

impl<J: Job> Clone for JobHandle<J> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            submitter: self.submitter.clone(),
            newly_submitted: self.newly_submitted,
            _marker: PhantomData,
        }
    }
}

impl<J: Job> JobHandle<J> {
    pub(crate) fn new(id: String, submitter: Submitter, newly_submitted: bool) -> Self {
        Self {
            id,
            submitter,
            newly_submitted,
            _marker: PhantomData,
        }
    }

    /// The ULID taquba assigned to this job.
    ///
    /// When the job was submitted with an idempotency key that matched an
    /// existing pending job, this is the *existing* job's ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// True if the call that produced this handle freshly enqueued the
    /// job; false if the call dedup-hit against a pending submission
    /// with the same [`Job::idempotency_key`](crate::Job::idempotency_key)
    /// and a matching payload.
    ///
    /// For submissions without an `idempotency_key`, the value is
    /// always `true` (every submit produces a new job).
    ///
    /// The value reflects the *call* that returned this handle: it
    /// does not update as the job progresses. Handles produced by
    /// [`Clone`] preserve the original value.
    pub fn newly_submitted(&self) -> bool {
        self.newly_submitted
    }

    /// The job's current lifecycle status, or `None` if no record exists
    /// (never enqueued, or already reaped after completion).
    pub async fn status(&self) -> Result<Option<JobStatus>> {
        Ok(self
            .submitter
            .queue()
            .get_job(&self.id)
            .await?
            .map(|job| job.status))
    }

    /// Read the job's persisted outcome without waiting.
    ///
    /// Returns `None` when no blob has been written yet for this job ID.
    /// That covers two indistinguishable cases:
    ///
    /// 1. The job is still pending, scheduled, or in-flight (no terminal
    ///    outcome to write yet).
    /// 2. The job reached a terminal state without a handler recording one
    ///    (e.g. dead-lettered by the reaper after a lease expiry).
    ///
    /// To disambiguate, combine with [`status`](Self::status) (which queries
    /// the queue record), or use [`join_timeout`](Self::join_timeout), which
    /// reconciles the two by consulting both sources.
    ///
    /// Reads from object storage, so it works across process restarts.
    pub async fn fetch_result(&self) -> Result<Option<std::result::Result<J::Output, JobError>>> {
        match self.submitter.results().get(&self.id).await? {
            None => Ok(None),
            Some(StoredOutcome::Success { output }) => {
                let value: J::Output = rmp_serde::from_slice(&output)?;
                Ok(Some(Ok(value)))
            }
            Some(StoredOutcome::Failure { kind, message }) => Ok(Some(Err(JobError {
                kind: kind.into(),
                message,
            }))),
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
    /// Returns `Ok(None)` if the timeout elapses first. On completion,
    /// prefers the durable result blob; if the job reached a terminal state
    /// with no blob (the reaper dead-lettered it after a lease expiry), the
    /// outcome is synthesized from the queue record as a transient
    /// [`JobError`].
    ///
    /// If the queue has no record of this ID *and* no result blob exists,
    /// returns [`Error::JobNotFound`]. Under taquba's default retention
    /// (`done_retention: None`), an ack deletes the queue record outright,
    /// so a job that runs and finishes between `submit` and the start of
    /// this wait can present as "no record"; the durable blob remains the
    /// source of truth.
    pub async fn join_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<std::result::Result<J::Output, JobError>>> {
        match self
            .submitter
            .queue()
            .wait_for_completion(&self.id, timeout)
            .await?
        {
            WaitOutcome::TimedOut => Ok(None),
            // The queue record is gone before we ever observed it. Under
            // default retention, an ack deletes the record outright, so a
            // job that finished between `submit` and the start of this
            // wait reaches us via the durable blob, not the queue. Only
            // error out if the blob is also missing.
            WaitOutcome::NotFound => match self.fetch_result().await? {
                Some(outcome) => Ok(Some(outcome)),
                None => Err(Error::JobNotFound(self.id.clone())),
            },
            WaitOutcome::Completed(record) => {
                if let Some(outcome) = self.fetch_result().await? {
                    return Ok(Some(outcome));
                }
                let message = record
                    .and_then(|record| record.last_error.clone())
                    .unwrap_or_else(|| "job terminated without recording a result".to_string());
                Ok(Some(Err(JobError {
                    kind: ErrorKind::Transient,
                    message,
                })))
            }
        }
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

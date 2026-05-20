use thiserror::Error;

/// Errors returned by `taquba-jobs` infrastructure operations.
///
/// This type covers *infrastructure* failures: the queue, the object store,
/// serialization. It does **not** represent a job's own logical failure; a
/// job that runs and returns `Err` surfaces as a [`JobError`](crate::JobError)
/// from [`JobHandle`](crate::JobHandle), not as one of these variants.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// [`JobRunnerBuilder::build`](crate::JobRunnerBuilder::build) was called
    /// without a queue configured.
    #[error("a queue is required to build a JobRunner")]
    MissingQueue,

    /// [`JobRunnerBuilder::build`](crate::JobRunnerBuilder::build) was called
    /// without an object store configured.
    #[error("an object store is required to build a JobRunner")]
    MissingObjectStore,

    /// An operation on the underlying taquba queue failed.
    #[error("queue error: {0}")]
    Queue(#[from] taquba::Error),

    /// A job input, output, or stored outcome failed to serialize.
    #[error("failed to serialize job data: {0}")]
    Encode(#[from] rmp_serde::encode::Error),

    /// A job input, output, or stored outcome failed to deserialize.
    #[error("failed to deserialize job data: {0}")]
    Decode(#[from] rmp_serde::decode::Error),

    /// Reading or writing a job's result blob in object storage failed.
    #[error("object store error: {0}")]
    Store(#[from] taquba::object_store::Error),

    /// A handle was awaited for a job ID the queue has no record of.
    #[error("job `{0}` not found")]
    JobNotFound(String),

    /// A submission's [`SubmitOptions::headers`](crate::SubmitOptions::headers)
    /// included a header key reserved by `taquba-jobs` for its own use (such
    /// as the job-type routing header).
    #[error("header key `{0}` is reserved by taquba-jobs and must not be set on submission")]
    ReservedHeader(String),

    /// A re-submission used the same `idempotency_key` as a previous
    /// submission but with a different payload. The string carries the
    /// conflicting key. Returned both on the in-process race window and
    /// across restarts (the input hash is durably recorded in the user
    /// KV namespace alongside the enqueue).
    #[error("submission for idempotency key `{0}` already exists with a different payload")]
    InputMismatch(String),
}

impl Error {
    /// True if this error has no chance of succeeding on retry.
    ///
    /// Builder misconfiguration (`MissingQueue`, `MissingObjectStore`),
    /// `(De)serialization` failures, `JobNotFound`, and
    /// `ReservedHeader` are all permanent: the caller's input would
    /// have to change for the operation to succeed. `Store(_)` is
    /// conservatively treated as transient (object-store I/O can
    /// blip). `Queue(_)` delegates to [`taquba::Error::is_permanent`].
    pub fn is_permanent(&self) -> bool {
        match self {
            Self::MissingQueue
            | Self::MissingObjectStore
            | Self::Encode(_)
            | Self::Decode(_)
            | Self::JobNotFound(_)
            | Self::ReservedHeader(_)
            | Self::InputMismatch(_) => true,
            Self::Store(_) => false,
            Self::Queue(e) => e.is_permanent(),
        }
    }
}

/// Convenience alias for `Result<T, Error>` used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jobs_variants_are_permanent() {
        assert!(Error::MissingQueue.is_permanent());
        assert!(Error::MissingObjectStore.is_permanent());
        assert!(Error::JobNotFound("job-1".into()).is_permanent());
        assert!(Error::ReservedHeader("jobs.type".into()).is_permanent());
        assert!(Error::InputMismatch("idem-key".into()).is_permanent());
    }

    #[test]
    fn store_is_transient() {
        let store_err = taquba::object_store::Error::NotFound {
            path: "x".into(),
            source: "missing".into(),
        };
        assert!(!Error::Store(store_err).is_permanent());
    }

    #[test]
    fn queue_classifies_per_inner_variant() {
        assert!(Error::Queue(taquba::Error::JobNotFound("job-1".into())).is_permanent());
        assert!(Error::Queue(taquba::Error::InvalidState).is_permanent());
    }
}

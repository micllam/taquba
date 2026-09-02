use thiserror::Error;

/// Errors returned by `taquba-jobs` infrastructure operations.
///
/// This type covers infrastructure failures: the queue, the workflow
/// runtime, the object store, serialization. A job's own logical failure
/// surfaces as a [`JobError`](crate::jobs::JobError) from
/// [`JobHandle`](crate::jobs::JobHandle).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// An operation on the underlying taquba queue failed.
    #[error("queue error: {0}")]
    Queue(#[from] taquba::Error),

    /// An operation on the underlying workflow runtime or memo store
    /// failed.
    #[error("workflow error: {0}")]
    Workflow(#[from] crate::Error),

    /// A job input, output or outcome record failed to serialize.
    #[error("failed to serialize job data: {0}")]
    Encode(#[from] rmp_serde::encode::Error),

    /// A job input, output or outcome record failed to deserialize.
    #[error("failed to deserialize job data: {0}")]
    Decode(#[from] rmp_serde::decode::Error),

    /// A handle was awaited for a job the runtime has no record of.
    #[error("job `{0}` not found")]
    JobNotFound(String),

    /// A submission's [`SubmitOptions::headers`](crate::jobs::SubmitOptions::headers)
    /// included a header key reserved by `taquba-jobs` (the job-type
    /// routing header).
    #[error("header key `{0}` is reserved by taquba-jobs and must not be set on submission")]
    ReservedHeader(String),

    /// A re-submission used the same `idempotency_key` as a previous
    /// submission with a different payload. The string is the key.
    #[error("submission for idempotency key `{0}` already exists with a different payload")]
    InputMismatch(String),
}

impl Error {
    /// True if this error has no chance of succeeding on retry.
    pub fn is_permanent(&self) -> bool {
        match self {
            Self::Encode(_)
            | Self::Decode(_)
            | Self::JobNotFound(_)
            | Self::ReservedHeader(_)
            | Self::InputMismatch(_) => true,
            Self::Queue(e) => e.is_permanent(),
            Self::Workflow(e) => e.is_permanent(),
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
        assert!(Error::JobNotFound("job-1".into()).is_permanent());
        assert!(Error::ReservedHeader("jobs.type".into()).is_permanent());
        assert!(Error::InputMismatch("idem-key".into()).is_permanent());
    }

    #[test]
    fn wrapped_errors_classify_per_inner_variant() {
        assert!(Error::Queue(taquba::Error::InvalidState).is_permanent());
        let store_err = taquba::object_store::Error::NotFound {
            path: "x".into(),
            source: "missing".into(),
        };
        assert!(!Error::Workflow(crate::Error::Store(store_err)).is_permanent());
    }
}

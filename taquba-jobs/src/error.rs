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
}

/// Convenience alias for `Result<T, Error>` used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

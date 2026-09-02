use thiserror::Error;

/// Errors returned by the bulk runner and its I/O adapters.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// An error from the underlying [`crate`] runtime (submission,
    /// status, cancellation).
    #[error(transparent)]
    Workflow(#[from] crate::Error),

    /// Reading an input source or writing an output sink failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Parsing or serializing JSON for an input/output line failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Encoding an input item to the queue's internal payload format failed.
    #[error("payload encode error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),

    /// A batch id was not 1 to 128 bytes of `[A-Za-z0-9_-]`.
    #[error("invalid batch id `{0}`: must be 1 to 128 bytes of `[A-Za-z0-9_-]`")]
    InvalidBatchId(String),

    /// [`BulkBuilder::headers`](crate::bulk::BulkBuilder::headers) included a
    /// key under the `bulk.` prefix, which the runner reserves.
    #[error("header key `{0}` is reserved by the bulk runner")]
    ReservedHeader(String),

    /// The run completed but the share of failed items exceeded the
    /// configured [`fail_threshold`](crate::bulk::BulkBuilder::fail_threshold).
    #[error("bulk run failed: {failed}/{total} items failed, over the {threshold:.1}% threshold")]
    FailureThresholdExceeded {
        /// Number of items that terminated failed.
        failed: usize,
        /// Total number of items submitted.
        total: usize,
        /// The configured threshold, as a percentage.
        threshold: f64,
    },
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

use thiserror::Error;

/// Errors returned by the bulk runner and its I/O adapters.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// An error from the underlying [`taquba_workflow`] runtime (submission,
    /// status, cancellation).
    #[error(transparent)]
    Workflow(#[from] taquba_workflow::Error),

    /// An error from a direct Taquba queue operation.
    #[error(transparent)]
    Queue(#[from] taquba::Error),

    /// Reading an input source or writing an output sink failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Parsing or serializing JSON for an input/output line failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Encoding an input item to the queue's internal payload format failed.
    #[error("payload encode error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),

    /// The run completed but the share of failed items exceeded the
    /// configured [`fail_threshold`](crate::BulkBuilder::fail_threshold).
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

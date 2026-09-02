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

    /// Encoding an input item or a manifest failed.
    #[error("payload encode error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),

    /// Decoding a manifest failed.
    #[error("payload decode error: {0}")]
    Decode(#[from] rmp_serde::decode::Error),

    /// Reading or writing a manifest in the object store failed.
    #[error("object store error: {0}")]
    Store(#[from] taquba::object_store::Error),

    /// Two items of one batch produced the same key.
    #[error("duplicate item key `{0}` in batch")]
    DuplicateKey(String),

    /// A run of an existing batch supplied a different item set than the
    /// batch's manifest.
    #[error("batch `{0}` exists with a different item set")]
    BatchMismatch(String),

    /// A resume named a batch with no manifest.
    #[error("batch `{0}` not found")]
    BatchNotFound(String),

    /// A run of a batch was started while a run of the same batch was
    /// active in this process.
    #[error("batch `{0}` is already running in this process")]
    BatchRunning(String),

    /// A batch id was not 1 to 128 bytes of `[A-Za-z0-9_-]`.
    #[error("invalid batch id `{0}`: must be 1 to 128 bytes of `[A-Za-z0-9_-]`")]
    InvalidBatchId(String),

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

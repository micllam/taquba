use thiserror::Error;

/// Errors returned by Taquba's public API.
#[derive(Debug, Error)]
pub enum Error {
    /// The underlying [SlateDB] storage layer reported a failure (transaction
    /// commit failed, object-store I/O error, etc.).
    ///
    /// [SlateDB]: https://github.com/slatedb/slatedb
    #[error("storage error: {0}")]
    Storage(#[from] slatedb::Error),

    /// Failed to encode a job record to MessagePack before writing it to the
    /// store. Typically indicates a payload that violates serde constraints.
    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),

    /// Failed to decode a job record read back from the store. Usually means
    /// the on-disk schema is from an incompatible Taquba version.
    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    /// A job lookup by ID found no matching record.
    #[error("job not found: {0}")]
    JobNotFound(String),

    /// An operation was issued against a job in the wrong state; for example,
    /// `ack`-ing a record that is missing its `lease_expires_at`, or
    /// `requeue_dead_job` on a record that is no longer in the dead state.
    #[error("job is not in the expected state")]
    InvalidState,

    /// A value passed to [`crate::Queue::enqueue_with_kv`] exceeded the
    /// configured maximum size for the user KV namespace. The cap is
    /// enforced at the API boundary to keep bulk payload out of the LSM
    /// tree; store large blobs in the underlying object store and put only
    /// the pointer in KV. See [`crate::MAX_KV_VALUE_SIZE`].
    #[error("kv value too large: {size} bytes (max {max})")]
    KvValueTooLarge {
        /// The value size that was rejected.
        size: usize,
        /// The configured maximum.
        max: usize,
    },
}

/// Convenience alias for `Result<T, Error>` returned throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

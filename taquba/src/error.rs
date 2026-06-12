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

    /// A settlement or lease operation found no claim under the record it
    /// was given: the lease expired and the reaper requeued the job, or
    /// the record is a stale copy from before a lease renewal rotated the
    /// claimed key. Retrying with the same record cannot succeed; a
    /// redelivered attempt settles the job instead.
    #[error("job claim is no longer held")]
    ClaimLost,

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

    /// A caller-supplied [`crate::EnqueueOptions::id_override`] failed
    /// validation. Caller-supplied ids must be 1-128 bytes of
    /// `[A-Za-z0-9_-]`; ids that violate either bound are rejected at the
    /// API boundary before any state is written.
    #[error("invalid job id `{id}`: {reason}")]
    InvalidId {
        /// The id that was rejected.
        id: String,
        /// Why it was rejected.
        reason: &'static str,
    },

    /// A caller-supplied [`crate::EnqueueOptions::id_override`] matched an
    /// existing indexed job id. Duplicate caller-supplied ids are rejected
    /// before any queue state or user KV writes are applied.
    #[error("duplicate job id `{id}`")]
    DuplicateJobId {
        /// The duplicate id that was rejected.
        id: String,
    },
}

impl Error {
    /// True if retrying the operation will not change the outcome; callers
    /// should fast-fail rather than back off.
    ///
    /// [`Self::Storage`] is conservatively treated as transient.
    /// The remaining variants are programmer / data-shape errors
    /// where retrying cannot help.
    pub fn is_permanent(&self) -> bool {
        match self {
            Self::Serialization(_)
            | Self::Deserialization(_)
            | Self::JobNotFound(_)
            | Self::InvalidState
            | Self::ClaimLost
            | Self::KvValueTooLarge { .. }
            | Self::InvalidId { .. }
            | Self::DuplicateJobId { .. } => true,
            Self::Storage(_) => false,
        }
    }
}

/// Convenience alias for `Result<T, Error>` returned throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_shape_and_state_variants_are_permanent() {
        assert!(Error::JobNotFound("job-1".into()).is_permanent());
        assert!(Error::InvalidState.is_permanent());
        assert!(Error::ClaimLost.is_permanent());
        assert!(Error::KvValueTooLarge { size: 10, max: 5 }.is_permanent());
        assert!(
            Error::InvalidId {
                id: "bad:id".into(),
                reason: "invalid char",
            }
            .is_permanent()
        );
        assert!(Error::DuplicateJobId { id: "job-1".into() }.is_permanent());
    }
}

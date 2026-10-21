use thiserror::Error;

/// Errors returned by Taquba's public API.
#[derive(Debug, Error)]
pub enum Error {
    /// The underlying [SlateDB] storage layer reported a failure, such as a
    /// failed transaction commit or an object-store I/O error.
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

    /// An operation was issued against a job in the wrong state, for example
    /// `requeue_dead_job` on a record that is not in the dead state. Settlement
    /// cannot report this: it takes a `Claim`, so a record read through
    /// `get_job` or `list_jobs` cannot be settled at all.
    #[error("job is not in the expected state")]
    InvalidState,

    /// A settlement or lease operation found no claim under the record it was
    /// given: the lease expired and the reaper requeued the job, the job was
    /// settled already or the store closed. Retrying with the same record
    /// cannot succeed, and a redelivered attempt settles the job. A record
    /// taken before a lease renewal is not stale, because the renewal leaves
    /// the claim id unchanged.
    #[error("job claim is no longer held")]
    ClaimLost,

    /// A lease renewal was refused because cancellation of the job was
    /// requested. The claim is still held and settleable. With renewal refused,
    /// the lease expires unless the delivery settles first.
    #[error("job cancellation has been requested")]
    CancelRequested,

    /// A [`crate::SettlementEffects`] value includes the same key in both
    /// `kv_writes` and `kv_deletes`. The effects are rejected before the
    /// settlement transaction begins.
    #[error("kv key {key:?} is both written and deleted by one settlement")]
    ConflictingKvEffect {
        /// The key named on both sides.
        key: Vec<u8>,
    },

    /// A value written to the user KV namespace exceeded the configured maximum
    /// size. The cap is enforced at the API boundary to keep bulk payload out
    /// of the LSM tree. Store large blobs in the underlying object store and
    /// put only the pointer in KV. See [`crate::MAX_KV_VALUE_SIZE`].
    #[error("kv value too large: {size} bytes (max {max})")]
    KvValueTooLarge {
        /// The value size that was rejected.
        size: usize,
        /// The configured maximum.
        max: usize,
    },

    /// A caller-supplied [`crate::EnqueueOptions::id_override`] failed
    /// validation. Caller-supplied ids must be 1-128 bytes of `[A-Za-z0-9_-]`.
    /// An id that violates either bound is rejected at the API boundary before
    /// any state is written.
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

    /// A [`crate::EnqueueOptions::signal_key`] is empty or was supplied without
    /// a `run_at`. The enqueue is rejected before any state is written.
    #[error("invalid signal key `{key}`: {reason}")]
    InvalidSignalKey {
        /// The rejected key.
        key: String,
        /// Why it was rejected.
        reason: &'static str,
    },

    /// An enqueue with a [`crate::EnqueueOptions::signal_key`] found a
    /// scheduled job waiting at the key. One waiter per key: the enqueue is
    /// rejected without a write, and the effects of the enqueue do not apply.
    #[error("a job is waiting at signal key `{key}`")]
    SignalWaiterExists {
        /// The key with a live waiter.
        key: String,
    },

    /// A queue name failed validation. Queue names are limited to
    /// [`crate::MAX_QUEUE_NAME_LEN`] bytes by the key encoding and are rejected
    /// at the API boundary before any state is written.
    #[error("invalid queue name `{queue}`: {reason}")]
    InvalidQueueName {
        /// The rejected queue name.
        queue: String,
        /// Why it was rejected.
        reason: &'static str,
    },

    /// The payload object store reported a failure while writing, reading or
    /// deleting an offloaded payload. See
    /// [`crate::OpenOptions::payload_offload_threshold`].
    #[error("payload store error: {0}")]
    PayloadStore(#[from] slatedb::object_store::Error),

    /// An offloaded payload object was absent when a claim or a job read tried
    /// to fetch it. On the writing [`crate::Queue`] the record still exists but
    /// its payload cannot be recovered, which indicates external deletion of
    /// the payload object. Through a [`crate::QueueReader`] the condition can
    /// be transient: a job removal deletes the payload object after its record,
    /// so a reader whose lagging view still contains the record can find the
    /// object gone until the view advances past the removal.
    #[error("offloaded payload missing for job `{id}`")]
    PayloadMissing {
        /// The job whose payload object was absent.
        id: String,
    },

    /// [`crate::QueueReader::open`] found no store at the path: the open failed
    /// on the missing manifest and the path contains zero objects, so no writer
    /// ever created the store there. A store whose path contains objects but
    /// whose manifest cannot be read reports [`Self::Storage`].
    #[error("no store exists at `{path}`")]
    StoreNotInitialized {
        /// The empty store path.
        path: String,
    },

    /// [`crate::open_url`] could not open a store from the URL.
    #[error("invalid store URL `{url}`: {source}")]
    InvalidStoreUrl {
        /// The URL as passed.
        url: String,
        /// Why the URL was rejected.
        #[source]
        source: StoreUrlError,
    },

    /// The inbox object store reported a failure while listing, reading,
    /// writing or deleting an entry. See [`crate::OpenOptions::inbox`].
    #[error("inbox store error: {0}")]
    InboxStore(#[source] slatedb::object_store::Error),

    /// An inbox kind name is outside `[a-z][a-z0-9._-]*` or over 64 bytes.
    #[error("invalid inbox kind `{kind}`: {reason}")]
    InvalidInboxKind {
        /// The rejected kind.
        kind: String,
        /// Why it was rejected.
        reason: &'static str,
    },

    /// [`crate::Queue::register_inbox_handler`] named a core kind or a kind
    /// with a live registration, or
    /// [`crate::Queue::register_inbox_handler_for`] named a value with a live
    /// registration or a field that differs from the field of the kind.
    #[error("inbox kind `{kind}` is taken{}", value.as_deref().map(|v| format!(" for `{v}`")).unwrap_or_default())]
    InboxKindTaken {
        /// The kind.
        kind: String,
        /// The value of the scope, `None` for a registration of the whole kind.
        value: Option<String>,
    },

    /// [`crate::Queue::register_inbox_handler`] was called on a queue opened
    /// without [`crate::OpenOptions::inbox`].
    #[error("the queue was opened without an inbox")]
    InboxNotConfigured,

    /// An [`crate::InboxHandler`] reported [`crate::Applied::Committed`] for an
    /// entry without a record. The pass stops at the entry.
    #[error("inbox entry `{id}` was reported committed without a record")]
    InboxRecordMissing {
        /// The id of the entry.
        id: String,
    },

    /// A transaction would write the record of the inbox entry `id`, and a
    /// record exists. The transaction is aborted, and the pass treats the entry
    /// as applied.
    #[error("inbox entry `{id}` is already applied")]
    InboxAlreadyApplied {
        /// The id of the entry.
        id: String,
    },

    /// An [`crate::InboxHandler`] failed. The pass stops at the entry, and the
    /// next pass applies it again.
    #[error("inbox handler of kind `{kind}` failed: {source}")]
    InboxHandler {
        /// The kind of the entry.
        kind: String,
        /// The error of the handler.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// [`crate::InboxSender`] could not encode an entry as JSON.
    #[error("inbox entry encoding error: {0}")]
    InboxEncoding(#[source] serde_json::Error),
}

/// The cause of an [`Error::InvalidStoreUrl`].
#[derive(Debug, Error)]
pub enum StoreUrlError {
    /// The URL does not parse.
    #[error("{0}")]
    Url(#[source] url::ParseError),
    /// The object store rejected the URL: its scheme is unknown or requires a
    /// backend feature that is not enabled, or the provider rejected an option.
    #[error("{0}")]
    Store(#[source] slatedb::object_store::Error),
}

impl Error {
    /// True if retrying the operation will not change the outcome, and a
    /// backoff before a retry does not help.
    ///
    /// [`Self::Storage`] is conservatively treated as transient, as is
    /// [`Self::StoreNotInitialized`]: a health check racing the first
    /// deployment observes it until the writer creates the store, and a later
    /// retry can succeed. [`Self::InboxHandler`] is transient, because the next
    /// pass applies the entry again. The remaining variants report incorrect
    /// usage or malformed data, which retrying cannot change.
    pub fn is_permanent(&self) -> bool {
        match self {
            Self::Serialization(_)
            | Self::Deserialization(_)
            | Self::JobNotFound(_)
            | Self::InvalidState
            | Self::ClaimLost
            | Self::CancelRequested
            | Self::KvValueTooLarge { .. }
            | Self::ConflictingKvEffect { .. }
            | Self::InvalidId { .. }
            | Self::DuplicateJobId { .. }
            | Self::InvalidSignalKey { .. }
            | Self::SignalWaiterExists { .. }
            | Self::InvalidQueueName { .. }
            | Self::PayloadMissing { .. }
            | Self::InvalidStoreUrl { .. }
            | Self::InvalidInboxKind { .. }
            | Self::InboxKindTaken { .. }
            | Self::InboxNotConfigured
            | Self::InboxRecordMissing { .. }
            | Self::InboxAlreadyApplied { .. }
            | Self::InboxEncoding(_) => true,
            Self::Storage(_)
            | Self::PayloadStore(_)
            | Self::StoreNotInitialized { .. }
            | Self::InboxStore(_)
            | Self::InboxHandler { .. } => false,
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
        assert!(Error::CancelRequested.is_permanent());
        assert!(Error::KvValueTooLarge { size: 10, max: 5 }.is_permanent());
        assert!(Error::ConflictingKvEffect { key: b"k".to_vec() }.is_permanent());
        assert!(
            Error::InvalidId {
                id: "bad:id".into(),
                reason: "invalid char",
            }
            .is_permanent()
        );
        assert!(Error::DuplicateJobId { id: "job-1".into() }.is_permanent());
        assert!(
            Error::InvalidSignalKey {
                key: String::new(),
                reason: "empty",
            }
            .is_permanent()
        );
        assert!(Error::SignalWaiterExists { key: "k".into() }.is_permanent());
        assert!(
            Error::InvalidStoreUrl {
                url: "x".into(),
                source: StoreUrlError::Url(url::ParseError::EmptyHost),
            }
            .is_permanent()
        );
    }
}

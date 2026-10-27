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

    /// A caller-supplied [`crate::EnqueueOptions::id_override`] or inbox entry
    /// id is outside its form. It is rejected before any state is written.
    #[error("invalid id `{id}`: {reason}")]
    InvalidId {
        /// The id that was rejected.
        id: String,
        /// The rule that the id breaks.
        reason: FormError,
    },

    /// A caller-supplied [`crate::EnqueueOptions::id_override`] matched an
    /// existing indexed job id. Duplicate caller-supplied ids are rejected
    /// before any queue state or user KV writes are applied.
    #[error("duplicate job id `{id}`")]
    DuplicateJobId {
        /// The duplicate id that was rejected.
        id: String,
    },

    /// An enqueue set [`crate::EnqueueOptions::signal_wait`] without a
    /// `run_at`. The enqueue is rejected before any state is written.
    #[error("a signal wait requires run_at")]
    SignalWaitWithoutRunAt,

    /// A queue name is outside the form of [`crate::QueueName`]. It is rejected
    /// at the API boundary before any state is written.
    #[error("invalid queue name `{queue}`: {reason}")]
    InvalidQueueName {
        /// The rejected queue name.
        queue: String,
        /// The rule that the name breaks.
        reason: FormError,
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

    /// A transaction would write the record of the inbox entry `id`, and a
    /// record exists. The transaction is aborted, and the pass treats the entry
    /// as applied.
    #[error("inbox entry `{id}` is already applied")]
    InboxAlreadyApplied {
        /// The id of the entry.
        id: String,
    },

    /// The custom path of the inbox, in the object store of the queue, is the
    /// path of the queue or a path inside it.
    #[error("inbox path `{path}` overlaps the queue's path `{queue_path}`")]
    InboxPathWithinQueue {
        /// The path of the inbox.
        path: String,
        /// The path of the queue.
        queue_path: String,
    },
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

/// The rule of a name or an id that a value breaks: the reason of
/// [`Error::InvalidQueueName`] and [`Error::InvalidId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FormError {
    /// The value is empty.
    #[error("must not be empty")]
    Empty,
    /// The value is longer than `max` bytes.
    #[error("exceeds the maximum length of {max} bytes")]
    TooLong {
        /// The maximum length in bytes.
        max: usize,
    },
    /// The first character is outside the characters that may start the value.
    #[error("starts with {character:?}, and must start with {allowed}")]
    InvalidStart {
        /// The first character.
        character: char,
        /// The characters that may start the value.
        allowed: &'static str,
    },
    /// A character is outside the characters of the form.
    #[error("contains {character:?}, and may contain only {allowed}")]
    InvalidCharacter {
        /// The first character outside the form.
        character: char,
        /// The characters of the form.
        allowed: &'static str,
    },
}

/// The form of a name or an id: a maximum length in bytes, the characters that
/// may start it and the characters it may contain.
pub(crate) struct Form {
    pub(crate) max: usize,
    pub(crate) start: Option<Characters>,
    pub(crate) characters: Characters,
}

/// A set of characters: its test and its description in an error message.
pub(crate) struct Characters {
    pub(crate) test: fn(char) -> bool,
    pub(crate) allowed: &'static str,
}

impl Form {
    /// The first rule of the form that `value` breaks, in the order of the
    /// variants of [`FormError`].
    pub(crate) fn check(&self, value: &str) -> std::result::Result<(), FormError> {
        let Some(first) = value.chars().next() else {
            return Err(FormError::Empty);
        };
        if value.len() > self.max {
            return Err(FormError::TooLong { max: self.max });
        }
        if let Some(start) = &self.start
            && !(start.test)(first)
        {
            return Err(FormError::InvalidStart {
                character: first,
                allowed: start.allowed,
            });
        }
        match value.chars().find(|&c| !(self.characters.test)(c)) {
            Some(character) => Err(FormError::InvalidCharacter {
                character,
                allowed: self.characters.allowed,
            }),
            None => Ok(()),
        }
    }
}

impl Error {
    /// True if retrying the operation will not change the outcome, and a
    /// backoff before a retry does not help.
    ///
    /// [`Self::Storage`] is conservatively treated as transient, as is
    /// [`Self::StoreNotInitialized`]: a health check racing the first
    /// deployment observes it until the writer creates the store, and a later
    /// retry can succeed. The remaining variants report incorrect usage or
    /// malformed data, which retrying cannot change.
    pub fn is_permanent(&self) -> bool {
        match self {
            Self::Deserialization(_)
            | Self::JobNotFound(_)
            | Self::InvalidState
            | Self::ClaimLost
            | Self::CancelRequested
            | Self::KvValueTooLarge { .. }
            | Self::ConflictingKvEffect { .. }
            | Self::InvalidId { .. }
            | Self::DuplicateJobId { .. }
            | Self::SignalWaitWithoutRunAt
            | Self::InvalidQueueName { .. }
            | Self::PayloadMissing { .. }
            | Self::InvalidStoreUrl { .. }
            | Self::InboxAlreadyApplied { .. }
            | Self::InboxPathWithinQueue { .. } => true,
            Self::Storage(_)
            | Self::PayloadStore(_)
            | Self::StoreNotInitialized { .. }
            | Self::InboxStore(_) => false,
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
                reason: FormError::Empty,
            }
            .is_permanent()
        );
        assert!(Error::DuplicateJobId { id: "job-1".into() }.is_permanent());
        assert!(Error::SignalWaitWithoutRunAt.is_permanent());
        assert!(
            Error::InvalidStoreUrl {
                url: "x".into(),
                source: StoreUrlError::Url(url::ParseError::EmptyHost),
            }
            .is_permanent()
        );
    }
}

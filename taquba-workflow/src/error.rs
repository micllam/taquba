use thiserror::Error;

/// Errors returned by the runtime's submission and worker paths.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A step job is missing the [`crate::HEADER_RUN_ID`] header.
    /// Permanent: a misconfigured job will not become valid on retry.
    #[error("step job is missing header `{0}`")]
    MissingHeader(&'static str),

    /// A step job's [`crate::HEADER_STEP`] header is not a valid `u32`.
    /// Permanent: header value won't change across retries.
    #[error("step job has invalid `{header}` header `{value}`")]
    InvalidStepHeader {
        /// Header name.
        header: &'static str,
        /// Offending value.
        value: String,
    },

    /// A submission included a user header starting with the reserved
    /// `workflow.*` prefix. The runtime owns that prefix; submitters must use
    /// any other key.
    #[error("submission header `{0}` uses the reserved `workflow.*` prefix")]
    ReservedHeaderInSubmit(String),

    /// A caller-supplied [`crate::RunSpec::run_id`] is empty, longer than
    /// [`crate::MAX_RUN_ID_LEN`] bytes or contains a character outside
    /// `[A-Za-z0-9_-]`. The run id becomes an object-store path segment
    /// and a key segment in the queue's key-value namespace, so it is
    /// restricted to the same character set as a Taquba job id.
    #[error("invalid run id `{run_id}`: {reason}")]
    InvalidRunId {
        /// The rejected run id.
        run_id: String,
        /// Which rule the run id broke.
        reason: &'static str,
    },

    /// A re-submission of `run_id` carried `spec.input` bytes that differ
    /// from the original submission's: the run is active, or it is a
    /// typed job whose outcome record is retained. Reusing a `run_id`
    /// with new input is treated as a programmer error: pick a fresh
    /// `run_id` for a new run.
    #[error("run `{0}` exists with a different input; pick a fresh run_id")]
    InputMismatch(String),

    /// A run's durable record exists without the current-step pointer
    /// written beside it. The two are written and deleted in one
    /// transaction, so this reports a store the runtime did not write.
    #[error("run `{0}` has a run record but no current-step pointer")]
    InconsistentRunState(String),

    /// A caller KV key passed via [`crate::RunSpec::kv_writes`] or staged
    /// through an [`crate::EffectsHandle`] starts with the reserved
    /// `workflow/` prefix. The runtime owns that prefix; callers must use
    /// any other key.
    #[error("kv key `{0}` uses the reserved `workflow/` prefix")]
    ReservedKvKey(String),

    /// A key was staged through an [`crate::EffectsHandle`] for both a
    /// write and a delete within one step. The combination has no defined
    /// order in the settlement transaction and is rejected when the
    /// second operation is staged.
    #[error("kv key `{0}` is staged for both a write and a delete")]
    ConflictingKvEffect(String),

    /// An effect was staged through an [`crate::EffectsHandle`] or
    /// [`crate::TerminalEffects`] clone after its delivery returned.
    /// Effects are collected when the runner or hook returns; an effect
    /// staged after that point cannot join the settlement.
    #[error("the effects handle is sealed; its delivery has returned")]
    EffectsSealed,

    /// Underlying error from a Taquba queue operation.
    #[error(transparent)]
    Queue(#[from] taquba::Error),

    /// Reading or writing a blob in object storage failed.
    #[error("object store error: {0}")]
    Store(#[from] taquba::object_store::Error),

    /// Serializing a value for workflow storage failed.
    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),

    /// Deserializing a stored value, a typed input or a typed output
    /// failed.
    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    /// Reading a bulk input source or writing an output sink failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Parsing or serializing JSON for a bulk input or output line failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// A [`jobs::JobHandle`](crate::jobs::JobHandle) was awaited for a
    /// job the runtime has no record of.
    #[error("job `{0}` not found")]
    JobNotFound(String),

    /// Two items of one bulk batch produced the same key.
    #[error("duplicate item key `{0}` in batch")]
    DuplicateItemKey(String),

    /// A run of an existing bulk batch supplied a different item set than
    /// the batch's manifest.
    #[error("batch `{0}` exists with a different item set")]
    BatchMismatch(String),

    /// A bulk batch operation named a batch with no manifest.
    #[error("batch `{0}` not found")]
    BatchNotFound(String),

    /// A run of a bulk batch was started while a run of the same batch
    /// was active in this process.
    #[error("batch `{0}` is already running in this process")]
    BatchRunning(String),

    /// A bulk batch id was not 1 to [`crate::MAX_RUN_ID_LEN`] bytes of
    /// `[A-Za-z0-9_-]`.
    #[error("invalid batch id `{0}`: must be 1 to 128 bytes of `[A-Za-z0-9_-]`")]
    InvalidBatchId(String),

    /// A bulk batch run completed but the share of failed items exceeded
    /// the configured
    /// [`fail_threshold`](crate::bulk::BulkBuilder::fail_threshold).
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

impl Error {
    /// True if retrying the operation will not change the outcome; callers
    /// should fast-fail (e.g. dead-letter a step, mark a submission as
    /// failed) rather than back off and try again.
    ///
    /// [`Self::Queue`] delegates to [`taquba::Error::is_permanent`].
    pub fn is_permanent(&self) -> bool {
        match self {
            Self::MissingHeader(_)
            | Self::InvalidStepHeader { .. }
            | Self::ReservedHeaderInSubmit(_)
            | Self::InvalidRunId { .. }
            | Self::InputMismatch(_)
            | Self::InconsistentRunState(_)
            | Self::ReservedKvKey(_)
            | Self::ConflictingKvEffect(_)
            | Self::EffectsSealed
            | Self::Serialization(_)
            | Self::Deserialization(_)
            | Self::Json(_)
            | Self::JobNotFound(_)
            | Self::DuplicateItemKey(_)
            | Self::BatchMismatch(_)
            | Self::BatchNotFound(_)
            | Self::InvalidBatchId(_)
            | Self::FailureThresholdExceeded { .. } => true,
            Self::Queue(e) => e.is_permanent(),
            Self::Store(_) | Self::Io(_) | Self::BatchRunning(_) => false,
        }
    }
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    struct BadSerialize;

    impl serde::Serialize for BadSerialize {
        fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("serialization failed"))
        }
    }

    #[test]
    fn is_permanent_classifies_every_variant() {
        let store_err = taquba::object_store::Error::NotFound {
            path: "x".into(),
            source: "missing".into(),
        };
        for (error, permanent) in [
            (Error::MissingHeader("workflow.run_id"), true),
            (
                Error::InvalidStepHeader {
                    header: "workflow.step",
                    value: "not-a-u32".into(),
                },
                true,
            ),
            (Error::ReservedHeaderInSubmit("workflow.foo".into()), true),
            (
                Error::InvalidRunId {
                    run_id: String::new(),
                    reason: "run id must not be empty",
                },
                true,
            ),
            (Error::InputMismatch("run-1".into()), true),
            (Error::InconsistentRunState("run-1".into()), true),
            (Error::ReservedKvKey("workflow/x".into()), true),
            (Error::ConflictingKvEffect("k".into()), true),
            (Error::EffectsSealed, true),
            (
                Error::Queue(taquba::Error::JobNotFound("job-1".into())),
                true,
            ),
            (Error::Queue(taquba::Error::InvalidState), true),
            (
                Error::Queue(taquba::Error::KvValueTooLarge { size: 10, max: 5 }),
                true,
            ),
            (
                Error::Queue(taquba::Error::StoreNotInitialized { path: "x".into() }),
                false,
            ),
            (Error::Store(store_err), false),
            (
                Error::Serialization(rmp_serde::to_vec_named(&BadSerialize).unwrap_err()),
                true,
            ),
            (
                Error::Deserialization(rmp_serde::from_slice::<u32>(b"").unwrap_err()),
                true,
            ),
            (Error::Io(std::io::Error::other("disk")), false),
            (
                Error::Json(serde_json::from_str::<u32>("x").unwrap_err()),
                true,
            ),
            (Error::JobNotFound("job-1".into()), true),
            (Error::DuplicateItemKey("k".into()), true),
            (Error::BatchMismatch("b".into()), true),
            (Error::BatchNotFound("b".into()), true),
            (Error::BatchRunning("b".into()), false),
            (Error::InvalidBatchId("a/b".into()), true),
            (
                Error::FailureThresholdExceeded {
                    failed: 1,
                    total: 2,
                    threshold: 10.0,
                },
                true,
            ),
        ] {
            assert_eq!(error.is_permanent(), permanent, "{error}");
        }
    }
}

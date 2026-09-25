use thiserror::Error;

use crate::keys::RunId;

/// Errors returned by the runtime's submission and worker paths.
#[derive(Debug, Error)]
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

    /// A run id or group id is empty, longer than
    /// [`crate::MAX_RUN_ID_LEN`] bytes or contains a character outside
    /// `[A-Za-z0-9_-]`. See [`crate::RunId`].
    #[error("invalid run id `{run_id}`: {reason}")]
    InvalidRunId {
        /// The rejected run id.
        run_id: String,
        /// Which rule the run id broke.
        reason: &'static str,
    },

    /// A re-submission of `run_id` carried `spec.input` bytes that differ
    /// from the original submission's: the run is active, or it is a
    /// typed job whose run result record is retained. Reusing a `run_id`
    /// with new input is treated as a programmer error: pick a fresh
    /// `run_id` for a new run.
    #[error("run `{0}` exists with a different input; pick a fresh run_id")]
    InputMismatch(RunId),

    /// The durable records of a run disagree with one another. The runtime
    /// writes and deletes them together, so the error reports a store the
    /// runtime did not write.
    #[error("run `{0}` has inconsistent durable state")]
    InconsistentRunState(RunId),

    /// A caller KV key passed via [`crate::RunSpec::effects`] or staged through
    /// an [`crate::EffectsHandle`] starts with the reserved `workflow/` prefix.
    /// The runtime owns that prefix. A caller must use another key.
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

    /// A wait named a run the runtime has no record of: never
    /// submitted, or terminated with no record retained.
    #[error("run `{0}` not found")]
    RunNotFound(RunId),

    /// A group operation waited on a member of the manifest that was
    /// not submitted; [`RunGroup::resume`](crate::RunGroup::resume)
    /// submits it.
    #[error("member `{key}` of group `{group_id}` was not submitted")]
    MemberNotSubmitted {
        /// The group id.
        group_id: RunId,
        /// The member's key.
        key: String,
    },

    /// Two members of one group have the same key.
    #[error("duplicate member key `{0}` in group")]
    DuplicateMemberKey(String),

    /// A submission to an existing group supplied a different member
    /// set than the group's manifest.
    #[error("group `{0}` exists with a different member set")]
    GroupMismatch(RunId),

    /// A group operation named a group with no manifest.
    #[error("group `{0}` not found")]
    GroupNotFound(RunId),
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
            | Self::RunNotFound(_)
            | Self::MemberNotSubmitted { .. }
            | Self::DuplicateMemberKey(_)
            | Self::GroupMismatch(_)
            | Self::GroupNotFound(_) => true,
            Self::Queue(e) => e.is_permanent(),
            Self::Store(_) => false,
        }
    }
}

/// The worker error reporting `err` from a step's delivery: a
/// [`taquba::PermanentFailure`] for a permanent error, which
/// dead-letters the step, and a retrying error otherwise.
pub(crate) fn worker_error(err: impl Into<Error>) -> taquba::WorkerError {
    crate::runner::StepError::from(err.into()).into_worker_error()
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::rid;

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
            (Error::InputMismatch(rid("run-1")), true),
            (Error::InconsistentRunState(rid("run-1")), true),
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
            (Error::DuplicateMemberKey("k".into()), true),
            (Error::GroupMismatch(rid("b")), true),
            (Error::GroupNotFound(rid("b")), true),
            (Error::RunNotFound(rid("run-1")), true),
            (
                Error::MemberNotSubmitted {
                    group_id: rid("b"),
                    key: "k".into(),
                },
                true,
            ),
        ] {
            assert_eq!(error.is_permanent(), permanent, "{error}");
        }
    }
}

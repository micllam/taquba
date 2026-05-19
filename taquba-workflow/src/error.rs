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

    /// A re-submission of an active `run_id` carried `spec.input` bytes
    /// that differ from the original submission's. Reusing a `run_id`
    /// with new input is treated as a programmer error: pick a fresh
    /// `run_id` for a new run, or wait for the active one to terminate.
    #[error("run `{0}` is active with a different input; pick a fresh run_id")]
    InputMismatch(String),

    /// Underlying error from a Taquba queue operation.
    #[error(transparent)]
    Queue(#[from] taquba::Error),
}

impl Error {
    /// True if retrying the operation will not change the outcome; callers
    /// should fast-fail (e.g. dead-letter a step, mark a submission as
    /// failed) rather than back off and try again.
    ///
    /// For [`Self::Queue`], classification is decided locally by
    /// pattern-matching on [`taquba::Error`]; [`taquba::Error::Storage`]
    /// (object-store I/O, transaction conflicts) is treated as transient.
    pub fn is_permanent(&self) -> bool {
        match self {
            Self::MissingHeader(_)
            | Self::InvalidStepHeader { .. }
            | Self::ReservedHeaderInSubmit(_)
            | Self::InputMismatch(_) => true,
            Self::Queue(e) => match e {
                taquba::Error::Serialization(_)
                | taquba::Error::Deserialization(_)
                | taquba::Error::JobNotFound(_)
                | taquba::Error::InvalidState
                | taquba::Error::KvValueTooLarge { .. } => true,
                taquba::Error::Storage(_) => false,
            },
        }
    }
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_variants_are_permanent() {
        assert!(Error::MissingHeader("workflow.run_id").is_permanent());
        assert!(
            Error::InvalidStepHeader {
                header: "workflow.step",
                value: "not-a-u32".into(),
            }
            .is_permanent()
        );
        assert!(Error::ReservedHeaderInSubmit("workflow.foo".into()).is_permanent());
        assert!(Error::InputMismatch("run-1".into()).is_permanent());
    }

    #[test]
    fn queue_classifies_per_inner_variant() {
        assert!(Error::Queue(taquba::Error::JobNotFound("job-1".into())).is_permanent());
        assert!(Error::Queue(taquba::Error::InvalidState).is_permanent());
        assert!(Error::Queue(taquba::Error::KvValueTooLarge { size: 10, max: 5 }).is_permanent());
    }
}

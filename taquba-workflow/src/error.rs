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

    /// Underlying error from a Taquba queue operation.
    #[error(transparent)]
    Queue(#[from] taquba::Error),
}

impl Error {
    /// True if this error should dead-letter the step rather than retry.
    pub(crate) fn is_permanent(&self) -> bool {
        matches!(
            self,
            Error::MissingHeader(_) | Error::InvalidStepHeader { .. }
        )
    }
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

//! The durable outcome record of a job, stored in the run-scoped memo of
//! the job's workflow run under a key reserved by this crate.

use crate::Memo;
use serde::{Deserialize, Serialize};

use crate::jobs::error::Result;
use crate::jobs::job::ErrorKind;

/// Run-memo key of the outcome record. Handlers receive the step-scoped
/// memo only, so no handler key can collide with it.
pub(crate) const OUTCOME_KEY: &str = "taquba_jobs.outcome";

/// The persisted terminal outcome of one job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OutcomeRecord {
    /// SHA-256 of the serialized input, checked by an idempotent
    /// re-submission after completion.
    pub(crate) input_hash: [u8; 32],
    pub(crate) outcome: StoredOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum StoredOutcome {
    Success {
        output: Vec<u8>,
    },
    Failure {
        kind: StoredErrorKind,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum StoredErrorKind {
    Transient,
    Permanent,
}

impl From<ErrorKind> for StoredErrorKind {
    fn from(kind: ErrorKind) -> Self {
        match kind {
            ErrorKind::Transient => Self::Transient,
            ErrorKind::Permanent => Self::Permanent,
        }
    }
}

impl From<StoredErrorKind> for ErrorKind {
    fn from(kind: StoredErrorKind) -> Self {
        match kind {
            StoredErrorKind::Transient => Self::Transient,
            StoredErrorKind::Permanent => Self::Permanent,
        }
    }
}

pub(crate) fn hash_input(input: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(input).into()
}

pub(crate) async fn read_outcome(run_memo: &Memo) -> Result<Option<OutcomeRecord>> {
    match run_memo.get(OUTCOME_KEY).await? {
        None => Ok(None),
        Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
    }
}

pub(crate) async fn write_outcome(run_memo: &Memo, record: &OutcomeRecord) -> Result<()> {
    let bytes = rmp_serde::to_vec_named(record)?;
    run_memo.put(OUTCOME_KEY, &bytes).await?;
    Ok(())
}

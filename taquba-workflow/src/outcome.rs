//! The durable outcome record of a typed single-step run, stored in the
//! run-scoped memo under a key reserved by this crate. Shared by the
//! [`jobs`](crate::jobs) and [`bulk`](crate::bulk) modules.

use std::future::Future;

use serde::{Deserialize, Serialize};

use crate::memo::Memo;
use crate::{Result, Step, StepError, StepErrorKind, StepOutcome};

/// Run-memo key of the outcome record. Runners receive the step-scoped
/// memo only, so no caller key can collide with it.
pub(crate) const OUTCOME_KEY: &str = "workflow.outcome";

/// The persisted terminal outcome of one typed single-step run.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum StoredErrorKind {
    Transient,
    Permanent,
}

impl From<StepErrorKind> for StoredErrorKind {
    fn from(kind: StepErrorKind) -> Self {
        match kind {
            StepErrorKind::Transient => Self::Transient,
            StepErrorKind::Permanent => Self::Permanent,
        }
    }
}

/// Run the typed part of a single-step run and record its outcome: on
/// `Ok(bytes)` the success record is written before the step succeeds with
/// `bytes`; on `Err` a failure record is written when the error is
/// permanent or the attempt is the step's last, and the error is returned.
/// A failed success-record write is a transient error, so the step
/// retries.
pub(crate) async fn run_recorded<F>(
    step: &Step,
    produce: F,
) -> std::result::Result<StepOutcome, StepError>
where
    F: Future<Output = std::result::Result<Vec<u8>, StepError>>,
{
    let input_hash = hash_input(&step.payload);
    match produce.await {
        Ok(bytes) => {
            let record = OutcomeRecord {
                input_hash,
                outcome: StoredOutcome::Success {
                    output: bytes.clone(),
                },
            };
            write_outcome(&step.run_memo, &record)
                .await
                .map_err(|err| StepError::transient(err.to_string()))?;
            Ok(StepOutcome::Succeed { result: bytes })
        }
        Err(err) => {
            let exhausted = step.attempts >= step.max_attempts;
            if matches!(err.kind, StepErrorKind::Permanent) || exhausted {
                let record = OutcomeRecord {
                    input_hash,
                    outcome: StoredOutcome::Failure {
                        kind: err.kind.into(),
                        message: err.message.clone(),
                    },
                };
                if let Err(write_err) = write_outcome(&step.run_memo, &record).await {
                    tracing::warn!(
                        run_id = %step.run_id,
                        "failed to persist the run's failure outcome: {write_err}"
                    );
                }
            }
            Err(err)
        }
    }
}

pub(crate) fn hash_input(input: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(input).into()
}

/// Read the outcome record of `run_memo`. A record that fails to decode is
/// treated as absent.
pub(crate) async fn read_outcome(run_memo: &Memo) -> Result<Option<OutcomeRecord>> {
    match run_memo.get(OUTCOME_KEY).await? {
        None => Ok(None),
        Some(bytes) => match rmp_serde::from_slice(&bytes) {
            Ok(record) => Ok(Some(record)),
            Err(err) => {
                tracing::warn!(
                    run_id = %run_memo.run_id(),
                    error = %err,
                    "outcome record failed to decode; treated as absent",
                );
                Ok(None)
            }
        },
    }
}

pub(crate) async fn write_outcome(run_memo: &Memo, record: &OutcomeRecord) -> Result<()> {
    let bytes = rmp_serde::to_vec_named(record)?;
    run_memo.put(OUTCOME_KEY, &bytes).await
}

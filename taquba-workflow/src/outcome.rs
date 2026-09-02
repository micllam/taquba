//! The typed single-step run shared by the [`jobs`](crate::jobs) and
//! [`bulk`](crate::bulk) modules: the adapter that decodes a typed
//! input, runs a typed handler and encodes its output
//! ([`run_typed_step`]), the durable outcome record it writes
//! before its settlement, stored in the run-scoped memo under a key
//! reserved by this crate, and the in-process wait for the run's
//! terminal state ([`wait_terminal`]).

use std::future::Future;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use taquba::{Queue, WaitOutcome};

use crate::keys::hash_input;
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

/// Run one typed single-step delivery: decode `input`, the serialized
/// typed input carried in the step's payload, as `I`, run `handler` on it
/// and encode its `O` as the step's result, recording the outcome through
/// [`run_recorded`]. An input that does not decode and an output that
/// does not encode are permanent errors, since a retry cannot change
/// either; `name` identifies the typed handler in those messages.
pub(crate) async fn run_typed_step<I, O, F, Fut>(
    step: &Step,
    name: &str,
    input: &[u8],
    handler: F,
) -> std::result::Result<StepOutcome, StepError>
where
    I: DeserializeOwned,
    O: Serialize,
    F: FnOnce(I) -> Fut,
    Fut: Future<Output = std::result::Result<O, StepError>>,
{
    run_recorded(step, async {
        let input: I = rmp_serde::from_slice(input)
            .map_err(|err| StepError::permanent(format!("invalid input for `{name}`: {err}")))?;
        let output = handler(input).await?;
        rmp_serde::to_vec_named(&output).map_err(|err| {
            StepError::permanent(format!(
                "`{name}` produced an output that failed to serialize: {err}"
            ))
        })
    })
    .await
}

/// Run the typed part of a single-step run and record its outcome: on
/// `Ok(bytes)` the success record is written before the step succeeds with
/// `bytes`; on `Err` a failure record is written when the error is
/// permanent or the attempt is the step's last, and the error is returned.
/// A failed success-record write is a transient error, so the step
/// retries.
async fn run_recorded<F>(step: &Step, produce: F) -> std::result::Result<StepOutcome, StepError>
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

/// How a typed single-step run ended, as observed by an in-process
/// waiter: with its outcome record, or without one.
pub(crate) enum Terminal {
    Recorded(OutcomeRecord),
    /// The run ended without the step recording an outcome: it was
    /// cancelled, dead-lettered by the reaper or its records are gone.
    Unrecorded(Unrecorded),
}

pub(crate) enum Unrecorded {
    /// The queue job was acknowledged; only an external cancellation
    /// acknowledges a typed step without a record.
    Done,
    /// The queue job was dead-lettered outside the step, with the queue
    /// record's last error.
    Dead(Option<String>),
    /// The queue job was removed by a cancellation before it was claimed.
    Cancelled,
    /// Neither a queue record nor an outcome record exists.
    NotFound,
}

/// Wait up to `timeout` for the typed single-step run whose step job is
/// `job_id` to reach a terminal state, then read its outcome record.
/// Returns `Ok(None)` when the timeout elapses first.
pub(crate) async fn wait_terminal(
    queue: &Queue,
    run_memo: &Memo,
    job_id: &str,
    timeout: Duration,
) -> Result<Option<Terminal>> {
    let unrecorded = match queue.wait_for_completion(job_id, timeout).await? {
        WaitOutcome::TimedOut => return Ok(None),
        WaitOutcome::Done(_) => Unrecorded::Done,
        WaitOutcome::Dead(record) => Unrecorded::Dead(record.last_error),
        WaitOutcome::Cancelled => Unrecorded::Cancelled,
        WaitOutcome::NotFound => Unrecorded::NotFound,
    };
    Ok(Some(match read_outcome(run_memo).await? {
        Some(record) => Terminal::Recorded(record),
        None => Terminal::Unrecorded(unrecorded),
    }))
}

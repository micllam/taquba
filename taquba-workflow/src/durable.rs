//! Stored forms of the runtime's public types. Each `Durable*` type
//! mirrors a public type and is what actually serializes, so the public
//! type can evolve without changing the stored layout. Records are
//! MessagePack maps with named fields, written through [`encode`] and
//! read through [`decode`] or, for a record at one key of the queue's
//! KV namespace, [`kv_record`].
//!
//! A record that fails to decode is treated in one of two ways, chosen
//! by what its absence means to the reader, and each treatment has a
//! function of its own. Where absence means that the work runs again, the reader
//! calls [`decode_or_absent`], which logs the failure and reports the
//! record as absent: a memo entry is recomputed, a step-output replay
//! record re-executes the step, a run result record is reported as
//! missing, and a member record skipped by the group listing is
//! submitted again. Where absence is read as a fresh entity, the reader
//! calls [`decode`] or [`kv_record`], and the failure propagates as
//! [`Error::Deserialization`](crate::Error::Deserialization): the run
//! record, the current-step pointer, the terminal record, a member
//! record read on its own and the group manifest, whose absence starts
//! a new run or group. A new reader calls one of these and does not
//! deserialize a record directly.

use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use taquba::QueueView;
use tracing::warn;

use crate::error::Result;

/// Encode one of the crate's own records as MessagePack with named
/// fields. The record types here hold strings, bytes, integers and
/// enumerations, whose encoding cannot fail.
pub(crate) fn encode<T: Serialize>(record: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(record).expect("a durable record encodes")
}

/// Decode one of the crate's own records. A record that does not decode
/// is [`Error::Deserialization`](crate::Error::Deserialization).
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(rmp_serde::from_slice(bytes)?)
}

/// Decode a record whose absence means that the work runs again. A
/// record that fails to decode is logged, as the `record` kind of the
/// entity `id`, and reported as absent.
pub(crate) fn decode_or_absent<T: DeserializeOwned>(
    bytes: &[u8],
    record: &'static str,
    id: &dyn Display,
) -> Option<T> {
    match rmp_serde::from_slice(bytes) {
        Ok(value) => Some(value),
        Err(err) => {
            warn!(record, %id, error = %err, "{record} failed to decode; treated as absent");
            None
        }
    }
}

/// The record at `key` of the queue's KV namespace, `None` when no
/// value is stored there.
pub(crate) async fn kv_record<T: DeserializeOwned>(
    view: &QueueView,
    key: &[u8],
) -> Result<Option<T>> {
    match view.kv_get(key).await? {
        Some(bytes) => decode(&bytes).map(Some),
        None => Ok(None),
    }
}

use crate::effects::StagedEffects;
use crate::keys::RunId;
use crate::runner::{StepErrorKind, StepOutcome, Trigger};
use crate::terminal::{RunOutcome, TerminalStatus};

/// Durable per-run record written atomically with the step-0 enqueue in
/// [`WorkflowRuntime::submit`] via [`Queue::enqueue_with_kv`]. Carries
/// just enough state to detect duplicate submissions across runtime
/// restarts, to reject re-submissions that change the input and to
/// carry a cancellation request to the run's next step. Deleted with
/// the settlement that terminates the run, staged in
/// `terminate_collecting_effects`.
///
/// `run_id` keeps the record self-describing for ad hoc operator
/// inspection; `submitted_at_ms` is useful for ordering and stale-record
/// auditing; `input_hash` is the SHA-256 of the original `spec.input` and
/// powers the `Error::InputMismatch` check on duplicate submissions.
/// `cancel_requested` is set by `WorkflowRuntime::cancel` on this key
/// so that the write conflicts with the termination's delete of the
/// record and a request can never outlive the run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableRunRecord {
    pub(crate) run_id: RunId,
    pub(crate) submitted_at_ms: u64,
    pub(crate) input_hash: [u8; 32],
    pub(crate) cancel_requested: bool,
}

/// Durable pointer from a run to the queue job currently representing
/// it, kept beside the run record: written with the step-0 enqueue,
/// rewritten in the settlement that enqueues each next step and deleted
/// with the termination. It is what a duplicate submission known only
/// from the durable record, or a reader outside the process, resolves a
/// run's live job from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableCurrentStep {
    pub(crate) step_number: u32,
    pub(crate) job_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct DurableDuration {
    secs: u64,
    nanos: u32,
}

impl From<Duration> for DurableDuration {
    fn from(duration: Duration) -> Self {
        Self {
            secs: duration.as_secs(),
            nanos: duration.subsec_nanos(),
        }
    }
}

impl From<DurableDuration> for Duration {
    fn from(duration: DurableDuration) -> Self {
        Duration::new(duration.secs, duration.nanos)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum DurableTrigger {
    Immediate,
    After(DurableDuration),
    OnSignal {
        correlation_key: String,
        timeout: DurableDuration,
    },
}

impl From<&Trigger> for DurableTrigger {
    fn from(trigger: &Trigger) -> Self {
        match trigger {
            Trigger::Immediate => Self::Immediate,
            Trigger::After(delay) => Self::After((*delay).into()),
            Trigger::OnSignal {
                correlation_key,
                timeout,
            } => Self::OnSignal {
                correlation_key: correlation_key.clone(),
                timeout: (*timeout).into(),
            },
        }
    }
}

impl From<DurableTrigger> for Trigger {
    fn from(trigger: DurableTrigger) -> Self {
        match trigger {
            DurableTrigger::Immediate => Self::Immediate,
            DurableTrigger::After(delay) => Self::After(delay.into()),
            DurableTrigger::OnSignal {
                correlation_key,
                timeout,
            } => Self::OnSignal {
                correlation_key,
                timeout: timeout.into(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum DurableStepOutcome {
    Continue {
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
        when: DurableTrigger,
    },
    Succeed {
        #[serde(with = "serde_bytes")]
        result: Vec<u8>,
    },
    Fail {
        reason: String,
    },
    Cancel {
        reason: String,
    },
}

impl From<&StepOutcome> for DurableStepOutcome {
    fn from(outcome: &StepOutcome) -> Self {
        match outcome {
            StepOutcome::Continue { payload, when } => Self::Continue {
                payload: payload.clone(),
                when: when.into(),
            },
            StepOutcome::Succeed { result } => Self::Succeed {
                result: result.clone(),
            },
            StepOutcome::Fail { reason } => Self::Fail {
                reason: reason.clone(),
            },
            StepOutcome::Cancel { reason } => Self::Cancel {
                reason: reason.clone(),
            },
        }
    }
}

impl From<DurableStepOutcome> for StepOutcome {
    fn from(outcome: DurableStepOutcome) -> Self {
        match outcome {
            DurableStepOutcome::Continue { payload, when } => Self::Continue {
                payload,
                when: when.into(),
            },
            DurableStepOutcome::Succeed { result } => Self::Succeed { result },
            DurableStepOutcome::Fail { reason } => Self::Fail { reason },
            DurableStepOutcome::Cancel { reason } => Self::Cancel { reason },
        }
    }
}

/// Storage envelope for a step-output replay entry. `stored_at_ms`
/// records when the outcome was persisted so a replayed delayed `Continue`
/// can schedule the next step relative to the original settlement
/// rather than the replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableStepOutcomeRecord {
    pub(crate) stored_at_ms: u64,
    pub(crate) outcome: DurableStepOutcome,
    /// Effects staged through [`crate::EffectsHandle`] during the
    /// recorded delivery, restored into the settlement when the outcome
    /// is replayed.
    pub(crate) effects: StagedEffects,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DurableTerminalStatus {
    Succeeded,
    Failed,
    Cancelled,
}

impl From<TerminalStatus> for DurableTerminalStatus {
    fn from(status: TerminalStatus) -> Self {
        match status {
            TerminalStatus::Succeeded => Self::Succeeded,
            TerminalStatus::Failed => Self::Failed,
            TerminalStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl From<DurableTerminalStatus> for TerminalStatus {
    fn from(status: DurableTerminalStatus) -> Self {
        match status {
            DurableTerminalStatus::Succeeded => Self::Succeeded,
            DurableTerminalStatus::Failed => Self::Failed,
            DurableTerminalStatus::Cancelled => Self::Cancelled,
        }
    }
}

/// The stored form of a [`RunTermination`](crate::RunTermination) with the
/// final step and the SHA-256 of the run's input: the terminal record under
/// `workflow/outcomes/{run_id}`, written in the settlement that terminates the
/// run and read by [`WorkflowView::status`](crate::WorkflowView::status) once
/// the run record is gone and by a typed re-submission after completion, the
/// termination half of a member record and the termination a run result record
/// belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DurableTermination {
    pub(crate) status: DurableTerminalStatus,
    pub(crate) error: Option<String>,
    pub(crate) error_kind: Option<DurableErrorKind>,
    pub(crate) final_step: u32,
    pub(crate) terminated_at_ms: u64,
    pub(crate) input_hash: [u8; 32],
}

/// The durable member record of a grouped run, written under
/// `workflow/groups/{group_id}/{key}` with the member's submission and
/// rewritten in the settlement that terminates it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableMember {
    pub(crate) run_id: RunId,
    /// The member's termination; `None` while it is active.
    pub(crate) terminated: Option<DurableTermination>,
}

/// Stored form of a [`RunOutcome`]: the payload of a terminal-notification
/// job and the outcome half of the run result record, self-contained so
/// both survive restarts and redeliveries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableRunOutcome {
    run_id: RunId,
    status: DurableTerminalStatus,
    #[serde(with = "serde_bytes")]
    result: Option<Vec<u8>>,
    error: Option<String>,
    headers: HashMap<String, String>,
    final_step: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DurableErrorKind {
    Transient,
    Permanent,
}

impl From<StepErrorKind> for DurableErrorKind {
    fn from(kind: StepErrorKind) -> Self {
        match kind {
            StepErrorKind::Transient => Self::Transient,
            StepErrorKind::Permanent => Self::Permanent,
        }
    }
}

impl From<DurableErrorKind> for StepErrorKind {
    fn from(kind: DurableErrorKind) -> Self {
        match kind {
            DurableErrorKind::Transient => Self::Transient,
            DurableErrorKind::Permanent => Self::Permanent,
        }
    }
}

/// The run result record, stored in the run memo under
/// [`RUN_RESULT_MEMO_KEY`](crate::memo::RUN_RESULT_MEMO_KEY) by the
/// worker before the settlement that terminates the run: the committed
/// outcome and the termination the record belongs to, equal to the
/// terminal record written by the same settlement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableRunResult {
    pub(crate) termination: DurableTermination,
    pub(crate) outcome: DurableRunOutcome,
}

impl From<&RunOutcome> for DurableRunOutcome {
    fn from(outcome: &RunOutcome) -> Self {
        Self {
            run_id: outcome.run_id.clone(),
            status: outcome.status.into(),
            result: outcome.result.clone(),
            error: outcome.error.clone(),
            headers: outcome.headers.clone(),
            final_step: outcome.final_step,
        }
    }
}

impl From<DurableRunOutcome> for RunOutcome {
    fn from(outcome: DurableRunOutcome) -> Self {
        Self {
            run_id: outcome.run_id,
            status: outcome.status.into(),
            result: outcome.result,
            error: outcome.error,
            headers: outcome.headers,
            final_step: outcome.final_step,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::rid;

    #[test]
    fn payload_bytes_are_stored_as_binary_strings() {
        // A binary string stores the bytes as they are, so the encoded
        // record contains the payload as one contiguous window. An
        // integer array prefixes every byte at or above `0x80`.
        let payload: Vec<u8> = (0..=255).collect();
        let is_contiguous = |bytes: &[u8]| bytes.windows(payload.len()).any(|w| w == payload);
        assert!(is_contiguous(&encode(&DurableStepOutcome::Continue {
            payload: payload.clone(),
            when: DurableTrigger::Immediate,
        })));
        assert!(is_contiguous(&encode(&DurableStepOutcome::Succeed {
            result: payload.clone(),
        })));
        let outcome = DurableRunOutcome {
            run_id: rid("run"),
            status: DurableTerminalStatus::Succeeded,
            result: Some(payload.clone()),
            error: None,
            headers: HashMap::new(),
            final_step: 0,
        };
        let stored = encode(&outcome);
        assert!(is_contiguous(&stored));
        let decoded: DurableRunOutcome = decode(&stored).unwrap();
        assert_eq!(decoded.result, Some(payload));
    }
}

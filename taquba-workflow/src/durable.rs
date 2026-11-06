//! Stored forms of the runtime's public types. Each `Durable*` type mirrors a
//! public type and serializes in its place, so the public type can evolve
//! without changing the stored layout. Records are MessagePack maps with named
//! fields, written through [`encode`] and read through [`decode`] or, for a
//! record at one key of the queue's KV namespace, [`kv_record`].
//!
//! A record that fails to decode is treated in one of two ways, chosen by what
//! its absence means to the reader, and each treatment has a function of its
//! own. Where absence means that the work runs again, the reader calls
//! [`decode_or_absent`], which logs the failure and reports the record as
//! absent: a memo entry is recomputed, a step-output replay record re-executes
//! the step, and a member record skipped by the group listing is submitted
//! again. Where absence is read as a fresh entity, the reader calls [`decode`]
//! or [`kv_record`], and the failure propagates as
//! [`Error::Deserialization`](crate::Error::Deserialization): the run record
//! with its terminal record, the current-step pointer, a member record read on
//! its own and the group manifest, whose absence starts a new run or group. A
//! new reader calls one of these and does not deserialize a record directly.

use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use taquba::QueueView;
use tracing::warn;

use crate::error::Result;

/// Encode one of the crate's own records as MessagePack with named fields. The
/// record types here hold strings, bytes, integers and enumerations, whose
/// encoding cannot fail.
pub(crate) fn encode<T: Serialize>(record: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(record).expect("a durable record encodes")
}

/// Decode one of the crate's own records. A record that does not decode is
/// [`Error::Deserialization`](crate::Error::Deserialization).
#[expect(clippy::disallowed_methods, reason = "a decode of a durable record")]
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(rmp_serde::from_slice(bytes)?)
}

/// Decode a record whose absence means that the work runs again. A record that
/// fails to decode is logged, as the `record` kind of the entity `id`, and
/// reported as absent.
#[expect(clippy::disallowed_methods, reason = "a decode of a durable record")]
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

/// The record at `key` of the queue's KV namespace, `None` when no value is
/// stored there.
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
use crate::owner::SubmissionId;
use crate::runner::{StepErrorKind, StepOutcome, Trigger};
use crate::terminal::{CancelOrigin, RunTermination, TerminalStatus};
use crate::view::StepRef;

/// The record of a run at `workflow/runs/{run_id}`. The submission writes the
/// run record atomically with the step-0 enqueue, and the settlement that
/// terminates the run replaces it with the terminal record, staged in
/// `termination_effects`. The memo sweep deletes the terminal record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum DurableRun {
    Active(DurableRunRecord),
    Terminated(DurableTermination),
}

/// The record of an active run. It contains enough state to detect duplicate
/// submissions across runtime restarts, to reject re-submissions that change
/// the input and to record a cancellation request for the run's next step.
///
/// `run_id` keeps the record self-describing for ad hoc operator inspection.
/// `submitted_at_ms` is the submission time that the run listing reports, and
/// the terminal record copies it. `input_hash` is the SHA-256 of the original
/// `spec.input` and powers the `Error::InputMismatch` check on duplicate
/// submissions. `cancel_request` is set by `WorkflowRuntime::cancel` with a
/// compare against the active record, so the write conflicts with the
/// termination's write of the terminal record and a request can never outlive
/// the run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableRunRecord {
    pub(crate) run_id: RunId,
    pub(crate) submission_id: SubmissionId,
    pub(crate) submitted_at_ms: u64,
    pub(crate) input_hash: [u8; 32],
    pub(crate) cancel_request: Option<DurableCancelRequest>,
}

/// The submission of a run as its run record states it, which a termination
/// copies into the terminal record.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Submission {
    pub(crate) submission_id: SubmissionId,
    pub(crate) input_hash: [u8; 32],
    pub(crate) submitted_at_ms: u64,
}

impl From<&DurableRunRecord> for Submission {
    fn from(record: &DurableRunRecord) -> Self {
        Self {
            submission_id: record.submission_id,
            input_hash: record.input_hash,
            submitted_at_ms: record.submitted_at_ms,
        }
    }
}

/// The cancellation request of a run, on its run record: the reason and the
/// origin of the first request and the runtime clock's time of its write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DurableCancelRequest {
    pub(crate) reason: Option<String>,
    pub(crate) origin: DurableCancelOrigin,
    pub(crate) requested_at_ms: u64,
}

/// Stored form of a [`CancelOrigin`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DurableCancelOrigin {
    Runner,
    Request,
    Owner,
}

impl From<CancelOrigin> for DurableCancelOrigin {
    fn from(origin: CancelOrigin) -> Self {
        match origin {
            CancelOrigin::Runner => Self::Runner,
            CancelOrigin::Request => Self::Request,
            CancelOrigin::Owner => Self::Owner,
        }
    }
}

impl From<DurableCancelOrigin> for CancelOrigin {
    fn from(origin: DurableCancelOrigin) -> Self {
        match origin {
            DurableCancelOrigin::Runner => Self::Runner,
            DurableCancelOrigin::Request => Self::Request,
            DurableCancelOrigin::Owner => Self::Owner,
        }
    }
}

/// Durable pointer from a run to the queue job currently representing it, kept
/// together with the run record: written with the step-0 enqueue, rewritten in
/// the settlement that enqueues each next step and deleted with the
/// termination. A duplicate submission known only from the durable record, or a
/// reader outside the process, resolves a run's live job from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DurableStepRef {
    pub(crate) step_number: u32,
    pub(crate) job_id: String,
}

impl From<&StepRef> for DurableStepRef {
    fn from(step: &StepRef) -> Self {
        Self {
            step_number: step.number,
            job_id: step.job_id.clone(),
        }
    }
}

impl From<DurableStepRef> for StepRef {
    fn from(step: DurableStepRef) -> Self {
        Self {
            number: step.step_number,
            job_id: step.job_id,
        }
    }
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
        timeout: DurableDuration,
    },
    OnGroup {
        group_id: RunId,
        timeout: DurableDuration,
    },
}

impl From<&Trigger> for DurableTrigger {
    fn from(trigger: &Trigger) -> Self {
        match trigger {
            Trigger::Immediate => Self::Immediate,
            Trigger::After(delay) => Self::After((*delay).into()),
            Trigger::OnSignal { timeout } => Self::OnSignal {
                timeout: (*timeout).into(),
            },
            Trigger::OnGroup { group_id, timeout } => Self::OnGroup {
                group_id: group_id.clone(),
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
            DurableTrigger::OnSignal { timeout } => Self::OnSignal {
                timeout: timeout.into(),
            },
            DurableTrigger::OnGroup { group_id, timeout } => Self::OnGroup {
                group_id,
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

/// Storage envelope for a step-output replay entry. `stored_at_ms` records when
/// the outcome was persisted so a replayed delayed `Continue` can schedule the
/// next step relative to the original settlement. The replay does not reset the
/// delay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableStepOutcomeRecord {
    pub(crate) stored_at_ms: u64,
    pub(crate) outcome: DurableStepOutcome,
    /// Effects staged through [`crate::EffectsHandle`] during the recorded
    /// delivery, restored into the settlement when the outcome is replayed.
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

/// The stored form of a [`RunTermination`] with the final step, the SHA-256 of
/// the run's input, the submission time, the submitter's headers and the
/// reference to the result: the terminal record of a [`DurableRun`] and the
/// termination half of a member record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DurableTermination {
    pub(crate) status: DurableTerminalStatus,
    pub(crate) error: Option<String>,
    pub(crate) cancelled_by: Option<DurableCancelOrigin>,
    pub(crate) error_kind: Option<DurableErrorKind>,
    pub(crate) final_step: DurableStepRef,
    pub(crate) terminated_at_ms: u64,
    pub(crate) input_hash: [u8; 32],
    pub(crate) submitted_at_ms: u64,
    pub(crate) headers: HashMap<String, String>,
    /// The result of a succeeded run, `None` for any other status.
    pub(crate) result: Option<DurableResult>,
}

impl From<&DurableTermination> for RunTermination {
    fn from(record: &DurableTermination) -> Self {
        Self {
            status: record.status.into(),
            error: record.error.clone(),
            cancelled_by: record.cancelled_by.map(Into::into),
            error_kind: record.error_kind.map(Into::into),
            final_step: record.final_step.clone().into(),
            terminated_at_ms: record.terminated_at_ms,
        }
    }
}

/// The reference from a terminal record to the result of a succeeded run. A
/// non-empty result is an object of the memo store, which the attempt `attempt`
/// of the final step writes before the terminating settlement. The attempt
/// makes the object name unique, so a stale attempt cannot overwrite a
/// committed result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DurableResult {
    Empty,
    Object { attempt: u32 },
}

/// The durable member record of a grouped run, written at
/// `workflow/groups/{group_id}/{key}` with the member's submission and
/// rewritten in the settlement that terminates it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableMember {
    pub(crate) run_id: RunId,
    /// The member's termination, or `None` while it is active.
    pub(crate) terminated: Option<DurableTermination>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_bytes_are_stored_as_binary_strings() {
        // A binary string stores the bytes as they are, so the encoded record
        // contains the payload as one contiguous window. An integer array
        // prefixes every byte at or above `0x80`.
        let payload: Vec<u8> = (0..=255).collect();
        let is_contiguous = |bytes: &[u8]| bytes.windows(payload.len()).any(|w| w == payload);
        assert!(is_contiguous(&encode(&DurableStepOutcome::Continue {
            payload: payload.clone(),
            when: DurableTrigger::Immediate,
        })));
        assert!(is_contiguous(&encode(&DurableStepOutcome::Succeed {
            result: payload.clone(),
        })));
    }
}

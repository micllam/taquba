//! Stored forms of the runtime's public types. Each `Durable*` type
//! mirrors a public type and is what actually serializes, so the public
//! type can evolve without changing the stored layout. Records are
//! encoded as MessagePack maps (`rmp_serde::to_vec_named`) at the call
//! sites.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Encode one of the crate's own records as MessagePack with named
/// fields. The record types here hold strings, bytes, integers and
/// enumerations, whose encoding cannot fail.
pub(crate) fn encode<T: Serialize>(record: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(record).expect("a durable record encodes")
}

use crate::effects::StagedEffects;
use crate::runner::{StepOutcome, Trigger};
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
    pub(crate) run_id: String,
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
        payload: Vec<u8>,
        when: DurableTrigger,
    },
    Succeed {
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

/// The durable terminal record of a run, written under
/// `workflow/outcomes/{run_id}` in the settlement that terminates the
/// run when memo retention is set, and read by
/// [`WorkflowRuntime::status`](crate::WorkflowRuntime::status) once the
/// run record is gone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableTermination {
    pub(crate) status: DurableTerminalStatus,
    pub(crate) error: Option<String>,
    pub(crate) final_step: u32,
    pub(crate) terminated_at_ms: u64,
}

/// The durable member record of a grouped run, written under
/// `workflow/groups/{group_id}/{key}` with the member's submission and
/// rewritten in the settlement that terminates it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableMember {
    pub(crate) run_id: String,
    /// The member's termination; `None` while it is active.
    pub(crate) terminated: Option<DurableTermination>,
}

/// Stored payload of a terminal-notification job: the committed
/// [`RunOutcome`], self-contained so the notification survives restarts
/// and redeliveries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableRunOutcome {
    run_id: String,
    status: DurableTerminalStatus,
    result: Option<Vec<u8>>,
    error: Option<String>,
    headers: HashMap<String, String>,
    final_step: u32,
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

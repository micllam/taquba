//! Stored forms of the runtime's public types. Each `Durable*` type
//! mirrors a public type and is what actually serializes, so the public
//! type can evolve without changing the stored layout. Records are
//! encoded as MessagePack maps (`rmp_serde::to_vec_named`) at the call
//! sites.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::effects::StagedEffects;
use crate::runner::{StepOutcome, Trigger};
use crate::terminal::{RunOutcome, TerminalStatus};

/// Durable per-run record written atomically with the step-0 enqueue in
/// [`WorkflowRuntime::submit`] via [`Queue::enqueue_with_kv`]. Carries
/// just enough state to detect duplicate submissions across runtime
/// restarts and to reject re-submissions that change the input;
/// the in-memory registry remains the source of truth for active-run
/// status and cancellation while a runtime is up. Deleted with the
/// settlement that terminates the run, staged in
/// `terminate_collecting_effects`.
///
/// `run_id` keeps the record self-describing for ad hoc operator
/// inspection; `submitted_at_ms` is useful for ordering and stale-record
/// auditing; `input_hash` is the SHA-256 of the original `spec.input` and
/// powers the `Error::InputMismatch` check on duplicate submissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableRunRecord {
    pub(crate) run_id: String,
    pub(crate) submitted_at_ms: u64,
    pub(crate) input_hash: [u8; 32],
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

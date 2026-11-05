use std::collections::HashMap;

use crate::effects::TerminalEffects;
use crate::keys::RunId;
use crate::runner::StepErrorKind;
use crate::runtime::StepRef;

/// Terminal state of a workflow run, passed to a [`TerminalHook`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalStatus {
    /// The runner returned [`crate::StepOutcome::Succeed`].
    Succeeded,
    /// One of:
    /// - The runner returned [`crate::StepOutcome::Fail`] (a runner decision).
    /// - A step returned [`crate::StepError::permanent`].
    /// - A step reached its attempt limit with transient errors.
    /// - A step was dead-lettered outside the runner, by a lease expiry past
    ///   the attempt limit, by crash recovery at open or by a permanent runtime
    ///   error before the runner ran. The worker's reconciliation then
    ///   terminated the run with the queue record's last error.
    Failed,
    /// The run was cancelled. Either:
    /// - [`crate::WorkflowRuntime::cancel`] was called for this run.
    /// - The runner returned [`crate::StepOutcome::Cancel`].
    ///
    /// Like [`Self::Failed`] from `StepOutcome::Fail`, this is a clean
    /// run-level outcome and distinct from an infrastructure error: the step is
    /// acked and no dead-letter is produced.
    Cancelled,
}

impl TerminalStatus {
    /// Canonical lower-case identifier for this status, suitable for HTTP
    /// headers, structured logs and other wire-format use. Stable across minor
    /// releases.
    pub fn as_str(&self) -> &'static str {
        match self {
            TerminalStatus::Succeeded => "succeeded",
            TerminalStatus::Failed => "failed",
            TerminalStatus::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for TerminalStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The origin of the cancellation of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOrigin {
    /// The runner returned [`crate::StepOutcome::Cancel`] without a request.
    Runner,
    /// A request through [`crate::WorkflowRuntime::cancel`] or a
    /// [`crate::CancelCommand`].
    Request,
}

impl CancelOrigin {
    /// Canonical lower-case identifier for this origin, as
    /// [`TerminalStatus::as_str`] has for a status.
    pub fn as_str(&self) -> &'static str {
        match self {
            CancelOrigin::Runner => "runner",
            CancelOrigin::Request => "request",
        }
    }
}

/// How a run ended, as its terminal record states it.
/// [`RunState::Terminated`](crate::RunState::Terminated) reports it without the
/// result, and [`TerminatedRun`] contains it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTermination {
    /// How the run ended.
    pub status: TerminalStatus,
    /// - When `status == Failed`: the human-readable reason recorded on
    ///   the terminal step's `last_error`.
    /// - When `status == Cancelled`: the reason of the
    ///   [`crate::StepOutcome::Cancel`], or the reason of the request, which
    ///   can be `None`.
    /// - When `status == Succeeded`: always `None`.
    pub error: Option<String>,
    /// The origin of the cancellation when `status == Cancelled`, `None`
    /// otherwise.
    pub cancelled_by: Option<CancelOrigin>,
    /// The classification of a failure: the kind of the
    /// [`StepError`](crate::StepError) that dead-lettered the run, or
    /// [`StepErrorKind::Permanent`] for a [`crate::StepOutcome::Fail`] outcome.
    /// `None` for a success, a cancellation and a termination outside the
    /// worker.
    pub error_kind: Option<StepErrorKind>,
    /// The step whose settlement terminated the run. Its job is the dead job of
    /// a run that a dead-letter terminated.
    pub final_step: StepRef,
    /// The runtime clock's time at the terminating settlement, as a Unix
    /// timestamp in milliseconds.
    pub terminated_at_ms: u64,
}

/// The end of a run: its termination, its result and the submitter's headers.
/// [`TerminalHook::stage_effects`] receives it, and
/// [`WorkflowRuntime::wait`](crate::WorkflowRuntime::wait) and
/// [`RunGroup::results`](crate::RunGroup::results) report it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminatedRun {
    /// The run's identifier.
    pub run_id: RunId,
    /// How the run ended.
    pub termination: RunTermination,
    /// The bytes the runner returned via [`crate::StepOutcome::Succeed`] when
    /// the run succeeded, `None` otherwise.
    pub result: Option<Vec<u8>>,
    /// Submitter-supplied metadata, threaded through from
    /// [`crate::RunOptions::headers`].
    pub headers: HashMap<String, String>,
}

/// User-implemented hook that stages effects into the settlement that
/// terminates a run. Set on a runtime with
/// [`WorkflowRuntimeBuilder::terminal_hook`](crate::WorkflowRuntimeBuilder::terminal_hook).
///
/// The staged effects commit atomically with the run's terminal outcome, on
/// every termination path: the worker's settlement, an external cancellation
/// and the reconciliation of a step dead-lettered outside the worker. A
/// reaction that runs code after the termination stages an enqueue to a queue
/// of the application, whose worker processes it at least once.
pub trait TerminalHook: Send + Sync {
    /// Stage effects that commit in the settlement that terminates the run of
    /// `outcome`. The runtime calls the method again for a settlement that it
    /// builds again: a failed settlement at the redelivery of the step, and a
    /// cancellation that finds the step running or settled. The method must
    /// therefore stage from `outcome` alone and stage the same effects for the
    /// same outcome. A staging error of `effects` is a programming error: the
    /// hook logs it and drops the effect, and the run terminates.
    fn stage_effects(&self, outcome: &TerminatedRun, effects: &TerminalEffects);

    /// Check the hook's configuration against a runtime with the queue
    /// `runtime_queue`. [`WorkflowRuntimeBuilder::build`](crate::WorkflowRuntimeBuilder::build)
    /// fails with the error. Defaults to `Ok`.
    fn check_runtime(&self, runtime_queue: &str) -> crate::Result<()> {
        let _ = runtime_queue;
        Ok(())
    }
}

#[cfg(feature = "webhooks")]
mod webhook {
    use super::{TerminalEffects, TerminalHook, TerminalStatus, TerminatedRun};
    use crate::error::Error;
    use std::time::Duration;
    use taquba_webhooks::{WebhookRequest, webhook_enqueue_request};
    use tracing::error;

    /// Terminal hook that delivers an HTTP webhook via `taquba-webhooks` when a
    /// run terminates.
    ///
    /// The hook reads the target URL from the run's submission headers under
    /// [`Self::URL_HEADER`] (default `"callback_url"`). A run without that
    /// header does not enqueue a delivery. The default key intentionally avoids
    /// the reserved `workflow.*` prefix so submitters can set it directly via
    /// [`crate::RunOptions::headers`].
    ///
    /// The webhook enqueue is staged in the settlement that terminates the run,
    /// so the delivery job is created exactly once and atomically with the
    /// termination. A hook whose target is the runtime's own queue fails the
    /// build of the runtime.
    ///
    /// The webhook body is the raw `result` bytes for succeeded runs, and the
    /// UTF-8 error message for failed runs and the reason for cancelled runs.
    /// The run identifier and terminal status are passed in the
    /// `Workflow-Run-Id` and `Workflow-Run-Status` HTTP headers respectively,
    /// and the origin of a cancellation in the `Workflow-Run-Cancelled-By`
    /// header.
    pub struct WebhookTerminalHook {
        target_queue: String,
        url_header: String,
        timeout: Option<Duration>,
    }

    impl WebhookTerminalHook {
        /// Default header key the hook looks for on each [`TerminatedRun`].
        /// Deliberately outside the reserved `workflow.*` prefix so submitters
        /// can set it on [`crate::RunOptions::headers`] without being rejected.
        pub const URL_HEADER: &'static str = "callback_url";

        /// Build a hook that enqueues webhook deliveries onto `target_queue`.
        /// The submitter sets a callback URL per run via the
        /// [`Self::URL_HEADER`] header on [`crate::RunOptions::headers`].
        pub fn new(target_queue: impl Into<String>) -> Self {
            Self {
                target_queue: target_queue.into(),
                url_header: Self::URL_HEADER.to_string(),
                timeout: None,
            }
        }

        /// Override the header key the hook reads. Defaults to
        /// [`Self::URL_HEADER`].
        #[must_use]
        pub fn with_url_header(mut self, header: impl Into<String>) -> Self {
            self.url_header = header.into();
            self
        }

        /// Set a per-delivery timeout passed through to the webhook worker.
        #[must_use]
        pub fn with_timeout(mut self, timeout: Duration) -> Self {
            self.timeout = Some(timeout);
            self
        }
    }

    impl TerminalHook for WebhookTerminalHook {
        fn stage_effects(&self, outcome: &TerminatedRun, effects: &TerminalEffects) {
            let Some(url) = outcome.headers.get(&self.url_header) else {
                return;
            };
            let termination = &outcome.termination;
            let mut req = WebhookRequest::new(url)
                .header("Workflow-Run-Id", outcome.run_id.as_str())
                .header("Workflow-Run-Status", termination.status.as_str());
            if let Some(origin) = termination.cancelled_by {
                req = req.header("Workflow-Run-Cancelled-By", origin.as_str());
            }
            if let Some(t) = self.timeout {
                req = req.timeout(t);
            }
            let body = match termination.status {
                TerminalStatus::Succeeded => outcome.result.clone().unwrap_or_default(),
                TerminalStatus::Failed | TerminalStatus::Cancelled => {
                    termination.error.clone().unwrap_or_default().into_bytes()
                }
            };
            let request = webhook_enqueue_request(&self.target_queue, req, body);
            if let Err(err) = effects.enqueue(request) {
                error!(run_id = %outcome.run_id, error = %err, "dropped the webhook delivery of a terminated run");
            }
        }

        /// Fails with [`Error::ReservedQueue`] when the target queue is the
        /// queue of the runtime.
        fn check_runtime(&self, runtime_queue: &str) -> crate::Result<()> {
            if self.target_queue == runtime_queue {
                return Err(Error::ReservedQueue(self.target_queue.clone()));
            }
            Ok(())
        }
    }
}

#[cfg(feature = "webhooks")]
pub use webhook::WebhookTerminalHook;

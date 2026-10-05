use std::collections::HashMap;
use std::future::Future;

use crate::effects::TerminalEffects;
use crate::keys::RunId;
use crate::runner::StepError;

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

/// Information passed to a [`TerminalHook`] when a run reaches a terminal
/// state.
#[derive(Debug, Clone)]
pub struct RunOutcome {
    /// The run's identifier.
    pub run_id: RunId,
    /// Whether the run completed successfully or failed.
    pub status: TerminalStatus,
    /// Set when `status == Succeeded`: the bytes the runner returned via
    /// [`crate::StepOutcome::Succeed`].
    pub result: Option<Vec<u8>>,
    /// - When `status == Failed`: the human-readable reason recorded on
    ///   the terminal step's `last_error`.
    /// - When `status == Cancelled`: `Some(reason)` if the runner
    ///   returned [`crate::StepOutcome::Cancel`], or `None` if
    ///   cancellation came from [`crate::WorkflowRuntime::cancel`]
    ///   (whose API does not accept a reason).
    /// - When `status == Succeeded`: always `None`.
    pub error: Option<String>,
    /// Submitter-supplied metadata, threaded through from
    /// [`crate::RunOptions::headers`].
    pub headers: HashMap<String, String>,
    /// Step number of the step that produced the terminal outcome (zero-based).
    pub final_step: u32,
}

/// User-implemented hook processing a run's termination.
///
/// A hook acts at two points. [`Self::stage_effects`] stages effects into the
/// settlement that terminates the run, and [`Self::on_termination`] runs after
/// that commit, as the worker of a notification job. [`Self::observes`] decides
/// whether the notification job is enqueued.
///
/// The settlement that commits a run's terminal outcome atomically enqueues the
/// **notification job** on the same queue. The consequences:
///
/// - The hook observes only outcomes that committed. A settlement that
///   loses its claim loses its notification with it, so a redelivered
///   terminal step notifies only the outcome it actually commits.
/// - Delivery is at-least-once: a crash after the hook ran but before
///   the notification job was acknowledged re-delivers it, so
///   implementations must be idempotent.
/// - The notification job takes the terminal step's priority and
///   `max_attempts`. A transient error ([`StepError::transient`]) retries the
///   notification job per the queue's backoff up to `max_attempts`. A permanent
///   error dead-letters it, where [`taquba::QueueView::dead_jobs`] finds it.
/// - Effects that [`Self::on_termination`] stages on its [`TerminalEffects`]
///   handle are applied in the same transaction as the notification's
///   acknowledgement when it returns `Ok`.
///
/// Runs terminated without an acknowledging settlement (an external
/// cancellation of a pending step, a step that dead-letters) enqueue the
/// notification job in the transaction of that transition, so it is created
/// exactly once on every worker and cancellation path. A step the reaper
/// dead-letters after its lease expires, or one dead-lettered during crash
/// recovery at open, is reconciled by the worker, which terminates the run as
/// failed and enqueues the notification in one transaction.
pub trait TerminalHook: Send + Sync {
    /// Process the termination of one run. `outcome` is the committed terminal
    /// state. Effects staged on `effects` commit with this notification's
    /// acknowledgement.
    fn on_termination(
        &self,
        outcome: &RunOutcome,
        effects: &TerminalEffects,
    ) -> impl Future<Output = std::result::Result<(), StepError>> + Send;

    /// Whether a notification job is enqueued for `outcome`. Consulted when the
    /// run terminates. A return value of `false` skips the notification
    /// entirely, and [`Self::on_termination`] is never called for that run.
    /// Defaults to `true`.
    fn observes(&self, outcome: &RunOutcome) -> bool {
        let _ = outcome;
        true
    }

    /// Check the hook's configuration against a runtime with the queue
    /// `runtime_queue`. [`WorkflowRuntimeBuilder::build`](crate::WorkflowRuntimeBuilder::build)
    /// fails with the error. Defaults to `Ok`.
    fn check_runtime(&self, runtime_queue: &str) -> crate::Result<()> {
        let _ = runtime_queue;
        Ok(())
    }

    /// Stage effects that commit in the settlement that terminates the run of
    /// `outcome`, on every termination path: the worker's settlement, an
    /// external cancellation and the reconciliation of a step dead-lettered
    /// outside the worker. A settlement that fails calls the method again at
    /// the redelivery of the step, so it must stage from `outcome` alone and
    /// stage the same effects for the same outcome. A staging error of
    /// `effects` is a programming error: the hook logs it and drops the effect,
    /// and the run terminates. Defaults to staging nothing.
    fn stage_effects(&self, outcome: &RunOutcome, effects: &TerminalEffects) {
        let _ = (outcome, effects);
    }
}

/// A no-op terminal hook. Declares itself unobservant, so runs terminate
/// without enqueueing a notification job.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopTerminalHook;

impl TerminalHook for NoopTerminalHook {
    async fn on_termination(
        &self,
        _outcome: &RunOutcome,
        _effects: &TerminalEffects,
    ) -> std::result::Result<(), StepError> {
        Ok(())
    }

    fn observes(&self, _outcome: &RunOutcome) -> bool {
        false
    }
}

#[cfg(feature = "webhooks")]
mod webhook {
    use super::{RunOutcome, StepError, TerminalEffects, TerminalHook, TerminalStatus};
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
    /// termination. No notification job is enqueued. A hook whose target is the
    /// runtime's own queue fails the build of the runtime.
    ///
    /// The webhook body is the raw `result` bytes for succeeded runs, and the
    /// UTF-8 error message for failed runs. The run identifier and terminal
    /// status are passed in the `Workflow-Run-Id` and `Workflow-Run-Status`
    /// HTTP headers respectively.
    pub struct WebhookTerminalHook {
        target_queue: String,
        url_header: String,
        timeout: Option<Duration>,
    }

    impl WebhookTerminalHook {
        /// Default header key the hook looks for on each [`RunOutcome`].
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
        pub fn with_url_header(mut self, header: impl Into<String>) -> Self {
            self.url_header = header.into();
            self
        }

        /// Set a per-delivery timeout passed through to the webhook worker.
        pub fn with_timeout(mut self, timeout: Duration) -> Self {
            self.timeout = Some(timeout);
            self
        }
    }

    impl TerminalHook for WebhookTerminalHook {
        async fn on_termination(
            &self,
            _outcome: &RunOutcome,
            _effects: &TerminalEffects,
        ) -> std::result::Result<(), StepError> {
            Ok(())
        }

        fn observes(&self, _outcome: &RunOutcome) -> bool {
            false
        }

        fn stage_effects(&self, outcome: &RunOutcome, effects: &TerminalEffects) {
            let Some(url) = outcome.headers.get(&self.url_header) else {
                return;
            };
            let mut req = WebhookRequest::new(url)
                .header("Workflow-Run-Id", outcome.run_id.as_str())
                .header("Workflow-Run-Status", outcome.status.as_str());
            if let Some(t) = self.timeout {
                req = req.timeout(t);
            }
            let body = match outcome.status {
                TerminalStatus::Succeeded => outcome.result.clone().unwrap_or_default(),
                TerminalStatus::Failed | TerminalStatus::Cancelled => {
                    outcome.error.clone().unwrap_or_default().into_bytes()
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

use std::collections::HashMap;
use std::future::Future;

use crate::effects::TerminalEffects;
use crate::runner::StepError;

/// Terminal state of a workflow run, passed to a [`TerminalHook`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TerminalStatus {
    /// The runner returned [`crate::StepOutcome::Succeed`].
    Succeeded,
    /// One of:
    /// - the runner returned [`crate::StepOutcome::Fail`] (runner verdict);
    /// - a step returned [`crate::StepError::permanent`];
    /// - a step exhausted its transient-retry budget; or
    /// - the worker hit a permanent runtime error (e.g. malformed step
    ///   headers).
    Failed,
    /// The run was cancelled. Either:
    /// - [`crate::WorkflowRuntime::cancel`] was called for this run; or
    /// - the runner returned [`crate::StepOutcome::Cancel`].
    ///
    /// Like [`Self::Failed`] from `StepOutcome::Fail`, this is a clean
    /// run-level outcome rather than an infrastructure error: the step is
    /// acked and no dead-letter is produced.
    Cancelled,
}

impl TerminalStatus {
    /// Canonical lowercase identifier for this status, suitable for HTTP
    /// headers, structured logs, and other wire-format use. Stable across
    /// minor releases.
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
    pub run_id: String,
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
    ///   (which takes no reason at the API level).
    /// - When `status == Succeeded`: always `None`.
    pub error: Option<String>,
    /// Submitter-supplied metadata, threaded through from
    /// [`crate::RunSpec::headers`].
    pub headers: HashMap<String, String>,
    /// Step number of the step that produced the terminal outcome (zero-based).
    pub final_step: u32,
}

impl RunOutcome {
    /// A `Succeeded` outcome; `result` holds the runner's result.
    pub(crate) fn succeeded(
        run_id: String,
        result: Vec<u8>,
        headers: HashMap<String, String>,
        final_step: u32,
    ) -> Self {
        Self {
            run_id,
            status: TerminalStatus::Succeeded,
            result: Some(result),
            error: None,
            headers,
            final_step,
        }
    }

    /// A `Failed` outcome; `error` holds the failure reason.
    pub(crate) fn failed(
        run_id: String,
        error: String,
        headers: HashMap<String, String>,
        final_step: u32,
    ) -> Self {
        Self {
            run_id,
            status: TerminalStatus::Failed,
            result: None,
            error: Some(error),
            headers,
            final_step,
        }
    }

    /// A `Cancelled` outcome. `error` is the runner's reason, `None`
    /// for an external cancellation.
    pub(crate) fn cancelled(
        run_id: String,
        error: Option<String>,
        headers: HashMap<String, String>,
        final_step: u32,
    ) -> Self {
        Self {
            run_id,
            status: TerminalStatus::Cancelled,
            result: None,
            error,
            headers,
            final_step,
        }
    }
}

/// User-implemented hook processing a run's termination.
///
/// Termination is delivered as a queue job: the settlement that commits
/// a run's terminal outcome atomically enqueues a **notification job**
/// on the same queue, and the hook runs as that job's worker. The
/// consequences:
///
/// - The hook observes only outcomes that committed. A settlement that
///   loses its claim loses its notification with it, so a redelivered
///   terminal step notifies only the outcome it actually commits.
/// - Delivery is at-least-once: a crash after the hook ran but before
///   the notification job was acknowledged re-delivers it, so
///   implementations must be idempotent.
/// - A transient error ([`StepError::transient`]) retries the
///   notification job per the queue's backoff up to the terminal step's
///   `max_attempts`; a permanent error dead-letters it, where
///   [`taquba::Queue::dead_jobs`] finds it.
/// - Effects staged on the [`TerminalEffects`] handle are applied in
///   the same transaction as the notification's acknowledgement when
///   the hook returns `Ok`.
///
/// Runs terminated without an acknowledging settlement (an external
/// cancellation of a pending step, a step that dead-letters) enqueue
/// the notification job in the transaction of that transition, so it
/// is created exactly once on every worker and cancellation path. A
/// step the reaper dead-letters after its lease expires, or one
/// dead-lettered during crash recovery at open, produces no
/// notification.
pub trait TerminalHook: Send + Sync {
    /// Process the termination of one run. `outcome` is the committed
    /// terminal state; effects staged on `effects` commit with this
    /// notification's acknowledgement.
    fn on_termination(
        &self,
        outcome: &RunOutcome,
        effects: &TerminalEffects,
    ) -> impl Future<Output = std::result::Result<(), StepError>> + Send;

    /// Whether a notification job should be enqueued for `outcome`.
    /// Consulted when the run terminates; returning `false` skips the
    /// notification entirely, so [`Self::on_termination`] is never
    /// called for that run. Defaults to `true`.
    fn observes(&self, outcome: &RunOutcome) -> bool {
        let _ = outcome;
        true
    }
}

/// A no-op terminal hook. Declares itself unobservant, so runs
/// terminate without enqueueing a notification job.
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
    use std::time::Duration;
    use taquba_webhooks::{WebhookRequest, webhook_enqueue_request};

    /// Terminal hook that delivers an HTTP webhook via `taquba-webhooks`
    /// when a run terminates.
    ///
    /// The hook reads the target URL from the run's submission headers
    /// under [`Self::URL_HEADER`] (default `"callback_url"`); runs
    /// without that header enqueue no notification at all. The default
    /// key intentionally avoids the reserved `workflow.*` prefix so
    /// submitters can set it directly via [`crate::RunSpec::headers`].
    ///
    /// The webhook enqueue is staged as a notification effect, so the
    /// delivery job is created exactly once, atomically with the
    /// notification's acknowledgement.
    ///
    /// The webhook body is the raw `result` bytes for succeeded runs, and
    /// the UTF-8 error message for failed runs. The run identifier and
    /// terminal status are passed in the `Workflow-Run-Id` and
    /// `Workflow-Run-Status` HTTP headers respectively.
    pub struct WebhookTerminalHook {
        target_queue: String,
        url_header: String,
        timeout: Option<Duration>,
    }

    impl WebhookTerminalHook {
        /// Default header key the hook looks for on each [`RunOutcome`].
        /// Deliberately outside the reserved `workflow.*` prefix so submitters
        /// can set it on [`crate::RunSpec::headers`] without being
        /// rejected.
        pub const URL_HEADER: &'static str = "callback_url";

        /// Build a hook that enqueues webhook deliveries onto
        /// `target_queue`. The submitter sets a callback URL per run via
        /// the [`Self::URL_HEADER`] header on [`crate::RunSpec::headers`].
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
            outcome: &RunOutcome,
            effects: &TerminalEffects,
        ) -> std::result::Result<(), StepError> {
            let Some(url) = outcome.headers.get(&self.url_header) else {
                return Ok(());
            };
            let mut req = WebhookRequest::new(url)
                .header("Workflow-Run-Id", &outcome.run_id)
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
            effects
                .enqueue(request)
                .map_err(|e| StepError::permanent(e.to_string()))?;
            Ok(())
        }

        fn observes(&self, outcome: &RunOutcome) -> bool {
            outcome.headers.contains_key(&self.url_header)
        }
    }
}

#[cfg(feature = "webhooks")]
pub use webhook::WebhookTerminalHook;

use std::collections::HashMap;
use std::future::Future;

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
}

impl TerminalStatus {
    /// Canonical lowercase identifier for this status, suitable for HTTP
    /// headers, structured logs, and other wire-format use. Stable across
    /// minor releases.
    pub fn as_str(&self) -> &'static str {
        match self {
            TerminalStatus::Succeeded => "succeeded",
            TerminalStatus::Failed => "failed",
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
    /// Set when `status == Failed`: the human-readable reason recorded on the
    /// terminal step's `last_error`.
    pub error: Option<String>,
    /// Submitter-supplied metadata, threaded through from
    /// [`crate::RunSpec::headers`].
    pub headers: HashMap<String, String>,
    /// Step number of the step that produced the terminal outcome (zero-based).
    pub final_step: u32,
}

/// User-implemented hook fired once per run when the run reaches a terminal
/// state.
///
/// The hook is called from the worker task that processed the terminal step,
/// after the step is acked / dead-lettered. Hook errors are not propagated;
/// implementations should either be infallible or log internally.
pub trait TerminalHook: Send + Sync {
    /// Called when a run reaches [`TerminalStatus::Succeeded`] or
    /// [`TerminalStatus::Failed`].
    fn on_termination(&self, outcome: &RunOutcome) -> impl Future<Output = ()> + Send;
}

/// A no-op terminal hook. Useful when the user only cares about run
/// observation via tracing or external state.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopTerminalHook;

impl TerminalHook for NoopTerminalHook {
    async fn on_termination(&self, _outcome: &RunOutcome) {}
}

#[cfg(feature = "webhooks")]
mod webhook {
    use super::{RunOutcome, TerminalHook, TerminalStatus};
    use std::sync::Arc;
    use std::time::Duration;
    use taquba::Queue;
    use taquba_webhooks::{WebhookRequest, enqueue_webhook};

    /// Convenience terminal hook that fires an HTTP webhook delivery via
    /// `taquba-webhooks` whenever a run terminates.
    ///
    /// The hook reads the target URL from the run's submission headers under
    /// [`Self::URL_HEADER`] (default `"callback_url"`). Runs without that
    /// header are simply ignored. The default key intentionally avoids the
    /// reserved `workflow.*` prefix so submitters can set it directly via
    /// [`crate::RunSpec::headers`].
    ///
    /// The webhook body is the raw `result` bytes for succeeded runs, and
    /// the UTF-8 error message for failed runs. The run identifier and
    /// terminal status are passed in the `Workflow-Run-Id` and
    /// `Workflow-Run-Status` HTTP headers respectively.
    pub struct WebhookTerminalHook {
        queue: Arc<Queue>,
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

        /// Build a hook that enqueues webhook deliveries onto `target_queue`
        /// of the supplied Taquba queue. The submitter sets a callback URL
        /// per run via the [`Self::URL_HEADER`] header on
        /// [`crate::RunSpec::headers`].
        pub fn new(queue: Arc<Queue>, target_queue: impl Into<String>) -> Self {
            Self {
                queue,
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
        async fn on_termination(&self, outcome: &RunOutcome) {
            let Some(url) = outcome.headers.get(&self.url_header) else {
                return;
            };
            let mut req = WebhookRequest::new(url)
                .header("Workflow-Run-Id", &outcome.run_id)
                .header("Workflow-Run-Status", outcome.status.as_str());
            if let Some(t) = self.timeout {
                req = req.timeout(t);
            }
            let body = match outcome.status {
                TerminalStatus::Succeeded => outcome.result.clone().unwrap_or_default(),
                TerminalStatus::Failed => outcome.error.clone().unwrap_or_default().into_bytes(),
            };
            if let Err(e) = enqueue_webhook(&self.queue, &self.target_queue, req, body).await {
                tracing::warn!(
                    run_id = %outcome.run_id,
                    error = %e,
                    "webhook terminal-hook enqueue failed"
                );
            }
        }
    }
}

#[cfg(feature = "webhooks")]
pub use webhook::WebhookTerminalHook;

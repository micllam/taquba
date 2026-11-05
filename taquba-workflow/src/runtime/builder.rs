use std::sync::Arc;
use std::time::Duration;

use taquba::object_store::ObjectStore;
use taquba::{Clock, Queue};

use super::{DEFAULT_QUEUE_NAME, RunStore, RuntimeCore, WorkflowRuntime};
use crate::error::{ControlQueueError, Error, Result};
use crate::group::GroupStore;
use crate::keys::{GROUP_TERMINAL_KV_PREFIX, TERMINAL_KV_PREFIX};
use crate::memo::MemoStore;
use crate::runner::StepRunner;
use crate::sweep::Sweep;
use crate::terminal::TerminalHook;
use crate::view::WorkflowView;

/// Builder for [`WorkflowRuntime`].
///
/// Construct via [`WorkflowRuntime::builder`].
pub struct WorkflowRuntimeBuilder<R> {
    queue: Arc<Queue>,
    object_store: Arc<dyn ObjectStore>,
    queue_name: String,
    memo_prefix: Option<String>,
    runner: R,
    terminal_hook: Option<Arc<dyn TerminalHook>>,
    max_concurrent_steps: usize,
    poll_interval: Duration,
    memo_retention: Option<Duration>,
    group_retention: Option<Duration>,
    step_output_replay: bool,
    clock: Arc<dyn Clock>,
    control_queue: Option<String>,
}

impl<R: StepRunner> WorkflowRuntimeBuilder<R> {
    /// The hook that stages effects into the settlement that terminates each
    /// run. When unset (default), a run terminates with the runtime's own
    /// effects alone.
    #[must_use]
    pub fn terminal_hook(mut self, hook: impl TerminalHook + 'static) -> Self {
        self.terminal_hook = Some(Arc::new(hook));
        self
    }

    /// The name of the Taquba queue that step jobs are enqueued onto. Defaults
    /// to `"workflow-steps"`. Multiple runtimes can share a `Queue` handle by
    /// using distinct queue names.
    #[must_use]
    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = name.into();
        self
    }

    /// The object-store path prefix of the
    /// [`Delivery::memo`](crate::Delivery::memo) entries. Defaults to
    /// `"{queue_name}-memo"`, so runtimes with distinct queue names on one
    /// object store have distinct memo namespaces.
    #[must_use]
    pub fn memo_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.memo_prefix = Some(prefix.into());
        self
    }

    /// Maximum number of steps processed concurrently in
    /// [`WorkflowRuntime::run`]. Defaults to 16.
    #[must_use]
    pub fn max_concurrent_steps(mut self, n: usize) -> Self {
        assert!(n > 0, "max_concurrent_steps must be at least 1");
        self.max_concurrent_steps = n;
        self
    }

    /// Maximum time the worker loop waits on an empty queue before re-checking.
    /// Defaults to 250ms. The retention sweeps run at this interval.
    #[must_use]
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Enable memo retention with the given window. When set, the runtime
    /// writes a terminal marker for every run that reaches a terminal state,
    /// and the in-process sweeper clears that run's memo entries and terminal
    /// record `retention` after termination. When unset (default), no marker is
    /// written and memo entries and terminal records are retained indefinitely.
    #[must_use]
    pub fn memo_retention(mut self, retention: Duration) -> Self {
        self.memo_retention = Some(retention);
        self
    }

    /// Enable content-addressed replay of runner-returned step outcomes.
    ///
    /// When enabled, the runtime writes every [`StepOutcome`] the runner
    /// returns, including `Fail` and `Cancel`, to object storage before
    /// applying it. Step errors ([`StepError`](crate::StepError)) are not
    /// recorded, so retries still invoke the runner. The replay key is scoped
    /// to `(run_id, step_number, SHA-256(step payload))`. If the same step is
    /// delivered again after a crash before ack, the stored outcome is replayed
    /// without invoking the runner again. The record includes the effects
    /// staged through [`Delivery::effects`](crate::Delivery::effects), so a
    /// replayed outcome applies them as well. A replayed
    /// [`StepOutcome::Continue`] with a [`Trigger::After`] delay reduces the
    /// delay by the time already elapsed after the outcome was stored,
    /// preserving the original schedule.
    ///
    /// This is disabled by default because it adds one object-store read per
    /// step delivery (the replay lookup) plus one write per recorded outcome,
    /// and makes that write part of step settlement.
    ///
    /// [`StepOutcome`]: crate::StepOutcome
    /// [`StepOutcome::Continue`]: crate::StepOutcome::Continue
    /// [`Trigger::After`]: crate::Trigger::After
    #[must_use]
    pub fn step_output_replay(mut self) -> Self {
        self.step_output_replay = true;
        self
    }

    /// Override the [`Clock`] the runtime reads its timestamps from. Defaults
    /// to the same clock the [`Queue`] was opened with (via [`Queue::clock`]).
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Remove a [`RunGroup`]'s state (its manifest, member records and the memo
    /// entries and terminal records of its members) `retention` after a
    /// [`RunGroup::results`] consumer observed the last member's termination,
    /// through a sweep when the worker starts and at every poll interval after
    /// that. A group submitted again after that observation is retained until
    /// `retention` after the next observation. When unset (default), no group
    /// terminal marker is written. A group whose results are never consumed is
    /// retained until [`RunGroup::forget`] in either case.
    ///
    /// [`RunGroup`]: crate::RunGroup
    /// [`RunGroup::results`]: crate::RunGroup::results
    /// [`RunGroup::forget`]: crate::RunGroup::forget
    #[must_use]
    pub fn group_retention(mut self, retention: Duration) -> Self {
        self.group_retention = Some(retention);
        self
    }

    /// The queue of the runtime's commands, such as `"workflow-steps.control"`.
    /// The worker applies each job on it as a [`Command`](crate::Command), so
    /// another process submits and cancels runs by enqueuing a command through
    /// a [`taquba::InboxSender`]. The queue must be in
    /// [`taquba::InboxOptions::queues`] and differ from [`Self::queue_name`].
    /// When unset (default), the worker leaves commands unapplied.
    ///
    /// [`WorkflowView::command`](crate::WorkflowView::command) reads the state
    /// of a command from its job, so the [`taquba::QueueConfig`] of the queue
    /// must keep done jobs and dead jobs for at least
    /// [`taquba::InboxOptions::record_retention`]: a
    /// [`keep_done_jobs`](taquba::QueueConfig::keep_done_jobs) of at least the
    /// retention, and a [`dead_retention`](taquba::QueueConfig::dead_retention)
    /// of `None` or at least the retention. A kept done job keeps its payload,
    /// so the store keeps the input of each submission for that retention.
    #[must_use]
    pub fn control_queue(mut self, name: impl Into<String>) -> Self {
        self.control_queue = Some(name.into());
        self
    }

    /// Finalise the builder.
    ///
    /// # Errors
    ///
    /// The error of [`TerminalHook::check_runtime`] for the configured queue
    /// name, and [`Error::InvalidControlQueue`] for a control queue that breaks
    /// a requirement of [`Self::control_queue`].
    pub fn build(self) -> Result<WorkflowRuntime<R>> {
        if let Some(hook) = &self.terminal_hook {
            hook.check_runtime(&self.queue_name)?;
        }
        if let Some(control) = &self.control_queue
            && let Err(reason) = self.check_control_queue(control)
        {
            return Err(Error::InvalidControlQueue {
                queue: control.clone(),
                reason,
            });
        }
        Ok(self.assemble())
    }

    /// The requirement of [`Self::control_queue`] that `control` breaks.
    fn check_control_queue(&self, control: &str) -> std::result::Result<(), ControlQueueError> {
        if control == self.queue_name {
            return Err(ControlQueueError::RuntimeQueue);
        }
        if !self.queue.inbox_accepts(control) {
            return Err(ControlQueueError::NotAccepted);
        }
        let retention = self
            .queue
            .inbox_record_retention()
            .expect("a queue whose inbox accepts a queue has an inbox");
        let config = self.queue.queue_config(control);
        if config.keep_done_jobs.is_none_or(|kept| kept < retention) {
            return Err(ControlQueueError::DoneJobsNotKept { retention });
        }
        if config.dead_retention.is_some_and(|kept| kept < retention) {
            return Err(ControlQueueError::DeadRetentionShort { retention });
        }
        Ok(())
    }

    /// The runtime of this configuration, without the check of the hook.
    pub(crate) fn assemble(self) -> WorkflowRuntime<R> {
        let (runner, core) = self.parts();
        WorkflowRuntime::from_parts(runner, core)
    }

    /// The runner and the core of this configuration.
    fn parts(self) -> (R, Arc<RuntimeCore>) {
        let memo_prefix = self
            .memo_prefix
            .unwrap_or_else(|| format!("{}-memo", self.queue_name));
        let memo_store = MemoStore::new(self.object_store, memo_prefix);
        let group_store = GroupStore::new(memo_store.clone(), self.queue.clone());
        let view = WorkflowView::new(self.queue.view().clone(), memo_store.clone());
        let memo_sweep = self.memo_retention.map(|retention| {
            Arc::new(Sweep::new(
                TERMINAL_KV_PREFIX,
                retention,
                RunStore {
                    memo_store: memo_store.clone(),
                    queue: self.queue.clone(),
                    view: view.clone(),
                },
            ))
        });
        let group_sweep = self.group_retention.map(|retention| {
            Arc::new(Sweep::new(
                GROUP_TERMINAL_KV_PREFIX,
                retention,
                group_store.clone(),
            ))
        });
        let core = RuntimeCore {
            queue: self.queue,
            view,
            queue_name: self.queue_name,
            max_concurrent_steps: self.max_concurrent_steps,
            poll_interval: self.poll_interval,
            memo_store,
            group_store,
            memo_sweep,
            group_sweep,
            step_output_replay: self.step_output_replay,
            clock: self.clock,
            terminal_hook: self.terminal_hook,
            control_queue: self.control_queue,
        };
        (self.runner, Arc::new(core))
    }
}

impl<R: StepRunner> WorkflowRuntime<R> {
    /// Start configuring a runtime. Takes the three required dependencies
    /// (Taquba queue, object store, [`StepRunner`]). Optional fields are set
    /// via [`WorkflowRuntimeBuilder`] methods before [`build`].
    ///
    /// The object store backs [`Delivery::memo`]. It can differ from the store
    /// the [`Queue`] was opened with, though sharing one store is the common
    /// case (a clone of the `Arc`). Use a distinct
    /// [`WorkflowRuntimeBuilder::memo_prefix`] when multiple runtimes share one
    /// store.
    ///
    /// [`Delivery::memo`]: crate::Delivery::memo
    /// [`build`]: WorkflowRuntimeBuilder::build
    pub fn builder(
        queue: Arc<Queue>,
        object_store: Arc<dyn ObjectStore>,
        runner: R,
    ) -> WorkflowRuntimeBuilder<R> {
        let clock = queue.clock();
        WorkflowRuntimeBuilder {
            queue,
            object_store,
            queue_name: DEFAULT_QUEUE_NAME.to_string(),
            memo_prefix: None,
            runner,
            terminal_hook: None,
            max_concurrent_steps: 16,
            poll_interval: Duration::from_millis(250),
            memo_retention: None,
            group_retention: None,
            step_output_replay: false,
            clock,
            control_queue: None,
        }
    }
}

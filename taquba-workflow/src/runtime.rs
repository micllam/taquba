use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use taquba::object_store::ObjectStore;
use taquba::{
    EnqueueOptions, EnqueueResult, JobRecord, PermanentFailure, Queue, Worker, WorkerError,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};

use crate::error::{Error, Result};
use crate::memo::Memo;
use crate::runner::{Step, StepError, StepErrorKind, StepOutcome, StepRunner};
use crate::terminal::{RunOutcome, TerminalHook, TerminalStatus};

/// Header key carrying the run identifier on every step job.
pub const HEADER_RUN_ID: &str = "workflow.run_id";
/// Header key carrying the zero-based step number on every step job.
pub const HEADER_STEP: &str = "workflow.step";
/// Reserved prefix the runtime owns on step-job headers. Submitter-supplied
/// headers must not start with this prefix; if they do, the runtime treats
/// them as its own and strips them before invoking the runner.
pub const RESERVED_HEADER_PREFIX: &str = "workflow.";

const DEDUP_PREFIX: &str = "run:";

/// Prefix for the durable per-run record in Taquba's user KV namespace.
const RUN_KV_PREFIX: &[u8] = b"workflow/runs/";

fn run_kv_key(run_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(RUN_KV_PREFIX.len() + run_id.len());
    k.extend_from_slice(RUN_KV_PREFIX);
    k.extend_from_slice(run_id.as_bytes());
    k
}

/// Durable per-run record written atomically with the step-0 enqueue in
/// [`WorkflowRuntime::submit`] via [`Queue::enqueue_with_kv`]. Carries
/// just enough state to detect duplicate submissions across runtime
/// restarts and to reject re-submissions that change the input;
/// the in-memory registry remains the source of truth for active-run
/// status and cancellation while a runtime is up. Cleaned up in
/// [`RuntimeInner::terminate`] when the run reaches a terminal state.
///
/// `run_id` keeps the record self-describing for ad hoc operator
/// inspection; `submitted_at_ms` is useful for ordering and stale-record
/// auditing; `input_hash` is the SHA-256 of the original `spec.input` and
/// powers the `Error::InputMismatch` check on duplicate submissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableRunRecord {
    run_id: String,
    submitted_at_ms: u64,
    input_hash: [u8; 32],
}

fn hash_input(input: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input);
    hasher.finalize().into()
}

/// Per-step enqueue options the runtime forwards through to Taquba. The
/// runtime always owns `headers` (it injects [`HEADER_RUN_ID`] and
/// [`HEADER_STEP`]) and `dedup_key` (it derives one from
/// `(run_id, step_number)`), so callers only pick the three fields below.
#[derive(Debug, Default)]
struct StepEnqueueOpts {
    /// Earliest claimable time for the step. `None` means immediate.
    run_at: Option<SystemTime>,
    /// Per-step priority override.
    priority: Option<u32>,
    /// Per-step `max_attempts` override.
    max_attempts: Option<u32>,
}

/// Spec passed to [`WorkflowRuntime::submit`].
#[derive(Debug, Clone, Default)]
pub struct RunSpec {
    /// Caller-supplied run identifier. If `None`, the runtime generates a
    /// ULID. The dedup key for the first step job is `run:{run_id}:0`, so
    /// re-submitting the same `run_id` while the run is active returns the
    /// existing job rather than creating a duplicate.
    pub run_id: Option<String>,
    /// Bytes handed to the runner as the first step's payload.
    pub input: Vec<u8>,
    /// Submitter-supplied metadata, threaded through every step of the run
    /// and surfaced to the terminal hook. Reserved `workflow.*` keys are
    /// rejected at submission with [`Error::ReservedHeaderInSubmit`].
    pub headers: HashMap<String, String>,
    /// Override the queue's default priority for every step of this run.
    pub priority: Option<u32>,
    /// Override the queue's `max_attempts` for every step of this run.
    pub max_attempts_per_step: Option<u32>,
}

/// Outcome of [`WorkflowRuntime::submit`].
///
/// `submit` is idempotent on `run_id`: re-submitting an active run is a
/// no-op and the returned `SubmitOutcome` carries `newly_submitted = false`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SubmitOutcome {
    /// The run's identifier (generated if the spec didn't carry one).
    pub run_id: String,
    /// `true` if this call enqueued a new run; `false` if a run with this
    /// id was already active (in this runtime's registry or in the
    /// durable cross-restart record) and this call was a no-op. Call
    /// [`WorkflowRuntime::status`] for the run's current state when
    /// needed.
    pub newly_submitted: bool,
}

/// In-memory status snapshot for an active run. Returned by
/// [`WorkflowRuntime::status`]. Terminal runs are not retained; once the
/// terminal hook fires, the registry entry is removed.
#[derive(Debug, Clone)]
pub struct RunStatus {
    /// The run's identifier.
    pub run_id: String,
    /// Lifecycle state of the run within this runtime process.
    pub state: RunState,
    /// Step number of the most recently observed step.
    pub current_step: u32,
}

/// Lifecycle state tracked in [`RunStatus::state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RunState {
    /// A step job exists in the queue but has not yet been claimed.
    Pending,
    /// A step is currently being processed by a worker.
    Running,
    /// [`WorkflowRuntime::cancel`] was called for this run and the
    /// terminal hook has not yet fired. Reported until the in-flight
    /// step returns and the runtime settles the run as
    /// [`crate::TerminalStatus::Cancelled`] (entry removed and hook
    /// fired); after that, [`WorkflowRuntime::status`] returns `None`.
    ///
    /// Only set by external cancellation. A pure runner-issued
    /// [`crate::StepOutcome::Cancel`] (with no external `cancel()`
    /// call) terminates as `Cancelled` without ever transitioning
    /// through `Cancelling`: the registry only learns the runner's
    /// verdict when `run_step` returns, at which point the entry is
    /// removed.
    Cancelling,
}

/// Builder for [`WorkflowRuntime`].
///
/// Construct via [`WorkflowRuntime::builder`].
pub struct WorkflowRuntimeBuilder<R, H> {
    queue: Arc<Queue>,
    object_store: Arc<dyn ObjectStore>,
    queue_name: String,
    memo_prefix: String,
    runner: R,
    terminal_hook: H,
    max_concurrent_steps: usize,
    poll_interval: Duration,
}

impl<R: StepRunner, H: TerminalHook> WorkflowRuntimeBuilder<R, H> {
    /// The Taquba queue name that step jobs are enqueued onto. Defaults to
    /// `"workflow-steps"`. Multiple runtimes can share a `Queue` handle by
    /// using distinct queue names.
    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = name.into();
        self
    }

    /// The object-store path prefix [`Step::memo`] entries live under.
    /// Defaults to `"workflow-memo"`. Pick a distinct value when multiple
    /// runtimes share an object store, so their memo namespaces don't
    /// collide.
    pub fn memo_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.memo_prefix = prefix.into();
        self
    }

    /// Maximum number of steps processed concurrently in [`WorkflowRuntime::run`].
    /// Defaults to 16.
    pub fn max_concurrent_steps(mut self, n: usize) -> Self {
        assert!(n > 0, "max_concurrent_steps must be at least 1");
        self.max_concurrent_steps = n;
        self
    }

    /// Maximum time the worker loop waits on an empty queue before re-checking.
    /// Defaults to 250ms.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Finalize the builder.
    pub fn build(self) -> WorkflowRuntime<R, H> {
        let memo = Memo::new(self.object_store, self.memo_prefix);
        let inner = RuntimeInner {
            queue: self.queue,
            queue_name: self.queue_name,
            runner: self.runner,
            terminal_hook: self.terminal_hook,
            max_concurrent_steps: self.max_concurrent_steps,
            poll_interval: self.poll_interval,
            registry: Mutex::new(HashMap::new()),
            memo,
        };
        WorkflowRuntime {
            inner: Arc::new(inner),
        }
    }
}

/// Durable runtime for workflow runs. Cheap to clone (internally `Arc`).
pub struct WorkflowRuntime<R, H> {
    inner: Arc<RuntimeInner<R, H>>,
}

impl<R, H> Clone for WorkflowRuntime<R, H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct RuntimeInner<R, H> {
    queue: Arc<Queue>,
    queue_name: String,
    runner: R,
    terminal_hook: H,
    max_concurrent_steps: usize,
    poll_interval: Duration,
    registry: Mutex<HashMap<String, RegistryEntry>>,
    memo: Memo,
}

/// Per-active-run state retained by the runtime. Combines the publicly
/// observable [`RunStatus`] with the in-process state needed to resolve
/// [`WorkflowRuntime::cancel`] races: the Taquba job currently
/// representing the run (so `cancel` can target it), the submitter's
/// headers (so the terminal hook fires with the right metadata even when
/// `cancel` fires it directly from a pending step), a flag for any
/// pending cancellation request, and a [`CancellationToken`] cloned into
/// the in-flight [`Step`] so runners can short-circuit cooperatively.
struct RegistryEntry {
    status: RunStatus,
    current_job_id: String,
    user_headers: HashMap<String, String>,
    cancel_requested: bool,
    cancel_token: CancellationToken,
    /// SHA-256 of the original `spec.input`. `Some` for entries created
    /// by [`WorkflowRuntime::submit`]; `None` for entries created by a
    /// worker resuming a step after restart, which doesn't have access
    /// to the original input. The duplicate-submit check falls through
    /// to the durable record (which always carries the hash) when this
    /// is `None`.
    input_hash: Option<[u8; 32]>,
}

impl<R: StepRunner, H: TerminalHook> WorkflowRuntime<R, H> {
    /// Start configuring a runtime. Takes the four required dependencies
    /// (Taquba queue, object store, [`StepRunner`], [`TerminalHook`]); optional
    /// fields are set via [`WorkflowRuntimeBuilder`] methods before [`build`].
    ///
    /// The object store backs [`Step::memo`]; it does **not** need to be the
    /// same store the [`Queue`] was opened with, though sharing one store is
    /// the common case (just clone the `Arc`). Use a distinct
    /// [`WorkflowRuntimeBuilder::memo_prefix`] when multiple runtimes share
    /// one store.
    ///
    /// Use [`crate::NoopTerminalHook`] if you don't need terminal callbacks.
    ///
    /// [`Step::memo`]: crate::Step::memo
    /// [`build`]: WorkflowRuntimeBuilder::build
    pub fn builder(
        queue: Arc<Queue>,
        object_store: Arc<dyn ObjectStore>,
        runner: R,
        terminal_hook: H,
    ) -> WorkflowRuntimeBuilder<R, H> {
        WorkflowRuntimeBuilder {
            queue,
            object_store,
            queue_name: "workflow-steps".to_string(),
            memo_prefix: "workflow-memo".to_string(),
            runner,
            terminal_hook,
            max_concurrent_steps: 16,
            poll_interval: Duration::from_millis(250),
        }
    }

    /// Submit a new run. Enqueues step 0 with payload `spec.input`.
    ///
    /// Idempotent on `(run_id, spec.input)`: if a run with the same id is
    /// already active (either in this runtime's in-memory registry or in
    /// the durable cross-restart record written to Taquba's user KV
    /// namespace) and `spec.input` matches the original submission, this
    /// call is a no-op and the returned [`SubmitOutcome`] has
    /// `newly_submitted = false`. A re-submission of an active `run_id`
    /// with a *different* input is rejected with [`Error::InputMismatch`];
    /// pick a fresh `run_id` for a new run.
    #[instrument(skip(self, spec), fields(run_id))]
    pub async fn submit(&self, spec: RunSpec) -> Result<SubmitOutcome> {
        let run_id = spec.run_id.unwrap_or_else(|| ulid::Ulid::new().to_string());
        tracing::Span::current().record("run_id", run_id.as_str());

        for k in spec.headers.keys() {
            if k.starts_with(RESERVED_HEADER_PREFIX) {
                return Err(Error::ReservedHeaderInSubmit(k.clone()));
            }
        }

        let input_hash = hash_input(&spec.input);

        // Hold the registry lock across enqueue so two concurrent submits
        // with the same `run_id` can't both pass the duplicate check before
        // either commits. Submission is not on a hot path; queue I/O
        // latency here is acceptable.
        let mut registry = self.inner.registry.lock().await;
        if let Some(entry) = registry.get(&run_id) {
            // Worker-resumed entries have no stored hash; fall through to
            // the durable-record check below, which always carries it.
            if let Some(existing) = entry.input_hash {
                if existing != input_hash {
                    return Err(Error::InputMismatch(run_id));
                }
                return Ok(SubmitOutcome {
                    run_id,
                    newly_submitted: false,
                });
            }
        }

        // Cross-restart duplicate check. The registry lock above closes
        // the in-process race window; this read closes the across-restart
        // one (same queue, fresh runtime).
        if let Some(bytes) = self.inner.queue.kv_get(&run_kv_key(&run_id)).await? {
            let existing: DurableRunRecord =
                rmp_serde::from_slice(&bytes).map_err(taquba::Error::from)?;
            if existing.input_hash != input_hash {
                return Err(Error::InputMismatch(run_id));
            }
            return Ok(SubmitOutcome {
                run_id,
                newly_submitted: false,
            });
        }

        let mut headers = spec.headers.clone();
        headers.insert(HEADER_RUN_ID.to_string(), run_id.clone());
        headers.insert(HEADER_STEP.to_string(), "0".to_string());
        let enqueue_opts = EnqueueOptions {
            headers,
            run_at: None,
            priority: spec.priority,
            max_attempts: spec.max_attempts_per_step,
            dedup_key: Some(format!("{DEDUP_PREFIX}{run_id}:0")),
            ..EnqueueOptions::default()
        };

        let record_bytes = rmp_serde::to_vec_named(&DurableRunRecord {
            run_id: run_id.clone(),
            submitted_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            input_hash,
        })
        .map_err(taquba::Error::from)?;
        let kv = HashMap::from([(run_kv_key(&run_id), record_bytes)]);

        let job_id = match self
            .inner
            .queue
            .enqueue_with_kv(&self.inner.queue_name, spec.input, enqueue_opts, kv)
            .await?
        {
            EnqueueResult::New(id) => id,
            // A dedup_key hit without our durable record means either
            // another writer beat us, or a prior run on `(run_id, step 0)`
            // released its dedup key (job claimed) but the durable record
            // is missing, which only happens if the run terminated
            // without going through `terminate`. Either way the safe
            // verdict is duplicate.
            EnqueueResult::AlreadyEnqueued(_) => {
                return Ok(SubmitOutcome {
                    run_id,
                    newly_submitted: false,
                });
            }
        };

        registry.insert(
            run_id.clone(),
            RegistryEntry {
                status: RunStatus {
                    run_id: run_id.clone(),
                    state: RunState::Pending,
                    current_step: 0,
                },
                current_job_id: job_id.clone(),
                user_headers: spec.headers.clone(),
                cancel_requested: false,
                cancel_token: CancellationToken::new(),
                input_hash: Some(input_hash),
            },
        );
        drop(registry);

        debug!(run_id = %run_id, job_id = %job_id, "run submitted");
        Ok(SubmitOutcome {
            run_id,
            newly_submitted: true,
        })
    }

    /// Look up the in-process status of a run. Returns `None` for unknown or
    /// already-terminated runs (the registry only retains active runs).
    ///
    /// Returns [`RunState::Cancelling`] for any run with a pending
    /// cancellation request, regardless of its underlying step lifecycle
    /// position; the cancellation overlay wins over `Pending`/`Running`
    /// until the terminal hook fires.
    pub async fn status(&self, run_id: &str) -> Option<RunStatus> {
        self.inner.registry.lock().await.get(run_id).map(|e| {
            let mut status = e.status.clone();
            if e.cancel_requested {
                status.state = RunState::Cancelling;
            }
            status
        })
    }

    /// Request cancellation of an active run.
    ///
    /// Returns `Ok(true)` if a cancellation was initiated for `run_id`, or
    /// `Ok(false)` if the run is not active in this runtime (already
    /// terminal, never submitted here, or owned by a different runtime
    /// instance).
    ///
    /// The terminal hook fires once with [`TerminalStatus::Cancelled`]:
    ///
    /// - **Pending / scheduled step**: the queued step job is cancelled in
    ///   Taquba and the hook fires from this call before it returns.
    /// - **Running step**: cancellation is delivered to the runner via
    ///   [`Step::cancel_token`]; runners that watch the token short-circuit
    ///   immediately. Runners that ignore the token are allowed to run to
    ///   completion (futures cannot be safely aborted mid-step). In both
    ///   cases the runner's [`StepOutcome`] / [`StepError`] is discarded
    ///   and the hook fires from the worker once the step returns, with
    ///   any pending transient retry suppressed and the step acked rather
    ///   than nacked.
    ///
    /// Cancellation is best-effort: if the run is already terminal by the
    /// time `cancel` is called (either because the runner returned a
    /// terminating [`StepOutcome`] or a prior `cancel` already settled
    /// it), `cancel` returns `Ok(false)`, the run keeps whatever terminal
    /// outcome it already delivered, and no additional hook fires.
    pub async fn cancel(&self, run_id: &str) -> Result<bool> {
        let (job_id, headers, current_step) = {
            let mut registry = self.inner.registry.lock().await;
            let Some(entry) = registry.get_mut(run_id) else {
                return Ok(false);
            };
            entry.cancel_requested = true;
            // Signal cooperative cancellation. Idempotent on
            // `CancellationToken`: a second `cancel()` is a no-op. Runners
            // that watch `step.cancel_token` can short-circuit; runners
            // that ignore it still get terminated by the worker via the
            // `cancel_requested` flag after `run_step` returns.
            entry.cancel_token.cancel();
            (
                entry.current_job_id.clone(),
                entry.user_headers.clone(),
                entry.status.current_step,
            )
        };

        match self.inner.queue.cancel(&job_id).await? {
            taquba::CancelOutcome::Removed => {
                // Job was Pending/Scheduled and is now removed; no worker
                // will ever see it. Fire the hook here. `error` is `None`:
                // external cancellation carries no reason at the API level.
                self.inner
                    .terminate(RunOutcome {
                        run_id: run_id.to_string(),
                        status: TerminalStatus::Cancelled,
                        result: None,
                        error: None,
                        headers,
                        final_step: current_step,
                    })
                    .await;
            }
            taquba::CancelOutcome::Requested => {
                // Worker is processing the step. The worker reads our own
                // registry `cancel_requested` flag after `run_step` returns
                // and fires the hook.
            }
            taquba::CancelOutcome::NotFound => {
                // Job already gone from Taquba (e.g. just acked between our
                // registry read and the queue call). The worker path still
                // honours our `cancel_requested` flag if it hasn't fired the
                // hook yet; if it has, this cancel is a no-op past the
                // registry update.
            }
        }
        Ok(true)
    }

    /// Drive the step worker loop until `shutdown` resolves. Spawns up to
    /// `max_concurrent_steps` step processors and drains them on shutdown.
    pub async fn run<F>(&self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
        R: 'static,
        H: 'static,
    {
        let worker = Arc::new(StepWorker {
            inner: self.inner.clone(),
        });
        taquba::run_worker_concurrent(
            &self.inner.queue,
            &self.inner.queue_name,
            worker,
            self.inner.max_concurrent_steps,
            self.inner.poll_interval,
            shutdown,
        )
        .await?;
        Ok(())
    }
}

struct StepWorker<R, H> {
    inner: Arc<RuntimeInner<R, H>>,
}

impl<R: StepRunner + 'static, H: TerminalHook + 'static> Worker for StepWorker<R, H> {
    async fn process(&self, job: &JobRecord) -> std::result::Result<(), WorkerError> {
        self.inner.process_step(job).await
    }
}

impl<R: StepRunner, H: TerminalHook> RuntimeInner<R, H> {
    async fn enqueue_step(
        &self,
        run_id: &str,
        step_number: u32,
        payload: Vec<u8>,
        user_headers: &HashMap<String, String>,
        opts: StepEnqueueOpts,
    ) -> Result<String> {
        let mut headers = user_headers.clone();
        headers.insert(HEADER_RUN_ID.to_string(), run_id.to_string());
        headers.insert(HEADER_STEP.to_string(), step_number.to_string());

        let enqueue_opts = EnqueueOptions {
            headers,
            run_at: opts.run_at,
            priority: opts.priority,
            max_attempts: opts.max_attempts,
            dedup_key: Some(format!("{DEDUP_PREFIX}{run_id}:{step_number}")),
            ..EnqueueOptions::default()
        };
        Ok(self
            .queue
            .enqueue_with(&self.queue_name, payload, enqueue_opts)
            .await?)
    }

    fn split_headers(headers: &HashMap<String, String>) -> HashMap<String, String> {
        headers
            .iter()
            .filter(|(k, _)| !k.starts_with(RESERVED_HEADER_PREFIX))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn parse_step_headers(job: &JobRecord) -> std::result::Result<(String, u32), Error> {
        let run_id = job
            .headers
            .get(HEADER_RUN_ID)
            .ok_or(Error::MissingHeader(HEADER_RUN_ID))?
            .to_string();
        let step_str = job
            .headers
            .get(HEADER_STEP)
            .ok_or(Error::MissingHeader(HEADER_STEP))?;
        let step_number: u32 = step_str.parse().map_err(|_| Error::InvalidStepHeader {
            header: HEADER_STEP,
            value: step_str.clone(),
        })?;
        Ok((run_id, step_number))
    }

    /// Settle a run into its terminal state: drop its registry entry,
    /// delete the durable run record from Taquba's KV namespace, and
    /// fire the terminal hook. Registry removal happens first so that
    /// [`WorkflowRuntime::status`] doesn't briefly report an
    /// already-terminated run as active while a slow hook (e.g. a webhook
    /// delivery) is in flight. KV cleanup is best-effort: a transient
    /// failure here leaves a stale durable record that will block a
    /// future submit with the same `run_id`, but does not affect the
    /// already-running cleanup of *this* run.
    async fn terminate(&self, outcome: RunOutcome) {
        self.registry.lock().await.remove(&outcome.run_id);
        if let Err(err) = self.queue.kv_delete(&run_kv_key(&outcome.run_id)).await {
            warn!(
                run_id = %outcome.run_id,
                "failed to clear durable run record: {err}"
            );
        }
        self.terminal_hook.on_termination(&outcome).await;
    }

    /// Transition the entry for `run_id` into [`RunState::Running`] for
    /// `step_number`, recording the Taquba job ID powering the step so a
    /// concurrent [`WorkflowRuntime::cancel`] can target it. Creates a
    /// fresh entry if the run is unknown to this runtime (e.g. after a
    /// restart on another runtime, where the worker first learns of the
    /// run by claiming its step). Returns the entry's
    /// [`CancellationToken`] for cloning into the in-flight [`Step`].
    async fn registry_mark_running(
        &self,
        run_id: &str,
        step_number: u32,
        job_id: &str,
        user_headers: &HashMap<String, String>,
    ) -> CancellationToken {
        let mut registry = self.registry.lock().await;
        match registry.get_mut(run_id) {
            Some(entry) => {
                entry.status.state = RunState::Running;
                entry.status.current_step = step_number;
                entry.current_job_id = job_id.to_string();
                entry.cancel_token.clone()
            }
            None => {
                let cancel_token = CancellationToken::new();
                registry.insert(
                    run_id.to_string(),
                    RegistryEntry {
                        status: RunStatus {
                            run_id: run_id.to_string(),
                            state: RunState::Running,
                            current_step: step_number,
                        },
                        current_job_id: job_id.to_string(),
                        user_headers: user_headers.clone(),
                        cancel_requested: false,
                        cancel_token: cancel_token.clone(),
                        input_hash: None,
                    },
                );
                cancel_token
            }
        }
    }

    async fn process_step(&self, job: &JobRecord) -> std::result::Result<(), WorkerError> {
        let (run_id, step_number) = match Self::parse_step_headers(job) {
            Ok(v) => v,
            Err(e) => {
                warn!(job_id = %job.id, error = %e, "workflow step has malformed headers");
                if e.is_permanent() {
                    return Err(PermanentFailure::new(e.to_string()).into());
                }
                return Err(e.to_string().into());
            }
        };

        let user_headers = Self::split_headers(&job.headers);

        let cancel_token = self
            .registry_mark_running(&run_id, step_number, &job.id, &user_headers)
            .await;

        let step = Step {
            run_id: run_id.clone(),
            step_number,
            payload: job.payload.clone(),
            headers: user_headers.clone(),
            job_id: job.id.clone(),
            attempts: job.attempts,
            cancel_token,
            memo: self.memo.clone(),
        };

        // Preserve the run's per-step priority and max_attempts across the
        // boundary by re-using the values from the just-processed job.
        let inherit_opts = || StepEnqueueOpts {
            run_at: None,
            priority: Some(job.priority),
            max_attempts: Some(job.max_attempts),
        };

        let outcome = self.runner.run_step(&step).await;
        let external_cancel = self
            .registry
            .lock()
            .await
            .get(&run_id)
            .is_some_and(|e| e.cancel_requested);

        // Cancellation precedence:
        // 1. A runner-issued `StepOutcome::Cancel` wins (it carries an
        //    in-step reason that we surface on `RunOutcome::error`).
        // 2. Otherwise an external `WorkflowRuntime::cancel` overrides
        //    whatever outcome the runner returned (including transient
        //    retries and permanent dead-letters), with `error: None` so
        //    consumers can distinguish external vs. runner-issued cancel.
        match outcome {
            Ok(StepOutcome::Cancel { reason }) => {
                self.terminate(RunOutcome {
                    run_id: run_id.clone(),
                    status: TerminalStatus::Cancelled,
                    result: None,
                    error: Some(reason),
                    headers: user_headers,
                    final_step: step_number,
                })
                .await;
                Ok(())
            }
            _ if external_cancel => {
                self.terminate(RunOutcome {
                    run_id: run_id.clone(),
                    status: TerminalStatus::Cancelled,
                    result: None,
                    error: None,
                    headers: user_headers,
                    final_step: step_number,
                })
                .await;
                Ok(())
            }
            Ok(StepOutcome::Continue { payload }) => {
                self.advance(
                    &run_id,
                    step_number + 1,
                    payload,
                    &user_headers,
                    inherit_opts(),
                )
                .await
            }
            Ok(StepOutcome::ContinueAfter { payload, delay }) => {
                let opts = StepEnqueueOpts {
                    run_at: Some(SystemTime::now() + delay),
                    ..inherit_opts()
                };
                self.advance(&run_id, step_number + 1, payload, &user_headers, opts)
                    .await
            }
            Ok(StepOutcome::Succeed { result }) => {
                self.terminate(RunOutcome {
                    run_id: run_id.clone(),
                    status: TerminalStatus::Succeeded,
                    result: Some(result),
                    error: None,
                    headers: user_headers,
                    final_step: step_number,
                })
                .await;
                Ok(())
            }
            Ok(StepOutcome::Fail { reason }) => {
                // Runner verdict: workflow failed but the step itself ran
                // cleanly. Ack the step (no dead-letter) and fire the hook
                // with `Failed`.
                self.terminate(RunOutcome {
                    run_id: run_id.clone(),
                    status: TerminalStatus::Failed,
                    result: None,
                    error: Some(reason),
                    headers: user_headers,
                    final_step: step_number,
                })
                .await;
                Ok(())
            }
            Err(StepError {
                message,
                kind: StepErrorKind::Permanent,
            }) => {
                self.terminate(RunOutcome {
                    run_id: run_id.clone(),
                    status: TerminalStatus::Failed,
                    result: None,
                    error: Some(message.clone()),
                    headers: user_headers,
                    final_step: step_number,
                })
                .await;
                Err(PermanentFailure::new(message).into())
            }
            Err(StepError {
                message,
                kind: StepErrorKind::Transient,
            }) => {
                // Last attempt: this nack will dead-letter. Fire the failure
                // hook now so the user is notified once, before the job
                // record disappears from the registry.
                if job.attempts >= job.max_attempts {
                    self.terminate(RunOutcome {
                        run_id: run_id.clone(),
                        status: TerminalStatus::Failed,
                        result: None,
                        error: Some(message.clone()),
                        headers: user_headers,
                        final_step: step_number,
                    })
                    .await;
                }
                Err(message.into())
            }
        }
    }

    async fn advance(
        &self,
        run_id: &str,
        next_step: u32,
        payload: Vec<u8>,
        user_headers: &HashMap<String, String>,
        opts: StepEnqueueOpts,
    ) -> std::result::Result<(), WorkerError> {
        match self
            .enqueue_step(run_id, next_step, payload, user_headers, opts)
            .await
        {
            Ok(new_job_id) => {
                // Make sure to preserve `cancel_requested`.
                if let Some(entry) = self.registry.lock().await.get_mut(run_id) {
                    entry.status.state = RunState::Pending;
                    entry.status.current_step = next_step;
                    entry.current_job_id = new_job_id;
                }
                Ok(())
            }
            // Transient: the runner already executed for this step; failing
            // the worker triggers a retry of the same step. The runner must be
            // idempotent for `(run_id, step_number)`.
            Err(e) => Err(e.to_string().into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terminal::NoopTerminalHook;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    use taquba::object_store::memory::InMemory;
    use taquba::{OpenOptions, QueueConfig};
    use tokio::sync::oneshot;

    /// Recording terminal hook backed by an mpsc channel.
    struct ChannelHook {
        tx: tokio::sync::mpsc::UnboundedSender<RunOutcome>,
    }

    impl TerminalHook for ChannelHook {
        async fn on_termination(&self, outcome: &RunOutcome) {
            let _ = self.tx.send(outcome.clone());
        }
    }

    /// Runner that executes a fixed list of step outcomes in order.
    struct ScriptedRunner {
        script: Arc<StdMutex<Vec<StepOutcome>>>,
    }

    impl ScriptedRunner {
        fn new(steps: Vec<StepOutcome>) -> Self {
            Self {
                script: Arc::new(StdMutex::new(steps)),
            }
        }
    }

    impl StepRunner for ScriptedRunner {
        async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
            let next = self.script.lock().unwrap().remove(0);
            Ok(next)
        }
    }

    async fn fresh_queue() -> (Arc<Queue>, Arc<dyn taquba::object_store::ObjectStore>) {
        let store: Arc<dyn taquba::object_store::ObjectStore> = Arc::new(InMemory::new());
        let queue = Arc::new(Queue::open(store.clone(), "test").await.unwrap());
        (queue, store)
    }

    /// Queue with zero retry backoff and a tight reaper, so multi-attempt
    /// tests run in well under a second.
    async fn fresh_queue_fast_retry() -> (Arc<Queue>, Arc<dyn taquba::object_store::ObjectStore>) {
        let opts = OpenOptions {
            default_queue_config: QueueConfig {
                retry_backoff_base: Duration::ZERO,
                ..QueueConfig::default()
            },
            reaper_interval: Duration::from_millis(50),
            scheduler_interval: Duration::from_millis(50),
            ..OpenOptions::default()
        };
        let store: Arc<dyn taquba::object_store::ObjectStore> = Arc::new(InMemory::new());
        let queue = Arc::new(
            Queue::open_with_options(store.clone(), "test", opts)
                .await
                .unwrap(),
        );
        (queue, store)
    }

    fn spawn_runtime<R, H>(runtime: WorkflowRuntime<R, H>) -> oneshot::Sender<()>
    where
        R: StepRunner + 'static,
        H: TerminalHook + 'static,
    {
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = runtime
                .run(async move {
                    let _ = rx.await;
                })
                .await;
        });
        tx
    }

    #[tokio::test(start_paused = true)]
    async fn single_step_succeeds_and_fires_hook() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![StepOutcome::Succeed {
                result: b"done".to_vec(),
            }]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"in".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(outcome.result.as_deref(), Some(b"done".as_slice()));
        assert_eq!(outcome.final_step, 0);
        assert!(runtime.status(&handle.run_id).await.is_none());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn multi_step_run_advances_through_continue() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![
                StepOutcome::Continue {
                    payload: b"step1".to_vec(),
                },
                StepOutcome::Continue {
                    payload: b"step2".to_vec(),
                },
                StepOutcome::Succeed {
                    result: b"final".to_vec(),
                },
            ]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"start".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.final_step, 2);
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(outcome.result.as_deref(), Some(b"final".as_slice()));

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_failure_dead_letters_and_fires_hook() {
        struct FailingRunner;
        impl StepRunner for FailingRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                Err(StepError::permanent("nope"))
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            FailingRunner,
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Failed);
        assert_eq!(outcome.error.as_deref(), Some("nope"));
        assert!(runtime.status(&handle.run_id).await.is_none());

        // Permanent runner errors *do* dead-letter the step.
        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 1, "permanent error should dead-letter");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn fail_outcome_terminates_run_without_dead_letter() {
        // StepOutcome::Fail is the runner's *verdict* path, not an
        // infrastructure error: the hook fires with Failed, the registry
        // entry is cleaned up, but the step is acked normally so no dead
        // job is left behind for operators to inspect.
        struct VerdictRunner;
        impl StepRunner for VerdictRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                Ok(StepOutcome::Fail {
                    reason: "agent declined the task".to_string(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            VerdictRunner,
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Failed);
        assert_eq!(outcome.error.as_deref(), Some("agent declined the task"));
        assert!(runtime.status(&handle.run_id).await.is_none());

        // Crucially: no dead-letter, distinguishing runner verdict from
        // infrastructure failure at the queue level.
        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0, "Fail verdict must not dead-letter");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_submit_in_process_with_same_input_is_idempotent() {
        // Pause forever on the first step so the run stays active in the
        // registry while we attempt the duplicate submit.
        struct PauseRunner;
        impl StepRunner for PauseRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                std::future::pending().await
            }
        }

        let (queue, store) = fresh_queue().await;
        let runtime =
            WorkflowRuntime::builder(queue, store.clone(), PauseRunner, NoopTerminalHook).build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                run_id: Some("fixed-id".to_string()),
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        // Wait for the worker to start the step so the registry observes the
        // run as Running (or at least Pending).
        for _ in 0..40 {
            if runtime.status(&handle.run_id).await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(runtime.status(&handle.run_id).await.is_some());

        let outcome = runtime
            .submit(RunSpec {
                run_id: Some("fixed-id".to_string()),
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.run_id, "fixed-id");
        assert!(!outcome.newly_submitted);

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_submit_in_process_with_different_input_errors() {
        struct PauseRunner;
        impl StepRunner for PauseRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                std::future::pending().await
            }
        }

        let (queue, store) = fresh_queue().await;
        let runtime =
            WorkflowRuntime::builder(queue, store.clone(), PauseRunner, NoopTerminalHook).build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                run_id: Some("fixed-id".to_string()),
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let err = runtime
            .submit(RunSpec {
                run_id: Some("fixed-id".to_string()),
                input: b"y".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(&err, Error::InputMismatch(id) if id == "fixed-id"));
        assert!(err.is_permanent());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_submit_across_runtime_restart_with_same_input_is_idempotent() {
        // Build a runtime, submit a run, then drop the runtime entirely
        // (simulating a process restart of the workflow layer) while
        // keeping the underlying Queue alive. The next runtime instance
        // sees a fresh in-memory registry but must still treat a
        // re-submit as idempotent because the durable run record persists
        // through the enqueue_with_kv path.
        struct PauseRunner;
        impl StepRunner for PauseRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                std::future::pending().await
            }
        }

        let (queue, store) = fresh_queue().await;

        // Submit via the first runtime, drop it without starting its
        // worker loop or going terminal.
        {
            let runtime = WorkflowRuntime::builder(
                queue.clone(),
                store.clone(),
                PauseRunner,
                NoopTerminalHook,
            )
            .build();
            runtime
                .submit(RunSpec {
                    run_id: Some("durable-id".to_string()),
                    input: b"x".to_vec(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        // The durable record is queryable independently of any runtime.
        assert!(
            queue
                .kv_get(&run_kv_key("durable-id"))
                .await
                .unwrap()
                .is_some(),
            "durable run record must persist past runtime drop"
        );

        // Fresh runtime, same queue. The registry is empty here, so the
        // duplicate verdict can only come from the durable KV record.
        let runtime2 =
            WorkflowRuntime::builder(queue.clone(), store.clone(), PauseRunner, NoopTerminalHook)
                .build();
        let outcome = runtime2
            .submit(RunSpec {
                run_id: Some("durable-id".to_string()),
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.run_id, "durable-id");
        assert!(!outcome.newly_submitted);
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_submit_across_runtime_restart_with_different_input_errors() {
        // Like the same-input idempotency test, but the re-submit carries
        // a different input. The check is sourced exclusively from the
        // durable KV record since the fresh runtime's registry is empty.
        struct PauseRunner;
        impl StepRunner for PauseRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                std::future::pending().await
            }
        }

        let (queue, store) = fresh_queue().await;

        {
            let runtime = WorkflowRuntime::builder(
                queue.clone(),
                store.clone(),
                PauseRunner,
                NoopTerminalHook,
            )
            .build();
            runtime
                .submit(RunSpec {
                    run_id: Some("durable-id".to_string()),
                    input: b"x".to_vec(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        let runtime2 =
            WorkflowRuntime::builder(queue.clone(), store.clone(), PauseRunner, NoopTerminalHook)
                .build();
        let err = runtime2
            .submit(RunSpec {
                run_id: Some("durable-id".to_string()),
                input: b"y".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(&err, Error::InputMismatch(id) if id == "durable-id"));
    }

    #[tokio::test(start_paused = true)]
    async fn reserved_header_on_submit_is_rejected() {
        let (queue, store) = fresh_queue().await;
        let runtime: WorkflowRuntime<ScriptedRunner, NoopTerminalHook> = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![]),
            NoopTerminalHook,
        )
        .build();
        let mut headers = HashMap::new();
        headers.insert("workflow.run_id".to_string(), "evil".to_string());

        let err = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                headers,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::ReservedHeaderInSubmit(k) if k == "workflow.run_id"),
            "got: {err:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn user_headers_thread_through_to_terminal_hook() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![
                StepOutcome::Continue { payload: vec![] },
                StepOutcome::Succeed { result: vec![] },
            ]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let mut headers = HashMap::new();
        headers.insert("trace_id".to_string(), "abc-123".to_string());
        headers.insert("tenant".to_string(), "acme".to_string());

        runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                headers,
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(outcome.headers.get("trace_id").unwrap(), "abc-123");
        assert_eq!(outcome.headers.get("tenant").unwrap(), "acme");
        // Reserved keys must not leak through.
        assert!(!outcome.headers.contains_key(HEADER_RUN_ID));
        assert!(!outcome.headers.contains_key(HEADER_STEP));

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn restart_resumes_at_next_step() {
        // Headline durability test: after step 0 has acked and step 1 is in
        // the queue, kill runtime A entirely and spawn runtime B on the same
        // Queue handle. B should claim and complete step 1 without re-running
        // step 0.
        //
        // To make this race-free we gate step 0's runner: the test holds the
        // gate while signalling shutdown to A so A enters drain mode without
        // ever claiming step 1. Then the gate is opened, A's spawned step-0
        // task finishes (enqueueing step 1 + acking step 0) and A exits.
        struct GatedRunner {
            gate: tokio::sync::Mutex<Option<oneshot::Receiver<Vec<u8>>>>,
        }

        impl StepRunner for GatedRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                match step.step_number {
                    0 => {
                        let rx = self.gate.lock().await.take().expect("gate consumed twice");
                        let payload = rx.await.expect("gate sender dropped");
                        Ok(StepOutcome::Continue { payload })
                    }
                    _ => std::future::pending().await,
                }
            }
        }

        struct CompleteOnStep1;
        impl StepRunner for CompleteOnStep1 {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                assert_eq!(step.step_number, 1, "runtime B should only ever see step 1");
                assert_eq!(step.payload.as_slice(), b"step1-payload");
                Ok(StepOutcome::Succeed {
                    result: b"resumed".to_vec(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;

        let (gate_tx, gate_rx) = oneshot::channel::<Vec<u8>>();
        let runtime_a = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            GatedRunner {
                gate: tokio::sync::Mutex::new(Some(gate_rx)),
            },
            NoopTerminalHook,
        )
        .max_concurrent_steps(1)
        .build();

        let (shutdown_a_tx, shutdown_a_rx) = oneshot::channel::<()>();
        let worker_a = {
            let runtime_a = runtime_a.clone();
            tokio::spawn(async move {
                let _ = runtime_a
                    .run(async move {
                        let _ = shutdown_a_rx.await;
                    })
                    .await;
            })
        };

        let handle = runtime_a
            .submit(RunSpec {
                input: b"input".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Wait for runtime A to claim step 0 and reach the gate (registry
        // shows Running for step 0).
        for _ in 0..80 {
            if let Some(s) = runtime_a.status(&handle.run_id).await {
                if s.state == RunState::Running && s.current_step == 0 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let s = runtime_a.status(&handle.run_id).await.expect("status");
        assert_eq!(s.state, RunState::Running);
        assert_eq!(s.current_step, 0);

        // A's worker is in the at-capacity select-loop. Signal shutdown
        // first, then open the gate so step 0 finishes processing inside
        // drain mode (A will not claim step 1).
        let _ = shutdown_a_tx.send(());
        let _ = gate_tx.send(b"step1-payload".to_vec());

        worker_a.await.expect("runtime A drained cleanly");

        // Bring up runtime B on the same Queue handle. It should pick up
        // step 1 from where A left off.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime_b =
            WorkflowRuntime::builder(queue, store.clone(), CompleteOnStep1, ChannelHook { tx })
                .build();
        let shutdown_b = spawn_runtime(runtime_b.clone());

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(outcome.result.as_deref(), Some(b"resumed".as_slice()));
        assert_eq!(outcome.final_step, 1);

        let _ = shutdown_b.send(());
    }

    /// Submits a run whose runner always returns
    /// [`StepError::transient`], capped at `max_attempts`. Asserts the
    /// runner is invoked exactly `max_attempts` times (per-step max-attempts
    /// propagation) and that the terminal hook fires Failed exactly once on
    /// the final attempt (fire-once-on-last-attempt logic).
    async fn assert_transient_retries_until_max(max_attempts: u32) {
        struct AlwaysTransient {
            calls: Arc<AtomicU32>,
        }
        impl StepRunner for AlwaysTransient {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err(StepError::transient("flaky"))
            }
        }

        let (queue, store) = fresh_queue_fast_retry().await;
        let calls = Arc::new(AtomicU32::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            AlwaysTransient {
                calls: calls.clone(),
            },
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                max_attempts_per_step: Some(max_attempts),
                ..Default::default()
            })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");

        assert_eq!(outcome.status, TerminalStatus::Failed);
        assert_eq!(outcome.error.as_deref(), Some("flaky"));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            max_attempts,
            "runner called once per attempt up to max_attempts"
        );

        // Settle window: assert no duplicate hook fires after the terminal one.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "hook fired more than once");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_outcome_terminates_run_without_dead_letter() {
        // `StepOutcome::Cancel` is the runner's cancellation verdict path:
        // the hook fires with Cancelled, the registry is cleaned up, the
        // step is acked, and no dead job is left behind.
        struct CancellingRunner;
        impl StepRunner for CancellingRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                Ok(StepOutcome::Cancel {
                    reason: "upstream aborted".to_string(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            CancellingRunner,
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert_eq!(outcome.error.as_deref(), Some("upstream aborted"));
        assert!(runtime.status(&handle.run_id).await.is_none());

        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0, "Cancel verdict must not dead-letter");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_pending_run_fires_cancelled_hook() {
        // Pending case: a run sits in the queue, we call `cancel()` before
        // any worker claims it. The hook fires from `cancel` itself.
        struct UnreachableRunner;
        impl StepRunner for UnreachableRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                unreachable!("worker must not claim the cancelled step");
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            UnreachableRunner,
            ChannelHook { tx },
        )
        .build();
        // Note: deliberately do NOT spawn the worker loop, so the submitted
        // step stays Pending in the queue while we cancel it.

        let mut headers = HashMap::new();
        headers.insert("tenant".to_string(), "acme".to_string());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                headers,
                ..Default::default()
            })
            .await
            .unwrap();
        let status = runtime.status(&handle.run_id).await.expect("active");
        assert_eq!(status.state, RunState::Pending);

        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(was_cancelled);

        let outcome = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");
        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        // External cancellation carries no reason: `error` is `None`.
        assert!(outcome.error.is_none());
        assert_eq!(outcome.headers.get("tenant").unwrap(), "acme");
        assert!(runtime.status(&handle.run_id).await.is_none());

        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0, "cancel must not dead-letter");
        assert_eq!(stats.pending, 0, "cancelled job must be removed");
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_during_running_step_overrides_outcome() {
        // Running case: the step is in-flight when cancel is called. The
        // runner's eventual outcome is discarded; the worker fires Cancelled.
        struct GatedRunner {
            claimed: Arc<tokio::sync::Notify>,
            gate: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
        }
        impl StepRunner for GatedRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.claimed.notify_one();
                let rx = self.gate.lock().await.take().expect("gate consumed twice");
                let _ = rx.await;
                // The runner "successfully completes" the step, but cancel
                // was requested mid-flight so the outcome should be ignored
                // and the hook should fire Cancelled instead.
                Ok(StepOutcome::Succeed {
                    result: b"would-have-succeeded".to_vec(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let claimed = Arc::new(tokio::sync::Notify::new());
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let (hook_tx, mut hook_rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            GatedRunner {
                claimed: claimed.clone(),
                gate: tokio::sync::Mutex::new(Some(gate_rx)),
            },
            ChannelHook { tx: hook_tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), claimed.notified())
            .await
            .expect("runner reached gate");

        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(was_cancelled);

        // Let the runner finish. The worker should observe `cancel_requested`
        // and fire Cancelled rather than advancing or firing Succeeded.
        let _ = gate_tx.send(());

        let outcome = tokio::time::timeout(Duration::from_secs(2), hook_rx.recv())
            .await
            .expect("hook fired")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert!(
            outcome.result.is_none(),
            "succeed payload must be discarded"
        );
        assert!(runtime.status(&handle.run_id).await.is_none());

        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0);

        let _ = shutdown.send(());
    }

    /// Drive a single step that blocks on a gate, calls `cancel(run_id)`
    /// while the step is in-flight, and then has the runner return the
    /// supplied error. Asserts that external cancellation suppresses the
    /// error path entirely: the hook fires `Cancelled` (not `Failed`),
    /// no dead-letter is produced regardless of `permanent`/`transient`,
    /// and the worker returns `Ok` (no retry, no PermanentFailure
    /// propagation).
    async fn assert_cancel_suppresses_runner_error(error: StepError) {
        struct GatedErrRunner {
            claimed: Arc<tokio::sync::Notify>,
            gate: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
            calls: Arc<AtomicU32>,
            error: StdMutex<Option<StepError>>,
        }
        impl StepRunner for GatedErrRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.claimed.notify_one();
                let rx = self.gate.lock().await.take().expect("gate consumed twice");
                let _ = rx.await;
                Err(self
                    .error
                    .lock()
                    .unwrap()
                    .take()
                    .expect("error consumed twice"))
            }
        }

        let (queue, store) = fresh_queue_fast_retry().await;
        let claimed = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicU32::new(0));
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let (hook_tx, mut hook_rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            GatedErrRunner {
                claimed: claimed.clone(),
                gate: tokio::sync::Mutex::new(Some(gate_rx)),
                calls: calls.clone(),
                error: StdMutex::new(Some(error)),
            },
            ChannelHook { tx: hook_tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), claimed.notified())
            .await
            .expect("runner reached gate");

        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(was_cancelled);

        // Release the runner. It returns Err; without cancellation this
        // would either dead-letter (permanent) or nack for retry
        // (transient). Cancellation must suppress both.
        let _ = gate_tx.send(());

        let outcome = tokio::time::timeout(Duration::from_secs(2), hook_rx.recv())
            .await
            .expect("hook fired")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert!(
            outcome.error.is_none(),
            "external cancel must carry no reason (Some(_) would imply runner-issued StepOutcome::Cancel)",
        );
        assert!(runtime.status(&handle.run_id).await.is_none());

        // Settle window: assert no retry attempt and no dead-letter or
        // duplicate hook fires after the terminal one.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "cancellation must suppress retries",
        );
        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0, "cancellation must suppress dead-letter");
        assert!(
            hook_rx.try_recv().is_err(),
            "hook must fire exactly once for the cancelled run",
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_suppresses_permanent_runner_error() {
        // Without cancellation, `StepError::permanent` dead-letters the
        // step and causes the worker to return `PermanentFailure`. With
        // an external cancel in flight, the worker must ack and fire
        // `Cancelled` instead.
        assert_cancel_suppresses_runner_error(StepError::permanent("would-dead-letter")).await;
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_suppresses_transient_runner_error() {
        // Without cancellation, `StepError::transient` nacks for retry
        // (and eventually dead-letters). With an external cancel in
        // flight, the worker must ack and fire `Cancelled` without
        // re-invoking the runner.
        assert_cancel_suppresses_runner_error(StepError::transient("would-retry")).await;
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_signals_step_token_for_cooperative_short_circuit() {
        // A runner that watches `step.cancel_token` should short-circuit
        // long after-claim work as soon as `WorkflowRuntime::cancel` is
        // called. Without the token, cancellation latency is bounded by
        // step duration; with it, the runner returns essentially
        // immediately. The test pins this by using a step that would
        // otherwise sleep for 30 seconds; if the token didn't fire, the
        // test would time out.
        struct CooperativeRunner {
            claimed: Arc<tokio::sync::Notify>,
        }
        impl StepRunner for CooperativeRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.claimed.notify_one();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        Ok(StepOutcome::Succeed { result: b"slow".to_vec() })
                    }
                    _ = step.cancel_token.cancelled() => {
                        Ok(StepOutcome::Cancel { reason: "cooperative".to_string() })
                    }
                }
            }
        }

        let (queue, store) = fresh_queue().await;
        let claimed = Arc::new(tokio::sync::Notify::new());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            CooperativeRunner {
                claimed: claimed.clone(),
            },
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), claimed.notified())
            .await
            .expect("runner observed token");

        let start = std::time::Instant::now();
        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(was_cancelled);

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired well before the 30s sleep would have")
            .expect("hook channel open");
        let elapsed = start.elapsed();

        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        // Runner-issued Cancel wins precedence over external cancel, so
        // the runner's reason surfaces.
        assert_eq!(outcome.error.as_deref(), Some("cooperative"));
        assert!(
            elapsed < Duration::from_secs(2),
            "cooperative cancel must short-circuit the 30s sleep (took {elapsed:?})",
        );
        assert!(runtime.status(&handle.run_id).await.is_none());

        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0);

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn double_cancel_fires_hook_once_and_second_call_returns_false() {
        // Submit a run and cancel twice while it sits pending. The first
        // call removes the queued step, fires the hook, and drops the
        // registry entry. The second call must see no entry and report
        // `Ok(false)`; crucially, the hook must NOT fire a second
        // time.
        struct UnreachableRunner;
        impl StepRunner for UnreachableRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                unreachable!("worker must not claim the cancelled step");
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime =
            WorkflowRuntime::builder(queue, store.clone(), UnreachableRunner, ChannelHook { tx })
                .build();
        // Deliberately do not spawn the worker loop, so step 0 stays
        // Pending while both cancels race.

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let first = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(first, "first cancel initiates termination");

        let second = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(
            !second,
            "second cancel must report Ok(false): registry entry is gone after the first fired the hook",
        );

        // Hook fires exactly once.
        let _ = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            rx.try_recv().is_err(),
            "hook must fire exactly once for a double-cancelled run",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_after_run_already_terminated_returns_false() {
        // Submit a run that succeeds normally, wait for the terminal
        // hook, then call `cancel`. The registry entry was removed when
        // the success hook fired, so `cancel` must report `Ok(false)`
        // and must not fire a second hook.
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![StepOutcome::Succeed {
                result: b"done".to_vec(),
            }]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("Succeeded hook fired")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert!(runtime.status(&handle.run_id).await.is_none());

        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(
            !was_cancelled,
            "cancel on an already-terminated run must report Ok(false)",
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            rx.try_recv().is_err(),
            "no Cancelled hook may fire after the run already terminated as Succeeded",
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn status_reports_cancelling_while_termination_in_flight() {
        // Once `cancel()` has been called but the terminal hook hasn't
        // fired yet, `status()` should report `RunState::Cancelling` so
        // external observers can see termination is in progress. A gated
        // runner holds the cancellation window open long enough to
        // observe it deterministically.
        struct GatedRunner {
            claimed: Arc<tokio::sync::Notify>,
            gate: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
        }
        impl StepRunner for GatedRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.claimed.notify_one();
                let rx = self.gate.lock().await.take().expect("gate consumed twice");
                let _ = rx.await;
                Ok(StepOutcome::Succeed {
                    result: b"would-have-succeeded".to_vec(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let claimed = Arc::new(tokio::sync::Notify::new());
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            GatedRunner {
                claimed: claimed.clone(),
                gate: tokio::sync::Mutex::new(Some(gate_rx)),
            },
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), claimed.notified())
            .await
            .expect("runner reached gate");

        // Before cancel: runner is in flight, state is Running.
        let before = runtime.status(&handle.run_id).await.expect("active");
        assert_eq!(before.state, RunState::Running);

        runtime.cancel(&handle.run_id).await.unwrap();

        // After cancel but before the gate is released: the step is still
        // in flight, but the cancellation overlay must dominate the
        // reported state.
        let during = runtime
            .status(&handle.run_id)
            .await
            .expect("entry retained while termination is in flight");
        assert_eq!(during.state, RunState::Cancelling);

        // Release the runner; the worker observes cancel_requested and
        // settles the run as Cancelled, removing the entry.
        let _ = gate_tx.send(());

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert!(runtime.status(&handle.run_id).await.is_none());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_unknown_run_returns_false() {
        let (queue, store) = fresh_queue().await;
        let runtime: WorkflowRuntime<ScriptedRunner, NoopTerminalHook> = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![]),
            NoopTerminalHook,
        )
        .build();

        let was_cancelled = runtime.cancel("never-submitted").await.unwrap();
        assert!(!was_cancelled);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_fires_once_on_single_attempt() {
        assert_transient_retries_until_max(1).await;
    }

    #[tokio::test(start_paused = true)]
    async fn transient_retries_up_to_max_attempts() {
        assert_transient_retries_until_max(3).await;
    }

    #[tokio::test(start_paused = true)]
    async fn step_memo_is_accessible_across_steps_of_a_run() {
        struct MemoRunner;
        impl StepRunner for MemoRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                match step.step_number {
                    0 => {
                        step.memo
                            .put(&step.run_id, 0, "k", b"cached-value")
                            .await
                            .map_err(|e| StepError::transient(e.to_string()))?;
                        Ok(StepOutcome::Continue {
                            payload: b"go".to_vec(),
                        })
                    }
                    1 => {
                        let got = step
                            .memo
                            .get(&step.run_id, 0, "k")
                            .await
                            .map_err(|e| StepError::transient(e.to_string()))?;
                        assert_eq!(got, Some(b"cached-value".to_vec()));
                        Ok(StepOutcome::Succeed {
                            result: got.unwrap_or_default(),
                        })
                    }
                    _ => unreachable!(),
                }
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime =
            WorkflowRuntime::builder(queue, store, MemoRunner, ChannelHook { tx }).build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: b"start".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(outcome.result.as_deref(), Some(b"cached-value".as_slice()));

        let _ = shutdown.send(());
    }
}

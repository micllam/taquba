//! Durable, at-least-once workflow runtime on top of the [Taquba] task queue.
//!
//! `taquba-workflow` is the plumbing for any multi-step process that
//! benefits from durable state between steps: idempotent step execution,
//! retries with backoff, graceful shutdown / restart, and terminal-state
//! notifications. Implement [`StepRunner`] with bytes-in / bytes-out
//! per-step logic and the runtime persists everything else.
//!
//! It's particularly well-suited for **AI agent runs**, where each step is
//! one LLM call (or one full agent loop) and a process restart between
//! steps shouldn't lose expensive intermediate work. See
//! `examples/rig_agent.rs` for a Rig integration. The runtime itself is
//! framework-neutral: equally usable for ETL pipelines, document
//! processing, payment flows, etc.
//!
//! # What this is / isn't
//!
//! `taquba-workflow` is an **imperative step orchestrator**: at each step,
//! the runner code decides what happens next by returning a
//! [`StepOutcome`] (Continue, Succeed, Fail, Cancel). External cancellation
//! is supported via [`WorkflowRuntime::cancel`]. It is *not*:
//!
//! - **A DAG executor.** There's no declarative graph definition, no
//!   built-in fan-out / fan-in, no dependency-driven scheduling.
//! - **An event-sourced workflow engine.** There's no event-history
//!   replay, no per-side-effect recording.
//!
//! Within the ecosystem, `taquba-jobs` is the sibling crate for
//! single-shot typed tasks: use it when the caller awaits a typed return
//! value and there are no intermediate steps to persist; use a workflow
//! (even a single-step one) when the caller observes the run through
//! cancellation and a terminal hook rather than awaiting a returned
//! value. `taquba-bulk` builds on this crate to run one pipeline over
//! many inputs with batch progress and cost rollup.
//!
//! # Single-process by design
//!
//! The submission API and worker pool live in the same binary and share one
//! `Arc<Queue>`.
//!
//! # Configuring the queue
//!
//! Per-queue retention ([`taquba::QueueConfig::keep_done_jobs`] and
//! [`taquba::QueueConfig::dead_retention`]) is set on the
//! [`taquba::Queue`] before it's handed to the runtime. Pick an explicit
//! name via [`WorkflowRuntimeBuilder::queue_name`] and key
//! [`taquba::OpenOptions::queue_configs`] on the same string.
//!
//! ```no_run
//! # use std::collections::HashMap;
//! # use std::sync::Arc;
//! # use std::time::Duration;
//! # use taquba::{OpenOptions, Queue, QueueConfig, object_store::memory::InMemory};
//! # use taquba_workflow::{NoopTerminalHook, StepError, StepOutcome, StepRunner, WorkflowRuntime, Step};
//! # struct EchoRunner;
//! # impl StepRunner for EchoRunner {
//! #     async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
//! #         Ok(StepOutcome::Succeed { result: step.payload.clone() })
//! #     }
//! # }
//! # async fn run() -> taquba_workflow::Result<()> {
//! let store = Arc::new(InMemory::new());
//! let opts = OpenOptions {
//!     queue_configs: HashMap::from([(
//!         "agent-runs".to_string(),
//!         QueueConfig {
//!             keep_done_jobs: Some(Duration::from_secs(24 * 60 * 60)),
//!             ..QueueConfig::default()
//!         },
//!     )]),
//!     ..OpenOptions::default()
//! };
//! let queue = Arc::new(Queue::open_with_options(store.clone(), "db", opts).await?);
//! let runtime = WorkflowRuntime::builder(queue, store, EchoRunner, NoopTerminalHook)
//!     .queue_name("agent-runs") // same string as in queue_configs
//!     .build();
//! # let _ = runtime;
//! # Ok(()) }
//! ```
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use taquba::{Queue, object_store::memory::InMemory};
//! use taquba_workflow::{
//!     NoopTerminalHook, RunSpec, Step, StepError, StepOutcome, StepRunner, WorkflowRuntime,
//! };
//!
//! struct EchoRunner;
//!
//! impl StepRunner for EchoRunner {
//!     async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
//!         Ok(StepOutcome::Succeed { result: step.payload.clone() })
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(InMemory::new());
//! let queue = Arc::new(Queue::open(store.clone(), "demo").await?);
//!
//! let runtime = WorkflowRuntime::builder(queue, store, EchoRunner, NoopTerminalHook).build();
//!
//! let runtime_for_worker = runtime.clone();
//! tokio::spawn(async move {
//!     runtime_for_worker.run(std::future::pending::<()>()).await
//! });
//!
//! let outcome = runtime.submit(RunSpec {
//!     input: b"hello".to_vec(),
//!     ..Default::default()
//! }).await?;
//! println!("submitted run {}", outcome.run_id);
//! # Ok(()) }
//! ```
//!
//! # Cancellation
//!
//! Call [`WorkflowRuntime::cancel`] to cancel an active run from outside
//! the runner:
//!
//! - If the current step is **pending or scheduled**, the queued step job
//!   is removed and the terminal hook fires from the `cancel` call before
//!   it returns.
//! - If the current step is **running**, cancellation is delivered via
//!   [`Step::cancel_token`] (a `tokio_util::sync::CancellationToken`).
//!   Runners that watch the token can short-circuit immediately:
//!
//!   ```ignore
//!   tokio::select! {
//!       out = call_llm(step) => out,
//!       _ = step.cancel_token.cancelled() => {
//!           Ok(StepOutcome::Cancel { reason: "cooperative".into() })
//!       }
//!   }
//!   ```
//!
//!   Runners that ignore the token are allowed to run to completion
//!   (futures cannot be safely aborted mid-step). In both cases the
//!   runner's [`StepOutcome`] is discarded, any pending transient retry
//!   is suppressed, and the worker fires the terminal hook with
//!   [`TerminalStatus::Cancelled`] once the step returns. Watching the
//!   token only reduces cancellation latency for slow steps; it doesn't
//!   change semantics.
//!
//! While termination is in flight, [`WorkflowRuntime::status`] reports a
//! [`RunState::Cancelling`] overlay until the entry is dropped.
//!
//! `cancel` returns `Ok(false)` if the run is unknown or already
//! terminal in this runtime. It only reaches runs submitted to this
//! [`WorkflowRuntime`] instance; a second runtime in the same process
//! (sharing the queue) maintains its own registry.
//!
//! # Idempotency model
//!
//! Each step is enqueued with [`taquba::EnqueueOptions::dedup_key`] of
//! `"run:{run_id}:{step_number}"`. This guarantees that no two pending or
//! scheduled jobs exist for the same `(run_id, step_number)` at the same
//! time. Taquba is at-least-once though, so a step can still be claimed and
//! executed more than once if its lease expires before ack: implementations
//! of [`StepRunner`] must be idempotent for the same input.
//!
//! # Memoizing within-step side effects
//!
//! Because retries can re-execute a step, expensive non-idempotent side
//! effects (LLM calls, paid APIs, multi-stage processing) need a place
//! to record their result so retries observe the cached value instead
//! of paying twice. [`Step::memo`] is a per-step durable key-value
//! store scoped to `(run_id, step_number)`:
//!
//! ```ignore
//! // Inside StepRunner::run_step:
//! if let Some(cached) = step.memo.get("draft").await? {
//!     return Ok(StepOutcome::Succeed { result: cached });
//! }
//! let draft = expensive_call(&step.payload).await?;
//! step.memo.put("draft", &draft).await?;
//! Ok(StepOutcome::Succeed { result: draft })
//! ```
//!
//! When the natural memo key is the content of an input value,
//! [`Memo::content_get`] and [`Memo::content_put`] serialize that input
//! as MessagePack, hash it with SHA-256, and use the digest as the memo
//! key:
//!
//! ```ignore
//! #[derive(serde::Serialize)]
//! struct DraftInput<'a> {
//!     operation: &'static str,
//!     payload: &'a [u8],
//! }
//!
//! let input = DraftInput {
//!     operation: "draft",
//!     payload: &step.payload,
//! };
//! if let Some(cached) = step.memo.content_get(&input).await? {
//!     return Ok(StepOutcome::Succeed { result: cached });
//! }
//! let draft = expensive_call(&step.payload).await?;
//! step.memo.content_put(&input, &draft).await?;
//! Ok(StepOutcome::Succeed { result: draft })
//! ```
//!
//! Content-addressed memo keys remain scoped to `(run_id, step_number)`;
//! they are not a cross-run cache. If multiple logical operations may
//! receive the same input shape, include an operation name in the
//! serialized input.
//!
//! Memo entries live in the object store passed to
//! [`WorkflowRuntime::builder`] under the path prefix configured by
//! [`WorkflowRuntimeBuilder::memo_prefix`] (default `"workflow-memo"`).
//! `Memo` is strictly per-step; the durable channel between steps is
//! [`StepOutcome::Continue`]'s payload, not memo.
//!
//! # Step-output replay
//!
//! [`WorkflowRuntimeBuilder::step_output_replay`] enables an additional
//! runtime-managed replay record for every outcome the runner returns,
//! including `Fail` and `Cancel`. Step errors ([`StepError`]) are not
//! recorded, so retries still invoke the runner. The record is keyed by
//! `(run_id, step_number, SHA-256(step payload))` and is written before
//! the runtime applies the outcome. If the same step is delivered again
//! after a crash before ack, the stored outcome is replayed without
//! invoking the runner again. A replayed [`StepOutcome::ContinueAfter`]
//! reduces its delay by the time already elapsed since the outcome was
//! stored, preserving the original schedule.
//!
//! This is disabled by default because it adds one object-store read per
//! step delivery (the replay lookup) plus one write per recorded outcome,
//! and makes that write part of step settlement. The replay records are
//! scoped to one run and step; they are not a cross-run cache. They are
//! cleared with the run's memo entries when memo retention is configured.
//!
//! # Memo retention
//!
//! By default memo entries are retained indefinitely (appropriate for
//! short-lived runs or workloads that manage cleanup externally). To
//! enable automatic cleanup, configure a retention window via
//! [`WorkflowRuntimeBuilder::memo_retention`]:
//!
//! ```ignore
//! let runtime = WorkflowRuntime::builder(queue, store, runner, hook)
//!     .memo_retention(Duration::from_secs(24 * 60 * 60))
//!     .build();
//! ```
//!
//! When retention is set, the runtime writes a small terminal marker
//! for every terminal state (Succeeded, Failed, Cancelled) and
//! [`WorkflowRuntime::run`] spawns a background sweeper that lists
//! those markers and clears the memo entries, step-output replay
//! entries, and marker for any run whose marker is older than the
//! retention window. The first sweep fires on startup so a restarted
//! process catches markers left behind by an earlier one.
//!
//! Advanced cleanup policies (selective retention, externally-driven
//! sweeps) can be built directly on [`MemoStore::list_terminal_markers`],
//! [`MemoStore::clear_memos_for_run`], and
//! [`MemoStore::delete_terminal_marker`] without configuring
//! [`WorkflowRuntimeBuilder::memo_retention`].
//!
//! # Time injection
//!
//! Every timestamp the runtime writes (the `submitted_at_ms` on
//! the durable per-run record, the `run_at` it computes when a
//! step returns [`StepOutcome::ContinueAfter`], and the terminal
//! marker timestamps the memo-retention sweep consumes) is read
//! through a [`taquba::Clock`] rather than `SystemTime::now()`. By
//! default the runtime inherits the clock its [`taquba::Queue`]
//! was opened with, so passing a [`taquba::MockClock`] to
//! [`taquba::OpenOptions::clock`] virtualises both the queue and
//! the workflow runtime in lockstep:
//!
//! ```rust,ignore
//! let clock = MockClock::new(1_700_000_000_000);
//! let opts = OpenOptions {
//!     clock: Arc::new(clock.clone()),
//!     ..OpenOptions::default()
//! };
//! let queue = Queue::open_with_options(store.clone(), "db", opts).await?;
//! let runtime = WorkflowRuntime::builder(queue, store, runner, hook).build();
//! // `runtime` reads the same clock as `queue`; `clock.advance(...)`
//! // moves every time-based decision the runtime makes.
//! ```
//!
//! Override the inherited default via
//! [`WorkflowRuntimeBuilder::clock`] when a test or specialised
//! setup needs the runtime on a different time source than the
//! queue. The common case for production callers is to leave the
//! default and let the queue's `SystemClock` flow through.
//!
//! This makes downstream tests deterministic:
//! [`StepOutcome::ContinueAfter`] delays, memo-retention sweep
//! eligibility, and terminal-marker ages all advance under
//! explicit `MockClock::advance` calls rather than wall-clock
//! waits.
//!
//! # Duplicate submissions
//!
//! [`WorkflowRuntime::submit`] is idempotent on `(run_id, spec.input)`.
//! A re-submission of an active run that carries the same input is a
//! no-op and the returned [`SubmitOutcome`] has `newly_submitted = false`.
//! A re-submission that carries a *different* input is rejected with
//! [`Error::InputMismatch`]: reusing a `run_id` with new content is a
//! programmer error; pick a fresh `run_id` for a new run.
//!
//! Duplicates are caught from two sources, in order:
//!
//! 1. An in-process registry catches duplicates within the same runtime.
//! 2. A **durable per-run record** written atomically with the step-0
//!    enqueue (via [`taquba::Queue::enqueue_with_kv`]) catches
//!    duplicates across process restarts, even after step 0 has been
//!    claimed and its dedup key released. The record carries a SHA-256
//!    of the original input so the cross-restart mismatch check works
//!    even when the in-memory registry is empty. The record is cleaned
//!    up when the run reaches a terminal state.
//!
//! # Reserved headers
//!
//! Step jobs reserve the `workflow.*` header prefix; concretely
//! [`HEADER_RUN_ID`] and [`HEADER_STEP`] are set by the runtime on every
//! step. Submitter-supplied headers must not start with `workflow.`; submission
//! rejects them. All other user headers are threaded through every step and
//! surfaced to the [`TerminalHook`].
//!
//! [Taquba]: https://docs.rs/taquba

#![warn(missing_docs)]

mod error;
mod memo;
mod runner;
mod runtime;
mod terminal;

pub use error::{Error, Result};
pub use memo::{Memo, MemoStore, TerminalMarker};
pub use runner::{Step, StepError, StepErrorKind, StepOutcome, StepRunner};
pub use runtime::{
    HEADER_RUN_ID, HEADER_STEP, RESERVED_HEADER_PREFIX, RunSpec, RunState, RunStatus,
    SubmitOutcome, WorkflowRuntime, WorkflowRuntimeBuilder,
};
#[cfg(feature = "webhooks")]
pub use terminal::WebhookTerminalHook;
pub use terminal::{NoopTerminalHook, RunOutcome, TerminalHook, TerminalStatus};

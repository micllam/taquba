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
//! # Single-process by design
//!
//! The submission API and worker pool live in the same binary and share one
//! `Arc<Queue>`.
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
//! let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "demo").await?);
//!
//! let runtime = WorkflowRuntime::builder(queue, EchoRunner, NoopTerminalHook).build();
//!
//! let runtime_for_worker = runtime.clone();
//! tokio::spawn(async move {
//!     runtime_for_worker.run(std::future::pending::<()>()).await
//! });
//!
//! let handle = runtime.submit(RunSpec {
//!     input: b"hello".to_vec(),
//!     ..Default::default()
//! }).await?;
//! println!("submitted run {}", handle.run_id);
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
mod runner;
mod runtime;
mod terminal;

pub use error::{Error, Result};
pub use runner::{Step, StepError, StepErrorKind, StepOutcome, StepRunner};
pub use runtime::{
    HEADER_RUN_ID, HEADER_STEP, RESERVED_HEADER_PREFIX, RunHandle, RunSpec, RunState, RunStatus,
    WorkflowRuntime, WorkflowRuntimeBuilder,
};
#[cfg(feature = "webhooks")]
pub use terminal::WebhookTerminalHook;
pub use terminal::{NoopTerminalHook, RunOutcome, TerminalHook, TerminalStatus};

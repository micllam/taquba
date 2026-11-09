//! Durable execution for Rust on object storage, built on the [Taquba] queue. A
//! run is a sequence of steps, and the runtime stores the output of each step,
//! the recorded results of its side effects and its application state in the
//! store of the queue. A process restarted mid-run resumes at the last
//! completed step. The
//! [README](https://github.com/micllam/taquba/tree/master/taquba-workflow)
//! states which workloads the runtime suits.
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use taquba::{Queue, object_store::memory::InMemory};
//! use taquba_workflow::{RunSpec, Step, StepError, StepOutcome, StepRunner, WorkflowRuntime};
//!
//! struct Summarize;
//!
//! impl StepRunner for Summarize {
//!     async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
//!         match step.step_number {
//!             // A retry of step 0 reads the recorded summary and does not
//!             // call the model again.
//!             0 => {
//!                 let summary: String = step
//!                     .memo
//!                     .memoized("summary", call_model(&step.payload))
//!                     .await?;
//!                 Ok(StepOutcome::continue_now(summary.into_bytes()))
//!             }
//!             // Step 1 receives the summary as its payload.
//!             _ => Ok(StepOutcome::Succeed { result: step.payload.clone() }),
//!         }
//!     }
//! }
//!
//! async fn call_model(input: &[u8]) -> Result<String, StepError> {
//!     Ok(format!("summary of {} bytes", input.len()))
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(InMemory::new());
//! let queue = Arc::new(Queue::open(store.clone(), "demo").await?);
//!
//! let runtime = WorkflowRuntime::builder(queue, store, Summarize).build()?;
//!
//! let worker = runtime.spawn(std::future::pending::<()>());
//!
//! let submitted = runtime.submit(RunSpec {
//!     input: b"a long document".to_vec(),
//!     ..Default::default()
//! }).await?;
//! let run = runtime.wait(&submitted.run_id).await?;
//! println!("run {} {}", run.run_id, run.termination.status);
//! worker.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! [`WorkflowRuntimeBuilder::queue_name`] sets the name of the step queue, and
//! [`taquba::OpenOptions::queue_configs`] sets its retention, lease duration
//! and attempt limit under that name.
//!
//! # Run lifecycle
//!
//! ```text
//!                Continue: now, after a delay,
//!                on a signal or on a group
//!                ┌─────────┐
//!                ▼         │
//! submit ──────▶ step ─────┘
//!                │
//!                ├── Succeed ───────────────────────▶ succeeded
//!                ├── Fail, or a dead-lettered step ─▶ failed
//!                └── Cancel, or a cancellation ─────▶ cancelled
//! ```
//!
//! A [`StepRunner`] receives a [`Step`] and returns a [`StepOutcome`].
//! [`StepOutcome::Continue`] enqueues the next step, and its [`Trigger`] defers
//! that step for a delay, until a signal or until a group ends. A [`StepError`]
//! retries the step after a backoff, and a permanent error or the attempt limit
//! dead-letters it. The worker terminates the run of a dead-lettered step as
//! failed.
//!
//! Delivery is at least once, so a runner must be idempotent for a run id and a
//! step number. A [`Delivery`] gives a step its handles: the memos, the staged
//! effects, the committed KV reads, the lease, the cancellation token and the
//! [`Owner`] of the run.
//!
//! # API map
//!
//! - **Runtime.** [`WorkflowRuntime::builder`] returns a
//!   [`WorkflowRuntimeBuilder`], which sets the memo prefix, memo retention,
//!   step-output replay, the clock, the terminal hook and the control queue.
//!   [`WorkflowRuntime::spawn`] runs the worker as a task.
//! - **Runs.** [`WorkflowRuntime::submit`] takes a [`RunSpec`] with
//!   [`RunOptions`] and returns a [`SubmitOutcome`]. [`WorkflowRuntime::wait`]
//!   returns the [`TerminatedRun`], [`WorkflowRuntime::status`] returns a
//!   [`RunStatus`] and [`WorkflowRuntime::cancel`] cancels a run.
//! - **Reads.** [`WorkflowView`] reads runs, terminated runs, groups and
//!   commands, also in a process without a runtime. [`WorkflowView::runs`]
//!   lists runs as [`ListedRun`] values under a [`RunFilter`].
//! - **Memo.** A [`Memo`] records the results of side effects for a step or a
//!   run, and the [`MemoStore`] keeps the entries in the object store.
//! - **Effects.** An [`EffectsHandle`] stages KV writes and enqueues into the
//!   settlement of a step, and a [`KvReadHandle`] reads committed KV state. A
//!   [`TerminalHook`] stages [`TerminalEffects`] into the settlement that
//!   terminates a run.
//! - **Groups.** [`WorkflowRuntime::group`] returns the [`RunGroup`] of an id,
//!   whose results arrive as [`MemberResult`] values. A [`jobs::JobGroup`] is a
//!   group of typed jobs.
//! - **Typed jobs.** The [`jobs`] module runs a typed async function as a
//!   single-step run.
//! - **Other processes.** A [`Command`] submits or cancels a run through the
//!   inbox of the queue.
//!
//! # Cargo features
//!
//! The `webhooks` feature enables `WebhookTerminalHook`, which delivers an HTTP
//! callback at the termination of a run through `taquba-webhooks`.
//!
//! [Taquba]: https://docs.rs/taquba

#![warn(missing_docs)]

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

mod blob;
mod command;
mod durable;
mod effects;
mod error;
mod group;
pub mod jobs;
mod keys;
mod kv;
mod memo;
mod owner;
mod runner;
mod runtime;
mod signal;
mod sweep;
mod terminal;
#[cfg(test)]
mod test_util;
mod view;
mod worker;

pub use command::{CancelCommand, Command, CommandState, SubmitCommand};
pub use effects::{EffectsHandle, TerminalEffects};
pub use error::{ControlQueueError, Error, Result};
pub use group::{GroupMember, GroupStatus, MemberResult, RunGroup};
pub use keys::{
    HEADER_RUN_ID, HEADER_STEP, MAX_RUN_ID_LEN, RESERVED_COUNT_PREFIX, RESERVED_HEADER_PREFIX,
    RESERVED_KV_PREFIX, RunId,
};
pub use kv::KvReadHandle;
pub use memo::{Memo, MemoStore};
pub use owner::Owner;
pub use runner::{Delivery, Step, StepError, StepErrorKind, StepOutcome, StepRunner, Trigger};
pub use runtime::{
    CancelRunOutcome, DEFAULT_QUEUE_NAME, RunOptions, RunSpec, RunnerHandle, SubmitOutcome,
    WorkflowRuntime, WorkflowRuntimeBuilder,
};
#[cfg(feature = "webhooks")]
pub use terminal::WebhookTerminalHook;
pub use terminal::{CancelOrigin, RunTermination, TerminalHook, TerminalStatus, TerminatedRun};
pub use view::{ListedRun, RunFilter, RunState, RunStatus, StepRef, WorkflowView};

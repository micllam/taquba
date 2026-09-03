//! Typed single-function jobs on the workflow runtime.
//!
//! A job is a function that runs reliably in the background: define a typed
//! [`Job`], submit instances of it and receive the typed result. Each job
//! runs as one workflow run with a single step, so durability, retries,
//! idempotent submission, memoization and retention are the workflow
//! runtime's, and this crate adds the function abstraction: typed inputs
//! and outputs, a type registry, a durable outcome record and an awaitable
//! handle.
//!
//! Use a job when the caller awaits a typed return value, and a
//! [`StepRunner`](crate::StepRunner) directly when one entity moves through
//! several durable steps with cancellation and a terminal hook. The word
//! "job" names the typed function here; the queue job that delivers a step
//! is [`Delivery::job_id`](crate::Delivery::job_id). Chaining jobs to model a
//! multi-step process is a sign the work belongs in a workflow.
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use serde::{Serialize, Deserialize};
//! use taquba::{Queue, object_store::memory::InMemory};
//! use taquba_workflow::jobs::{Job, JobContext, JobRunner};
//!
//! #[derive(Serialize, Deserialize)]
//! struct SendEmail {
//!     to: String,
//!     subject: String,
//! }
//!
//! #[derive(Debug, thiserror::Error)]
//! #[error("email error: {0}")]
//! struct EmailError(String);
//!
//! impl Job for SendEmail {
//!     const NAME: &'static str = "email.send";
//!     type Output = String; // message id
//!     type Error = EmailError;
//!
//!     async fn run(&self, _ctx: JobContext<'_>) -> Result<String, EmailError> {
//!         // ... call your email provider ...
//!         Ok(format!("msg-for-{}", self.to))
//!     }
//!
//!     fn idempotency_key(&self) -> Option<String> {
//!         Some(format!("email:{}:{}", self.to, self.subject))
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(InMemory::new());
//! let queue = Arc::new(Queue::open(store.clone(), "background-jobs").await?);
//!
//! let mut runner = JobRunner::builder(queue, store)
//!     .max_concurrent_jobs(50)
//!     .register::<SendEmail>()
//!     .build();
//! let handle = runner.spawn(std::future::pending::<()>());
//!
//! let job = runner
//!     .submit(SendEmail { to: "user@example.com".into(), subject: "Welcome".into() })
//!     .await?;
//! let message_id = job.await?;
//!
//! handle.shutdown().await?;
//! # let _ = message_id;
//! # Ok(())
//! # }
//! ```
//!
//! # Architecture
//!
//! Like the rest of the Taquba ecosystem, the runner is single-process:
//! one [`JobRunner`] per process, owning a workflow runtime over one
//! [`taquba::Queue`]. A submission becomes a workflow run whose input is
//! the job's [`Job::NAME`] and its serialized fields, and whose single
//! step routes by that name to the registered handler.
//!
//! A job's outcome is durable: the handler's step writes an outcome record
//! (the serialized output, or the failure) to the run's memo in the object
//! store before its settlement. Awaiting a [`JobHandle`] is in-process (it
//! uses Taquba's in-process completion notification), but the outcome can
//! be read back with [`JobHandle::fetch_result`] after a process restart.
//!
//! Delivery is at-least-once, inherited from Taquba: **job handlers must be
//! idempotent.** A retried attempt that runs after an earlier attempt
//! already wrote an outcome record overwrites it with the new attempt's
//! outcome. The [`memo`](crate::Delivery::memo) gives a handler a durable
//! memo for the results of expensive calls, so a retried attempt reads
//! them back.
//!
//! Outcome records and memo entries are retained indefinitely by default;
//! enable [`JobRunnerBuilder::retention`] (see [Retention]) to remove them
//! on a schedule, or apply a lifecycle policy to the object-store prefix.
//!
//! [Retention]: #retention
//! [Idempotent submissions]: #idempotent-submissions
//!
//! # Idempotent submissions
//!
//! [`Job::idempotency_key`] collapses duplicate submissions to a single
//! job. The key's SHA-256 digest is the job's id and its workflow run id.
//!
//! - **Before the original completes** (pending, scheduled or in flight):
//!   a second submission with the same key returns a [`JobHandle`] to the
//!   in-flight job, with [`JobHandle::newly_submitted`] `== false`. If the
//!   payload differs from the original, the submission fails with
//!   [`Error::InputMismatch`](crate::Error::InputMismatch). The check
//!   survives process restarts: the
//!   SHA-256 of the serialized payload is stored in the workflow's run
//!   record, atomically with the enqueue.
//! - **After the original completes**: the outcome record holds the same
//!   hash, so a re-submission with a matching payload returns a handle to
//!   the recorded outcome (success or terminal failure) without running
//!   the job again, and a differing payload fails with
//!   [`Error::InputMismatch`](crate::Error::InputMismatch).
//!
//! If [`JobRunnerBuilder::retention`] is configured and the outcome record
//! has been removed, the re-submission runs the job again under the same
//! id. Size the retention window to cover the longest gap callers need
//! between the original submission and an idempotent re-submission.
//!
//! For jobs where "same input means same key" is the right semantics,
//! [`payload_idempotency_key`] hashes the serialized payload directly.
//! Custom keys are appropriate when the dedup identity is narrower than
//! the full payload (for example `"email:{recipient}:{date}"`).
//!
//! # Job groups
//!
//! A [`JobGroup`] submits many jobs of one type as one durable set and
//! joins their typed results: [`JobRunner::group`] names the group,
//! [`JobGroup::submit`] writes its manifest and submits the members, and
//! [`JobGroup::join`] waits for every member and returns the results in
//! submission order. Members are identified within the group by key
//! (the job's [`Job::idempotency_key`], or the positional `item-{i}`),
//! and a member's job id is derived from the group id and its key. A
//! second submission of the same set runs again every member that did
//! not succeed, so a step that fans out re-submits its group on a retry
//! and joins the recorded results of the members that completed.
//! [`JobGroup::status`] reads the group's durable state and
//! [`JobGroup::forget`] removes it; [`JobRunnerBuilder::group_retention`]
//! removes it a window after the group's members all terminated.
//!
//! ```ignore
//! let group = runner.group::<FetchPage>(format!("fetch-{run_id}"))?;
//! group.submit(urls.iter().map(|url| FetchPage { url: url.clone() })).await?;
//! for member in group.join().await? {
//!     println!("{}: {:?}", member.key, member.result);
//! }
//! ```
//!
//! # Retention
//!
//! [`JobRunnerBuilder::retention`] removes a job's outcome record and memo
//! entries a configured window after the job reaches a terminal state,
//! through the workflow runtime's memo retention. When the option is unset
//! (default), records are retained indefinitely.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use std::time::Duration;
//! # use taquba::{Queue, object_store::memory::InMemory};
//! # use taquba_workflow::jobs::JobRunner;
//! # async fn run() -> taquba_workflow::Result<()> {
//! # let store = Arc::new(InMemory::new());
//! # let queue = Arc::new(Queue::open(store.clone(), "demo").await?);
//! let runner = JobRunner::builder(queue, store)
//!     .retention(Duration::from_secs(24 * 60 * 60))
//!     .build();
//! # let _ = runner; Ok(()) }
//! ```
//!
//! Once a record is removed, [`JobHandle::fetch_result`] for that job
//! returns `Ok(None)` and an idempotent re-submission of the same payload
//! runs the job again (see [Idempotent submissions]).
//!
//! # Time injection
//!
//! The runner inherits its clock from the queue ([`taquba::Queue::clock`]),
//! so a [`taquba::MockClock`] passed to
//! [`taquba::Queue::open_with_options`] virtualises time for retention as
//! well. [`JobRunnerBuilder::clock`] overrides it.
//!
//! # Configuring the queue
//!
//! Per-queue retention ([`taquba::QueueConfig::keep_done_jobs`] and
//! [`taquba::QueueConfig::dead_retention`]) is set on the [`taquba::Queue`]
//! before it is handed to the runner. Choose an explicit name via
//! [`JobRunnerBuilder::queue_name`] and key
//! [`taquba::OpenOptions::queue_configs`] on the same string.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use std::time::Duration;
//! # use taquba::{OpenOptions, Queue, QueueConfig, object_store::memory::InMemory};
//! # use taquba_workflow::jobs::JobRunner;
//! # async fn run() -> taquba_workflow::Result<()> {
//! let store = Arc::new(InMemory::new());
//! let opts = OpenOptions::default().queue_config(
//!     "background-jobs",
//!     QueueConfig::default().keep_done_jobs(Duration::from_secs(60 * 60)),
//! );
//! let queue = Arc::new(Queue::open_with_options(store.clone(), "db", opts).await?);
//! let runner = JobRunner::builder(queue, store)
//!     .queue_name("background-jobs") // same string as in queue_configs
//!     .build();
//! # let _ = runner;
//! # Ok(()) }
//! ```
//!
//! # The handler context
//!
//! [`JobContext`] gives a handler its registered application state and
//! dereferences to the job's [`Delivery`](crate::Delivery): the job's
//! identity and attempt count, the delivery's lease and cancellation
//! token, a durable [`memo`](crate::Delivery::memo) for the results of
//! expensive calls, staged KV effects ([`effects`](crate::Delivery::effects))
//! applied atomically with the job's successful completion and committed
//! KV reads ([`kv`](crate::Delivery::kv)). A handler that submits further
//! jobs holds a [`JobRunner`] in its registered state.
//!
//! # Core types
//!
//! - [`Job`]: the trait defining a typed job (input fields, [`Job::Output`],
//!   [`Job::Error`] and the [`Job::run`] body, plus hooks for idempotency,
//!   attempt limits and error classification).
//! - [`JobRunner`]: submits jobs and spawns the worker; job types are
//!   registered on its builder.
//! - [`JobContext`]: the per-call context passed to [`Job::run`].
//! - [`JobHandle`]: returned from [`JobRunner::submit`]; await it for the
//!   typed result, or read its [`status`](JobHandle::status) and
//!   [`fetch_result`](JobHandle::fetch_result).
//! - [`JobGroup`]: many jobs of one type submitted as one durable set and
//!   joined together.
//!
//! # Retries and failure
//!
//! A job that returns `Err` is classified by [`Job::classify`] as
//! [`StepErrorKind::Transient`](crate::StepErrorKind::Transient) (retried
//! with backoff up to the attempt limit, then dead-lettered) or
//! [`StepErrorKind::Permanent`](crate::StepErrorKind::Permanent)
//! (dead-lettered on that attempt). Backoff is a queue-level Taquba setting; [`Job::max_attempts`]
//! and per-submission [`SubmitOptions`] cover the per-job settings.

mod context;
mod group;
mod handle;
mod job;
mod runner;

pub use crate::RunnerHandle;
pub use context::JobContext;
pub use group::{GroupResult, GroupStatus, JobGroup};
pub use handle::{JobError, JobHandle, JoinError};
pub use job::{Job, payload_idempotency_key};
pub use runner::{JobRunner, JobRunnerBuilder, SubmitOptions};

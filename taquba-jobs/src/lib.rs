//! Durable, typed async function execution on top of the [Taquba] durable
//! task queue.
//!
//! `taquba-jobs` is a primitive for running functions reliably in the
//! background: define a typed [`Job`], submit instances of it, get the typed
//! result back. Durability, retries, idempotency and result persistence are
//! handled for you; the worker process stays stateless and replaceable
//! because all state lives in object storage via Taquba.
//!
//! It sits one level above [`taquba`]: Taquba is the raw durable queue
//! (opaque byte payloads, lease-based claims, dead-letter queue) and
//! `taquba-jobs` adds the function-shaped abstraction (typed inputs, typed
//! outputs, a type registry and durable result delivery).
//!
//! # Architecture
//!
//! Like all of the Taquba ecosystem, `taquba-jobs` is **single-process**: one
//! [`JobRunner`] per process, owning one [`taquba::Queue`]. The runner spawns
//! a concurrent worker that claims jobs, routes each to its registered
//! handler by a type tag, runs it, and persists the outcome.
//!
//! Job *results* are durable: every terminal outcome is written as a blob to
//! an object store you provide (typically the same store the queue lives on,
//! under a sibling prefix of the SlateDB path). Awaiting a [`JobHandle`] is
//! in-process (it uses Taquba's in-process completion notification), but
//! the result itself can be read back with [`JobHandle::fetch_result`] even
//! after a process restart.
//!
//! Delivery is at-least-once, inherited from Taquba: **job handlers must be
//! idempotent.** A retried attempt that re-runs after a prior attempt
//! already wrote a result blob will overwrite that blob with the new
//! attempt's outcome, so a non-idempotent handler can have its earlier
//! "successful" result replaced.
//!
//! Result blobs accumulate indefinitely in this version: there is no
//! automatic retention or cleanup. Long-running deployments should plan
//! their own lifecycle policy on the object-store prefix (S3 lifecycle
//! rules, GCS object-lifecycle management, etc.); a built-in retention
//! option is planned for a later release.
//!
//! # Fan-out from handlers
//!
//! [`JobContext::submit`] lets a running handler enqueue follow-up jobs
//! against the same runner. Use it for chaining (job A submits job B) or
//! for fan-out (a coordinator job submits N independent children). Child
//! submissions are independent: they are not awaited as part of the parent
//! and survive the parent's completion.
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use serde::{Serialize, Deserialize};
//! use taquba::{Queue, object_store::memory::InMemory};
//! use taquba_jobs::{Job, JobContext, JobRunner};
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
//! let mut runner = JobRunner::builder()
//!     .queue(queue)
//!     .object_store(store)
//!     .max_concurrent_jobs(50)
//!     .build()?;
//!
//! runner.register::<SendEmail>();
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
//! # Core types
//!
//! - [`Job`]: the trait defining a typed job (input fields, [`Job::Output`],
//!   [`Job::Error`], and the [`Job::run`] body, plus hooks for idempotency,
//!   attempt limits, and error classification).
//! - [`JobRunner`]: registers job types, submits jobs, spawns the worker.
//! - [`JobContext`]: the per-call context handed to [`Job::run`]: application
//!   state, the queue, the job's identity, a cancellation token.
//! - [`JobHandle`]: returned from [`JobRunner::submit`]; await it for the
//!   typed result, or poll its [`status`](JobHandle::status) /
//!   [`fetch_result`](JobHandle::fetch_result).
//!
//! # Retries and failure
//!
//! A job that returns `Err` is classified by [`Job::classify`] as
//! [`ErrorKind::Transient`] (retried with backoff up to the queue's attempt
//! limit, then dead-lettered) or [`ErrorKind::Permanent`] (dead-lettered
//! immediately). Per-job-type backoff curves are not configurable in this
//! version; backoff is a queue-level Taquba setting; [`Job::max_attempts`]
//! and per-submission [`SubmitOptions`] cover the per-job settings that
//! exist today.
//!
//! [Taquba]: https://docs.rs/taquba

#![warn(missing_docs)]

mod context;
mod error;
mod handle;
mod job;
mod result_store;
mod runner;

pub use context::JobContext;
pub use error::{Error, Result};
pub use handle::{JobError, JobHandle, JoinError};
pub use job::{ErrorKind, Job, payload_idempotency_key};
pub use runner::{JobRunner, JobRunnerBuilder, RunnerHandle, SubmitOptions};

/// Re-export of the underlying [`taquba`] crate.
pub use taquba;

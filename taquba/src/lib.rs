//! A durable, single-process task queue for Rust, backed by object storage.
//!
//! Taquba persists every job-state transition through [SlateDB] to an
//! [`object_store`] backend (local disk, S3, GCS, Azure Blob, MinIO, etc.) so the
//! queue survives process restarts, node loss and ephemeral disks.
//!
//! # When Taquba fits
//!
//! - A single-binary service that needs durable background jobs without
//!   operating a queue server.
//! - Edge or ephemeral compute where the local disk is gone after each
//!   invocation but the bucket persists.
//! - Low-to-moderate-throughput workloads where cheap per-PUT pricing on
//!   object storage beats running a database or broker.
//!
//! # When Taquba does not fit
//!
//! If you need a worker fleet spread across multiple machines.
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use taquba::{Queue, object_store::memory::InMemory};
//!
//! # async fn run() -> taquba::Result<()> {
//! let q = Queue::open(Arc::new(InMemory::new()), "demo").await?;
//!
//! q.enqueue("email", b"alice@example.com".to_vec()).await?;
//!
//! if let Some(job) = q.claim("email", Duration::from_secs(30)).await? {
//!     // ... do the work ...
//!     q.ack(&job).await?;
//! }
//!
//! q.close().await
//! # }
//! ```
//!
//! # Job lifecycle
//!
//! ```text
//! pending → claimed → done
//!               ↘
//!            failed → (backoff → pending | dead-letter)
//! ```
//!
//! - At-least-once delivery: workers must be idempotent.
//! - Lease-based claims: a background reaper requeues abandoned jobs.
//! - Exponential retry backoff via the scheduled key space (configurable per
//!   queue, see [`QueueConfig`]).
//! - Bounded dead-letter retention with paginated inspection.
//!
//! # Background tasks
//!
//! [`Queue::open`] spawns two background tokio tasks for the lifetime of the
//! handle:
//!
//! - **Reaper** - re-queues jobs whose lease expired and runs the done /
//!   dead-letter retention sweeps (interval: [`OpenOptions::reaper_interval`]).
//! - **Scheduler** - promotes scheduled jobs whose `run_at` has passed
//!   (interval: [`OpenOptions::scheduler_interval`]).
//!
//! Call [`Queue::close`] for a clean shutdown; it stops both tasks and
//! flushes the underlying SlateDB instance.
//!
//! # Cargo features
//!
//! No backend is enabled by default: the in-memory and local-disk stores work
//! without any feature. Pick exactly one for production:
//!
//! ```bash
//! cargo add taquba --features aws    # S3 / MinIO
//! cargo add taquba --features gcp    # Google Cloud Storage
//! cargo add taquba --features azure  # Azure Blob
//! ```
//!
//! [SlateDB]: https://github.com/slatedb/slatedb

#![warn(missing_docs)]

mod error;
mod job;
mod queue;
mod reaper;
mod scheduler;
mod stats;
/// Worker-loop primitives: the [`worker::Worker`] trait, plus the
/// [`worker::run_worker`] / [`worker::run_worker_concurrent`] drivers that
/// own the claim -> process -> ack/nack lifecycle and graceful shutdown.
pub mod worker;

pub use error::{Error, Result};
pub use job::{JobRecord, JobStatus};
pub use queue::{
    EnqueueOptions, OpenOptions, PRIORITY_HIGH, PRIORITY_LOW, PRIORITY_NORMAL, Queue, QueueConfig,
};
pub use stats::QueueStats;
pub use worker::{PermanentFailure, Worker, WorkerError, run_worker, run_worker_concurrent};

pub use slatedb::object_store;

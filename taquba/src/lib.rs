//! A durable, single-process task queue for Rust with **no stateful service
//! to operate**. Queue state lives directly in your object storage; compute
//! is stateless and replaceable.
//!
//! Built on [SlateDB] and the [`object_store`] crate (local disk, S3, GCS,
//! Azure Blob, MinIO, etc.). All producers and workers for a given store run
//! inside one process and share an `Arc<Queue>`. Use Taquba when you want
//! durable background jobs whose state survives node loss, ephemeral disks,
//! and region failures, without operating a queue server or a separate
//! state layer (typically Postgres).
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
//! # Coordinating with caller state
//!
//! [`Queue::enqueue_with_kv`] enqueues a job *and* applies a set of writes
//! to a caller-owned KV namespace in a single transaction, so a downstream
//! crate can keep its own durable coordination state (status markers,
//! dedup records, pointers to externally-stored blobs) consistent with
//! the queue across crashes. [`Queue::kv_get`] and [`Queue::kv_delete`]
//! read and clean up those entries.
//!
//! Caller keys live under a reserved `usr:` prefix internally so they
//! cannot collide with Taquba's own layout. Per-value size is capped at
//! [`MAX_KV_VALUE_SIZE`]; the namespace is sized for coordination
//! state, not bulk payload. Store large blobs in the underlying object
//! store under a content-addressed key and put only the pointer in KV.
//!
//! The namespace is mutated **only** as a side effect of queue
//! operations; there is no standalone `kv_put`. To create or update
//! an entry, include it in the `kv_writes` map of an
//! [`Queue::enqueue_with_kv`] call (which makes the write atomic with
//! the enqueue). [`Queue::kv_delete`] is the one standalone primitive,
//! for terminal cleanup of entries whose related queue op has already
//! completed.
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

mod claim_cursor;
mod clock;
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

pub use clock::{Clock, MockClock, SystemClock};
pub use error::{Error, Result};
pub use job::{JobRecord, JobStatus};
pub use queue::{
    CancelOutcome, EnqueueOptions, EnqueueResult, MAX_KV_VALUE_SIZE, OpenOptions, PRIORITY_HIGH,
    PRIORITY_LOW, PRIORITY_NORMAL, Queue, QueueConfig, WaitOutcome,
};
pub use stats::QueueStats;
pub use worker::{PermanentFailure, Worker, WorkerError, run_worker, run_worker_concurrent};

pub use slatedb::object_store;

//! A durable, single-process task queue for Rust with **no stateful service
//! to operate**. Queue state lives directly in your object storage; compute
//! is stateless and replaceable. Because all state shares one transactional
//! store, queue operations compose atomically: a single transaction can
//! acknowledge a job, enqueue its follow-ups, and update caller-owned KV
//! state.
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
//! - If you need a worker fleet spread across multiple machines.
//! - Sustained high-throughput pipelines or low-latency request paths.
//!
//! Measured performance numbers, with the environment and commit that
//! produced them, are recorded in `RESULTS.md` in the source repository.
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
//! See `examples/quickstart.rs` for a runnable version.
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
//! - Scheduled jobs become pending at their `run_at`, or earlier via the
//!   targeted [`Queue::wake_scheduled`], which can attach bytes the worker
//!   observes on delivery.
//! - Bounded dead-letter retention with paginated inspection.
//!
//! # Worker loop
//!
//! Production workers rarely call [`Queue::claim`] and [`Queue::ack`]
//! directly: implement [`Worker`] and let [`run_worker`] handle the
//! claim / ack / nack loop, retries, and graceful shutdown:
//!
//! ```no_run
//! use taquba::{JobRecord, Worker, WorkerError};
//!
//! struct EmailWorker;
//!
//! impl Worker for EmailWorker {
//!     async fn process(&self, job: &JobRecord) -> Result<(), WorkerError> {
//!         let to = std::str::from_utf8(&job.payload)?;
//!         # async fn send_email(_to: &str) -> Result<(), WorkerError> { Ok(()) }
//!         send_email(to).await?;
//!         Ok(())
//!     }
//! }
//! ```
//!
//! Pass any future as the shutdown signal: `tokio::signal::ctrl_c()`, a
//! oneshot, etc. Shutdown is honoured at safe points: between jobs and
//! during idle waits. In-flight jobs always finish, so leases are never
//! abandoned to the reaper.
//!
//! Settlement failures do not stop the loop: when a job outlives its
//! lease and the reaper requeues it, the late acknowledgement fails
//! with [`Error::ClaimLost`], the loop logs it and continues, and the
//! redelivered attempt settles the job instead. Errors on the claim
//! path still stop the loop.
//!
//! A worker can implement [`Worker::process_with_effects`] instead of
//! [`Worker::process`] to return [`AckEffects`]: follow-up enqueues and
//! caller KV changes the loop applies atomically with the job's
//! acknowledgement via [`Queue::ack_with`].
//!
//! [`run_worker_concurrent`] is the same loop processing up to
//! `concurrency` jobs in parallel. It claims jobs in batches sized to
//! its free capacity (one claim transaction per batch via
//! [`Queue::claim_batch`]), spawns each job onto a task set, and acks
//! each job individually. On shutdown it stops claiming and drains the
//! in-flight set before returning. Idle workers of both loops wait on a
//! queue-scoped notification that wakes one waiting worker per inserted
//! job, so the poll interval only bounds the latency of out-of-band
//! events such as a scheduled job becoming due.
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
//! Caller keys live under a reserved user key tag internally so they
//! cannot collide with Taquba's own layout. Per-value size is capped at
//! [`MAX_KV_VALUE_SIZE`]; the namespace is sized for coordination
//! state, not bulk payload. Store large blobs in the underlying object
//! store under a content-addressed key and put only the pointer in KV.
//!
//! The primary pattern couples KV mutations to queue operations: to
//! create or update an entry atomically with a queue transition,
//! include it in the `kv_writes` map of an [`Queue::enqueue_with_kv`]
//! or [`Queue::ack_with`] call. Standalone primitives exist for state
//! whose lifecycle is not tied to a single queue transition:
//! [`Queue::kv_put`] writes an entry durably on its own,
//! [`Queue::kv_delete`] removes one (terminal cleanup), and
//! [`Queue::kv_compare_delete`] consumes an entry only if it still
//! holds the value the caller read, so a concurrent replacement is
//! never deleted by mistake.
//!
//! [`Queue::ack_with`] extends the same atomicity to settlement: it
//! acknowledges a claimed job and, in the same transaction, enqueues
//! follow-up jobs and applies caller KV writes and deletes. If the
//! job's lease expired and the claim is gone, the call fails and
//! nothing is applied, so a chained job exists only if the settlement
//! that created it won.
//!
//! See `examples/atomic_settlement.rs` for a runnable order pipeline
//! built on these primitives.
//!
//! # Large payloads
//!
//! A job record is rewritten on every state transition (enqueue, claim,
//! nack, ack), so a large inline payload is written many times over its
//! lifetime. Payloads larger than
//! [`OpenOptions::payload_offload_threshold`] (default 256 KiB) are
//! therefore offloaded: written once as an object in a payload object
//! store, with the record storing a reference
//! ([`JobRecord::payload_ref`]) instead of the bytes. Claims and job
//! reads fetch the object and return the payload as usual, so
//! offloading is transparent to worker code. The object is deleted
//! when the record leaves the queue: on ack (or with the done record's
//! retention sweep when
//! [`QueueConfig::keep_done_jobs`] is set), on cancel and with the
//! dead-letter retention sweep.
//!
//! By default payload objects live next to the queue's own state, under
//! `"{path}-payloads"` in the object store the queue is opened on.
//! [`OpenOptions::payload_store`] and [`OpenOptions::payload_path`]
//! place them in a different prefix, bucket or account instead.
//! Setting [`OpenOptions::payload_offload_threshold`] to `None`
//! disables offloading; payloads then stay inline regardless of size.
//!
//! # Inspecting and operating a queue
//!
//! The queue exposes its state for operational triage: [`Queue::list_queues`]
//! names every queue that has ever held a job, [`Queue::stats`] returns
//! per-state job counts for one queue, [`Queue::get_job`] looks up a single
//! job by ID in any state, [`Queue::list_jobs`] pages through one queue's
//! jobs in one lifecycle state and [`Queue::dead_jobs`] pages through the
//! dead-letter set. [`Queue::attempt_history`] returns a job's recorded
//! delivery history: one [`JobAttempt`] per settled attempt (retry,
//! dead-letter, lease expiry, completion on a queue with retention), so a
//! job that failed three different ways reports all three errors rather
//! than only the last. Interventions cover the common operator actions:
//! [`Queue::requeue_dead_job`] revives a dead job with a fresh retry budget,
//! [`Queue::cancel`] removes a pending or scheduled job (or requests
//! cooperative cancellation of a claimed one) and [`Queue::wake_scheduled`]
//! promotes a scheduled job before its `run_at`.
//!
//! Because a store is single-writer, an admin surface that mutates state
//! must live inside the process that owns the queue.
//! `examples/admin_http.rs` shows the resulting shape: a minimal HTTP
//! server inside the owning process, mapping these APIs onto JSON
//! endpoints.
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
//! The optional `metrics` feature emits queue health metrics (throughput,
//! dead rate, and claim/ack/enqueue latency histograms) through the
//! [`metrics`](https://docs.rs/metrics) facade. No exporter is pulled in;
//! the host process installs a recorder (for example Prometheus or an OTLP
//! bridge). Metrics are no-ops until a recorder is installed. Setting
//! [`OpenOptions::metrics_sample_interval`] additionally runs a background
//! sampler that emits per-queue depth and oldest-pending-age gauges, and
//! SlateDB's own storage metrics are forwarded into the same recorder.
//!
//! [SlateDB]: https://github.com/slatedb/slatedb

#![warn(missing_docs)]

mod claim_cursor;
mod clock;
mod error;
mod history;
mod job;
mod keys;
#[cfg(feature = "metrics")]
mod metrics_sampler;
mod obs;
mod payload_store;
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
pub use history::{AttemptOutcome, JobAttempt};
pub use job::{JobRecord, JobStatus};
pub use keys::MAX_QUEUE_NAME_LEN;
pub use queue::{
    AckEffects, CancelOutcome, DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD, EnqueueOptions, EnqueueRequest,
    EnqueueResult, JobPage, MAX_KV_VALUE_SIZE, OpenOptions, PRIORITY_HIGH, PRIORITY_LOW,
    PRIORITY_NORMAL, Queue, QueueConfig, WaitOutcome, WakeOutcome,
};
pub use stats::QueueStats;
pub use worker::{PermanentFailure, Worker, WorkerError, run_worker, run_worker_concurrent};

pub use slatedb::object_store;

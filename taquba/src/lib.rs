//! A durable, single-process task queue for Rust with **no stateful service
//! to operate**. Queue state lives directly in your object storage; compute
//! is stateless and replaceable. Because all state shares one transactional
//! store, queue operations compose atomically: a single transaction can
//! acknowledge a job, enqueue its follow-ups, and update caller-owned KV
//! state.
//!
//! Built on [SlateDB] and the [`object_store`] crate (local disk, S3, GCS,
//! Azure Blob, MinIO, etc.). All producers and workers for a given store run
//! inside one process and share an `Arc<Queue>`. Use Taquba for durable
//! background jobs whose state survives node loss, ephemeral disks, and
//! region failures, without operating a queue server or a separate state
//! layer (typically Postgres).
//!
//! # When Taquba fits
//!
//! Choose object storage as the backing store when at least one of the
//! following holds:
//!
//! - Payloads are big: payloads above a threshold are offloaded to their
//!   own objects with a transactional lifecycle, written once and
//!   deleted when the job leaves the queue.
//! - Backlogs are big or bursty: a million-job backlog costs only its
//!   storage and requires no maintenance.
//! - The workload is mostly idle: an idle queue incurs only storage
//!   costs, and per-tenant isolation is a key prefix. There is no
//!   server with a baseline cost that accrues while nothing runs.
//! - State crosses machine or account boundaries: a bucket is reachable
//!   from a laptop, a CI runner or a spot instance with credentials
//!   alone, so compute is replaceable by construction.
//! - The work coordinates data already in the bucket: queue state,
//!   payloads, results and history share one lifecycle, one restore
//!   domain and one access policy.
//! - The execution trail must be tamper-proof: attempt history, results
//!   and terminal markers commit in the same transactions as the work,
//!   and object lock makes the trail immutable by storage policy.
//! - Steps are expensive and IO-bound: for steps that take much longer
//!   than an object-store write the per-transition durability cost is
//!   negligible, a retried run does not re-pay memoized steps, and
//!   expensive steps sit far from the commit-rate ceiling. LLM calls
//!   and rate-limited external APIs are the strongest fit.
//!
//! # When Taquba does not fit
//!
//! - The queue must share the application's database transactions: a
//!   database-backed queue enqueues jobs and commits their effects
//!   inside the application's own transactions.
//! - A worker fleet across machines: Taquba is single-writer, so one
//!   process owns each store. This caps compute that runs in the
//!   worker process itself at one node; steps that call remote
//!   services scale with async concurrency inside the single process.
//! - Latency-sensitive paths: every durable transition is an
//!   object-store write, so end-to-end latency is floored by a PUT
//!   round trip. [`OpenOptions::wal_object_store`] places the
//!   write-ahead log on a separate store; one with lower write
//!   latency lowers that floor while compacted state stays on the
//!   primary store.
//! - Cheap jobs at high volume: throughput is bound by the durable
//!   commit rate, and sub-millisecond jobs pay the durability cost on
//!   every transition.
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
//! use taquba::{JobRecord, LeaseHandle, Worker, WorkerError};
//!
//! struct EmailWorker;
//!
//! impl Worker for EmailWorker {
//!     async fn process(
//!         &self,
//!         job: &JobRecord,
//!         _lease: &LeaseHandle,
//!     ) -> Result<(), WorkerError> {
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
//! abandoned to the reaper; the drain has no internal bound, so the
//! process supervisor's kill timeout bounds a restart.
//!
//! Settlement failures do not stop the loop: when a job outlives its
//! lease and the reaper requeues it, the late acknowledgement fails
//! with [`Error::ClaimLost`], the loop logs it and continues, and the
//! redelivered attempt settles the job instead. Errors on the claim
//! path still stop the loop. A long-running [`Worker::process`] can
//! avoid the requeue by extending its own lease through the
//! [`LeaseHandle`] it receives: call [`LeaseHandle::ensure_at_least`]
//! at progress points, or once with a slow call's timeout before
//! issuing it. The claim stays with the loop, which settles the job
//! when `process` returns. See `examples/long_running.rs` for both
//! patterns.
//!
//! A worker can implement [`Worker::process_with_effects`] instead of
//! [`Worker::process`] to return [`SettlementEffects`]: follow-up enqueues and
//! caller KV changes the loop applies atomically with the job's
//! acknowledgement via [`Queue::ack_with`]. A failing worker can attach
//! effects by returning the error wrapped in [`FailWith`]: the loop
//! applies them atomically with a settlement that dead-letters the job
//! ([`Queue::dead_letter_with`] for a [`PermanentFailure`], the
//! dead-letter branch of [`Queue::nack_with`] otherwise) and discards
//! them when the job is retried.
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
//! Claims serialise per queue. The claim lock is held across the scan
//! and the commit, so a queue's claim rate is the batch size divided by
//! the scan-and-commit latency and does not increase with the number of
//! workers: additional workers on one queue wait on the lock, raising
//! claim latency in proportion without raising throughput. Batching
//! amortizes one lock hold across the batch, and sharding work across
//! queues raises that limit further, the lock being per queue.
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
//! or [`Queue::ack_with`] call. Note the dedup interaction: a
//! `dedup_key` hit discards the accompanying `kv_writes`, so derive
//! them deterministically from the dedup key (see
//! [`Queue::enqueue_with_kv`]).
//!
//! Standalone primitives exist for state whose lifecycle is not tied
//! to a single queue transition: [`Queue::kv_put`] writes an entry
//! durably on its own, [`Queue::kv_delete`] removes one (terminal
//! cleanup), [`Queue::kv_compare_delete`] consumes an entry only if it
//! still holds the value the caller read, so a concurrent replacement
//! is never deleted by mistake, and [`Queue::kv_compare_put`] writes
//! only if the key still holds an expected value (or is still absent),
//! the read-modify-write primitive that makes concurrent updates of
//! one entry lose no writes. [`Queue::kv_scan`] lists entries under a
//! key prefix in pages, for enumerating live state and for exporting
//! the namespace, and [`Queue::kv_entries`] reads the same listing as
//! one stream. [`Queue::commit_effects`] applies a
//! [`SettlementEffects`] (enqueues, KV writes and deletes) as one
//! transaction with no job transition, for state that must move in one
//! step when no transition of its own carries it.
//!
//! [`Queue::ack_with`] extends the same atomicity to settlement: it
//! acknowledges a claimed job and, in the same transaction, enqueues
//! follow-up jobs and applies caller KV writes and deletes. If the
//! job's lease expired and the claim is gone, the call fails and
//! nothing is applied, so a chained job exists only if the settlement
//! that created it won.
//!
//! [`Queue::dead_letter_with`], [`Queue::nack_with`] and
//! [`Queue::cancel_with`] extend it to the failure and cancellation
//! transitions: dead-lettering a job, the attempts-exhausted branch of
//! a nack and the removal of a pending or scheduled job each apply the
//! same effects atomically with the transition. A worker running under
//! the worker loop attaches effects to a failure by returning the
//! error wrapped in [`FailWith`]; the loop applies them with a
//! dead-lettering settlement and discards them when the job is
//! retried.
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
//! jobs in one lifecycle state ([`Queue::jobs`] reads the same listing
//! as one stream) and [`Queue::dead_jobs`] pages through the dead-letter
//! set. [`Queue::attempt_history`] returns a job's recorded
//! delivery history: one [`JobAttempt`] per settled attempt (retry,
//! dead-letter, lease expiry, interruption at open, operator requeue,
//! completion on a queue with retention), so a job that failed three
//! different ways reports all three errors rather than only the last.
//! Interventions cover the common operator actions:
//! [`Queue::requeue_dead_job`] revives a dead job with a fresh retry budget,
//! [`Queue::cancel`] removes a pending or scheduled job (or requests
//! cooperative cancellation of a claimed one) and [`Queue::wake_scheduled`]
//! promotes a scheduled job before its `run_at`.
//!
//! Because a store is single-writer, an admin surface that mutates state
//! must live inside the process that owns the queue.
//! `examples/admin_http.rs` demonstrates the pattern: a minimal HTTP
//! server inside the owning process, mapping these APIs onto JSON
//! endpoints.
//!
//! # Observing from another process
//!
//! The single-writer rule constrains only writes. [`QueueReader`]
//! opens the same store path from any process with bucket credentials
//! and serves the queue's read-only API: [`QueueReader::stats`],
//! [`QueueReader::list_queues`], [`QueueReader::list_jobs`],
//! [`QueueReader::dead_jobs`], [`QueueReader::get_job`],
//! [`QueueReader::attempt_history`] and the KV reads. Dashboards, CLIs
//! and health checks observe a live queue without an admin endpoint
//! inside the worker process.
//!
//! A reader is observation only: it takes no writes, offers no lease
//! view and reads a lagging view of the store. The lag
//! is bounded by the writer's flush interval plus the reader's
//! [`ReaderOptions::manifest_poll_interval`], and the reader sees whole
//! commits or nothing. A job reported `Claimed` means a claim was taken
//! and no settlement is visible yet; whether the claim is live is state
//! inside the writer process, which the reader cannot see.
//!
//! By default a reader maintains a checkpoint so the objects its view
//! references are protected from garbage collection. Checkpoint
//! refreshes are manifest writes, so this mode requires write
//! credentials to the bucket; it never touches the writer's epoch and
//! does not fence the writer. [`ReaderMode::FollowLatest`] performs no
//! object-store writes, for read-only credentials, at the cost of a
//! read failing when garbage collection removes an object under an
//! aged view. Two caveats: reader and writer must run the same taquba
//! minor version, because the layout may change between minors and no
//! version stamp is stored, and opening a reader against a path no
//! writer has ever created fails with [`Error::StoreNotInitialized`],
//! which a health check racing the first deployment must expect.
//!
//! A reader also answers whether a writer process is alive, the first
//! question admin tooling asks before a destructive act such as
//! opening the store as a writer, which fences a live one.
//! [`QueueReader::last_store_activity`] reads the manifest's newest L0
//! flush timestamp and the writer epoch for display, plus the durable
//! sequence number; a destructive operation watches the sequence
//! number for advance over a few poll intervals, a judgment that
//! involves no clock comparison. With
//! [`OpenOptions::liveness_heartbeat`] set, the writer additionally
//! commits a beat on an interval and
//! [`QueueReader::writer_heartbeat`] reads the latest one. A beat is
//! an ordinary store commit, so a writer that lost the store to a
//! successor stops producing observable beats at its next flush: a
//! fresh beat proves the process that owns the store is alive, and
//! proves nothing about that process's workers. A clean
//! [`Queue::close`] commits a final beat marked closed, so a stale
//! closed beat indicates a deliberate shutdown rather than a vanished
//! writer.
//!
//! To make job outcomes observable across processes, settle them into
//! the KV namespace: [`Queue::ack_with`] writes outcome entries
//! atomically with the settlement (and [`Queue::enqueue_with_kv`] maps
//! caller identifiers to job ids at submit), and the reader's
//! [`QueueReader::kv_get`] / [`QueueReader::kv_scan`] serve them from
//! any process.
//!
//! # Background tasks
//!
//! [`Queue::open`] spawns background tokio tasks for the lifetime of the
//! handle:
//!
//! - **Reaper** - re-queues jobs whose lease expired and runs the done /
//!   dead-letter retention sweeps (interval: [`OpenOptions::reaper_interval`]).
//! - **Scheduler** - promotes scheduled jobs whose `run_at` has passed
//!   (interval: [`OpenOptions::scheduler_interval`]).
//! - **Metrics sampler** - emits depth and oldest-pending-age gauges, only
//!   when [`OpenOptions::metrics_sample_interval`] is set.
//! - **Liveness heartbeat** - commits a beat readable by a [`QueueReader`]
//!   from another process, only when [`OpenOptions::liveness_heartbeat`]
//!   is set.
//!
//! Opening a queue also re-queues every job left claimed by a previous
//! process: crash recovery happens at open, and lease expiry detects a
//! delivery that stops progressing while the process lives. A job
//! interrupted this way consumes one attempt at its next claim, so
//! `max_attempts` also counts hard restarts of a long job.
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

mod background;
mod claim_cursor;
mod clock;
mod completion;
mod effects;
mod error;
mod history;
mod job;
mod keys;
mod kv;
mod lease;
mod lease_registry;
mod liveness;
#[cfg(feature = "metrics")]
mod metrics_sampler;
mod obs;
mod options;
mod payload_store;
mod queue;
mod queue_core;
mod read;
mod reader;
mod reaper;
mod scheduler;
mod stats;
#[cfg(test)]
mod test_util;
mod txn;
/// Worker-loop primitives: the [`worker::Worker`] trait, plus the
/// [`worker::run_worker`] / [`worker::run_worker_concurrent`] drivers that
/// own the claim -> process -> ack/nack lifecycle and graceful shutdown.
pub mod worker;

pub use clock::{Clock, MockClock, SystemClock};
pub use effects::{EnqueueRequest, SettlementEffects};
pub use error::{Error, Result};
pub use history::{AttemptOutcome, JobAttempt};
pub use job::{Claim, JobRecord, JobStatus};
pub use keys::MAX_QUEUE_NAME_LEN;
pub use kv::{KvPage, MAX_KV_VALUE_SIZE};
pub use lease::LeaseHandle;
pub use liveness::{StoreActivity, WriterHeartbeat};
pub use options::{
    DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD, EnqueueOptions, OpenOptions, PRIORITY_HIGH, PRIORITY_LOW,
    PRIORITY_NORMAL, QueueConfig,
};
pub use queue::{
    CancelOutcome, EnqueueResult, JobPage, NackOutcome, Queue, WaitOutcome, WakeOutcome,
};
pub use reader::{QueueReader, ReaderMode, ReaderOptions};
pub use stats::QueueStats;
pub use worker::{
    FailWith, PermanentFailure, Worker, WorkerError, run_worker, run_worker_concurrent,
};

pub use slatedb::object_store;

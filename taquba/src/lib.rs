//! A durable, single-process task queue for Rust **without a stateful service
//! to operate**. Queue state lives directly in the application's object
//! storage, and compute is stateless and replaceable.
//!
//! > The foundation of the [Taquba ecosystem](https://github.com/micllam/taquba).
//! > The workspace README describes the durable-execution workflow runtime,
//! > cron, jobs and webhooks crates that build on this queue.
//!
//! Built on [SlateDB] and the [`object_store`] crate. Use Taquba for durable
//! background jobs whose state persists across node loss, ephemeral disks and
//! region failures, without operating a queue server or a database server.
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
//! `examples/quickstart.rs` is a runnable version, and `examples/worker.rs`
//! runs the worker loop with retries and dead-letter inspection.
//!
//! # Cargo features
//!
//! The in-memory and local-disk stores work without a feature flag. A cloud
//! store requires the feature of its backend:
//!
//! ```bash
//! cargo add taquba --features aws    # S3 / MinIO
//! cargo add taquba --features gcp    # Google Cloud Storage
//! cargo add taquba --features azure  # Azure Blob
//! ```
//!
//! [`open_url`] opens the store of a URL such as `s3://bucket/prefix`, with the
//! provider options from the environment.
//!
//! The optional `metrics` feature emits throughput, dead-rate and latency
//! metrics through the [`metrics`](https://docs.rs/metrics) facade, and
//! SlateDB's storage metrics through the same recorder. The crate does not
//! include an exporter, and the metrics are no-ops until the host process
//! installs a recorder. Spans and log events go through
//! [`tracing`](https://docs.rs/tracing), with or without the feature.
//!
//! # Job lifecycle
//!
//! ```text
//! enqueue at a future run_at    enqueue
//!     │                            │
//!     ▼      run_at, or a wake     ▼       claim             ack
//! scheduled ──────────────────▶ pending ────────▶ claimed ──────────▶ done
//!     ▲                            ▲                 │
//!     │                            └─────────────────┤ lease expiry
//!     └──────────────────────────────────────────────┤ nack, retried later
//!                                                    └──────────────▶ dead
//!                                                      nack at the
//!                                                      attempt limit
//! ```
//!
//! A scheduled job waits until its `run_at`, unless a call, a signal to its id
//! or a count reaching zero wakes it earlier. A worker keeps a claimed job
//! under a lease, and the job returns to pending if the lease expires before
//! the settlement. Delivery is therefore at least once, so a worker must be
//! idempotent.
//!
//! A settlement commits atomically with its effects, such as follow-up jobs and
//! writes to the caller's state. One process owns a store and commits every
//! transition, and other processes send their requests through an inbox.
//!
//! # When to choose Taquba
//!
//! Choose object storage as the backing store when at least one of the
//! following is true:
//!
//! - Payloads are big: payloads above a threshold are offloaded to their
//!   own objects with a transactional lifecycle, written once and
//!   deleted when the job leaves the queue.
//! - Backlogs are big or bursty: a million-job backlog costs only its
//!   storage and does not require maintenance.
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
//! - Steps are slow and IO-bound: for steps that take much longer than an
//!   object-store write, the per-transition durability cost is negligible
//!   and a retried run does not re-pay memoized steps. Such steps also sit
//!   far from the commit-rate ceiling. LLM calls and rate-limited external
//!   APIs are the strongest fit.
//!
//! # When not to choose Taquba
//!
//! - The queue must share the application's database transactions: a
//!   database-backed queue enqueues jobs and commits their effects
//!   inside the application's own transactions.
//! - A worker fleet across machines: Taquba is single-writer, so one
//!   process owns each store. This caps compute that runs in the
//!   worker process itself at one node. Steps that call remote services
//!   scale with async concurrency inside the single process.
//! - Latency-sensitive paths: every durable transition is an object-store
//!   write, so a PUT round trip bounds the end-to-end latency from below. A
//!   write-ahead log on a store with lower write latency
//!   ([`OpenOptions::wal_object_store`]) lowers that bound.
//! - Short jobs at high volume: throughput is bound by the durable
//!   commit rate, and sub-millisecond jobs pay the durability cost on
//!   every transition.
//!
//! Measured performance numbers, with the environment and commit that produced
//! them, are recorded in `taquba-bencher/RESULTS.md` in the source repository.
//!
//! # Stability
//!
//! Taquba is pre-1.0. A minor release can break the Rust API and the on-disk
//! format, and a patch release preserves both. An upgrade to a new minor
//! version requires a drained queue or an empty store.
//!
//! [SlateDB]: https://github.com/slatedb/slatedb

#![warn(missing_docs)]

mod background;
mod claim_cursor;
mod clock;
mod codec;
mod completion;
mod count;
mod effects;
mod error;
mod expiry;
mod history;
mod inbox;
mod job;
mod keys;
mod kv;
mod lease;
mod lease_registry;
mod liveness;
#[cfg(feature = "metrics")]
mod metrics_sampler;
mod objects;
mod obs;
mod options;
mod payload_store;
mod queue;
mod queue_core;
mod reader;
mod reaper;
mod scheduler;
mod stats;
mod store_url;
#[cfg(test)]
mod test_util;
mod time_bound;
mod txn;
mod ulids;
mod view;
/// Worker-loop primitives: the [`worker::Worker`] trait, plus the
/// [`worker::run_worker`] / [`worker::run_worker_concurrent`] drivers that own
/// the claim -> process -> ack/nack lifecycle and graceful shutdown, and the
/// [`worker::WorkerHandle`] that runs a loop as a Tokio task.
pub mod worker;

pub use clock::{Clock, MockClock, SystemClock};
pub use effects::{EnqueueRequest, KvCompare, SettlementEffects};
pub use error::{Error, FormError, Result, StoreUrlError};
pub use expiry::{Expired, ExpiryIndex};
pub use history::{AttemptOutcome, JobAttempt};
pub use inbox::entry::{
    CancelEntry, EnqueueEntry, EntryKind, InboxEntry, MAX_INBOX_ENTRY_SIZE, MAX_INBOX_ID_LEN,
    SignalEntry,
};
pub use inbox::listing::PendingEntry;
pub use inbox::record::{InboxOutcome, InboxRecord, Rejection};
pub use inbox::sender::{EntryRead, InboxSender};
pub use inbox::{
    DEFAULT_INBOX_INTERVAL, DEFAULT_INBOX_RECORD_RETENTION, DEFAULT_INBOX_TOUCH_POLL_INTERVAL,
    INBOX_UNREADABLE_AFTER, InboxOptions, default_inbox_path,
};
pub use job::{Claim, JobRecord, JobStatus};
pub use keys::{MAX_QUEUE_NAME_LEN, QueueName};
pub use kv::{KvOrder, KvPage, KvRange, MAX_KV_VALUE_SIZE};
pub use lease::{ClaimFence, LeaseHandle};
pub use liveness::{StoreActivity, WriterHeartbeat};
pub use options::{
    DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD, EnqueueOptions, OpenOptions, PRIORITY_HIGH, PRIORITY_LOW,
    PRIORITY_NORMAL, QueueConfig,
};
pub use queue::{
    CancelOutcome, ClaimOutcome, EnqueueResult, JobPage, NackOutcome, Queue, SignalOutcome,
    WaitOutcome, WakeOutcome,
};
pub use reader::{QueueReader, ReaderMode, ReaderOptions};
pub use stats::QueueStats;
pub use store_url::open_url;
pub use view::QueueView;
pub use worker::{
    FailWith, PermanentFailure, Worker, WorkerError, WorkerHandle, run_worker,
    run_worker_concurrent,
};

pub use slatedb::object_store;

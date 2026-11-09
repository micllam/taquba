//! A durable task queue that stores its state in object storage through
//! [SlateDB] and the [`object_store`] crate. One process owns a store and
//! commits every transition, and producers and workers share one [`Queue`]. The
//! [README](https://github.com/micllam/taquba/tree/master/taquba) states when
//! the queue is the correct choice, and `taquba-workflow` runs durable
//! multi-step processes on it.
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
//! [`Queue::enqueue_with`] takes [`EnqueueOptions`]: a `run_at`, a priority, a
//! dedup key and a wait for a signal or for a count to reach zero. A scheduled
//! job becomes pending at its `run_at`, or earlier through
//! [`Queue::wake_scheduled`], [`Queue::signal`] or its count reaching zero.
//!
//! [`Queue::claim`] puts a job under a lease, and [`Queue::renew_lease`]
//! extends it. The job returns to pending if the lease expires before the
//! settlement. Delivery is therefore at least once, so a worker must be
//! idempotent.
//!
//! [`Queue::ack`], [`Queue::nack`] and [`Queue::dead_letter`] settle a claim. A
//! nack retries the job after a backoff until the attempt limit of its
//! [`QueueConfig`], and then dead-letters it. [`Queue::cancel`] removes a
//! pending or scheduled job and fires the cancellation token of a claimed job.
//! [`Queue::requeue_dead_job`] returns a dead job to pending.
//!
//! # API map
//!
//! - **Opening a store.** [`Queue::open_with_options`] takes [`OpenOptions`]
//!   and a [`QueueConfig`] per queue. [`open_url`] builds the object store from
//!   a URL such as `s3://bucket/prefix` and the environment.
//! - **Workers.** The [`worker`] module runs the loop of claims and
//!   settlements: [`Worker`], [`run_worker`], [`run_worker_concurrent`] and
//!   [`WorkerHandle`]. A [`PermanentFailure`] dead-letters a job without a
//!   retry.
//! - **Settlement effects.** [`Queue::ack_with`], [`Queue::nack_with`],
//!   [`Queue::dead_letter_with`] and [`Queue::enqueue_with_effects`] commit
//!   [`SettlementEffects`] in the transaction of the call. The effects are
//!   enqueues, KV writes under [`KvCompare`] conditions, count changes and
//!   [`ExpiryIndex`] entries. A [`ClaimFence`] makes effects conditional on a
//!   live claim.
//! - **Reads.** [`QueueView`], from [`Queue::view`], reads jobs, attempt
//!   history, KV entries and statistics. [`QueueReader`] gives another process
//!   a lagging copy of the view and reads the [`WriterHeartbeat`].
//! - **Other processes.** An [`InboxSender`] sends enqueues, cancellations and
//!   signals, which the writer applies.
//! - **Time.** The queue reads every time from its [`Clock`]. [`MockClock`]
//!   moves time by hand in tests.
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
//! The optional `metrics` feature emits throughput, dead-rate and latency
//! metrics through the [`metrics`](https://docs.rs/metrics) facade, and
//! SlateDB's storage metrics through the same recorder. The crate does not
//! include an exporter, and the metrics are no-ops until the host process
//! installs a recorder. Spans and log events go through
//! [`tracing`](https://docs.rs/tracing), with or without the feature.
//!
//! # Stability
//!
//! Taquba is pre-1.0. A minor release can break the Rust API and the on-disk
//! format, whereas a patch release preserves both. An upgrade to a new minor
//! version requires a drained queue or an empty store.
//!
//! [SlateDB]: https://github.com/slatedb/slatedb

#![warn(missing_docs)]

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

mod background;
mod claim;
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

pub use claim::Claim;
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
pub use job::{JobRecord, JobStatus};
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

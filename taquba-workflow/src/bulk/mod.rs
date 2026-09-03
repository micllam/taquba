//! Bulk multi-step processing on the workflow runtime.
//!
//! [`BulkRunner`] runs one [`Pipeline`] over many input items in parallel,
//! inside a single process, with per-item memoization, retry, streamed
//! output, and a rolled-up cost report. It is the per-batch orchestrator for
//! workloads that fan out 10-1000x per run: bulk LLM jobs (classify, look up,
//! draft, check, refine over thousands of tickets), document/OCR pipelines,
//! data enrichment, parameter sweeps. The pipeline contract is workload
//! agnostic.
//!
//! # Execution model: one item, one run, one step
//!
//! Each input item becomes one [`crate`] run whose single step invokes
//! [`Pipeline::run`]. The pipeline's own logical steps live inside that method
//! as [`Memo::memoized`](crate::Memo::memoized) calls on the item's
//! [`memo`](crate::Delivery::memo). Taquba delivers at-least-once, so a step
//! may run again if its lease expires before it acks; memoization makes that
//! replay inexpensive, because each completed logical step returns its cached
//! result instead of repeating a paid call. A pipeline error retries with
//! backoff and then dead-letters the item (terminating it failed); the rest of
//! the batch is unaffected.
//!
//! That memo is the run's per-step memo applied at a finer
//! granularity: the item's single step holds one memo entry per logical
//! phase, so the phases of [`Pipeline::run`] resume individually even
//! though the workflow sees one step.
//!
//! # Content-addressed memoization
//!
//! Use [`Memo::memoized_by_content`](crate::Memo::memoized_by_content) when
//! the natural memo key is a serialized input value:
//!
//! ```ignore
//! #[derive(serde::Serialize)]
//! struct LookupKey<'a> {
//!     operation: &'static str,
//!     query: &'a str,
//! }
//!
//! let key = LookupKey {
//!     operation: "lookup",
//!     query: &ctx.input.body,
//! };
//! let response = ctx
//!     .memo
//!     .memoized_by_content(&key, async {
//!         Ok::<_, StepError>(lookup(&ctx.input.body).await?)
//!     })
//!     .await?;
//! ```
//!
//! The helper serializes the key as MessagePack, hashes it with SHA-256,
//! and uses the digest inside the item's existing workflow memo namespace.
//! The entry remains scoped to one item run; this is not a cross-item cache.
//! Include an operation name in the serialized key when multiple logical
//! operations may receive identical inputs.
//!
//! # Single process, remote work per step
//!
//! The orchestrator is single-process by design: SlateDB allows one writer
//! per store, so all producers and workers for a batch share one
//! `Arc<Queue>` (see the Taquba docs). That is not a throughput ceiling for
//! bulk work. Each step's expensive operation is a call to a remote service
//! (an LLM API, an OCR service), so the process is I/O-bound and one host
//! sustains hundreds of concurrent items. The remote call runs elsewhere and
//! its response is memoized on return.
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use serde::{Deserialize, Serialize};
//! use taquba::{Queue, object_store::memory::InMemory};
//! use taquba_workflow::bulk::{BulkRunner, BulkCtx, CostReport, Pipeline};
//! use taquba_workflow::StepError;
//!
//! #[derive(Serialize, Deserialize)]
//! struct Ticket { id: String, body: String }
//!
//! #[derive(Serialize, Deserialize)]
//! struct Processed { id: String, classification: String }
//!
//! struct TicketPipeline;
//!
//! impl Pipeline for TicketPipeline {
//!     type Input = Ticket;
//!     type Output = Processed;
//!     type Error = StepError;
//!
//!     async fn run(&self, ctx: &BulkCtx<Ticket>) -> Result<Processed, StepError> {
//!         let classification = ctx
//!             .memoized_with_cached_cost("classify", async {
//!                 let cost = CostReport::new();
//!                 cost.record("llm_calls", 1.0);
//!                 Ok::<_, StepError>(("billing".to_string(), cost))
//!             })
//!             .await?;
//!         Ok(Processed { id: ctx.input.id.clone(), classification })
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(InMemory::new());
//! let queue = Arc::new(Queue::open(store.clone(), "db").await?);
//!
//! let mut bulk = BulkRunner::builder(queue, store, TicketPipeline)
//!     .key_fn(|t| t.id.clone())
//!     .max_concurrent(200)
//!     .build();
//! let worker = bulk.spawn(std::future::pending::<()>());
//!
//! let inputs = vec![
//!     Ticket { id: "t1".into(), body: "help".into() },
//!     Ticket { id: "t2".into(), body: "refund".into() },
//! ];
//! let report = bulk.run(inputs).await?;
//! println!("{}/{} succeeded", report.succeeded, report.total);
//! worker.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! # Cost tracking
//!
//! Pipelines report arbitrary named metrics via [`BulkCtx::record_cost`]
//! (token counts, paid-API units, compute-seconds, dollars). Per-item totals
//! roll up into [`ProgressSnapshot::cost`] and [`BatchReport::cost`], so the
//! batch cost is visible live and in the final report. See [`CostReport`].
//! When counters are produced inside a memoized closure, return `(value,
//! cost)` from [`BulkCtx::memoized_with_cached_cost`] or
//! [`BulkCtx::memoized_by_content_with_cached_cost`] so the same counters are
//! recorded on a cache hit.
//!
//! # Application KV effects and reads
//!
//! [`BulkCtx`] dereferences to the item's [`Delivery`](crate::Delivery).
//! Its [`effects`](crate::Delivery::effects) stage writes and deletes to
//! Taquba's caller KV namespace, applied atomically with the item's
//! successful completion, so per-item application state (a result
//! marker, a status row) cannot diverge from the item's outcome on a
//! crash; a failing item applies nothing. Its [`kv`](crate::Delivery::kv)
//! reads a committed value from the same namespace; effects staged by the
//! running item are excluded. Both are [`crate`]'s KV effects, whose
//! staging rules ([`EffectsHandle`](crate::EffectsHandle)) apply unchanged.
//!
//! # Batches
//!
//! A [`BulkRunner`] spawns one worker ([`BulkRunner::spawn`]) and runs
//! batches over it, several at a time if wanted: [`BulkRunner::run`] creates a
//! batch with a generated id, [`BulkRunner::batch`] names one, and
//! [`Batch::run`] submits the batch's items and waits until every one has
//! terminated. Dropping that future stops the wait; the items keep
//! running on the worker and keep their durable state. Items are
//! identified within a batch by key
//! ([`BulkRunnerBuilder::key_fn`], or the positional `item-{i}` default), and an
//! item's workflow run id is the SHA-256 digest of `{batch_id}/{key}`, so
//! batches never share run state. Each item writes an outcome record to its
//! run memo before its settlement. A later [`Batch::run`] of the same batch
//! reads those records: an item whose record is a success is counted and written
//! to the sink from the record without running again, and an item whose
//! record is a failure runs again. [`BatchReport::failed_keys`] is the set a
//! later [`Batch::run`] re-executes.
//!
//! A batch is a run group of the runtime. Before submitting any item,
//! [`Batch::run`] writes the batch's manifest (its keys and serialized
//! inputs) to `<memo_prefix>/groups/<batch_id>/manifest`; a
//! [`Batch::run`] of an existing batch with a different item set is
//! rejected with [`Error::GroupMismatch`](crate::Error::GroupMismatch).
//! [`Batch::resume`] runs a batch from its manifest alone: completed
//! items are answered from their outcome records, items still queued
//! continue, and the rest run.
//!
//! Each item has a member record under
//! `workflow/groups/<batch_id>/<key>` in the queue's KV namespace,
//! written with its submission and rewritten by the settlement that
//! commits its terminal outcome (the acknowledgement of a success, the
//! dead-lettering settlement of a failure, or the cancellation) with its
//! status, error and, for a success or a failure, the cost its step
//! recorded. An item cancelled from outside runs again in the next
//! [`Batch::run`] of the batch. [`Batch::status`] reads the manifest and
//! the member records, so a batch's durable state is available without
//! running it and from another process through the same prefix.
//! [`Batch::run`] observes each item's termination through the queue's
//! in-process completion notification and reads the item's outcome
//! record to stream its output; an item runs as one queue job.
//!
//! A batch's state is retained until [`Batch::forget`] removes it, or,
//! under [`BulkRunnerBuilder::batch_retention`], until the window after
//! the batch's completion has passed: a completing [`Batch::run`] writes
//! a terminal marker to `workflow/group-terminals/<ts>/<batch_id>`, and
//! the worker removes the batches whose markers have expired when it
//! starts and on every retention interval after that.
//!
//! # Failure policy
//!
//! Per-item failures are recorded, not fatal: each failed item is written to
//! the output sink with its error and its key is collected on
//! [`BatchReport::failed_keys`]. Set [`BulkRunnerBuilder::fail_threshold`] to
//! make [`Batch::run`] return an
//! [`Error::FailureThresholdExceeded`](crate::Error::FailureThresholdExceeded)
//! when the
//! share of failures crosses a percentage, so a silent mass failure
//! surfaces.
//!
//! # Input and output
//!
//! Line-delimited JSON: [`read_jsonl`] decodes inputs and
//! [`JsonlSink`] writes one result record per line. Both sides are traits
//! ([`OutputSink`]), so other formats can be added without touching the
//! runner. The default sink is [`NullSink`], for pipelines whose results are
//! side effects.

mod batch;
mod cost;
mod io;
mod pipeline;
mod progress;
mod runner;

pub use batch::{Batch, BulkRunner, BulkRunnerBuilder};
pub use cost::CostReport;
pub use io::{JsonlSink, NullSink, OutputRecord, OutputSink, read_jsonl};
pub use pipeline::{BulkCtx, Pipeline};
pub use progress::{BatchReport, BatchStatus, ProgressSnapshot};

//! Bulk multi-step processing on the workflow runtime.
//!
//! [`Bulk`] runs one [`Pipeline`] over many input items in parallel,
//! inside a single process, with per-item memoization, retry, streamed
//! output, and a rolled-up cost report. It is the per-batch orchestrator for
//! workloads that fan out 10-1000x per run: bulk LLM jobs (classify, look up,
//! draft, check, refine over thousands of tickets), document/OCR pipelines,
//! data enrichment, parameter sweeps. The pipeline contract is workload
//! agnostic.
//!
//! # Execution model: one item, one run, one step
//!
//! Each input item becomes one [`crate`] run whose single step
//! invokes [`Pipeline::run`]. The pipeline's own logical steps live inside
//! that method as [`Memo::memoized`](crate::Memo::memoized) calls on
//! [`BulkCtx::memo`]. Taquba delivers at-least-once, so
//! a step may run again if its lease expires before it acks; memoization makes
//! that replay inexpensive, because each completed logical step returns its
//! cached result instead of repeating a paid call. A pipeline error retries with
//! backoff and then dead-letters the item (terminating it failed); the rest of
//! the batch is unaffected.
//!
//! [`BulkCtx::memo`] is the run's per-step memo applied at a finer
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
//!     .memo()
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
//! use taquba_workflow::bulk::{Bulk, BulkCtx, CostReport, Pipeline};
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
//! let bulk = Bulk::builder(queue, store, TicketPipeline)
//!     .key_fn(|t| t.id.clone())
//!     .max_concurrent(200)
//!     .build();
//!
//! let inputs = vec![
//!     Ticket { id: "t1".into(), body: "help".into() },
//!     Ticket { id: "t2".into(), body: "refund".into() },
//! ];
//! let report = bulk.run(inputs).await?;
//! println!("{}/{} succeeded", report.succeeded, report.total);
//! # Ok(()) }
//! ```
//!
//! # Cost tracking
//!
//! Pipelines report arbitrary named metrics via [`BulkCtx::record_cost`]
//! (token counts, paid-API units, compute-seconds, dollars). Per-item totals
//! roll up into [`ProgressSnapshot::cost`] and [`BulkReport::cost`], so the
//! batch cost is visible live and in the final report. See [`CostReport`].
//! When counters are produced inside a memoized closure, return `(value,
//! cost)` from [`BulkCtx::memoized_with_cached_cost`] or
//! [`BulkCtx::memoized_by_content_with_cached_cost`] so the same counters are
//! recorded on a cache hit.
//!
//! # Application KV effects and reads
//!
//! [`BulkCtx::effects`] stages writes and deletes to Taquba's caller KV
//! namespace, applied atomically with the item's successful completion,
//! so per-item application state (a result marker, a status row) cannot
//! diverge from the item's outcome on a crash; a failing item applies
//! nothing. [`BulkCtx::kv_get`] reads a committed value from the same
//! namespace; effects staged by the running item are excluded. Both
//! surfaces delegate to [`crate`]'s KV effects, whose staging
//! rules ([`EffectsHandle`](crate::EffectsHandle)) apply unchanged.
//!
//! # Batches
//!
//! Every run is a batch: [`Bulk::run`] creates one with a generated id,
//! [`Bulk::batch`] names one. Items are identified within a batch by key
//! ([`BulkBuilder::key_fn`], or the positional `item-{i}` default), and an
//! item's workflow run id is the SHA-256 digest of `{batch_id}/{key}`, so
//! batches never share run state. Each item writes an outcome record to its
//! run memo before its settlement. A second run of the same batch reads
//! those records: an item whose record is a success is counted and written
//! to the sink from the record without running again, and an item whose
//! record is a failure runs again. [`BulkReport::failed_keys`] is the set a
//! second run re-executes.
//!
//! Before submitting any item, a run writes the batch's manifest (its keys
//! and serialized inputs) to `<memo_prefix>/batches/<batch_id>/manifest`;
//! a run of an existing batch with a different item set is rejected with
//! [`Error::BatchMismatch`]. [`Batch::resume`] drives a batch from its
//! manifest alone: completed items are answered from their outcome
//! records, items still queued continue, and the rest run.
//!
//! Each terminal notification writes the item's marker (status, error,
//! cost) to `workflow/bulk/batches/<batch_id>/items/<key>` in the queue's
//! KV namespace, committed with the notification's acknowledgement.
//! [`Batch::status`] reads the manifest and the markers, so a batch's
//! durable state is available without running it and from another
//! process through the same prefix.
//!
//! A batch's state is retained until [`Batch::forget`] removes it, or,
//! under [`BulkBuilder::batch_retention`], until the window after the
//! batch's completion has passed: a completing run writes a terminal
//! marker to `workflow/bulk/terminals/<ts>/<batch_id>`, and every later run
//! removes the batches whose markers have expired before submitting its
//! items and again on every retention interval while it runs.
//!
//! # Failure policy
//!
//! Per-item failures are recorded, not fatal: each failed item is written to
//! the output sink with its error and its key is collected on
//! [`BulkReport::failed_keys`]. Set [`BulkBuilder::fail_threshold`] to
//! turn the whole run into an [`Error::FailureThresholdExceeded`] when the
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
mod error;
mod io;
mod manifest;
mod pipeline;
mod progress;
mod runner;

pub use batch::{Batch, Bulk, BulkBuilder};
pub use cost::CostReport;
pub use error::{Error, Result};
pub use io::{JsonlSink, NullSink, OutputRecord, OutputSink, read_jsonl};
pub use pipeline::{BulkCtx, Pipeline};
pub use progress::{BatchStatus, BulkReport, ProgressSnapshot};

//! Bulk multi-step processing on top of [`taquba_workflow`].
//!
//! `taquba-bulk` runs one [`Pipeline`] over many input items in parallel,
//! inside a single process, with per-item memoization, retry, streamed
//! output, and a rolled-up cost report. It is the per-batch orchestrator for
//! workloads that fan out 10-1000x per run: bulk LLM jobs (classify, look up,
//! draft, check, refine over thousands of tickets), document/OCR pipelines,
//! data enrichment, parameter sweeps. The pipeline contract is workload
//! agnostic.
//!
//! # Execution model: one item, one run, one step
//!
//! Each input item becomes one [`taquba_workflow`] run whose single step
//! invokes [`Pipeline::run`]. The pipeline's own logical steps live inside
//! that method as [`BulkCtx::memoized`] calls. Taquba delivers at-least-once,
//! so a step may run again if its lease expires before it acks; memoization
//! makes that replay cheap, because each completed logical step returns its
//! cached result instead of repeating a paid call. A pipeline error retries
//! with backoff and then dead-letters the item (terminating it failed); the
//! rest of the batch is unaffected.
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
//! use taquba_bulk::{Bulk, BulkCtx, Pipeline, StepError};
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
//!             .memoized("classify", async {
//!                 ctx.record_cost("llm_calls", 1.0);
//!                 // one paid call, cached on retry
//!                 Ok::<_, StepError>("billing".to_string())
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
//!
//! # Failure policy
//!
//! Per-item failures are recorded, not fatal: each failed item is written to
//! the output sink with its error and its run id is collected on
//! [`BulkReport::failed_run_ids`]. Set [`BulkBuilder::fail_threshold`] to
//! turn the whole run into an [`Error::FailureThresholdExceeded`] when the
//! share of failures crosses a percentage, so a silent mass failure
//! surfaces.
//!
//! # Replay
//!
//! Because memo entries are retained, re-submitting a failed item's input
//! with the same run id resumes from its last cached step rather than
//! recomputing. [`BulkReport::failed_run_ids`] is the set to replay.
//!
//! # Input and output
//!
//! Line-delimited JSON: [`read_jsonl`] decodes inputs and
//! [`JsonlSink`] writes one result record per line. Both sides are traits
//! ([`OutputSink`]), so other formats can be added without touching the
//! runner. The default sink is [`NullSink`], for pipelines whose results are
//! side effects.

#![warn(missing_docs)]

mod bulk;
mod cost;
mod error;
mod io;
mod pipeline;
mod progress;
mod runner;

pub use bulk::{Bulk, BulkBuilder};
pub use cost::CostReport;
pub use error::{Error, Result};
pub use io::{JsonlSink, NullSink, OutputRecord, OutputSink, read_jsonl};
pub use pipeline::{BulkCtx, Pipeline};
pub use progress::{BulkReport, ProgressSnapshot};

/// Re-exported from [`taquba_workflow`]: the error type a [`Pipeline`]
/// returns, with [`StepError::transient`] / [`StepError::permanent`]
/// controlling retry versus immediate dead-letter.
pub use taquba_workflow::{StepError, StepErrorKind};

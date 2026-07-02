# taquba-bulk

[![crates.io](https://img.shields.io/crates/v/taquba-bulk.svg)](https://crates.io/crates/taquba-bulk)
[![docs.rs](https://img.shields.io/docsrs/taquba-bulk)](https://docs.rs/taquba-bulk)
[![license](https://img.shields.io/crates/l/taquba-bulk.svg)](#license)

Bulk multi-step processing on top of [taquba-workflow](../taquba-workflow) and
the [Taquba](../taquba) durable task queue.

> Part of the [Taquba ecosystem](https://github.com/micllam/taquba); see the
> workspace README for the queue core and the other crates that compose with
> this one.

`taquba-bulk` runs one `Pipeline` over many input items in parallel, inside a
single process, with per-item memoization, retry, streamed output, and a
rolled-up cost report. It is the per-batch orchestrator for workloads that fan
out 10-1000x per run: bulk LLM jobs (classify, look up, draft, check, refine
over thousands of tickets), document/OCR pipelines, data enrichment, parameter
sweeps. The pipeline contract is workload agnostic.

## Execution model: one item, one run, one step

Each input item becomes one `taquba-workflow` run whose single step invokes
`Pipeline::run`. The pipeline's own logical steps live inside that method as
`BulkCtx::memoized` or `BulkCtx::memoized_by_content` calls. Taquba delivers
at-least-once, so a step may run again if its lease expires before it acks;
memoization makes that replay cheap, because each completed logical step
returns its cached result instead of repeating a paid call. A pipeline error
retries with backoff and then dead-letters the item (terminating it failed);
the rest of the batch is unaffected.

`BulkCtx::memoized` is `taquba-workflow`'s per-step memo store applied at a
finer granularity: the item's single step holds one memo entry per logical
phase, so the phases of `Pipeline::run` resume individually even though the
workflow sees one step.

## Content-addressed memoization

Use `BulkCtx::memoized_by_content` when the natural memo key is a serialized
input value rather than a caller-supplied string:

```rust,ignore
#[derive(serde::Serialize)]
struct LookupKey<'a> {
    operation: &'static str,
    query: &'a str,
}

let key = LookupKey {
    operation: "lookup",
    query: &ctx.input.body,
};
let response = ctx
    .memoized_by_content(&key, async {
        Ok::<_, StepError>(lookup(&ctx.input.body).await?)
    })
    .await?;
```

The helper serializes the key as MessagePack, hashes it with SHA-256, and
uses the digest inside the item's existing workflow memo namespace. The entry
remains scoped to one item run; this is not a cross-item cache. Include an
operation name in the serialized key when multiple logical operations may
receive the same input shape.

## Single process, remote work per step

The orchestrator is single-process by design: SlateDB allows one writer per
store, so all producers and workers for a batch share one `Arc<Queue>`. That
is not a throughput ceiling for bulk work. Each step's expensive operation is
a call to a remote service (an LLM API, an OCR service), so the process is
I/O-bound and one host sustains hundreds of concurrent items. The remote call
runs elsewhere and its response is memoized on return.

## Install

```bash
cargo add taquba-bulk taquba
cargo add tokio --features full
```

## Quick start

```rust
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use taquba::{Queue, object_store::memory::InMemory};
use taquba_bulk::{Bulk, BulkCtx, CostReport, Pipeline, StepError};

#[derive(Serialize, Deserialize)]
struct Ticket { id: String, body: String }

#[derive(Serialize, Deserialize)]
struct Processed { id: String, classification: String }

struct TicketPipeline;

impl Pipeline for TicketPipeline {
    type Input = Ticket;
    type Output = Processed;
    type Error = StepError;

    async fn run(&self, ctx: &BulkCtx<Ticket>) -> Result<Processed, StepError> {
        let classification = ctx
            .memoized_with_cached_cost("classify", async {
                let cost = CostReport::new();
                cost.record("llm_calls", 1.0);
                Ok::<_, StepError>(("billing".to_string(), cost))
            })
            .await?;
        Ok(Processed { id: ctx.input.id.clone(), classification })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "db").await?);

    let bulk = Bulk::builder(queue, store, TicketPipeline)
        .key_fn(|t| t.id.clone())
        .max_concurrent(200)
        .build();

    let inputs = vec![
        Ticket { id: "t1".into(), body: "help".into() },
        Ticket { id: "t2".into(), body: "refund".into() },
    ];
    let report = bulk.run(inputs).await?;
    println!("{}/{} succeeded", report.succeeded, report.total);
    Ok(())
}
```

See [`examples/wordcount.rs`](examples/wordcount.rs) for a runnable,
network-free end-to-end run, and
[`examples/document_pipeline.rs`](examples/document_pipeline.rs) for a
pipeline with several stages (extract, classify, validate) demonstrating
per-stage memoization across retries, transient versus permanent failures and
cost metering that survives retries.

## Cost tracking

Pipelines report arbitrary named metrics via `BulkCtx::record_cost` (token
counts, paid-API units, compute-seconds, dollars). Per-item totals roll up
into `ProgressSnapshot::cost` and `BulkReport::cost`, so the batch cost is
visible live and in the final report.
When counters are produced inside a memoized closure, return `(value, cost)`
from `BulkCtx::memoized_with_cached_cost` or
`BulkCtx::memoized_by_content_with_cached_cost` so the same counters are
recorded on a cache hit.

## Failure policy

Per-item failures are recorded, not fatal: each failed item is written to the
output sink with its error and its run id is collected on
`BulkReport::failed_run_ids`. Set `BulkBuilder::fail_threshold` to turn the
whole run into an `Error::FailureThresholdExceeded` when the share of failures
crosses a percentage, so a silent mass failure surfaces.

## Replay

Because memo entries are retained, re-submitting a failed item's input with
the same run id resumes from its last cached step rather than recomputing.
`BulkReport::failed_run_ids` is the set to replay.

## Input and output

Line-delimited JSON: `read_jsonl` decodes inputs and `JsonlSink` writes one
result record per line. Both sides are traits (`OutputSink`), so other
formats can be added without touching the runner. The default sink is
`NullSink`, for pipelines whose results are side effects.

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or
   <http://www.apache.org/licenses/LICENSE-2.0>)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or
   <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.

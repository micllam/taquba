# Taquba

[![crates.io](https://img.shields.io/crates/v/taquba.svg)](https://crates.io/crates/taquba)
[![docs.rs](https://img.shields.io/docsrs/taquba)](https://docs.rs/taquba)
[![CI](https://github.com/micllam/taquba/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/micllam/taquba/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/taquba.svg)](#license)

Durable execution for Rust on object storage. Multi-step runs, memoized side
effects and background jobs keep their state in S3, GCS, Azure Blob or a local
directory through [SlateDB](https://github.com/slatedb/slatedb), without a
database, broker or control plane to operate.

Taquba is a library embedded in the application. One process owns each store and
runs its producers and workers, while other processes send their requests
through an inbox. Because the state is in object storage, a replacement process
resumes where the previous one stopped.

A single transaction of the queue can acknowledge a job, enqueue its follow-up
jobs and write application state. A state machine built on the queue therefore
remains consistent across crashes, without an outbox or a separate database.

The design suits slow work that waits on IO or remote APIs, such as LLM agents
and document pipelines. For a worker fleet across machines, a latency-sensitive
request path or a high volume of short jobs, a different tool is the correct
choice. See [When to use it, and when not to](#when-to-use-it-and-when-not-to)
for more details.

## Quick example

```bash
cargo add taquba taquba-workflow
cargo add tokio --features full
```

A workflow on an in-memory store. Replace `InMemory` with an S3, GCS or Azure
builder in production.

```rust
use std::sync::Arc;
use taquba::{Queue, object_store::memory::InMemory};
use taquba_workflow::{RunSpec, Step, StepError, StepOutcome, StepRunner, WorkflowRuntime};

struct Summarize;

impl StepRunner for Summarize {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        match step.step_number {
            // A retry of step 0 reads the recorded summary and does not call
            // the model again.
            0 => {
                let summary: String = step
                    .memo
                    .memoized("summary", call_model(&step.payload))
                    .await?;
                Ok(StepOutcome::continue_now(summary.into_bytes()))
            }
            // Step 1 receives the summary as its payload.
            _ => Ok(StepOutcome::Succeed { result: step.payload.clone() }),
        }
    }
}

async fn call_model(input: &[u8]) -> Result<String, StepError> {
    Ok(format!("summary of {} bytes", input.len()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "demo").await?);

    let runtime = WorkflowRuntime::builder(queue, store, Summarize).build()?;
    let worker = runtime.spawn(std::future::pending::<()>());

    let submitted = runtime.submit(RunSpec {
        input: b"a long document".to_vec(),
        ..Default::default()
    }).await?;
    let run = runtime.wait(&submitted.run_id).await?;
    println!("run {} {}", run.run_id, run.termination.status);
    worker.shutdown().await?;
    Ok(())
}
```

On any store other than `InMemory`, a restarted process resumes an interrupted
run at its last completed step. A memoized call, such as an LLM request, does
not run again on the retry, as
[`taquba-workflow/examples/crash_resume.rs`](./taquba-workflow/examples/crash_resume.rs)
shows.

## When to use it, and when not to

**Taquba is a good fit when any of the following is true:**

- **Steps are slow and IO-bound.** For a step that runs for seconds, such as an
  LLM call or a rate-limited API, the object-store write at each transition is a
  small share of its duration.
- **The workload is idle or bursty.** No server runs while the process is
  stopped, so an idle queue is billed for its stored objects alone. A large
  backlog also adds only stored objects.
- **State crosses machine or account boundaries.** A bucket is reachable from a
  laptop, a CI runner, a spot instance or another cloud, and a run resumes on
  any of them.
- **The work coordinates data already in a bucket.** Queue state, payloads and
  results share one storage system and one access policy with the data they
  concern.

**A different tool is the better choice when any of the following is true:**

- **Enqueues must commit in the transactions of another database.** A queue
  stored in the application's database commits its jobs inside the transactions
  of the application.
- **A worker fleet across machines is required.** Compute in the worker process
  is capped at one node, although steps that call remote services scale with
  async concurrency.
- **The path is latency-sensitive.** Every durable transition waits for an
  object-store write, so the application must tolerate 50 to 100 ms of write
  latency. The latency falls with a shorter flush interval and a write-ahead log
  on a faster store, and recent transitions then have the durability of that
  store.
- **Jobs are short and volume is high.** Throughput is bound by the durable
  commit rate of thousands of transitions per second, which sub-millisecond jobs
  at high volume reach quickly.

[`taquba-bencher/RESULTS.md`](./taquba-bencher/RESULTS.md) records the
measurements for these claims, with their environment.

## Crates

The crates share a `taquba` queue that the application opens.

| Crate | What it does | Best for |
|---|---|---|
| [`taquba-workflow`](./taquba-workflow) | Durable multi-step runs, typed jobs and job groups | LLM agent runs, document pipelines, payment flows, typed background tasks |
| [`taquba`](./taquba) | Durable task queue with transactional KV, leases, retries, scheduling and dead-letter | A custom execution layer, or background jobs with opaque payloads |
| [`taquba-cron`](./taquba-cron) | POSIX cron scheduling onto a Taquba queue | Periodic enqueues (reports, sweeps, reminders) |
| [`taquba-webhooks`](./taquba-webhooks) | HTTP webhook delivery with retries and dead-letter | Outbound webhook fan-out with durable retries |

## Stability

The crates are pre-1.0. A minor release can break the Rust API and the on-disk
format, whereas a patch release preserves both. An upgrade to a new minor
version requires a drained queue or an empty store.

<!-- vale off -->

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

<!-- vale on -->

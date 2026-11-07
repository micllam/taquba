# taquba-workflow

[![crates.io](https://img.shields.io/crates/v/taquba-workflow.svg)](https://crates.io/crates/taquba-workflow)
[![docs.rs](https://img.shields.io/docsrs/taquba-workflow)](https://docs.rs/taquba-workflow)
[![license](https://img.shields.io/crates/l/taquba-workflow.svg)](#license)

Durable execution for Rust on object storage. A run is a sequence of steps, and
the runtime stores the output of each step, the recorded results of its side
effects and its application state in S3, GCS, Azure Blob or a local directory. A
process restarted mid-run resumes at the last completed step.

The runtime is an embedded library on the
[`taquba`](https://github.com/micllam/taquba/tree/master/taquba) queue. One
process owns a store, and its producers and workers share one `Arc<Queue>`. The
runtime suits slow, IO- or API-bound work: LLM agent runs, document pipelines,
payment flows and command-line tools that resume after an interruption.

> Part of the [Taquba ecosystem](https://github.com/micllam/taquba). The
> workspace README states when object storage is the correct choice and how the
> crates compose.

## Quick start

```bash
cargo add taquba-workflow taquba
cargo add tokio --features full
```

```rust
use std::sync::Arc;
use taquba::{Queue, object_store::memory::InMemory};
use taquba_workflow::{RunSpec, Step, StepError, StepOutcome, StepRunner, WorkflowRuntime};

struct EchoRunner;

impl StepRunner for EchoRunner {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        Ok(StepOutcome::Succeed { result: step.payload.clone() })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "demo").await?);

    let runtime = WorkflowRuntime::builder(queue, store, EchoRunner).build()?;
    let worker = runtime.spawn(std::future::pending::<()>());

    let submitted = runtime.submit(RunSpec {
        input: b"hello".to_vec(),
        ..Default::default()
    }).await?;
    let run = runtime.wait(&submitted.run_id).await?;
    println!("run {} {}", run.run_id, run.termination.status);
    worker.shutdown().await?;
    Ok(())
}
```

The [examples](https://github.com/micllam/taquba/tree/master/taquba-workflow/examples)
include a run that resumes after a crash, an approval that waits for a signal,
typed jobs, a document pipeline over a job group and an LLM agent on
[Rig](https://github.com/0xPlaygrounds/rig).

## Run lifecycle

```text
               Continue: now, after a delay,
               on a signal or on a group
               ┌─────────┐
               ▼         │
submit ──────▶ step ─────┘
               │
               ├── Succeed ───────────────────────▶ succeeded
               ├── Fail, or a dead-lettered step ─▶ failed
               └── Cancel, or a cancellation ─────▶ cancelled
```

A `StepRunner` receives the payload of a step and returns a `StepOutcome`. A
`Continue` outcome enqueues the next step with its payload, which is the durable
channel between steps. A step error retries the step after a backoff, and a
permanent error or the attempt limit dead-letters it.

The runner decides each next step. The runtime does not execute a declared
graph, and it does not replay an event history: it records a side effect only
where the runner memoizes it.

Delivery is at least once, so a step whose lease expires before its settlement
runs again. A runner must therefore be idempotent for a run id and a step
number. A cancellation from outside the runner reaches a running step through
its cancellation token.

## Recorded side effects

A retry runs the code of a step again. A slow or paid call, such as an LLM
request, records its result in the memo of the step, and a retry reads the
recorded result without a second call:

```rust,ignore
let draft: Draft = step
    .memo
    .memoized("draft", async { expensive_call(&step.payload).await })
    .await?;
```

The memo of the run shares values between its steps. A retention window removes
the memo and the records of a terminated run.

## Transactional effects

A step stages writes to the queue's key-value namespace and enqueues to other
queues, and they commit in the settlement of the step. A terminal hook stages
effects into the settlement that terminates a run. With the `webhooks` feature,
`WebhookTerminalHook` delivers an HTTP callback at the termination.

## Waiting without a worker

A step can defer the next step for a delay, until a signal to its id or until
every member of a group terminates. A signal contains an approval, a webhook
callback or a remote reply. The waiting step is a scheduled job in the store, so
the wait persists across a restart and does not occupy a worker.

## Typed jobs and groups

The `jobs` module runs a typed async function as a single-step run and returns
its result to the awaiting caller. A typed job suits a caller that awaits a
value, and a step runner suits a caller that observes the run through
cancellation, headers and a terminal hook.

```rust,ignore
use taquba_workflow::jobs::{Job, JobContext, JobRunner};

#[derive(serde::Serialize, serde::Deserialize)]
struct SendEmail { to: String }

impl Job for SendEmail {
    const NAME: &'static str = "email.send";
    type Output = String;
    type Error = EmailError;

    async fn run(&self, _ctx: JobContext<'_>) -> Result<String, EmailError> {
        Ok(format!("msg-for-{}", self.to))
    }
}

let runner = JobRunner::builder(queue, store)
    .register::<SendEmail>()
    .build();
let worker = runner.spawn(std::future::pending::<()>());
let message_id = runner.submit(SendEmail { to: "user@example.com".into() }).await?.await?;
```

A group submits many runs or jobs as one durable set. Its results stream back as
they complete, and a second submission runs again only the members that did not
succeed. A step fans out by submitting a group and waiting for it.

## Stability

taquba-workflow is pre-1.0. A minor release can break the Rust API and the
on-disk format, and a patch release preserves both. An upgrade to a new minor
version requires a store without active runs.

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

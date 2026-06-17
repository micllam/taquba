# taquba-jobs

Durable, typed async function execution on top of the [Taquba](https://docs.rs/taquba) durable task queue.

> Part of the [Taquba ecosystem](https://github.com/micllam/taquba); see the
> workspace README for the queue core and the other crates that compose with
> this one.

`taquba-jobs` is a primitive for running functions reliably in the background:
define a typed `Job`, submit instances of it, get the typed result back.
Durability, retries, idempotency and result persistence are handled for you;
the worker process stays stateless and replaceable because all state lives in
object storage via Taquba.

It sits one level above [`taquba`](https://docs.rs/taquba): Taquba is the raw
durable queue (opaque byte payloads, lease-based claims, dead-letter queue)
and `taquba-jobs` adds the function-shaped abstraction (typed inputs, typed
outputs, a type registry, and durable result delivery).

Within the ecosystem, [`taquba-workflow`](https://docs.rs/taquba-workflow)
is the sibling crate for multi-step processes: use `taquba-jobs` when the
caller awaits a typed return value, and `taquba-workflow` when one entity
moves through durable steps with cancellation and a terminal hook. Chaining
jobs through `JobContext::submit` to model a multi-step process is a sign
the work belongs in a workflow. To run one pipeline over many inputs with
batch progress and cost rollup, use
[`taquba-bulk`](https://docs.rs/taquba-bulk).

## Architecture

Like all of the Taquba ecosystem, `taquba-jobs` is **single-process**: one
`JobRunner` per process, owning one `taquba::Queue`. The runner spawns a
concurrent worker that claims jobs, routes each to its registered handler by a
type tag, runs it, and persists the outcome.

Job *results* are durable: every terminal outcome is written as a blob to an
object store you provide (typically the same store the queue lives on, under a
sibling prefix of the SlateDB path). Awaiting a `JobHandle` is in-process (it
uses Taquba's in-process completion notification), but the result itself can be
read back with `JobHandle::fetch_result` even after a process restart.

Delivery is at-least-once, inherited from Taquba: **job handlers must be
idempotent.** A retried attempt that re-runs after a prior attempt already
wrote a result blob will overwrite that blob with the new attempt's outcome,
so a non-idempotent handler can have its earlier "successful" result replaced.

Result blobs accumulate indefinitely by default; enable
`JobRunnerBuilder::result_retention(...)` (see [Result retention](#result-retention))
to clear them on a schedule, or plan a lifecycle policy on the
object-store prefix (S3 lifecycle rules, GCS object-lifecycle management,
etc.) if you prefer to manage retention out-of-band.

## Idempotent submissions

`Job::idempotency_key` collapses duplicate submissions to a single
job. Two phases:

- **Before the original completes** (pending, scheduled, or in
  flight): a second submission with the same key returns a
  `JobHandle` pointing at the in-flight job, with
  `newly_submitted() == false`. If the payload differs from the
  original, the submission fails with `Error::InputMismatch` instead
  of silently dedup-hitting a job whose input was something else.
  The payload check survives process restarts: the SHA-256 of the
  serialized payload is persisted in Taquba's user KV namespace
  atomically with the enqueue.
- **After the original completes**: the same dedup record carries
  the original job's id, so a re-submission with a matching payload
  returns a handle pointing at the cached result blob. Awaiting it
  (or calling `JobHandle::fetch_result`) yields the cached outcome
  (success or terminal failure) without re-running the work.

If `result_retention` is configured and the cached blob has been
swept, the dedup record still points to a missing blob; the
re-submission then falls through to the normal enqueue path and
re-runs the job. This is deliberate: rather than guard each blob
deletion against a delayed reader, the submit path re-verifies that
the blob is present before trusting the cached result and re-runs when
it is gone. Size the retention window so it covers the longest gap
callers need between the original submission and an idempotent
re-submit.

For jobs where "same input means same key" is the right semantics,
the `payload_idempotency_key` helper hashes the serialized payload
directly. Custom keys are appropriate when the dedup identity is
narrower than the full payload (e.g. `"email:{recipient}:{date}"`).

## Result retention

`JobRunnerBuilder::result_retention(Duration)` enables an in-process
sweeper that deletes a job's persisted outcome blob `Duration` after the
job reaches a terminal state. When the option is unset (default), blobs
are retained indefinitely.

```rust
use std::sync::Arc;
use std::time::Duration;
use taquba::{Queue, object_store::memory::InMemory};
use taquba_jobs::JobRunner;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "background-jobs").await?);

    let runner = JobRunner::builder()
        .queue(queue)
        .object_store(store)
        .result_retention(Duration::from_secs(24 * 60 * 60))
        .build()?;

    // ... register and spawn as usual ...
    drop(runner);
    Ok(())
}
```

The runner writes a small terminal marker every time a job reaches a
terminal state (success or terminal failure); a background sweeper
spawned alongside the dispatch worker periodically lists markers,
deletes the result blob and marker for each marker older than the
retention window, and exits cleanly when the runner shuts down.

Once a blob is swept, `JobHandle::fetch_result` for that job returns
`Ok(None)` and an idempotent re-submission of the same payload falls
through to re-running the job instead of short-circuiting to a cached
result (see [Idempotent submissions](#idempotent-submissions)). Size
the window so it covers the longest gap callers need between the
original submission and an idempotent re-submit.

## Time injection

The runner inherits its clock from the queue (`Queue::clock`), so a
`MockClock` passed to `Queue::open_with_options` virtualises time for
the runner's terminal-marker timestamps and retention-sweep cutoff as
well. `JobRunnerBuilder::clock` overrides it for the rarer case where
the runner needs a different clock than the queue.

## Configuring the queue

Per-queue retention (`QueueConfig::keep_done_jobs` and
`QueueConfig::dead_retention`) is set on the `taquba::Queue` before it's
handed to the runner. Pick an explicit name via
`JobRunnerBuilder::queue_name` and key `OpenOptions::queue_configs` on the
same string.

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use taquba::{OpenOptions, Queue, QueueConfig, object_store::memory::InMemory};
use taquba_jobs::JobRunner;

let store = Arc::new(InMemory::new());
let opts = OpenOptions {
    queue_configs: HashMap::from([(
        "background-jobs".to_string(),
        QueueConfig {
            keep_done_jobs: Some(Duration::from_secs(60 * 60)),
            ..QueueConfig::default()
        },
    )]),
    ..OpenOptions::default()
};
let queue = Arc::new(Queue::open_with_options(store.clone(), "db", opts).await?);
let runner = JobRunner::builder()
    .queue(queue)
    .object_store(store)
    .queue_name("background-jobs") // same string as in queue_configs
    .build()?;
```

## Fan-out from handlers

`JobContext::submit` lets a running handler enqueue follow-up jobs against the
same runner. Use it for chaining (job A submits job B) or for fan-out (a
coordinator job submits N independent children). Child submissions are
independent: they are not awaited as part of the parent and survive the
parent's completion.

## Quick start

```rust
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use taquba::{Queue, object_store::memory::InMemory};
use taquba_jobs::{Job, JobContext, JobRunner};

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

#[derive(Debug, thiserror::Error)]
#[error("email error: {0}")]
struct EmailError(String);

impl Job for SendEmail {
    const NAME: &'static str = "email.send";
    type Output = String; // message id
    type Error = EmailError;

    async fn run(&self, _ctx: JobContext<'_>) -> Result<String, EmailError> {
        // ... call your email provider ...
        Ok(format!("msg-for-{}", self.to))
    }

    fn idempotency_key(&self) -> Option<String> {
        Some(format!("email:{}:{}", self.to, self.subject))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "background-jobs").await?);

    let mut runner = JobRunner::builder()
        .queue(queue)
        .object_store(store)
        .max_concurrent_jobs(50)
        .build()?;

    runner.register::<SendEmail>();
    let handle = runner.spawn(std::future::pending::<()>());

    let job = runner
        .submit(SendEmail { to: "user@example.com".into(), subject: "Welcome".into() })
        .await?;
    let message_id = job.await?;
    println!("sent: {message_id}");

    handle.shutdown().await?;
    Ok(())
}
```

## Core types

- **`Job`**: the trait defining a typed job (input fields, `Job::Output`,
  `Job::Error`, and the `Job::run` body, plus hooks for idempotency, attempt
  limits, and error classification).
- **`JobRunner`**: registers job types, submits jobs, spawns the worker.
- **`JobContext`**: the per-call context handed to `Job::run`: application
  state, the queue, the job's identity, a cancellation token.
- **`JobHandle`**: returned from `JobRunner::submit`; await it for the typed
  result, or poll its `status` / `fetch_result`.

## Retries and failure

A job that returns `Err` is classified by `Job::classify` as `Transient`
(retried with backoff up to the queue's attempt limit, then dead-lettered) or
`Permanent` (dead-lettered immediately). Per-job-type backoff curves are not
configurable in this version: backoff is a queue-level Taquba setting;
`Job::max_attempts` and per-submission `SubmitOptions` cover the per-job
settings that exist today.

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

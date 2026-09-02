# Taquba

[![crates.io](https://img.shields.io/crates/v/taquba.svg)](https://crates.io/crates/taquba)
[![docs.rs](https://img.shields.io/docsrs/taquba)](https://docs.rs/taquba)
[![CI](https://github.com/micllam/taquba/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/micllam/taquba/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/taquba.svg)](#license)

Durable execution for Rust on object storage. Multi-step runs, memoized
side effects and background jobs keep their state in a bucket or a local
directory, with transactional coordination built in and no database,
broker or control plane to operate.

Taquba is a workspace of Rust crates. `taquba-workflow` runs durable
multi-step processes; `taquba` is the durable queue underneath it and the
substrate of every other crate. Job records, run records and memos are
stored in object storage (S3, GCS, Azure Blob or local disk) through
[SlateDB](https://github.com/slatedb/slatedb). Workers hold no state, so
a replacement process resumes where the previous one stopped.

Taquba is embedded and single-process: one process owns each store, and
producers and workers share it. It is built for long-running, expensive,
IO- or API-bound work in one Rust process: LLM agents, batch document
pipelines and command-line tools whose runs survive an interruption. A
worker fleet across machines, a latency-sensitive request path or
high-volume cheap jobs are better served by a different tool; see
[When to use it, and when not to](#when-to-use-it-and-when-not-to).

## Why this is different

Durable execution is usually provided as a workflow engine you operate (a
server cluster with its own database), as a hosted service (run state in
the vendor's control plane) or as a job queue embedded in your binary with
its state in a database or Redis server deployed beside it. Taquba is an
embedded library whose state is in object storage. Its distinguishing
properties:

- **Transactional coordination without a database.** One transaction can
  acknowledge a job, enqueue its follow-up jobs and update caller-owned
  durable KV state (`ack_with`, `enqueue_with_kv`), so state machines
  built on the queue remain consistent across crashes without an outbox or
  a second datastore.
- **Data residency by construction.** Records are written only to the
  bucket you configure.

For LLM agent stacks, composition libraries such as
[Rig](https://github.com/0xPlaygrounds/rig) cover providers, tools and
prompts in process, and Taquba provides the durable execution layer
underneath. The reference agent
[`taquba-research`](https://github.com/micllam/taquba-research) is built
this way.

## Quick example

```bash
cargo add taquba taquba-workflow
cargo add tokio --features full
```

A workflow on an in-memory store. Replace `InMemory` with an S3, GCS or
Azure builder in production.

```rust
use std::sync::Arc;
use taquba::{Queue, object_store::memory::InMemory};
use taquba_workflow::{
    NoopTerminalHook, RunSpec, Step, StepError, StepOutcome, StepRunner, WorkflowRuntime,
};

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

    let runtime = WorkflowRuntime::builder(queue, store, EchoRunner, NoopTerminalHook).build();
    let worker = runtime.clone();
    tokio::spawn(async move { worker.run(std::future::pending::<()>()).await });

    let outcome = runtime.submit(RunSpec {
        input: b"hello".to_vec(),
        ..Default::default()
    }).await?;
    println!("submitted run {}", outcome.run_id);
    Ok(())
}
```

With a persistent backend, a run interrupted mid-step resumes at its last
completed step when the process restarts: each step's output is persisted
before the next step starts, and inside a step, results recorded through
the memo store are returned to the retried step without re-executing the
calls that produced them (LLM requests, paid APIs). Both are demonstrated
by interrupting and restarting
[`taquba-workflow/examples/crash_resume.rs`](./taquba-workflow/examples/crash_resume.rs).

## When to use it, and when not to

**Taquba is a good fit when any of the following holds:**

- **Payloads are large.** Payloads above a threshold are offloaded to
  their own objects with the record's lifecycle: written before the
  record, deleted after its removal. Databases commonly move large values
  to external blob storage with a pointer, splitting the queue and its
  payloads into two lifecycles.
- **Backlogs are large or bursty.** A million-job backlog costs only its
  storage, and compaction of enqueue and settle churn runs inside the
  process with no operator maintenance.
- **The workload is mostly idle.** A queue whose process is not running
  incurs only storage cost; there is no server with a baseline cost.
  Per-tenant isolation is a key prefix, so an idle tenant incurs no cost
  beyond its storage.
- **State crosses machine or account boundaries.** A bucket is reachable
  from a laptop, a CI runner, a spot instance or another cloud, so a run
  can be resumed from any of them.
- **The work coordinates data already in the bucket.** Queue state,
  payloads, results and history share one storage system and one access
  policy with the data they concern.
- **The execution trail must be auditable.** Attempt history and
  terminal markers commit in the same transactions as the work, and
  bucket versioning and replication apply to them as to any other object.
- **Steps are expensive and IO-bound.** Fit rises with per-step cost. For
  a step that takes much longer than an object-store write, the
  per-transition durability cost is negligible, a retried run does not
  re-pay memoized steps and few transitions per second occur, far below
  the commit-rate ceiling. LLM calls, remote renders and rate-limited
  third-party APIs are the strongest fit.

**A different tool is the better choice when any of the following holds:**

- **The queue must share the application's database transactions.** A
  database-backed queue enqueues jobs and commits their effects inside the
  application's own transactions.
- **A worker fleet across machines is required.** Compute that runs in
  the worker process (GPU or CPU hours) is capped at one node; steps that
  call remote services scale with async concurrency inside the process.
  Development stays within the single-process model.
- **The path is latency-sensitive.** Every durable transition waits for
  an object-store write, so end-to-end latency has a lower bound of one
  PUT round trip: tens to hundreds of milliseconds on S3. A user-facing
  request path should not block on such a write.
- **Jobs are cheap and volume is high.** Throughput is bound by the
  durable commit rate: thousands of transitions per second against S3,
  scaling with concurrency. Sub-millisecond jobs incur the durability cost
  on every transition and reach that ceiling quickly.

The measured numbers behind these claims, with the environment and commit
that produced them, are in
[`taquba-bencher/RESULTS.md`](./taquba-bencher/RESULTS.md); the
benchmarks are in the internal [`taquba-bencher`](./taquba-bencher) crate.

## Crates

The crates form three tiers. The execution tier is the entry point, the
substrate is what it runs on and the components are for programs that
already use the stack.

| Tier | Crate | What it does | Best for |
|---|---|---|---|
| Execution | [`taquba-workflow`](./taquba-workflow) | Runs one durable multi-step process: a sequence of steps with per-step memoization, retries, durable signals and a terminal hook. Its `jobs` module runs one typed async function as a single-step run and returns the result to an awaiting caller; its `bulk` module applies one pipeline to many inputs in parallel with batch progress, cost rollup and streamed output | LLM agent runs, payment flows, document pipelines, typed background tasks, bulk LLM and document workloads |
| Substrate | [`taquba`](./taquba) | Durable task queue with transactional KV, leases, retries, scheduling and dead-letter | Building your own execution layer, or background jobs with opaque payloads |
| Component | [`taquba-cron`](./taquba-cron) | POSIX cron scheduling onto a Taquba queue | Periodic enqueues (reports, sweeps, reminders) |
| Component | [`taquba-webhooks`](./taquba-webhooks) | HTTP webhook delivery with retries and dead-letter | Outbound webhook fan-out with durable retries |

Every crate above the substrate consumes one `Arc<Queue>`.

### Choosing between them

- **A single-step workflow or a job.** Use a typed job (the `jobs`
  module) when the caller awaits a typed return value in process, and a
  step runner when the caller observes the run through cancellation,
  headers and a terminal hook.
- **Chained jobs or a workflow.** A job can submit further jobs, so a
  pipeline can be approximated by chaining. Chained jobs share no run
  identity, no end-to-end terminal status and no resume point; a process
  modelled by chaining belongs in a step runner.
- **Job fan-out or bulk.** Submitting N typed jobs and awaiting their
  handles yields N independent typed results. Use the `bulk` module when
  each item is itself a multi-phase pipeline whose completed phases must
  survive a retry, and the batch needs progress, cost rollup and a
  failure threshold.
- **Fan-out inside one run.** Compose steps with typed jobs: a workflow
  step submits N typed jobs, joins their results and memoizes the
  aggregate so a step retry does not re-submit. The
  reference agent uses this for its parallel page-fetch phase and cancels
  in-flight jobs when the surrounding run is cancelled. It is a manual
  pattern, demonstrated in
  [`taquba-workflow/examples/fanout_jobs.rs`](./taquba-workflow/examples/fanout_jobs.rs).

## Stability

Pre-1.0. Minor version bumps may break source compatibility and the
on-disk layout. Drain in-flight runs before upgrading across minors. Patch
bumps preserve both.

## Links

- Per-crate docs: links in the crates table, or browse on
  [docs.rs](https://docs.rs/taquba).
- Issues and discussion: [GitHub](https://github.com/micllam/taquba).

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

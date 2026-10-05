# Taquba

[![crates.io](https://img.shields.io/crates/v/taquba.svg)](https://crates.io/crates/taquba)
[![docs.rs](https://img.shields.io/docsrs/taquba)](https://docs.rs/taquba)
[![CI](https://github.com/micllam/taquba/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/micllam/taquba/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/taquba.svg)](#license)

Durable execution for Rust on object storage. Multi-step runs, memoized side
effects and background jobs keep their state in a bucket or a local directory,
with transactional coordination built in and no database, broker or control
plane to operate.

Taquba is a workspace of Rust crates. `taquba-workflow` runs durable multi-step
processes, and `taquba` is the durable queue underneath it and the substrate of
every other crate. Job records, run records and memos are stored in object
storage (S3, GCS, Azure Blob or local disk) through
[SlateDB](https://github.com/slatedb/slatedb). Workers do not keep state, so a
replacement process resumes where the previous one stopped.

Taquba is embedded and single-process: one process owns each store, and
producers and workers share it. It is built for long-running, slow, IO- or
API-bound work in one Rust process: LLM agents, batch document pipelines and
command-line tools whose runs resume after an interruption. A different tool is
the better choice for a worker fleet across machines, a latency-sensitive
request path or a high volume of short jobs. The section
[When to use it, and when not to](#when-to-use-it-and-when-not-to) states the
criteria.

## Why this is different

Durable execution is usually provided as a self-operated workflow engine (a
server cluster with its own database), as a hosted service (run state in the
vendor's control plane) or as a job queue embedded in the application binary,
with its state in a separate database or Redis server. Taquba is an embedded
library whose state is in object storage. Its distinguishing properties:

- **Transactional coordination without a database.** One transaction can
  acknowledge a job, enqueue its follow-up jobs and update caller-owned durable
  KV state (`ack_with`, `enqueue_with_effects`), so state machines built on the
  queue remain consistent across crashes without an outbox or a second
  datastore.
- **Data residency by construction.** Records are written only to the configured
  bucket.

For LLM agent stacks, composition libraries such as
[Rig](https://github.com/0xPlaygrounds/rig) cover providers, tools and prompts
in process. Taquba provides the durable execution layer underneath them. The
reference agent [`taquba-research`](https://github.com/micllam/taquba-research)
is built this way.

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

    let runtime = WorkflowRuntime::builder(queue, store, EchoRunner, NoopTerminalHook).build()?;
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
completed step when the process restarts: each step's output is persisted before
the next step starts, and inside a step, results recorded through the memo store
are returned to the retried step without re-executing the calls that produced
them (LLM requests, paid APIs). Both are demonstrated by interrupting and
restarting
[`taquba-workflow/examples/crash_resume.rs`](./taquba-workflow/examples/crash_resume.rs).

## When to use it, and when not to

**Taquba is a good fit when any of the following is true:**

- **Payloads are large.** Payloads above a threshold are offloaded to their own
  objects with the record's lifecycle: written before the record, deleted after
  its removal. Databases commonly move large values to external blob storage
  with a pointer, splitting the queue and its payloads into two lifecycles.
- **Backlogs are large or bursty.** A million-job backlog costs only its
  storage, and compaction of enqueue and settle churn runs inside the process
  without operator maintenance.
- **The workload is mostly idle.** A queue whose process is not running incurs
  only storage cost, and there is no server with a baseline cost. Per-tenant
  isolation is a key prefix, so an idle tenant incurs only its storage cost.
- **State crosses machine or account boundaries.** A bucket is reachable from a
  laptop, a CI runner, a spot instance or another cloud, and a run resumes on
  any of them.
- **The work coordinates data already in the bucket.** Queue state, payloads,
  results and history share one storage system and one access policy with the
  data they concern.
- **The execution trail must be auditable.** Attempt history and terminal
  markers commit in the same transactions as the work, and bucket versioning and
  replication apply to them as to any other object.
- **Steps are slow and IO-bound.** Fit rises with the duration of a step. For a
  step that takes much longer than an object-store write, the per-transition
  durability cost is negligible, a retried run does not re-pay memoized steps
  and few transitions per second occur, far below the commit-rate ceiling. LLM
  calls, remote renders and rate-limited third-party APIs are the strongest fit.

**A different tool is the better choice when any of the following is true:**

- **The queue must share the application's database transactions.** A
  database-backed queue enqueues jobs and commits their effects inside the
  application's own transactions.
- **A worker fleet across machines is required.** Compute that runs in the
  worker process (GPU or CPU hours) is capped at one node. Steps that call
  remote services scale with async concurrency inside the process. Development
  stays within the single-process model.
- **The path is latency-sensitive.** Every durable transition waits for an
  object-store write, so end-to-end latency has a lower bound of one PUT round
  trip: tens to hundreds of milliseconds on S3. A user-facing request path that
  waits for such a write inherits that lower bound.
- **Jobs are short and volume is high.** Throughput is bound by the durable
  commit rate: thousands of transitions per second against S3, scaling with
  concurrency. Sub-millisecond jobs incur the durability cost on every
  transition and reach that ceiling quickly.

The measured numbers for these claims, with the environment and commit that
produced them, are in
[`taquba-bencher/RESULTS.md`](./taquba-bencher/RESULTS.md). The benchmarks are
in the internal [`taquba-bencher`](./taquba-bencher) crate.

## Crates

The crates form three tiers. The execution tier is the entry point and runs on
the substrate. The components are for programs that already use the stack.

| Tier | Crate | What it does | Best for |
|---|---|---|---|
| Execution | [`taquba-workflow`](./taquba-workflow) | Runs one durable multi-step process: a sequence of steps with per-step memoization, retries, durable signals and a terminal hook. Its `jobs` module runs one typed async function as a single-step run and returns the result to an awaiting caller, and submits many such jobs as one durable group whose results stream back as they complete | LLM agent runs, payment flows, document pipelines, typed background tasks, batch LLM and document workloads |
| Substrate | [`taquba`](./taquba) | Durable task queue with transactional KV, leases, retries, scheduling and dead-letter | A custom execution layer, or background jobs with opaque payloads |
| Component | [`taquba-cron`](./taquba-cron) | POSIX cron scheduling onto a Taquba queue | Periodic enqueues (reports, sweeps, reminders) |
| Component | [`taquba-webhooks`](./taquba-webhooks) | HTTP webhook delivery with retries and dead-letter | Outbound webhook fan-out with durable retries |

Every crate above the substrate consumes one `Arc<Queue>`.

### Choosing between them

- **A single-step workflow or a job.** Use a typed job (the `jobs` module) when
  the caller awaits a typed return value in process, and a step runner when the
  caller observes the run through cancellation, headers and a terminal hook.
- **Chained jobs or a workflow.** A job can submit further jobs, and a chain of
  jobs approximates a pipeline. Chained jobs do not share a run identity, an
  end-to-end terminal status or a resume point. A process modelled by chaining
  belongs in a step runner.
- **Independent jobs or a job group.** Submitting N typed jobs and awaiting
  their handles yields N independent results. A job group submits the N as one
  durable set: a second submission runs again only the members that did not
  succeed, the results stream back as they complete, and each member's completed
  phases persist across a retry through its memo.
- **Fan-out inside one run.** A workflow step opens a job group named after the
  step, submits one job per item and joins the results. A retry of the step
  re-submits the group and joins the recorded results of the members that
  completed. Demonstrated in
  [`taquba-workflow/examples/fanout_jobs.rs`](./taquba-workflow/examples/fanout_jobs.rs).

## Stability

Pre-1.0. A minor version bump can break source compatibility and the on-disk
layout. Drain in-flight runs before upgrading across minors. Patch bumps
preserve both.

## Links

- Per-crate docs: links in the crates table, or browse on
  [docs.rs](https://docs.rs/taquba).
- Issues and discussion: [GitHub](https://github.com/micllam/taquba).

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

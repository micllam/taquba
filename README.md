# Taquba

[![crates.io](https://img.shields.io/crates/v/taquba.svg)](https://crates.io/crates/taquba)
[![docs.rs](https://img.shields.io/docsrs/taquba)](https://docs.rs/taquba)
[![license](https://img.shields.io/crates/l/taquba.svg)](#license)

A durable task queue and durable-execution workflow runtime for Rust with
**built-in transactional coordination on object storage**, and no stateful
service to operate. Workflow state lives directly in your object storage;
every compute node is replaceable. Database-backed queues excel at jobs
attached to an application database; Taquba is well-suited to work
attached to data.

Taquba is a workspace of Rust crates that compose into a durable execution
stack. There is no Postgres, Redis, or broker daemon to run alongside your
workers. Queue records, workflow memos, lease bookkeeping, and retention all
live in customer-owned object storage (S3, GCS, Azure Blob, or local disk) via
[SlateDB](https://github.com/slatedb/slatedb). Workers are stateless and
interchangeable, making spot / preemptible compute the default deployment
model.

## Why this is different

- **Transactional coordination.** A single atomic
  transaction can acknowledge a job, enqueue its follow-up jobs, and update
  caller-owned durable KV state (`ack_with`, `enqueue_with_kv`). State
  machines built on the queue stay consistent across crashes without an
  outbox pattern or a second datastore.
- **No stateful service.** Most single-process durable queue libraries
  require a database (typically Postgres) to hold their state. Taquba uses
  the object storage you already have.
- **State sovereignty by construction.** Workflow records never leave your
  account because there is nowhere else for them to go.
- **A library, not infrastructure.** Embedded in your binary as a Rust
  crate. No control plane to deploy, scale, or upgrade.
- **Spot-native by design.** Stateless compute plus durable state make
  preemption a recoverable event.

## Quick example

```bash
cargo add taquba taquba-workflow
cargo add tokio --features full
```

A workflow on an in-memory store. Replace `InMemory` with an S3 / GCS /
Azure builder in production; nothing else changes.

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

The only stateful component is `store`. No broker daemon, no database, no
control plane.

### Crash recovery

The example above uses the in-memory store. Open the queue on a persistent
backend and the durability becomes observable: terminate the process
mid-run, start it again and the run resumes at its last completed step
instead of starting over.

```rust
use taquba::object_store::local::LocalFileSystem;

let store = Arc::new(LocalFileSystem::new_with_prefix("/tmp/taquba-demo")?);
```

Multi-step runs persist each step's output before the next step starts, and
steps that record results through the memo store extend the same protection
inside a step: a retried step returns cached results for completed calls
(LLM requests, paid APIs) instead of re-executing them.

## How it compares

Durable execution today usually comes in one of three forms:

- **A workflow engine you operate.** A server cluster with its own database
  that your workers connect to. Powerful, but you run a distributed system
  to get durability.
- **A hosted durable-execution service.** Durability as a managed service;
  your run state lives in the vendor's control plane.
- **A job queue over Redis or Postgres.** Embedded in your binary like
  Taquba, but the durable state lives in a database or Redis server you
  deploy alongside it.

The most common alternative in practice is none of these: an in-house
combination of tokio tasks, Redis, and custom retry loops. Taquba provides
what that stack eventually rebuilds (leases, retries, exponential backoff,
dead-letter retention, scheduling, fan-out) as one library, with the state on
object storage instead of on a server you operate.

Taquba is deliberately **embedded, not operated**. It advances within the
single-process, single-writer model (transactional settlement, per-step
memoization, correctness guarantees that are auditable end to end) rather
than growing into a brokered multi-tenant service. If you need a worker
fleet across machines or routing between services, an operated queue is the
appropriate tool, not this workspace.

For LLM agent stacks: composition libraries such as
[Rig](https://github.com/0xPlaygrounds/rig) cover providers, tools, and
prompts in process, and Taquba supplies the durable execution layer
underneath. The reference agent
[`taquba-research`](https://github.com/micllam/taquba-research) is built
this way.

## When to use it, and when not to

Taquba's strongest fit is a single-process service performing expensive,
IO- or API-bound, retry-worthy work over data that lives in (or belongs in)
object storage.

**Taquba is a good fit when:**

- **Payloads are big.** Object storage is built for large values. Payloads
  above a threshold are offloaded to their own objects with a transactional
  lifecycle: written once and deleted when the job leaves the queue,
  leaving neither orphaned objects nor dangling references. Large values in
  a database inflate its WAL, replication and backups, and beyond a
  practical size standard practice moves them to external blob storage with
  a pointer, splitting the queue and its payloads into two lifecycles.
- **Backlogs are big or bursty.** A million-job backlog costs only its
  storage, and heavy enqueue and settle churn requires no maintenance.
- **The workload is mostly idle.** An idle queue incurs only storage costs;
  there is no server with a baseline cost that accrues while nothing runs.
  Per-tenant isolation is a key prefix, so thousands of hibernating tenants
  cost only the bytes they store.
- **State must cross machine or account boundaries.** A bucket is reachable
  from a laptop, a CI runner, a spot instance or another cloud with
  credentials alone. Replacement compute needs only a role, which is why
  spot and preemptible workers are the default deployment model.
- **The work coordinates data already in the bucket.** Queue state,
  payloads, results and history share one storage system: one lifecycle,
  one restore domain, one access policy. Document and LLM pipelines whose
  inputs and outputs live in the bucket by necessity inherit this coherence
  without additional machinery.
- **The execution trail must be tamper-proof.** Attempt history, results
  and terminal markers commit in the same transactions as the work, and
  object lock, versioning and replication are bucket configuration, so
  the execution trail is immutable by storage policy.
- **Steps are expensive and IO-bound.** Fit rises with per-step cost: for
  steps that take much longer than an object-store write, the
  per-transition durability cost is negligible; a retried run does not
  re-pay memoized steps, so the value of that protection scales with the
  cost of the step; and expensive steps mean few transitions per second,
  far from the commit-rate ceiling. LLM calls, remote renders and
  rate-limited third-party APIs are the strongest fit.

**A different tool is the better choice when:**

- **You need the queue to share the application's database transactions.**
  A database-backed queue enqueues jobs and commits their effects inside
  the application's own transactions.
- **You need a worker fleet across machines.** Taquba is single-writer:
  one process owns each store, and producers and workers share an
  `Arc<Queue>` in that binary. This caps compute that runs in the worker
  process itself (GPU or CPU hours) at one node; steps that call remote
  services scale with async concurrency inside the single process.
- **The path is latency-sensitive.** Every durable transition is an
  object-store write, so end-to-end latency is floored by a PUT round
  trip: tens to hundreds of milliseconds on S3. A user-facing request path
  should not block on such a write.
- **Jobs are cheap and volume is high.** Throughput is bound by the
  durable commit rate: thousands of transitions per second against S3,
  scaling with concurrency, but sub-millisecond jobs pay the durability
  cost on every transition and reach that ceiling quickly.
The measured numbers behind these claims, stamped with the environment and
commit that produced them, are in
[`taquba-bencher/RESULTS.md`](./taquba-bencher/RESULTS.md).

## Crates

| Crate | What it does | Best for |
|---|---|---|
| [`taquba`](./taquba) | Core durable task queue | Background jobs, dead-letter, scheduled work, parallel in-process workers |
| [`taquba-workflow`](./taquba-workflow) | Multi-step orchestration with per-step memoization | LLM agent runs, payment flows, document pipelines |
| [`taquba-bulk`](./taquba-bulk) | Applies one pipeline definition to many inputs in parallel, with one workflow run per input, per-item memoization, and cost rollup | Bulk LLM workloads, document/OCR pipelines, data enrichment, parameter sweeps |
| [`taquba-jobs`](./taquba-jobs) | Typed async function execution with awaitable results | Typed background tasks where you await the return value |
| [`taquba-cron`](./taquba-cron) | POSIX cron scheduling onto a Taquba queue | Periodic enqueues (reports, sweeps, reminders) |
| [`taquba-webhooks`](./taquba-webhooks) | HTTP webhook delivery with retries and dead-letter | Outbound webhook fan-out with durable retries |

## How the crates relate

`taquba` is the base; every other crate is a consumer of one `Arc<Queue>`.
Above it sit two independent execution layers, plus a batch orchestrator:

- **`taquba-jobs`** runs one typed async function and lets you await its
  result. Single-shot, with idempotent submission and per-job result
  retention.
- **`taquba-workflow`** runs one durable multi-step process: a sequence of
  steps with per-step memoization, retries, and a terminal hook.
- **`taquba-bulk`** applies one `Pipeline` definition to many inputs in
  parallel. Each input becomes its own workflow run; bulk adds batch-level
  progress, cost rollup, streamed output, and replay. It is built on
  `taquba-workflow`, not on `taquba-jobs`.

`jobs` and `workflow` are siblings, not layers: neither depends on the other.
Use `jobs` when you dispatch a typed task and await its return value;
`workflow` when you have one multi-step run; `bulk` to run a multi-step
pipeline across a whole dataset.

### Boundary cases

Three recurring choices, and how to decide:

- **A single-step workflow or a job?** Both run one task durably. Use
  `jobs` when the caller awaits a typed return value in process; use
  `workflow` when the caller observes the run instead, through
  cancellation, headers, and a terminal hook.
- **Chained jobs or a workflow?** `JobContext::submit` lets job A submit
  job B, so a pipeline can be approximated by chaining. If you are
  chaining jobs to model one process, use `workflow`: chained jobs share
  no run identity, no end-to-end terminal status, and no resume point.
- **Job fan-out or bulk?** Submitting N jobs and awaiting their handles
  yields N independent typed results. Use `bulk` when each item is itself
  a multi-phase pipeline whose completed phases should survive a retry,
  and the batch needs progress, cost rollup, and a failure threshold.

### Composing workflow + jobs

The two compose for **fan-out inside a single run**: a workflow step submits
N typed jobs to a shared `JobRunner`, joins their results, and memoizes the
aggregate so a step retry does not re-submit. The reference agent
[`taquba-research`](https://github.com/micllam/taquba-research) uses this for
its parallel page-fetch phase, cancelling
in-flight jobs when the surrounding run is cancelled. This is the inner
counterpart to bulk's outer fan-out: `bulk` parallelizes whole runs, while the
composition parallelizes sub-tasks within one run. Today it is a manual
composition pattern, not a separate crate; a runnable demonstration lives at
[`taquba-workflow/examples/fanout_jobs.rs`](./taquba-workflow/examples/fanout_jobs.rs).

## Stability

Pre-1.0. Minor version bumps may break source compatibility *and* the on-disk
layout. Drain in-flight runs before upgrading across minors. Patch bumps
preserve both.

## Performance

Taquba is built for durability and operational simplicity rather than raw
speed. End-to-end latency is floored by an object-store write round trip
(tens to hundreds of milliseconds on S3), and durable throughput reaches
thousands of transitions per second, scaling with producer concurrency.
Measured numbers, with the environment and commit that produced them, are
recorded in [`taquba-bencher/RESULTS.md`](./taquba-bencher/RESULTS.md);
reproducible benchmarks for every crate live in the internal
[`taquba-bencher`](./taquba-bencher) crate (see its
[README](./taquba-bencher/README.md) for the benchmark catalogue and
instructions for running them).

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

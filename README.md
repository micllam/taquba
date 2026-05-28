# Taquba

A durable task queue and workflow runtime for Rust, with **no stateful service
to operate**. Workflow state lives directly in your object storage; every
compute node is replaceable.

Taquba is a workspace of Rust crates that compose into a durable execution
stack. There is no Postgres, Redis, or broker daemon to run alongside your
workers. Queue records, workflow memos, lease bookkeeping, and retention all
live in customer-owned object storage (S3, GCS, Azure Blob, or local disk) via
[SlateDB](https://github.com/slatedb/slatedb). Workers are stateless and
interchangeable, making spot / preemptible compute the default deployment shape
rather than an optimisation.

## Why this is different

- **No stateful service.** Most single-process durable queue libraries
  require a database (typically Postgres) to hold their state. Taquba uses
  the object storage you already have.
- **State sovereignty for free.** Workflow records never leave your account
  because there is nowhere else for them to go.
- **Library-shaped, not infrastructure.** Embedded in your binary as a Rust
  crate. No control plane to deploy, scale, or upgrade.
- **Spot-native by design.** Stateless compute plus durable state make
  preemption a recoverable event, not a disaster.

## Crates

| Crate | What it does | Best for |
|---|---|---|
| [`taquba`](./taquba) | Core durable task queue | Background jobs, dead-letter, scheduled work, parallel in-process workers |
| [`taquba-workflow`](./taquba-workflow) | Multi-step orchestration with per-step memoization | LLM agent runs, payment flows, document pipelines |
| [`taquba-jobs`](./taquba-jobs) | Typed async function execution with awaitable results | Typed background tasks where you await the return value |
| [`taquba-cron`](./taquba-cron) | POSIX cron scheduling onto a Taquba queue | Periodic enqueues (reports, sweeps, reminders) |
| [`taquba-webhooks`](./taquba-webhooks) | HTTP webhook delivery with retries and dead-letter | Outbound webhook fan-out with durable retries |

## Quick taste

A workflow on an in-memory store. Swap `InMemory` for an S3 / GCS / Azure
builder in production; nothing else changes.

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

## What this isn't

- **Not multi-node.** SlateDB's single-writer model means one process owns
  each store. Producers and workers must share an `Arc<Queue>` in the same
  binary.

## Stability

Pre-1.0. Minor version bumps may break source compatibility *and* the on-disk
layout. Drain in-flight runs before upgrading across minors. Patch bumps
preserve both.

## Performance

Reproducible benchmarks live under [`taquba/benches/`](./taquba/benches);
see [`taquba/benches/README.md`](./taquba/benches/README.md) for what's
there and how to run them.

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

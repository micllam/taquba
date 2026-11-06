# Taquba

[![crates.io](https://img.shields.io/crates/v/taquba.svg)](https://crates.io/crates/taquba)
[![docs.rs](https://img.shields.io/docsrs/taquba)](https://docs.rs/taquba)
[![license](https://img.shields.io/crates/l/taquba.svg)](#license)

A durable task queue for Rust whose state lives in object storage. Jobs, their
payloads and the application's own state are stored in S3, GCS, Azure Blob or a
local directory through [SlateDB](https://github.com/slatedb/slatedb). There is
no queue server or database server to operate.

Taquba is embedded: one process owns a store and commits every transition, and
its producers and workers share one `Queue`. Workers are stateless, so a
replacement process resumes where the previous one stopped. Other processes send
their requests through an inbox.

> An application with durable multi-step processes or typed jobs should use
> [`taquba-workflow`](https://github.com/micllam/taquba/tree/master/taquba-workflow),
> the durable-execution runtime built on this queue. The
> [workspace README](https://github.com/micllam/taquba) describes every crate.

## Quick start

```bash
cargo add taquba
cargo add tokio --features full
```

```rust
use std::sync::Arc;
use std::time::Duration;
use taquba::{Queue, object_store::memory::InMemory};

#[tokio::main]
async fn main() -> taquba::Result<()> {
    let q = Queue::open(Arc::new(InMemory::new()), "demo").await?;

    q.enqueue("email", b"alice@example.com".to_vec()).await?;

    if let Some(job) = q.claim("email", Duration::from_secs(30)).await? {
        // ... do the work ...
        q.ack(&job).await?;
    }

    q.close().await
}
```

The [examples](https://github.com/micllam/taquba/tree/master/taquba/examples)
include a worker loop with retries and dead-letter inspection, and stores on
local disk.

## Job lifecycle

```text
enqueue at a future run_at    enqueue
    │                            │
    ▼      run_at, or a wake     ▼       claim             ack
scheduled ──────────────────▶ pending ────────▶ claimed ──────────▶ done
    ▲                            ▲                 │
    │                            └─────────────────┤ lease expiry
    └──────────────────────────────────────────────┤ nack, retried later
                                                   └──────────────▶ dead
                                                     nack at the
                                                     attempt limit
```

A scheduled job becomes pending at its `run_at`, or earlier on a wake or a
signal to its id. A worker keeps a claimed job under a lease, and the job
returns to pending if the lease expires before the settlement. Delivery is
therefore at least once, so a worker must be idempotent.

A cancellation removes a pending or scheduled job and asks the worker of a
claimed job to stop. A dead job stays for inspection until a requeue returns it
to pending.

## Transactional settlement

A settlement commits in one transaction with its effects:

- Follow-up jobs.
- Writes and deletes of the application's keys, with conditions on their current
  values.
- Changes to counts, the named numbers of active items that a scheduled job can
  wait for.

After a crash, the settlement and all its effects are present, or none of them.
A state machine built on the queue therefore stays consistent without an outbox
or a second datastore.
[`examples/atomic_settlement.rs`](https://github.com/micllam/taquba/blob/master/taquba/examples/atomic_settlement.rs)
acknowledges an order, enqueues its confirmation and records its status in one
commit.

## Storage backends

The in-memory and local-disk stores work without a feature flag. A cloud store
requires the feature of its backend:

```bash
cargo add taquba --features aws    # S3 / MinIO
cargo add taquba --features gcp    # Google Cloud Storage
cargo add taquba --features azure  # Azure Blob
```

The `metrics` feature emits queue and storage metrics through the
[`metrics`](https://docs.rs/metrics) facade. Spans and log events go through
[`tracing`](https://docs.rs/tracing).

## When to choose Taquba

Taquba is the correct choice when:

- Steps are slow and IO-bound, such as LLM calls and rate-limited APIs.
- Backlogs are large or bursty, or the workload is mostly idle.
- Payloads are large.
- State crosses machines or accounts, or belongs with data already in a bucket.

A different tool is the correct choice for:

- A queue that must share the application's database transactions.
- Compute spread over a fleet of workers on several machines.
- A latency-sensitive path, because each transition waits for an object-store
  write.
- Short jobs at high volume.

The
[workspace README](https://github.com/micllam/taquba#when-to-use-it-and-when-not-to)
explains each criterion, and
[`taquba-bencher/RESULTS.md`](https://github.com/micllam/taquba/blob/master/taquba-bencher/RESULTS.md)
records the measurements.

## Stability

Taquba is pre-1.0. A minor release can break the Rust API and the on-disk
format, and a patch release preserves both. An upgrade to a new minor version
requires a drained queue or an empty store.

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

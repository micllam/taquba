# Taquba

A durable, single-process task queue for Rust with **no stateful service to
operate**. Queue state lives directly in your object storage; compute is
stateless and replaceable.

> The foundation of the [Taquba ecosystem](https://github.com/micllam/taquba);
> see the workspace README for the workflow runtime, cron, jobs, and webhooks
> crates that build on this queue.

Built on [SlateDB](https://github.com/slatedb/slatedb). All producers and
workers for a given store run inside one process and share an `Arc<Queue>`.
Use Taquba when you want durable background jobs whose state survives node
loss, ephemeral disks, and region failures, without operating a queue
server or a separate state layer (typically Postgres).

## Features

- At-least-once delivery with lease-based claims and crash recovery.
- Multiple named queues per store with per-queue configuration.
- Priority levels (FIFO within each priority).
- Scheduled jobs, dedup keys, custom priority/attempts.
- Exponential retry backoff on `nack`.
- Bounded dead-letter retention with paginated inspection.
- Atomic batch enqueue.
- Worker loop with graceful shutdown and notify-based wakeups (no busy polling).

## Stability

Taquba is pre-1.0. The Rust API may evolve between minor versions per Cargo's
standard `0.x.y` semantics (`0.1` -> `0.2` may break source compatibility), and
the on-disk format on object storage is *not* guaranteed stable across minor
versions either. Treat a Taquba minor-version bump as a one-way migration:
drain your queue first, or be prepared to start the bucket fresh.

Patch releases (`0.1.0` -> `0.1.1`) preserve both the Rust API and the on-disk
format.

## Install

The in-memory and local-disk stores work with no feature flag, handy for
tests and the quick-start below:

```bash
cargo add taquba
cargo add tokio --features full
```

For production, opt in to exactly one cloud backend:

```bash
cargo add taquba --features aws    # S3 / MinIO
cargo add taquba --features gcp    # Google Cloud Storage
cargo add taquba --features azure  # Azure Blob
```

## Quick start

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

## Worker loop

Implement `Worker` and let `run_worker` handle the claim / ack / nack
loop, retries, and graceful shutdown:

```rust
use taquba::{run_worker, JobRecord, Worker, WorkerError};

struct EmailWorker;

impl Worker for EmailWorker {
    async fn process(&self, job: &JobRecord) -> Result<(), WorkerError> {
        let to = std::str::from_utf8(&job.payload)?;
        send_email(to).await?;
        Ok(())
    }
}
```

Pass any future as the shutdown signal: `tokio::signal::ctrl_c()`,
a oneshot, etc. Shutdown is honoured at safe points: between jobs and during
idle waits. In-flight jobs always finish, so leases are never abandoned to the
reaper. See [`examples/worker.rs`](examples/worker.rs) for a full setup.

## Coordinating with caller state

`Queue::enqueue_with_kv` enqueues a job *and* applies a set of writes to a
caller-owned KV namespace in a single transaction, so a downstream crate can
keep its own durable coordination state (status markers, dedup records,
pointers to externally-stored blobs) consistent with the queue across crashes.
`Queue::kv_get` and `Queue::kv_delete` read and clean up those entries.

Caller keys live under a reserved `usr:` prefix internally so they cannot
collide with Taquba's own layout. Per-value size is capped at
`MAX_KV_VALUE_SIZE` (256 KiB); the namespace is sized for coordination state,
not bulk payload. Store large blobs in the underlying object store under a
content-addressed key and put only the pointer in KV.

The namespace is mutated **only** as a side effect of queue operations so there
is no standalone `kv_put`. To create or update an entry, include it in the
`kv_writes` map of an `enqueue_with_kv` call (which makes the write atomic
with the enqueue). `kv_delete` is the one standalone primitive, for terminal
cleanup of entries whose related queue op has already completed.

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

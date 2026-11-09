# taquba-webhooks

[![crates.io](https://img.shields.io/crates/v/taquba-webhooks.svg)](https://crates.io/crates/taquba-webhooks)
[![docs.rs](https://img.shields.io/docsrs/taquba-webhooks)](https://docs.rs/taquba-webhooks)
[![license](https://img.shields.io/crates/l/taquba-webhooks.svg)](#license)

Outbound HTTP webhooks for the
[`taquba`](https://github.com/micllam/taquba/tree/master/taquba) queue. An
application enqueues a delivery as a job, and a worker sends the request to the
receiver with retries. The deliveries are stored with the queue, in S3, GCS,
Azure Blob or a local directory, so a pending delivery persists across a
restart.

The worker runs in the process that owns the queue and keeps only the status of
each response. It suits a notification that does not need a reply, such as an
alert, an event relay or the callback of a finished workflow run. A delivery can
commit in the settlement of other work, and the queue then enqueues it only when
that work commits.

> Part of the [Taquba ecosystem](https://github.com/micllam/taquba). The
> workspace README states when object storage is the correct choice and how the
> crates compose.

## Quick start

```bash
cargo add taquba-webhooks taquba
cargo add tokio --features full
```

```rust,no_run
use std::sync::Arc;
use std::time::Duration;
use taquba::{Queue, object_store::memory::InMemory, run_worker};
use taquba_webhooks::{WebhookRequest, WebhookWorker, enqueue_webhook};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Queue::open(Arc::new(InMemory::new()), "demo").await?;

    let request = WebhookRequest::new("https://example.com/hook")
        .header("Content-Type", "application/json")
        .timeout(Duration::from_secs(10));
    enqueue_webhook(&queue, "webhooks", request, br#"{"event":"ping"}"#.to_vec()).await?;

    let worker = WebhookWorker::new();
    run_worker(
        &queue,
        "webhooks",
        &worker,
        Duration::from_millis(100),
        std::future::pending::<()>(),
    )
    .await?;
    Ok(())
}
```

The
[example](https://github.com/micllam/taquba/tree/master/taquba-webhooks/examples)
delivers requests to a local HTTP listener and prints what arrives.

## Delivery

A 2xx response completes a delivery. A 4xx response other than 408 and 429
dead-letters it without a retry, because the receiver has rejected the request.
Any other failure, such as a 5xx response or a timeout, retries the delivery
with backoff up to the attempt limit of the queue.

Every request has a timeout, and the worker extends the lease of the job to
cover it. The job therefore remains claimed while a slow receiver responds, and
the queue does not deliver it to another worker meanwhile.

## Idempotent receivers

Delivery is at least once, and a retry sends a request again after a lost
response. By default each request has the id of its job in the header
`Webhook-Id`, and a receiver must deduplicate on it.

## Stability

taquba-webhooks is pre-1.0. A minor release can break the Rust API and the
on-disk format, whereas a patch release preserves both.

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

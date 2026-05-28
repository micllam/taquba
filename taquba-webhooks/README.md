# taquba-webhooks

HTTP webhook delivery on top of the [Taquba](../taquba) durable task queue.

> Part of the [Taquba ecosystem](https://github.com/micllam/taquba); see the
> workspace README for the queue core and the other crates that compose with
> this one.

Aimed at workloads where a webhook is the announcement of an event
(alerting on a timeseries DB, event relays, push notifications).

## Usage

Taquba is single-process: producer and worker run in the same process and
share one `Arc<Queue>`.

```rust
use std::sync::Arc;
use std::time::Duration;
use taquba::{Queue, object_store::memory::InMemory, run_worker};
use taquba_webhooks::{WebhookRequest, WebhookWorker, enqueue_webhook};

let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "demo").await?);

// Worker: deliver each request.
let worker_queue = queue.clone();
tokio::spawn(async move {
    let worker = WebhookWorker::new();
    run_worker(
        &worker_queue,
        "webhooks",
        &worker,
        Duration::from_millis(100),
        std::future::pending::<()>(),
    )
    .await
});

// Producer: enqueue a delivery.
let request = WebhookRequest::new("https://example.com/hook")
    .header("Content-Type", "application/json")
    .timeout(Duration::from_secs(10));
enqueue_webhook(&queue, "webhooks", request, br#"{"event":"ping"}"#.to_vec()).await?;
```

## Reserved header keys

Webhook configuration travels with each job in `JobRecord::headers` under
reserved keys:

| Key | Required | Meaning |
|---|---|---|
| `webhook.url` | yes | Target URL |
| `webhook.method` | no | HTTP method (default `POST`) |
| `webhook.timeout_ms` | no | Per-request timeout |
| `http.<name>` | no | HTTP header to send (e.g. `http.Content-Type`) |

Other entries in `headers` are ignored by the worker — your application can
use them for its own metadata.

## Delivery semantics

- **2xx response**: ack (Taquba marks the job done).
- **5xx, 408 Request Timeout, 429 Too Many Requests, transport errors,
  timeouts**: nack. Taquba retries on its configured exponential backoff
  up to the queue's `max_attempts`, then dead-letters.
- **Other 4xx (client errors), missing/invalid configuration headers**:
  dead-letter immediately via `taquba::PermanentFailure`. The receiver has
  explicitly rejected the request; retrying won't help.

## Receiver-side idempotency

Each delivery includes a `Webhook-Id` header (configurable via
`WebhookWorker::with_delivery_id_header`) carrying `JobRecord::id`. Taquba
guarantees at-least-once, not exactly-once, so receivers must dedupe on this
header to handle retries correctly.

## License

Apache-2.0

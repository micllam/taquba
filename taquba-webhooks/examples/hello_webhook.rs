// cargo run -p taquba-webhooks --example hello_webhook
//
// End-to-end demo of taquba-webhooks: spins up a local axum HTTP listener
// that captures incoming requests, enqueues a few webhook deliveries pointed
// at it, runs the WebhookWorker, prints what arrived, and shuts down.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    body::Bytes,
    http::{HeaderMap, Method, StatusCode, Uri},
    routing::post,
};
use taquba::{Queue, object_store::memory::InMemory, run_worker};
use taquba_webhooks::{WebhookRequest, WebhookWorker, enqueue_webhook};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Bind to an ephemeral port.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let url = format!("http://{addr}/hook");
    println!("HTTP listener bound to {url}");

    // Spawn the listener. The runtime cancels it when main returns.
    let app = Router::new().route("/hook", post(receive_webhook));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // Open a Taquba queue and enqueue three webhook deliveries.
    let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "demo").await?);

    for (i, body) in [
        br#"{"event":"alert.fired","severity":"warning"}"# as &[u8],
        br#"{"event":"alert.fired","severity":"critical"}"#,
        br#"{"event":"alert.resolved"}"#,
    ]
    .iter()
    .enumerate()
    {
        let request = WebhookRequest::new(&url)
            .header("Content-Type", "application/json")
            .header("User-Agent", "hello_webhook/1.0")
            .timeout(Duration::from_secs(5));
        let id = enqueue_webhook(&queue, "webhooks", request, body.to_vec()).await?;
        println!("Enqueued webhook #{} as {id}", i + 1);
    }
    println!();

    let worker = WebhookWorker::new();

    let (worker_shutdown_tx, worker_shutdown_rx) = oneshot::channel::<()>();
    let q = queue.clone();
    let worker_handle = tokio::spawn(async move {
        run_worker(
            &q,
            "webhooks",
            &worker,
            Duration::from_millis(50),
            async move {
                let _ = worker_shutdown_rx.await;
            },
        )
        .await
    });

    // Drain: wait until the queue is empty (jobs claimed and acked).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let stats = queue.stats("webhooks").await?;
        if stats.pending == 0 && stats.claimed == 0 && stats.scheduled == 0 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for queue to drain".into());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let stats = queue.stats("webhooks").await?;
    println!("Final stats: done={} dead={}", stats.done, stats.dead);

    let _ = worker_shutdown_tx.send(());
    let _ = worker_handle.await?;

    Ok(())
}

async fn receive_webhook(method: Method, uri: Uri, headers: HeaderMap, body: Bytes) -> StatusCode {
    println!("  > {method} {uri}");
    for (name, value) in headers.iter() {
        let v = value.to_str().unwrap_or("<non-ascii>");
        println!("  > {name}: {v}");
    }
    println!("  >");
    println!("  > {}", String::from_utf8_lossy(&body));
    println!();
    StatusCode::OK
}

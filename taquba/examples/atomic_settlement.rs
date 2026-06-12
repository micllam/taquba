// cargo run -p taquba --example atomic_settlement
//
// Demonstrates transactional coordination between queue state and
// caller-owned KV state, via `Queue::enqueue_with_kv`,
// `Worker::process_with_effects`, and `Queue::ack_with`:
//
// - Intake: `enqueue_with_kv` creates each order job and its durable
//   status marker ("received") in one transaction, with a dedup key so
//   a duplicate submission of the same order collapses onto the
//   in-flight job.
// - Processing: the order worker returns `AckEffects` from
//   `process_with_effects`; the worker loop applies them via
//   `Queue::ack_with`, so the order's ack, the follow-up confirmation
//   enqueue, and the status update to "processed" land in the same
//   transaction. A confirmation job exists only if the settlement that
//   created it won: if the order's lease had expired and the claim was
//   gone, nothing would be applied and the retried attempt would
//   settle instead.
// - Confirmation: the confirmation worker settles the same way, moving
//   the status to "confirmed".
//
// A crash at any point leaves the status marker consistent with the
// queue: there is no window where an order is acked but its follow-up
// or status update is missing, and no outbox pattern or second
// datastore is involved.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use taquba::{
    AckEffects, EnqueueOptions, EnqueueRequest, EnqueueResult, JobRecord, Queue, Worker,
    WorkerError, object_store::memory::InMemory, run_worker,
};

const ORDERS_QUEUE: &str = "orders";
const CONFIRMATIONS_QUEUE: &str = "confirmations";

fn status_key(order_id: &str) -> Vec<u8> {
    format!("order:{order_id}").into_bytes()
}

/// Processes an order and, atomically with its ack, enqueues the
/// confirmation job and moves the order's status to "processed".
struct OrderWorker;

impl Worker for OrderWorker {
    async fn process_with_effects(&self, job: &JobRecord) -> Result<AckEffects, WorkerError> {
        let order_id = std::str::from_utf8(&job.payload)?.to_string();
        println!("[orders]        processing order {order_id}");

        // ... charge the customer, reserve stock, etc. ...

        let mut effects = AckEffects::default();
        effects.enqueues.push(EnqueueRequest {
            queue: CONFIRMATIONS_QUEUE.to_string(),
            payload: order_id.clone().into_bytes(),
            options: EnqueueOptions::default(),
        });
        effects
            .kv_writes
            .insert(status_key(&order_id), b"processed".to_vec());
        Ok(effects)
    }
}

/// Settles the confirmation, moving the order's status to "confirmed"
/// atomically with the confirmation job's ack.
struct ConfirmationWorker;

impl Worker for ConfirmationWorker {
    async fn process_with_effects(&self, job: &JobRecord) -> Result<AckEffects, WorkerError> {
        let order_id = std::str::from_utf8(&job.payload)?.to_string();
        println!("[confirmations] confirming order {order_id}");

        let mut effects = AckEffects::default();
        effects
            .kv_writes
            .insert(status_key(&order_id), b"confirmed".to_vec());
        Ok(effects)
    }
}

async fn submit_order(q: &Queue, order_id: &str) -> taquba::Result<()> {
    let mut kv = HashMap::new();
    kv.insert(status_key(order_id), b"received".to_vec());
    let outcome = q
        .enqueue_with_kv(
            ORDERS_QUEUE,
            order_id.as_bytes().to_vec(),
            EnqueueOptions {
                dedup_key: Some(format!("order:{order_id}")),
                ..EnqueueOptions::default()
            },
            kv,
        )
        .await?;
    match outcome {
        EnqueueResult::New(id) => println!("[intake]        order {order_id} accepted ({id})"),
        EnqueueResult::AlreadyEnqueued(id) => {
            println!("[intake]        order {order_id} already in flight ({id})")
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let q = Arc::new(Queue::open(Arc::new(InMemory::new()), "settlement-demo").await?);

    let orders = ["1001", "1002", "1003"];
    for order_id in orders {
        submit_order(&q, order_id).await?;
    }
    // The duplicate hits the dedup key and does not enqueue.
    submit_order(&q, "1001").await?;
    println!();

    // One worker loop per queue, each stopped via a oneshot once every
    // order has reached "confirmed".
    let (orders_tx, orders_rx) = tokio::sync::oneshot::channel::<()>();
    let (confirm_tx, confirm_rx) = tokio::sync::oneshot::channel::<()>();
    let order_loop = {
        let q = q.clone();
        tokio::spawn(async move {
            run_worker(
                &q,
                ORDERS_QUEUE,
                &OrderWorker,
                Duration::from_millis(50),
                async move {
                    let _ = orders_rx.await;
                },
            )
            .await
        })
    };
    let confirm_loop = {
        let q = q.clone();
        tokio::spawn(async move {
            run_worker(
                &q,
                CONFIRMATIONS_QUEUE,
                &ConfirmationWorker,
                Duration::from_millis(50),
                async move {
                    let _ = confirm_rx.await;
                },
            )
            .await
        })
    };

    // Watch the status markers until every order is confirmed.
    loop {
        let mut confirmed = 0;
        for order_id in orders {
            if q.kv_get(&status_key(order_id)).await?.as_deref() == Some(b"confirmed".as_slice()) {
                confirmed += 1;
            }
        }
        if confirmed == orders.len() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = orders_tx.send(());
    let _ = confirm_tx.send(());
    order_loop.await??;
    confirm_loop.await??;

    println!();
    for order_id in orders {
        let status = q.kv_get(&status_key(order_id)).await?;
        println!(
            "order {order_id}: {}",
            String::from_utf8_lossy(status.as_deref().unwrap_or(b"<missing>"))
        );
        // Terminal cleanup: the queue operations these markers relate
        // to have all completed, so standalone deletion is safe.
        q.kv_delete(&status_key(order_id)).await?;
    }
    Ok(())
}

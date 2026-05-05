// cargo run -p taquba --example in_memory
//
// Demonstrates the core taquba API using an in-memory store.
// Nothing is persisted: the queue disappears when the process exits.
// This is the fastest way to explore the API locally.

use std::sync::Arc;
use std::time::Duration;

use taquba::{EnqueueOptions, OpenOptions, Queue, QueueConfig, object_store::memory::InMemory};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    // Disable retry backoff so the demo can re-claim a nacked job immediately.
    // In production the default exponential backoff (1s base, 5min cap) will park
    // failed jobs in the scheduled space until their delay elapses.
    let opts = OpenOptions {
        default_queue_config: QueueConfig {
            retry_backoff_base: Duration::ZERO,
            retry_backoff_max: Duration::ZERO,
            ..QueueConfig::default()
        },
        ..OpenOptions::default()
    };
    let q = Queue::open_with_options(store, "demo", opts).await?;

    let id_a = q.enqueue("tasks", b"task A".to_vec()).await?;
    let id_b = q.enqueue("tasks", b"task B".to_vec()).await?;
    // Explicit max_attempts overrides the queue default.
    let id_c = q
        .enqueue_with(
            "tasks",
            b"task C".to_vec(),
            EnqueueOptions {
                max_attempts: Some(1),
                ..Default::default()
            },
        )
        .await?;

    println!("enqueued: {id_a}, {id_b}, {id_c}");

    let s = q.stats("tasks").await?;
    println!(
        "after enqueue: pending:{} claimed:{} done:{} dead:{}",
        s.pending, s.claimed, s.done, s.dead
    );

    let job_a = q
        .claim("tasks", Duration::from_secs(30))
        .await?
        .expect("queue not empty");
    assert_eq!(job_a.id, id_a); // FIFO order
    println!(
        "claimed '{}' (attempt {}/{})",
        String::from_utf8_lossy(&job_a.payload),
        job_a.attempts,
        job_a.max_attempts
    );

    q.ack(&job_a).await?;
    println!("acked {}", job_a.id);

    let job_b = q
        .claim("tasks", Duration::from_secs(30))
        .await?
        .expect("queue not empty");
    q.nack(job_b, "something went wrong").await?;
    println!("nacked task B - it will be retried");

    // task B is back at the front; claim it again
    let job_b2 = q
        .claim("tasks", Duration::from_secs(30))
        .await?
        .expect("retried job is available");
    assert_eq!(job_b2.attempts, 2);
    println!("re-claimed task B (attempt {})", job_b2.attempts);
    q.ack(&job_b2).await?;

    let job_c = q
        .claim("tasks", Duration::from_secs(30))
        .await?
        .expect("task C pending");
    // max_attempts=1 so this nack goes straight to dead-letter
    q.nack(job_c, "unrecoverable failure").await?;

    let dead = q.dead_jobs("tasks", None, 100).await?;
    assert_eq!(dead.len(), 1);
    println!("dead-letter: {}: {:?}", dead[0].id, dead[0].last_error);

    q.requeue_dead_job(dead.into_iter().next().unwrap()).await?;
    println!("requeued dead job for a fresh attempt");

    let s = q.stats("tasks").await?;
    println!(
        "final stats: pending:{} claimed:{} done:{} dead:{}",
        s.pending, s.claimed, s.done, s.dead
    );

    println!("known queues: {:?}", q.list_queues().await?);

    q.close().await?;
    Ok(())
}

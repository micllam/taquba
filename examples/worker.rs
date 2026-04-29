// cargo run --example worker
//
// Demonstrates the Worker trait. Implement `process` and taquba handles
// the claim / ack / nack loop automatically, including retry on failure.

use std::sync::Arc;
use std::time::Duration;

use taquba::{
    JobRecord, OpenOptions, Queue, QueueConfig, Worker, WorkerError,
    object_store::memory::InMemory, run_worker,
};

struct PrintWorker {
    /// Simulate occasional failures to show the retry path.
    fail_every: u32,
    count: std::sync::atomic::AtomicU32,
}

impl PrintWorker {
    fn new(fail_every: u32) -> Self {
        Self {
            fail_every,
            count: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

impl Worker for PrintWorker {
    async fn process(&self, job: &JobRecord) -> Result<(), WorkerError> {
        let n = self
            .count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let payload = String::from_utf8_lossy(&job.payload);

        if self.fail_every > 0 && n % self.fail_every == 0 {
            println!(
                "  [attempt {}/{}] FAIL  '{}'",
                job.attempts, job.max_attempts, payload
            );
            return Err(format!("simulated failure on call {n}").into());
        }

        println!(
            "  [attempt {}/{}] OK    '{}'",
            job.attempts, job.max_attempts, payload
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = OpenOptions::default();
    opts.queue_configs.insert(
        "jobs".to_string(),
        QueueConfig {
            max_attempts: 3,
            lease_duration: Duration::from_secs(10),
            // Tight backoff so the demo runs quickly while still exercising the
            // scheduled-retry path that production deployments rely on.
            retry_backoff_base: Duration::from_millis(50),
            retry_backoff_max: Duration::from_millis(50),
            ..QueueConfig::default()
        },
    );

    let q = Arc::new(Queue::open_with_options(Arc::new(InMemory::new()), "demo", opts).await?);

    // Enqueue a mix of jobs.
    for i in 1..=6 {
        q.enqueue("jobs", format!("job-{i}").into_bytes()).await?;
    }
    println!("enqueued 6 jobs");
    println!();

    // The worker fails every 3rd call, demonstrating automatic retry.
    let worker = Arc::new(PrintWorker::new(3));

    // Drive the worker via a oneshot shutdown signal. When the queue is fully
    // drained we send `()`, run_worker finishes its current poll and returns
    // cleanly (no aborted in-flight jobs).
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let q2 = q.clone();
    let w = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker(
            &q2,
            "jobs",
            w.as_ref(),
            Duration::from_millis(50),
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
    });

    // Wait until the queue is drained, including jobs parked in `scheduled`
    // for retry backoff.
    loop {
        let s = q.stats("jobs").await?;
        if s.pending == 0 && s.claimed == 0 && s.scheduled == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = shutdown_tx.send(());
    let _ = handle.await;

    println!();
    let s = q.stats("jobs").await?;
    println!(
        "done - pending:{} claimed:{} done:{} dead:{}",
        s.pending, s.claimed, s.done, s.dead
    );

    if s.dead > 0 {
        println!();
        println!("dead-letter jobs:");
        for job in q.dead_jobs("jobs", None, 100).await? {
            println!("  {} - {:?}", job.id, job.last_error);
        }
    }

    Ok(())
}

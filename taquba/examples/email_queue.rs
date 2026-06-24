// cargo run -p taquba --example email_queue
//
// Demonstrates a realistic email notification queue with two tiers:
//
//   "transactional" - password resets, order confirmations, OTPs.
//                     High priority, up to 5 delivery attempts.
//
//   "marketing"     - newsletters, promotional emails.
//                     Low priority, up to 2 delivery attempts.
//
// Both queues share a single Queue instance. Workers drain transactional
// jobs before marketing jobs thanks to the priority difference.

use std::sync::Arc;
use std::time::Duration;

use taquba::{
    EnqueueOptions, JobRecord, OpenOptions, PRIORITY_HIGH, PRIORITY_LOW, PRIORITY_NORMAL, Queue,
    QueueConfig, Worker, WorkerError, object_store::memory::InMemory, run_worker,
};

// In a real application you would encode these as JSON or MessagePack.
// Here we use a simple "to:subject" text format to keep the example dependency-free.
fn encode(to: &str, subject: &str) -> Vec<u8> {
    format!("{to}\x00{subject}").into_bytes()
}

fn decode(payload: &[u8]) -> (&str, &str) {
    let s = std::str::from_utf8(payload).unwrap_or("");
    let mut parts = s.splitn(2, '\x00');
    let to = parts.next().unwrap_or("");
    let subject = parts.next().unwrap_or("");
    (to, subject)
}

struct EmailWorker {
    /// A counter to simulate occasional SMTP failures.
    calls: std::sync::atomic::AtomicU32,
    /// Fail every Nth call (0 = never fail).
    fail_every: u32,
}

impl EmailWorker {
    fn new(fail_every: u32) -> Self {
        Self {
            calls: std::sync::atomic::AtomicU32::new(0),
            fail_every,
        }
    }
}

impl Worker for EmailWorker {
    async fn process(&self, job: &JobRecord) -> Result<(), WorkerError> {
        let n = self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let (to, subject) = decode(&job.payload);

        if self.fail_every > 0 && n.is_multiple_of(self.fail_every) {
            eprintln!(
                "  [attempt {}/{}] SMTP error sending '{}' to {}",
                job.attempts, job.max_attempts, subject, to
            );
            return Err("connection refused by SMTP relay".into());
        }

        println!(
            "  [attempt {}/{}] sent:  '{}' -> {}",
            job.attempts, job.max_attempts, subject, to
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = OpenOptions::default();

    opts.queue_configs.insert(
        "transactional".to_string(),
        QueueConfig {
            max_attempts: 5,
            lease_duration: Duration::from_secs(30),
            default_priority: PRIORITY_HIGH,
            ..QueueConfig::default()
        },
    );
    opts.queue_configs.insert(
        "marketing".to_string(),
        QueueConfig {
            max_attempts: 2,
            lease_duration: Duration::from_secs(60),
            default_priority: PRIORITY_LOW,
            ..QueueConfig::default()
        },
    );

    // To use S3 or MinIO instead of memory, swap InMemory for an S3Builder:
    //
    //   let store = Arc::new(
    //       object_store::aws::AmazonS3Builder::new()
    //           .with_bucket_name("my-queue-bucket")
    //           .with_region("us-east-1")
    //           .build()?,
    //   );
    let q = Arc::new(Queue::open_with_options(Arc::new(InMemory::new()), "demo", opts).await?);

    println!("Enqueueing jobs...");

    // Transactional jobs inherit PRIORITY_HIGH from the queue config.
    q.enqueue(
        "transactional",
        encode("alice@example.com", "Your password reset link"),
    )
    .await?;
    q.enqueue(
        "transactional",
        encode("bob@example.com", "Order #4821 confirmed"),
    )
    .await?;
    q.enqueue(
        "transactional",
        encode("carol@example.com", "Your one-time passcode"),
    )
    .await?;

    // Marketing jobs inherit PRIORITY_LOW; they will drain after transactional.
    q.enqueue(
        "marketing",
        encode("alice@example.com", "This week's deals"),
    )
    .await?;
    q.enqueue(
        "marketing",
        encode("bob@example.com", "You have a new recommendation"),
    )
    .await?;

    // A marketing job can be explicitly boosted if needed.
    q.enqueue_with(
        "marketing",
        encode("vip@example.com", "Your exclusive early access"),
        EnqueueOptions {
            priority: Some(PRIORITY_NORMAL),
            ..Default::default()
        },
    )
    .await?;

    println!(
        "  transactional: {} pending",
        q.stats("transactional").await?.pending
    );
    println!(
        "  marketing:     {} pending",
        q.stats("marketing").await?.pending
    );
    println!();

    // Spawn one worker per queue.
    // The worker fails every 4th SMTP call to exercise the retry path.
    let worker = Arc::new(EmailWorker::new(4));

    let (t_shutdown_tx, t_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (m_shutdown_tx, m_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    println!("Processing transactional queue (high priority)...");
    let qt = q.clone();
    let wt = worker.clone();
    let t_handle = tokio::spawn(async move {
        run_worker(
            &qt,
            "transactional",
            wt.as_ref(),
            Duration::from_millis(25),
            async move {
                let _ = t_shutdown_rx.await;
            },
        )
        .await
    });

    // Give the transactional worker a head-start, then drain marketing.
    tokio::time::sleep(Duration::from_millis(50)).await;

    println!();
    println!("Processing marketing queue (low priority)...");
    let qm = q.clone();
    let wm = worker.clone();
    let m_handle = tokio::spawn(async move {
        run_worker(
            &qm,
            "marketing",
            wm.as_ref(),
            Duration::from_millis(25),
            async move {
                let _ = m_shutdown_rx.await;
            },
        )
        .await
    });

    // Wait for both queues to drain. Note that a nacked job sits in
    // retry-backoff under `Scheduled` between attempts.
    loop {
        let ts = q.stats("transactional").await?;
        let ms = q.stats("marketing").await?;
        if ts.pending == 0
            && ts.claimed == 0
            && ts.scheduled == 0
            && ms.pending == 0
            && ms.claimed == 0
            && ms.scheduled == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let _ = t_shutdown_tx.send(());
    let _ = m_shutdown_tx.send(());
    let _ = t_handle.await;
    let _ = m_handle.await;

    println!();
    let ts = q.stats("transactional").await?;
    let ms = q.stats("marketing").await?;
    println!("transactional - done:{} dead:{}", ts.done, ts.dead);
    println!("marketing     - done:{} dead:{}", ms.done, ms.dead);

    for queue in ["transactional", "marketing"] {
        let dead = q.dead_jobs(queue, None, 100).await?;
        if !dead.is_empty() {
            println!();
            println!("Dead-letter ({queue}):");
            for job in dead {
                let (to, subject) = decode(&job.payload);
                println!(
                    "  {} - '{}' -> {} - {:?}",
                    job.id, subject, to, job.last_error
                );
            }
        }
    }

    Ok(())
}

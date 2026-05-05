// cargo run -p taquba --example image_resize
//
// Demonstrates a realistic image-processing queue with:
//
//   • Structured job payloads encoded with MessagePack
//   • Two priority tiers: urgent thumbnails (PRIORITY_HIGH) and
//     bulk batch resizes (PRIORITY_NORMAL)
//   • A scheduled job representing off-peak batch work (enqueue_in)
//   • A Worker that decodes the payload and "processes" the image

use std::sync::Arc;
use std::time::Duration;

use taquba::{
    EnqueueOptions, JobRecord, OpenOptions, PRIORITY_HIGH, PRIORITY_NORMAL, Queue, QueueConfig,
    Worker, WorkerError, object_store::memory::InMemory, run_worker,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ResizeTask {
    input: String,
    output: String,
    width: u32,
    height: u32,
}

impl ResizeTask {
    fn encode(&self) -> Vec<u8> {
        rmp_serde::to_vec_named(self).expect("serialization is infallible for this type")
    }

    fn decode(bytes: &[u8]) -> Self {
        rmp_serde::from_slice(bytes).expect("payload must be a valid ResizeTask")
    }
}

struct ResizeWorker;

impl Worker for ResizeWorker {
    async fn process(&self, job: &JobRecord) -> Result<(), WorkerError> {
        let task = ResizeTask::decode(&job.payload);

        // In a real worker you would call an image library here.
        // For this example we just simulate work with a short sleep.
        tokio::time::sleep(Duration::from_millis(10)).await;

        println!(
            "  [{}x{}] {} -> {}  (priority {:?}, attempt {}/{})",
            task.width,
            task.height,
            task.input,
            task.output,
            job.priority,
            job.attempts,
            job.max_attempts,
        );

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = OpenOptions::default();
    opts.queue_configs.insert(
        "resize".to_string(),
        QueueConfig {
            max_attempts: 3,
            lease_duration: Duration::from_secs(30),
            default_priority: PRIORITY_NORMAL,
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

    println!("Enqueueing urgent thumbnail jobs (PRIORITY_HIGH)...");
    for i in 1..=3 {
        let task = ResizeTask {
            input: format!("uploads/photo-{i}.jpg"),
            output: format!("thumbnails/photo-{i}-thumb.jpg"),
            width: 120,
            height: 120,
        };
        q.enqueue_with(
            "resize",
            task.encode(),
            EnqueueOptions {
                priority: Some(PRIORITY_HIGH),
                ..Default::default()
            },
        )
        .await?;
    }

    // Bulk batch resizes (normal priority).
    println!("Enqueueing bulk resize jobs (PRIORITY_NORMAL)...");
    for i in 1..=4 {
        let task = ResizeTask {
            input: format!("archive/image-{i}.jpg"),
            output: format!("archive/image-{i}-1920.jpg"),
            width: 1920,
            height: 1080,
        };
        q.enqueue("resize", task.encode()).await?;
    }

    println!("Scheduling an off-peak batch job (runs in ~1 ms)...");
    let task = ResizeTask {
        input: "raw/timelapse.mov".to_string(),
        output: "processed/timelapse-720p.mp4".to_string(),
        width: 1280,
        height: 720,
    };
    q.enqueue_with(
        "resize",
        task.encode(),
        EnqueueOptions {
            run_at: Some(std::time::SystemTime::now() + Duration::from_millis(1)),
            ..Default::default()
        },
    )
    .await?;

    let s = q.stats("resize").await?;
    println!();
    println!(
        "before promotion: pending:{} scheduled:{}",
        s.pending, s.scheduled
    );

    // Advance past the schedule window and promote.
    tokio::time::sleep(Duration::from_millis(5)).await;
    q.promote_scheduled_now().await?;

    let s = q.stats("resize").await?;
    println!(
        "after promotion: pending:{} scheduled:{}",
        s.pending, s.scheduled
    );
    println!();

    // Drain the queue.
    println!("Processing (high-priority thumbnails drain first)...");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let q2 = q.clone();
    let handle = tokio::spawn(async move {
        run_worker(
            &q2,
            "resize",
            &ResizeWorker,
            Duration::from_millis(10),
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
    });

    loop {
        let s = q.stats("resize").await?;
        if s.pending == 0 && s.claimed == 0 && s.scheduled == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = shutdown_tx.send(());
    let _ = handle.await;

    let s = q.stats("resize").await?;
    println!();
    println!(
        "done: pending:{} done:{} dead:{}",
        s.pending, s.done, s.dead
    );

    Ok(())
}

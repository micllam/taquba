// cargo run -p taquba-cron --example hello_cron
//
// Wires a cron schedule to a Taquba worker. Registers a single schedule
// firing every minute on the 0-second mark, plus a worker that prints each
// enqueued job. The example runs for up to 10 minutes or until Ctrl-C,
// whichever comes first.

use std::sync::Arc;
use std::time::Duration;

use taquba::{JobRecord, Queue, Worker, WorkerError, object_store::memory::InMemory, run_worker};
use taquba_cron::CronScheduler;
use tokio::sync::oneshot;

struct PrintWorker;

impl Worker for PrintWorker {
    async fn process(&self, job: &JobRecord) -> Result<(), WorkerError> {
        let payload = std::str::from_utf8(&job.payload).unwrap_or("<binary>");
        let now = chrono::Utc::now().format("%H:%M:%S UTC");
        println!(
            "[{now}] received cron job from queue '{}': {payload}",
            job.queue
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // In production you'd swap InMemory for an S3 / GCS / Azure / local-disk store.
    let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "cron-demo").await?);

    let mut scheduler = CronScheduler::new(queue.clone());
    scheduler.schedule(
        "minutely-task",
        "* * * * *",
        "tasks",
        b"hello from cron".to_vec(),
    )?;

    // Worker drains the `tasks` queue. Wrapping in `Arc` lets us share it
    // between this `main` future and the spawned worker task.
    let worker = Arc::new(PrintWorker);
    let (worker_shutdown_tx, worker_shutdown_rx) = oneshot::channel::<()>();
    let q = queue.clone();
    let w = worker.clone();
    let worker_handle = tokio::spawn(async move {
        run_worker(
            &q,
            "tasks",
            w.as_ref(),
            Duration::from_millis(100),
            async move {
                let _ = worker_shutdown_rx.await;
            },
        )
        .await
    });

    // Scheduler runs alongside the worker.
    let (scheduler_shutdown_tx, scheduler_shutdown_rx) = oneshot::channel::<()>();
    let scheduler_handle = tokio::spawn(async move {
        scheduler
            .run(async move {
                let _ = scheduler_shutdown_rx.await;
            })
            .await
    });

    println!("Running for up to 10 minutes. Press Ctrl-C to exit early.");
    println!("`* * * * *` fires at the start of each UTC minute.");
    println!();

    tokio::select! {
        _ = tokio::time::sleep(Duration::from_mins(10)) => {
            println!("\nTime's up; shutting down.");
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\nCtrl-C received; shutting down.");
        }
    }

    // Stop the scheduler first so no new jobs land while the worker drains.
    let _ = scheduler_shutdown_tx.send(());
    let _ = scheduler_handle.await?;

    let _ = worker_shutdown_tx.send(());
    let _ = worker_handle.await?;

    Ok(())
}

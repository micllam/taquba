// cargo run --example local_disk
//
// Demonstrates taquba backed by the local filesystem.
// Jobs written in one run survive process restarts; run it twice to see
// the second run claim the jobs left by the first.

use std::sync::Arc;

use taquba::{Queue, object_store::local::LocalFileSystem};

const QUEUE_DIR: &str = "/tmp/taquba-local-disk-example";
const QUEUE_NAME: &str = "work";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The directory must exist before LocalFileSystem can canonicalize it.
    std::fs::create_dir_all(QUEUE_DIR)?;

    let store = Arc::new(LocalFileSystem::new_with_prefix(QUEUE_DIR)?);
    let q = Queue::open(store, "data").await?;

    let pending_before = q.stats(QUEUE_NAME).await?.pending;

    if pending_before == 0 {
        // First run: enqueue some work.
        println!("No pending jobs found - enqueueing three jobs.");
        println!("Run this example again to claim them.");
        println!();

        for i in 1..=3 {
            let id = q
                .enqueue(QUEUE_NAME, format!("job-{i}").into_bytes())
                .await?;
            println!("  enqueued {id}");
        }
    } else {
        // Subsequent run: claim and process whatever is pending.
        println!("{pending_before} pending job(s) found - claiming them now.");
        println!();

        let lease = std::time::Duration::from_secs(30);
        while let Some(job) = q.claim(QUEUE_NAME, lease).await? {
            let payload = String::from_utf8_lossy(&job.payload);
            println!(
                "  processing '{}' (attempt {}/{})",
                payload, job.attempts, job.max_attempts
            );
            q.ack(&job).await?;
        }

        println!();
        let s = q.stats(QUEUE_NAME).await?;
        println!("stats: pending:{} done:{}", s.pending, s.done);
        println!();
        println!("Queue is now empty.  Run again to enqueue fresh jobs.");
    }

    q.close().await?;
    Ok(())
}

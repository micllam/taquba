// cargo run -p taquba --example long_running
//
// Demonstrates keeping a claim alive past the queue's lease with the
// `LeaseHandle` handed to `Worker::process`, in the two supported
// patterns: extending the lease at progress points, and covering a
// single slow call that has a timeout.

use std::sync::Arc;
use std::time::Duration;

use taquba::{
    JobRecord, LeaseHandle, OpenOptions, Queue, QueueConfig, Worker, WorkerError,
    object_store::memory::InMemory, run_worker,
};

// Deliberately short: the lease is a hang timer, and long work holds
// its claim by extending it.
const LEASE: Duration = Duration::from_secs(2);
const PHASE: Duration = Duration::from_secs(1);

struct LongRunningWorker;

impl Worker for LongRunningWorker {
    async fn process(&self, job: &JobRecord, lease: &LeaseHandle) -> Result<(), WorkerError> {
        match job.payload.as_slice() {
            b"phased" => phased(job, lease).await,
            _ => bounded(job, lease).await,
        }
    }
}

// Extend at progress points. Each phase secures enough lease before it
// runs, so the 3s job survives the 2s lease for as long as it keeps
// making progress; a phase that stalls lets the lease expire.
async fn phased(job: &JobRecord, lease: &LeaseHandle) -> Result<(), WorkerError> {
    for phase in 1..=3 {
        lease.ensure_at_least(PHASE)?;
        tokio::time::sleep(PHASE).await;
        println!("  [{}] phase {phase}/3 done", job.id);
    }
    Ok(())
}

// Cover one slow call. The call gets a timeout and the lease
// is extended to cover it, so the two limit each other; a call with
// no timeout gives the lease nothing to cover.
async fn bounded(job: &JobRecord, lease: &LeaseHandle) -> Result<(), WorkerError> {
    let bound = Duration::from_secs(3);
    lease.ensure_at_least(bound)?;
    tokio::time::timeout(bound, external_call())
        .await
        .map_err(|_| "external call exceeded its bound")??;
    println!("  [{}] external call done", job.id);
    Ok(())
}

async fn external_call() -> Result<(), WorkerError> {
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Frequent reaper ticks, so an expired lease would be noticed
    // well within the demo's runtime.
    let mut opts = OpenOptions::default().reaper_interval(Duration::from_millis(500));
    opts.queue_configs.insert(
        "work".to_string(),
        QueueConfig::default().lease_duration(LEASE),
    );
    let q = Arc::new(Queue::open_with_options(Arc::new(InMemory::new()), "demo", opts).await?);

    let phased_id = q.enqueue("work", b"phased".to_vec()).await?;
    let bounded_id = q.enqueue("work", b"bounded".to_vec()).await?;
    println!("enqueued {phased_id} (phased) and {bounded_id} (bounded); lease is {LEASE:?}");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let q2 = q.clone();
    let handle = tokio::spawn(async move {
        run_worker(
            &q2,
            "work",
            &LongRunningWorker,
            Duration::from_millis(50),
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
    });

    for id in [&phased_id, &bounded_id] {
        q.wait_for_completion(id, Duration::from_secs(15)).await?;
    }
    let _ = shutdown_tx.send(());
    let _ = handle.await;

    // Both jobs outlived the lease and still settled on their first
    // attempt: nothing was reaped back to pending or dead-lettered.
    let s = q.stats("work").await?;
    println!();
    println!(
        "done:{} pending:{} claimed:{} dead:{}",
        s.done, s.pending, s.claimed, s.dead
    );

    Ok(())
}

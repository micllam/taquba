// cargo run -p taquba-jobs --example hello_jobs
//
// Defines a single typed job (`Greet`), spins up a JobRunner backed by an
// in-memory object store, submits a handful of jobs concurrently, and waits
// for each typed result.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use taquba::{Queue, object_store::memory::InMemory};
use taquba_jobs::{Job, JobContext, JobRunner};

#[derive(Serialize, Deserialize)]
struct Greet {
    name: String,
}

#[derive(Debug, thiserror::Error)]
#[error("greet error: {0}")]
struct GreetError(String);

impl Job for Greet {
    const NAME: &'static str = "demo.greet";
    type Output = String;
    type Error = GreetError;

    async fn run(&self, _ctx: JobContext<'_>) -> Result<String, GreetError> {
        if self.name.is_empty() {
            return Err(GreetError("name must not be empty".into()));
        }
        Ok(format!("Hello, {}!", self.name))
    }

    fn idempotency_key(&self) -> Option<String> {
        Some(format!("greet:{}", self.name))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // In production swap InMemory for an S3 / GCS / Azure / local-disk store.
    // The queue and the result-blob store can be the same handle (as here) or
    // separate stores entirely.
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "jobs-demo").await?);

    let mut runner = JobRunner::builder()
        .queue(queue)
        .object_store(store)
        .max_concurrent_jobs(4)
        .build()?;

    runner.register::<Greet>();
    let handle = runner.spawn(std::future::pending::<()>());

    // Submit three jobs, then await each typed result.
    let mut submitted = Vec::new();
    for name in ["Alice", "Bob", "Carol"] {
        submitted.push(
            runner
                .submit(Greet {
                    name: name.to_string(),
                })
                .await?,
        );
    }

    for job in submitted {
        let greeting = job.await?;
        println!("{greeting}");
    }

    handle.shutdown().await?;
    Ok(())
}

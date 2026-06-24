// The smallest end-to-end taquba program: open a queue on an object store,
// enqueue a job, claim it, do the work, then acknowledge it.

use std::sync::Arc;
use std::time::Duration;

use taquba::{Queue, object_store::memory::InMemory};

#[tokio::main]
async fn main() -> taquba::Result<()> {
    // Point at any object store: S3, GCS, Azure Blob, MinIO, or a local dir.
    let q = Queue::open(Arc::new(InMemory::new()), "demo").await?;

    q.enqueue("email", b"alice@example.com".to_vec()).await?;

    if let Some(job) = q.claim("email", Duration::from_secs(30)).await? {
        // ... do the work ...
        q.ack(&job).await?;
    }

    q.close().await
}

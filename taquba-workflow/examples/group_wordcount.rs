//! A dependency-free job group: count the words in each document.
//!
//! Submits one job per document as a group, reads the results as they
//! terminate and rolls the per-document counters up in the caller.
//!
//! Run with: `cargo run -p taquba-workflow --example group_wordcount`

use std::sync::Arc;

use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use taquba::{Queue, object_store::memory::InMemory};
use taquba_workflow::jobs::{Job, JobContext, JobRunner};

#[derive(Serialize, Deserialize)]
struct CountWords {
    id: String,
    text: String,
}

#[derive(Serialize, Deserialize)]
struct WordCount {
    words: usize,
    chars: usize,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct CountError(String);

impl Job for CountWords {
    const NAME: &'static str = "example.count_words";
    type Output = WordCount;
    type Error = CountError;

    async fn run(&self, _ctx: JobContext<'_>) -> Result<WordCount, CountError> {
        Ok(WordCount {
            words: self.text.split_whitespace().count(),
            chars: self.text.len(),
        })
    }

    fn idempotency_key(&self) -> Option<String> {
        Some(self.id.clone())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let documents = [
        ("doc-1", "the quick brown fox"),
        ("doc-2", "lorem ipsum dolor sit amet"),
        ("doc-3", "hello world"),
    ];

    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "db").await?);
    let mut runner = JobRunner::builder(queue, store)
        .register::<CountWords>()
        .build();
    let worker = runner.spawn(std::future::pending::<()>());

    let group = runner.group::<CountWords>("wordcount")?;
    group
        .submit(documents.iter().map(|(id, text)| CountWords {
            id: (*id).to_string(),
            text: (*text).to_string(),
        }))
        .await?;

    let (mut succeeded, mut failed, mut chars) = (0, 0, 0);
    let mut results = std::pin::pin!(group.results().await?);
    while let Some(result) = results.try_next().await? {
        match result.result {
            Ok(count) => {
                println!("{}: {} words", result.key, count.words);
                succeeded += 1;
                chars += count.chars;
            }
            Err(err) => {
                println!("{}: failed: {err}", result.key);
                failed += 1;
            }
        }
    }
    worker.shutdown().await?;

    let status = group.status().await?;
    eprintln!(
        "\n{succeeded}/{} succeeded, {failed} failed; {chars} chars counted",
        status.total
    );
    Ok(())
}

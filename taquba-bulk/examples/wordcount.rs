//! A dependency-free bulk run: count the words in each document.
//!
//! Demonstrates the full path (submit N, process concurrently, stream output,
//! roll up cost) over an in-memory store, with no network calls.
//!
//! Run with: `cargo run -p taquba-bulk --example wordcount`

use std::io::{BufWriter, stdout};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use taquba::{Queue, object_store::memory::InMemory};
use taquba_bulk::{Bulk, BulkCtx, JsonlSink, Pipeline, StepError, read_jsonl};

#[derive(Serialize, Deserialize)]
struct Document {
    id: String,
    text: String,
}

#[derive(Serialize, Deserialize)]
struct WordCount {
    id: String,
    words: usize,
}

struct WordCounter;

impl Pipeline for WordCounter {
    type Input = Document;
    type Output = WordCount;
    type Error = StepError;

    async fn run(&self, ctx: &BulkCtx<Document>) -> Result<WordCount, StepError> {
        let words = ctx.input.text.split_whitespace().count();
        // Report a domain metric so the cost rollup has something to show.
        ctx.record_cost("chars", ctx.input.text.len() as f64);
        Ok(WordCount {
            id: ctx.input.id.clone(),
            words,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw = "\
{\"id\":\"doc-1\",\"text\":\"the quick brown fox\"}
{\"id\":\"doc-2\",\"text\":\"lorem ipsum dolor sit amet\"}
{\"id\":\"doc-3\",\"text\":\"hello world\"}
";
    let documents: Vec<Document> = read_jsonl(raw.as_bytes()).collect::<Result<_, _>>()?;

    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "db").await?);

    let sink = Arc::new(JsonlSink::new(BufWriter::new(stdout())));
    let bulk = Bulk::builder(queue, store, WordCounter)
        .output(sink)
        .key_fn(|doc| doc.id.clone())
        .build();

    let report = bulk.run(documents).await?;

    eprintln!(
        "\n{}/{} succeeded, {} failed in {:?}",
        report.succeeded, report.total, report.failed, report.elapsed,
    );
    for (metric, total) in report.cost.entries() {
        eprintln!("cost: {metric} = {total}");
    }
    Ok(())
}

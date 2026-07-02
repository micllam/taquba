//! A document pipeline in three stages: extract fields, classify, validate.
//!
//! This example shows a pipeline with several logical stages inside one
//! [`Pipeline::run`], each wrapped in a memoized call so a retried item does
//! not re-execute completed stages.
//!
//! Demonstrated:
//!
//!   - Per-stage memoization: one item's classify stage fails transiently on
//!     its first execution; the retry re-runs classify only, with extract
//!     served from the memo. Stage-execution counters printed at the end
//!     show this.
//!   - Transient versus permanent failures: the transient classify failure
//!     retries and succeeds; an empty document fails permanently at extract
//!     and is recorded as failed without retries.
//!   - Cost metering across retries: counters returned through
//!     `memoized_with_cached_cost` are recorded once per item regardless of
//!     retries.
//!
//! Run with: `cargo run -p taquba-bulk --example document_pipeline`

use std::collections::BTreeMap;
use std::io::{BufWriter, stdout};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use taquba::{OpenOptions, Queue, QueueConfig, object_store::memory::InMemory};
use taquba_bulk::{Bulk, BulkCtx, CostReport, JsonlSink, Pipeline, StepError};

#[derive(Serialize, Deserialize)]
struct Document {
    id: String,
    text: String,
}

#[derive(Serialize, Deserialize)]
struct Extracted {
    title: Option<String>,
    fields: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct ProcessedDocument {
    id: String,
    class: String,
    confidence: f64,
    passed: bool,
    warnings: Vec<String>,
}

/// Three-stage pipeline over the sample documents. The atomic counters record
/// stage executions for the summary output; a real pipeline does not need
/// them.
struct DocPipeline {
    extract_runs: Arc<AtomicUsize>,
    classify_runs: Arc<AtomicUsize>,
    validate_runs: Arc<AtomicUsize>,
    /// Set once the simulated classifier failure has been injected.
    failure_injected: Arc<AtomicBool>,
}

impl Pipeline for DocPipeline {
    type Input = Document;
    type Output = ProcessedDocument;
    type Error = StepError;

    async fn run(&self, ctx: &BulkCtx<Document>) -> Result<ProcessedDocument, StepError> {
        // Stage 1: parse `KEY: value` lines into structured fields. Cost
        // counters are returned through the memoized call so a memo hit on a
        // retried attempt still records them.
        let extracted: Extracted = ctx
            .memoized_with_cached_cost("extract:v1", async {
                self.extract_runs.fetch_add(1, Ordering::Relaxed);
                let extracted = extract(&ctx.input.text)?;
                let cost = CostReport::new();
                cost.record("chars_extracted", ctx.input.text.len() as f64);
                Ok::<_, StepError>((extracted, cost))
            })
            .await?;

        // Stage 2: classification represents an expensive external call. One
        // document's first execution fails transiently to exercise the retry
        // path; stage 1 is not recomputed on the retry.
        let (class, confidence) = ctx
            .memoized_with_cached_cost("classify:v1", async {
                self.classify_runs.fetch_add(1, Ordering::Relaxed);
                if ctx.input.id == "invoice-042"
                    && !self.failure_injected.swap(true, Ordering::Relaxed)
                {
                    return Err(StepError::transient("classifier rate-limited"));
                }
                let class = classify(&extracted);
                let cost = CostReport::new();
                cost.record("classify_calls", 1.0);
                Ok::<_, StepError>((class, cost))
            })
            .await?;

        // Stage 3: validation; a plain memoized call with no cost counters.
        let warnings: Vec<String> = ctx
            .memoized("validate:v1", async {
                self.validate_runs.fetch_add(1, Ordering::Relaxed);
                Ok::<_, StepError>(validate(&extracted, &class))
            })
            .await?;

        Ok(ProcessedDocument {
            id: ctx.input.id.clone(),
            class,
            confidence,
            passed: warnings.is_empty(),
            warnings,
        })
    }
}

fn extract(text: &str) -> Result<Extracted, StepError> {
    if text.trim().is_empty() {
        // An empty document is empty on every retry; fail permanently so the
        // item is recorded as failed without retries.
        return Err(StepError::permanent("document is empty"));
    }
    let mut title = None;
    let mut fields = BTreeMap::new();
    for line in text.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim().to_ascii_uppercase();
        let value = value.trim();
        if key.is_empty() || value.is_empty() {
            continue;
        }
        if key == "TITLE" {
            title = Some(value.to_string());
        } else {
            fields.insert(key, value.to_string());
        }
    }
    Ok(Extracted { title, fields })
}

fn classify(extracted: &Extracted) -> (String, f64) {
    if let Some(kind) = extracted.fields.get("TYPE") {
        return (kind.to_ascii_lowercase(), 0.95);
    }
    let search_text = extracted
        .fields
        .values()
        .chain(extracted.title.iter())
        .map(|s| s.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ");
    for (class, keyword) in [("invoice", "amount due"), ("claim", "incident")] {
        if search_text.contains(keyword) {
            return (class.to_string(), 0.7);
        }
    }
    ("unknown".to_string(), 0.0)
}

fn validate(extracted: &Extracted, class: &str) -> Vec<String> {
    let mut warnings = Vec::new();
    if extracted.title.is_none() {
        warnings.push("missing TITLE field".to_string());
    }
    let required: &[&str] = match class {
        "invoice" => &["TOTAL", "DATE"],
        "claim" => &["CLAIM_ID"],
        _ => &[],
    };
    for field in required {
        if !extracted.fields.contains_key(*field) {
            warnings.push(format!("missing {field} field"));
        }
    }
    warnings
}

fn sample_documents() -> Vec<Document> {
    let doc = |id: &str, text: &str| Document {
        id: id.to_string(),
        text: text.to_string(),
    };
    vec![
        doc(
            "invoice-041",
            "TITLE: April invoice\nTYPE: invoice\nTOTAL: 120.00\nDATE: 2026-04-30\n",
        ),
        // The first classify execution for this document fails transiently.
        doc(
            "invoice-042",
            "TITLE: May invoice\nTYPE: invoice\nTOTAL: 80.00\nDATE: 2026-05-31\n",
        ),
        doc(
            "claim-007",
            "TITLE: Water damage\nTYPE: claim\nCLAIM_ID: C-7\n",
        ),
        // No TYPE field: classified by keyword scan, with validation warnings.
        doc("memo-001", "TITLE: Reminder\nNOTE: amount due next week\n"),
        // Empty: fails permanently at the extract stage.
        doc("blank-001", "   \n"),
    ]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A short retry backoff so the injected transient failure is retried
    // promptly; production deployments keep the default backoff.
    let mut opts = OpenOptions::default();
    opts.queue_configs.insert(
        "docs".to_string(),
        QueueConfig {
            retry_backoff_base: Duration::from_millis(50),
            retry_backoff_max: Duration::from_millis(50),
            ..QueueConfig::default()
        },
    );
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open_with_options(store.clone(), "db", opts).await?);

    let extract_runs = Arc::new(AtomicUsize::new(0));
    let classify_runs = Arc::new(AtomicUsize::new(0));
    let validate_runs = Arc::new(AtomicUsize::new(0));
    let pipeline = DocPipeline {
        extract_runs: extract_runs.clone(),
        classify_runs: classify_runs.clone(),
        validate_runs: validate_runs.clone(),
        failure_injected: Arc::new(AtomicBool::new(false)),
    };

    let sink = Arc::new(JsonlSink::new(BufWriter::new(stdout())));
    let bulk = Bulk::builder(queue, store, pipeline)
        .output(sink)
        .key_fn(|doc: &Document| doc.id.clone())
        .queue_name("docs")
        .build();

    let report = bulk.run(sample_documents()).await?;

    eprintln!(
        "\n{}/{} succeeded, {} failed in {:?}",
        report.succeeded, report.total, report.failed, report.elapsed,
    );
    for (metric, total) in report.cost.entries() {
        eprintln!("cost: {metric} = {total}");
    }
    eprintln!(
        "stage executions: extract={} classify={} validate={}",
        extract_runs.load(Ordering::Relaxed),
        classify_runs.load(Ordering::Relaxed),
        validate_runs.load(Ordering::Relaxed),
    );
    eprintln!(
        "(5 documents: blank-001 failed permanently at extract; invoice-042 failed \
         transiently in classify once and retried without re-running extract, so \
         classify shows one more execution than validate)"
    );
    Ok(())
}

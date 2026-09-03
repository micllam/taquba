//! A document pipeline in three stages: extract fields, classify, validate.
//!
//! Each document is one job of a group, and the stages are memoized
//! calls inside the job, so a retried document does not re-execute its
//! completed stages.
//!
//! Demonstrated:
//!
//!   - Per-stage memoization: one document's classify stage fails
//!     transiently on its first execution; the retry re-runs classify
//!     only, with extract read from the memo. Stage-execution counters
//!     printed at the end show this.
//!   - Transient versus permanent failures: the transient classify
//!     failure retries and succeeds; an empty document fails permanently
//!     at extract and is recorded as failed without retries.
//!   - Counters across retries: the counters a stage returns with its
//!     memoized value are read back on a retry, so the caller's rollup
//!     over the results counts each stage once per document.
//!
//! Run with: `cargo run -p taquba-workflow --example group_document_pipeline`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use taquba::{OpenOptions, Queue, QueueConfig, object_store::memory::InMemory};
use taquba_workflow::StepErrorKind;
use taquba_workflow::jobs::{Job, JobContext, JobRunner};

#[derive(Serialize, Deserialize)]
struct ProcessDocument {
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
    class: String,
    confidence: f64,
    passed: bool,
    warnings: Vec<String>,
    /// Characters extracted and classifier calls made, counted once per
    /// document across retries.
    chars_extracted: usize,
    classify_calls: usize,
}

#[derive(Debug, thiserror::Error)]
enum DocError {
    #[error("{0}")]
    Transient(String),
    #[error("{0}")]
    Permanent(String),
    #[error(transparent)]
    Runtime(#[from] taquba_workflow::Error),
}

/// Stage-execution counters and the injected-failure flag, registered
/// as handler state; a real pipeline does not need them.
#[derive(Default)]
struct Stages {
    extract_runs: AtomicUsize,
    classify_runs: AtomicUsize,
    validate_runs: AtomicUsize,
    failure_injected: AtomicBool,
}

impl Job for ProcessDocument {
    const NAME: &'static str = "example.process_document";
    type Output = ProcessedDocument;
    type Error = DocError;

    async fn run(&self, ctx: JobContext<'_>) -> Result<ProcessedDocument, DocError> {
        let stages = ctx.state::<Arc<Stages>>();

        // Stage 1: parse `KEY: value` lines into structured fields. The
        // counter is memoized with the value, so a retry reads it back.
        let (extracted, chars_extracted): (Extracted, usize) = ctx
            .memo
            .memoized("extract:v1", async {
                stages.extract_runs.fetch_add(1, Ordering::Relaxed);
                Ok::<_, DocError>((extract(&self.text)?, self.text.len()))
            })
            .await?;

        // Stage 2: classification represents an expensive external call.
        // One document's first execution fails transiently to exercise
        // the retry path; stage 1 is not recomputed on the retry.
        let (class, confidence, classify_calls): (String, f64, usize) = ctx
            .memo
            .memoized("classify:v1", async {
                stages.classify_runs.fetch_add(1, Ordering::Relaxed);
                if self.id == "invoice-042"
                    && !stages.failure_injected.swap(true, Ordering::Relaxed)
                {
                    return Err(DocError::Transient("classifier rate-limited".into()));
                }
                let (class, confidence) = classify(&extracted);
                Ok((class, confidence, 1))
            })
            .await?;

        // Stage 3: validation.
        let warnings: Vec<String> = ctx
            .memo
            .memoized("validate:v1", async {
                stages.validate_runs.fetch_add(1, Ordering::Relaxed);
                Ok::<_, DocError>(validate(&extracted, &class))
            })
            .await?;

        Ok(ProcessedDocument {
            class,
            confidence,
            passed: warnings.is_empty(),
            warnings,
            chars_extracted,
            classify_calls,
        })
    }

    fn idempotency_key(&self) -> Option<String> {
        Some(self.id.clone())
    }

    fn classify(&self, error: &DocError) -> StepErrorKind {
        match error {
            DocError::Permanent(_) => StepErrorKind::Permanent,
            DocError::Transient(_) | DocError::Runtime(_) => StepErrorKind::Transient,
        }
    }
}

fn extract(text: &str) -> Result<Extracted, DocError> {
    if text.trim().is_empty() {
        // An empty document is empty on every retry; fail permanently so
        // the document is recorded as failed without retries.
        return Err(DocError::Permanent("document is empty".into()));
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

fn sample_documents() -> Vec<ProcessDocument> {
    let doc = |id: &str, text: &str| ProcessDocument {
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
        QueueConfig::default()
            .retry_backoff_base(Duration::from_millis(50))
            .retry_backoff_max(Duration::from_millis(50)),
    );
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open_with_options(store.clone(), "db", opts).await?);

    let stages = Arc::new(Stages::default());
    let mut runner = JobRunner::builder(queue, store)
        .queue_name("docs")
        .register::<ProcessDocument>()
        .state(stages.clone())
        .build();
    let worker = runner.spawn(std::future::pending::<()>());

    let group = runner.group::<ProcessDocument>("documents")?;
    group.submit(sample_documents()).await?;
    let results = group.join().await?;
    worker.shutdown().await?;

    let (mut succeeded, mut failed, mut chars, mut calls) = (0, 0, 0, 0);
    for result in &results {
        match &result.result {
            Ok(doc) => {
                println!(
                    "{}: {} ({:.2}) passed={} warnings={:?}",
                    result.key, doc.class, doc.confidence, doc.passed, doc.warnings
                );
                succeeded += 1;
                chars += doc.chars_extracted;
                calls += doc.classify_calls;
            }
            Err(err) => {
                println!("{}: failed ({:?}): {}", result.key, err.kind, err.message);
                failed += 1;
            }
        }
    }
    eprintln!("\n{succeeded}/{} succeeded, {failed} failed", results.len());
    eprintln!("chars_extracted={chars} classify_calls={calls}");
    eprintln!(
        "stage executions: extract={} classify={} validate={}",
        stages.extract_runs.load(Ordering::Relaxed),
        stages.classify_runs.load(Ordering::Relaxed),
        stages.validate_runs.load(Ordering::Relaxed),
    );
    eprintln!(
        "(5 documents: blank-001 failed permanently at extract; invoice-042 failed \
         transiently in classify once and retried without re-running extract, so \
         classify shows one more execution than validate)"
    );
    Ok(())
}

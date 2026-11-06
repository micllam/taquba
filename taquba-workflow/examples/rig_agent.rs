//! Two-stage agent run powered by the [Rig](https://crates.io/crates/rig) crate:
//!
//! - **Step 0 (`research`)**: an agent equipped with a mocked
//!   `lookup_fact` tool gathers information about the submitted topic.
//! - **Step 1 (`write`)**: a second agent (different preamble, no tools)
//!   synthesizes the findings into a polished paragraph and returns it as
//!   the run's final result.
//!
//! Each step is one full Rig `agent.prompt()` call. taquba-workflow's value
//! here is *between-step durability*: if the worker crashes after step 0 and
//! before step 1, the research is not lost: the next process resumes at step 1
//! from queue state.
//!
//! The submitted run includes the topic in the `topic` user header. The payload
//! is used only for between-step state (the research findings as UTF-8 bytes
//! after step 0).
//!
//! The example runs `claude-haiku-5-5` when `ANTHROPIC_API_KEY` is set, and
//! `gpt-6-luna` otherwise:
//!
//! ```text
//! ANTHROPIC_API_KEY=... cargo run -p taquba-workflow --example rig_agent
//! OPENAI_API_KEY=...    cargo run -p taquba-workflow --example rig_agent
//! ```
//!
//! A provider error that Rig classifies as retryable fails the step as
//! transient, and the queue retries it. Any other error, such as a rejected
//! key, dead-letters the step.

use std::sync::Arc;

use anyhow::{Context, Result};
use rig_agent::prelude::*;
use rig_agent::tool::ToolContext;
use rig_core::catalog::Catalog;
use rig_core::operation::Completion;
use serde::{Deserialize, Serialize};
use serde_json::json;
use taquba::Queue;
use taquba::object_store::memory::InMemory;
use taquba_workflow::{
    RunOptions, RunSpec, Step, StepError, StepOutcome, StepRunner, TerminalStatus, WorkflowRuntime,
};

/// Mocked "fact lookup" tool. It returns hardcoded strings where a real version
/// queries a search API or a vector store.
#[derive(Deserialize, Serialize)]
struct LookupFact;

#[derive(Deserialize)]
struct LookupArgs {
    query: String,
}

#[derive(Debug, thiserror::Error)]
#[error("lookup error: {0}")]
struct LookupError(String);

impl Tool for LookupFact {
    const NAME: &'static str = "lookup_fact";
    type Error = LookupError;
    type Args = LookupArgs;
    type Output = String;

    fn description(&self) -> String {
        "Look up a short factual statement about a query. Use for any topic \
         the user asks about."
            .to_string()
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The thing to look up a fact about."
                }
            },
            "required": ["query"]
        })
    }

    async fn call(
        &self,
        _context: &mut ToolContext,
        args: Self::Args,
    ) -> Result<Self::Output, Self::Error> {
        // Stub. A real implementation calls a search or RAG backend.
        Ok(format!(
            "Stub fact about '{}': it is widely studied and has many surprising \
             properties that experts continue to investigate.",
            args.query
        ))
    }
}

const STEP_RESEARCH: u32 = 0;
const STEP_WRITE: u32 = 1;

const ANTHROPIC_MODEL: &str = "anthropic/claude-haiku-5-5";
const OPENAI_MODEL: &str = "openai/gpt-6-luna";

// A reasoning model counts its hidden reasoning tokens against `max_tokens`,
// and a small limit can leave the visible reply empty.
const MAX_TOKENS: u64 = 4096;

const RESEARCH_PREAMBLE: &str = "You are a researcher. Use the `lookup_fact` tool to gather information \
     about the topic, then summarize what you found in 2-3 sentences. Do not \
     invent facts; only report what the tool returns.";

const WRITE_PREAMBLE: &str = "You are a technical writer. Given a topic and research findings, produce \
     a polished one-paragraph summary aimed at a curious general audience. \
     Do not add facts beyond what is in the findings.";

/// The agents of the two steps, built once on the selected model.
struct RigRunner {
    researcher: Agent,
    writer: Agent,
}

impl RigRunner {
    fn new(model: DynModel<Completion>) -> Self {
        let researcher = AgentBuilder::new(model.clone())
            .preamble(RESEARCH_PREAMBLE)
            .tool(LookupFact)
            .max_tokens(MAX_TOKENS)
            .build();
        let writer = AgentBuilder::new(model)
            .preamble(WRITE_PREAMBLE)
            .max_tokens(MAX_TOKENS)
            .build();
        Self { researcher, writer }
    }
}

impl StepRunner for RigRunner {
    async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
        let topic = step
            .headers
            .get("topic")
            .ok_or_else(|| StepError::permanent("missing `topic` header"))?
            .clone();

        match step.step_number {
            STEP_RESEARCH => {
                let findings = self
                    .researcher
                    .prompt(topic.as_str())
                    .max_turns(5)
                    .await
                    .map_err(|e| step_error("research", e))?
                    .output();
                if findings.trim().is_empty() {
                    return Err(StepError::transient(
                        "research step returned empty findings (model may have \
                         used its whole `max_tokens` limit on hidden reasoning); \
                         retrying",
                    ));
                }
                println!("[step 0] research findings: {findings}");
                Ok(StepOutcome::continue_now(findings.into_bytes()))
            }
            STEP_WRITE => {
                let findings = std::str::from_utf8(&step.payload)
                    .map_err(|e| StepError::permanent(format!("non-utf8 findings payload: {e}")))?;
                let summary = self
                    .writer
                    .prompt(format!("Topic: {topic}\n\nFindings: {findings}"))
                    .await
                    .map_err(|e| step_error("write", e))?
                    .output();
                println!("[step 1] final summary: {summary}");
                Ok(StepOutcome::Succeed {
                    result: summary.into_bytes(),
                })
            }
            other => Err(StepError::permanent(format!(
                "unexpected step number {other}"
            ))),
        }
    }
}

/// The step error of a failed prompt of `stage`: transient when Rig classifies
/// the failure as retryable, permanent otherwise.
fn step_error(stage: &str, err: PromptError) -> StepError {
    let retryable = match &err {
        PromptError::Provider(error) => error.is_retryable(),
        PromptError::Report(report) => report.is_retryable(),
        PromptError::MaxTurns { .. }
        | PromptError::Cancelled { .. }
        | PromptError::UnknownToolCall { .. } => false,
        _ => true,
    };
    let message = format!("{stage} call failed: {err}");
    if retryable {
        StepError::transient(message)
    } else {
        StepError::permanent(message)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let reference = if std::env::var_os("ANTHROPIC_API_KEY").is_some() {
        ANTHROPIC_MODEL
    } else {
        OPENAI_MODEL
    };
    let model = Catalog::builtin()
        .connect(reference)
        .with_context(|| format!("cannot connect the model `{reference}`"))?;
    println!("using model: {reference}");

    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "rig-demo").await?);

    let runtime = WorkflowRuntime::builder(queue, store, RigRunner::new(model))
        .max_concurrent_steps(2)
        .build()?;
    let worker = runtime.spawn(std::future::pending());

    let mut headers = std::collections::HashMap::new();
    headers.insert(
        "topic".to_string(),
        "the migratory patterns of arctic terns".to_string(),
    );
    let handle = runtime
        .submit(RunSpec {
            input: Vec::new(),
            options: RunOptions {
                headers,
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    println!("submitted run {}", handle.run_id);

    let run = runtime.wait(&handle.run_id).await?;
    let termination = &run.termination;
    println!(
        "\n=== run {} {} (final_step={}) ===",
        run.run_id, termination.status, termination.final_step.number
    );
    if termination.status == TerminalStatus::Failed
        && let Some(err) = &termination.error
    {
        eprintln!("error: {err}");
    }

    worker.shutdown().await?;
    Ok(())
}

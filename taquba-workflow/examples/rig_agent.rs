//! Two-stage agent run powered by the [Rig](https://crates.io/crates/rig-core) crate:
//!
//! - **Step 0 (`research`)**: an agent equipped with a mocked
//!   `lookup_fact` tool gathers information about the submitted topic.
//! - **Step 1 (`write`)**: a second agent (different preamble, no tools)
//!   synthesizes the findings into a polished paragraph and returns it as
//!   the run's final result.
//!
//! Each step is one full Rig `agent.prompt()` call. taquba-workflow's value
//! here is *between-step durability*: if the worker crashes after step 0
//! and before step 1, the research isn't lost: the next process resumes
//! at step 1 from queue state.
//!
//! The submitted run carries the topic on the `topic` user header. The
//! payload is used only for between-step state (the research findings as
//! UTF-8 bytes after step 0).
//!
//! Pinned to `rig-core = "0.36"`. Picks the LLM provider from the
//! environment:
//!
//! - `LLM_PROVIDER=anthropic` (default if `ANTHROPIC_API_KEY` is set):
//!   uses `claude-haiku-4-5`.
//! - `LLM_PROVIDER=openai` (default if only `OPENAI_API_KEY` is set):
//!   uses `gpt-5-nano`.
//!
//! Run with either:
//!
//! ```text
//! ANTHROPIC_API_KEY=... cargo run -p taquba-workflow --example rig_agent
//! OPENAI_API_KEY=...    cargo run -p taquba-workflow --example rig_agent
//! LLM_PROVIDER=openai OPENAI_API_KEY=... cargo run -p taquba-workflow --example rig_agent
//! ```

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use rig::client::{CompletionClient, ProviderClient};
use rig::completion::{Prompt, ToolDefinition};
use rig::providers::{anthropic, openai};
use rig::tool::Tool;
use serde::{Deserialize, Serialize};
use serde_json::json;
use taquba::Queue;
use taquba::object_store::memory::InMemory;
use taquba_workflow::{
    RunOutcome, RunSpec, Step, StepError, StepOutcome, StepRunner, TerminalHook, TerminalStatus,
    WorkflowRuntime,
};
use tokio::sync::oneshot;

/// Mocked "fact lookup" tool. A real version would hit a search API or
/// vector store; for the example it returns hardcoded strings.
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

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Look up a short factual statement about a query. Use \
                          for any topic the user asks about."
                .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The thing to look up a fact about."
                    }
                },
                "required": ["query"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        // Stub. Real impls would call out to a search/RAG backend.
        Ok(format!(
            "Stub fact about '{}': it is widely studied and has many surprising \
             properties that experts continue to investigate.",
            args.query
        ))
    }
}

const STEP_RESEARCH: u32 = 0;
const STEP_WRITE: u32 = 1;

// GPT-5-class reasoning models count *hidden* reasoning tokens against the
// same budget as visible output, so a small `max_tokens` (e.g. 512) can be
// fully consumed by reasoning before any text is emitted, leaving the
// visible message empty. 4096 leaves comfortable headroom for both that
// case and Claude Haiku.
const MAX_TOKENS: u64 = 4096;

const RESEARCH_PREAMBLE: &str = "You are a researcher. Use the `lookup_fact` tool to gather information \
     about the topic, then summarize what you found in 2-3 sentences. Do not \
     invent facts; only report what the tool returns.";

const WRITE_PREAMBLE: &str = "You are a technical writer. Given a topic and research findings, produce \
     a polished one-paragraph summary aimed at a curious general audience. \
     Do not add facts beyond what is in the findings.";

/// Pre-built clients for both supported providers. Each `run_step` picks one
/// based on the `provider` field. Building agents per step (rather than
/// reusing a built agent) is cheap: agents are config wrappers around the
/// shared client.
enum Provider {
    Anthropic(anthropic::Client),
    Openai(openai::Client),
}

impl Provider {
    fn label(&self) -> &'static str {
        match self {
            Provider::Anthropic(_) => "anthropic/claude-haiku-4-5",
            Provider::Openai(_) => "openai/gpt-5-nano",
        }
    }
}

struct RigRunner {
    provider: Provider,
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
                let findings = research(&self.provider, &topic).await?;
                if findings.trim().is_empty() {
                    return Err(StepError::transient(
                        "research step returned empty findings (model may have \
                         exhausted its token budget on hidden reasoning); retrying",
                    ));
                }
                println!("[step 0] research findings: {findings}");
                Ok(StepOutcome::Continue {
                    payload: findings.into_bytes(),
                })
            }
            STEP_WRITE => {
                let findings = std::str::from_utf8(&step.payload)
                    .map_err(|e| StepError::permanent(format!("non-utf8 findings payload: {e}")))?;
                let summary = write(&self.provider, &topic, findings).await?;
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

async fn research(provider: &Provider, topic: &str) -> std::result::Result<String, StepError> {
    let map_err = |e: anyhow::Error| StepError::transient(format!("research call failed: {e}"));
    match provider {
        Provider::Anthropic(c) => {
            let agent = c
                .agent(anthropic::completion::CLAUDE_HAIKU_4_5)
                .preamble(RESEARCH_PREAMBLE)
                .tool(LookupFact)
                .max_tokens(MAX_TOKENS)
                .build();
            agent
                .prompt(topic)
                .max_turns(5)
                .await
                .map_err(|e| map_err(e.into()))
        }
        Provider::Openai(c) => {
            let agent = c
                .agent(openai::completion::GPT_5_NANO)
                .preamble(RESEARCH_PREAMBLE)
                .tool(LookupFact)
                .max_tokens(MAX_TOKENS)
                .build();
            agent
                .prompt(topic)
                .max_turns(5)
                .await
                .map_err(|e| map_err(e.into()))
        }
    }
}

async fn write(
    provider: &Provider,
    topic: &str,
    findings: &str,
) -> std::result::Result<String, StepError> {
    let prompt = format!("Topic: {topic}\n\nFindings: {findings}");
    let map_err = |e: anyhow::Error| StepError::transient(format!("write call failed: {e}"));
    match provider {
        Provider::Anthropic(c) => {
            let agent = c
                .agent(anthropic::completion::CLAUDE_HAIKU_4_5)
                .preamble(WRITE_PREAMBLE)
                .max_tokens(MAX_TOKENS)
                .build();
            agent
                .prompt(prompt.as_str())
                .await
                .map_err(|e| map_err(e.into()))
        }
        Provider::Openai(c) => {
            let agent = c
                .agent(openai::completion::GPT_5_NANO)
                .preamble(WRITE_PREAMBLE)
                .max_tokens(MAX_TOKENS)
                .build();
            agent
                .prompt(prompt.as_str())
                .await
                .map_err(|e| map_err(e.into()))
        }
    }
}

/// Decide which provider to use:
///   1. If `LLM_PROVIDER` is set, honour it (and require the matching key).
///   2. Otherwise, prefer `ANTHROPIC_API_KEY`, then `OPENAI_API_KEY`.
fn select_provider() -> Result<Provider> {
    let explicit = std::env::var("LLM_PROVIDER").ok();
    let has_anthropic = std::env::var_os("ANTHROPIC_API_KEY").is_some();
    let has_openai = std::env::var_os("OPENAI_API_KEY").is_some();

    let choice = match explicit.as_deref() {
        Some("anthropic") => "anthropic",
        Some("openai") => "openai",
        Some(other) => {
            bail!("LLM_PROVIDER=`{other}` is not recognized; use `anthropic` or `openai`")
        }
        None if has_anthropic => "anthropic",
        None if has_openai => "openai",
        None => bail!(
            "set ANTHROPIC_API_KEY or OPENAI_API_KEY (and optionally LLM_PROVIDER) to run \
             this example"
        ),
    };

    match choice {
        "anthropic" => {
            let client =
                anthropic::Client::from_env().context("ANTHROPIC_API_KEY missing or invalid")?;
            Ok(Provider::Anthropic(client))
        }
        "openai" => {
            let client = openai::Client::from_env().context("OPENAI_API_KEY missing or invalid")?;
            Ok(Provider::Openai(client))
        }
        _ => unreachable!(),
    }
}

struct ShutdownOnComplete {
    shutdown: tokio::sync::Mutex<Option<oneshot::Sender<()>>>,
}

impl TerminalHook for ShutdownOnComplete {
    async fn on_termination(&self, outcome: &RunOutcome) {
        println!(
            "\n=== run {} {} (final_step={}) ===",
            outcome.run_id, outcome.status, outcome.final_step
        );
        if outcome.status == TerminalStatus::Failed {
            if let Some(err) = &outcome.error {
                eprintln!("error: {err}");
            }
        }
        if let Some(tx) = self.shutdown.lock().await.take() {
            let _ = tx.send(());
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let provider = select_provider()?;
    println!("using provider: {}", provider.label());

    let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "rig-demo").await?);

    let (tx, rx) = oneshot::channel::<()>();

    let runtime = WorkflowRuntime::builder(
        queue,
        RigRunner { provider },
        ShutdownOnComplete {
            shutdown: tokio::sync::Mutex::new(Some(tx)),
        },
    )
    .max_concurrent_steps(2)
    .build();

    let worker_runtime = runtime.clone();
    let worker_task = tokio::spawn(async move {
        worker_runtime
            .run(async move {
                let _ = rx.await;
            })
            .await
    });

    let mut headers = std::collections::HashMap::new();
    headers.insert(
        "topic".to_string(),
        "the migratory patterns of arctic terns".to_string(),
    );
    let handle = runtime
        .submit(RunSpec {
            input: Vec::new(),
            headers,
            ..Default::default()
        })
        .await?;
    println!("submitted run {}", handle.run_id);

    worker_task.await??;
    Ok(())
}

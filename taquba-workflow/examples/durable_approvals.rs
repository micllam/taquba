//! An agent loop with a durable approval gate, resumed across processes.
//!
//! An agent investigates a refund claim one turn per step, storing its state in
//! the step payload, and the loop decides how many steps the run takes. When
//! the agent proposes a refund it returns `StepOutcome::continue_on_signal` and
//! this process exits: the waiting run is a scheduled job in the store, so no
//! process runs while the approval is pending. The decision arrives as a new
//! invocation of this binary, which reopens the store, reads the job id of the
//! waiting step from `RunStatus::step`, delivers the signal to it and resumes
//! the run to completion.
//!
//! ```text
//! cargo run -p taquba-workflow --example durable_approvals               # run until waiting
//! cargo run -p taquba-workflow --example durable_approvals -- approve
//! cargo run -p taquba-workflow --example durable_approvals -- reject over-budget
//! ```
//!
//! The two delivery paths:
//!
//! - A decision delivered while the run waits wakes the waiting step
//!   (`SignalOutcome::Delivered`) and the run completes in the
//!   delivering process.
//! - Without a decision inside the timeout (five minutes), the next plain
//!   invocation promotes the waiting step with `Step::signal == None`
//!   and the run escalates.
//!
//! The timeout runs on wall clock through downtime. A run that waits longer
//! than the timeout escalates at the next open, and a decision for a run that
//! does not wait is rejected without a change.
//!
//! State lives under `/tmp/taquba-durable-approvals-example`. Remove the
//! directory to discard the run and start over.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use taquba::object_store::local::LocalFileSystem;
use taquba::{Queue, SignalOutcome};
use taquba_workflow::{
    RunId, RunSpec, RunState, Step, StepError, StepOutcome, StepRunner, TerminalStatus,
    TerminatedRun, WorkflowRuntime,
};

const QUEUE_DIR: &str = "/tmp/taquba-durable-approvals-example";
const RUN_ID: &str = "refund-claim-4021";
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(300);
const EVIDENCE_TURNS: usize = 3;

/// The agent's working state, stored in the step payload. Each step is one
/// turn, and the state decides what the turn does.
#[derive(Serialize, Deserialize, Default)]
struct AgentState {
    evidence: Vec<String>,
    proposal: Option<u32>,
}

fn encode(state: &AgentState) -> Result<Vec<u8>, StepError> {
    serde_json::to_vec(state).map_err(|e| StepError::permanent(format!("state: {e}")))
}

/// One investigation turn. The stub returns a fixed finding per turn where a
/// real agent calls a model or a search tool.
fn lookup_evidence(turn: usize) -> String {
    const FINDINGS: [&str; EVIDENCE_TURNS] = [
        "order 4021 was delivered 11 days late",
        "the customer was charged twice for shipping",
        "the second shipping charge was never reversed",
    ];
    FINDINGS[turn].to_string()
}

struct RefundAgent;

impl StepRunner for RefundAgent {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        let mut state: AgentState = if step.payload.is_empty() {
            AgentState::default()
        } else {
            serde_json::from_slice(&step.payload)
                .map_err(|e| StepError::permanent(format!("state: {e}")))?
        };

        // Decision turn: the run was woken by a signal or by its timeout.
        if let Some(amount) = state.proposal {
            let verdict = match step.signal.as_deref() {
                Some(b"approve") => {
                    issue_refund(step, amount).await?;
                    format!("refund of {amount} EUR issued")
                }
                Some(reason) => format!("refund denied: {}", String::from_utf8_lossy(reason)),
                None => "escalated: no decision inside the timeout".to_string(),
            };
            println!("[decision] {verdict}");
            return Ok(StepOutcome::Succeed {
                result: verdict.into_bytes(),
            });
        }

        // Investigation turn.
        if state.evidence.len() < EVIDENCE_TURNS {
            let finding = lookup_evidence(state.evidence.len());
            println!("[turn {}] {finding}", step.step_number);
            state.evidence.push(finding);
            if state.evidence.len() < EVIDENCE_TURNS {
                return Ok(StepOutcome::continue_now(encode(&state)?));
            }
        }

        // The evidence is complete: propose a refund and hold the run until a
        // decision arrives, because issuing the refund is irreversible.
        let amount = 25 * state.evidence.len() as u32;
        state.proposal = Some(amount);
        println!(
            "[turn {}] proposing a refund of {amount} EUR; awaiting approval",
            step.step_number
        );
        Ok(StepOutcome::continue_on_signal(
            encode(&state)?,
            APPROVAL_TIMEOUT,
        ))
    }
}

/// The irreversible effect, guarded by a memo key: a redelivered decision step
/// observes the recorded refund and does not issue it again. The guard is
/// written after the effect, so a crash between the two can still replay the
/// effect once.
async fn issue_refund(step: &Step, amount: u32) -> Result<(), StepError> {
    const KEY: &str = "refund-issued";
    if step.memo.get(KEY).await?.is_some() {
        println!("[decision] refund already recorded by an earlier attempt");
        return Ok(());
    }
    println!("[decision] issuing refund of {amount} EUR");
    step.memo.put(KEY, amount.to_string().as_bytes()).await?;
    Ok(())
}

/// Print how the run `run` ended.
fn report(run: &TerminatedRun) {
    let termination = &run.termination;
    println!();
    match termination.status {
        TerminalStatus::Succeeded => println!(
            "run {} succeeded: {}",
            run.run_id,
            String::from_utf8_lossy(run.result.as_deref().unwrap_or(&[]))
        ),
        TerminalStatus::Failed | TerminalStatus::Cancelled => println!(
            "run {} terminated as {}: {}",
            run.run_id,
            termination.status,
            termination.error.as_deref().unwrap_or("(no error)")
        ),
    }
}

enum Mode {
    Run,
    Decide(Vec<u8>),
}

fn parse_mode() -> Result<Mode, String> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None => Ok(Mode::Run),
        Some("approve") => Ok(Mode::Decide(b"approve".to_vec())),
        Some("reject") => {
            let reason = args.next().unwrap_or_else(|| "no reason given".to_string());
            Ok(Mode::Decide(reason.into_bytes()))
        }
        Some(other) => Err(format!(
            "unknown mode `{other}`; use `approve` or `reject <reason>`"
        )),
    }
}

/// Resolves once the store contains only the waiting decision step. The initial
/// delay lets the scheduler promote a wait whose timeout elapsed before this
/// process started, so an overdue run escalates.
async fn awaiting_approval(queue: Arc<Queue>) {
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut stable = 0;
    loop {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let Ok(stats) = queue.view().stats("workflow-steps").await else {
            continue;
        };
        if stats.scheduled >= 1 && stats.pending == 0 && stats.claimed == 0 {
            stable += 1;
            if stable >= 2 {
                return;
            }
        } else {
            stable = 0;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = parse_mode()?;

    // The directory must exist before LocalFileSystem can canonicalize it.
    std::fs::create_dir_all(QUEUE_DIR)?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(QUEUE_DIR)?);
    let queue = Arc::new(Queue::open(store.clone(), "workflow").await?);

    let runtime = WorkflowRuntime::builder(queue.clone(), store, RefundAgent)
        .poll_interval(Duration::from_millis(200))
        .build()?;
    let run_id = RunId::new(RUN_ID)?;

    match mode {
        Mode::Decide(payload) => {
            let status = runtime.status(&run_id).await?;
            let Some(status) =
                status.filter(|status| matches!(status.state, RunState::Waiting { .. }))
            else {
                println!("run {RUN_ID} does not wait for a decision; nothing was changed");
                return Ok(());
            };
            match queue.signal(&status.step.job_id, payload).await? {
                SignalOutcome::Delivered => {
                    println!("decision delivered; resuming the run");
                    let worker = runtime.spawn(std::future::pending());
                    report(&runtime.wait(&run_id).await?);
                    worker.shutdown().await?;
                }
                SignalOutcome::NotWaiting => {
                    println!("the wait ended before the decision; nothing was changed");
                }
            }
        }
        Mode::Run => {
            let outcome = runtime
                .submit(RunSpec {
                    run_id: Some(run_id.clone()),
                    input: Vec::new(),
                    ..Default::default()
                })
                .await?;
            if !outcome.newly_submitted
                && let Some(run) = runtime.view().terminated_run(&outcome.run_id).await?
            {
                println!(
                    "run {} already ended as {}; remove {QUEUE_DIR} to start over",
                    outcome.run_id, run.termination.status
                );
                return Ok(());
            }
            if outcome.newly_submitted {
                println!("submitted run {}", outcome.run_id);
            } else {
                println!("run {} already exists; resuming it", outcome.run_id);
            }

            let worker = runtime.spawn(std::future::pending());
            tokio::select! {
                run = runtime.wait(&run_id) => report(&run?),
                _ = awaiting_approval(queue.clone()) => {
                            println!();
                            println!("run is waiting for approval; this process exits");
                            println!(
                                "deliver the decision within {}s with one of:",
                                APPROVAL_TIMEOUT.as_secs()
                            );
                            println!(
                                "  cargo run -p taquba-workflow --example durable_approvals -- approve"
                            );
                            println!(
                                "  cargo run -p taquba-workflow --example durable_approvals -- reject <reason>"
                            );
                }
            }
            worker.shutdown().await?;
        }
    }

    println!("remove {QUEUE_DIR} to discard the run and start over");
    Ok(())
}

# taquba-workflow

Durable, at-least-once workflow runtime on top of the
[Taquba](../taquba) durable task queue.

`taquba-workflow` is the plumbing for any multi-step process that
benefits from durable state between steps: idempotent step execution,
retries with backoff, graceful restart, and terminal-state notifications.
Implement `StepRunner` with bytes-in / bytes-out per-step logic; the
runtime persists everything else.

Particularly well-suited for **AI agent runs** (see
[`examples/rig_agent.rs`](examples/rig_agent.rs) for a
[Rig](https://github.com/0xPlaygrounds/rig) integration), but the runtime
itself is framework-neutral and equally usable for ETL pipelines, document
processing, payment flows, etc.

## What this is / isn't

`taquba-workflow` is an **imperative step orchestrator**: at each step
the runner decides what happens next via `StepOutcome` (Continue,
Succeed, Fail, Cancel). External cancellation is supported via
`WorkflowRuntime::cancel`. It is *not*:

- **A DAG executor**. There's no declarative graph, no fan-out / fan-in, no
  dependency-driven scheduling.
- **An event-sourced workflow engine**. There's no event-history replay, no
  per-side-effect recording.

## Install

```bash
cargo add taquba-workflow taquba
cargo add tokio --features full
```

Enable the `webhooks` feature for `WebhookTerminalHook`:

```bash
cargo add taquba-workflow --features webhooks
```

## Quick start

```rust
use std::sync::Arc;
use taquba::{Queue, object_store::memory::InMemory};
use taquba_workflow::{
    NoopTerminalHook, RunSpec, Step, StepError, StepOutcome, StepRunner, WorkflowRuntime,
};

struct EchoRunner;

impl StepRunner for EchoRunner {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        Ok(StepOutcome::Succeed { result: step.payload.clone() })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "demo").await?);

    let runtime = WorkflowRuntime::builder(queue, EchoRunner, NoopTerminalHook).build();

    let worker = runtime.clone();
    tokio::spawn(async move { worker.run(std::future::pending::<()>()).await });

    let handle = runtime.submit(RunSpec {
        input: b"hello".to_vec(),
        ..Default::default()
    }).await?;
    println!("submitted run {}", handle.run_id);
    Ok(())
}
```

## Examples

```bash
cargo run -p taquba-workflow --example single_step
cargo run -p taquba-workflow --example multi_step
ANTHROPIC_API_KEY=... cargo run -p taquba-workflow --example rig_agent
OPENAI_API_KEY=...    cargo run -p taquba-workflow --example rig_agent
```

`rig_agent` is a two-stage AI agent (research, then write) that
demonstrates between-step durability: kill the process after step 0 and
a fresh process resumes at step 1.

## Step outcomes

| Outcome | Effect |
|---|---|
| `StepOutcome::Continue { payload }` | Enqueue the next step immediately. |
| `StepOutcome::ContinueAfter { payload, delay }` | Schedule the next step `delay` from now. |
| `StepOutcome::Succeed { result }` | Ack; terminal hook fires `Succeeded`. |
| `StepOutcome::Fail { reason }` | Ack; terminal hook fires `Failed`. Runner verdict: no dead-letter. |
| `StepOutcome::Cancel { reason }` | Ack; terminal hook fires `Cancelled`. Runner verdict: no dead-letter. |
| `Err(StepError::transient(_))` | Retry per backoff up to `max_attempts`, then dead-letter. |
| `Err(StepError::permanent(_))` | Dead-letter immediately. |

`StepOutcome::Fail` / `StepOutcome::Cancel` vs `Err(StepError::permanent)`:
runner verdicts ack normally; an infrastructure error dead-letters so
operators can find it via `queue.dead_jobs()`.

## Cancellation

Call `WorkflowRuntime::cancel(run_id)` to cancel an active run from
outside the runner:

- If the current step is **pending or scheduled**, the queued step job is
  removed and the terminal hook fires from the `cancel` call before it
  returns.
- If the current step is **running**, cancellation is delivered via
  `Step::cancel_token` (a `tokio_util::sync::CancellationToken`).
  Runners that watch the token can short-circuit immediately:

  ```rust,ignore
  tokio::select! {
      out = call_llm(step) => out,
      _ = step.cancel_token.cancelled() => {
          Ok(StepOutcome::Cancel { reason: "cooperative".into() })
      }
  }
  ```

  Runners that ignore the token are allowed to run to completion (futures
  cannot be safely aborted mid-step). In both cases the runner's
  `StepOutcome` is discarded, any pending transient retry is suppressed,
  and the worker fires the terminal hook with `Cancelled` once the step
  returns. Watching the token only reduces cancellation latency for slow
  steps; it doesn't change semantics.

Returns `Ok(false)` if the run is unknown or already terminal in this
runtime. `cancel` only reaches runs submitted to this `WorkflowRuntime`
instance; a second runtime in the same process (sharing the queue)
maintains its own registry.

## Reserved headers

Step jobs reserve the `workflow.*` prefix; submission rejects user
headers starting with it. Other headers on `RunSpec::headers` thread
through every step and reach the terminal hook on `RunOutcome::headers`.

| Key | Meaning |
|---|---|
| `workflow.run_id` | Run identifier. |
| `workflow.step` | Zero-based step number. |

## Idempotency

Each step is enqueued with `dedup_key = "run:{run_id}:{step_number}"`,
preventing concurrent duplicate steps. But Taquba is at-least-once: a
step can be claimed and executed twice if its lease expires before ack.
**`StepRunner` impls must be idempotent for the same
`(run_id, step_number)`.**

## Terminal hook

`TerminalHook::on_termination` fires once per run on `Succeeded`,
`Failed`, or `Cancelled`, receiving the submitter's headers and the
runner's result or error. `WebhookTerminalHook` (behind the `webhooks`
feature) fires HTTP callbacks via `taquba-webhooks`; set the per-run URL
on `RunSpec::headers["callback_url"]`.

## License

Apache-2.0

# taquba-workflow

Durable, at-least-once workflow runtime on top of the
[Taquba](../taquba) durable task queue.

> Part of the [Taquba ecosystem](https://github.com/micllam/taquba); see the
> workspace README for the queue core and the other crates that compose with
> this one.

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

## Configuring the queue

Per-queue retention (`QueueConfig::keep_done_jobs` and
`QueueConfig::dead_retention`) is set on the `taquba::Queue` before it's
handed to the runtime. Pick an explicit name via
`WorkflowRuntimeBuilder::queue_name` and key `OpenOptions::queue_configs`
on the same string.

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use taquba::{OpenOptions, Queue, QueueConfig, object_store::memory::InMemory};
use taquba_workflow::{NoopTerminalHook, StepError, StepOutcome, StepRunner, WorkflowRuntime, Step};

struct EchoRunner;
impl StepRunner for EchoRunner {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        Ok(StepOutcome::Succeed { result: step.payload.clone() })
    }
}

let store = Arc::new(InMemory::new());
let opts = OpenOptions {
    queue_configs: HashMap::from([(
        "agent-runs".to_string(),
        QueueConfig {
            keep_done_jobs: Some(Duration::from_secs(24 * 60 * 60)),
            ..QueueConfig::default()
        },
    )]),
    ..OpenOptions::default()
};
let queue = Arc::new(Queue::open_with_options(store.clone(), "db", opts).await?);
let runtime = WorkflowRuntime::builder(queue, store, EchoRunner, NoopTerminalHook)
    .queue_name("agent-runs") // same string as in queue_configs
    .build();
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
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "demo").await?);

    let runtime = WorkflowRuntime::builder(queue, store, EchoRunner, NoopTerminalHook).build();

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

While termination is in flight, `WorkflowRuntime::status` reports a
`RunState::Cancelling` overlay until the entry is dropped.

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

## Memoizing within-step side effects

Because retries can re-execute a step, expensive non-idempotent side
effects (LLM calls, paid APIs, multi-stage processing) need a place to
record their result so retries observe the cached value instead of
paying twice. `Step::memo` is a per-step durable key-value store
scoped to `(run_id, step_number)`:

```rust,ignore
// Inside StepRunner::run_step:
if let Some(cached) = step.memo.get("draft").await? {
    return Ok(StepOutcome::Succeed { result: cached });
}
let draft = expensive_call(&step.payload).await?;
step.memo.put("draft", &draft).await?;
Ok(StepOutcome::Succeed { result: draft })
```

Memo entries live in the object store passed to
`WorkflowRuntime::builder` under the path prefix configured by
`WorkflowRuntimeBuilder::memo_prefix` (default `"workflow-memo"`).
`Memo` is strictly per-step; the durable channel between steps is
`StepOutcome::Continue`'s payload, not memo.

## Memo retention

By default memo entries are retained indefinitely (appropriate for
short-lived runs or workloads that manage cleanup externally). To
enable automatic cleanup, configure a retention window via
`WorkflowRuntimeBuilder::memo_retention`:

```rust,ignore
let runtime = WorkflowRuntime::builder(queue, store, runner, hook)
    .memo_retention(Duration::from_secs(24 * 60 * 60))
    .build();
```

When retention is set, the runtime writes a small terminal marker for
every terminal state (Succeeded, Failed, Cancelled) and
`WorkflowRuntime::run` spawns a background sweeper that lists those
markers and clears the memos plus marker for any run whose marker is
older than the retention window. The first sweep fires on startup so
a restarted process catches markers left behind by an earlier one.

Advanced cleanup policies (selective retention, externally-driven
sweeps) can be built directly on `MemoStore::list_terminal_markers`,
`MemoStore::clear_memos_for_run`, and `MemoStore::delete_terminal_marker`
without configuring `WorkflowRuntimeBuilder::memo_retention`.

## Time injection

Every timestamp the runtime writes (the `submitted_at_ms` on the
durable per-run record, the `run_at` it computes when a step
returns `ContinueAfter`, and the terminal-marker timestamps the
memo-retention sweep consumes) is read through a `taquba::Clock`
rather than `SystemTime::now()`. By default the runtime inherits
the clock its `Queue` was opened with, so passing a `MockClock`
to `OpenOptions::clock` virtualises both the queue and the
workflow runtime in lockstep:

```rust,ignore
let clock = MockClock::new(1_700_000_000_000);
let opts = OpenOptions {
    clock: Arc::new(clock.clone()),
    ..OpenOptions::default()
};
let queue = Queue::open_with_options(store.clone(), "db", opts).await?;
let runtime = WorkflowRuntime::builder(queue, store, runner, hook).build();
// `runtime` reads the same clock as `queue`; `clock.advance(...)`
// moves every time-based decision the runtime makes.
```

Override the inherited default via `WorkflowRuntimeBuilder::clock`
when a test or specialised setup needs the runtime on a different
time source than the queue. The common case for production callers
is to leave the default and let the queue's `SystemClock` flow
through.

This makes downstream tests deterministic: `ContinueAfter` delays,
memo-retention sweep eligibility, and terminal-marker ages all
advance under explicit `MockClock::advance` calls rather than
wall-clock waits.

## Duplicate submissions

`WorkflowRuntime::submit` is idempotent on `(run_id, spec.input)`. A
re-submission of an active run that carries the same input is a no-op
and the returned `SubmitOutcome` has `newly_submitted = false`. A
re-submission that carries a *different* input is rejected with
`Error::InputMismatch`: reusing a `run_id` with new content is a
programmer error; pick a fresh `run_id` for a new run.

Duplicates are caught from two sources, in order:

1. An in-process registry catches duplicates within the same runtime.
2. A **durable per-run record** written atomically with the step-0
   enqueue (via Taquba's `enqueue_with_kv`) catches duplicates across
   process restarts, even after step 0 has been claimed and its dedup
   key released. The record carries a SHA-256 of the original input so
   the cross-restart mismatch check works even when the in-memory
   registry is empty. The record is cleaned up when the run reaches a
   terminal state.

## Terminal hook

`TerminalHook::on_termination` fires once per run on `Succeeded`,
`Failed`, or `Cancelled`, receiving the submitter's headers and the
runner's result or error. `WebhookTerminalHook` (behind the `webhooks`
feature) fires HTTP callbacks via `taquba-webhooks`; set the per-run URL
on `RunSpec::headers["callback_url"]`.

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or
   <http://www.apache.org/licenses/LICENSE-2.0>)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or
   <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.

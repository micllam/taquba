# taquba-workflow

[![crates.io](https://img.shields.io/crates/v/taquba-workflow.svg)](https://crates.io/crates/taquba-workflow)
[![docs.rs](https://img.shields.io/docsrs/taquba-workflow)](https://docs.rs/taquba-workflow)
[![license](https://img.shields.io/crates/l/taquba-workflow.svg)](#license)

Durable execution on object storage: an at-least-once workflow runtime on
top of the [Taquba](../taquba) durable task queue.

> Part of the [Taquba ecosystem](https://github.com/micllam/taquba); see the
> workspace README for the queue core and the other crates that compose with
> this one.

`taquba-workflow` provides the durable machinery for any multi-step process
that benefits from durable state between steps: idempotent step execution,
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

The `jobs` module runs single-shot typed tasks: use it when the caller
awaits a typed return value and there are no intermediate steps to
persist; use a step runner (even for a single step) when the caller
observes the run through cancellation and a terminal hook and awaits no
returned value. The `bulk` module runs one pipeline over many inputs with
batch progress and cost rollup.

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
handed to the runtime. Choose an explicit name via
`WorkflowRuntimeBuilder::queue_name` and key `OpenOptions::queue_configs`
on the same string.

```rust
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
let opts = OpenOptions::default().queue_config(
    "agent-runs",
    QueueConfig::default().keep_done_jobs(Duration::from_secs(24 * 60 * 60)),
);
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

    let worker = runtime.spawn(std::future::pending::<()>());

    let handle = runtime.submit(RunSpec {
        input: b"hello".to_vec(),
        ..Default::default()
    }).await?;
    println!("submitted run {}", handle.run_id);
    worker.shutdown().await?;
    Ok(())
}
```

## Examples

```bash
cargo run -p taquba-workflow --example single_step
cargo run -p taquba-workflow --example multi_step
cargo run -p taquba-workflow --example crash_resume
cargo run -p taquba-workflow --example durable_approvals
cargo run -p taquba-workflow --example fanout_jobs
ANTHROPIC_API_KEY=... cargo run -p taquba-workflow --example rig_agent
OPENAI_API_KEY=...    cargo run -p taquba-workflow --example rig_agent
```

`crash_resume` runs on the local filesystem and demonstrates recovery
directly: start it, interrupt it during any stage and start it again.
The second process resumes the same run, skips the stages already
committed and serves the completed units of the interrupted stage from
the memo store.

`durable_approvals` runs an agent loop on the local filesystem and
holds it for approval across processes: the first invocation
investigates a claim one turn per step, proposes a refund and exits
with the run waiting; a later invocation with `-- approve` or
`-- reject <reason>` delivers the decision and resumes the run to
completion. A decision delivered before the first invocation is
buffered and consumed at registration, and a run that waits past its
timeout escalates at the next plain invocation.

`rig_agent` is a two-stage AI agent (research, then write) structured
for between-step durability: step 0's research is persisted as queue
state before step 1 begins, so on a persistent store a process that
crashes between the steps resumes at step 1 without re-running the
research. The example itself runs on the in-memory store; substitute a
persistent `object_store` backend, as `crash_resume` does, to observe
recovery across a process restart.

`fanout_jobs` composes the runtime with typed jobs for fan-out
inside one run: a step submits one typed job per URL to a shared
`JobRunner`, joins the typed results, and memoizes the aggregate so a
step retry does not re-submit the fan-out.

## Step outcomes

| Outcome | Effect |
|---|---|
| `StepOutcome::Continue { payload, when }` | Enqueue the next step; `when` (a `Trigger`) decides when it becomes claimable: `Trigger::Immediate`, `Trigger::After(delay)` or `Trigger::OnSignal { correlation_key, timeout }`. Constructors: `StepOutcome::continue_now(payload)`, `StepOutcome::continue_after(payload, delay)`, `StepOutcome::continue_on_signal(payload, key, timeout)`. |
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
  removed and the run's notification job is enqueued before the `cancel`
  call returns.
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
  and the worker settles the run as `Cancelled` once the step returns.
  Watching the token only reduces cancellation latency for slow steps;
  it doesn't change semantics.

While termination is in flight, `WorkflowRuntime::status` reports a
`RunState::Cancelling` overlay until the entry is dropped.

Returns `Ok(false)` if the run is unknown or already terminal in this
runtime. `cancel` only reaches runs submitted to this `WorkflowRuntime`
instance; a second runtime in the same process (sharing the queue)
maintains its own registry.

## Long-running steps

A step that outlives the queue's lease is re-queued by the reaper and
delivered a second time. A long-running runner avoids this by extending
its lease through `Step::lease`: call `LeaseHandle::ensure_at_least` at
progress points, or once, with a slow call's timeout, before issuing
the call.

## Durable signals

A step can pause the rest of its run until an external event. Returning
`StepOutcome::continue_on_signal` (a `Trigger::OnSignal`) defers the next
step until a signal for the chosen correlation key arrives via
`WorkflowRuntime::signal`, or until the timeout elapses. The next step
reads `Step::signal`: `Some(payload)` when a signal arrived, `None` when
the timeout fired. The natural fit is a run that waits for an approval, a
webhook callback or another run's completion, with the timeout as the
escalation path.

```rust,ignore
// In the runner: pause the run for the payment webhook, or escalate
// after seven days.
Ok(StepOutcome::continue_on_signal(
    order_id.into_bytes(),
    format!("payment:{order_id}"),
    Duration::from_secs(7 * 24 * 3600),
))

// In the webhook handler (same process):
match runtime.signal(&format!("payment:{order_id}"), body).await? {
    SignalOutcome::Delivered => { /* a waiting run was woken */ }
    SignalOutcome::Buffered => { /* held for the next waiter */ }
}
```

Signals are durable in both directions. The waiting step is a scheduled
job in the store, so the wait survives restarts and occupies no worker
while pending. A signal with no registered waiter is buffered durably
under its correlation key and consumed by the next waiter registered for
it, so a signal that arrives before its waiter is not lost;
`WorkflowRuntime::clear_signal` discards a buffered signal that is no
longer wanted. A waiting run resumes under the runner hosted by the
resuming process: the wait survives restarts of the agent, and a runner
changed while the run waits is the caller's compatibility concern.

Semantics: delivery follows the crate's at-least-once model (the woken
step can be redelivered and observes the same `Step::signal` value on
every attempt). One buffered signal is held per correlation key; a second
signal before consumption replaces the first. One waiter is allowed per
correlation key; registering a second one fails that run, so choose keys
unique to the waiter (include the run id if uniqueness is uncertain). Signals are
scoped to the store: the signaller is the same process that hosts the
runtime, per the single-process design.

See [`examples/durable_approvals.rs`](examples/durable_approvals.rs)
for a runnable approval flow covering all three delivery paths (signal,
timeout, buffered) across process restarts.

## Application KV effects

Application state that describes a run (a status row, a progress marker,
an outcome record) can be written to Taquba's caller KV namespace in the
same transaction as the run's own transitions, so a crash cannot leave
the two disagreeing. Two surfaces:

- `RunSpec::kv_writes`: writes applied atomically with the step-0
  enqueue. A duplicate submission drops its writes.
- `Step::effects`: an `EffectsHandle` that stages writes and deletes
  during a step. Everything staged is applied in the settlement
  transaction that commits the outcome the runner returned, whichever
  outcome that is (`Continue`, `Succeed`, `Fail` or `Cancel`).

```rust,ignore
// Inside StepRunner::run_step: the outcome record commits with the
// step's own settlement.
step.effects.put(format!("app/runs/{}", step.run_id), summary)?;
Ok(StepOutcome::Succeed { result })
```

Semantics:

- Delivery is at-least-once, so a retried step stages its effects again;
  every staged value must be correct when applied more than once (write
  absolute values).
- No effects are applied when the runner returns a `StepError` (the step
  retries or dead-letters) or when an external `WorkflowRuntime::cancel`
  overrides the outcome. A runner-issued `StepOutcome::Cancel` keeps its
  effects.
- Operations are validated as they are staged: the `workflow/` prefix
  (`RESERVED_KV_PREFIX`) is reserved for the runtime, values are capped
  at `taquba::MAX_KV_VALUE_SIZE` and a key cannot be staged for both a
  write and a delete within one step.
- With `WorkflowRuntimeBuilder::step_output_replay` enabled, the replay
  record stores the staged effects with the outcome, so a replayed
  delivery applies them without invoking the runner.

The written values are readable inside a step through `Step::kv` (a
`KvReadHandle` exposing `get` only, answering from committed state, so
effects staged by the running step are excluded), through
`Queue::kv_get` and, from another process, through a `QueueReader`.

See [`examples/kv_effects.rs`](examples/kv_effects.rs) for a runnable
order flow maintaining a status row through both surfaces.

## Reserved headers

Step jobs reserve the `workflow.*` prefix; submission rejects user
headers starting with it. Other headers on `RunSpec::headers` thread
through every step and reach the terminal hook on `RunOutcome::headers`.

| Key | Meaning |
|---|---|
| `workflow.run_id` | Run identifier. |
| `workflow.step` | Zero-based step number. |

## Bulk processing

The `bulk` module runs one `Pipeline` over many input items in parallel,
one run per item with the pipeline's phases as memoized calls inside the
item's single step, and adds batch-level progress, cost rollup, streamed
output and a failure threshold. The module documentation covers the
execution model, cost tracking, the failure policy and replay.

## Typed jobs

The `jobs` module runs one typed async function as a single-step run and
returns its result to an awaiting caller: define a `Job` with typed input
fields, an `Output` and an `Error`, register it on a `JobRunner`, submit
instances and await the `JobHandle`. The outcome record is stored in the
run's memo, so `JobHandle::fetch_result` reads it after a restart, and a
`Job::idempotency_key` collapses duplicate submissions before and after
completion. The module documentation covers idempotent submission,
retention and the handler context.

```rust,ignore
use taquba_workflow::jobs::{Job, JobContext, JobRunner};

#[derive(serde::Serialize, serde::Deserialize)]
struct SendEmail { to: String }

impl Job for SendEmail {
    const NAME: &'static str = "email.send";
    type Output = String;
    type Error = EmailError;

    async fn run(&self, _ctx: JobContext<'_>) -> Result<String, EmailError> {
        Ok(format!("msg-for-{}", self.to))
    }
}

let mut runner = JobRunner::builder(queue, store)
    .register::<SendEmail>()
    .build();
let worker = runner.spawn(std::future::pending::<()>());
let message_id = runner.submit(SendEmail { to: "user@example.com".into() }).await?.await?;
worker.shutdown().await?;
```

## Idempotency

Each step is enqueued with `dedup_key = "run:{run_id}:{step_number}"`,
preventing concurrent duplicate steps. But Taquba is at-least-once: a
step can be claimed and executed twice if its lease expires before ack.
**`StepRunner` implementations must be idempotent for the same
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

`Memo::memoized` is the typed form: it returns the stored value when
one exists and otherwise runs the computation, stores its value and
returns it. Values are encoded as MessagePack with named fields. An
entry that fails to decode is treated as absent and is overwritten by
the recomputed value; an error from the computation stores nothing.

```rust,ignore
let draft: Draft = step
    .memo
    .memoized("draft", async { expensive_call(&step.payload).await })
    .await?;
```

When the natural memo key is the content of an input value,
`Memo::content_get` and `Memo::content_put` serialize that input as
MessagePack, hash it with SHA-256, and use the digest as the memo key:

```rust,ignore
#[derive(serde::Serialize)]
struct DraftInput<'a> {
    operation: &'static str,
    payload: &'a [u8],
}

let input = DraftInput {
    operation: "draft",
    payload: &step.payload,
};
if let Some(cached) = step.memo.content_get(&input).await? {
    return Ok(StepOutcome::Succeed { result: cached });
}
let draft = expensive_call(&step.payload).await?;
step.memo.content_put(&input, &draft).await?;
Ok(StepOutcome::Succeed { result: draft })
```

Content-addressed memo keys remain scoped to `(run_id, step_number)`;
they are not a cross-run cache. If multiple logical operations may
receive identical inputs, include an operation name in the
serialized input. `Memo::content_key` returns the derived key, for
use with `Memo::get` and `Memo::put` or for locating an entry from
outside the runtime, and `Memo::memoized_by_content` is the typed
form over that key.

`Step::run_memo` is the run-scoped variant: one namespace shared by
every step of the run, for values a later step reads back (an
accumulating journal, for example). Its entries live beside the
per-step entries and are removed with them when the run's retention
expires.

Memo entries live in the object store passed to
`WorkflowRuntime::builder` under the path prefix configured by
`WorkflowRuntimeBuilder::memo_prefix` (default `"workflow-memo"`).
A memo is a retry-safety cache whose readers tolerate absence by
re-executing; the durable channel between steps is
`StepOutcome::Continue`'s payload.

## Step-output replay

`WorkflowRuntimeBuilder::step_output_replay` enables an additional
runtime-managed replay record for every outcome the runner returns,
including `Fail` and `Cancel`. Step errors (`StepError`) are not recorded,
so retries still invoke the runner. The record is keyed by
`(run_id, step_number, SHA-256(step payload))` and is written before the
runtime applies the outcome. If the same step is delivered again after a
crash before ack, the stored outcome is replayed without invoking the
runner again. The record includes the effects staged through
`Step::effects`, so a replayed outcome applies them as well. A replayed
`Continue` with a `Trigger::After` delay reduces
the delay by the time already elapsed since the outcome was stored,
preserving the original schedule.

This is disabled by default because it adds one object-store read per step
delivery (the replay lookup) plus one write per recorded outcome, and makes
that write part of step settlement. The replay records are scoped to one
run and step; they are not a cross-run cache. They are cleared with the
run's memo entries when memo retention is configured.

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

When retention is set, the runtime records a small terminal marker for
every terminal state (Succeeded, Failed, Cancelled) and
`WorkflowRuntime::run` spawns a background sweeper that reads those
markers and clears the memo entries, step-output replay entries, and
marker for any run whose marker is older than the retention window. The
first sweep fires on startup so a restarted process catches markers left
behind by an earlier one.

A marker is a key under `workflow/terminals/` in the queue's key-value
namespace, written in the same transaction that settles the run. It
therefore exists exactly when the run's terminal outcome committed: a
settlement that does not commit leaves no marker, and no path writes one
for a run that is still executing. The terminating timestamp precedes
the run id in the key, so the sweep reads the expired set from the start
of the range and stops at the first unexpired marker.

Because the sweep is keyed on those terminal markers, and a terminated
run never resumes, it does not delete the memo or replay entries of an
in-flight run that a resume may still read, with one exception given
below. A resuming step that finds an entry absent re-executes the work
(delivery is at-least-once regardless), so a missing entry is always
safe to observe rather than a dangling reference: deletion is left
unguarded precisely because every reader tolerates absence.

The exception is a re-submitted run id. Entries are addressed by run id
and a terminated run releases its id, so a second run submitted under
that id shares the first run's entries, and the first run's marker
expires against them while the second run may still be executing. The
second run re-executes the affected steps. A run id's entries are
therefore retained from the first run's termination, and every later run
submitted under that id shares that window.

Advanced cleanup policies (selective retention, externally-driven
sweeps) can be built on `Queue::kv_scan` over that prefix and
`MemoStore::clear_memos_for_run`, without configuring
`WorkflowRuntimeBuilder::memo_retention`.

## Time injection

Every timestamp the runtime writes (the `submitted_at_ms` on the
durable per-run record, the `run_at` it computes when a step
continues with a `Trigger::After` delay, and the terminal-marker timestamps the
memo-retention sweep consumes) is read through a `taquba::Clock`
rather than `SystemTime::now()`. By default the runtime inherits
the clock its `Queue` was opened with, so passing a `MockClock`
to `OpenOptions::clock` virtualises both the queue and the
workflow runtime in lockstep:

```rust,ignore
let clock = MockClock::new(1_700_000_000_000);
let opts = OpenOptions::default().clock(Arc::new(clock.clone()));
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

This makes downstream tests deterministic: `Trigger::After` delays,
memo-retention sweep eligibility, and terminal-marker ages all
advance under explicit `MockClock::advance` calls rather than
wall-clock waits.

## Duplicate submissions

`WorkflowRuntime::submit` is idempotent on `(run_id, spec.input)`. A
re-submission of an active run that carries the same input is a no-op
and the returned `SubmitOutcome` has `newly_submitted = false`. A
re-submission that carries a *different* input is rejected with
`Error::InputMismatch`: reusing a `run_id` with new content is a
programmer error; choose a fresh `run_id` for a new run.

Duplicates are caught from two sources, in order:

1. An in-process registry catches duplicates within the same runtime.
2. A **durable per-run record** written atomically with the step-0 enqueue
   (via Taquba's `enqueue_with_kv`) catches duplicates across process
   restarts, even after step 0 has been claimed and its dedup key released.
   The record carries a SHA-256 of the original input so the cross-restart
   mismatch check works even when the in-memory registry is empty. A
   current-step pointer under `workflow/steps/` is written beside it,
   rewritten in the settlement that enqueues each next step, and names the
   queue job `SubmitOutcome::job_id` reports for a duplicate; a
   `QueueReader` can read it to resolve a run's live job from outside the
   process. Both are cleaned up when the run reaches a terminal state.

## Terminal hook

`TerminalHook::on_termination` processes a run's termination
(`Succeeded`, `Failed` or `Cancelled`), receiving the submitter's
headers and the runner's result or error. Termination is delivered as a
queue job: the settlement that commits a run's terminal outcome
atomically enqueues a notification job, and the hook runs as that job's
worker. The hook therefore observes only outcomes that committed, and
delivery is at-least-once, so implementations must be idempotent. A
transient error retries the notification per the queue's backoff up to
the terminal step's `max_attempts`; a permanent error dead-letters it.

The hook stages effects on a `TerminalEffects` handle: KV writes and
deletes plus follow-up enqueues, applied in the same transaction as the
notification's acknowledgement. `TerminalHook::observes` (default
`true`) is consulted when a run terminates; returning `false` skips the
notification job for that run. `NoopTerminalHook` observes nothing, so
runs terminate with no notification cost.

Runs terminated without an acknowledging settlement (an external
cancellation of a pending step, a step that dead-letters) settle their
notification the same way: the effects are applied by the dead-letter,
by the attempts-exhausting nack or by the cancellation's removal, so the
notification job is created exactly once on every worker and
cancellation path. Two terminations occur outside any settlement the
runtime performs: a job the reaper dead-letters after its lease expires
past the attempt limit, and one dead-lettered during crash recovery when
the queue is opened. The worker reconciles them: whenever the queue's dead
count changes it terminates every run whose dead step job still has a run
record, as `Failed` with the queue record's last error, enqueueing the
notification in the same transaction.

`WebhookTerminalHook` (behind the `webhooks` feature) delivers HTTP
callbacks via `taquba-webhooks`, staging the delivery enqueue as a
notification effect so it is created exactly once with the
acknowledgement; set the per-run URL on
`RunSpec::headers["callback_url"]`. Runs without that header enqueue no
notification.

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

# taquba-workflow

[![crates.io](https://img.shields.io/crates/v/taquba-workflow.svg)](https://crates.io/crates/taquba-workflow)
[![docs.rs](https://img.shields.io/docsrs/taquba-workflow)](https://docs.rs/taquba-workflow)
[![license](https://img.shields.io/crates/l/taquba-workflow.svg)](#license)

Durable execution for Rust on object storage: an at-least-once workflow
runtime over the [Taquba](../taquba) durable task queue. A run is a
sequence of steps whose state (each step's output, memoized side effects
and application KV effects) is stored in a bucket or a local directory,
so a process restarted mid-run resumes at its last committed step.

> Part of the [Taquba ecosystem](https://github.com/micllam/taquba); see the
> workspace README for the queue core and the other crates that compose with
> this one.

The runtime is an embedded library: producers and workers share one
`Arc<Queue>` in one process, and no server, database or control plane
runs beside it. It is built for long-running, expensive, IO- or API-bound
work: LLM agent runs (see [`examples/rig_agent.rs`](examples/rig_agent.rs)
for a [Rig](https://github.com/0xPlaygrounds/rig) integration), document
pipelines, payment flows and command-line tools whose runs survive an
interruption. Implement `StepRunner` with bytes-in, bytes-out per-step
logic; the runtime persists everything else.

## Scope

`taquba-workflow` is an imperative step orchestrator: at each step the
runner returns a `StepOutcome` (Continue, Succeed, Fail or Cancel) that
decides what happens next, and `WorkflowRuntime::cancel` cancels a run
from outside. It is neither a DAG executor (no declarative graph, no
fan-out or fan-in, no dependency-driven scheduling) nor an event-sourced
engine (no event-history replay; a side effect is recorded only where
the runner memoizes it).

The `jobs` module is the typed presentation of the runtime: it runs one
typed async function as a single-step run and returns its result to an
awaiting caller, and a `JobGroup` submits many such jobs as one durable
set. Use a job when the caller awaits a typed return value and there
are no intermediate steps to persist, a job group when many inputs go
through one function and the caller reads the results as they complete,
and a step runner, even for a single step, when the caller observes the
run through cancellation, headers and a terminal hook.

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
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "demo").await?);

    let runtime = WorkflowRuntime::builder(queue, store, EchoRunner, NoopTerminalHook).build();

    let worker = runtime.spawn(std::future::pending::<()>());

    let outcome = runtime.submit(RunSpec {
        input: b"hello".to_vec(),
        ..Default::default()
    }).await?;
    println!("submitted run {}", outcome.run_id);
    worker.shutdown().await?;
    Ok(())
}
```

Replace `InMemory` with an S3, GCS or Azure builder in production. The
runtime's queue is named by `WorkflowRuntimeBuilder::queue_name`;
per-queue settings in `OpenOptions::queue_configs` (retention, lease
duration, attempt limit) are keyed on that name when the `Queue` is
opened.

## Examples

```bash
cargo run -p taquba-workflow --example <name>
```

The header of each file under [`examples/`](examples) states what it
demonstrates.

## Step outcomes

| Outcome | Effect |
|---|---|
| `StepOutcome::Continue { payload, when }` | Enqueue the next step; `when` (a `Trigger`) decides when it becomes claimable: `Trigger::Immediate`, `Trigger::After(delay)` or `Trigger::OnSignal { correlation_key, timeout }`. Constructors: `StepOutcome::continue_now(payload)`, `StepOutcome::continue_after(payload, delay)`, `StepOutcome::continue_on_signal(payload, key, timeout)`. |
| `StepOutcome::Succeed { result }` | Ack; the terminal hook observes `Succeeded`. |
| `StepOutcome::Fail { reason }` | Ack; the terminal hook observes `Failed`. A runner verdict: no dead-letter. |
| `StepOutcome::Cancel { reason }` | Ack; the terminal hook observes `Cancelled`. A runner verdict: no dead-letter. |
| `Err(StepError::transient(_))` | Retry per backoff up to `max_attempts`, then dead-letter. |
| `Err(StepError::permanent(_))` | Dead-letter immediately. |

`StepOutcome::Fail` and `StepOutcome::Cancel` are runner verdicts and
acknowledge normally; an `Err(StepError::permanent)` is an infrastructure
error and dead-letters, so operators find it through
`Queue::dead_jobs`.

## The delivery

A `Step` dereferences to its `Delivery`: the run id, the submitter's
headers, the queue job id, the attempt count and limit
(`Delivery::is_last_attempt` reports whether a transient error from this
attempt dead-letters the step) and the delivery's handles (the
cancellation token, the lease, the per-step and run-scoped memos, the
staged KV effects and committed KV reads). `jobs::JobContext`
dereferences to the same type.
`Delivery::detached` and `Step::detached` build instances bound to no
queue, for tests.

## Submissions

`WorkflowRuntime::submit` takes a `RunSpec`: the first step's `input`, an
optional `run_id` (1 to `MAX_RUN_ID_LEN` bytes of `[A-Za-z0-9_-]`; a ULID
is generated when absent), `headers`, a `priority` and
`max_attempts_per_step` overriding the queue's defaults for every step,
a `run_at` before which the first step is not claimable and `kv_writes`
applied with the enqueue. The returned `SubmitOutcome` names the run and
the queue job currently representing it.

`submit` is idempotent on `(run_id, input)`. A re-submission of an
active run with the same input is a no-op and the returned
`SubmitOutcome` has `newly_submitted = false`; one with a different
input is rejected with `Error::InputMismatch`. Duplicates are caught by
a durable per-run record written atomically with the step-0 enqueue
(via Taquba's `enqueue_with_kv`), so they are caught across process
restarts, even after step 0 has been claimed and its dedup key
released. The record holds a SHA-256 of the original input for the
mismatch check. A current-step pointer under `workflow/steps/` is
written beside it, rewritten in the settlement that enqueues each next
step and names the queue job `SubmitOutcome::job_id` reports for a
duplicate; a `QueueReader` can read it to resolve a run's live job from
outside the process. Both are removed when the run reaches a terminal
state.

`WorkflowRuntime::status` reads the record, the pointer and the step's
queue job into a `RunStatus` (`Pending`, `Running` or `Cancelling`,
with the current step number), so it answers after a restart and from
any runtime over the same queue. Under memo retention a terminated run
reports `RunState::Terminated` with its status, error and time of
termination, read from the terminal record described under
[Memo retention](#memo-retention); without it, a terminated run has no
status.

## Cancellation

`WorkflowRuntime::cancel(run_id)` cancels an active run from outside the
runner. The request is recorded on the run's durable record, so it
survives a restart and reaches the run from any runtime over the same
queue; while termination is in flight, `status` reports
`RunState::Cancelling`. It returns `Ok(false)` if the run is unknown or
already terminal.

- If the current step is pending or scheduled, the queued step job is
  removed and the run's notification job is enqueued before the call
  returns.
- If the current step is running, the request is delivered through
  `Delivery::cancel_token` (a `tokio_util::sync::CancellationToken`). A
  runner that watches the token returns at once:

  ```rust,ignore
  tokio::select! {
      out = call_llm(step) => out,
      _ = step.cancel_token.cancelled() => {
          Ok(StepOutcome::Cancel { reason: "cooperative".into() })
      }
  }
  ```

  A runner that ignores the token runs to completion (a future cannot be
  aborted safely mid-step). In both cases the runner's `StepOutcome` is
  discarded, any pending transient retry is suppressed and the worker
  settles the run as `Cancelled` once the step returns. Watching the
  token reduces the latency of cancelling a slow step; the semantics are
  the same.
- A step claimed after the request is settled as cancelled without
  running.

## Long-running steps

A step that outlives the queue's lease is re-queued by the reaper and
delivered a second time. A long-running runner avoids this by extending
its lease through `Delivery::lease`: call `LeaseHandle::ensure_at_least`
at progress points, or once, with a slow call's timeout, before issuing
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
resuming process, so a runner changed while the run waits is the
caller's compatibility concern.

Delivery follows the crate's at-least-once model: the woken step can be
redelivered and observes the same `Step::signal` value on every attempt.
One buffered signal is held per correlation key; a second signal before
consumption replaces the first. One waiter is allowed per correlation
key; registering a second one fails that run, so choose keys unique to
the waiter (include the run id if uniqueness is uncertain). Signals are
scoped to the store: the signaller is the same process that hosts the
runtime, per the single-process design.

See [`examples/durable_approvals.rs`](examples/durable_approvals.rs)
for a runnable approval flow covering all three delivery paths (signal,
timeout and buffered) across process restarts.

## Application KV effects

Application state that describes a run (a status row, a progress marker,
an outcome record) can be written to Taquba's caller KV namespace in the
same transaction as the run's own transitions, so a crash cannot leave
the two disagreeing. Two surfaces:

- `RunSpec::kv_writes`: writes applied atomically with the step-0
  enqueue. A duplicate submission drops its writes.
- `Delivery::effects`: an `EffectsHandle` that stages writes and deletes
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

The written values are readable inside a step through `Delivery::kv` (a
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

## Typed jobs

The `jobs` module runs one typed async function as a single-step run and
returns its result to an awaiting caller: define a `Job` with typed input
fields, an `Output` and an `Error`, register it on a `JobRunner`, submit
instances and await the `JobHandle`. The outcome record is stored in the
run's memo, so `JobHandle::fetch_result` reads it after a restart, and a
`Job::idempotency_key` collapses duplicate submissions before and after
completion. A handler that submits further jobs holds a `JobRunner` in
its registered state. The module documentation covers idempotent
submission, retention and the handler context.

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

## Job groups

A `JobGroup` submits many jobs of one type as one durable set:
`JobRunner::group` names it, `JobGroup::submit` writes its manifest (the
members' keys and inputs) and submits the members, `JobGroup::results`
yields each member's typed result as it terminates and `JobGroup::join`
returns them in submission order. Members are keyed by the job's
idempotency key or the positional `item-{i}`, and a member's job id is
derived from the group id and its key, so groups never share run state.
A second submission of the same set, or `JobGroup::resume` from the
manifest alone, runs again only the members that did not succeed, which
is how a step that fans out stays safe under a retry and how a batch of
inputs is run again after a partial failure.

The group's durable state is the manifest in the object store and one
member record per key under `workflow/groups/` in the queue's key-value
namespace, written with the member's submission and rewritten with its
status and error by the settlement that terminates it; `JobGroup::status`
reads it, `JobGroup::forget` removes it and
`JobRunnerBuilder::group_retention` removes it a window after the members
all terminated, through a sweep over `workflow/group-terminals/`.
Streamed output, progress, a failure threshold and a cost rollup are
folds the caller writes over the results;
[`examples/group_document_pipeline.rs`](examples/group_document_pipeline.rs)
shows a per-document pipeline of memoized stages with counters rolled up
by the caller.

## Idempotency

Each step is enqueued with `dedup_key = "run:{run_id}:{step_number}"`,
so no two pending or scheduled jobs exist for the same step at the same
time. Delivery is at-least-once, so a step can still be claimed and
executed twice if its lease expires before its acknowledgement:
**`StepRunner` implementations must be idempotent for the same
`(run_id, step_number)`.**

## Memoizing within-step side effects

Because a retry can re-execute a step, an expensive non-idempotent side
effect (an LLM call, a paid API, one stage of a multi-stage step) records
its result so a retry observes the recorded value and does not repeat
the call. `Delivery::memo` is a per-step durable key-value store scoped
to `(run_id, step_number)`:

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
`Memo::memoized_by_content` (and the untyped `Memo::content_get` and
`Memo::content_put`) serializes that input as MessagePack, hashes it
with SHA-256 and uses the digest as the memo key. The entry remains
scoped to `(run_id, step_number)`; it is not a cross-run cache. If
several logical operations may receive identical inputs, include an
operation name in the serialized input. `Memo::content_key` returns the
derived key, for use with `Memo::get` and `Memo::put` or for locating an
entry from outside the runtime.

```rust,ignore
#[derive(serde::Serialize)]
struct DraftInput<'a> {
    operation: &'static str,
    payload: &'a [u8],
}

let input = DraftInput { operation: "draft", payload: &step.payload };
let draft: Draft = step
    .memo
    .memoized_by_content(&input, async { expensive_call(&step.payload).await })
    .await?;
```

`Delivery::run_memo` is the run-scoped variant: one namespace shared by
every step of the run, for values a later step reads back (an
accumulating journal, for example). Its entries are stored beside the
per-step entries and are removed with them when the run's retention
expires.

Memo entries are stored in the object store passed to
`WorkflowRuntime::builder` under the path prefix configured by
`WorkflowRuntimeBuilder::memo_prefix` (default `"workflow-memo"`). A
memo is a retry-safety cache whose readers tolerate absence by
re-executing; the durable channel between steps is
`StepOutcome::Continue`'s payload.

## Step-output replay

`WorkflowRuntimeBuilder::step_output_replay` enables a runtime-managed
replay record for every outcome the runner returns, including `Fail`
and `Cancel`. Step errors (`StepError`) are not recorded, so a retry
still invokes the runner. The record is keyed by
`(run_id, step_number, SHA-256(step payload))` and is written before the
runtime applies the outcome. If the same step is delivered again after a
crash before its acknowledgement, the stored outcome is replayed without
invoking the runner. The record includes the effects staged through
`Delivery::effects`, so a replayed outcome applies them as well. A
replayed `Continue` with a `Trigger::After` delay reduces the delay by
the time already elapsed since the outcome was stored, preserving the
original schedule.

Replay is disabled by default because it adds one object-store read per
step delivery (the replay lookup) plus one write per recorded outcome,
and makes that write part of step settlement. The records are scoped to
one run and step, and are removed with the run's memo entries when memo
retention is configured.

## Memo retention

By default memo entries are retained indefinitely. To remove them
automatically, configure a retention window via
`WorkflowRuntimeBuilder::memo_retention`:

```rust,ignore
let runtime = WorkflowRuntime::builder(queue, store, runner, hook)
    .memo_retention(Duration::from_secs(24 * 60 * 60))
    .build();
```

When retention is set, every settlement that commits a terminal outcome
(`Succeeded`, `Failed` or `Cancelled`) writes two keys in the queue's
key-value namespace in the same transaction: a terminal marker under
`workflow/terminals/` and a terminal record under `workflow/outcomes/`
holding the status, the error, the final step and the time of
termination, so both exist exactly when the run's terminal outcome
committed. `WorkflowRuntime::run` sweeps the markers on startup and on
every retention interval, removing the memo entries, the step-output
replay entries, the terminal record and the marker of every run whose
marker is older than the window. The terminating timestamp precedes the
run id in the marker key, so the sweep reads the expired set from the
start of the range and stops at the first unexpired marker.

Because the sweep is keyed on terminal markers and a terminated run
never resumes, the entries of an in-flight run are not removed, with
one exception: entries are addressed by run id, and a terminated run
releases its id, so a second run submitted under that id shares the
first run's entries, and the first run's marker expires against them
while the second run may still be executing. The second run re-executes
the affected steps. Deletion is unguarded because every reader tolerates
absence: a step that finds an entry absent re-executes the work, as it
would under at-least-once delivery anyway.

Other cleanup policies (selective retention, externally-driven sweeps)
can be built on `Queue::kv_scan` over that prefix and
`MemoStore::clear_memos_for_run`, without configuring
`WorkflowRuntimeBuilder::memo_retention`.

## Time injection

Every timestamp the runtime writes (the `submitted_at_ms` on the durable
per-run record, the `run_at` it computes for a `Trigger::After` delay and
the terminal-marker timestamps the memo-retention sweep consumes) is read
through a `taquba::Clock`. By default the runtime inherits the clock its
`Queue` was opened with, so passing a `MockClock` to `OpenOptions::clock`
virtualises both in lockstep, and `MockClock::advance` moves every
time-based decision the runtime makes: `Trigger::After` delays, sweep
eligibility and terminal-marker ages.

```rust,ignore
let clock = MockClock::new(1_700_000_000_000);
let opts = OpenOptions::default().clock(Arc::new(clock.clone()));
let queue = Queue::open_with_options(store.clone(), "db", opts).await?;
let runtime = WorkflowRuntime::builder(queue, store, runner, hook).build();
// `runtime` reads the same clock as `queue`.
```

`WorkflowRuntimeBuilder::clock` overrides the inherited default when a
test or specialised setup needs the runtime on a different time source
than the queue.

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

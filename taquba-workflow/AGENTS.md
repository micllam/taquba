# AGENTS.md: taquba-workflow

Each rule identifies the code it applies to, whose docs describe the mechanism.

## Settlement and effects

- **Application KV effects** (`worker.rs::process_step`). Do not add a second,
  crate-private effects channel. The external-cancel discard stays conditional
  inside the `settled.map` tail.
- **Terminal effects** (`runtime.rs::terminate_collecting_effects`). Do not
  stage the hook's effects in the `settled.map` tail. The write of the terminal
  record over the run record stays on the terminal ack.
- **Terminal hook** (`terminal.rs`). The hook stages effects into the
  terminating settlement and nothing else. Do not add an asynchronous hook
  method, a notification job or a filter of the runs that the hook sees. For a
  reaction after the termination, the hook stages an enqueue.
- **Terminal record.** Every termination arm passes the input hash to
  `RuntimeCore::termination`. The record contains the reference to the result of
  a succeeded run, never the result or a cost, because the run listing scans the
  record and the member record copies it.
- **Result** (`view.rs::outcome_of`). Only this function resolves the result
  reference of a terminal record. Every result object is named by the step job
  id and the attempt of the settlement that writes it.
- **Cancellation.** Do not add per-run in-process state.
- **Decode failures.** The decode policy in the `durable.rs` module docs does
  not apply to the typed job layer, which decodes caller types.

## Runtime structure

- **Delivery** (`runner.rs`). A new per-delivery handle is a field on
  `Delivery`. Do not add a field that lets a runner settle its own delivery.
  `process_step` is the one construction site, and `Delivery::detached` the one
  detached constructor.
- **Control plane** (`runtime.rs`). Do not put `R` back on a control type. Read
  the current step's job through `WorkflowView::current_job`, never by pairing
  `current_step_if_active` with a job read.
- **Run wait** (`RuntimeCore::wait_run`). Do not reconcile dead steps inline in
  the wait.
- **Retention sweeps** (`sweep.rs`). A new retention need implements
  `sweep::Clearable` and registers a `sweep::Sweep`. Do not add a second sweep
  loop.
- **Memo** (`memo.rs`). Do not add a codec, decode-policy or version parameter.
  A layout change updates the literal digests in
  `entries_are_stored_at_the_documented_paths`.

## Typed jobs and groups

- **Typed jobs** (`jobs`). `jobs::handle::decode_end` is the one site that turns
  a `TerminatedRun` into a typed result. A handler that submits keeps a
  `JobRunner` in its state. Do not add `submit` to `JobContext`, a queue job id
  to `JobHandle`, a second handler trait, an open-coded decode-run-encode
  sequence or a runtime wrapper.
- **Run groups** (`group.rs`). Do not add a batch type, a second typed
  presentation, per-member settings or membership on the run record.

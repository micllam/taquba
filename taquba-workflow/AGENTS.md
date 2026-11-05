# AGENTS.md: taquba-workflow

Each rule identifies the code it applies to, whose docs describe the mechanism.

## Settlement and effects

- **Application KV effects** (`worker.rs::process_step`). Do not add a second,
  crate-private effects channel. The external-cancel discard inside the
  `settled.map` tail must stay conditional.
- **Terminal effects** (`runtime/terminate.rs::termination_effects`). Do not
  stage the hook's effects in the `settled.map` tail. The write of the terminal
  record over the run record must stay on the terminal ack.
- **Terminal hook** (`terminal.rs`). Do not add an asynchronous hook method, a
  notification job or a filter of the runs that the hook sees. A reaction after
  the termination must be an enqueue that the hook stages.
- **Terminal record.** The record must contain only the reference to the result
  of a succeeded run, never the result or a cost, because the run listing scans
  the record and the member record copies it.
- **Result** (`view.rs::outcome_of`). Only this function may resolve the result
  reference of a terminal record. Every result object must be named by the step
  job id and the attempt of the settlement that writes it.
- **Cancellation.** Do not add per-run in-process state.
- **Decode failures.** The typed job layer must not use the decode policy in the
  `durable.rs` module docs, because it decodes caller types.

## Runtime structure

- **Delivery** (`runner.rs`). A new per-delivery handle must be a field on
  `Delivery`. Do not add a field that lets a runner settle its own delivery.
  `process_step` must stay the one construction site, and `Delivery::detached`
  the one detached constructor.
- **Control plane** (`runtime.rs`). Do not put `R` back on a control type. Read
  the current step's job through `WorkflowView::current_job`, never by pairing
  `current_step_if_active` with a job read.
- **Run wait** (`RuntimeCore::wait_run`). Do not reconcile dead steps inline in
  the wait.
- **Retention sweeps** (`sweep.rs`). A new retention need must implement
  `sweep::Clearable` and register a `sweep::Sweep`. Do not add a second sweep
  loop.
- **Memo** (`memo.rs`). Do not add a codec, decode-policy or version parameter.

## Typed jobs and groups

- **Typed jobs** (`jobs`). `jobs::handle::decode_end` must stay the one site
  that turns a `TerminatedRun` into a typed result. A handler that submits must
  keep a `JobRunner` in its state. Do not add `submit` to `JobContext`, a queue
  job id to `JobHandle`, a second handler trait, an open-coded decode-run-encode
  sequence or a runtime wrapper.
- **Run groups** (`group.rs`). Do not add a batch type, a second typed
  presentation, per-member settings or membership on the run record.

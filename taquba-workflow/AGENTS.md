# AGENTS.md: taquba-workflow

Each rule identifies the code it applies to, whose docs describe the mechanism.

## Settlement and effects

- **Application KV effects** (`worker.rs::process_step`). Do not add a second,
  crate-private effects channel. The external-cancel discard stays conditional
  inside the `settled.map` tail.
- **Terminal notifications** (`runtime.rs::terminate_collecting_effects`). Do
  not build the notification enqueue in the `settled.map` tail. The run-record
  delete stays on the terminal ack.
- **Terminal record.** Every termination arm passes the input hash to
  `RuntimeCore::termination`. Do not add a result payload or a cost to the
  record.
- **Run result record.** `WorkflowView::run_result_of` is the one site that
  matches a record to a termination.
- **Cancellation.** Do not add per-run in-process state.
- **Decode failures.** The decode policy in the `durable.rs` module docs does
  not apply to the typed job layer, which decodes caller types.

## Runtime structure

- **Delivery** (`runner.rs`). A new per-delivery handle is a field on
  `Delivery`. Do not add a field that lets a runner settle its own delivery.
  `process_step` is the one construction site, and `Delivery::detached` the one
  detached constructor.
- **Control plane** (`runtime.rs`). Do not put `R` or `H` back on a control
  type. Read the current step's job through `WorkflowView::current_job`, never
  by pairing `current_step_if_active` with a job read.
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
  a termination and an outcome into a typed result. A handler that submits
  keeps a `JobRunner` in its state. Do not add `submit` to `JobContext`, a
  queue job id to `JobHandle`, a second handler trait, an open-coded
  decode-run-encode sequence or a runtime wrapper.
- **Run groups** (`group.rs`). Do not add a batch type, a second typed
  presentation, per-member settings or membership on the run record.

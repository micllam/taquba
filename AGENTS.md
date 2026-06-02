# AGENTS.md

This file provides guidance to agents when working with code in this repository.

Workspace: `taquba` core, plus `taquba-jobs`, `taquba-cron`, `taquba-webhooks`, and `taquba-workflow` built on it, and `taquba-bulk` built on `taquba-workflow`. See each crate's README for surface-level usage.

## Build / test

Tests live inline in `mod tests`; there is no `tests/` directory.

The `taquba` crate's `aws` / `gcp` / `azure` features are mutually exclusive; pick one for cloud builds.

Canonical workspace check (run locally before pushing):

```bash
cargo fmt --all
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

## Architectural invariants

These constrain almost every design decision; violating them breaks correctness.

- **Single-process, single-writer.** SlateDB allows one writer per store, so all producers and workers for a `Queue` share one `Arc<Queue>` in the same process. Multi-node worker fleets and out-of-process producers are out of scope.
- **At-least-once delivery.** Workers must be idempotent. There is no exactly-once mode and there will not be one.
- **Durability is per-transition.** Every state change is a SlateDB write. When adding state, design the key layout first.
- **Pre-1.0:** minor bumps may break source compat *and* on-disk layout; patch bumps preserve both. Anything that changes a key prefix or serialized record layout is a minor-version change.

## Misc

- Key-prefix convention (see helpers at the top of `taquba/src/queue.rs`): put the field you scan by first. `claimed:` and `scheduled:` keys lead with a zero-padded timestamp (before the queue name) so the reaper/scheduler do one *global* prefix scan across all queues with early exit. Follow the same layout when adding a new prefix.
- Worker errors: returning a `PermanentFailure` dead-letters the job immediately; any other error nacks and retries with exponential backoff up to `QueueConfig::max_attempts`.
- `taquba-bulk` maps each input item to one `taquba-workflow` run with a single `Succeed` step that runs the user's `Pipeline`. The pipeline's logical steps are `BulkCtx::memoized` keys inside that step, not workflow `Continue` steps; the runner never emits `Continue`. The `Pipeline` trait hides the workflow backend, so optimising the single-step path later stays non-breaking.
- Keep each crate's `lib.rs` top-level `//!` docstring and its `README.md` in content parity: anything substantive (new sections, design notes, semantics callouts) lands in both. Format may differ (lib.rs uses intra-doc `[Foo]` links and `#`-hidden doctest lines; README uses URL links and full `#[tokio::main]` blocks so code is copy-pasteable).
- Keep docstrings about the code, not the conversation: state what a type or function is and any non-obvious behaviour or invariant; omit rationale that only makes sense for the change that introduced it (call sites, design history, debate). Where the non-obvious *why* matters, put a short note here in CLAUDE.md rather than in a docstring that will drift.

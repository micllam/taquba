# AGENTS.md

This file provides guidance to agents when working with code in this repository.

Workspace: `taquba` core, plus `taquba-jobs`, `taquba-cron`, `taquba-webhooks`, and `taquba-workflow` built on it, and `taquba-bulk` built on `taquba-workflow`. `taquba-bencher` is an internal, unpublished member holding every crate's benchmarks (the published crates carry no bench targets). See each crate's README for surface-level usage.

## Build / test

Tests live inline in `mod tests`; there is no `tests/` directory. Benchmarks live in `taquba-bencher/benches/` as `harness = false` binaries; shared bench setup is `taquba-bencher`'s lib.

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
- Retention-sweep safety against a stalled writer: the two object-store sweepers (jobs result-blob retention, workflow memo/replay retention) delete by a bare age threshold (`terminal_at_ms < now - retention`) with no transaction and no verify-before-delete. This is safe not because deletion is guarded but because every *consumer* tolerates absence: a missing jobs result blob makes the idempotent submit path re-verify and re-run rather than trust a dangling pointer; a missing workflow memo/replay entry makes the step re-execute (delivery is at-least-once). The workflow sweep is additionally keyed on terminal markers, so an in-flight run's entries are never swept out from under a resume. This is a deliberate alternative to a verify-before-delete / boundary-file guard: push the safety to the readers, not the deleter. The queue core (reaper, done/dead/scheduled cleanup) is different: those delete inside SlateDB transactions with verify-before-delete, and the settlement path is fenced by `Error::ClaimLost`. Locked by `idempotent_resubmit_after_result_swept_reruns` (taquba-jobs) and `sweeper_keeps_memos_of_runs_without_a_terminal_marker` (taquba-workflow).
- `taquba-bulk` maps each input item to one `taquba-workflow` run with a single `Succeed` step that runs the user's `Pipeline`. The pipeline's logical steps are `BulkCtx::memoized` keys inside that step, not workflow `Continue` steps; the runner never emits `Continue`. The `Pipeline` trait hides the workflow backend, so optimising the single-step path later stays non-breaking.
- Keep each crate's `lib.rs` top-level `//!` docstring and its `README.md` in content parity: anything substantive (new sections, design notes, semantics callouts) lands in both. Format may differ (lib.rs uses intra-doc `[Foo]` links and `#`-hidden doctest lines; README uses URL links and full `#[tokio::main]` blocks so code is copy-pasteable).
- When adding or changing a `taquba-bencher` bench, update its `README.md` entry in the same commit: the catalog table row, a running example, and the output-format section (plus any new env var worth surfacing).
- Keep docstrings about the code, not the conversation: state what a type or function is and any non-obvious behaviour or invariant; omit rationale that only makes sense for the change that introduced it (call sites, design history, debate). Where the non-obvious *why* matters, put a short note here in AGENTS.md rather than in a docstring that will drift.
- Write doc comments in formal, precise language; avoid colloquial wording.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Workspace: `taquba` core, plus `taquba-cron` and `taquba-webhooks` built on it. See each crate's README for surface-level usage.

## Build / test

Tests live inline in `mod tests`; there is no `tests/` directory.

The `taquba` crate's `aws` / `gcp` / `azure` features are mutually exclusive; pick one for cloud builds.

## Architectural invariants

These constrain almost every design decision; violating them breaks correctness.

- **Single-process, single-writer.** SlateDB allows only one writer per store, so all producers and workers for a given `Queue` must live in the same process and share one `Arc<Queue>`. Do not propose multi-node worker fleets or out-of-process producers; that is explicitly out of scope.
- **At-least-once delivery.** Workers must be idempotent. There is no exactly-once mode and there will not be one.
- **Durability is per-transition.** Every state change is a SlateDB write. When adding state, design the key layout first.
- **Pre-1.0:** minor bumps may break source compat *and* on-disk layout; patch bumps preserve both. Anything that changes a key prefix or serialized record layout is a minor-version change.

## Misc

- Key-prefix convention (see helpers at the top of `taquba/src/queue.rs`): put the field you scan by first. `claimed:` and `scheduled:` keys lead with a zero-padded timestamp (before the queue name) so the reaper/scheduler do one *global* prefix scan across all queues with early exit. Follow the same layout when adding a new prefix.
- Worker errors: returning a `PermanentFailure` dead-letters the job immediately; any other error nacks and retries with exponential backoff up to `QueueConfig::max_attempts`.

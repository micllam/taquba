# AGENTS.md: taquba

Each rule identifies the code it applies to, whose docs describe the mechanism.

## Records, keys and reads

- **Record encoding** (`job.rs`). Do not re-propose a positional layout or short
  `#[serde(rename)]` names.
- **Key spaces** (`keys.rs`). A new key space extends
  `key_spaces_do_not_overlap`.
- **Reader** (`reader.rs`). Do not add `wait_for` or a cross-process control
  convenience to `QueueReader`.
- **Paged reads.** A new paged read of the KV namespace or of a job status uses
  `QueueView::kv_entries` or `QueueView::jobs`, never a custom cursor loop.
- **Scan read-ahead** (`reaper.rs::sweep_scan_options`). Do not extend
  read-ahead to another scan without an access-pattern argument.

## Claims and leases

- **Claim-only delivery.** A job reaches a worker only through a claim
  transaction. See [ADR-0003](../docs/decisions/0003-claim-only-delivery.md).
- **Claim fencing** (`txn::take_claim`). Do not fence on key identity, and do
  not reuse a claim id across a reap.
- **Claim id.** Do not rename it to `token`, and do not add a public accessor.
- **Leases** (`lease.rs`). Do not add a flag that skips the requeue at open, a
  shortening or absolute-set renewal method, an automatic renewer or a
  `max_lease_lifetime`.
- **Lease registry** (`lease_registry.rs`). Per-claim process state goes in the
  registry entry. Do not add a second map with the job id as its key.
- **Claim scan.** Do not remove `ScanOptions::with_cache_blocks(true)` from the
  claim scan, and do not add an in-memory index of pending keys.
- **Claim by id.** Do not add a fused enqueue-and-claim.

## Settlement

- **Failure effects** (`effects.rs`). Do not add an effects field to
  `PermanentFailure`, a transient-to-permanent conversion in a runtime or a
  per-queue `DeadLetterHook`.
- **Claim ends** (`txn.rs`). A new claim-ending path goes through
  `txn::stage_claim_end` and `QueueCore::finish_claim_end`. `txn::retry` is the
  one retry loop. Do not re-propose a body that borrows the transaction.
- **Spawned loops.** Do not hold a loop run as a Tokio task through a token and
  `JoinHandle` pair. Use `WorkerHandle`.

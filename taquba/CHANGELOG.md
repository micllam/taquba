# Changelog

All notable changes to the `taquba` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `LeaseHandle`: the lease capability passed to `Worker::process`.
  `LeaseHandle::ensure_at_least(remaining)` extends the lease when it
  would end sooner than `remaining` from now (plus an internal
  settlement margin), never shortens it, and fails with
  `Error::ClaimLost` once the claim has ended. `LeaseHandle::detached`
  builds a no-op handle so worker types stay constructible in unit
  tests.
- `Queue::lease_expiry`: the current lease expiry of a claimed job, read
  synchronously from the in-memory lease state. Returns `None` when this
  process holds no live lease for the job.
- `Error::CancelRequested`: `Queue::renew_lease` and
  `LeaseHandle::ensure_at_least` refuse renewal once `Queue::cancel` has
  requested cancellation of the job, leaving the lease to expire. The
  claim is still held and settleable, so a delivery that observes its
  cancellation token settles as usual.
- Metrics: the new `taquba_lease_renewals_total{queue}` counter counts
  lease renewals, incremented by `Queue::renew_lease` and by a
  `LeaseHandle::ensure_at_least` call that extends the lease (`metrics`
  feature).

### Changed

- **Breaking (on-disk):** a claimed job's record now lives under a
  `Claimed` key that is stable for the life of the claim, and the lease
  itself (current expiry and claim token) is in-memory process state
  rather than stored. A lease renewal is a synchronous memory operation
  with no durable write. Drain a store of claimed jobs before upgrading:
  jobs left claimed under the old layout are not migrated.
- **Breaking (behaviour):** every claimed job found when the queue is
  opened is re-queued immediately (or dead-lettered if out of attempts),
  rather than waiting out its recorded lease: a claim present at open
  belongs to a process that no longer holds the store. If that process
  is in fact still running (a fenced writer, for example), its side
  effects can overlap the requeued job's as soon as the store opens.
  The requeue appends the new `AttemptOutcome::Interrupted` history
  entry; the next claim consumes an attempt as usual, so `max_attempts`
  still bounds a crash-looping job.
- **Breaking (source):** `Queue::renew_lease` is now synchronous (no
  longer `async`), since renewal writes nothing durable.
- **Breaking (source):** the `Queue::claim*` calls return a new `Claim`
  type instead of `JobRecord`, and `Queue::ack`, `Queue::ack_with`,
  `Queue::nack`, `Queue::dead_letter` and `Queue::renew_lease` take one by
  shared reference. `Claim` dereferences to the claimed record, so
  `claim.payload` and `claim.id` read as before; `Claim::job` borrows the
  record and `Claim::into_job` takes it by value. A claim survives a lease
  renewal and settles afterwards; a claim held across a reaper requeue is
  still rejected with `Error::ClaimLost`. Inside a `Worker`, renewal goes
  through the `LeaseHandle` instead; `Queue::renew_lease` serves callers
  that call `claim`/`claim_batch` directly.
- **Breaking (source):** `Worker::process` and `Worker::process_with_effects`
  take `&JobRecord` plus `&LeaseHandle`. The claim never reaches user
  code: the worker loop keeps it and settles the job when `process`
  returns, and the handle extends the lease without being able to
  settle, so a handler cannot settle its own job mid-execution even if
  it holds the queue.
- `Queue::get_job` and `Queue::list_jobs` return `JobRecord`, which the
  settlement calls no longer accept, so settling a record without holding
  its claim is a compile error rather than `Error::InvalidState` at run
  time.
- **`Queue::renew_lease` returns the new expiry** (epoch milliseconds)
  instead of updating the record in place.
- **Breaking (behaviour):** `Queue::list_jobs` with `JobStatus::Claimed`
  returns jobs in enqueue order rather than lease-expiry order, and scans
  only the requested queue. Lease-expiry order is not stable under renewal.
- **Breaking (source):** `JobRecord::lease_expires_at` is removed; the
  lease expiry is process state, read with `Queue::lease_expiry`.

### Fixed

- A `Queue::cancel` request persisted while the job was claimed is no
  longer lost when the delivery settles with `Queue::nack` or
  `Queue::dead_letter`: the settlement bases the record it writes on the
  record stored at settlement time rather than the claim's copy.

## [0.10.0] - 2026-08-07

### Added

- `Queue::wake_scheduled`: move a scheduled job to pending before its
  `run_at`, optionally attaching bytes that the worker observes on delivery
  via the new `JobRecord::wake_payload` field. The wake stamps the new
  `JobRecord::woken_at` field, so an early wake is distinguishable from
  ordinary promotion at `run_at` regardless of whether bytes were attached.
  The targeted counterpart of the scheduler's due-job promotion; returns a
  `WakeOutcome`.
- `Queue::kv_put`: standalone durable write to the user KV namespace, for
  coordination state whose lifecycle is not tied to a single queue
  transition. Writes coupled to a queue transition should continue to use
  `enqueue_with_kv` / `ack_with`.
- `Queue::kv_compare_delete`: delete a user KV entry only if it still holds
  the expected value, so a concurrently replaced value is never deleted by
  mistake.
- Automatic payload offload: payloads larger than
  `OpenOptions::payload_offload_threshold` (default 256 KiB, the new
  `DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD`) are written once as objects in a
  payload object store instead of inline in the job record, so state
  transitions no longer rewrite large payloads. The record stores the new
  `JobRecord::payload_ref` field; claims and job reads fetch the object
  transparently, and the object is deleted when the record leaves the queue.
  The payload store defaults to `"{path}-payloads"` in the queue's own
  object store and is separately configurable via
  `OpenOptions::payload_store` and `OpenOptions::payload_path`. New error
  variants `Error::PayloadStore` and `Error::PayloadMissing`. Setting the
  threshold to `None` disables offloading.
- `QueueStats` implements `Serialize` and `Deserialize`, so stats snapshots
  can be serialized directly (for example by a JSON admin endpoint) without
  a caller-defined intermediate type.
- `Queue::list_jobs`: page through one queue's jobs in one lifecycle state,
  returning the new `JobPage` (jobs plus an opaque resume cursor). Jobs are
  returned in the state's scan order: pending in claim order, scheduled by
  `run_at`, claimed by lease expiry, done by completion time and dead in
  enqueue order. Complements `Queue::dead_jobs`, which remains the
  ID-cursor listing of the dead-letter set.
- Per-attempt history: every settlement of a claim (ack on a queue with
  `keep_done_jobs`, nack, dead-letter, lease expiry) appends one entry to a
  durable per-job history, read back via the new
  `Queue::attempt_history` as a `Vec<JobAttempt>` (attempt number, claim
  and settlement times, an `AttemptOutcome` and the reported error), so a
  job that failed multiple times exposes every error rather than only
  `last_error`. `Queue::requeue_dead_job` appends a `Requeued` marker and
  keeps prior entries. The history is written through a merge operator
  (one write per settlement, no read-modify-write) and is removed in the
  same transaction that removes the job's last record.
- `Queue::kv_compare_put`: write a user KV entry only if it still holds an
  expected value (`Some`) or is still absent (`None`), in one transaction.
  The read-modify-write primitive for the namespace; concurrent updates of
  one entry lose no writes.
- `Queue::kv_scan`: paged listing of the user KV namespace under a key
  prefix, in ascending key order with an opaque resume cursor (returned as
  the new `KvPage`). The enumeration and export primitive for the
  namespace; internal key spaces are never visible in the results.
- `Queue::next_job_id`: take a job id without enqueuing, for callers that
  need it before the enqueue commits and pass it back as
  `EnqueueOptions::id_override`. Ids come from the queue's own generator,
  so an override taken this way preserves claim order where an
  independently generated one need not.

### Changed

- **Breaking (on-disk):** internal keys use a binary encoding. Every key is
  `[tag, version, fields...]`: a one-byte key-space tag, a one-byte format
  version and order-preserving binary fields (big-endian timestamps and
  priorities, length-prefixed queue names), replacing the string prefixes
  and zero-padded ASCII fields. Queue names are now limited to
  `MAX_QUEUE_NAME_LEN` (255) bytes; longer names return the new
  `Error::InvalidQueueName`.
- A job id's timestamp component is taken from the queue's `Clock` rather
  than from the system clock. When that clock repeats or goes backwards
  the id keeps the preceding id's timestamp and increments its random
  component, so ids still ascend and a timestamp never moves backwards.
  Under the default clock the timestamp is the system time in
  milliseconds, as before.
- Upgraded the SlateDB dependency to 0.15.0 (from 0.13.1).
- The claim scan opts in to block caching, which SlateDB leaves off for
  scans. Without it a claim re-read the same block from object storage
  every time, once the compacted level held a sorted run the scan had to
  consult, so an unbatched queue was limited to one claim per
  object-store read. Claim throughput now holds across that transition.

### Removed

- **Breaking:** `Queue::wait_for_jobs`, the queue-agnostic wait. Use
  the queue-scoped `Queue::wait_for_jobs_on`, which wakes one waiter
  per inserted job; to wait on several queues at once, `select!` over
  one call per queue. The internal queue-agnostic notify channel is
  removed with it.
- **Breaking:** `CancelOutcome::acted`. Match on the enum instead:
  `!matches!(outcome, CancelOutcome::NotFound)`.
- **Breaking:** `MockClock::set`. Construct with `MockClock::new` and
  move time forward with `MockClock::advance`; tests should not move a
  shared clock backwards.

## [0.9.0] - 2026-06-23

### Added

- Optional `metrics` feature emitting queue health metrics through the
  [`metrics`](https://docs.rs/metrics) facade: counters
  `taquba_jobs_{enqueued,claimed,completed,nacked,dead_lettered,reaped}_total`
  and latency histograms `taquba_{enqueue,claim,ack}_duration_seconds`, all
  labelled by queue. No exporter is pulled in; the host process installs a
  recorder (for example Prometheus or an OTLP bridge), and emission is a
  no-op until one is installed. With the feature off, all emission compiles
  to nothing.
- `OpenOptions::metrics_sample_interval` (defaults to `None`): when set and
  the `metrics` feature is on, a background sampler periodically emits the
  depth gauges `taquba_pending_jobs` / `taquba_claimed_jobs` and
  `taquba_oldest_pending_age_seconds` (age of the job at the front of the
  claim order), per queue.
- With the `metrics` feature, SlateDB's own storage metrics (write, flush,
  compaction, and cache, with dot-separated names such as
  `slatedb.db.write_ops`) are forwarded into the same `metrics` recorder, so
  storage and queue metrics appear together in one scrape.

### Fixed

- Job ids now increase with enqueue order even when generated inside one
  millisecond, so jobs of equal priority are claimed in enqueue order as
  documented. Ids come from one monotonic ULID generator per store rather
  than from `Ulid::new()`, which ordered ids of the same millisecond
  arbitrarily; pending keys sort by id within a priority, so those jobs
  could be claimed out of order, which concurrent producers made reachable
  in ordinary use. `enqueue_batch` was already monotonic within one call
  and is now monotonic across calls as well. Ids remain ULIDs and the key
  layout is unchanged; a caller-supplied `EnqueueOptions::id_override`
  still determines its own ordering.
- `Queue::claim` and `Queue::claim_batch` now bound the cursor-resumed
  scan to the queue's `pending:` prefix instead of scanning unbounded to
  the end of the keyspace. When a claim resumed from the recorded cursor
  bound, the step that detects a drained queue continued past the last
  live `pending:` key into the remainder of the keyspace before
  terminating, so on a shallow or near-empty queue nearly every claim
  incurred that traversal and claim latency increased as tombstones
  accumulated between compactions. The scan now ends at the prefix upper
  bound, within which every live pending key sorts. The front prefix scan
  taken on cold start or restart was already bounded and is unchanged. No
  API or on-disk change.

## [0.8.0] - 2026-06-15

### Added

- `Queue::claim_batch` claims up to `max_jobs` pending jobs in one
  transaction, sharing one claim-lock hold and one commit across the
  batch. Jobs are returned in claim order and share one lease.
  `Queue::claim` is now a batch of one.
- `Queue::wait_for_jobs_on` blocks until a job becomes claimable on
  one queue. Unlike `Queue::wait_for_jobs`, the wakeup is queue-scoped
  and delivered to one waiter per inserted job.
- `Queue::ack_with` acknowledges a job and applies a set of effects in
  the same transaction: follow-up enqueues (`AckEffects::enqueues`,
  honouring `run_at`, `dedup_key`, `priority`, and `id_override` per
  request) and caller KV writes and deletes. Either the ack and every
  effect land together or nothing does; when the claim is gone the
  call fails with `ClaimLost` and applies nothing, so a chained job
  exists only if the settlement that created it won. `Queue::ack` is
  now `ack_with` with empty effects.
- `Error::ClaimLost`: returned by `ack`, `ack_with`, `nack`,
  `dead_letter`, and `renew_lease` when the record's claim is no
  longer present (the lease expired and the reaper requeued the job,
  or the record is a stale copy from before a lease renewal rotated
  the claimed key). These cases previously returned the catch-all
  `Error::InvalidState`, which remains for genuine misuse (a record
  missing `lease_expires_at`, `requeue_dead_job` on a non-dead
  record).
- `Worker::process_with_effects`: workers can return `AckEffects`
  from processing, which `run_worker` and `run_worker_concurrent`
  apply atomically with the job's acknowledgement via
  `Queue::ack_with`. `process` and `process_with_effects` default to
  each other; implement exactly one. Existing `Worker`
  implementations are unaffected.
- `Queue::close` persists each queue's claim-scan state (scan bound
  and emptiness marker) under a new `cursor:` key prefix; the next
  open restores the in-memory state from it and deletes the record. The
  first claim after a clean restart resumes at the recorded bound
  instead of re-scanning the tombstone band left by previously claimed
  jobs, whose cost grows with the band and the store's latency. After
  a crash the record is absent and the first claim falls back to the
  front prefix scan as before.

### Changed

- `run_worker` no longer exits when settling a job fails. Settlement
  failures (including `ClaimLost` when a job outlives its lease and
  the reaper requeues it) are logged and the loop continues, matching
  `run_worker_concurrent`; the redelivered attempt settles the job.
  Claim-path errors still terminate both loops. Both loops log a lost
  claim distinctly from other settlement failures.
- `run_worker_concurrent` claims jobs in batches sized to its free
  capacity via `Queue::claim_batch`, costing one claim transaction
  per batch instead of per job under a backlog. Jobs are still
  processed concurrently and acked individually.
- `Queue::claim_with_wait` and the `run_worker` / `run_worker_concurrent`
  loops wait on a queue-scoped wakeup that wakes one waiter per
  inserted job, instead of the process-wide notification that woke
  every waiting worker on every insert. A pool of idle workers no
  longer contends on the claim path when a single job arrives, and a
  worker claiming a job passes one wakeup on so a backlog keeps waking
  further workers. `Queue::claim_with_wait` now also keeps waiting out
  its full `max_wait` after losing a claim race instead of returning
  `None` early.
- `Queue::claim` commits without awaiting WAL durability. Claims
  serialise per queue through the claim lock, which excluded them from
  WAL group commit: the lock holder awaited its flush before the next
  claim could start, making the flush round trip the queue's claim
  throughput ceiling.
  Losing an unflushed claim in a crash leaves the job pending, so it
  is redelivered immediately on recovery instead of after its lease
  expires; at-least-once delivery is unaffected, and a settled job's
  claim is always durable because later durable commits flush
  preceding WAL entries.
- The scheduler promotes due jobs without awaiting WAL durability,
  for the same reasons and with the same crash behaviour as the
  reaper change below: a lost promotion leaves the scheduled key in
  place with its `run_at` in the past, and the next tick re-promotes
  it. A backlog of due jobs (a retry-backoff wave, or scheduled jobs
  accumulated during downtime) no longer promotes at one job per
  flush interval.
- The reaper requeues and dead-letters expired claims without awaiting
  WAL durability. Each expired claim is processed in its own
  transaction, and awaiting the flush serialised the sweep at one job
  per flush interval (about ten per second at the default 100 ms
  flush). A commit lost in a crash leaves the expired claim in place
  for the next sweep, which re-processes it without consuming an
  attempt, and later durable commits flush preceding WAL entries, so
  a settled job's requeue is durable by ordering.
- The done and dead-letter retention sweeps delete expired records
  without awaiting WAL durability, for the same reasons as the reaper
  and scheduler changes above: a delete lost in a crash leaves the
  record in place for the next sweep, whose existence re-check keeps
  the rerun idempotent. With this, no background sweep awaits the
  flush; only caller-driven operations do. A retention backlog no
  longer delays the lease reaping that shares its tick.
- `Queue::claim` tracks per-queue emptiness and a scan bound in
  process memory. Polling an empty queue answers without a storage
  scan or the claim lock, and the pending tombstone band is never
  re-walked from the front while the process stays up; a full prefix
  scan now happens only on cold start or process restart.
- Queue stats counter merges are excluded from transaction conflict
  detection. The merges are commutative, so concurrent job-state
  transitions on the same queue no longer abort and retry each other
  over the shared stats keys.

### Fixed

- A `pending:` insert landing behind the claim cursor while a claim
  was in flight could have its cursor invalidation overwritten by
  that claim's cursor update, hiding the job from cursor scans until
  the queue next drained. The scan bound now moves back to include
  such inserts, and a claim drops its bound advance when the bound
  moved while it ran.
- A `pending:` key could be hidden from claims indefinitely when its
  insert committed while a claim was in flight and the key sorted at
  or below the keys that claim advanced the scan bound past. Job ids
  are generated before the enqueue transaction commits, so commit
  order can invert key order under concurrent producers, and a
  requeue (reaper or nack) restores a job at its original key. The
  next claim then recorded emptiness at a valid epoch and the queue
  answered `None` while live jobs were pending. Bound advances now
  clamp to the smallest key recorded since the bound was observed,
  including when no bound exists yet (the first claim after a
  process restart) and when the key equals the claimed one (the
  claimed job requeued after its lease expired within the claim).
- Duplicate `EnqueueOptions::id_override` values are now rejected
  transactionally with `Error::DuplicateJobId` instead of overwriting
  `jobindex:{id}` and leaving older queue-state records behind.
- `Queue::ack`, `Queue::nack`, `Queue::dead_letter`, and
  `Queue::renew_lease` now check that the expected `claimed:` record
  still exists before settling a job. A worker finishing after its
  lease was reaped now gets `Error::ClaimLost` instead of being
  able to ack, retry, dead-letter, renew, or corrupt stats from a
  stale `JobRecord`.
- `Queue::nack` and `Queue::renew_lease` now retry on transaction
  conflict like `Queue::ack` and `Queue::dead_letter` already did.
  A reaper committing the expired-lease delete concurrently with a
  late settlement is now retried (and resolves to `Error::ClaimLost`
  on the next attempt) instead of surfacing a raw SlateDB transaction
  error to the caller.
- `Queue::requeue_dead_job` now checks that the dead-letter record
  still exists before reviving it. Requeueing a stale record after
  dead-letter retention swept it now returns `Error::JobNotFound`
  instead of recreating the job and corrupting queue stats.

## [0.7.0] - 2026-05-28

### Added

- `EnqueueOptions::id_override` lets callers supply the job id instead
  of receiving a generated ULID. Useful when the id must be known before
  the enqueue returns. Ids are validated at the API boundary (1-128 bytes
  of `[A-Za-z0-9_-]`) and bad inputs return the new
  `Error::InvalidId { id, reason }` variant. Callers should prefer
  ULID-shaped ids when FIFO-within-priority claim order matters:
  `pending`/`scheduled` keys end with the id, so claim order follows
  id sort.
- `Queue::clock()` accessor returns the `Arc<dyn Clock>` the queue
  was opened with (or the default `SystemClock`). Lets downstream
  crates share the queue's time source for their own timestamp work
  so virtualising time with `MockClock` advances the whole stack
  in lockstep.
- `OpenOptions::flush_interval: Option<Duration>` exposes SlateDB's
  WAL flush interval. `None` keeps SlateDB's own default (100ms).
  Every taquba state transition (`enqueue`, `claim`, `ack`, `nack`,
  `dead_letter`) blocks on `txn.commit()` which waits for the next
  flush tick, so this value is the lower bound on per-operation
  latency.

### Changed

- **Breaking on-disk layout:** the `done:` keyspace is reordered from
  `done:{queue}:{id}` to `done:{completed_at:020}:{queue}:{id}`,
  mirroring the existing time-first layout of `claimed:` and
  `scheduled:`. The retention sweep can now early-exit on the first
  unexpired record instead of walking the full prefix. Public API is
  unchanged; in-flight runs from prior versions must be drained
  before upgrading because the old keys will not be observed by the
  reaper.
- `Queue::claim` (and therefore `claim_next` / `claim_with_wait`)
  serialises same-queue claim attempts through an in-process
  `tokio::sync::Mutex`. Same-queue attempts no longer rely on
  SlateDB's transaction-conflict retry to resolve which worker
  takes the head of `pending:`. The lock is per-queue, so different
  queues' claims still run in parallel. Per-claim wall-clock latency
  under high single-queue concurrency drops from seconds to roughly
  one commit interval. Public API unchanged.
- `Queue::claim` now maintains an in-memory per-queue cursor that
  records the most recently claimed `pending:` key, and starts the
  next claim's scan from immediately after it. This skips the
  tombstone band left by previously claimed (and deleted) `pending:`
  entries that the SlateDB iterator would otherwise walk. The
  cursor is invalidated whenever a `pending:` key is written at or
  before it (nack-requeue, dead-job requeue, reaper-requeue,
  scheduler promotion, and any enqueue at a lower-numbered
  priority); when this happens the next claim falls back to a full
  prefix scan. The cursor is not persisted: on process restart the
  first claim falls back to the prefix scan and re-warms naturally.
  Public API unchanged.
- Bumped minimum `slatedb` version from 0.13 to 0.13.1.

### Fixed

- `enqueue_with`'s non-dedup path (`write_new`) now retries on
  transaction conflict, matching the dedup path (`write_unique`),
  `enqueue_with_kv`, `ack`, `dead_letter`, and every other write path
  in the crate. Previously a conflict during a non-dedup enqueue would
  surface as `Error::Storage` to the caller; under normal contention
  this would have manifested as spurious enqueue failures that a retry
  could resolve.

## [0.6.0] - 2026-05-20

### Added

- `Error::is_permanent()`: classifies each variant as transient or
  permanent so downstream crates can decide whether to retry or
  fast-fail. `Serialization`, `Deserialization`, `JobNotFound`,
  `InvalidState`, and `KvValueTooLarge` are permanent; `Storage(_)` is
  conservatively treated as transient.
- New `Clock` trait + `SystemClock` (default) + `MockClock` types for
  virtualising taquba's time source. Every state-transition timestamp
  (`enqueued_at`, `completed_at`, `failed_at`, `lease_expires_at`) and
  every time-based comparison (retention cutoffs, scheduled-job
  promotion, lease-expiry detection) reads through
  `OpenOptions::clock`. Production callers leave the default; tests can
  pass a `MockClock` and call `MockClock::advance(Duration)` to move
  time deterministically without `std::thread::sleep`.
- Per-queue retention via new `QueueConfig::keep_done_jobs` and
  `QueueConfig::dead_retention` fields. Different queues sharing one
  `Queue` instance can now pick different retention windows (e.g.
  short for ephemeral deliveries, longer for workflow runs).
  `Queue::ack` and the background reaper consult the per-queue value
  via the new `Queue::queue_keep_done_jobs` /
  `Queue::queue_dead_retention` resolvers.

### Changed

- Updated `slatedb` dependency from 0.12 to 0.13. `taquba`'s public API is
  unchanged.
- **Breaking:** `OpenOptions` gained a `clock: Arc<dyn Clock>` field.
  Code using `..OpenOptions::default()` is unaffected; explicit struct
  literals must set it (`clock: Arc::new(SystemClock)` reproduces the
  prior behaviour).
- **Breaking:** `keep_done_jobs` and `dead_retention` have moved from
  `OpenOptions` to `QueueConfig`. Migration: set them on
  `OpenOptions::default_queue_config` for an instance-wide default, or
  per queue in `OpenOptions::queue_configs`. The previous defaults
  (`None` for `keep_done_jobs`, `Some(7 days)` for `dead_retention`)
  now live on `QueueConfig::default()` and apply unchanged when
  unspecified.
### Removed

- **Breaking:** `Queue::sweep_done_now(retention)` and
  `Queue::sweep_dead_now(retention)` removed.

### Fixed

- `Queue::ack` and `Queue::dead_letter` now retry on transaction conflict,
  matching every other write path in the crate. Previously, a conflict
  during ack or dead-letter would surface the error to the caller and
  leave the job in `Claimed` until the lease expired and the reaper
  requeued or dead-lettered it, adding up to
  `lease_duration + reaper_interval` of wall-clock latency to the job's
  terminal state.

## [0.5.0] - 2026-05-15

### Added

- `Queue::enqueue_with_kv(queue, payload, opts, kv_writes)` enqueues a
  job *and* applies a set of writes to a caller-owned KV namespace in
  a single SlateDB transaction. Returns the new
  `EnqueueResult::{New, AlreadyEnqueued}` enum: on a `dedup_key` hit,
  the existing job's id is returned and **no KV writes are applied**.
  Enables downstream crates to keep durable coordination state
  (status markers, dedup records, pointers to externally-stored blobs)
  consistent with the queue across crashes.
- `Queue::kv_get(key)` and `Queue::kv_delete(key)` for reading and
  cleanup of entries in the user KV namespace. There is no standalone
  `kv_put` by design: the namespace mutates only as a side effect of
  queue operations.
- `MAX_KV_VALUE_SIZE` (256 KiB) constant, enforced at the API
  boundary. Values exceeding the cap return the new
  `Error::KvValueTooLarge { size, max }` variant.
- Reserved `usr:` key prefix for the user KV namespace. Caller keys
  are internally scoped under this prefix so they cannot collide
  with Taquba's internal layout (`pending:`, `claimed:`, `dead:`,
  `done:`, `scheduled:`, `jobindex:`, `dedup:`, `stats:`).

## [0.4.0] - 2026-05-14

### Added

- Cooperative cancellation of `Claimed` jobs. `Queue::cancel` now
  handles every lifecycle state and returns a `CancelOutcome` enum
  (`Removed` | `Requested` | `NotFound`). For `Claimed` jobs it
  persists a new `cancel_requested` flag and fires the in-process
  `CancellationToken` exposed on the new `JobRecord::cancel_token`
  field. Workers receive the token on every `claim*` path and can
  `select!` on it to short-circuit. The persisted flag ensures that if
  the lease expires and the reaper requeues the job, the next claim's
  token starts pre-cancelled. `requeue_dead_job` clears the flag:
  reviving a dead job is an operator decision to give it a fresh start.
- `Queue::wait_for_completion(id, timeout) -> WaitOutcome`. Notify-based:
  every terminal transition in the queue (`ack`, `nack`-to-dead,
  `dead_letter`, `cancel`-Removed, reaper dead-letter) fires a shared
  `Notify` that the call listens on, so there is no per-job polling.
  Returns one of:
  - `WaitOutcome::Completed(Some(Box<JobRecord>))` when taquba kept a
    terminal record (`Dead` always; `Done` only when `keep_done_jobs`
    is set).
  - `WaitOutcome::Completed(None)` when the job terminated but no
    record was retained (default `ack`, or a `cancel` of a
    Pending/Scheduled job).
  - `WaitOutcome::TimedOut` if `timeout` elapsed first.
  - `WaitOutcome::NotFound` if the job ID was not present at call
    time. With the default config, `Completed(None)` is ambiguous
    between "success" and "cancelled before claim"; see the
    `WaitOutcome` docs for the full retention matrix.

### Changed

- **Breaking:** `Queue::cancel` now returns `Result<CancelOutcome>`
  instead of `Result<bool>`. Migration: existing call sites that
  matched on `true` for "removed from queue" should match on
  `CancelOutcome::Removed`; sites that distinguish "Claimed (worker
  has it)" from "Pending/Scheduled" should match on
  `CancelOutcome::Requested` vs `Removed`. `CancelOutcome::acted()`
  is a `bool` helper covering the previous "any cancellation happened"
  semantics.
- `JobRecord` gained two fields:
  - `cancel_requested: bool` (persisted; defaults to `false` on
    records written by earlier versions, so the on-disk layout is
    backward-compatible).
  - `cancel_token: Option<CancellationToken>` (skipped from serde;
    populated by `claim*`, `None` everywhere else).

  Code that constructs `JobRecord` directly via a struct literal now
  has to set both fields.

## [0.3.0] - 2026-05-06

### Added

- `PermanentFailure` marker error type. Returning it from `Worker::process`
  routes the job to a new `Queue::dead_letter` exit instead of `nack`,
  skipping retry/backoff for failures known to be permanent (e.g. an HTTP
  410 Gone, a malformed input that won't change). `run_worker` and
  `run_worker_concurrent` downcast the worker error and route accordingly.
- `Queue::dead_letter` for moving a claimed job to the dead-letter set
  unconditionally, without bumping `attempts`.

## [0.2.0] - 2026-05-05

### Added

- `headers: HashMap<String, String>` on `JobRecord` and `EnqueueOptions`
  for application-defined per-job metadata (target URLs, signing key ids,
  schedule names, etc.). Serialized only when non-empty.

### Changed

- `Option` fields on `JobRecord` (`claimed_at`, `lease_expires_at`,
  `run_at`, `last_error`, `dedup_key`, `completed_at`, `failed_at`) skip
  serialization when `None`, reducing on-disk size for typical jobs.
  Backwards-compatible with records written by 0.1.0.

## [0.1.0] - 2026-05-01

Initial release.

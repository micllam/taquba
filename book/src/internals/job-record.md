# The job record

`JobRecord` and the states it moves through, in `taquba/src/job.rs`
([source][job]).

## What a job is

A job is the unit a caller enqueues: a payload, the settings the delivery runs
under and the queue's own record of how that delivery has gone. The queue hands
it to a worker at least once, and the worker settles it by acknowledging it,
reporting a failure or letting the delivery lapse.

A job is in exactly one of five states at any time.

- **Pending.** Waiting to be claimed by the next worker.
- **Scheduled.** Waiting for a time: the `run_at` given at enqueue, or the
  backoff window a failed attempt put it in.
- **Claimed.** Held by a worker under a lease.
- **Done.** Acknowledged. The record is kept only on a queue configured to keep
  done jobs; otherwise the ack removes it.
- **Dead.** Out of attempts, or failed permanently. It stays for inspection
  until the dead retention expires.

The moves between them are these:

```text
enqueue      ──▶ Pending      Scheduled instead when run_at is set
Scheduled    ──▶ Pending      scheduler at run_at or an early wake
Pending      ──▶ Claimed      claim, attempts += 1
Claimed      ──▶ Done         ack, when the queue keeps done jobs
Claimed      ──▶ removed      ack, when it does not
Claimed      ──▶ Scheduled    nack, waiting out the retry backoff
Claimed      ──▶ Dead         attempts exhausted or a permanent failure
Claimed      ──▶ Pending      reaper requeue, or requeue at open
Dead         ──▶ Pending      requeue_dead_job
Done, Dead   ──▶ removed      retention sweep
```

Three properties of that lifecycle shape everything else:

- **Delivery is at-least-once.** A claim hands a job to a worker, and the same
  job can be handed out again after a failed attempt or an expired lease, so a
  worker has to be idempotent. Deliveries repeat until the job is acknowledged
  or dead-lettered.
- **A claim is held under a lease.** A background reaper returns a job to
  `Pending` once its lease expires, which is what makes a worker that dies
  mid-delivery recoverable.
- **A failed attempt waits out a backoff.** It sits in the scheduled space
  until an exponentially growing delay passes, bounded by the job's attempt
  limit.

`Scheduled` therefore serves two purposes: the scheduler's promotion is the
retry mechanism as well as the delay mechanism. And a `Claimed` job can return
to `Pending` with no worker settling it, when the reaper finds its lease expired
or the queue requeues it at open.

## What the record holds

A job is stored as a single `JobRecord`, encoded as MessagePack.

The record holds:

- **The caller's data:** `payload` and `headers`.
- **The per-job settings,** resolved at enqueue from `EnqueueOptions` against
  the queue's defaults: `priority`, `max_attempts`, `run_at` for a scheduled
  job and `dedup_key` until the first claim clears it.
- **The delivery record:** `attempts`, `claimed_at`, `last_error` and the
  terminal timestamps `completed_at` and `failed_at`.

Two related pieces of state are kept elsewhere:

- **The live lease.** Its expiry and claim token are process state in the lease
  registry, so they do not appear on any record.
- **The queue's configuration.** `lease_duration`, the retry backoff bounds,
  `keep_done_jobs` and `dead_retention` stay in `QueueConfig` and are read per
  queue at the moment they are needed.

Every optional field is skipped when it is `None` or empty, so a fresh pending
record encodes only the fields that hold information.

Two things the stored value never contains. The lifecycle status is derived
from the key the record is read under, which [Key layout](key-layout.md)
covers. And the payload bytes of an offloaded payload live in the payload
store, described below.

## A large payload moves out of the record

A payload lives in one of two places, decided at enqueue by its size. Below
`OpenOptions::payload_offload_threshold` (256 KiB by default) it is stored in
the record. Above it, the bytes are written to the payload object store and the
record holds the object's name in `payload_ref`.

```text
inline payload

  record ┌────────────────────┐
         │ id, queue, ...     │
         │ payload: the bytes │
         └────────────────────┘

offloaded payload

  record ┌────────────────────┐        payload store
         │ id, queue, ...     │        ┌────────────────────┐
         │ payload_ref: name  │───────▶│ <prefix>/<ulid>    │
         └────────────────────┘        └────────────────────┘
         the payload bytes are not in the record
```

The threshold exists because a record is rewritten on every transition: each
move between key spaces writes it again under its new key. An inline payload is
part of that write, so a large one would be written again at every step of the
job's life, while an offloaded payload is written once and the transitions that
follow move a small record.

`JobRecord::stored_bytes` implements the split. Every record write goes through
it, and it leaves the inline payload out when `payload_ref` is set;
`JobRecord::stored_clone` applies the same exclusion when a record is copied.
Setting the threshold to `None` disables offloading, keeping every payload
inline whatever its size.

A record returned by a claim or a read always has `payload` populated:
`PayloadStore::materialize` fills it from the object when `payload_ref` is set
([payload_store.rs][payload_store]). The ordering rule around the object is
documented there: write it before the transaction that writes the record, and
delete it only after the transaction that removed the record commits.

## What a transition changes

A transition rewrites the whole record, and most of it is copied through
unchanged. These are the fields a transition sets:

- **A claim** stamps `claimed_at`, increments `attempts` and takes
  `dedup_key` off the record.
- **An ack** stamps `completed_at`, on a queue that keeps done jobs.
- **A nack** clears `claimed_at` and records `last_error`.
- **A dead-letter** clears `claimed_at`, records `last_error` and stamps
  `failed_at`.
- **An early wake** stamps `woken_at` and attaches `wake_payload`.
- **A cancel** of a claimed job sets `cancel_requested`.
- **`Queue::requeue_dead_job`** resets `attempts`, `last_error`, `claimed_at`,
  `failed_at` and `cancel_requested`, so the revived job starts again from zero
  attempts.

`attempts` counts claims: every delivery increments it, whether it succeeds,
fails or is interrupted. `JobRecord::is_last_attempt` compares it against
`max_attempts`, which is how the reaper decides between returning an
interrupted job to `Pending` and dead-lettering it ([reaper.rs][reaper]).

Three fields are written once and then deliberately preserved by the
transitions that follow, because their value means something after the
transition that set it:

- **`enqueued_at`** is preserved by `requeue_dead_job`, so a revived job keeps
  its original enqueue time. That is why the dead retention
  sweep ages jobs by `failed_at`, which is stamped afresh at each
  dead-lettering.
- **`woken_at` and `wake_payload`** stay on the record after the wake, so a
  worker sees the early wake on every delivery of that job, redeliveries
  included. `woken_at` marks the wake even when no bytes were attached.
- **`cancel_requested`** stays set, so a job re-claimed after a lease expires
  begins with its cancellation token already fired.

One field is deliberately removed instead. `dedup_key` leaves the record at the
first claim, which frees the key for a new job as soon as this one starts
running. Leaving it on would be unsafe: a later nack would return a pending
record still holding the key, and the next claim would delete a dedup index
that might by then belong to a different job.

[job]: https://github.com/micllam/taquba/blob/master/taquba/src/job.rs
[payload_store]: https://github.com/micllam/taquba/blob/master/taquba/src/payload_store.rs
[reaper]: https://github.com/micllam/taquba/blob/master/taquba/src/reaper.rs

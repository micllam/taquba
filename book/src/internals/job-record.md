# The job record

[`JobRecord`][JobRecord] and the states it moves through, in `taquba/src/job.rs`
([source][job]).

## The lifecycle of a job

A job is the unit a caller enqueues. It consists of a payload, the settings its
deliveries run under and the queue's own record of those deliveries. The queue
assigns it to a worker at least once, and each assignment is a delivery. The
worker settles it with an ack (a report of success), with a nack (a report of
failure) or by letting the delivery lapse.

A job is in exactly one of five states at any time.

- **Pending.** Waiting to be claimed by the next worker.
- **Scheduled.** Waiting for a time: the `run_at` given at enqueue, or the
  backoff window a failed attempt put it in.
- **Claimed.** Held by a worker under a lease, the time bound on a claim.
- **Done.** Acknowledged. The record is kept only on a queue configured to keep
  done jobs. On any other queue the ack removes it.
- **Dead.** Out of attempts, or failed permanently. A job that enters this
  state is dead-lettered. It stays for inspection until the dead retention
  expires.

The moves between them:

```text
                             enqueue
                                │
                 ┌──────────────┴──────────────┐
                 ▼                             ▼
          ┌─▶ Pending ◀─────────────────── Scheduled
          │      │           scheduler,        ▲
          │      │ claim     early wake        │
  reaper, │      │                             │ nack,
  requeue │      ▼                             │ attempts left
  at open └── Claimed ─────────────────────────┘
                 │
                 ├───────────────┐
             ack │               │ nack, attempts exhausted,
                 │               │ or a permanent failure
                 ▼               ▼
               Done            Dead
                 │               │
                 └───────┬───────┘
                         │ retention sweep
                         ▼
                      removed
```

The edges:

- **Enqueue.** A job with `run_at` set enters at `Scheduled`, and every other
  job at `Pending`.
- **Scheduler, early wake.** The scheduler promotes a job at its `run_at`. An
  early wake is a caller's request, through `wake_scheduled`, to promote it
  before its time.
- **Claim.** Increments `attempts`.
- **Nack.** The job is held in `Scheduled` for the retry backoff while attempts
  remain. The scheduler's promotion is therefore the retry mechanism as well
  as the delay mechanism.
- **Ack.** Enters `Done` only on a queue that keeps done jobs, and removes the
  record otherwise.
- **Reaper, requeue at open.** A `Claimed` job returns to `Pending` without a
  settlement when the reaper finds its lease expired, or when the queue
  requeues it at open.

Three properties of the lifecycle:

- **Delivery is at-least-once.** A claim assigns a job to a worker, and the same
  job can be assigned again after a failed attempt or an expired lease. A
  worker must therefore be idempotent. Deliveries repeat until the job is
  acknowledged or dead-lettered.
- **A claim is held under a lease.** The reaper returns a job to `Pending` once
  its lease expires. [Claiming and settling](claiming.md) describes the claim
  and the lease.
- **A failed attempt waits out a backoff.** It is held in `Scheduled` until an
  exponentially growing delay passes, bounded by the job's attempt limit.

## The stored fields

A job is stored as a single `JobRecord`, encoded as a MessagePack map with
field names. MessagePack is compact, and the named fields keep a stored record
readable when the set of fields changes. The two byte fields, `payload` and
`wake_payload`, are binary strings, so the record stores their bytes as they
are.

The record contains:

- **The caller's data:** `payload` and `headers`.
- **The per-job settings,** resolved at enqueue from
  [`EnqueueOptions`][EnqueueOptions] against the queue's defaults: `priority`,
  `max_attempts`, `run_at` for a scheduled job and `dedup_key`. The dedup key
  is a caller key that deduplicates enqueues while the job waits, and the first
  claim clears it.
- **The delivery record:** `attempts`, `claimed_at`, `last_error` and the
  terminal timestamps `completed_at` and `failed_at`.

`attempts` counts claims: every delivery increments it, whether it succeeds,
fails or is interrupted. `JobRecord::is_last_attempt` compares it against
`max_attempts`. That comparison is how the reaper decides between returning an
interrupted job to `Pending` and dead-lettering it ([reaper.rs][reaper]).

Every optional field is skipped when it is `None` or empty, so a fresh pending
record encodes only the fields that contain information.

## State kept outside the record

Four pieces of state describe a job and are not in its record:

- **The live lease.** Its expiry and claim id are process state in the lease
  registry, so they do not appear on any record.
- **The queue's configuration.** `lease_duration`, the retry backoff bounds,
  `keep_done_jobs` and `dead_retention` stay in [`QueueConfig`][QueueConfig].
  The queue reads them per queue at the moment they are needed.
- **The lifecycle status.** It is derived from the record's key, which
  [Key layout](key-layout.md#the-status-comes-from-the-key) covers.
- **An offloaded payload.** Its bytes are in the payload store, described in
  the section that follows.

## Payload offload

A payload is stored in one of two places, decided at enqueue by its size. Below
[`OpenOptions::payload_offload_threshold`][threshold] (256 KiB by default) it
is stored in the record. Above it, the bytes are written to the payload object
store and the record contains the object's name in `payload_ref`.

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

The threshold exists because a record is rewritten on every transition. Each
move between key spaces writes it again with its new key. An inline payload is
part of that write, so a large one is written again at every step of the job's
life. An offloaded payload is written once, and the transitions that follow
move a small record.

`JobRecord::stored_bytes` implements the split. Every record write goes through
it, and it leaves the inline payload out when `payload_ref` is set.
`JobRecord::stored_clone` applies the same exclusion when a record is copied. A
threshold of `None` disables offloading, and every payload then stays inline
regardless of its size.

A record returned by a claim or by `get_job` has `payload` populated:
`PayloadStore::materialize` fills it from the object when `payload_ref` is set
([payload_store.rs][payload_store]). A listing and `job_record` return the
record as stored, without the bytes of an offloaded payload. The object exists
for the whole life of the record. It is written before the transaction that
writes the record, and deleted only after the transaction that removes the
record commits.

## The fields a transition sets

A transition rewrites the whole record, and most of it is copied through
unchanged. These are the fields a transition sets:

- **A claim** sets `claimed_at`, increments `attempts` and removes `dedup_key`
  from the record.
- **An ack** sets `completed_at`, on a queue that keeps done jobs.
- **A nack** clears `claimed_at` and records `last_error`.
- **A dead-letter** clears `claimed_at`, records `last_error` and sets
  `failed_at`.
- **An early wake** sets `woken_at` and attaches `wake_payload`.
- **A cancel** of a claimed job sets `cancel_requested`.
- **[`Queue::requeue_dead_job`][requeue]** resets `attempts`, `last_error`,
  `claimed_at`, `failed_at` and `cancel_requested`, so the revived job starts
  again from zero attempts.

Three fields are written once and preserved by the transitions that follow,
because their value means something after the transition that set it:

- **`enqueued_at`** is preserved by `requeue_dead_job`, so a revived job keeps
  its original enqueue time. That is why the dead retention sweep ages jobs by
  `failed_at`, which is set afresh at each dead-lettering.
- **`woken_at` and `wake_payload`** stay on the record after the wake, so a
  worker sees the early wake on every delivery of that job, redeliveries
  included. `woken_at` marks the wake even when no bytes were attached.
- **`cancel_requested`** stays set, so a job re-claimed after a lease expires
  begins with its cancellation token already fired.

One field is removed on purpose. `dedup_key` leaves the record at the first
claim, which frees the key for a new job as soon as this one starts running.
[The dedup key](claiming.md#the-dedup-key) describes the index and why the
record drops the key.

[JobRecord]: https://docs.rs/taquba/latest/taquba/struct.JobRecord.html
[EnqueueOptions]: https://docs.rs/taquba/latest/taquba/struct.EnqueueOptions.html
[QueueConfig]: https://docs.rs/taquba/latest/taquba/struct.QueueConfig.html
[threshold]: https://docs.rs/taquba/latest/taquba/struct.OpenOptions.html#structfield.payload_offload_threshold
[requeue]: https://docs.rs/taquba/latest/taquba/struct.Queue.html#method.requeue_dead_job
[job]: https://github.com/micllam/taquba/blob/master/taquba/src/job.rs
[payload_store]: https://github.com/micllam/taquba/blob/master/taquba/src/payload_store.rs
[reaper]: https://github.com/micllam/taquba/blob/master/taquba/src/reaper.rs

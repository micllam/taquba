# Leases and cancellation

This chapter describes what a worker receives when it claims a job, and the
in-memory state, separate from the store, that decides which claim of a job is
current,
when it expires and how it is cancelled. The code is in `taquba/src/job.rs`
([source][job]), `taquba/src/lease_registry.rs` ([source][lease_registry]) and
`taquba/src/lease.rs` ([source][lease]).

## The `Claim` value

A worker calls [`Queue::claim`][claim-method] to claim one job and receives a
[`Claim`][claim-struct], or [`claim_batch`][claim_batch] to claim several and
receives one `Claim` per job.

A `Claim` is the worker's side of the delivery. It contains three things:

- The job record, as it was at the moment of the claim. The `Claim`
  dereferences to it, so a caller that only reads the payload and the headers
  treats it as a `JobRecord`.
- The claim id, which identifies this one delivery.
- The cancellation token, through which the queue cancels the processing of
  a claimed job.

The claim id and the cancellation token are described in turn.

The claim id is 64 random bits that identify this one delivery. The queue
generates it at claim time and keeps a copy in memory, and at settlement it
compares the two copies. A match proves that the caller's claim is still the
live one. [The lease registry](#the-lease-registry) describes where the
queue's copy is kept and how the comparison works.

Every settlement takes a `&Claim`, and so does [`renew_lease`][renew_lease].
Only `Queue::claim` and `claim_batch` create a `Claim`, and the claim id inside it
is private. A worker can therefore settle only the claims it owns. A settlement
that fails on a transient storage error keeps the `Claim` intact, so the caller
retries with the same value.

The cancellation token is the handle for cancelling the processing of a
claimed job. [`Queue::cancel`][cancel] fires it while the claim is live, and a
worker that watches it stops early and settles as usual.

There is no cancelled state in the queue. Its terminal states are done and
dead, so a cancelled job ends as one of those. The `cancel_requested` flag
retained on the record is what marks the end as a cancellation. The worker
chooses the settlement:

```text
the token fires and the worker stops
  │
  ├─ ack           done, with cancel_requested retained
  ├─ dead_letter   dead, with the reason given, cancel_requested retained
  └─ nack          scheduled for a retry, and the next claim fires the
                   token again at once
```

An ack is the right settlement when stopping early is an acceptable end, and
a dead-letter when the job must stay visible for inspection. A nack only cycles the job through
its remaining attempts, because every later claim begins cancelled. In the
worker loop, `process` returns `Ok` for the ack and a `PermanentFailure` for
the dead-letter.

A requested cancel also refuses lease renewal, with `Error::CancelRequested`.
A worker that ignores the token therefore cannot keep extending its lease, and
the reaper ends the claim at the expiry.

A cancel outlives the claim it was aimed at. On a claimed job `Queue::cancel`
first sets `cancel_requested` on the stored record, then fires the token. The
flag stays on every record the job is written to afterwards.

```text
Queue::cancel(id), while the job is claimed
  1. sets cancel_requested on the stored record
  2. fires the claim's cancellation token

the worker watches the token       the worker ignores the token
  it stops early and settles         an ack ends the job, flag retained
  the flag stays on the record       a nack or an expired lease returns it
                                     to pending, flag set, and the next
                                     claim fires its own token
```

On that next claim the token is already fired when the worker receives the
job. A worker that selects on it stops at once, and one that checks it before
starting skips the work. It then settles as described, without having
processed anything.

The `Claim` does not include the lease expiry. Renewal changes the expiry, so
the queue reads it from the registry on request.

In the worker loop the `Claim` stays with the loop. `Worker::process` receives
the record and a `LeaseHandle` for renewal, and the loop settles the claim
when `process` returns. User code therefore cannot settle the delivery
mid-`process`, and the claim protocol of [Claiming and
settling](claiming.md) is the loop's concern.

## The lease registry

A claimed job has one key in the store, and its value is the job record. A job
that is claimed, reaped and claimed again is written with that same key each
time. The record differs only in its attempt count and claim time. The store
therefore does not record three facts about a claimed job:

- Which claim of the job is current.
- When the claim expires.
- How to interrupt the work while it runs.

`LeaseRegistry` ([lease_registry.rs][lease_registry]) records all three. It
contains one entry per live claim, and the entry names which claim is current,
when it expires and how to stop it.

The `Claim` and the registry entry are the two parts of one lease. The worker
task owns the `Claim`, which is the proof that this claim of the job belongs
to this task. The queue owns the entry, which is its authority on which claim of
the job is live now.

The reaper and `Queue::cancel` reach a lease through the
entry, because neither has the `Claim`. A settlement reaches it through the
`Claim`, and the claim id is what joins the two parts.

```text
worker task                          queue
┌─ Claim ─────────────────┐          ┌─ LeaseRegistry entry (queue, id) ─┐
│ record                  │          │ expiry                            │
│ claim id ───────────────┼──────────┼ claim id                          │
│ cancellation token ─────┼──────────┼ cancellation token                │
└─────────────────────────┘          │ reaping mark                      │
                                     └───────────────────────────────────┘

reached through the Claim:           reached through the entry:
ack, nack, dead_letter, renew_lease  the reaper, Queue::cancel
```

The entry's copy of the claim id is the queue's record of which claim is
live. A job that is claimed again after a failure gets a different claim id, so
the copy inside an older `Claim` no longer matches. The claim id is never
written to the store.

Each field of the entry has its writer and its reader:

| Field | Written by | Read by |
| --- | --- | --- |
| expiry | The claim sets it, and renewal extends it. | The reaper, which takes the entries that are due, soonest first, through a second index of the entries, ordered by expiry. |
| claim id | The claim sets it. | Settlement, which compares it with the claim id in the `Claim` and refuses the settlement when the two differ. |
| cancellation token | The claim creates it. | `Queue::cancel`, which fires it, and the worker, which stops early when it fires. |
| reaping mark | The reaper sets it when the entry is due. | Renewal, which refuses a marked entry. |

The reaper marks each due entry and leaves it in place. Renewal and the reaper
do not share a durable key, so the mark is what orders them. A reap that fails leaves
its mark for the next tick, so one unreapable job does not block the entries
behind it.

Nothing in the registry is written to the store, because a lease matters only
to the process that owns it. Recovery when the queue opens returns every claim
it finds, regardless of its expiry.

The [settlement fence](claiming.md#the-settlement-fence) rests on two rules
about that entry:

- The queue inserts the entry before the claim's transaction commits. A failed
  commit therefore leaves a stale entry, and the reaper drops it when it is due
  and finds the claimed record gone. The opposite order risks a missing entry.
  A missing entry hides a live claim from the reaper until the queue opens
  again. A cancellation that arrives during the commit then does not find a
  token to fire.
- The queue removes the entry only after the transaction that ends the claim
  commits, and only if the entry's claim id matches the claim's. A removal that
  runs after the job is claimed again does not remove anything, and the entry
  of the new claim stays in place.

The second rule is what makes the registry lag the store. Between an ending
commit and its removal, the entry still names a claim that already ended.

[claim-method]: https://docs.rs/taquba/latest/taquba/struct.Queue.html#method.claim
[claim_batch]: https://docs.rs/taquba/latest/taquba/struct.Queue.html#method.claim_batch
[claim-struct]: https://docs.rs/taquba/latest/taquba/struct.Claim.html
[renew_lease]: https://docs.rs/taquba/latest/taquba/struct.Queue.html#method.renew_lease
[cancel]: https://docs.rs/taquba/latest/taquba/struct.Queue.html#method.cancel
[lease_registry]: https://github.com/micllam/taquba/blob/master/taquba/src/lease_registry.rs
[job]: https://github.com/micllam/taquba/blob/master/taquba/src/job.rs
[lease]: https://github.com/micllam/taquba/blob/master/taquba/src/lease.rs

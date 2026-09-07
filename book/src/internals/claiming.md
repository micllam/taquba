# Claiming and settling

This chapter describes the protocol that assigns a job to one worker and ends that
assignment. The code is in `taquba/src/queue.rs` ([source][queue]) and
`taquba/src/txn.rs` ([source][txn]).

A delivery is the assignment of a job to a worker, and it is outstanding until
the worker reports the outcome. Two transactions implement it. The claim assigns
the job, and the settlement ends the delivery with an ack, a nack or a
dead-letter. A job that is claimed again after a failure begins a new delivery.

Claiming enforces exclusivity. While one worker processes a job, no other
worker can process it. A job whose worker hangs or dies returns to the queue, and a later claim
delivers it again.

Every worker reads from and writes to the store directly, and the queue does
not identify workers.

Exclusivity is therefore decided in two places:

- The store decides who claims a job. The move out of the pending space into
  the claimed space is one transaction, and exactly one worker can make it.
- The in-memory [lease registry](leases.md#the-lease-registry) decides whose
  claim is current. A job that is
  reaped and claimed again is written with the same key and the same record.
  The store cannot tell the two claims apart.

Every claim has an expiry, because a worker that stops responding leaves the
claimed record in place. A job whose expiry passes returns to the queue.

## The lifecycle of a claim

```text
           claim                  settlement
  pending ────────▶ claimed ────────────▶ done       ack
     ▲                 │                  scheduled  nack, retried later
     │                 │                  dead       dead-letter, or nack
     └─────────────────┘                             at the attempt limit
      reaper at expiry, or recovery when the queue opens
```

The return arrow is the path of a claim whose settlement never arrives. Two
mechanisms follow it:

- The reaper, a task inside the queue, returns a job to the pending space once
  its claim expires. This covers a worker that hangs.
- Recovery when the queue opens returns every claim the previous process
  held, regardless of its expiry. This covers a process that died.

The sections that follow describe the claim transaction and the settlement.
[Leases and cancellation](leases.md) describes what the worker receives and
the in-memory state separate from the store.

## The claim transaction

Claiming is one [transaction](store.md#transactions). It reads jobs from the
pending space and writes them into the claimed space, so either the whole
batch is claimed or none of it is.

A claim changes five key spaces, listed in the table under [The claimed
record](#the-claimed-record).
All of the changes commit together. That keeps a job from existing in two
key spaces at once or in neither. It also keeps the index from naming a key that
does not exist.

The claim transaction sees the pending space as it was when the transaction
began. A job enqueued, promoted by the scheduler or requeued by the reaper
while the transaction is open is not seen. A later claim takes it.

A cancel that removes a pending job while the transaction is open is a
conflict, because both delete the same pending key. The first of the two to
commit wins, and the store rejects the other. A rejected claim transaction
starts again on a fresh snapshot and claims what it finds then, which can be
fewer jobs or none.

### Candidate selection

A pending job is one key in the pending space. Within a queue those keys sort
by priority first and by job id second. A generated id begins with its creation
time, so within one priority the oldest job is first in key order.

A scan of the queue's
prefix therefore yields the highest-priority job first, and the oldest job
within a priority. The scan takes the first keys in that order, up to the number the caller
requested. Those keys are the candidates: the pending jobs the transaction
claims if its commit succeeds.

[The scan bound](scan-bound.md) describes where in the prefix the scan
begins, which is past its start in the usual case.

### The claimed record

The transaction decodes each candidate and updates it in memory. It sets
`claimed_at` to the current time and increments `attempts`. The counter
therefore records claims, and it is incremented before the outcome of the
delivery is known.

The transaction then stages the effect of the claim on each key space, which
[Key layout](key-layout.md#the-key-spaces) describes. The stages commit
together, so the order between them does not matter.

| Space | Before the claim | After it |
| --- | --- | --- |
| `Pending` | the job's record | deleted |
| `Claimed` | nothing | the job's record |
| `JobIndex` | names the pending key | names the claimed key |
| `Dedup` | names the job, when it was enqueued with a dedup key | deleted |
| `Stats` | pending and claimed counts | pending down one, claimed up one |

The queue also records each claim in the lease registry as the transaction is
built. [The lease registry](leases.md#the-lease-registry) describes that
entry.

### The dedup key

A job can include a dedup key, supplied at enqueue. The dedup index, a key
space in the store, maps a queue and a dedup key to the id of the job enqueued
with it. An enqueue whose
dedup key is in the index returns that id and does not create a job. The entry
exists from the enqueue until the claim transaction deletes it, so it covers
the time the job is `Pending` or `Scheduled`. An enqueue with the same dedup
key after the claim creates a new job.

Once claimed, a job is never deduplicated again, regardless of its later attempts.
A job enqueued later with the same dedup key gets an index entry of its own.
The later attempts of the first job leave that entry in place:

```text
                                   dedup index    job record of A
enqueue job A with dedup key d     d → A          dedup key d
claim A                            entry deleted  dedup key removed
enqueue job B with dedup key d     d → B
nack A                             d → B
claim A again                      d → B
```

The claim removed the dedup key from the record of A, so the second claim of
A does not delete the entry that B owns.

### Commit durability

The claim transaction commits without waiting for the WAL flush
([Durability](store.md#durability)), because every outcome leaves the job
claimable again.

```text
              claim commits, applied in memory
                             │
              ┌──────────────┴──────────────┐
              │                             │
       process survives               process dies
              │                             │
              ▼                ┌────────────┴────────────┐
        the flush completes    │                         │
        and the claim    before the flush          after the flush
        is durable             │                         │
                               ▼                         ▼
                        the pending key           the claimed record
                        was never deleted,        is found when the
                        so the job is             queue opens, and the
                        still Pending             job returns to Pending
```

The two crash cases differ in one respect: the claim that reached the WAL
counts as an attempt, and the claim lost with the WAL does not. A process that
dies between a claim and its flush therefore leaves the attempt count one
lower than a durable claim leaves it. The job is delivered again in both
cases, which is the only property a claim must preserve.

## Settlement

A settlement ends the claim and writes the outcome. `txn::stage_claim_end`
writes the record at the done, scheduled or dead key, as the outcome
requires.

The commit waits for the WAL flush, because a settlement lost with an
unflushed WAL is not repeated: the job returns to `Pending` at the next open
as if the worker had not settled it. The wait delays the caller's return only.
The outcome is visible in the store as soon as the commit applies, so a nacked
job can be scheduled and claimed again before its flush completes. A crash in
that window loses the settlement and the new claim together, and the job is
delivered once more at the next open.

After the commit the queue removes the lease registry entry if its claim id
still matches the claim's. The condition covers the gap between the commit and the
removal, in which the job can be claimed again:

```text
settlement commits     the job is scheduled or pending again
  │
  │  a new claim of the job replaces the registry entry with its own
  │
removal runs           the entry's claim id is the new claim's, so the
                       removal leaves it in place
```

That is the second of the [registry rules](leases.md#the-lease-registry).

### The settlement fence

The settlement fence rejects a settlement whose claim is not the
current one. Every settlement begins by ending the claim, in
`txn::take_claim`, which performs three checks. Each check catches a different
case.

- **The registry claim id must match.** This check rejects a settlement that a
  re-claim superseded. The presence of the key cannot do the same. A reap and a
  re-claim rewrite the same claimed key, and that key then looks untouched.
- **The claimed record is read inside the transaction.** A settlement that
  begins while the registry lags the store passes the claim id check. It began
  after the commit that ended its claim, so it does not conflict with anything. The read
  catches it, because the record is gone.
- **The claimed key is deleted in the transaction.** SlateDB's snapshot
  isolation tracks the write set. A requeue or a re-claim that commits after
  the snapshot writes the same key, so the settlement's commit is rejected as
  a conflict.

The checks run inside the settlement's retry loop, on every retry, because
each retry begins from a fresh snapshot. A settlement that fails any of them
returns [`Error::ClaimLost`][ClaimLost], and the job belongs to the worker
that claimed it next.

[ClaimLost]: https://docs.rs/taquba/latest/taquba/enum.Error.html#variant.ClaimLost
[queue]: https://github.com/micllam/taquba/blob/master/taquba/src/queue.rs
[txn]: https://github.com/micllam/taquba/blob/master/taquba/src/txn.rs

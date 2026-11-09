# Claiming and settling

This chapter describes the protocol that assigns a job to one worker and ends
that assignment. The code is in `taquba/src/queue.rs` ([source][queue]) and
`taquba/src/txn.rs` ([source][txn]).

A delivery is the assignment of a job to a worker, and it is outstanding until
the worker reports the outcome. Two transactions implement it. The claim assigns
the job, and the settlement ends the delivery with an ack, a nack or a
dead-letter. A job that is claimed again after a failure begins a new delivery.

Claiming enforces exclusivity. While one worker processes a job, no other worker
can process it. A job whose worker hangs or dies returns to the queue, and a
later claim delivers it again.

Every worker reads from and writes to the store directly, and the queue does not
identify workers.

The store decides exclusivity: a single transaction moves a job from the pending
space into the claimed space, and exactly one competing claim commits. Because
the claimed key contains the claim id, each claim of a job has its own key,
including a claim after a reap. The in-memory
[lease registry](leases.md#the-lease-registry) keeps the expiry and the
cancellation token of each claim in an entry under the same id.

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
- Recovery when the queue opens returns every claim the previous process held,
  regardless of its expiry. This covers a process that died.

The sections that follow describe the claim transaction and the settlement.
[Leases and cancellation](leases.md) describes what the worker receives and the
in-memory state separate from the store.

## The claim transaction

Claiming is one [transaction](store.md#transactions). It reads jobs from the
pending space and writes them into the claimed space, so either the whole batch
is claimed or none of it is.

A claim changes five key spaces, listed in the table under [The claimed
record](#the-claimed-record). All of the changes commit together. That keeps a
job from existing in two key spaces at once or in neither. It also keeps the
index from referring to a key that does not exist.

The claim transaction sees the pending space as it was when the transaction
began. A job enqueued, promoted by the scheduler or requeued by the reaper while
the transaction is open is not seen. A later claim takes it.

A cancel that removes a pending job while the transaction is open is a conflict,
because both delete the same pending key. The first of the two to commit wins,
and the store rejects the other. A rejected claim transaction starts again on a
fresh snapshot and claims what it finds then, which can be fewer jobs or none.

### Candidate selection

A pending job is one key in the pending space. Within a queue those keys sort by
priority first and by job id second. A generated id begins with its creation
time, so within one priority the oldest job is first in key order.

A scan of the queue's prefix therefore yields the highest-priority job first,
and the oldest job within a priority. The scan takes the first keys in that
order, up to the number the caller requested. Those keys are the candidates: the
pending jobs the transaction claims if its commit succeeds.

[The scan bound](scan-bound.md) describes where in the prefix the scan begins,
which is past its start in the usual case.

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
| `JobIndex` | the pending key | the claimed key |
| `Dedup` | the job's id, when it was enqueued with a dedup key | deleted |
| `Stats` | pending and claimed counts | pending down one, claimed up one |

The queue also records each claim in the lease registry as the transaction is
built. [The lease registry](leases.md#the-lease-registry) describes that entry.

### The dedup key

A job can include a dedup key, supplied at enqueue. The dedup index, a key space
in the store, maps a queue and a dedup key to the id of the job enqueued with
it. An enqueue whose dedup key is in the index returns that id and does not
create a job. The entry exists from the enqueue until the claim transaction
deletes it, so it covers the time the job is `Pending` or `Scheduled`. An
enqueue with the same dedup key after the claim creates a new job.

Once claimed, a job is never deduplicated again, regardless of its later
attempts. A job enqueued later with the same dedup key gets an index entry of
its own. The later attempts of the first job leave that entry in place:

```text
                                   dedup index    job record of A
enqueue job A with dedup key d     d → A          dedup key d
claim A                            entry deleted  dedup key removed
enqueue job B with dedup key d     d → B
nack A                             d → B
claim A again                      d → B
```

The claim removed the dedup key from the record of A, so the second claim of A
does not delete the entry that B owns.

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

The two crash cases differ in one respect: the claim that reached the WAL counts
as an attempt, and the claim lost with the WAL does not. A process that dies
between a claim and its flush therefore leaves the attempt count one lower than
a durable claim leaves it. The job is delivered again in both cases, which is
the only property a claim must preserve.

## Settlement

A settlement ends the claim and writes the outcome. `Txn::stage_claim_end`
writes the record at the done, scheduled or dead key, as the outcome requires.

The commit waits for the WAL flush, because a settlement lost with an unflushed
WAL is not repeated: the job returns to `Pending` at the next open as if the
worker had not settled it. The wait delays the caller's return only. The outcome
is visible in the store as soon as the commit applies, so a nacked job can be
scheduled and claimed again before its flush completes. A crash in that window
loses the settlement and the new claim together, and the job is delivered once
more at the next open.

After the commit the queue removes the lease registry entry of the claim. Each
claim has its own entry, so a new claim of the job that registers between the
commit and the removal keeps its entry. That is the second of the
[registry rules](leases.md#the-lease-registry).

### The settlement fence

The settlement fence rejects a settlement whose claim has ended. Every
settlement begins by reading the claim's record, in `txn::read_claim`, and ends
the claim in `txn::stage_claim_end`. The two steps make two checks, and each
check catches a different case.

- **The claimed record is read inside the transaction**, at the key that
  contains the claim's id. A reap, a requeue or a settlement of the claim
  deletes that key, and a re-claim writes another key. A settlement that begins
  after the claim has ended therefore does not find the record.
- **The claimed key is deleted in the transaction**, by the transition that ends
  the claim. SlateDB's snapshot isolation tracks the write set. An end of the
  claim that commits after the snapshot deletes the same key, so the
  settlement's commit is rejected as a conflict.

The checks run inside the settlement's retry loop, on every retry, because each
retry begins from a fresh snapshot. A settlement that fails any of them returns
[`Error::ClaimLost`][ClaimLost], and the job belongs to the worker that claimed
it next.

[ClaimLost]: https://docs.rs/taquba/latest/taquba/enum.Error.html#variant.ClaimLost
[queue]: https://github.com/micllam/taquba/blob/master/taquba/src/queue.rs
[txn]: https://github.com/micllam/taquba/blob/master/taquba/src/txn.rs

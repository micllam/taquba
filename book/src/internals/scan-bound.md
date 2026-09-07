# The scan bound

This chapter describes where the claim scan of [Claiming and
settling](claiming.md#candidate-selection) begins, and why. The code is in
`taquba/src/claim_cursor.rs` ([source][claim_cursor]).

The candidate scan does not begin at the start of the queue's pending prefix.
Claiming a job deletes its pending key, and a [delete](store.md#deletes)
leaves a tombstone, a marker for a deleted key, until compaction removes it.

A busy queue therefore grows a band of dead keys at the
start of its prefix. A scan that begins there reads all of them before it
reaches a job it can claim.

The queue avoids that band. It remembers where the previous scan stopped and
resumes from that bound. `ClaimCursor` ([claim_cursor.rs][claim_cursor]) keeps
that bound, one per queue. Each claim turns the keys it takes into tombstones
and leaves the bound past them. The dead band grows behind the bound and is
never read again.

```text
one queue's pending prefix in key order, one box per key

  ▨ ▨ ▨ ▨ ▨ ▨ ▨ ▨ □ □ □ □ □ □ □ □
                  ▲
                  └ the bound: where the next scan begins

a claim takes the next two keys and deletes them, so those boxes
become tombstones and the bound moves past them

  ▨ ▨ ▨ ▨ ▨ ▨ ▨ ▨ ▨ ▨ □ □ □ □ □ □
                      ▲
                      └ the bound after the claim
```

Every live pending key must sort at or after the bound. Only then is a resume
from the bound safe. An enqueue can put a key behind it, because keys sort by
priority first. A job enqueued at a higher priority than the jobs already
claimed sorts before them. Its key is then inside the band of tombstones that
the scan skips.

```text
a job is enqueued at a higher priority than those already claimed,
so its key sorts inside the band the scan skips

  ▨ ▨ ▨ ▨ ★ ▨ ▨ ▨ ▨ ▨ □ □ □ □ □ □
          ▲           ▲
          │           └ the bound: where the scan starts without the insert
          └ the new key, behind the bound

the insert moves the bound back to the new key

  ▨ ▨ ▨ ▨ ★ ▨ ▨ ▨ ▨ ▨ □ □ □ □ □ □
          ▲
          └ the bound after the insert
```

Every pending insert moves the bound back far enough to include the new key.
The condition is true again as soon as the insert commits. The next scan then
re-reads the tombstones between the new key and the point the previous scan
reached. The queue accepts that work to deliver the new job promptly.

The bound is in-memory state. `Queue::close` writes it to the store as part of
closing. A process that shut down cleanly stored its bound, and the next process
resumes from it.

After a crash there is no such record, and the first scan begins at the start
of the prefix.

A bound is valid only for the moment it was taken, because
enqueues move it back. A scan from the start of the prefix cannot skip
the jobs that arrived after a captured bound. The start is
the one position that sits behind every live key. The first scan after a crash
reads the tombstone band, and every scan after it resumes from the bound again.

Claiming runs under a per-queue lock. `ClaimCursor` contains the lock and the
bound. The lock covers both the scan and the commit, so claims on one queue
happen one at a time, and two claims never conflict on a pending key.

The lock also sets how fast a queue can be drained. It
yields one batch per scan-and-commit, no matter how many workers wait on it. A
batch amortises that cost, because one batch takes the lock once and uses one
transaction and one commit regardless of its size.

[claim_cursor]: https://github.com/micllam/taquba/blob/master/taquba/src/claim_cursor.rs

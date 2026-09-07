# The store

Every job record, index and counter is in one SlateDB store. The store has one
writer process at any time, and every worker is a task in that process.
This chapter describes the parts of SlateDB's model that the other chapters
rely on: writes and their durability, reads, transactions and deletes. SlateDB
itself is documented at [slatedb.io](https://slatedb.io).

## Writes

A write goes to two places. It is applied to the memtable, SlateDB's in-memory
write buffer, where later reads see it. It is also appended to the WAL, the
write-ahead log, which SlateDB flushes to object storage on the flush interval.
The interval is SlateDB's default unless
[`OpenOptions::flush_interval`][flush_interval] sets it.

```text
write ──▶ memtable ─── size limit ──▶ L0 SST ─── compaction ──▶ sorted runs
  │       in memory,                  in object                in object
  │       read first                  storage                  storage
  └─────▶ WAL ─── flush interval ──▶ object storage
          replayed at the next open
```

When the memtable reaches its size limit, SlateDB freezes it and writes it to
object storage at level 0 (L0). The written file is an SST, a sorted immutable
file. Compaction later merges SSTs into sorted runs, larger files that replace
the SSTs they merge.

## Durability

A write is durable once the flush that contains it completes. A memtable that
is lost with the process is rebuilt at the next open from the flushed WAL. A
write that the flush did not reach is lost with the memtable.

A commit can wait for the flush that makes its writes durable, or it can return
as soon as the writes are in the memtable. The queue chooses per transition,
described under [Commit durability](claiming.md#commit-durability).

## Reads

SlateDB offers three read forms:

- **`get`** returns the value at one key.
- **`scan`** returns the entries of an explicit key range, in key order.
- **`scan_prefix`** returns the entries that share a byte prefix, in key
  order, with an optional subrange inside the prefix.

The queue uses `get` and `scan_prefix`.

A read consults the memtable first and then the SSTs, newest first, and
stops at the first match:

```text
read ──▶ memtable ──▶ L0 SSTs ──▶ sorted runs
         in memory    newest       oldest
                      first        last
```

A committed write is therefore visible to the next read at once, before its
flush.

## Transactions

Every job transition is a SlateDB transaction, which runs in three steps:

- **Begin.** The transaction takes a snapshot, a fixed view of the store at
  that instant, and every read in the transaction reads from it.
- **Stage.** Its writes are collected in the transaction and are not visible
  to any other read.
- **Commit.** The store applies the staged writes as one batch, or rejects the
  transaction.

The store rejects the commit when another transaction wrote one of the same
keys after the snapshot was taken:

```text
T1   begin ──── stage a write to K ─────────────────── commit: rejected
T2         begin ──── stage a write to K ──── commit: applied

     K changed after T1's snapshot, so T1 retries on a new one
```

Only written keys count: a key that the transaction read and another
transaction changed does not cause a conflict.

## Deletes

A delete is a write of a tombstone, a marker that the key is deleted. The
tombstone is written to the memtable and flushed like any other write, and the
older value stays in the SST that contains it. A read that meets the tombstone
before the value reports the key as absent:

```text
delete K

  memtable, or a newer SST        an older SST
  ┌──────────────────┐            ┌──────────────────┐
  │ K: tombstone     │            │ K: value         │
  └──────────────────┘            └──────────────────┘
    a read of K stops here          never reached
    and reports K absent
```

Compaction removes the value when it merges the two files, and removes the
tombstone once it reaches the oldest sorted run. Until then a scan over a
range of deleted keys reads every tombstone in it before it reaches a live
key:

```text
scan from K1, before compaction

  K1: tombstone   K2: tombstone   K3: tombstone   K4: value
  read, skipped   read, skipped   read, skipped   returned
```

[The scan bound](scan-bound.md) describes the consequence for the claim scan.

[flush_interval]: https://docs.rs/taquba/latest/taquba/struct.OpenOptions.html#structfield.flush_interval

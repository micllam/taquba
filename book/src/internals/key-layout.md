# Key layout

Module `taquba/src/keys.rs` ([source][keys]).

## The store

All of the queue's state is in one store:

- Job records, in five lifecycle key spaces.
- The job index, the dedup index and the attempt histories.
- The claim cursor.
- Per-queue counters.
- The writer heartbeat.
- The caller's own KV namespace.

The encoding meets three requirements, and the sections that follow describe
how the layout meets each of them.

**Separation.** Every key states which key space it belongs to. The caller writes
opaque bytes of its own into the same store through the KV namespace. Without
that marker a caller key can coincide with the key of a job record and
overwrite it. A scan of one key space can then return the keys of another.

**Scan order.** A scanned key space places its keys so that one scan reads a
contiguous range in the order the code needs.

**Parseability.** A key can be read field by field. A scan therefore parses a
timestamp or a queue name from a key without decoding its record.

## The key header

Every internal key begins with a two-byte header, followed by the fields of
its key space.

```text
[ tag ][ version ][ fields ... ]
   1        1        variable
```

The tag byte partitions the keyspace. Every key of one key space starts with the
same tag, so those keys sort together with no key of another key space between them.

A scan of a whole key space is therefore a scan of the two-byte prefix
returned by `tag_prefix`, and it ends where the next key space begins.

The version byte is inside every scan prefix. A scan therefore selects one
version of one key space, and a key with any other version byte is outside its
range.

```text
keys with tag 0x03, in sorted order

  03 01 ...  ┐
  03 01 ...  ├─ the prefix 03 01 selects these
  03 01 ...  ┘
  03 02 ...     a later version: outside that prefix, unseen by the scan
```

`KEY_VERSION` is `1`. A layout change leaves the earlier version's keys in the
store, outside every scan, parse and sweep, until the store is discarded.
Nothing migrates them, and there is no store-level layout record. Before 1.0 a
minor release can change the layout outright.

The byte versions the key alone.
A record's value is a self-describing map, so a field added to it does not
need a bump ([The job record](job-record.md#the-stored-fields)).

`0x00` is reserved as invalid for both the tag and the version.

Caller KV keys are the exception. `user_scoped_key` writes `[0xFF, caller
bytes]` with no version byte, because the caller's bytes are opaque data this
module does not own. `0xFF` also places the entire caller namespace after every
internal key space.

## The key spaces

Twelve key spaces, all defined on `KeyTag`. The `Fields` column lists what follows
the two-byte header.

| Space | Byte | Fields | Value |
| --- | --- | --- | --- |
| `Pending` | `0x01` | qlen, queue, priority `u32` BE, id | job record |
| `Claimed` | `0x02` | qlen, queue, id | job record |
| `Scheduled` | `0x03` | `run_at` `u64` BE, qlen, queue, id | job record |
| `Done` | `0x04` | `completed_at` `u64` BE, qlen, queue, id | job record |
| `Dead` | `0x05` | qlen, queue, id | job record |
| `JobIndex` | `0x06` | id | the job's current primary key |
| `Dedup` | `0x07` | qlen, queue, dedup key | the job's id |
| `Cursor` | `0x08` | queue | that queue's `PersistedCursor` |
| `Stats` | `0x09` | qlen, queue, metric name | counter, merge-appended |
| `AttemptHistory` | `0x0A` | id | concatenated `JobAttempt` entries |
| `Heartbeat` | `0x0B` | none, one key per store | the writer's last heartbeat |
| `User` | `0xFF` | caller bytes, no version byte | the caller's value |

A job record is therefore stored at exactly one of the first five keys at any
time, and `JobIndex` names which one.

A `Stats` value is a counter that a merge operator maintains. A transition
writes a delta to the key, and `QueueMergeOperator` ([stats.rs][stats]) adds
the deltas together when the key is read or compacted. Two transitions that
write the same counter therefore do not conflict. `AttemptHistory` uses the
same operator to append entries.

`JobIndex` and `AttemptHistory` use the id alone as their key, because an id
identifies one job across the store. Generated ids are ULIDs, which sort by
creation time ([Order-preserving encodings](#order-preserving-encodings)
describes the encoding). A caller-supplied
[`EnqueueOptions::id_override`][id_override]
is checked against `JobIndex` inside the enqueue transaction, and a duplicate
is rejected with [`Error::DuplicateJobId`][DuplicateJobId] (`stage_job_writes`,
[effects.rs][effects]).

## The status comes from the key

A record's status is derived from the key space its key belongs to:

- The stored value leaves the status out, because [`JobRecord::status`][status]
  is marked `#[serde(skip, default = "JobStatus::initial")]`. A record
  therefore cannot disagree with its key.
- Reading a record goes through `JobRecord::decode`, which deserialises the
  value and sets the status from the key's tag. `decode` rejects a key
  outside the five job-state key spaces with an error.
- Deserialising the bytes directly yields `Pending` regardless of the key they came
  from, so nothing in the crate calls `rmp_serde::from_slice` on a record.

## Scan order

The field a scan orders by comes first.

`Scheduled` and `Done` lead with a timestamp. The scheduler
(`promote_due_jobs`, [scheduler.rs][scheduler]) and the done retention sweep
(`sweep_expired`, [reaper.rs][reaper]) each read one global range in time
order. Each exits at the first key past its cutoff.

`Pending`, `Claimed` and `Dead` lead with the queue name, so the claim scan and
`list_jobs` ([view.rs][view]) read one queue's range. `Pending` is ordered by
priority and then by id, and the other two by id.

`Claimed` is read whole once, by `requeue_interrupted_claims`
([reaper.rs][reaper]) when the queue is opened. That read requeues every claim
the previous process held, and so covers every queue.

`Cursor` and `Stats` are read whole as well. The cursor records are read when
the queue is opened (`restore_cursor_state`, [claim_cursor.rs][claim_cursor]),
and the stats space by `list_queues`, which discovers queue names from it. A
queue's own counters are point gets.

`JobIndex`, `AttemptHistory`, `Dedup`, `Heartbeat` and `User` are reached by
point lookup, so their keys contain only what identifies the record. That is a
job id for the first two, and a queue plus dedup key for `Dedup`. The single
heartbeat key ends at the header, and a `User` key has the caller's
own bytes. A prefix relation between two keys inside those key spaces is therefore
harmless.

## Order-preserving encodings

A field encoding is order-preserving when, for two values `a < b`, the encoded
bytes of `a` sort before those of `b` under bytewise comparison. The store
provides byte order. The encoding is what makes byte order agree with the
field's own order.

Two properties give that:

**Fixed width.** Every value of the field occupies the same number of bytes, so
a comparison never ends because one key ran out of field. Decimal text fails
here: `"10"` sorts before `"9"`.

**Most significant byte first.** Bytewise comparison decides on the first
differing byte, so that byte must be the one that dominates the value. In
little-endian, `1u32` encodes as `01 00 00 00` and `256u32` as `00 01 00 00`,
so `256` sorts before `1`. Big-endian gives `00 00 00 01` and `00 00 01 00`, in
numeric order.

Timestamps are `u64` big-endian and priorities `u32` big-endian, both unsigned,
so no sign handling arises. Values do not have that requirement. The statistics
counters merge little-endian `i64` deltas (`update_stats`, [stats.rs][stats]).

Priority orders by ascending number. The constants run `PRIORITY_HIGH = 100`,
`PRIORITY_NORMAL = 1_000` and `PRIORITY_LOW = 10_000`, so a claim reaches the
high bucket first ([options.rs][options]).

The job id occupies the tail of a key and is stored as text, so no byte order
arises. Its ordering comes from the ULID encoding:

```text
01ARZ3NDEK TSV4RRFFQ69G5FAV
└────┬───┘ └───────┬───────┘
     │             └── 16 characters, random, incremented by the
     │                 generator within one millisecond
     └── 10 characters, the millisecond timestamp, most significant
         digit first
```

The Crockford base32 alphabet ascends in ASCII, so comparing the characters as
bytes compares the value they encode. `next_job_id` ([effects.rs][effects])
owns the `ulid::Generator` and generates from the queue's clock. Pending keys
within one priority therefore come out in enqueue order.

`EnqueueOptions::id_override` accepts 1 to 128 bytes of `[A-Za-z0-9_-]`
(`validate_id_override`, [queue.rs][queue]). `-` and `_` are outside the ULID
alphabet, so caller ids interleave with generated ones by their own byte order.
The option's documentation asks for ULID-form ids where FIFO ordering within a
priority matters.

## Parseable fields

A scan that parses a field from a key must find that field at a known offset.
The `qlen` byte, the length of the queue name, appears wherever a field follows
the name. `Cursor`, where the name is the last field, omits it. One worked
layout of each:

```text
scheduled key, run_at 1768000000000, queue "email", id "01J9ZQ8XKF..."

 0x03 │ 0x01 │ 00 00 01 9B A5 03 10 00 │ 0x05 │ e m a i l │ 01J9ZQ8XKF...
──────┼──────┼─────────────────────────┼──────┼───────────┼───────────────
 tag  │ ver  │ run_at, u64 BE          │ qlen │ queue     │ id, to the end
   1  │   1  │            8            │   1  │   qlen    │      rest

cursor key, queue "email"

 0x08 │ 0x01 │ e m a i l
──────┼──────┼───────────────────
 tag  │ ver  │ queue, to the end
   1  │   1  │       rest
```

The queue name follows its length in the first, so it can contain any bytes
and still end at a known offset. The length also separates names that
share a prefix, so `pending_prefix("a")` excludes the keys of queue `ab`. The
cursor key is read by exact key, so it needs neither. The one-byte length
bounds a name at 255 bytes. That bound is the type `QueueName`
([keys.rs][keys]). Its constructor rejects a longer name, and it is the
parameter type of every key builder and the type of `JobRecord::queue`, so no
key is built over a name past the bound.

One consequence of putting the length first: within a key space, keys sort by
queue-name length before name, so queue `z` sorts before queue `aa`. No scan
of those key spaces depends on the order between queues, because each one reads
either a single queue's prefix or the whole key space.

Both scans that parse a field from a key run within a version prefix, so a key
of another version never reaches them. `parse_key_timestamp` and
`parse_stats_key` still strip the whole two-byte header and return `None` for
anything else. That covers a truncated or malformed key: the `Done` sweep skips
such a row and the stats read drops it.

[status]: https://docs.rs/taquba/latest/taquba/struct.JobRecord.html#structfield.status
[id_override]: https://docs.rs/taquba/latest/taquba/struct.EnqueueOptions.html#structfield.id_override
[DuplicateJobId]: https://docs.rs/taquba/latest/taquba/enum.Error.html#variant.DuplicateJobId
[keys]: https://github.com/micllam/taquba/blob/master/taquba/src/keys.rs
[effects]: https://github.com/micllam/taquba/blob/master/taquba/src/effects.rs
[stats]: https://github.com/micllam/taquba/blob/master/taquba/src/stats.rs
[options]: https://github.com/micllam/taquba/blob/master/taquba/src/options.rs
[queue]: https://github.com/micllam/taquba/blob/master/taquba/src/queue.rs
[reaper]: https://github.com/micllam/taquba/blob/master/taquba/src/reaper.rs
[scheduler]: https://github.com/micllam/taquba/blob/master/taquba/src/scheduler.rs
[view]: https://github.com/micllam/taquba/blob/master/taquba/src/view.rs
[claim_cursor]: https://github.com/micllam/taquba/blob/master/taquba/src/claim_cursor.rs

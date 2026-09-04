# Key layout

Module `taquba/src/keys.rs` ([source][keys]).

## The store and its reads

SlateDB offers two read forms: `get` returns the value under one key, and a scan
returns the entries of a key range in key order, either over an explicit range
(`scan`) or over the keys under a byte prefix with an optional subrange inside
it (`scan_prefix`). The queue uses `get` and `scan_prefix`.

All of the queue's state lives in one store: job records in five lifecycle
spaces, three indexes, the claim cursor, per-queue counters, the writer
heartbeat and the caller's own KV namespace.

The encoding meets three requirements:

**Separation.** Every key states which space it belongs to. The caller writes
opaque bytes of its own into the same store through the KV namespace, so without
that marker a caller key could coincide with the key of a job record and
overwrite it, and a scan of one space could return the keys of another.

**Scan order.** A scanned space places its keys so that one scan reads a
contiguous range in the order the code needs.

**Parseability.** A key can be read field by field, so a scan can take a
timestamp or a queue name off a key without decoding the record under it.

## The key header

Every internal key begins with a two-byte header, followed by the fields of
its space.

```text
[ tag ][ version ][ fields ... ]
   1        1        variable
```

The tag byte partitions the keyspace. Every key of one space starts with the
same tag, so those keys sort together with no key of another space between them.

A whole-space scan is therefore a scan of the two-byte prefix returned by
`tag_prefix`, and it ends where the next space begins.

The version byte sits inside every scan prefix, so a scan selects one version of
one space and a key written under any other version falls outside its range.

```text
keys under tag 0x03, in sorted order

  03 01 ...  ┐
  03 01 ...  ├─ the prefix 03 01 selects these
  03 01 ...  ┘
  03 02 ...     a later version: outside that prefix, unseen by the scan
```

Both scans that read a field out of a key run under that prefix, so a key of
another version never reaches them. `parse_key_timestamp` and
`parse_stats_key` still strip the whole two-byte header and return `None` for
anything else, which covers a truncated or malformed key: the `Done` sweep
skips such a row and the stats read drops it.

`KEY_VERSION` is `1`. A layout change would leave the earlier version's keys in
the store, outside every scan, parse and sweep, until the store is discarded;
nothing migrates them and no store-level layout record exists. Before 1.0 a
minor release may change the layout outright.

`0x00` is reserved as invalid for both the tag and the version, asserted by
`every_tag_is_recovered_from_its_byte`.

Caller KV keys are the exception: `user_scoped_key` writes `[0xFF, caller
bytes]` with no version byte, since the caller's bytes are opaque data this
module does not own. `0xFF` also places the entire caller namespace after every
internal space.

## The tags

Twelve spaces, all defined on `KeyTag`. The `Fields` column lists what follows
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

A job record therefore lives under exactly one of the first five keys at any
time, and `JobIndex` names which one.

That is also where a record's status comes from:

- The stored value leaves the status out, since `JobRecord::status` is marked
  `#[serde(skip, default = "JobStatus::initial")]`. A record therefore cannot
  disagree with the key it was read under.
- Reading a record goes through `JobRecord::decode`, which deserialises the
  value and stamps the status from the key's tag. A key outside the five
  job-state spaces is a decode error.
- Deserialising the bytes directly would yield `Pending` whatever key they came
  from, so nothing in the crate calls `rmp_serde::from_slice` on a record.

Two tests keep this true: `the_stored_value_carries_no_status` checks that the
encoded bytes name no status field, and `decode_takes_the_status_from_the_key`
decodes one byte string under a dead key and a pending key and gets `Dead` and
`Pending`.

`JobIndex` and `AttemptHistory` key by id alone, which holds because an id
identifies one job across the store: generated ids are ULIDs, and a
caller-supplied `EnqueueOptions::id_override` is rejected against `JobIndex`
inside the enqueue
transaction with `Error::DuplicateJobId` (`stage_job_writes`,
[effects.rs][effects]).

The `qlen` byte appears wherever a field follows the queue name. Two worked
layouts, one from each ordering family:

```text
pending key, queue "email", priority 1000, id "01J9ZQ8XKF..."

 0x01 │ 0x01 │ 0x05 │ e m a i l │ 00 00 03 E8 │ 01J9ZQ8XKF...
──────┼──────┼──────┼───────────┼─────────────┼───────────────
 tag  │ ver  │ qlen │ queue     │ priority BE │ id, to the end
   1  │   1  │   1  │   qlen    │      4      │      rest

scheduled key, run_at 1768000000000, queue "email", same id

 0x03 │ 0x01 │ 00 00 01 9B A5 03 10 00 │ 0x05 │ e m a i l │ 01J9ZQ8XKF...
──────┼──────┼─────────────────────────┼──────┼───────────┼───────────────
 tag  │ ver  │ run_at, u64 BE          │ qlen │ queue     │ id, to the end
   1  │   1  │            8            │   1  │   qlen    │      rest
```

The queue name sits behind its length in both, so it can hold any bytes and
still end at a known offset. The length also separates names that share a
prefix, so `pending_prefix("a")` excludes the keys of queue `ab`, asserted by
`pending_prefixes_of_nested_queue_names_do_not_collide` and
`claimed_keys_of_nested_queue_names_do_not_collide`.

One consequence of putting the length first: within a space, keys sort by
queue-name length before name, so queue `z` sorts ahead of queue `aa`. No scan
of those spaces depends on the order between queues, since each one reads
either a single queue's prefix or the whole space.

## Order-preserving fields

A field encoding is order-preserving when, for two values `a < b`, the encoded
bytes of `a` sort before those of `b` under bytewise comparison. The store
provides byte order; the encoding is what makes byte order agree with the
field's own order.

Two properties give that:

**Fixed width.** Every value of the field occupies the same number of bytes, so
a comparison never ends because one key ran out of field. Decimal text fails
here: `"10"` sorts before `"9"`.

**Most significant byte first.** Bytewise comparison decides on the first
differing byte, so that byte has to be the one that dominates the value. In
little-endian, `1u32` encodes as `01 00 00 00` and `256u32` as `00 01 00 00`, so
`256` would sort first. Big-endian gives `00 00 00 01` and `00 00 01 00`, in
numeric order.


Timestamps are `u64` big-endian and priorities `u32` big-endian, both unsigned,
so no sign handling arises. Values do not have that requirement: the statistics
counters merge little-endian `i64` deltas (`update_stats`, [stats.rs][stats]).

Priority orders by ascending number, so the constants run `PRIORITY_HIGH = 100`,
`PRIORITY_NORMAL = 1_000`, `PRIORITY_LOW = 10_000` and a claim reaches the high
bucket first ([options.rs][options]).

The job id occupies the tail of a key and is stored as text, so no byte order
arises. Its ordering comes from the ULID encoding: all ids are 26 characters,
the first 10 hold the millisecond timestamp with its most significant digit
first, and the Crockford base32 alphabet ascends in ASCII, so comparing the
characters as bytes compares the value they encode.

Ordering inside one millisecond comes from the generator. `next_job_id`
([effects.rs][effects]) holds a `ulid::Generator` and generates from the queue's
clock, and the generator increments the previous random component while the
millisecond is unchanged, which `test_ids_increase_within_one_millisecond` locks
under a frozen clock. Pending keys within one priority therefore come out in
enqueue order.

`EnqueueOptions::id_override` accepts 1 to 128 bytes of `[A-Za-z0-9_-]`
(`validate_id_override`, [queue.rs][queue]). `-` and `_` sit outside the ULID
alphabet, so caller ids interleave with generated ones by their own byte order,
and the option's documentation asks for ULID-form ids where FIFO ordering
within a priority matters.

## The ordering rule

The field a scan orders by comes first.

`Scheduled` and `Done` lead with a timestamp, so the scheduler
(`promote_due_jobs`, [scheduler.rs][scheduler]) and the done retention sweep
(`sweep_expired`, [reaper.rs][reaper]) each read one global range in time order
and exit at the first key past their cutoff.

`Pending`, `Claimed` and `Dead` lead with the queue name, so the claim scan and
`list_jobs` ([read.rs][read]) read one queue's range, `Pending` ordered by
priority and then by id and the other two by id.

`Claimed` is read whole once, by `requeue_interrupted_claims`
([reaper.rs][reaper]) at open, which re-queues every claim the previous process
left behind and so covers every queue.

`Cursor` and `Stats` are read whole as well: the cursor records at open
(`restore_cursor_state`, [claim_cursor.rs][claim_cursor]) and the stats space
by `list_queues`, which discovers queue names from it. A queue's own counters
are point gets.

`JobIndex`, `AttemptHistory`, `Dedup`, `Heartbeat` and `User` are reached by
point lookup, so their keys hold only what identifies the record: a job id for
the first two, a queue and dedup key, nothing after the header for the single
heartbeat key and the caller's own bytes. A prefix relation between two keys
inside those spaces is therefore harmless.

[keys]: https://github.com/micllam/taquba/blob/master/taquba/src/keys.rs
[effects]: https://github.com/micllam/taquba/blob/master/taquba/src/effects.rs
[stats]: https://github.com/micllam/taquba/blob/master/taquba/src/stats.rs
[options]: https://github.com/micllam/taquba/blob/master/taquba/src/options.rs
[queue]: https://github.com/micllam/taquba/blob/master/taquba/src/queue.rs
[reaper]: https://github.com/micllam/taquba/blob/master/taquba/src/reaper.rs
[scheduler]: https://github.com/micllam/taquba/blob/master/taquba/src/scheduler.rs
[read]: https://github.com/micllam/taquba/blob/master/taquba/src/read.rs
[claim_cursor]: https://github.com/micllam/taquba/blob/master/taquba/src/claim_cursor.rs

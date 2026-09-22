//! The user KV namespace of a [`Queue`]: the value-size cap, the
//! standalone and compare operations and the page type for scans.
//! Caller-supplied keys are scoped under a reserved internal key tag,
//! so they cannot collide with the queue's own layout. Writes coupled
//! to a queue transition are [`Queue::enqueue_with_kv`] and the KV
//! fields of [`SettlementEffects`](crate::SettlementEffects).

use std::ops::{Bound, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};

use bytes::Bytes;
use futures_util::Stream;
use slatedb::DbTransaction;

use crate::error::{Error, Result};
use crate::keys::user_scoped_key;
use crate::queue::Queue;
use crate::txn::{Attempt, Durability, retry};

/// Maximum size of a single value in the user KV namespace.
///
/// The KV namespace is sized for coordination state (pointers, status
/// markers, dedup records, small lifecycle records), not bulk payload.
/// Values exceeding this cap return [`Error::KvValueTooLarge`].
///
/// Store large blobs in the underlying [`ObjectStore`](crate::object_store::ObjectStore) under a
/// content-addressed key and put only the pointer in KV.
pub const MAX_KV_VALUE_SIZE: usize = 256 * 1024;

/// Validate a user KV value against [`MAX_KV_VALUE_SIZE`].
pub(crate) fn validate_kv_value_size(value: &[u8]) -> Result<()> {
    if value.len() > MAX_KV_VALUE_SIZE {
        return Err(Error::KvValueTooLarge {
            size: value.len(),
            max: MAX_KV_VALUE_SIZE,
        });
    }
    Ok(())
}

/// A range of keys within a prefix, as [`Queue::kv_scan`] takes it. The
/// standard range forms over any byte string implement it: `..`,
/// `key..`, `..key`, `..=key`, `a..b` and `a..=b`. A pair of [`Bound`]s
/// implements it, which is the form of an exclusive start.
pub trait KvRange {
    /// The lower bound of the range.
    fn start_bound(&self) -> Bound<&[u8]>;
    /// The upper bound of the range.
    fn end_bound(&self) -> Bound<&[u8]>;
}

impl<K: AsRef<[u8]>> KvRange for Range<K> {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Included(self.start.as_ref())
    }
    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Excluded(self.end.as_ref())
    }
}

impl<K: AsRef<[u8]>> KvRange for RangeInclusive<K> {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Included(self.start().as_ref())
    }
    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Included(self.end().as_ref())
    }
}

impl<K: AsRef<[u8]>> KvRange for RangeFrom<K> {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Included(self.start.as_ref())
    }
    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }
}

impl<K: AsRef<[u8]>> KvRange for RangeTo<K> {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }
    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Excluded(self.end.as_ref())
    }
}

impl<K: AsRef<[u8]>> KvRange for RangeToInclusive<K> {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }
    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Included(self.end.as_ref())
    }
}

impl KvRange for RangeFull {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }
    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }
}

impl<K: AsRef<[u8]>> KvRange for (Bound<K>, Bound<K>) {
    fn start_bound(&self) -> Bound<&[u8]> {
        self.0.as_ref().map(AsRef::as_ref)
    }
    fn end_bound(&self) -> Bound<&[u8]> {
        self.1.as_ref().map(AsRef::as_ref)
    }
}

/// One page of a user KV listing. Returned by [`Queue::kv_scan`].
#[derive(Debug, Clone)]
pub struct KvPage {
    /// Entries on this page as `(key, value)` pairs, in ascending byte
    /// order of the keys. Keys are in the caller namespace, without the
    /// internal user key tag.
    pub entries: Vec<(Vec<u8>, Bytes)>,
    /// Whether an entry within the range followed the last entry of this
    /// page at scan time. The listing continues with the range from
    /// `Bound::Excluded` of that entry's key to the same end.
    pub more: bool,
}

impl Queue {
    /// Read a value from the user KV namespace.
    ///
    /// Caller-supplied keys are internally scoped under a reserved
    /// user key tag and cannot collide with Taquba's internal layout.
    pub async fn kv_get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        crate::read::kv_get(self.core.db.as_ref(), key).await
    }

    /// Write a value to the user KV namespace.
    ///
    /// Caller-supplied keys are internally scoped under a reserved
    /// user key tag and cannot collide with Taquba's internal layout.
    /// Values above [`MAX_KV_VALUE_SIZE`] return
    /// [`Error::KvValueTooLarge`]; unlike job payloads, user KV values
    /// are never offloaded to the payload store, so the cap is a hard
    /// error. Store larger values as objects under caller-owned keys
    /// and keep only the pointer in KV. The write is durable before the
    /// call returns.
    ///
    /// This is the standalone form; to couple a KV write with a queue
    /// transition in one transaction, use [`Self::enqueue_with_kv`] or
    /// [`SettlementEffects::kv_writes`](crate::SettlementEffects::kv_writes) via [`Self::ack_with`].
    pub async fn kv_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        validate_kv_value_size(value)?;
        let handle = self.core.db.put(user_scoped_key(key), value).await?;
        handle.await_durable().await?;
        Ok(())
    }

    /// Delete a value from the user KV namespace.
    ///
    /// Caller-supplied keys are internally scoped under a reserved
    /// user key tag and cannot collide with Taquba's internal layout.
    pub async fn kv_delete(&self, key: &[u8]) -> Result<()> {
        let handle = self.core.db.delete(user_scoped_key(key)).await?;
        handle.await_durable().await?;
        Ok(())
    }

    /// Delete a value from the user KV namespace only if its current
    /// value equals `expected`.
    ///
    /// Returns `true` when the value matched and was deleted, `false`
    /// when the key was absent or held a different value (nothing is
    /// changed in that case). The read and the delete execute in one
    /// transaction, so no concurrent write can be interleaved between
    /// the compare and the delete: either this call deletes the value
    /// it compared against, or it reports `false`. The delete is
    /// durable before the call returns `true`.
    ///
    /// Use this to consume a value that a concurrent writer may replace,
    /// where an unconditional [`Self::kv_delete`] could delete a newer
    /// value than the one read.
    pub async fn kv_compare_delete(&self, key: &[u8], expected: &[u8]) -> Result<bool> {
        self.kv_compare_then(key, Some(expected), |txn, scoped| txn.delete(scoped))
            .await
    }

    /// Write a value to the user KV namespace only if its current state
    /// matches `expected`.
    ///
    /// `expected` is the compare arm: `Some(v)` requires the key to
    /// currently hold exactly `v`; `None` requires the key to be
    /// absent. Returns `true` when the state matched and the write was
    /// applied, `false` when it did not (nothing is changed in that
    /// case). The read and the write execute in one transaction, so no
    /// concurrent write can be interleaved between the compare and the
    /// write: either this call replaces the state it compared against,
    /// or it reports `false`. The write is durable before the call
    /// returns `true`.
    ///
    /// Values above [`MAX_KV_VALUE_SIZE`] return
    /// [`Error::KvValueTooLarge`].
    ///
    /// This is the read-modify-write primitive for the namespace: read
    /// a value, compute its successor and call
    /// `kv_compare_put(key, Some(&read), &next)` in a retry loop, or
    /// claim a key exclusively with `kv_compare_put(key, None, &init)`.
    /// Transaction conflicts with concurrent writers are retried
    /// internally, but a contended key serializes its writers; state
    /// with many independent writers scales better split across
    /// multiple keys than concentrated in one key.
    pub async fn kv_compare_put(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool> {
        validate_kv_value_size(value)?;
        self.kv_compare_then(key, expected, |txn, scoped| txn.put(scoped, value))
            .await
    }

    /// Compare the current state of the user KV key against `expected`
    /// (`None` requires absence) and, when it matches, stage `write` in
    /// the same transaction and commit durably. Returns whether the
    /// state matched; conflicts are retried.
    async fn kv_compare_then(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        write: impl Fn(&DbTransaction, &[u8]) -> std::result::Result<(), slatedb::Error>,
    ) -> Result<bool> {
        let (scoped, write) = (&user_scoped_key(key), &write);
        retry(&self.core.db, Durability::Awaited, |txn| async move {
            let matched = match (txn.get(scoped).await?, expected) {
                (Some(current), Some(e)) => current.as_ref() == e,
                (None, None) => true,
                _ => false,
            };
            if !matched {
                txn.rollback();
                return Ok(Attempt::Abort(false));
            }
            write(&txn, scoped)?;
            Ok(Attempt::Commit(txn, true))
        })
        .await
    }

    /// List entries of the user KV namespace under `prefix` within
    /// `range`, in ascending byte order of the keys.
    ///
    /// An empty `prefix` lists the whole namespace, and `..` lists every
    /// key within the prefix. The bounds of `range`, a [`KvRange`], are
    /// keys in the caller namespace: `key..` begins at `key`, and
    /// `(Bound::Excluded(key), Bound::Unbounded)` begins after it, which
    /// continues a listing from the last key of a page. The page contains
    /// the keys within the prefix that the range contains. A bound
    /// outside the prefix is correct as given, and a range without such
    /// a key returns an empty page. The listing is not a snapshot, so an
    /// entry written or deleted between page reads is missed or observed
    /// depending on the position of its key.
    ///
    /// Only caller-namespace entries are returned, and Taquba's internal
    /// key spaces are never visible here. This is the enumeration and
    /// export primitive for the namespace: a full sweep (`prefix = b""`,
    /// `..`, continued while [`KvPage::more`]) observes every entry that
    /// existed for the whole sweep.
    pub async fn kv_scan(
        &self,
        prefix: &[u8],
        range: impl KvRange,
        limit: usize,
    ) -> Result<KvPage> {
        crate::read::kv_scan(self.core.db.as_ref(), prefix, range, limit).await
    }

    /// Every entry of the user KV namespace under `prefix` within
    /// `range`, in ascending byte order of the keys, as one stream that
    /// reads through [`Self::kv_scan`] `page_size` entries at a time. A
    /// consumer that stops reading does not fetch a further page. The
    /// listing semantics are those of `kv_scan`.
    pub fn kv_entries<'a>(
        &'a self,
        prefix: &'a [u8],
        range: impl KvRange,
        page_size: usize,
    ) -> impl Stream<Item = Result<(Vec<u8>, Bytes)>> + 'a {
        crate::read::kv_entries(self.core.db.as_ref(), prefix, range, page_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::pending_key;
    use crate::test_util::*;

    #[tokio::test(start_paused = true)]
    async fn test_kv_compare_put_stalls_during_store_outage_without_partial_state() {
        let store = FaultStore::wrap();
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.kv_put(b"slot", b"v1").await.unwrap();

        store.fail_puts(true);
        // The compare-miss arm is read-only and completes despite the
        // write fault.
        assert!(!q.kv_compare_put(b"slot", Some(b"v0"), b"v2").await.unwrap());
        // The matched arm awaits durability. SlateDB retries transient
        // store errors with backoff instead of failing the flush, so the
        // call must stall rather than report success. Paused runtime time
        // drives the retry backoff virtually; the elapsed timeout drops
        // the in-flight call, simulating a crash mid-outage.
        let stalled = tokio::time::timeout(
            Duration::from_secs(30),
            q.kv_compare_put(b"slot", Some(b"v1"), b"v2"),
        )
        .await;
        assert!(stalled.is_err());
        drop(q);

        store.fail_puts(false);
        let q = Queue::open(store, "test").await.unwrap();
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );
        assert!(q.kv_compare_put(b"slot", Some(b"v1"), b"v2").await.unwrap());
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v2".as_slice())
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_put_roundtrip_and_size_cap() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.kv_put(b"config", b"v1").await.unwrap();
        assert_eq!(
            q.kv_get(b"config").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );

        let oversized = vec![0u8; MAX_KV_VALUE_SIZE + 1];
        assert!(matches!(
            q.kv_put(b"blob", &oversized).await,
            Err(Error::KvValueTooLarge { .. })
        ));

        q.kv_delete(b"config").await.unwrap();
        assert!(q.kv_get(b"config").await.unwrap().is_none());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_compare_delete() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.kv_put(b"latch", b"v1").await.unwrap();

        assert!(!q.kv_compare_delete(b"latch", b"v2").await.unwrap());
        assert_eq!(
            q.kv_get(b"latch").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );

        assert!(q.kv_compare_delete(b"latch", b"v1").await.unwrap());
        assert!(q.kv_get(b"latch").await.unwrap().is_none());

        assert!(!q.kv_compare_delete(b"latch", b"v1").await.unwrap());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_compare_put() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        assert!(!q.kv_compare_put(b"slot", Some(b"v1"), b"v2").await.unwrap());
        assert!(q.kv_get(b"slot").await.unwrap().is_none());

        assert!(q.kv_compare_put(b"slot", None, b"v1").await.unwrap());
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );

        assert!(!q.kv_compare_put(b"slot", None, b"v2").await.unwrap());
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );

        assert!(!q.kv_compare_put(b"slot", Some(b"v0"), b"v2").await.unwrap());
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );

        assert!(q.kv_compare_put(b"slot", Some(b"v1"), b"v2").await.unwrap());
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v2".as_slice())
        );

        let oversized = vec![0u8; MAX_KV_VALUE_SIZE + 1];
        assert!(matches!(
            q.kv_compare_put(b"slot", Some(b"v2"), &oversized).await,
            Err(Error::KvValueTooLarge { .. })
        ));

        q.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_kv_compare_put_loses_no_updates_under_contention() {
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        q.kv_put(b"counter", &0u64.to_be_bytes()).await.unwrap();

        let mut handles = Vec::new();
        for _ in 0..4 {
            let q = Arc::clone(&q);
            handles.push(tokio::spawn(async move {
                for _ in 0..25 {
                    loop {
                        let current = q.kv_get(b"counter").await.unwrap().unwrap();
                        let n = u64::from_be_bytes(current.as_ref().try_into().unwrap());
                        let next = (n + 1).to_be_bytes();
                        if q.kv_compare_put(b"counter", Some(current.as_ref()), &next)
                            .await
                            .unwrap()
                        {
                            break;
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let total = q.kv_get(b"counter").await.unwrap().unwrap();
        assert_eq!(u64::from_be_bytes(total.as_ref().try_into().unwrap()), 100);

        let q = Arc::try_unwrap(q).unwrap_or_else(|_| panic!("queue still shared"));
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_scan_pages_and_filters_by_prefix() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        for i in 0..5u8 {
            q.kv_put(&[b"runs/".as_slice(), &[b'0' + i]].concat(), &[i])
                .await
                .unwrap();
        }
        q.kv_put(b"config", b"c").await.unwrap();

        let page = q.kv_scan(b"runs/", .., 3).await.unwrap();
        assert_eq!(page.entries.len(), 3);
        assert_eq!(page.entries[0].0, b"runs/0");
        assert!(page.more);

        let last = page.entries[2].0.as_slice();
        let rest = q
            .kv_scan(b"runs/", (Bound::Excluded(last), Bound::Unbounded), 10)
            .await
            .unwrap();
        assert_eq!(rest.entries.len(), 2);
        assert_eq!(rest.entries[1].0, b"runs/4");
        assert!(!rest.more);

        let all = q.kv_scan(b"", .., 100).await.unwrap();
        assert_eq!(all.entries.len(), 6);
        assert_eq!(all.entries[0].0, b"config");

        let empty = q.kv_scan(b"", .., 0).await.unwrap();
        assert!(empty.entries.is_empty() && !empty.more);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn kv_scan_reads_the_keys_within_the_range() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        for i in 0..5u8 {
            q.kv_put(&[b"runs/".as_slice(), &[b'0' + i]].concat(), &[i])
                .await
                .unwrap();
        }
        let keys = |page: KvPage| page.entries.into_iter().map(|(k, _)| k).collect::<Vec<_>>();
        let key = |i: u8| [b"runs/".as_slice(), &[b'0' + i]].concat();

        // Each bound form, with a bound that is not a stored key.
        assert_eq!(
            keys(q.kv_scan(b"runs/", b"runs/2".., 10).await.unwrap()),
            [key(2), key(3), key(4)]
        );
        assert_eq!(
            keys(q.kv_scan(b"runs/", b"runs/25".., 10).await.unwrap()),
            [key(3), key(4)]
        );
        assert_eq!(
            keys(q.kv_scan(b"runs/", ..b"runs/2", 10).await.unwrap()),
            [key(0), key(1)]
        );
        assert_eq!(
            keys(q.kv_scan(b"runs/", ..=b"runs/2", 10).await.unwrap()),
            [key(0), key(1), key(2)]
        );
        assert_eq!(
            keys(q.kv_scan(b"runs/", b"runs/1"..b"runs/3", 10).await.unwrap()),
            [key(1), key(2)]
        );

        // A bound outside the prefix is before every key or after every key.
        assert_eq!(
            q.kv_scan(b"runs/", b"a".., 10).await.unwrap().entries.len(),
            5
        );
        assert_eq!(
            q.kv_scan(b"runs/", ..b"z", 10).await.unwrap().entries.len(),
            5
        );
        assert!(
            q.kv_scan(b"runs/", b"z".., 10)
                .await
                .unwrap()
                .entries
                .is_empty()
        );
        assert!(
            q.kv_scan(b"runs/", ..b"a", 10)
                .await
                .unwrap()
                .entries
                .is_empty()
        );

        // A range without a key within it, including an inverted one.
        let inverted = q.kv_scan(b"runs/", b"runs/3"..b"runs/1", 10).await.unwrap();
        assert!(inverted.entries.is_empty() && !inverted.more);
        let point = (Bound::Excluded(b"runs/2"), Bound::Excluded(b"runs/2"));
        assert!(
            q.kv_scan(b"runs/", point, 10)
                .await
                .unwrap()
                .entries
                .is_empty()
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_scan_excludes_internal_keys() {
        let initial = 1_700_000_000_000u64;
        let opts = OpenOptions {
            clock: Arc::new(MockClock::new(initial)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("jobs", b"payload".to_vec()).await.unwrap();
        q.enqueue_with(
            "jobs",
            b"later".to_vec(),
            EnqueueOptions {
                run_at: Some(std::time::UNIX_EPOCH + Duration::from_millis(initial + 3_600_000)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        q.kv_put(b"only", b"entry").await.unwrap();

        let page = q.kv_scan(b"", .., 100).await.unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].0, b"only");

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_state_survives_crash_reopen() {
        let store = make_store();
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.kv_put(b"standalone", b"v1").await.unwrap();
        let mut kv = HashMap::new();
        kv.insert(b"coupled".to_vec(), b"v2".to_vec());
        q.enqueue_with_kv("jobs", b"p".to_vec(), EnqueueOptions::default(), kv)
            .await
            .unwrap();
        drop(q);

        let q = Queue::open(store, "test").await.unwrap();
        assert_eq!(
            q.kv_get(b"standalone").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );
        assert_eq!(
            q.kv_get(b"coupled").await.unwrap().as_deref(),
            Some(b"v2".as_slice())
        );
        assert_eq!(q.stats("jobs").await.unwrap().pending, 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_keys_cannot_collide_with_internal_layout() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        // Enqueue a real job so the internal pending key space is in use.
        q.enqueue("work", b"payload".to_vec()).await.unwrap();

        // A user key that matches a real internal key byte-for-byte is
        // scoped under the user tag and cannot interfere with queue state.
        q.enqueue_with_kv(
            "other",
            b"sentinel".to_vec(),
            EnqueueOptions::default(),
            HashMap::from([(pending_key(&qn("work"), 1, "fake-id"), b"trickery".to_vec())]),
        )
        .await
        .unwrap();

        // The original job is still claimable from the original queue.
        let s = q.stats("work").await.unwrap();
        assert_eq!(s.pending, 1);
        let claimed = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.payload, b"payload");

        // The user-visible key still reads back fine.
        let v = q
            .kv_get(&pending_key(&qn("work"), 1, "fake-id"))
            .await
            .unwrap();
        assert_eq!(v.as_deref(), Some(b"trickery".as_slice()));

        q.close().await.unwrap();
    }
}

//! A time-ordered index over one prefix of the caller KV namespace,
//! whose due entries a pass removes with the state they refer to.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt;
use tracing::warn;

use crate::effects::SettlementEffects;
use crate::error::Result;
use crate::queue::Queue;
use crate::time_bound::TimeBound;

/// Entries read per page by a pass.
const PAGE_SIZE: usize = 256;

/// A time-ordered index over one prefix of the caller KV namespace.
///
/// An entry's key is the prefix, the time of its event as 8 bytes
/// big-endian and a free suffix, with an empty value. A pass reads the
/// entries oldest first, calls the consumer's callback for every entry
/// whose time is `retention` or more before the time of the pass and
/// stops at the first entry that is not due. The callback's KV deletes
/// commit in the transaction that deletes the entry.
///
/// The index keeps in memory the earliest time of an entry that a pass
/// did not remove. A pass returns without a read until that time is
/// due, and starts its scan at the key with that time, so the keys of
/// the entries that earlier passes deleted are not read. An entry is
/// written with [`SettlementEffects::expiry_entry`], and the queue
/// lowers that time to the entry's time once the effects commit. Clones
/// of an index share the time, so every writer of an index and its pass
/// hold clones of one `ExpiryIndex`. An entry written by another path
/// is outside the scan while its time is before the bound. It is read
/// once an entry at or before its time commits, or by a new
/// `ExpiryIndex`.
#[derive(Debug, Clone)]
pub struct ExpiryIndex {
    prefix: Arc<[u8]>,
    /// The earliest time of an entry that a pass did not remove.
    bound: Arc<TimeBound>,
}

/// The outcome of one entry in a pass, returned by the callback.
#[derive(Debug, Clone)]
pub enum Expired {
    /// Commit the effects in the transaction that deletes the entry.
    Delete(SettlementEffects),
    /// As [`Delete`](Self::Delete) when the user KV key `key` matches
    /// `expected`, as [`Queue::kv_compare_commit`] compares it, and a
    /// delete of the entry alone when it does not.
    DeleteIf {
        /// The key compared.
        key: Vec<u8>,
        /// The value the key must contain, or `None` for an absent key.
        expected: Option<Vec<u8>>,
        /// The effects committed with the entry delete on a match.
        effects: SettlementEffects,
    },
    /// Leave the entry and continue the pass. The next pass reads the
    /// entry again.
    Keep,
}

impl ExpiryIndex {
    /// An index over the entries with `prefix`. Every key with the
    /// prefix is read as an entry, so the prefix must not contain
    /// other keys.
    pub fn new(prefix: impl Into<Vec<u8>>) -> Self {
        Self {
            prefix: Arc::from(prefix.into()),
            bound: Arc::new(TimeBound::new()),
        }
    }

    /// The key of the entry for an event at `at_ms` with `suffix`:
    /// `{prefix}{at_ms as 8 bytes big-endian}{suffix}`. The call does
    /// not record the entry. An entry written with this key through
    /// [`SettlementEffects::kv_put`] is outside the scan while its time
    /// is before the bound.
    pub fn entry_key(&self, at_ms: u64, suffix: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.prefix.len() + 8 + suffix.len());
        key.extend_from_slice(&self.prefix);
        key.extend_from_slice(&at_ms.to_be_bytes());
        key.extend_from_slice(suffix);
        key
    }

    /// The time and the suffix of the entry at `key`, or `None` for a
    /// key outside the prefix or with fewer than 8 bytes after it.
    pub fn parse<'k>(&self, key: &'k [u8]) -> Option<(u64, &'k [u8])> {
        let rest = key.strip_prefix(&*self.prefix)?;
        let (time, suffix) = rest.split_first_chunk::<8>()?;
        Some((u64::from_be_bytes(*time), suffix))
    }

    /// Records that an entry at `at_ms` is committed. Called after the
    /// commit.
    pub(crate) fn written(&self, at_ms: u64) {
        self.bound.lower(at_ms);
    }

    /// One pass at `now_ms`: calls `clear` with the time and the suffix
    /// of every entry whose time is `retention` or more before `now_ms`,
    /// oldest first from the bound, and applies the [`Expired`] its
    /// future returns. Returns the count of entries removed through
    /// `clear`. A key with fewer than 8 bytes after the prefix is
    /// deleted without a call. A pass before an entry can be due
    /// returns without a read. A KV failure ends the pass with the
    /// error. The next pass reads the entries that a failed pass, or a
    /// pass whose future is dropped, did not remove.
    pub async fn pass<F, Fut>(
        &self,
        queue: &Queue,
        now_ms: u64,
        retention: Duration,
        mut clear: F,
    ) -> Result<usize>
    where
        F: FnMut(u64, Vec<u8>) -> Fut,
        Fut: Future<Output = Expired>,
    {
        let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
        let Some(mut scan) = self.bound.begin(now_ms, retention_ms) else {
            return Ok(0);
        };
        let mut removed = 0;
        let start = self.entry_key(scan.from(), &[]);
        let mut entries = pin!(queue.view().kv_entries(&self.prefix, start.., PAGE_SIZE));
        while let Some((key, _)) = entries.try_next().await? {
            let Some((at_ms, suffix)) = self.parse(&key) else {
                warn!(key = %String::from_utf8_lossy(&key), "expiry index key without a time; deleted");
                queue.kv_delete(&key).await?;
                continue;
            };
            if !scan.due(at_ms) {
                scan.retain(at_ms);
                break;
            }
            match clear(at_ms, suffix.to_vec()).await {
                Expired::Delete(effects) => {
                    queue.commit_effects(effects.kv_delete(key)).await?;
                    removed += 1;
                }
                Expired::DeleteIf {
                    key: compared,
                    expected,
                    effects,
                } => {
                    let applied = queue
                        .kv_compare_commit(
                            &compared,
                            expected.as_deref(),
                            effects.kv_delete(key.clone()),
                        )
                        .await?;
                    if applied.is_none() {
                        queue.kv_delete(&key).await?;
                    }
                    removed += 1;
                }
                Expired::Keep => scan.retain(at_ms),
            }
        }
        scan.complete();
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::MAX_KV_VALUE_SIZE;
    use crate::test_util::*;

    const RETENTION: Duration = Duration::from_millis(1_000);

    /// Writes the entry at `at_ms` with `suffix` through the effects.
    async fn put(q: &Queue, index: &ExpiryIndex, at_ms: u64, suffix: &[u8]) {
        q.commit_effects(SettlementEffects::default().expiry_entry(index, at_ms, suffix))
            .await
            .unwrap();
    }

    /// The suffixes of the entries within `prefix`, in key order.
    async fn suffixes(q: &Queue, index: &ExpiryIndex, prefix: &[u8]) -> Vec<Vec<u8>> {
        let page = q.view().kv_scan(prefix, .., 100).await.unwrap();
        page.entries
            .iter()
            .map(|(key, _)| index.parse(key).unwrap().1.to_vec())
            .collect()
    }

    #[test]
    fn entry_key_leads_with_the_time_and_parses_back() {
        let index = ExpiryIndex::new(b"x/".to_vec());
        let key = index.entry_key(0x0102, b"run-1");
        assert_eq!(key, b"x/\0\0\0\0\0\0\x01\x02run-1");
        assert_eq!(index.parse(&key), Some((0x0102, &b"run-1"[..])));
        assert_eq!(index.parse(b"y/\0\0\0\0\0\0\x01\x02run-1"), None);
        assert_eq!(index.parse(b"x/\0\0\0\0\0\0\x01"), None);
    }

    #[tokio::test]
    async fn pass_removes_the_due_entries_oldest_first_and_keeps_the_rest() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        for (at_ms, suffix) in [(3_000, "c"), (1_000, "a"), (2_000, "b")] {
            q.kv_put(&index.entry_key(at_ms, suffix.as_bytes()), b"")
                .await
                .unwrap();
        }

        let mut seen = Vec::new();
        let removed = index
            .pass(&q, 3_000, RETENTION, |at_ms, suffix| {
                seen.push((at_ms, suffix));
                std::future::ready(Expired::Delete(SettlementEffects::default()))
            })
            .await
            .unwrap();

        assert_eq!(removed, 2);
        assert_eq!(seen, [(1_000, b"a".to_vec()), (2_000, b"b".to_vec())]);
        assert_eq!(suffixes(&q, &index, b"x/").await, [b"c".to_vec()]);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn delete_effects_commit_with_the_entry_delete() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        put(&q, &index, 1_000, b"a").await;
        q.kv_put(b"records/a", b"state").await.unwrap();

        index
            .pass(&q, 2_000, RETENTION, |_, _| {
                std::future::ready(Expired::Delete(
                    SettlementEffects::default()
                        .kv_delete(b"records/a".to_vec())
                        .kv_put(b"log/a", b"expired"),
                ))
            })
            .await
            .unwrap();

        assert!(suffixes(&q, &index, b"x/").await.is_empty());
        assert_eq!(q.view().kv_get(b"records/a").await.unwrap(), None);
        assert_eq!(
            q.view().kv_get(b"log/a").await.unwrap().as_deref(),
            Some(&b"expired"[..])
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn delete_if_applies_the_effects_on_a_match_and_deletes_the_entry_alone_otherwise() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        for suffix in [b"a", b"b"] {
            put(&q, &index, 1_000, suffix).await;
            let mut record = b"records/".to_vec();
            record.extend_from_slice(suffix);
            q.kv_put(&record, b"state").await.unwrap();
        }

        let removed = index
            .pass(&q, 2_000, RETENTION, |_, suffix| {
                let mut record = b"records/".to_vec();
                record.extend_from_slice(&suffix);
                let expected = if suffix == b"a" { b"state" } else { b"other" };
                std::future::ready(Expired::DeleteIf {
                    key: record.clone(),
                    expected: Some(expected.to_vec()),
                    effects: SettlementEffects::default().kv_delete(record),
                })
            })
            .await
            .unwrap();

        assert_eq!(removed, 2);
        assert!(suffixes(&q, &index, b"x/").await.is_empty());
        assert_eq!(q.view().kv_get(b"records/a").await.unwrap(), None);
        assert_eq!(
            q.view().kv_get(b"records/b").await.unwrap().as_deref(),
            Some(&b"state"[..])
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn keep_leaves_the_entry_and_the_next_pass_reads_it_again() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        put(&q, &index, 1_000, b"a").await;
        put(&q, &index, 1_500, b"b").await;
        let mut kept = 0;

        for now_ms in [2_500, 2_999, 3_000] {
            index
                .pass(&q, now_ms, RETENTION, |_, suffix| {
                    if suffix == b"a" {
                        kept += 1;
                        std::future::ready(Expired::Keep)
                    } else {
                        std::future::ready(Expired::Delete(SettlementEffects::default()))
                    }
                })
                .await
                .unwrap();
        }

        // The bound stays at the kept entry's time, so the entry removed
        // after it does not defer the next read.
        assert_eq!(kept, 3);
        assert_eq!(suffixes(&q, &index, b"x/").await, [b"a".to_vec()]);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_kv_failure_ends_the_pass_and_the_next_pass_reads_the_entries_left() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        put(&q, &index, 1_000, b"a").await;
        put(&q, &index, 2_000, b"b").await;

        // The effects of the first entry fail the commit.
        let err = index
            .pass(&q, 3_000, RETENTION, |_, _| {
                std::future::ready(Expired::Delete(
                    SettlementEffects::default().kv_put(b"big", vec![0u8; MAX_KV_VALUE_SIZE + 1]),
                ))
            })
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::KvValueTooLarge { .. }));

        let mut seen = Vec::new();
        let removed = index
            .pass(&q, 3_000, RETENTION, |_, suffix| {
                seen.push(suffix);
                std::future::ready(Expired::Delete(SettlementEffects::default()))
            })
            .await
            .unwrap();
        assert_eq!(removed, 2);
        assert_eq!(seen, [b"a".to_vec(), b"b".to_vec()]);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_dropped_pass_future_leaves_the_bound_at_the_entries_left() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        put(&q, &index, 1_000, b"a").await;
        put(&q, &index, 2_000, b"b").await;

        // The callback of the second entry never returns, and the pass
        // future is dropped once it is reached.
        let reached = tokio::sync::Notify::new();
        let pass = index.pass(&q, 3_000, RETENTION, |at_ms, _| {
            let reached = &reached;
            async move {
                if at_ms == 1_000 {
                    return Expired::Delete(SettlementEffects::default());
                }
                reached.notify_one();
                std::future::pending().await
            }
        });
        tokio::select! {
            _ = pass => unreachable!("the callback of the second entry never returns"),
            () = reached.notified() => {}
        }

        let mut seen = Vec::new();
        let removed = index
            .pass(&q, 3_000, RETENTION, |_, suffix| {
                seen.push(suffix);
                std::future::ready(Expired::Delete(SettlementEffects::default()))
            })
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert_eq!(seen, [b"b".to_vec()]);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn pass_before_the_bound_is_due_does_not_read_the_index() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        put(&q, &index, 1_000, b"a").await;
        // The pass reads the entry, which is not due, and leaves the
        // bound at its time.
        let mut calls = 0;
        let removed = index
            .pass(&q, 1_500, RETENTION, |_, _| {
                calls += 1;
                std::future::ready(Expired::Keep)
            })
            .await
            .unwrap();
        assert_eq!((removed, calls), (0, 0));

        let removed = index
            .pass(&q, 1_999, RETENTION, |_, _| {
                calls += 1;
                std::future::ready(Expired::Keep)
            })
            .await
            .unwrap();
        assert_eq!((removed, calls), (0, 0));

        let removed = index
            .pass(&q, 2_000, RETENTION, |_, _| {
                calls += 1;
                std::future::ready(Expired::Delete(SettlementEffects::default()))
            })
            .await
            .unwrap();
        assert_eq!((removed, calls), (1, 1));
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn the_scan_starts_at_the_bound_and_a_new_index_reads_from_the_front() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        put(&q, &index, 1_000, b"a").await;
        // The pass leaves the bound at 1_000, the time of the entry it
        // read and did not remove.
        index
            .pass(&q, 1_500, RETENTION, |_, _| {
                std::future::ready(Expired::Keep)
            })
            .await
            .unwrap();

        // An entry before the bound, written without a record of its
        // commit, is before the start of the scan.
        q.kv_put(&index.entry_key(500, b"raw"), b"").await.unwrap();
        let mut seen = Vec::new();
        index
            .pass(&q, 2_000, RETENTION, |_, suffix| {
                seen.push(suffix);
                std::future::ready(Expired::Delete(SettlementEffects::default()))
            })
            .await
            .unwrap();
        assert_eq!(seen, [b"a".to_vec()]);
        assert_eq!(suffixes(&q, &index, b"x/").await, [b"raw".to_vec()]);

        let mut seen = Vec::new();
        ExpiryIndex::new(b"x/".to_vec())
            .pass(&q, 2_000, RETENTION, |_, suffix| {
                seen.push(suffix);
                std::future::ready(Expired::Delete(SettlementEffects::default()))
            })
            .await
            .unwrap();
        assert_eq!(seen, [b"raw".to_vec()]);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_entry_committed_during_a_pass_lowers_the_bound_the_pass_leaves() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        put(&q, &index, 1_000, b"a").await;

        // The callback writes an entry at 1_500 while the pass runs.
        let (queue, writer) = (&q, &index);
        index
            .pass(&q, 2_000, RETENTION, |_, _| async move {
                put(queue, writer, 1_500, b"b").await;
                Expired::Delete(SettlementEffects::default())
            })
            .await
            .unwrap();

        // A bound left at the start of the pass defers the read to 3_000.
        let removed = index
            .pass(&q, 2_500, RETENTION, |_, _| {
                std::future::ready(Expired::Delete(SettlementEffects::default()))
            })
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert!(suffixes(&q, &index, b"x/").await.is_empty());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_entry_committed_through_the_effects_is_read_by_the_next_due_pass() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        // A pass over an empty index leaves the bound at the maximum.
        index
            .pass(&q, 1_000, RETENTION, |_, _| {
                std::future::ready(Expired::Keep)
            })
            .await
            .unwrap();

        put(&q, &index, 1_000, b"a").await;
        let removed = index
            .pass(&q, 2_000, RETENTION, |_, _| {
                std::future::ready(Expired::Delete(SettlementEffects::default()))
            })
            .await
            .unwrap();
        assert_eq!(removed, 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn effects_that_do_not_apply_do_not_lower_the_bound() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        index
            .pass(&q, 1_000, RETENTION, |_, _| {
                std::future::ready(Expired::Keep)
            })
            .await
            .unwrap();
        // An entry the index does not know about. A lowered bound
        // exposes it to the next pass.
        q.kv_put(&index.entry_key(1_000, b"raw"), b"")
            .await
            .unwrap();

        // A nack with attempts remaining commits without its effects.
        q.enqueue("jobs", b"x".to_vec()).await.unwrap();
        let claim = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.nack_with(
            &claim,
            "transient",
            SettlementEffects::default().expiry_entry(&index, 1_000, b"b"),
        )
        .await
        .unwrap();

        let mut calls = 0;
        index
            .pass(&q, 5_000, RETENTION, |_, _| {
                calls += 1;
                std::future::ready(Expired::Keep)
            })
            .await
            .unwrap();
        assert_eq!(calls, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_key_with_fewer_than_8_bytes_after_the_prefix_is_deleted() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let index = ExpiryIndex::new(b"x/".to_vec());
        q.kv_put(b"x/abc", b"").await.unwrap();

        let mut calls = 0;
        let removed = index
            .pass(&q, 2_000, RETENTION, |_, _| {
                calls += 1;
                std::future::ready(Expired::Keep)
            })
            .await
            .unwrap();

        assert_eq!((removed, calls), (0, 0));
        assert!(
            q.view()
                .kv_scan(b"x/", .., 10)
                .await
                .unwrap()
                .entries
                .is_empty()
        );
        q.close().await.unwrap();
    }
}

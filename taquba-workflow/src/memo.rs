//! Per-step durable key-value store for memoizing within-step side
//! effects, backed by object storage.
//!
//! [`Memo`] makes within-step side effects retry-safe. Taquba delivers
//! at-least-once, so a step may run more than once if its lease expires
//! before ack; without a durable place to record intermediate results,
//! expensive operations (LLM calls, paid external APIs, multi-step side
//! effects) silently re-run on each retry.
//!
//! Each memo entry is keyed by `(run_id, step_number, user_key)`, so
//! distinct steps and runs see independent namespaces. User keys are
//! SHA-256-hashed before becoming object-store path segments so any
//! string is a valid key regardless of length or characters.
//!
//! # Layout
//!
//! [`MemoStore`] owns a single object-store prefix and partitions it into
//! three sub-prefixes:
//!
//! - `<prefix>/memos/<run_id>/<step_number>/<sha256(user_key)>`: memo
//!   entries written by [`Memo::put`].
//! - `<prefix>/step-outputs/<run_id>/<step_number>/<sha256(step_payload)>`:
//!   step-output replay entries written by the workflow runtime when
//!   enabled.
//! - `<prefix>/terminals/<terminal_at_ms:020>_<run_id>`: terminal
//!   markers written by [`MemoStore::write_terminal_marker`]. The leading
//!   zero-padded millisecond timestamp orders markers chronologically,
//!   so a retention sweeper can early-exit a prefix scan once it
//!   reaches markers younger than the retention window.
//!
//! # Cleanup
//!
//! The [`Memo`] primitive has no lifecycle management of its own.
//! [`MemoStore::clear_memos_for_run`] removes every memo entry and
//! step-output replay entry for a given run. [`MemoStore::write_terminal_marker`],
//! [`MemoStore::list_terminal_markers`], and
//! [`MemoStore::delete_terminal_marker`] are the building blocks a
//! caller (typically the workflow runtime) composes into a retention
//! sweeper.

use std::sync::Arc;

use futures_util::StreamExt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use taquba::object_store::{Error as ObjectStoreError, ObjectStore, path::Path};

use crate::error::{Error, Result};

/// Backing store for [`Memo`] entries, parametrised by an
/// [`ObjectStore`] and a path prefix. Builds per-step [`Memo`]
/// views via [`MemoStore::new_memo`].
///
/// Owns the memo, step-output, and terminal-marker sub-prefixes; see
/// the module docs for the path layout.
#[derive(Clone)]
pub struct MemoStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl std::fmt::Debug for MemoStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The object store doesn't implement Debug; show the prefix
        // (the operationally interesting part) and elide the rest.
        f.debug_struct("MemoStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl MemoStore {
    /// Build a `MemoStore` over the given object store and path prefix.
    /// Memo entries live under `<prefix>/memos/...` and terminal markers
    /// under `<prefix>/terminals/...`; the prefix should not overlap
    /// with the queue's SlateDB path or with any other consumer of the
    /// same store.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    /// Read a previously stored value for `(run_id, step_number, key)`,
    /// or `Ok(None)` if none has been written.
    async fn get(&self, run_id: &str, step_number: u32, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.memo_path(run_id, step_number, key);
        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(Some(bytes.to_vec()))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(Error::Store(err)),
        }
    }

    /// Store `value` against `(run_id, step_number, key)`. A subsequent
    /// `put` with the same key overwrites the prior value; on
    /// at-least-once retries this means the most recent attempt's
    /// value wins.
    async fn put(&self, run_id: &str, step_number: u32, key: &str, value: &[u8]) -> Result<()> {
        let path = self.memo_path(run_id, step_number, key);
        self.store.put(&path, value.to_vec().into()).await?;
        Ok(())
    }

    /// Build a [`Memo`] bound to `(run_id, step_number)`.
    pub fn new_memo(&self, run_id: impl Into<String>, step_number: u32) -> Memo {
        Memo::new(self.clone(), run_id, step_number)
    }

    /// Delete every memo entry and runtime step-output replay entry for
    /// `run_id`. Returns the number of entries removed. Errors during
    /// individual deletes are logged (best-effort cleanup) but do not
    /// stop the sweep; an aggregated error is returned only if a list
    /// operation fails.
    pub async fn clear_memos_for_run(&self, run_id: &str) -> Result<usize> {
        let memo_deleted = self
            .clear_prefix(run_id, self.memos_run_prefix(run_id), "memo")
            .await?;
        let step_output_deleted = self
            .clear_prefix(run_id, self.step_outputs_run_prefix(run_id), "step output")
            .await?;
        Ok(memo_deleted + step_output_deleted)
    }

    async fn clear_prefix(&self, run_id: &str, prefix: Path, kind: &'static str) -> Result<usize> {
        let mut stream = self.store.list(Some(&prefix));
        let mut deleted = 0usize;
        while let Some(item) = stream.next().await {
            let meta = item.map_err(Error::Store)?;
            match self.store.delete(&meta.location).await {
                Ok(()) => deleted += 1,
                Err(ObjectStoreError::NotFound { .. }) => {}
                Err(err) => {
                    tracing::warn!(
                        run_id = %run_id,
                        path = %meta.location,
                        error = %err,
                        "failed to delete {kind} entry",
                    );
                }
            }
        }
        Ok(deleted)
    }

    pub(crate) async fn get_step_output(
        &self,
        run_id: &str,
        step_number: u32,
        step_payload: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let path = self.step_output_path(run_id, step_number, step_payload);
        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(Some(bytes.to_vec()))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(Error::Store(err)),
        }
    }

    pub(crate) async fn put_step_output(
        &self,
        run_id: &str,
        step_number: u32,
        step_payload: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let path = self.step_output_path(run_id, step_number, step_payload);
        self.store.put(&path, value.to_vec().into()).await?;
        Ok(())
    }

    /// Write a terminal marker for `run_id` at `terminal_at_ms`. The
    /// marker is a zero-byte object whose path encodes both fields so a
    /// sweeper can decide retention without reading any content.
    /// Idempotent: a second call with the same `(run_id, terminal_at_ms)`
    /// overwrites the empty value with another empty value.
    pub async fn write_terminal_marker(&self, run_id: &str, terminal_at_ms: u64) -> Result<()> {
        let path = self.terminal_marker_path(run_id, terminal_at_ms);
        self.store.put(&path, Vec::new().into()).await?;
        Ok(())
    }

    /// List every terminal marker currently in the store.
    ///
    /// Markers are returned in arbitrary order (object-store list order
    /// is not guaranteed by the trait); callers that care about
    /// chronological order should sort by [`TerminalMarker::terminal_at_ms`].
    /// Markers whose filenames cannot be parsed are skipped with a
    /// warning rather than failing the whole listing.
    pub async fn list_terminal_markers(&self) -> Result<Vec<TerminalMarker>> {
        let prefix = self.terminals_prefix();
        let mut stream = self.store.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let meta = item.map_err(Error::Store)?;
            let Some(name) = meta.location.filename() else {
                continue;
            };
            match parse_terminal_marker_name(name) {
                Some((terminal_at_ms, run_id)) => out.push(TerminalMarker {
                    run_id,
                    terminal_at_ms,
                }),
                None => {
                    tracing::warn!(
                        path = %meta.location,
                        "unparseable terminal marker; skipping",
                    );
                }
            }
        }
        Ok(out)
    }

    /// Delete the terminal marker identified by `marker`.
    ///
    /// A missing marker (already swept by another pass) is treated as
    /// success.
    pub async fn delete_terminal_marker(&self, marker: &TerminalMarker) -> Result<()> {
        let path = self.terminal_marker_path(&marker.run_id, marker.terminal_at_ms);
        match self.store.delete(&path).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(err) => Err(Error::Store(err)),
        }
    }

    fn memo_path(&self, run_id: &str, step_number: u32, key: &str) -> Path {
        self.memos_run_prefix(run_id)
            .child(step_number.to_string())
            .child(hex_sha256(key.as_bytes()))
    }

    fn memos_run_prefix(&self, run_id: &str) -> Path {
        Path::from(format!("{}/memos/{}", self.prefix, run_id))
    }

    fn step_outputs_run_prefix(&self, run_id: &str) -> Path {
        Path::from(format!("{}/step-outputs/{}", self.prefix, run_id))
    }

    fn step_output_path(&self, run_id: &str, step_number: u32, step_payload: &[u8]) -> Path {
        self.step_outputs_run_prefix(run_id)
            .child(step_number.to_string())
            .child(hex_sha256(step_payload))
    }

    fn terminals_prefix(&self) -> Path {
        Path::from(format!("{}/terminals", self.prefix))
    }

    fn terminal_marker_path(&self, run_id: &str, terminal_at_ms: u64) -> Path {
        self.terminals_prefix()
            .child(format!("{terminal_at_ms:020}_{run_id}"))
    }
}

/// A terminal marker as returned by [`MemoStore::list_terminal_markers`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalMarker {
    /// The run this marker belongs to.
    pub run_id: String,
    /// Wall-clock millisecond timestamp recorded when the run reached
    /// its terminal state.
    pub terminal_at_ms: u64,
}

/// A view onto a [`MemoStore`] scoped to a specific
/// `(run_id, step_number)` pair.
#[derive(Clone)]
pub struct Memo {
    store: MemoStore,
    run_id: String,
    step_number: u32,
}

impl Memo {
    fn new(store: MemoStore, run_id: impl Into<String>, step_number: u32) -> Self {
        Self {
            store,
            run_id: run_id.into(),
            step_number,
        }
    }

    /// The run identifier this memo is bound to.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// The step number this memo is bound to.
    pub fn step_number(&self) -> u32 {
        self.step_number
    }

    /// Read a previously stored value for `key`, or `Ok(None)` if
    /// none has been written.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.store.get(&self.run_id, self.step_number, key).await
    }

    /// Store `value` for `key`, overwriting any prior value.
    ///
    /// Overwrite is intentional: a retry that produces the same value
    /// is idempotent. A retry that produces a *different* value
    /// indicates the handler isn't perfectly idempotent; the memo
    /// reflects whatever the most recent attempt wrote.
    pub async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        self.store
            .put(&self.run_id, self.step_number, key, value)
            .await
    }

    /// Read a memo entry whose key is derived from serialized `input`.
    ///
    /// The input is encoded as MessagePack and hashed with SHA-256 to
    /// derive the memo key.
    ///
    /// The derived key is stable only when `input` serializes
    /// deterministically; types with unordered iteration, such as
    /// `HashMap`, can serialize the same logical content into different
    /// bytes and therefore different keys. If several
    /// logical operations may receive the same input shape, include an
    /// operation name in the serialized input.
    pub async fn content_get<T>(&self, input: &T) -> Result<Option<Vec<u8>>>
    where
        T: Serialize + ?Sized,
    {
        let key = content_key(input)?;
        self.get(&key).await
    }

    /// Store `value` under a memo key derived from serialized `input`.
    ///
    /// See [`Self::content_get`] for the key derivation and namespace
    /// semantics.
    pub async fn content_put<T>(&self, input: &T, value: &[u8]) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        let key = content_key(input)?;
        self.put(&key, value).await
    }
}

impl std::fmt::Debug for Memo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Memo")
            .field("run_id", &self.run_id)
            .field("step_number", &self.step_number)
            .finish_non_exhaustive()
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

fn content_key<T>(input: &T) -> Result<String>
where
    T: Serialize + ?Sized,
{
    let bytes = rmp_serde::to_vec_named(input)?;
    Ok(format!("content:{}", hex_sha256(&bytes)))
}

/// Parse a terminal marker filename in the form `<ts:020>_<run_id>`.
/// Returns `None` if the leading 20 characters are not a base-10
/// integer or the underscore separator is missing.
fn parse_terminal_marker_name(name: &str) -> Option<(u64, String)> {
    let (ts_str, rest) = name.split_at_checked(20)?;
    let ts: u64 = ts_str.parse().ok()?;
    let run_id = rest.strip_prefix('_')?;
    Some((ts, run_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use taquba::object_store::memory::InMemory;

    #[derive(Serialize)]
    struct ContentInput<'a> {
        operation: &'static str,
        payload: &'a [u8],
    }

    fn make_memo() -> Memo {
        MemoStore::new(Arc::new(InMemory::new()), "memo").new_memo("run-1", 0)
    }

    #[tokio::test]
    async fn get_returns_none_for_missing_key() {
        let memo = make_memo();
        assert_eq!(memo.get("missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn put_then_get_round_trips() {
        let memo = make_memo();
        memo.put("k", b"hello").await.unwrap();
        assert_eq!(memo.get("k").await.unwrap(), Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn put_overwrites_prior_value() {
        let memo = make_memo();
        memo.put("k", b"first").await.unwrap();
        memo.put("k", b"second").await.unwrap();
        assert_eq!(memo.get("k").await.unwrap(), Some(b"second".to_vec()));
    }

    #[tokio::test]
    async fn run_id_namespaces_are_isolated() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        let in_run_a = store.new_memo("run-a", 0);
        let in_run_b = store.new_memo("run-b", 0);
        in_run_a.put("k", b"value-a").await.unwrap();
        in_run_b.put("k", b"value-b").await.unwrap();
        assert_eq!(in_run_a.get("k").await.unwrap(), Some(b"value-a".to_vec()));
        assert_eq!(in_run_b.get("k").await.unwrap(), Some(b"value-b".to_vec()));
    }

    #[tokio::test]
    async fn step_number_namespaces_are_isolated() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        let at_step_0 = store.new_memo("run-1", 0);
        let at_step_1 = store.new_memo("run-1", 1);
        at_step_0.put("k", b"step-0").await.unwrap();
        at_step_1.put("k", b"step-1").await.unwrap();
        assert_eq!(at_step_0.get("k").await.unwrap(), Some(b"step-0".to_vec()));
        assert_eq!(at_step_1.get("k").await.unwrap(), Some(b"step-1".to_vec()));
    }

    #[tokio::test]
    async fn distinct_user_keys_map_to_distinct_entries() {
        let memo = make_memo();
        memo.put("k1", b"one").await.unwrap();
        memo.put("k2", b"two").await.unwrap();
        assert_eq!(memo.get("k1").await.unwrap(), Some(b"one".to_vec()));
        assert_eq!(memo.get("k2").await.unwrap(), Some(b"two".to_vec()));
    }

    #[tokio::test]
    async fn awkward_user_keys_round_trip() {
        // Keys with `/`, spaces, and non-ASCII should all work because
        // they're hashed before becoming a path segment.
        let memo = make_memo();
        let keys = [
            "",
            "with/slash",
            "with spaces",
            "üñíçødé",
            &"a".repeat(10_000),
        ];
        for (i, key) in keys.iter().enumerate() {
            let expected = format!("v{i}").into_bytes();
            memo.put(key, &expected).await.unwrap();
            assert_eq!(memo.get(key).await.unwrap(), Some(expected));
        }
    }

    #[tokio::test]
    async fn empty_value_round_trips() {
        let memo = make_memo();
        memo.put("k", b"").await.unwrap();
        assert_eq!(memo.get("k").await.unwrap(), Some(Vec::new()));
    }

    #[tokio::test]
    async fn content_put_then_content_get_round_trips() {
        let memo = make_memo();
        let input = ContentInput {
            operation: "draft",
            payload: b"hello",
        };

        memo.content_put(&input, b"value").await.unwrap();

        assert_eq!(
            memo.content_get(&input).await.unwrap(),
            Some(b"value".to_vec()),
        );
    }

    #[tokio::test]
    async fn content_key_distinguishes_serialized_inputs() {
        let memo = make_memo();
        let first = ContentInput {
            operation: "draft",
            payload: b"hello",
        };
        let second = ContentInput {
            operation: "review",
            payload: b"hello",
        };

        memo.content_put(&first, b"first").await.unwrap();

        assert_eq!(
            memo.content_get(&first).await.unwrap(),
            Some(b"first".to_vec()),
        );
        assert!(memo.content_get(&second).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn content_entries_remain_step_scoped() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        let at_step_0 = store.new_memo("run-1", 0);
        let at_step_1 = store.new_memo("run-1", 1);
        let input = ContentInput {
            operation: "draft",
            payload: b"hello",
        };

        at_step_0.content_put(&input, b"step-0").await.unwrap();

        assert_eq!(
            at_step_0.content_get(&input).await.unwrap(),
            Some(b"step-0".to_vec()),
        );
        assert!(at_step_1.content_get(&input).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn step_output_entries_are_scoped_by_payload_hash() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");

        store
            .put_step_output("run-1", 0, b"payload-a", b"out-a")
            .await
            .unwrap();

        assert_eq!(
            store
                .get_step_output("run-1", 0, b"payload-a")
                .await
                .unwrap(),
            Some(b"out-a".to_vec()),
        );
        assert!(
            store
                .get_step_output("run-1", 0, b"payload-b")
                .await
                .unwrap()
                .is_none(),
        );
    }

    #[tokio::test]
    async fn clear_memos_for_run_removes_step_output_entries() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        store.new_memo("run-1", 0).put("k", b"memo").await.unwrap();
        store
            .put_step_output("run-1", 0, b"payload", b"out")
            .await
            .unwrap();

        let deleted = store.clear_memos_for_run("run-1").await.unwrap();

        assert_eq!(deleted, 2);
        assert!(store.new_memo("run-1", 0).get("k").await.unwrap().is_none());
        assert!(
            store
                .get_step_output("run-1", 0, b"payload")
                .await
                .unwrap()
                .is_none(),
        );
    }

    #[tokio::test]
    async fn content_key_reports_serialization_errors() {
        struct BadSerialize;

        impl Serialize for BadSerialize {
            fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(serde::ser::Error::custom("serialization failed"))
            }
        }

        let memo = make_memo();
        assert!(matches!(
            memo.content_get(&BadSerialize).await,
            Err(Error::Serialization(_)),
        ));
    }

    #[tokio::test]
    async fn instances_sharing_a_backing_store_see_the_same_entries() {
        // Two MemoStores over the same object store + prefix yield
        // memos that observe each other's writes -- the storage is
        // the source of truth, not any in-memory state.
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = MemoStore::new(backing.clone(), "memo").new_memo("run-1", 0);
        let reader = MemoStore::new(backing, "memo").new_memo("run-1", 0);
        writer.put("k", b"shared").await.unwrap();
        assert_eq!(reader.get("k").await.unwrap(), Some(b"shared".to_vec()));
    }

    #[tokio::test]
    async fn clear_memos_for_run_removes_only_that_runs_entries() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = MemoStore::new(backing, "memo");
        let in_run_a = store.new_memo("run-a", 0);
        let in_run_a_step1 = store.new_memo("run-a", 1);
        let in_run_b = store.new_memo("run-b", 0);
        in_run_a.put("k", b"a-0").await.unwrap();
        in_run_a_step1.put("k", b"a-1").await.unwrap();
        in_run_b.put("k", b"b-0").await.unwrap();

        let deleted = store.clear_memos_for_run("run-a").await.unwrap();
        assert_eq!(deleted, 2);

        assert_eq!(in_run_a.get("k").await.unwrap(), None);
        assert_eq!(in_run_a_step1.get("k").await.unwrap(), None);
        assert_eq!(in_run_b.get("k").await.unwrap(), Some(b"b-0".to_vec()));
    }

    #[tokio::test]
    async fn clear_memos_for_run_returns_zero_when_nothing_to_delete() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        let deleted = store
            .clear_memos_for_run("run-with-no-memos")
            .await
            .unwrap();
        assert_eq!(deleted, 0);
    }

    #[tokio::test]
    async fn clear_memos_for_run_does_not_match_run_id_as_prefix() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        store.new_memo("run", 0).put("k", b"short").await.unwrap();
        store
            .new_memo("run-suffix", 0)
            .put("k", b"long")
            .await
            .unwrap();

        let deleted = store.clear_memos_for_run("run").await.unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(
            store.new_memo("run-suffix", 0).get("k").await.unwrap(),
            Some(b"long".to_vec()),
        );
    }

    #[tokio::test]
    async fn write_terminal_marker_then_list_returns_it() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        store
            .write_terminal_marker("run-1", 1_700_000_000_000)
            .await
            .unwrap();

        let terminals = store.list_terminal_markers().await.unwrap();
        assert_eq!(terminals.len(), 1);
        assert_eq!(terminals[0].run_id, "run-1");
        assert_eq!(terminals[0].terminal_at_ms, 1_700_000_000_000);
    }

    #[tokio::test]
    async fn list_terminal_markers_is_empty_when_none_written() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        let terminals = store.list_terminal_markers().await.unwrap();
        assert!(terminals.is_empty());
    }

    #[tokio::test]
    async fn list_terminal_markers_returns_all() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        store.write_terminal_marker("run-a", 1_000).await.unwrap();
        store.write_terminal_marker("run-b", 2_000).await.unwrap();
        store.write_terminal_marker("run-c", 3_000).await.unwrap();

        let mut terminals = store.list_terminal_markers().await.unwrap();
        terminals.sort_by_key(|t| t.terminal_at_ms);
        assert_eq!(
            terminals,
            vec![
                TerminalMarker {
                    run_id: "run-a".into(),
                    terminal_at_ms: 1_000
                },
                TerminalMarker {
                    run_id: "run-b".into(),
                    terminal_at_ms: 2_000
                },
                TerminalMarker {
                    run_id: "run-c".into(),
                    terminal_at_ms: 3_000
                },
            ],
        );
    }

    #[tokio::test]
    async fn delete_terminal_marker_removes_only_the_named_one() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        store.write_terminal_marker("run-a", 1_000).await.unwrap();
        store.write_terminal_marker("run-b", 2_000).await.unwrap();

        store
            .delete_terminal_marker(&TerminalMarker {
                run_id: "run-a".into(),
                terminal_at_ms: 1_000,
            })
            .await
            .unwrap();

        let terminals = store.list_terminal_markers().await.unwrap();
        assert_eq!(terminals.len(), 1);
        assert_eq!(terminals[0].run_id, "run-b");
    }

    #[tokio::test]
    async fn delete_terminal_marker_succeeds_on_missing() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        store
            .delete_terminal_marker(&TerminalMarker {
                run_id: "nope".into(),
                terminal_at_ms: 1_000,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_terminal_marker_is_idempotent() {
        // A second delete of an already-deleted marker is the path
        // a crash-and-retry sweeper takes when it recovers mid-cleanup;
        // both deletes must succeed.
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        let marker = TerminalMarker {
            run_id: "run-1".into(),
            terminal_at_ms: 1_000,
        };
        store
            .write_terminal_marker(&marker.run_id, marker.terminal_at_ms)
            .await
            .unwrap();
        store.delete_terminal_marker(&marker).await.unwrap();
        store.delete_terminal_marker(&marker).await.unwrap();
    }

    #[tokio::test]
    async fn terminal_markers_and_memos_do_not_collide() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        store.new_memo("run-1", 0).put("k", b"v").await.unwrap();
        store.write_terminal_marker("run-1", 1_000).await.unwrap();

        // Memo survives terminal marking.
        assert_eq!(
            store.new_memo("run-1", 0).get("k").await.unwrap(),
            Some(b"v".to_vec()),
        );
        // Terminal marker survives memo writes.
        let terminals = store.list_terminal_markers().await.unwrap();
        assert_eq!(terminals.len(), 1);
        // clear_memos_for_run does not touch terminal markers.
        store.clear_memos_for_run("run-1").await.unwrap();
        let terminals = store.list_terminal_markers().await.unwrap();
        assert_eq!(terminals.len(), 1);
    }
}

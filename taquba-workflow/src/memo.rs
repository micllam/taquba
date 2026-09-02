//! Per-step durable key-value store for memoizing within-step side
//! effects, backed by object storage.
//!
//! [`Memo`] makes within-step side effects retry-safe. Taquba delivers
//! at-least-once, so a step may run more than once if its lease expires
//! before ack; without a durable place to record intermediate results,
//! expensive operations (LLM calls, paid external APIs, multi-step side
//! effects) silently re-run on each retry.
//!
//! Each per-step memo entry is keyed by `(run_id, step_number,
//! user_key)`, so distinct steps and runs see independent namespaces; a
//! run-scoped memo drops the step dimension and is shared by every step
//! of its run. User keys are SHA-256-hashed before becoming
//! object-store path segments so any string is a valid key regardless
//! of length or characters.
//!
//! # Layout
//!
//! [`MemoStore`] owns a single object-store prefix and partitions it into
//! two sub-prefixes:
//!
//! - `<prefix>/memos/<run_id>/<step_number>/<sha256(user_key)>`: per-step
//!   memo entries written by [`Memo::put`]. A content-addressed entry
//!   ([`Memo::content_put`]) is stored under the user key
//!   `content:<sha256(msgpack(input))>`, so its path segment is the
//!   digest of that key.
//! - `<prefix>/memos/<run_id>/run/<sha256(user_key)>`: run-scoped memo
//!   entries; the `run` segment sits beside the numeric step segments,
//!   so [`MemoStore::clear_memos_for_run`] removes both kinds together.
//! - `<prefix>/step-outputs/<run_id>/<step_number>/<sha256(step_payload)>`:
//!   step-output replay entries written by the workflow runtime when
//!   enabled.
//!
//! # Cleanup
//!
//! The [`Memo`] primitive has no lifecycle management of its own.
//! [`MemoStore::clear_memos_for_run`] removes every memo entry and
//! step-output replay entry for a given run. Deciding *which* runs are
//! eligible is the caller's concern: the workflow runtime records a
//! terminal marker for each finished run in the queue's key-value
//! namespace, in the same transaction that settles the run, and its
//! retention sweep pairs that marker with `clear_memos_for_run`.

use std::future::Future;
use std::sync::Arc;

use futures_util::StreamExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use taquba::object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, path::Path};

use crate::error::{Error, Result};
use crate::keys::hex_sha256;

/// Backing store for [`Memo`] entries, parametrised by an
/// [`ObjectStore`] and a path prefix. Builds per-step [`Memo`]
/// views via [`MemoStore::new_memo`].
///
/// Owns the memo and step-output sub-prefixes; see the module docs for
/// the path layout.
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
    /// Memo entries live under `<prefix>/memos/...` and step-output
    /// replay entries under `<prefix>/step-outputs/...`; the prefix
    /// should not overlap with the queue's SlateDB path or with any
    /// other consumer of the same store.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    /// Read the object at `path`, or `Ok(None)` when it does not
    /// exist. A missing object is not an error: the retention sweep
    /// may remove any entry at any time and every reader tolerates
    /// absence by re-executing.
    async fn read_opt(&self, path: &Path) -> Result<Option<Vec<u8>>> {
        match self.store.get(path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(Some(bytes.to_vec()))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(Error::Store(err)),
        }
    }

    /// Write `value` at `path`, overwriting any prior object.
    async fn write(&self, path: &Path, value: &[u8]) -> Result<()> {
        self.store.put(path, value.to_vec().into()).await?;
        Ok(())
    }

    /// Build a [`Memo`] bound to `(run_id, step_number)`.
    pub fn new_memo(&self, run_id: impl Into<String>, step_number: u32) -> Memo {
        Memo::new(self.clone(), run_id, MemoScope::Step(step_number))
    }

    /// Build a [`Memo`] scoped to `run_id` as a whole, shared by every
    /// step of the run.
    pub fn new_run_memo(&self, run_id: impl Into<String>) -> Memo {
        Memo::new(self.clone(), run_id, MemoScope::Run)
    }

    /// Delete every memo entry and runtime step-output replay entry for
    /// `run_id`. Returns the number of entries removed. Errors during
    /// individual deletes are logged (best-effort cleanup) but do not
    /// stop the sweep; an aggregated error is returned only if a list
    /// operation fails. Fails with [`Error::InvalidRunId`] for an invalid
    /// run id; an empty one would resolve to the memo prefix itself.
    pub async fn clear_memos_for_run(&self, run_id: &str) -> Result<usize> {
        crate::keys::validate_run_id(run_id)?;
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
        self.read_opt(&self.step_output_path(run_id, step_number, step_payload))
            .await
    }

    pub(crate) async fn put_step_output(
        &self,
        run_id: &str,
        step_number: u32,
        step_payload: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.write(
            &self.step_output_path(run_id, step_number, step_payload),
            value,
        )
        .await
    }

    fn memo_path(&self, run_id: &str, scope: MemoScope, key: &str) -> Path {
        let segment = match scope {
            MemoScope::Step(step_number) => step_number.to_string(),
            MemoScope::Run => "run".to_string(),
        };
        self.memos_run_prefix(run_id)
            .join(segment)
            .join(hex_sha256(&[key.as_bytes()]))
    }

    fn memos_run_prefix(&self, run_id: &str) -> Path {
        Path::from(format!("{}/memos/{}", self.prefix, run_id))
    }

    fn step_outputs_run_prefix(&self, run_id: &str) -> Path {
        Path::from(format!("{}/step-outputs/{}", self.prefix, run_id))
    }

    fn step_output_path(&self, run_id: &str, step_number: u32, step_payload: &[u8]) -> Path {
        self.step_outputs_run_prefix(run_id)
            .join(step_number.to_string())
            .join(hex_sha256(&[step_payload]))
    }
}

/// The namespace a [`Memo`] is bound to within its run: one step, or
/// the run as a whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemoScope {
    Step(u32),
    Run,
}

/// A view onto a [`MemoStore`] scoped to a `(run_id, step_number)`
/// pair, or to a run as a whole.
#[derive(Clone)]
pub struct Memo {
    store: MemoStore,
    run_id: String,
    scope: MemoScope,
}

impl Memo {
    fn new(store: MemoStore, run_id: impl Into<String>, scope: MemoScope) -> Self {
        Self {
            store,
            run_id: run_id.into(),
            scope,
        }
    }

    /// The run identifier this memo is bound to.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// The step number this memo is bound to; `None` for a run-scoped
    /// memo.
    pub fn step_number(&self) -> Option<u32> {
        match self.scope {
            MemoScope::Step(step_number) => Some(step_number),
            MemoScope::Run => None,
        }
    }

    /// Read a previously stored value for `key`, or `Ok(None)` if
    /// none has been written.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.store
            .read_opt(&self.store.memo_path(&self.run_id, self.scope, key))
            .await
    }

    /// Store `value` for `key`, overwriting any prior value.
    ///
    /// Overwrite is intentional: a retry that produces the same value
    /// is idempotent. A retry that produces a *different* value
    /// indicates the handler isn't perfectly idempotent; the memo
    /// reflects whatever the most recent attempt wrote.
    pub async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        self.store
            .write(&self.store.memo_path(&self.run_id, self.scope, key), value)
            .await
    }

    /// Return the value stored under `key`, or run `compute`, store its
    /// value under `key` and return it.
    ///
    /// The value is encoded as MessagePack with named fields. An entry
    /// that fails to decode as `R` is treated as absent: `compute` runs
    /// and its value overwrites the entry. An error from `compute` is
    /// returned without storing anything, so a later call runs `compute`
    /// again.
    pub async fn memoized<R, F, E>(&self, key: &str, compute: F) -> std::result::Result<R, E>
    where
        R: Serialize + DeserializeOwned,
        F: Future<Output = std::result::Result<R, E>>,
        E: From<Error>,
    {
        if let Some(bytes) = self.get(key).await? {
            match rmp_serde::from_slice::<R>(&bytes) {
                Ok(value) => return Ok(value),
                Err(err) => tracing::warn!(
                    run_id = %self.run_id,
                    key = %key,
                    error = %err,
                    "memo entry failed to decode; recomputing",
                ),
            }
        }
        let value = compute.await?;
        let bytes = rmp_serde::to_vec_named(&value).map_err(Error::Serialization)?;
        self.put(key, &bytes).await?;
        Ok(value)
    }

    /// [`Self::memoized`] under [`Self::content_key`] of `input`.
    pub async fn memoized_by_content<K, R, F, E>(
        &self,
        input: &K,
        compute: F,
    ) -> std::result::Result<R, E>
    where
        K: Serialize + ?Sized,
        R: Serialize + DeserializeOwned,
        F: Future<Output = std::result::Result<R, E>>,
        E: From<Error>,
    {
        let key = Self::content_key(input)?;
        self.memoized(&key, compute).await
    }

    /// Derive the memo key for a content-addressed entry: `content:`
    /// followed by the hex SHA-256 digest of `input` encoded as
    /// MessagePack with named fields.
    ///
    /// The key is stable only when `input` serializes
    /// deterministically; types with unordered iteration, such as
    /// `HashMap`, can serialize the same logical content into different
    /// bytes and therefore different keys. If several logical
    /// operations may receive identical inputs, include an operation
    /// name in the serialized input.
    pub fn content_key<T>(input: &T) -> Result<String>
    where
        T: Serialize + ?Sized,
    {
        let bytes = rmp_serde::to_vec_named(input)?;
        Ok(format!("content:{}", hex_sha256(&[&bytes])))
    }

    /// Read the memo entry stored under [`Self::content_key`] of
    /// `input`.
    pub async fn content_get<T>(&self, input: &T) -> Result<Option<Vec<u8>>>
    where
        T: Serialize + ?Sized,
    {
        let key = Self::content_key(input)?;
        self.get(&key).await
    }

    /// Store `value` under [`Self::content_key`] of `input`.
    pub async fn content_put<T>(&self, input: &T, value: &[u8]) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        let key = Self::content_key(input)?;
        self.put(&key, value).await
    }
}

impl std::fmt::Debug for Memo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Memo")
            .field("run_id", &self.run_id)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

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
    async fn put_get_round_trips() {
        let memo = make_memo();
        assert_eq!(memo.get("missing").await.unwrap(), None);
        memo.put("k", b"first").await.unwrap();
        assert_eq!(memo.get("k").await.unwrap(), Some(b"first".to_vec()));
        memo.put("k", b"second").await.unwrap();
        assert_eq!(memo.get("k").await.unwrap(), Some(b"second".to_vec()));
        memo.put("k2", b"").await.unwrap();
        assert_eq!(memo.get("k2").await.unwrap(), Some(Vec::new()));
        assert_eq!(memo.get("k").await.unwrap(), Some(b"second".to_vec()));
    }

    #[tokio::test]
    async fn run_and_step_namespaces_are_isolated() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        let in_run_a = store.new_memo("run-a", 0);
        let in_run_a_step_1 = store.new_memo("run-a", 1);
        let in_run_b = store.new_memo("run-b", 0);
        in_run_a.put("k", b"a-0").await.unwrap();
        in_run_a_step_1.put("k", b"a-1").await.unwrap();
        in_run_b.put("k", b"b-0").await.unwrap();
        assert_eq!(in_run_a.get("k").await.unwrap(), Some(b"a-0".to_vec()));
        assert_eq!(
            in_run_a_step_1.get("k").await.unwrap(),
            Some(b"a-1".to_vec()),
        );
        assert_eq!(in_run_b.get("k").await.unwrap(), Some(b"b-0".to_vec()));
    }

    #[tokio::test]
    async fn a_run_memo_is_scoped_beside_the_step_memos() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        let at_step_0 = store.new_memo("run-1", 0);
        let for_run = store.new_run_memo("run-1");
        at_step_0.put("k", b"step-0").await.unwrap();
        for_run.put("k", b"run").await.unwrap();
        assert_eq!(at_step_0.get("k").await.unwrap(), Some(b"step-0".to_vec()));
        assert_eq!(for_run.get("k").await.unwrap(), Some(b"run".to_vec()));
        assert_eq!(
            store.new_run_memo("run-1").get("k").await.unwrap(),
            Some(b"run".to_vec()),
        );
        assert_eq!(store.new_run_memo("run-2").get("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn memoized_runs_the_computation_once() {
        let memo = make_memo();
        let calls = AtomicU32::new(0);
        let compute = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(7u32)
        };

        assert_eq!(memo.memoized("k", compute()).await.unwrap(), 7);
        assert_eq!(memo.memoized("k", compute()).await.unwrap(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            memo.get("k").await.unwrap(),
            Some(rmp_serde::to_vec_named(&7u32).unwrap()),
        );
    }

    #[tokio::test]
    async fn memoized_stores_nothing_for_a_failed_computation() {
        let memo = make_memo();
        let failed = memo
            .memoized("k", async { Err::<u32, Error>(Error::EffectsSealed) })
            .await;

        assert!(matches!(failed, Err(Error::EffectsSealed)));
        assert_eq!(memo.get("k").await.unwrap(), None);
        assert_eq!(
            memo.memoized("k", async { Ok::<_, Error>(7u32) })
                .await
                .unwrap(),
            7,
        );
    }

    #[tokio::test]
    async fn a_memo_entry_that_fails_to_decode_is_recomputed() {
        let memo = make_memo();
        memo.put("k", b"not msgpack for a string").await.unwrap();

        let value: String = memo
            .memoized("k", async { Ok::<_, Error>("fresh".to_string()) })
            .await
            .unwrap();

        assert_eq!(value, "fresh");
        assert_eq!(
            memo.get("k").await.unwrap(),
            Some(rmp_serde::to_vec_named("fresh").unwrap()),
        );
    }

    #[tokio::test]
    async fn memoized_by_content_stores_under_the_content_key() {
        let memo = make_memo();
        let input = ContentInput {
            operation: "draft",
            payload: b"hello",
        };
        let calls = AtomicU32::new(0);
        let compute = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(7u32)
        };

        assert_eq!(
            memo.memoized_by_content(&input, compute()).await.unwrap(),
            7
        );
        assert_eq!(
            memo.memoized_by_content(&input, compute()).await.unwrap(),
            7
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            memo.content_get(&input).await.unwrap(),
            Some(rmp_serde::to_vec_named(&7u32).unwrap()),
        );
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
    async fn entries_are_stored_at_the_documented_paths() {
        let backing = Arc::new(InMemory::new());
        let store = MemoStore::new(backing.clone(), "memo");
        store.new_memo("run-1", 0).put("k", b"step").await.unwrap();
        store.new_run_memo("run-1").put("k", b"run").await.unwrap();
        store
            .new_memo("run-1", 0)
            .content_put("hello", b"content")
            .await
            .unwrap();
        store
            .put_step_output("run-1", 0, b"payload", b"out")
            .await
            .unwrap();

        let mut paths = Vec::new();
        let mut listing = backing.list(None);
        while let Some(item) = listing.next().await {
            paths.push(item.unwrap().location.to_string());
        }
        paths.sort();

        // Digests are, in order: sha256("content:" + hex sha256 of the
        // MessagePack encoding of "hello"), sha256("k") twice and
        // sha256("payload").
        assert_eq!(
            paths,
            [
                "memo/memos/run-1/0/1adc4f8ba16f15ba2172dab9b84bb9ba73f5cf4f156f50df4dda663b4f9c61ba",
                "memo/memos/run-1/0/8254c329a92850f6d539dd376f4816ee2764517da5e0235514af433164480d7a",
                "memo/memos/run-1/run/8254c329a92850f6d539dd376f4816ee2764517da5e0235514af433164480d7a",
                "memo/step-outputs/run-1/0/239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5",
            ],
        );
    }

    #[tokio::test]
    async fn clear_memos_for_run_removes_step_output_and_run_memo_entries() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        store.new_memo("run-1", 0).put("k", b"memo").await.unwrap();
        store.new_run_memo("run-1").put("k", b"run").await.unwrap();
        store
            .put_step_output("run-1", 0, b"payload", b"out")
            .await
            .unwrap();

        let deleted = store.clear_memos_for_run("run-1").await.unwrap();

        assert_eq!(deleted, 3);
        assert!(store.new_memo("run-1", 0).get("k").await.unwrap().is_none());
        assert!(
            store
                .new_run_memo("run-1")
                .get("k")
                .await
                .unwrap()
                .is_none()
        );
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
        assert_eq!(store.clear_memos_for_run("run-a").await.unwrap(), 0);
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
}

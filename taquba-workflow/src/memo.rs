//! Per-step durable key-value store for memoizing within-step side
//! effects, backed by object storage.
//!
//! [`Memo`] makes within-step side effects retry-safe. Taquba delivers
//! at-least-once, so a step may run more than once if its lease expires
//! before ack; without a durable place to record intermediate results,
//! expensive operations (LLM calls, paid external APIs, multi-step side
//! effects) silently re-run on each retry.
//!
//! Each entry is keyed by `(run_id, step_number, user_key)`, so distinct
//! steps and runs see independent namespaces. User keys are
//! SHA-256-hashed before becoming object-store path segments so any
//! string is a valid key regardless of length or characters.
//!
//! This is a primitive: it has no lifecycle management of its own. Cleanup
//! is the caller's responsibility (typically tied to a workflow's
//! terminal hook).

use std::sync::Arc;

use sha2::{Digest, Sha256};
use taquba::object_store::{Error as ObjectStoreError, ObjectStore, path::Path};

use crate::error::{Error, Result};

/// Backing store for [`Memo`] entries, parametrised by an
/// [`ObjectStore`] and a path prefix. Builds per-step [`Memo`]
/// views via [`MemoStore::new_memo`].
///
/// Has no lifecycle management of its own; cleaning up entries is
/// the caller's responsibility.
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
    /// All keys written through this store live under `<prefix>/...`;
    /// the prefix should not overlap with the queue's SlateDB path or
    /// with any other consumer of the same store.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    /// Read a previously stored value for `(run_id, step_number, key)`,
    /// or `Ok(None)` if none has been written.
    async fn get(&self, run_id: &str, step_number: u32, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.path(run_id, step_number, key);
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
        let path = self.path(run_id, step_number, key);
        self.store.put(&path, value.to_vec().into()).await?;
        Ok(())
    }

    /// Build a [`Memo`] bound to `(run_id, step_number)`.
    pub fn new_memo(&self, run_id: impl Into<String>, step_number: u32) -> Memo {
        Memo::new(self.clone(), run_id, step_number)
    }

    /// Compose the full object-store path for a memo entry. User keys
    /// are hashed so any string maps to a fixed-shape path segment
    /// safe for every supported backend.
    fn path(&self, run_id: &str, step_number: u32, key: &str) -> Path {
        let key_hash = hex_sha256(key.as_bytes());
        Path::from(format!(
            "{}/{}/{}/{}",
            self.prefix, run_id, step_number, key_hash
        ))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use taquba::object_store::memory::InMemory;

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
}

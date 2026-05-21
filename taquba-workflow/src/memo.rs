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

/// Per-step durable key-value store for memoizing within-step side
/// effects, backed by object storage.
///
/// Makes within-step side effects retry-safe. Taquba delivers
/// at-least-once, so a step may run more than once if its lease expires
/// before ack; without a durable place to record intermediate results,
/// expensive operations (LLM calls, paid external APIs, multi-step side
/// effects) silently re-run on each retry. `Memo::put` records the
/// result of one such operation; on the next attempt, `Memo::get`
/// returns it, letting the runner skip the call.
///
/// Each entry is keyed by `(run_id, step_number, user_key)`, so distinct
/// steps and runs see independent namespaces. User keys are
/// SHA-256-hashed before becoming object-store path segments so any
/// string is a valid key regardless of length or characters.
///
/// `Memo` is cheap to clone (it holds an `Arc` to the object store and
/// a `String` prefix). It is a primitive: it has no lifecycle
/// management of its own. Cleanup is the caller's responsibility,
/// typically tied to a workflow's terminal hook.
#[derive(Clone)]
pub struct Memo {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl std::fmt::Debug for Memo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The object store doesn't implement Debug; show the prefix
        // (the operationally interesting part) and elide the rest.
        f.debug_struct("Memo")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl Memo {
    /// Build a `Memo` over the given object store and path prefix. All
    /// keys written through this `Memo` live under `<prefix>/...` in the
    /// store; the prefix should not overlap with the queue's SlateDB
    /// path or with any other consumer of the same store.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    /// Read a previously stored value for `(run_id, step_number, key)`.
    ///
    /// Returns `Ok(None)` when no value has been written for that key;
    /// `Ok(Some(bytes))` for a stored value; `Err(_)` for an
    /// object-store error other than not-found.
    pub async fn get(&self, run_id: &str, step_number: u32, key: &str) -> Result<Option<Vec<u8>>> {
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

    /// Store `value` against `(run_id, step_number, key)`, overwriting
    /// any prior value for the same key.
    ///
    /// Overwrite is intentional: a retry that produces the same value
    /// after a prior attempt's blob already exists is idempotent. A
    /// retry that produces a *different* value indicates the handler
    /// isn't perfectly idempotent; the cache reflects whatever the
    /// most recent attempt wrote.
    pub async fn put(&self, run_id: &str, step_number: u32, key: &str, value: &[u8]) -> Result<()> {
        let path = self.path(run_id, step_number, key);
        self.store.put(&path, value.to_vec().into()).await?;
        Ok(())
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
        Memo::new(Arc::new(InMemory::new()), "memo")
    }

    #[tokio::test]
    async fn get_returns_none_for_missing_key() {
        let memo = make_memo();
        assert_eq!(memo.get("run-1", 0, "missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn put_then_get_round_trips() {
        let memo = make_memo();
        memo.put("run-1", 0, "k", b"hello").await.unwrap();
        assert_eq!(
            memo.get("run-1", 0, "k").await.unwrap(),
            Some(b"hello".to_vec())
        );
    }

    #[tokio::test]
    async fn put_overwrites_prior_value() {
        let memo = make_memo();
        memo.put("run-1", 0, "k", b"first").await.unwrap();
        memo.put("run-1", 0, "k", b"second").await.unwrap();
        assert_eq!(
            memo.get("run-1", 0, "k").await.unwrap(),
            Some(b"second".to_vec())
        );
    }

    #[tokio::test]
    async fn run_id_namespaces_are_isolated() {
        let memo = make_memo();
        memo.put("run-a", 0, "k", b"value-a").await.unwrap();
        memo.put("run-b", 0, "k", b"value-b").await.unwrap();
        assert_eq!(
            memo.get("run-a", 0, "k").await.unwrap(),
            Some(b"value-a".to_vec())
        );
        assert_eq!(
            memo.get("run-b", 0, "k").await.unwrap(),
            Some(b"value-b".to_vec())
        );
    }

    #[tokio::test]
    async fn step_number_namespaces_are_isolated() {
        let memo = make_memo();
        memo.put("run-1", 0, "k", b"step-0").await.unwrap();
        memo.put("run-1", 1, "k", b"step-1").await.unwrap();
        assert_eq!(
            memo.get("run-1", 0, "k").await.unwrap(),
            Some(b"step-0".to_vec())
        );
        assert_eq!(
            memo.get("run-1", 1, "k").await.unwrap(),
            Some(b"step-1".to_vec())
        );
    }

    #[tokio::test]
    async fn distinct_user_keys_map_to_distinct_entries() {
        let memo = make_memo();
        memo.put("run-1", 0, "k1", b"one").await.unwrap();
        memo.put("run-1", 0, "k2", b"two").await.unwrap();
        assert_eq!(
            memo.get("run-1", 0, "k1").await.unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(
            memo.get("run-1", 0, "k2").await.unwrap(),
            Some(b"two".to_vec())
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
            memo.put("run-1", 0, key, &expected).await.unwrap();
            assert_eq!(memo.get("run-1", 0, key).await.unwrap(), Some(expected));
        }
    }

    #[tokio::test]
    async fn empty_value_round_trips() {
        let memo = make_memo();
        memo.put("run-1", 0, "k", b"").await.unwrap();
        assert_eq!(memo.get("run-1", 0, "k").await.unwrap(), Some(Vec::new()));
    }

    #[tokio::test]
    async fn instances_sharing_a_store_see_the_same_entries() {
        // Two Memo instances over the same store + prefix observe each
        // other's writes; the storage is the source of truth, not any
        // in-memory state on the Memo itself.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = Memo::new(store.clone(), "memo");
        let reader = Memo::new(store, "memo");
        writer.put("run-1", 0, "k", b"shared").await.unwrap();
        assert_eq!(
            reader.get("run-1", 0, "k").await.unwrap(),
            Some(b"shared".to_vec())
        );
    }
}

use std::sync::Arc;

use bytes::Bytes;
use taquba::Queue;

use crate::error::Result;

/// Read access to Taquba's caller KV namespace during a step.
///
/// Obtained through [`Step::kv`](crate::Step::kv). [`get`](Self::get)
/// answers from committed state: a value written by an earlier
/// settlement (a previous step's [`EffectsHandle`](crate::EffectsHandle)
/// writes, a [`RunSpec::kv_writes`](crate::RunSpec::kv_writes) entry, a
/// direct [`taquba::Queue::kv_put`]) is visible, and an effect staged by
/// the current step becomes visible only once this step's settlement
/// commits it. The read is also not transactional with that settlement:
/// a value read here can change before the step's outcome commits.
/// Delivery is at-least-once, so a read that misses (for example a
/// marker a crashed settlement never committed) re-executes work that
/// must be idempotent downstream.
///
/// The handle is cheap to clone and exposes no write or settlement
/// operation. Use [`KvReadHandle::detached`] when constructing a
/// [`Step`](crate::Step) in tests.
#[derive(Clone)]
pub struct KvReadHandle {
    queue: Option<Arc<Queue>>,
}

impl std::fmt::Debug for KvReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.queue {
            Some(_) => f.debug_struct("KvReadHandle").finish_non_exhaustive(),
            None => f.write_str("KvReadHandle::detached"),
        }
    }
}

impl KvReadHandle {
    /// Build a handle bound to no queue, for constructing a
    /// [`Step`](crate::Step) in tests. [`get`](Self::get) on a detached
    /// handle returns `Ok(None)` for every key.
    pub fn detached() -> Self {
        Self { queue: None }
    }

    pub(crate) fn for_delivery(queue: Arc<Queue>) -> Self {
        Self { queue: Some(queue) }
    }

    /// Read the committed value under `key` from the caller KV
    /// namespace, `None` when no value exists.
    ///
    /// # Errors
    ///
    /// [`Error::Queue`](crate::Error::Queue) when the underlying read
    /// fails.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        match &self.queue {
            Some(queue) => Ok(queue.kv_get(key).await?),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_detached_handle_reads_no_value() {
        let handle = KvReadHandle::detached();
        assert_eq!(handle.get(b"any").await.unwrap(), None);
    }
}

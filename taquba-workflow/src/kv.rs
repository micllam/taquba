use std::sync::Arc;

use bytes::Bytes;
use futures_util::TryStreamExt;
use taquba::{KvOrder, Queue, QueueView};

use crate::error::Result;
use crate::keys::{RUN_KV_ENTRY_PREFIX, RunId, run_kv_entries_range, run_kv_entry_key};

/// Read access to Taquba's caller KV namespace and to the run KV space of the
/// delivery's run during a step.
///
/// Obtained through [`Delivery::kv`](crate::Delivery::kv). [`get`](Self::get)
/// reads from committed state: a value written by an earlier settlement (a
/// previous step's [`EffectsHandle`](crate::EffectsHandle) writes, a
/// [`RunSpec::effects`](crate::RunSpec::effects) write, a direct
/// [`taquba::Queue::kv_put`]) is visible, and an effect staged by the current
/// step becomes visible only once this step's settlement commits it. The read
/// is also not transactional with that settlement: a value read here can change
/// before the step's outcome commits. Delivery is at-least-once, so a read that
/// misses (for example a marker a crashed settlement never committed)
/// re-executes work that must be idempotent downstream.
///
/// A clone of the handle increments a reference count. The handle does not
/// expose a write or settlement operation. Use [`KvReadHandle::detached`] when
/// constructing a [`Step`](crate::Step) in tests.
#[derive(Clone)]
pub struct KvReadHandle {
    queue: Option<(Arc<Queue>, RunId)>,
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
    /// Build a handle that is not bound to a queue, for constructing a
    /// [`Step`](crate::Step) in tests. [`get`](Self::get) and
    /// [`get_run_kv`](Self::get_run_kv) on a detached handle return `Ok(None)`
    /// for every key.
    pub fn detached() -> Self {
        Self { queue: None }
    }

    /// A handle for a delivery of the run `run_id` over `queue`.
    pub(crate) fn for_delivery(queue: Arc<Queue>, run_id: RunId) -> Self {
        Self {
            queue: Some((queue, run_id)),
        }
    }

    /// Read the committed value under `key` from the caller KV namespace,
    /// `None` when no value exists.
    ///
    /// # Errors
    ///
    /// [`Error::Queue`](crate::Error::Queue) when the underlying read fails.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        match &self.queue {
            Some((queue, _)) => Ok(queue.view().kv_get(key).await?),
            None => Ok(None),
        }
    }

    /// Read the committed value under `key` from the run KV space of the
    /// delivery's run, `None` when no value exists.
    ///
    /// # Errors
    ///
    /// [`Error::Queue`](crate::Error::Queue) when the underlying read fails.
    pub async fn get_run_kv(&self, key: &[u8]) -> Result<Option<Bytes>> {
        match &self.queue {
            Some((queue, run_id)) => {
                Ok(queue.view().kv_get(&run_kv_entry_key(run_id, key)).await?)
            }
            None => Ok(None),
        }
    }
}

/// The keys of the run KV entries of `run_id` through `view`, for a sweep that
/// removes the run.
pub(crate) async fn run_kv_entry_keys(view: &QueueView, run_id: &RunId) -> Result<Vec<Vec<u8>>> {
    let range = run_kv_entries_range(run_id);
    Ok(view
        .kv_entries(
            RUN_KV_ENTRY_PREFIX,
            range,
            KvOrder::Ascending,
            RUN_KV_PAGE_SIZE,
        )
        .map_ok(|(key, _)| key)
        .try_collect()
        .await?)
}

/// The page size of [`run_kv_entry_keys`].
const RUN_KV_PAGE_SIZE: usize = 256;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_detached_handle_reads_no_value() {
        let handle = KvReadHandle::detached();
        assert_eq!(handle.get(b"any").await.unwrap(), None);
    }
}

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

/// Per-queue resume point for the next claim scan.
///
/// Stores the `pending:` key of the most recently claimed job per
/// queue. The claim path scans from immediately after the recorded
/// key, skipping the tombstone band left by previously claimed (and
/// deleted) `pending:` entries. The cursor is invalidated whenever
/// a `pending:` write would land at or before it, so it never
/// causes the claim path to skip a key that should be next.
///
/// Shared across the queue, reaper, and scheduler via `Clone`; all
/// clones reference the same in-memory map. Not persisted: on
/// process restart the first claim falls back to a prefix scan and
/// re-warms the cursor naturally.
#[derive(Clone, Default)]
pub(crate) struct ClaimCursor {
    inner: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl ClaimCursor {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get(&self, queue: &str) -> Option<Bytes> {
        self.inner.lock().unwrap().get(queue).cloned()
    }

    pub(crate) fn set(&self, queue: &str, cursor: Bytes) {
        self.inner
            .lock()
            .unwrap()
            .insert(queue.to_string(), cursor);
    }

    pub(crate) fn clear(&self, queue: &str) {
        self.inner.lock().unwrap().remove(queue);
    }

    /// Drop the cursor for `queue` if `new_key` would sort at or
    /// before it. Every site that writes a `pending:` key (enqueue,
    /// nack-requeue, dead-job requeue, reaper-requeue, scheduler
    /// promotion) calls this so the cursor never causes the claim
    /// path to skip a key that should be next.
    pub(crate) fn invalidate_if_at_or_before(&self, queue: &str, new_key: &str) {
        let mut map = self.inner.lock().unwrap();
        if let Some(cursor) = map.get(queue) {
            if new_key.as_bytes() <= cursor.as_ref() {
                map.remove(queue);
            }
        }
    }
}

//! The state of an open queue that its handle and background tasks operate on.

use std::collections::HashMap;
use std::sync::Arc;

use slatedb::Db;

use crate::claim_cursor::ClaimCursor;
use crate::clock::Clock;
use crate::completion::CompletionWaiters;
use crate::lease_registry::LeaseRegistry;
use crate::payload_store::PayloadStore;
use crate::queue::QueueConfig;

/// The per-queue configurations of an open queue: a default and the
/// overrides keyed by queue name.
pub(crate) struct QueueConfigs {
    default: QueueConfig,
    per_queue: HashMap<String, QueueConfig>,
}

impl QueueConfigs {
    pub(crate) fn new(default: QueueConfig, per_queue: HashMap<String, QueueConfig>) -> Self {
        Self { default, per_queue }
    }

    /// The configuration of `queue`: its override, or the default.
    pub(crate) fn get(&self, queue: &str) -> &QueueConfig {
        self.per_queue.get(queue).unwrap_or(&self.default)
    }

    /// The default and every override.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &QueueConfig> {
        std::iter::once(&self.default).chain(self.per_queue.values())
    }
}

/// The handles every component of an open queue operates on: the
/// store, the clock, the configurations and the in-process registries.
/// Held as one `Arc` by the [`Queue`](crate::Queue) and by each
/// background task.
pub(crate) struct QueueCore {
    pub(crate) db: Arc<Db>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) configs: QueueConfigs,
    pub(crate) claim_cursor: ClaimCursor,
    pub(crate) lease_registry: LeaseRegistry,
    pub(crate) completion_waiters: Arc<CompletionWaiters>,
    pub(crate) payload_store: Arc<PayloadStore>,
}

impl QueueCore {
    /// Current time in milliseconds since the UNIX epoch, read from
    /// the configured [`Clock`].
    pub(crate) fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }
}

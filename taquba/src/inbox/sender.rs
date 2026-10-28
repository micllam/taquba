//! The sender of inbox entries: another process writes an entry as one object
//! in the inbox.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use slatedb::object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, path::Path};
use tracing::{debug, warn};
use ulid::Ulid;

use crate::error::{Error, Result};
use crate::inbox::entry::{InboxEntry, validate_id};
use crate::inbox::{EntryKey, EntryPath, entry_path, list_entries, queue_prefix, touch_path};
use crate::keys::QueueName;
use crate::ulids::MonotonicUlids;

/// The orders of the entries of every sender in the process.
static ORDERS: MonotonicUlids = MonotonicUlids::new();

/// The writes of the touch object per send, the first and one retry. A failed
/// touch delays the entry to the next listing of the writer, at most
/// [`InboxOptions::interval`](crate::InboxOptions::interval) later, and the
/// send still succeeds. The retry covers a write that a store rejects while
/// other senders write the touch object. Each further attempt would delay the
/// return of the send by [`TOUCH_RETRY_DELAY`] to save at most one listing
/// interval.
const TOUCH_ATTEMPTS: u32 = 2;

/// The delay before a retry of the touch, longer than the interval at which a
/// store throttles the writes to one object.
const TOUCH_RETRY_DELAY: Duration = Duration::from_secs(1);

/// An entry in the inbox, as [`InboxSender::pending`] lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingEntry {
    /// The id of the entry.
    pub id: String,
    /// The entry, of the kind in its path.
    pub entry: InboxEntry,
    /// When the object store wrote the entry, as milliseconds from the Unix
    /// epoch on the clock of the store.
    pub sent_at_ms: u64,
}

/// A sender of inbox entries over the object store of an inbox. It writes one
/// object per entry at `{path}/v1/{queue}/{kind}/{order}.{id}` with the body of
/// the entry and never opens the store of the queue. `order` is a ULID that the
/// sender generates at each send. After the entry, it touches the inbox: it
/// overwrites the touch object at `{path}/touch` with a fresh ULID, so the
/// writer lists the inbox at its next poll of the touch object. The writer
/// applies the entry at its next pass, and the sender reads the outcome through
/// [`QueueView::inbox_record`](crate::QueueView::inbox_record) of a
/// [`QueueReader`](crate::QueueReader): the outcome of the operation of the
/// entry's kind, or a rejection with its reason.
///
/// An entry is an [`InboxEntry`] for a queue of
/// [`InboxOptions::queues`](crate::InboxOptions::queues). A layer receives its
/// own requests as jobs that a sender enqueues. A sender in another language
/// writes the body that [`InboxEntry`] states, and `order` as a ULID in its
/// 26-character text form from a monotonic generator. A sender and its writer
/// must run the same minor version of the crate, because the format of an
/// entry, as the layout of the store, changes between minor releases.
///
/// The writer applies the entries of a listing in the order of their ULIDs and
/// then by id. The ULIDs of every sender in a process increase with call order.
/// Entries sent one after the other, each send awaited before the next, apply
/// in send order. The sends of two processes apply in the order of their
/// clocks. An entry that depends on an entry of another process must be sent
/// after the record of the first exists. Its sender reads that record through
/// [`QueueView::inbox_record`](crate::QueueView::inbox_record). An entry
/// written after a pass that applied a later ULID applies at a later pass.
///
/// An id is unique per queue, across kinds. The record of an applied entry is
/// at its queue and its id, and a later entry of any kind at that queue and id
/// is deleted without an effect.
#[derive(Clone)]
pub struct InboxSender {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl InboxSender {
    /// A sender over `store` at the inbox path `path`: the
    /// [`InboxOptions::path`](crate::InboxOptions::path) of the writer, or
    /// [`default_inbox_path`](crate::default_inbox_path) of the queue's path.
    pub fn new(store: Arc<dyn ObjectStore>, path: impl Into<String>) -> Self {
        Self {
            store,
            prefix: path.into(),
        }
    }

    /// Write `entry` for `queue` with the ULID of the send as its id, and
    /// return the id.
    pub async fn send(&self, queue: &str, entry: impl Into<InboxEntry>) -> Result<String> {
        let order = ORDERS.next(SystemTime::now());
        let id = order.to_string();
        self.send_at(queue, order, &id, entry.into()).await?;
        Ok(id)
    }

    /// Write `entry` for `queue` with the caller's `id`: `[A-Za-z0-9_-]`, 1 to
    /// [`MAX_INBOX_ID_LEN`](crate::MAX_INBOX_ID_LEN) bytes. Each call writes an
    /// object of its own, and of the entries of one id at `queue` the first in
    /// send order applies. A later entry of the id is deleted without an effect
    /// while the record of the first is retained.
    ///
    /// A failed touch is retried once after a second and then logged, and the
    /// send succeeds: the writer applies the entry at its next listing of
    /// [`InboxOptions::interval`](crate::InboxOptions::interval).
    pub async fn send_with_id(
        &self,
        queue: &str,
        id: &str,
        entry: impl Into<InboxEntry>,
    ) -> Result<()> {
        self.send_at(queue, ORDERS.next(SystemTime::now()), id, entry.into())
            .await
    }

    /// Write `entry` at the path of `order` and `id`, then touch the inbox.
    async fn send_at(&self, queue: &str, order: Ulid, id: &str, entry: InboxEntry) -> Result<()> {
        QueueName::new(queue)?;
        validate_id(id)?;
        let body = entry.encode();
        let path = Path::from(entry_path(&self.prefix, queue, entry.kind(), order, id));
        self.store
            .put(&path, body.into())
            .await
            .map_err(Error::InboxStore)?;
        self.touch().await;
        Ok(())
    }

    /// Overwrite the touch object with a fresh ULID, in up to
    /// [`TOUCH_ATTEMPTS`] writes [`TOUCH_RETRY_DELAY`] apart.
    async fn touch(&self) {
        let path = Path::from(touch_path(&self.prefix));
        for attempt in 1..=TOUCH_ATTEMPTS {
            let body = Ulid::new().to_string().into_bytes();
            match self.store.put(&path, body.into()).await {
                Ok(_) => return,
                Err(err) if attempt < TOUCH_ATTEMPTS => {
                    debug!(%err, "inbox touch failed; retrying");
                    tokio::time::sleep(TOUCH_RETRY_DELAY).await;
                }
                Err(err) => warn!(%err, "inbox touch failed"),
            }
        }
    }

    /// The entries for `queue` in the inbox, of every kind, in the order of a
    /// pass. The listing contains the entries of every sender and costs one
    /// LIST request and one GET per entry. An entry whose record committed
    /// before the deletion of its object is listed as well, and
    /// [`QueueView::inbox_record`](crate::QueueView::inbox_record) reads the
    /// record. The listing omits a malformed entry and an entry of an unknown
    /// kind, which the writer rejects at the pass that reaches it.
    pub async fn pending(&self, queue: &str) -> Result<Vec<PendingEntry>> {
        QueueName::new(queue)?;
        let under = queue_prefix(&self.prefix, queue);
        let listed = list_entries(&*self.store, &self.prefix, &under)
            .await?
            .entries;
        let mut pending = Vec::with_capacity(listed.len());
        for (EntryPath { key, .. }, object) in listed {
            let EntryKey { id, kind, .. } = key;
            let body = match self.store.get(&object.location).await {
                Ok(result) => result.bytes().await.map_err(Error::InboxStore)?,
                // Applied and deleted after the listing.
                Err(ObjectStoreError::NotFound { .. }) => continue,
                Err(err) => return Err(Error::InboxStore(err)),
            };
            match InboxEntry::decode(&kind, &body) {
                Ok(entry) => pending.push(PendingEntry {
                    id,
                    entry,
                    sent_at_ms: u64::try_from(object.last_modified.timestamp_millis()).unwrap_or(0),
                }),
                Err(reason) => debug!(id, %reason, "inbox entry does not decode; not listed"),
            }
        }
        Ok(pending)
    }
}

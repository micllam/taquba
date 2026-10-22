//! The sender of inbox entries: another process writes an entry as one object
//! in the inbox.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use slatedb::object_store::{ObjectStore, ObjectStoreExt, path::Path};
use tracing::{debug, warn};
use ulid::{Generator, Ulid};

use crate::error::{Error, Result};
use crate::inbox::entry::{InboxKind, validate_id, validate_kind};
use crate::inbox::{entry_path, touch_path};

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

/// A sender of inbox entries over the object store of an inbox. It writes one
/// object per entry at `{path}/v1/{kind}/{id}` with the JSON body of the entry
/// and never opens the store of the queue. After the entry, it touches the
/// inbox: it overwrites the touch object at `{path}/touch` with a fresh ULID,
/// so the writer lists the inbox at its next poll of the touch object. The
/// writer applies the entry at its next pass, and the sender reads the outcome
/// through [`QueueView::inbox_record`](crate::QueueView::inbox_record) of a
/// [`QueueReader`](crate::QueueReader).
///
/// The writer applies the entries of a listing in id order. The ids of
/// [`Self::send`] increase with call order on one sender and its clones, so
/// entries sent one after the other, each send awaited before the next, apply
/// in send order. Concurrent sends, and the sends of two senders, apply in id
/// order among the entries of one listing, and an entry written after a pass
/// that applied a later id applies at a later pass. An id is unique per inbox,
/// across kinds: the record of an applied entry is at the id, and a later entry
/// of any kind with the id of a recorded one is deleted without an effect.
#[derive(Clone)]
pub struct InboxSender {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    ids: Arc<Mutex<Generator>>,
}

impl InboxSender {
    /// A sender over `store` at the inbox path `path`: the
    /// [`InboxOptions::path`](crate::InboxOptions::path) of the writer, or
    /// [`default_inbox_path`](crate::default_inbox_path) of the queue's path.
    pub fn new(store: Arc<dyn ObjectStore>, path: impl Into<String>) -> Self {
        Self {
            store,
            prefix: path.into(),
            ids: Arc::new(Mutex::new(Generator::new())),
        }
    }

    /// Write `entry` with a generated id (a ULID) and return the id.
    pub async fn send<E: InboxKind>(&self, entry: &E) -> Result<String> {
        let id = self.next_id();
        self.send_with_id(&id, entry).await?;
        Ok(id)
    }

    /// Write `entry` with the caller's `id`: `[A-Za-z0-9_-]`, 1 to
    /// [`MAX_INBOX_ID_LEN`](crate::MAX_INBOX_ID_LEN) bytes. An entry sent again
    /// with the id of an applied entry is deleted without an effect while the
    /// record of the first is retained.
    ///
    /// A failed touch is retried once after a second and then logged, and the
    /// send succeeds: the writer applies the entry at its next listing of
    /// [`InboxOptions::interval`](crate::InboxOptions::interval).
    pub async fn send_with_id<E: InboxKind>(&self, id: &str, entry: &E) -> Result<()> {
        validate_kind(E::KIND)?;
        validate_id(id)?;
        let body = serde_json::to_vec(entry).map_err(Error::InboxEncoding)?;
        let path = Path::from(entry_path(&self.prefix, E::KIND, id));
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

    /// An id that sorts after every id this sender generated.
    fn next_id(&self) -> String {
        let mut ids = self.ids.lock().expect("id generator mutex poisoned");
        match ids.generate() {
            Ok(id) => id.to_string(),
            // Unreachable short of 2^80 ids inside one millisecond.
            Err(_) => Ulid::new().to_string(),
        }
    }
}

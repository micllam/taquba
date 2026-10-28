//! The inbox: a prefix of objects that the writer applies, through which
//! another process enqueues, cancels and signals.
//!
//! An entry is one object at `{path}/v1/{queue}/{kind}/{order}.{id}` with the
//! MessagePack body that [`InboxEntry`] states, for the queue in its path. The
//! kinds are `enqueue`, `cancel` and `signal`, and `order` is a ULID of the
//! send. A pass lists the version prefix, sorts the entries by order and then
//! by id and applies each in turn through the queue. After the entries, the
//! pass rejects each object name without a ULID before the id, so an entry of
//! that id applies first.
//!
//! A pass rejects an entry it cannot apply, with a record of its [`Rejection`].
//! The application commits an [`InboxRecord`](crate::InboxRecord) with its
//! outcome at the queue and the id in the transaction of the entry's effect,
//! and a rejection commits its record in a transaction of its own. A pass that
//! finds the record of an object settles the object without a read, and passes
//! run one at a time, so ingestion is at least once and application is once
//! within the retention of the record.
//!
//! A pass rejects an object larger than [`MAX_INBOX_ENTRY_SIZE`] by the size of
//! its listing, without a read. It settles its objects in batches of
//! [`INBOX_BATCH`], and reads the bodies of up to [`INBOX_PREFETCH`] objects
//! and [`INBOX_PREFETCH_BYTES`] before their application. The commits of a
//! batch do not await durability. One flush after the batch makes them durable.
//! The pass then deletes the payload objects of the jobs that the batch removed
//! and the objects of the batch. A crash before the flush loses the commits of
//! the batch, and the next pass applies its entries again.
//!
//! The writer runs one pass at open before the scheduler starts. After it, the
//! writer polls the touch object at `{path}/touch` once per
//! [`InboxOptions::touch_poll_interval`], and runs a pass when the touch object
//! changed or once per [`InboxOptions::interval`]. A sender touches the inbox
//! after each entry: it overwrites the touch object. The writer polls the touch
//! object before the listing, so an entry written during a listing changes the
//! touch object for the next poll.
//!
//! A pass skips an entry whose object is missing at its read. A failed delete
//! of an object is logged and counted, and a later pass deletes the object. A
//! failed read commits the time of the first failure at the entry's failure key
//! and ends the pass. From [`INBOX_UNREADABLE_AFTER`] after that time, a pass
//! rejects the entry with [`Rejection::Unreadable`] and continues. A pass that
//! ends without an error deletes every failure key.
//!
//! Any other error ends a pass at its entry, after the flush and the deletes of
//! the entries before it in its batch. The next pass starts again at the first
//! entry without a record. After a pass that ends at an error, the writer runs
//! a pass at the next poll.

pub(crate) mod apply;
pub(crate) mod entry;
pub(crate) mod listing;
pub(crate) mod record;
pub(crate) mod sender;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::FuturesOrdered;
use slatedb::object_store::{
    Error as ObjectStoreError, ObjectMeta, ObjectStore, ObjectStoreExt, path::Path,
};
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::background::Periodic;
use crate::error::{Error, Result};
use crate::inbox::entry::{InboxEntry, MAX_INBOX_ENTRY_SIZE};
use crate::inbox::listing::{PendingEntry, Read, list_entries, read_object};
use crate::inbox::record::{InboxOutcome, InboxRecordHandle, Rejection};
use crate::keys::{KeyTag, QueueName, inbox_failure_key, inbox_record_key, tag_prefix};
use crate::objects::delete_objects;
use crate::queue_core::QueueCore;
use crate::time_bound::TimeBound;
use crate::txn::{Attempt, Durability};

/// The objects that a pass settles before one flush of their commits and the
/// deletion of the objects.
const INBOX_BATCH: usize = 1_000;

/// The objects whose bodies a pass reads before their application.
const INBOX_PREFETCH: usize = 16;

/// The listed bytes of the entries that a pass reads before their application,
/// counted for the entries that the pass settles without a read as well. An
/// entry of this size is read alone.
const INBOX_PREFETCH_BYTES: u64 = MAX_INBOX_ENTRY_SIZE as u64;

/// Default value of [`InboxOptions::interval`].
pub const DEFAULT_INBOX_INTERVAL: Duration = Duration::from_secs(60);

/// Default value of [`InboxOptions::touch_poll_interval`].
pub const DEFAULT_INBOX_TOUCH_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Default value of [`InboxOptions::record_retention`].
pub const DEFAULT_INBOX_RECORD_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);

/// How long the reads of an entry's object fail, from the first failed read on
/// the queue's clock, before a pass rejects the entry with
/// [`Rejection::Unreadable`]. Until then, a failed read ends the pass.
pub const INBOX_UNREADABLE_AFTER: Duration = Duration::from_secs(3600);

/// The inbox path of a queue opened at `path` without [`InboxOptions::path`]:
/// `"{path}-inbox"`, a sibling of the queue's path in its object store. A
/// sender over such a queue passes it to [`InboxSender::new`].
///
/// [`InboxSender::new`]: crate::InboxSender::new
pub fn default_inbox_path(path: &str) -> String {
    format!("{path}-inbox")
}

/// Configuration of the inbox, set on [`OpenOptions::inbox`](crate::OpenOptions::inbox).
///
/// The inbox is a prefix of objects outside the store, at
/// [`default_inbox_path`] unless [`Self::path`] is set, through which another
/// process enqueues, cancels and signals. The writer accepts the entries for
/// the queues of [`Self::queues`].
///
/// The writer lists the inbox at open, after a change of its touch object and
/// once per [`Self::interval`]. A pass applies the entries in the order that
/// [`InboxSender`](crate::InboxSender) states. It commits an
/// [`InboxRecord`](crate::InboxRecord) with its outcome at the queue and the id
/// in the transaction of the entry's effect. One flush per batch of up to 1,000
/// entries makes the commits durable, and the pass then deletes their objects.
///
/// A pass rejects an entry it cannot apply, with a record of its [`Rejection`].
/// A transient error ends the pass at its entry, and the next pass starts again
/// there. An entry whose record exists is deleted without an effect, so an
/// entry sent twice applies once within [`Self::record_retention`].
pub struct InboxOptions {
    /// How often the writer lists the inbox without a touch. With the touch
    /// poll, the writer rounds the interval up to a whole number of polls, at
    /// least one. Each pass sends one LIST request, 1,440 a day at the default.
    /// Defaults to [`DEFAULT_INBOX_INTERVAL`].
    pub interval: Duration,
    /// How often the writer polls the touch object with a HEAD request, and
    /// lists the inbox when the touch object changed. Defaults to
    /// [`DEFAULT_INBOX_TOUCH_POLL_INTERVAL`], 43,200 HEAD requests a day.
    /// `None` turns off the poll, and the writer lists the inbox at
    /// [`Self::interval`] alone.
    pub touch_poll_interval: Option<Duration>,
    /// Object store of the inbox. `None` (the default) uses the object store
    /// the queue is opened on.
    pub object_store: Option<Arc<dyn ObjectStore>>,
    /// Path prefix of the inbox within its object store. `None` (the default)
    /// uses [`default_inbox_path`] of the path the queue is opened at. A custom
    /// value that shares the object store with the queue must lie outside the
    /// queue's `path`: the open fails with [`Error::InboxPathWithinQueue`] for
    /// the queue's path or a path inside it.
    pub path: Option<String>,
    /// The queues of the entries that the inbox accepts. A pass rejects an
    /// entry for a queue outside the list.
    pub queues: Vec<String>,
    /// How long the record of an applied entry is retained, which is the window
    /// within which an entry with the same id is not applied again. Defaults to
    /// [`DEFAULT_INBOX_RECORD_RETENTION`].
    pub record_retention: Duration,
}

impl InboxOptions {
    /// The options of an inbox that accepts the entries of `queues`, with the
    /// default of every other field.
    pub fn new<Q: Into<String>>(queues: impl IntoIterator<Item = Q>) -> Self {
        Self {
            interval: DEFAULT_INBOX_INTERVAL,
            touch_poll_interval: Some(DEFAULT_INBOX_TOUCH_POLL_INTERVAL),
            object_store: None,
            path: None,
            queues: queues.into_iter().map(Into::into).collect(),
            record_retention: DEFAULT_INBOX_RECORD_RETENTION,
        }
    }

    /// Set [`Self::interval`].
    #[must_use]
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Set [`Self::touch_poll_interval`].
    #[must_use]
    pub fn touch_poll_interval(mut self, interval: impl Into<Option<Duration>>) -> Self {
        self.touch_poll_interval = interval.into();
        self
    }

    /// Set [`Self::object_store`].
    #[must_use]
    pub fn object_store(mut self, object_store: impl Into<Option<Arc<dyn ObjectStore>>>) -> Self {
        self.object_store = object_store.into();
        self
    }

    /// Set [`Self::path`].
    #[must_use]
    pub fn path(mut self, path: impl Into<Option<String>>) -> Self {
        self.path = path.into();
        self
    }

    /// Set [`Self::record_retention`].
    #[must_use]
    pub fn record_retention(mut self, record_retention: Duration) -> Self {
        self.record_retention = record_retention;
        self
    }
}

/// The inbox of an open queue: its store, its allow-list and the intervals of
/// its pass task.
pub(crate) struct InboxState {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    interval: Duration,
    touch_poll_interval: Option<Duration>,
    queues: HashSet<String>,
    pub(crate) retention: Duration,
    /// Held for the duration of a pass.
    pass_lock: tokio::sync::Mutex<()>,
    /// The bound of the inbox expiry key space: the earliest `applied_at` of a
    /// live record, lowered after every commit that writes one.
    pub(crate) bound: TimeBound,
}

impl InboxState {
    /// The inbox of `options` for a queue opened on `store` at `path`.
    pub(crate) fn new(
        options: InboxOptions,
        store: &Arc<dyn ObjectStore>,
        path: &str,
    ) -> Result<Self> {
        for queue in &options.queues {
            QueueName::new(queue.as_str())?;
        }
        if options.object_store.is_none()
            && let Some(custom) = &options.path
            && Path::from(custom.as_str()).prefix_matches(&Path::from(path))
        {
            return Err(Error::InboxPathWithinQueue {
                path: custom.clone(),
                queue_path: path.to_string(),
            });
        }
        Ok(Self {
            store: options.object_store.unwrap_or_else(|| store.clone()),
            prefix: options.path.unwrap_or_else(|| default_inbox_path(path)),
            interval: options.interval,
            touch_poll_interval: options.touch_poll_interval,
            queues: options.queues.into_iter().collect(),
            retention: options.record_retention,
            pass_lock: tokio::sync::Mutex::new(()),
            bound: TimeBound::new(),
        })
    }

    /// Whether the inbox accepts the entries of `queue`.
    pub(crate) fn accepts(&self, queue: &str) -> bool {
        self.queues.contains(queue)
    }

    /// The rejection of `entry` without a read: a queue outside the allow-list,
    /// else the rejection of the listing.
    fn rejection(&self, entry: &PendingEntry) -> Option<Rejection> {
        if self.accepts(entry.queue()) {
            entry.listing_rejection()
        } else {
            Some(Rejection::QueueNotAccepted)
        }
    }

    /// The metadata of the touch object, or `None` before the first touch.
    async fn touch_meta(&self) -> Result<Option<ObjectMeta>> {
        let path = Path::from(touch_path(&self.prefix));
        match self.store.head(&path).await {
            Ok(meta) => Ok(Some(meta)),
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(Error::InboxStore(err)),
        }
    }

    /// Delete the objects at `locations` through the bulk delete of the store,
    /// and log one warning for the failed deletes and count them. A later pass
    /// finds the record of an object whose delete failed, and deletes the
    /// object.
    async fn delete_all(&self, locations: Vec<Path>) {
        if let Some((failed, err)) = delete_objects(&*self.store, locations).await {
            warn!(failed, %err, "failed to delete inbox objects; a later pass retries");
            crate::obs::inbox_delete_failed(failed);
        }
    }
}

/// The path of the touch object of the inbox at `prefix`.
pub(crate) fn touch_path(prefix: &str) -> String {
    format!("{prefix}/touch")
}

/// The pass task of the inbox. A tick sends a HEAD for the touch object, and a
/// pass runs when the touch object changed or when the last pass of the task is
/// older than [`InboxOptions::interval`].
pub(crate) struct Inbox {
    core: Arc<QueueCore>,
    inbox: Arc<InboxState>,
    tick: Duration,
    /// The touch object at the HEAD before the last pass.
    touched: Option<ObjectMeta>,
    /// When the last pass of the task started, or `None` after a failed pass.
    last_pass: Option<Instant>,
}

impl Inbox {
    /// The pass task of `core` after its first pass, or `None` for a queue
    /// without an inbox. A failed first pass fails the open.
    pub(crate) async fn open(core: Arc<QueueCore>) -> Result<Option<Self>> {
        let Some(inbox) = core.inbox.clone() else {
            return Ok(None);
        };
        let (tick, touched) = match inbox.touch_poll_interval {
            Some(tick) => (tick, seen_touch(&inbox).await),
            None => (inbox.interval, None),
        };
        let started = Instant::now();
        core.inbox_pass().await?;
        Ok(Some(Self {
            core,
            inbox,
            tick,
            touched,
            last_pass: Some(started),
        }))
    }

    /// The interval of the task's ticks.
    pub(crate) fn tick(&self) -> Duration {
        self.tick
    }
}

/// The touch object of `inbox`, or `None` when the HEAD fails.
async fn seen_touch(inbox: &InboxState) -> Option<ObjectMeta> {
    inbox.touch_meta().await.unwrap_or_else(|err| {
        warn!(%err, "inbox touch poll failed");
        None
    })
}

impl Periodic for Inbox {
    const NAME: &'static str = "inbox";

    async fn step(&mut self) -> Result<()> {
        let mut due = self
            .last_pass
            .is_none_or(|started| started.elapsed() >= self.inbox.interval);
        if self.inbox.touch_poll_interval.is_some() {
            match self.inbox.touch_meta().await {
                Ok(touched) if touched != self.touched => {
                    self.touched = touched;
                    due = true;
                }
                Ok(_) => {}
                Err(err) => warn!(%err, "inbox touch poll failed"),
            }
        }
        if !due {
            return Ok(());
        }
        let started = Instant::now();
        let passed = self.core.inbox_pass().await;
        self.last_pass = passed.is_ok().then_some(started);
        passed
    }
}

impl QueueCore {
    /// One pass over the inbox, as the module documents. A queue without an
    /// inbox returns without a request.
    pub(crate) async fn inbox_pass(&self) -> Result<()> {
        let Some(inbox) = &self.inbox else {
            return Ok(());
        };
        let _pass = inbox.pass_lock.lock().await;
        let passed = self.inbox_pass_over(inbox).await;
        if passed.is_err() {
            crate::obs::inbox_pass_failed();
        }
        passed
    }

    async fn inbox_pass_over(&self, inbox: &InboxState) -> Result<()> {
        let entries = list_entries(&*inbox.store, &inbox.prefix, None).await?;
        for batch in entries.chunks(INBOX_BATCH) {
            self.inbox_batch(inbox, batch).await?;
        }
        self.clear_inbox_failures().await
    }

    /// Delete every failure key. A pass that reaches this step settled each
    /// listed entry, so no entry of the inbox is failing.
    async fn clear_inbox_failures(&self) -> Result<()> {
        let mut iter = self
            .db
            .scan_prefix(tag_prefix(KeyTag::InboxFailure), ..)
            .await?;
        let mut cleared = Vec::new();
        while let Some(kv) = iter.next().await? {
            cleared.push(kv.key.to_vec());
        }
        drop(iter);
        if cleared.is_empty() {
            return Ok(());
        }
        let cleared = &cleared;
        self.retry(Durability::Deferred, |txn| async move {
            for key in cleared {
                txn.delete(key)?;
            }
            Ok((txn, Attempt::Commit(())))
        })
        .await
    }

    /// The rejection of the entry `id` of `queue` after the failed read `err`,
    /// once [`INBOX_UNREADABLE_AFTER`] has passed from the first failed read of
    /// the entry, or `None` before. The first failed read commits its time, and
    /// the commit awaits durability, so a failed open keeps it.
    async fn unreadable(
        &self,
        queue: &QueueName,
        id: &str,
        err: &ObjectStoreError,
    ) -> Result<Option<Rejection>> {
        let key = &inbox_failure_key(queue, id);
        let now = self.now_ms();
        let first = self
            .db
            .get(key)
            .await?
            .and_then(|value| <[u8; 8]>::try_from(value.as_ref()).ok())
            .map(u64::from_be_bytes);
        let Some(first) = first else {
            self.retry(Durability::Awaited, |txn| async move {
                txn.put(key, now.to_be_bytes())?;
                Ok((txn, Attempt::Commit(())))
            })
            .await?;
            return Ok(None);
        };
        let after = u64::try_from(INBOX_UNREADABLE_AFTER.as_millis()).unwrap_or(u64::MAX);
        Ok(
            (now.saturating_sub(first) >= after).then(|| Rejection::Unreadable {
                detail: err.to_string(),
            }),
        )
    }

    /// Settle the entries of `batch`, then flush their commits once and delete
    /// the payloads of the jobs that they removed and their objects. An error
    /// at an entry ends the batch after the flush and the deletes of the
    /// entries before it.
    async fn inbox_batch(&self, inbox: &InboxState, batch: &[PendingEntry]) -> Result<()> {
        let mut settled = Vec::new();
        let mut payload_refs = Vec::new();
        let applied = self
            .settle_batch(inbox, batch, &mut settled, &mut payload_refs)
            .await;
        if !settled.is_empty() {
            self.db.flush().await?;
            self.payload_store.delete_all(payload_refs).await;
            inbox.delete_all(settled).await;
        }
        applied
    }

    /// Settle the entries of `batch` in order, with up to [`INBOX_PREFETCH`]
    /// entries and [`INBOX_PREFETCH_BYTES`] of their listed sizes read before
    /// their application. The location of each settled entry goes to `settled`.
    async fn settle_batch(
        &self,
        inbox: &InboxState,
        batch: &[PendingEntry],
        settled: &mut Vec<Path>,
        payload_refs: &mut Vec<String>,
    ) -> Result<()> {
        let mut reads = FuturesOrdered::new();
        let (mut next, mut reading) = (0, 0);
        for entry in batch {
            while let Some(ahead) = batch.get(next) {
                let full =
                    reads.len() >= INBOX_PREFETCH || reading + ahead.size() > INBOX_PREFETCH_BYTES;
                if full && !reads.is_empty() {
                    break;
                }
                reads.push_back(self.prefetch(inbox, ahead));
                reading += ahead.size();
                next += 1;
            }
            let prefetched = reads.next().await.expect("one read per entry")?;
            reading -= entry.size();
            self.settle_entry(inbox, entry, prefetched, payload_refs)
                .await?;
            settled.push(entry.location().clone());
        }
        Ok(())
    }

    /// The read of `entry` in `inbox`, or `None` when its application does not
    /// read the entry: the pass rejects it without a read or its record exists.
    async fn prefetch(&self, inbox: &InboxState, entry: &PendingEntry) -> Result<Option<Read>> {
        if inbox.rejection(entry).is_some()
            || self
                .db
                .get(inbox_record_key(entry.queue(), entry.id()))
                .await?
                .is_some()
        {
            return Ok(None);
        }
        Ok(Some(read_object(&*inbox.store, entry.location()).await))
    }

    /// Apply or reject `entry` and commit its record. `prefetched` is the read
    /// before the application, and an entry without it is read here unless the
    /// pass rejects it without a read. An entry whose record exists is settled
    /// without an effect, and an entry deleted after the listing is skipped.
    /// The payload ref of a job that the application removed goes to
    /// `payload_refs`.
    async fn settle_entry(
        &self,
        inbox: &InboxState,
        entry: &PendingEntry,
        prefetched: Option<Read>,
        payload_refs: &mut Vec<String>,
    ) -> Result<()> {
        let (queue, id, kind) = (entry.queue(), entry.id(), entry.kind());
        if self.db.get(inbox_record_key(queue, id)).await?.is_some() {
            debug!(%kind, id, "inbox entry already applied");
            return Ok(());
        }
        let record = InboxRecordHandle::new(queue.clone(), id.to_string(), self.now_ms());
        let decoded = if let Some(rejection) = inbox.rejection(entry) {
            Err(rejection)
        } else {
            let read = match prefetched {
                Some(read) => read,
                None => read_object(&*inbox.store, entry.location()).await,
            };
            match read {
                Read::Body(body) => InboxEntry::decode(kind, &body),
                Read::Rejected(rejection) => Err(rejection),
                Read::Missing => {
                    debug!(%kind, id, "inbox entry deleted after the listing; skipped");
                    return Ok(());
                }
                Read::Failed(err) => match self.unreadable(queue, id, &err).await? {
                    Some(rejection) => Err(rejection),
                    None => return Err(Error::InboxStore(err)),
                },
            }
        };
        let outcome = match decoded {
            Ok(inbox_entry) => {
                self.apply_entry(queue, &record, inbox_entry, payload_refs)
                    .await?
            }
            Err(rejection) => self.commit_rejection(&record, rejection).await?,
        };
        if let InboxOutcome::Rejected(reason) = &outcome {
            crate::obs::inbox_rejected(kind);
            warn!(%kind, id, %reason, "inbox entry rejected");
        } else {
            crate::obs::inbox_applied(kind);
            debug!(%kind, id, "inbox entry applied");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

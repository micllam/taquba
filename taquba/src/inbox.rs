//! The inbox: a prefix of objects that the writer applies, through which
//! another process enqueues, cancels and signals.
//!
//! An entry is one object at `{path}/v1/{queue}/{kind}/{id}` with the
//! MessagePack body that [`InboxEntry`] states, for the queue in its path. The
//! kinds are `enqueue`, `cancel` and `signal`. A pass lists the version prefix,
//! sorts the entries by id and applies each in order through the queue. The
//! pass rejects an entry for a queue outside [`InboxOptions::queues`], an entry
//! of another kind, a body that does not decode and an entry whose operation
//! fails with a permanent error. The application commits an
//! [`InboxRecord`](crate::InboxRecord) at the queue and the id in the
//! transaction of the entry's effect. After an idempotent effect or none, the
//! record commits in a transaction of its own. The object is deleted after the
//! commit. A pass that finds the record of an object deletes the object without
//! a read, and passes run one at a time, so ingestion is at least once and
//! application is once within the retention of the record.
//!
//! The writer runs one pass at open before the scheduler starts. After it, the
//! writer polls the touch object at `{path}/touch` once per
//! [`InboxOptions::touch_poll_interval`], and runs a pass when the touch object
//! changed or once per [`InboxOptions::interval`]. A sender touches the inbox
//! after each entry: it overwrites the touch object. The writer polls the touch
//! object before the listing, so an entry written during a listing changes the
//! touch object for the next poll.
//!
//! Any other error ends a pass at its entry, and the next pass starts again at
//! the first entry without a record. After a pass that ends at an error, the
//! writer runs a pass at the next poll.

pub(crate) mod apply;
pub(crate) mod entry;
pub(crate) mod record;
pub(crate) mod sender;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt;
use slatedb::object_store::{
    Error as ObjectStoreError, ObjectMeta, ObjectStore, ObjectStoreExt, path::Path,
};
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::background::Periodic;
use crate::effects::SettlementEffects;
use crate::error::{Error, Result};
use crate::inbox::entry::{InboxEntry, validate_id};
use crate::inbox::record::{InboxOutcome, InboxRecordHandle, Settled, StagedRecord};
use crate::keys::{QueueName, inbox_record_key};
use crate::queue_core::QueueCore;
use crate::time_bound::TimeBound;

/// Default value of [`InboxOptions::interval`].
pub const DEFAULT_INBOX_INTERVAL: Duration = Duration::from_secs(60);

/// Default value of [`InboxOptions::touch_poll_interval`].
pub const DEFAULT_INBOX_TOUCH_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Default value of [`InboxOptions::record_retention`].
pub const DEFAULT_INBOX_RECORD_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);

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
/// once per [`Self::interval`]. A pass applies the entries in id order. It
/// commits an [`InboxRecord`](crate::InboxRecord) at the queue and the id in
/// the transaction of the entry's effect. After an idempotent effect or none,
/// it commits the record in a transaction of its own. It then deletes the
/// object. The pass rejects these entries with a record of the reason:
///
/// - A malformed entry.
/// - An entry for a queue outside [`Self::queues`].
/// - An entry of another kind.
/// - A cancel or a signal for a job of another queue.
/// - An entry whose operation fails with a permanent error.
///
/// Any other error ends the pass at its entry, and the next pass starts again
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

    async fn get(&self, location: &Path) -> Result<Vec<u8>> {
        let result = self.store.get(location).await.map_err(Error::InboxStore)?;
        Ok(result.bytes().await.map_err(Error::InboxStore)?.to_vec())
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

    /// Delete the object at `location`. An absent object counts as deleted.
    async fn delete(&self, location: &Path) -> Result<()> {
        match self.store.delete(location).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(err) => Err(Error::InboxStore(err)),
        }
    }
}

/// The prefix of the entries of `queue` in the inbox at `prefix`.
pub(crate) fn queue_prefix(prefix: &str, queue: &str) -> String {
    format!("{}/{queue}", version_prefix(prefix))
}

/// The prefix of the entries of the layout version in the inbox at `prefix`.
fn version_prefix(prefix: &str) -> String {
    format!("{prefix}/v1")
}

/// The path of the entry `id` of `kind` and `queue` in the inbox at `prefix`.
pub(crate) fn entry_path(prefix: &str, queue: &str, kind: &str, id: &str) -> String {
    format!("{}/{kind}/{id}", queue_prefix(prefix, queue))
}

/// The path of the touch object of the inbox at `prefix`.
pub(crate) fn touch_path(prefix: &str) -> String {
    format!("{prefix}/touch")
}

/// The segments of an entry path within the version prefix, ordered by id and
/// then by queue and kind.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct EntryPath {
    pub(crate) id: String,
    pub(crate) queue: QueueName,
    pub(crate) kind: String,
}

impl EntryPath {
    /// The segments of the path `relative` to the version prefix, or `None` for
    /// a path outside the layout.
    fn parse(relative: &str) -> Option<Self> {
        let mut segments = relative.split('/');
        let (queue, kind, id) = (segments.next()?, segments.next()?, segments.next()?);
        if segments.next().is_some() || kind.is_empty() || validate_id(id).is_err() {
            return None;
        }
        Some(Self {
            id: id.to_string(),
            queue: QueueName::new(queue).ok()?,
            kind: kind.to_string(),
        })
    }
}

/// The entries within `under` in the inbox at `prefix` of `store`, in the order
/// of [`EntryPath`]. A path outside the layout is logged and left in place.
pub(crate) async fn list_entries(
    store: &dyn ObjectStore,
    prefix: &str,
    under: &str,
) -> Result<Vec<(EntryPath, ObjectMeta)>> {
    let version = format!("{}/", version_prefix(prefix));
    let mut objects = store.list(Some(&Path::from(under)));
    let mut entries = Vec::new();
    while let Some(object) = objects.try_next().await.map_err(Error::InboxStore)? {
        let path = object.location.as_ref().strip_prefix(version.as_str());
        match path.and_then(EntryPath::parse) {
            Some(path) => entries.push((path, object)),
            None => warn!(path = %object.location, "object outside the inbox layout; skipped"),
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(entries)
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
        let entries =
            list_entries(&*inbox.store, &inbox.prefix, &version_prefix(&inbox.prefix)).await?;
        for (EntryPath { id, queue, kind }, ObjectMeta { location, .. }) in entries {
            if self.db.get(inbox_record_key(&queue, &id)).await?.is_some() {
                inbox.delete(&location).await?;
                debug!(kind, id, "inbox entry already applied; object deleted");
                continue;
            }
            let record = InboxRecordHandle::new(queue.clone(), id.clone(), self.now_ms());
            let settled = if inbox.accepts(&queue) {
                let body = inbox.get(&location).await?;
                match InboxEntry::decode(&kind, &body) {
                    Ok(entry) => self.apply_entry(&queue, &record, entry).await,
                    Err(reason) => Ok(Settled::Rejected(reason)),
                }
            } else {
                Ok(Settled::Rejected(format!(
                    "queue `{queue}` is outside the inbox allow-list"
                )))
            };
            let rejected = self.record_entry(&kind, &record, settled?).await?;
            if rejected {
                crate::obs::inbox_rejected(&kind);
            } else {
                crate::obs::inbox_applied(&kind);
                debug!(kind, id, "inbox entry applied");
            }
            inbox.delete(&location).await?;
        }
        Ok(())
    }

    /// Commit the record of an entry of `kind` that `settled` reports
    /// separately, and return whether the entry was rejected.
    async fn record_entry(
        &self,
        kind: &str,
        record: &InboxRecordHandle,
        settled: Settled,
    ) -> Result<bool> {
        match settled {
            Settled::Committed => Ok(false),
            Settled::Applied(staged) => {
                self.commit_record(staged).await?;
                Ok(false)
            }
            Settled::Rejected(reason) => {
                let staged = record.staged(InboxOutcome::Rejected(reason.clone()));
                self.commit_record(staged).await?;
                warn!(kind, id = record.id, reason, "inbox entry rejected");
                Ok(true)
            }
        }
    }

    /// Commit `record` in a transaction of its own.
    async fn commit_record(&self, record: StagedRecord) -> Result<()> {
        let effects = SettlementEffects {
            inbox_record: Some(record),
            ..SettlementEffects::default()
        };
        self.commit_effects_where(effects, &[]).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

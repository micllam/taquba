//! The entry objects of the inbox: their paths, their listing in the order of a
//! pass and the read of an object.

use futures_util::TryStreamExt;
use slatedb::object_store::{
    Error as ObjectStoreError, ObjectMeta, ObjectStore, ObjectStoreExt, path::Path,
};
use tracing::warn;
use ulid::Ulid;

use crate::error::{Error, Result};
use crate::inbox::entry::{EntryKind, MAX_INBOX_ENTRY_SIZE, validate_id};
use crate::inbox::record::Rejection;
use crate::keys::QueueName;

/// An entry in the inbox, as [`InboxSender::pending`](crate::InboxSender::pending)
/// lists it from the path and the metadata of its object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingEntry {
    queue: QueueName,
    id: String,
    kind: EntryKind,
    size: u64,
    sent_at_ms: u64,
    location: Path,
    /// The ULID before the id in the object name, or `None` for a name without
    /// one.
    order: Option<Ulid>,
}

impl PendingEntry {
    /// The queue of the entry.
    pub fn queue(&self) -> &QueueName {
        &self.queue
    }

    /// The id of the entry.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The kind of the entry.
    pub fn kind(&self) -> &EntryKind {
        &self.kind
    }

    /// The size of the body in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// When the object store wrote the entry, as milliseconds from the Unix
    /// epoch on the clock of the store.
    pub fn sent_at_ms(&self) -> u64 {
        self.sent_at_ms
    }

    /// The location of the object of the entry.
    pub(crate) fn location(&self) -> &Path {
        &self.location
    }

    /// The rejection that the listing decides: an object name without a ULID
    /// before the id, else a size above [`MAX_INBOX_ENTRY_SIZE`].
    pub(crate) fn listing_rejection(&self) -> Option<Rejection> {
        match self.order {
            Some(_) => oversized(self.size),
            None => Some(Rejection::Unordered),
        }
    }

    /// The entry of `object` at the path `relative` to the version prefix, or
    /// `None` for a path outside the layout.
    fn parse(relative: &str, object: &ObjectMeta) -> Option<Self> {
        let mut segments = relative.split('/');
        let (queue, kind, name) = (segments.next()?, segments.next()?, segments.next()?);
        let (order, id) = match name.split_once('.') {
            Some((order, id)) => (Ulid::from_string(order).ok(), id),
            None => (None, name),
        };
        if segments.next().is_some() || kind.is_empty() || validate_id(id).is_err() {
            return None;
        }
        Some(Self {
            queue: QueueName::new(queue).ok()?,
            id: id.to_string(),
            kind: EntryKind::of_segment(kind),
            size: object.size,
            sent_at_ms: u64::try_from(object.last_modified.timestamp_millis()).unwrap_or(0),
            location: object.location.clone(),
            order,
        })
    }

    /// The sort key of the entry in a pass.
    fn pass_order(&self) -> (bool, Option<Ulid>, &str, &QueueName, &EntryKind) {
        (
            self.order.is_none(),
            self.order,
            &self.id,
            &self.queue,
            &self.kind,
        )
    }
}

/// The read of an entry object.
pub(crate) enum Read {
    Body(Vec<u8>),
    /// The object was deleted after the listing.
    Missing,
    /// The read rejects the entry without its body.
    Rejected(Rejection),
    /// The store failed the read.
    Failed(ObjectStoreError),
}

/// Read the object at `location` of `store`, with the size of the read checked
/// against [`MAX_INBOX_ENTRY_SIZE`] before its body.
pub(crate) async fn read_object(store: &dyn ObjectStore, location: &Path) -> Read {
    let result = match store.get(location).await {
        Ok(result) => result,
        Err(ObjectStoreError::NotFound { .. }) => return Read::Missing,
        Err(err) => return Read::Failed(err),
    };
    if let Some(rejection) = oversized(result.meta.size) {
        return Read::Rejected(rejection);
    }
    match result.bytes().await {
        Ok(body) => Read::Body(body.to_vec()),
        Err(err) => Read::Failed(err),
    }
}

/// The rejection of an object of `size` bytes above [`MAX_INBOX_ENTRY_SIZE`].
fn oversized(size: u64) -> Option<Rejection> {
    (size > MAX_INBOX_ENTRY_SIZE as u64).then_some(Rejection::TooLarge { size })
}

/// The prefix of the entries of `queue` in the inbox at `prefix`.
fn queue_prefix(prefix: &str, queue: &str) -> String {
    format!("{}/{queue}", version_prefix(prefix))
}

/// The prefix of the entries of the layout version in the inbox at `prefix`.
fn version_prefix(prefix: &str) -> String {
    format!("{prefix}/v1")
}

/// The path of the entry `id` of `kind` and `queue`, sent at `order`, in the
/// inbox at `prefix`.
pub(crate) fn entry_path(
    prefix: &str,
    queue: &str,
    kind: &EntryKind,
    order: Ulid,
    id: &str,
) -> String {
    format!("{}/{kind}/{order}.{id}", queue_prefix(prefix, queue))
}

/// The entries of `queue`, or of every queue for `None`, in the inbox at
/// `prefix` of `store`, in the order of a pass. The entries with an order come
/// first, sorted by order and then by id, queue and kind. A path outside the
/// layout is logged and left in place.
pub(crate) async fn list_entries(
    store: &dyn ObjectStore,
    prefix: &str,
    queue: Option<&str>,
) -> Result<Vec<PendingEntry>> {
    let version = version_prefix(prefix);
    let under = match queue {
        Some(queue) => queue_prefix(prefix, queue),
        None => version.clone(),
    };
    let version = format!("{version}/");
    let mut objects = store.list(Some(&Path::from(under)));
    let mut listed = Vec::new();
    while let Some(object) = objects.try_next().await.map_err(Error::InboxStore)? {
        let relative = object.location.as_ref().strip_prefix(version.as_str());
        match relative.and_then(|relative| PendingEntry::parse(relative, &object)) {
            Some(entry) => listed.push(entry),
            None => warn!(path = %object.location, "object outside the inbox layout; skipped"),
        }
    }
    listed.sort_by(|a, b| a.pass_order().cmp(&b.pass_order()));
    Ok(listed)
}

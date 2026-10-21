//! The inbox: a prefix of objects that the writer applies, through which
//! another process enqueues, cancels and signals.
//!
//! An entry is one object at `{path}/v1/{kind}/{id}` with a JSON body. A pass
//! lists the version prefix, sorts the entries by id and applies each in order:
//! the entry of a core kind through the queue, and the entry of a registered
//! kind through its [`InboxHandler`]. The application commits an
//! [`InboxRecord`](crate::InboxRecord) at the id in the transaction of the
//! entry's effect, and the object is deleted after the commit. A pass that
//! finds the record of an object deletes the object without a read, and the
//! transaction that writes a record fails when one exists, so ingestion is at
//! least once and application is once within the retention of the record. An
//! entry of a kind without a handler stays until a pass finds one, without a
//! read. An entry of a kind registered by a field, with a value without a
//! handler, stays as well and costs one GET per pass.
//!
//! The writer runs one pass at open before the scheduler starts, and one per
//! [`InboxOptions::interval`] after it. An error ends a pass at its entry, and
//! the next pass starts again at the first entry without a record.

pub(crate) mod entry;
pub(crate) mod kinds;
pub(crate) mod record;
pub(crate) mod sender;

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures_util::TryStreamExt;
use slatedb::object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, path::Path};
use tracing::{debug, warn};

use crate::background::Periodic;
use crate::effects::SettlementEffects;
use crate::error::{Error, Result};
use crate::inbox::entry::{InboxKind, is_core_kind, validate_id, validate_kind};
use crate::inbox::kinds::CoreKind;
use crate::inbox::record::{Applied, InboxRecordHandle, Settled, malformed};
use crate::keys::{QueueName, inbox_record_key};
use crate::queue_core::QueueCore;
use crate::time_bound::TimeBound;

/// Default value of [`InboxOptions::interval`].
pub const DEFAULT_INBOX_INTERVAL: Duration = Duration::from_secs(10);

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
pub struct InboxOptions {
    /// How often the writer lists the inbox. Defaults to
    /// [`DEFAULT_INBOX_INTERVAL`]. Each pass costs one LIST request.
    pub interval: Duration,
    /// Object store of the inbox. `None` (the default) uses the object store
    /// the queue is opened on.
    pub object_store: Option<Arc<dyn ObjectStore>>,
    /// Path prefix of the inbox within its object store. `None` (the default)
    /// uses [`default_inbox_path`] of the path the queue is opened at. A custom
    /// value that shares the object store with the queue must not equal or nest
    /// within the queue's `path`.
    pub path: Option<String>,
    /// The queues that the core kinds `enqueue` and `cancel` may touch. An
    /// entry for a queue outside the list is rejected.
    pub queues: Vec<String>,
    /// How long the record of an applied entry is retained, which is the window
    /// within which an entry with the same id is not applied again. Defaults to
    /// [`DEFAULT_INBOX_RECORD_RETENTION`].
    pub record_retention: Duration,
}

impl InboxOptions {
    /// Set [`Self::interval`].
    #[must_use]
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
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

    /// Add one queue to [`Self::queues`].
    #[must_use]
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queues.push(queue.into());
        self
    }

    /// Set [`Self::queues`].
    #[must_use]
    pub fn queues(mut self, queues: Vec<String>) -> Self {
        self.queues = queues;
        self
    }

    /// Set [`Self::record_retention`].
    #[must_use]
    pub fn record_retention(mut self, record_retention: Duration) -> Self {
        self.record_retention = record_retention;
        self
    }
}

impl Default for InboxOptions {
    fn default() -> Self {
        Self {
            interval: DEFAULT_INBOX_INTERVAL,
            object_store: None,
            path: None,
            queues: Vec::new(),
            record_retention: DEFAULT_INBOX_RECORD_RETENTION,
        }
    }
}

/// The error of an [`InboxHandler`].
pub type InboxHandlerError = Box<dyn std::error::Error + Send + Sync>;

/// The handler of one inbox kind, registered with
/// [`Queue::register_inbox_handler`](crate::Queue::register_inbox_handler).
///
/// The pass decodes the body of an entry as [`Self::Kind`] and rejects a body
/// that does not decode. The handler applies the entry once. It commits the
/// record of the entry in the transaction of the entry's effect, through the
/// [`SettlementEffects`] that [`InboxRecordHandle::effects`] or
/// [`InboxRecordHandle::attach`] builds, and reports [`Applied::Committed`].
/// When the effect is idempotent, or the entry is rejected, the handler reports
/// [`Applied::Separately`] with the outcome and the pass commits the record on
/// its own. An error ends the pass, and the next pass applies the entry again.
///
/// [`SettlementEffects`]: crate::SettlementEffects
pub trait InboxHandler: Send + Sync + 'static {
    /// The entry type of the kind.
    type Kind: InboxKind;

    /// Apply `entry` with the record `record`.
    fn apply(
        &self,
        entry: Self::Kind,
        record: &InboxRecordHandle<OutcomeOf<Self>>,
    ) -> impl Future<Output = std::result::Result<Applied<OutcomeOf<Self>>, InboxHandlerError>> + Send;
}

/// The outcome of the kind of the handler `H`.
type OutcomeOf<H> = <<H as InboxHandler>::Kind as InboxKind>::Outcome;

/// [`InboxHandler`] without its type, held by the queue.
trait DynInboxHandler: Send + Sync {
    /// Decode `body`, apply the entry `id` and encode its outcome, recorded at
    /// `applied_at_ms`.
    fn apply<'a>(
        &'a self,
        id: &'a str,
        applied_at_ms: u64,
        body: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<Settled>> + Send + 'a>>;
}

impl<H: InboxHandler> DynInboxHandler for H {
    fn apply<'a>(
        &'a self,
        id: &'a str,
        applied_at_ms: u64,
        body: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<Settled>> + Send + 'a>> {
        Box::pin(async move {
            let record = InboxRecordHandle::new(id.to_string(), applied_at_ms);
            let applied = match serde_json::from_value::<H::Kind>(body) {
                Ok(entry) => InboxHandler::apply(self, entry, &record)
                    .await
                    .map_err(|source| Error::InboxHandler {
                        kind: H::Kind::KIND.to_string(),
                        source,
                    })?,
                Err(err) => malformed(err),
            };
            record.settle(applied)
        })
    }
}

/// The registration of an [`InboxHandler`] for one kind, or for one value of a
/// field of the kind, returned by
/// [`Queue::register_inbox_handler`](crate::Queue::register_inbox_handler) and
/// [`Queue::register_inbox_handler_for`](crate::Queue::register_inbox_handler_for).
/// The registration lasts while the value lives, and dropping it unregisters
/// the handler: an entry of the kind, or of the value, then waits for the next
/// registration.
#[must_use = "dropping the registration unregisters the handler"]
pub struct InboxRegistration {
    core: Arc<QueueCore>,
    kind: String,
    /// The value of the scope, `None` for a registration of the whole kind.
    value: Option<String>,
}

impl Drop for InboxRegistration {
    fn drop(&mut self) {
        let Some(inbox) = &self.core.inbox else {
            return;
        };
        let mut handlers = inbox.handlers.write().unwrap();
        match (&self.value, handlers.get_mut(&self.kind)) {
            (Some(value), Some(Registered::ByField { values, .. })) => {
                values.remove(value);
                if values.is_empty() {
                    handlers.remove(&self.kind);
                }
            }
            (None, Some(Registered::Whole(_))) => {
                handlers.remove(&self.kind);
            }
            _ => {}
        }
    }
}

/// The handlers of one kind: one for the whole kind, or one per value of a
/// field of the body.
enum Registered {
    Whole(Arc<dyn DynInboxHandler>),
    ByField {
        field: String,
        values: BTreeMap<String, Arc<dyn DynInboxHandler>>,
    },
}

/// How the entries of a kind reach a handler.
enum Lookup {
    Core(CoreKind),
    Whole(Arc<dyn DynInboxHandler>),
    /// The handler is selected by the string at the field of the body.
    ByField(String),
}

/// The inbox of an open queue: its store, its allow-list and its handlers.
pub(crate) struct InboxState {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    queues: HashSet<String>,
    pub(crate) retention: Duration,
    handlers: RwLock<BTreeMap<String, Registered>>,
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
        Ok(Self {
            store: options.object_store.unwrap_or_else(|| store.clone()),
            prefix: options.path.unwrap_or_else(|| default_inbox_path(path)),
            queues: options.queues.into_iter().collect(),
            retention: options.record_retention,
            handlers: RwLock::new(BTreeMap::new()),
            bound: TimeBound::new(),
        })
    }

    /// Whether the core kinds may touch `queue`.
    pub(crate) fn allows(&self, queue: &str) -> bool {
        self.queues.contains(queue)
    }

    /// Register `handler` for `kind`, or for the entries of `kind` with the
    /// value of `scope` at its field. Fails for a core kind, for a kind with a
    /// registration of the whole kind, for a value with a registration and for
    /// a scope whose field differs from the field of the kind.
    fn register(
        &self,
        kind: &str,
        scope: Option<(&str, &str)>,
        handler: Arc<dyn DynInboxHandler>,
    ) -> Result<()> {
        validate_kind(kind)?;
        let taken = |value: Option<&str>| Error::InboxKindTaken {
            kind: kind.to_string(),
            value: value.map(str::to_string),
        };
        if is_core_kind(kind) {
            return Err(taken(None));
        }
        let mut handlers = self.handlers.write().unwrap();
        match (handlers.get_mut(kind), scope) {
            (None, None) => {
                handlers.insert(kind.to_string(), Registered::Whole(handler));
            }
            (None, Some((field, value))) => {
                handlers.insert(
                    kind.to_string(),
                    Registered::ByField {
                        field: field.to_string(),
                        values: BTreeMap::from([(value.to_string(), handler)]),
                    },
                );
            }
            (Some(Registered::ByField { field, values }), Some((scope_field, value)))
                if field == scope_field =>
            {
                if values.contains_key(value) {
                    return Err(taken(Some(value)));
                }
                values.insert(value.to_string(), handler);
            }
            (Some(_), scope) => return Err(taken(scope.map(|(_, value)| value))),
        }
        Ok(())
    }

    /// How the entries of `kind` reach a handler, or `None` for a kind without
    /// a registration.
    fn lookup(&self, kind: &str) -> Option<Lookup> {
        Some(match self.handlers.read().unwrap().get(kind)? {
            Registered::Whole(handler) => Lookup::Whole(handler.clone()),
            Registered::ByField { field, .. } => Lookup::ByField(field.clone()),
        })
    }

    /// The handler of `kind` for `value`, or `None` without a registration.
    fn scoped(&self, kind: &str, value: &str) -> Option<Arc<dyn DynInboxHandler>> {
        match self.handlers.read().unwrap().get(kind)? {
            Registered::ByField { values, .. } => values.get(value).cloned(),
            Registered::Whole(_) => None,
        }
    }

    /// The entries within the version prefix, in id order. A path outside the
    /// layout is logged and left in place.
    async fn list(&self) -> Result<Vec<Listed>> {
        let prefix = Path::from(format!("{}/v1", self.prefix));
        let mut objects = self.store.list(Some(&prefix));
        let mut entries = Vec::new();
        while let Some(object) = objects.try_next().await.map_err(Error::InboxStore)? {
            let relative = object
                .location
                .as_ref()
                .strip_prefix(prefix.as_ref())
                .and_then(|rest| rest.strip_prefix('/'));
            match relative.and_then(parse_entry_path) {
                Some((kind, id)) => entries.push(Listed {
                    kind,
                    id,
                    location: object.location,
                }),
                None => warn!(path = %object.location, "object outside the inbox layout; skipped"),
            }
        }
        entries.sort_by(|a, b| a.id.cmp(&b.id).then_with(|| a.kind.cmp(&b.kind)));
        Ok(entries)
    }

    async fn get(&self, location: &Path) -> Result<Vec<u8>> {
        let result = self.store.get(location).await.map_err(Error::InboxStore)?;
        Ok(result.bytes().await.map_err(Error::InboxStore)?.to_vec())
    }

    /// Delete the object at `location`. An absent object counts as deleted.
    async fn delete(&self, location: &Path) -> Result<()> {
        match self.store.delete(location).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(err) => Err(Error::InboxStore(err)),
        }
    }
}

/// One listed entry: its path segments and its location.
struct Listed {
    kind: String,
    id: String,
    location: Path,
}

/// The path of the entry `id` of `kind` in the inbox at `prefix`.
pub(crate) fn entry_path(prefix: &str, kind: &str, id: &str) -> String {
    format!("{prefix}/v1/{kind}/{id}")
}

/// The kind and the id of an entry path relative to the version prefix, or
/// `None` for a path outside the layout.
fn parse_entry_path(relative: &str) -> Option<(String, String)> {
    let (kind, id) = relative.split_once('/')?;
    if id.contains('/') || validate_kind(kind).is_err() || validate_id(id).is_err() {
        return None;
    }
    Some((kind.to_string(), id.to_string()))
}

pub(crate) struct Inbox {
    core: Arc<QueueCore>,
}

impl Inbox {
    pub(crate) fn new(core: Arc<QueueCore>) -> Self {
        Self { core }
    }
}

impl Periodic for Inbox {
    const NAME: &'static str = "inbox";

    async fn step(&self) -> Result<()> {
        self.core.inbox_pass().await
    }
}

impl QueueCore {
    /// Register `handler` for its kind on the inbox, as
    /// [`Queue::register_inbox_handler`](crate::Queue::register_inbox_handler)
    /// documents.
    pub(crate) fn register_inbox_handler<H: InboxHandler>(
        self: &Arc<Self>,
        scope: Option<(&str, &str)>,
        handler: H,
    ) -> Result<InboxRegistration> {
        let kind = H::Kind::KIND;
        let inbox = self.inbox.as_ref().ok_or(Error::InboxNotConfigured)?;
        inbox.register(kind, scope, Arc::new(handler))?;
        Ok(InboxRegistration {
            core: self.clone(),
            kind: kind.to_string(),
            value: scope.map(|(_, value)| value.to_string()),
        })
    }

    /// One pass over the inbox, as the module documents. A queue without an
    /// inbox returns without a request.
    pub(crate) async fn inbox_pass(&self) -> Result<()> {
        let Some(inbox) = &self.inbox else {
            return Ok(());
        };
        let passed = self.inbox_pass_over(inbox).await;
        if passed.is_err() {
            crate::obs::inbox_pass_failed();
        }
        passed
    }

    async fn inbox_pass_over(&self, inbox: &InboxState) -> Result<()> {
        for listed in inbox.list().await? {
            let Listed { kind, id, location } = listed;
            if self.db.get(inbox_record_key(&id)).await?.is_some() {
                inbox.delete(&location).await?;
                debug!(kind, id, "inbox entry already applied; object deleted");
                continue;
            }
            let lookup = match CoreKind::parse(&kind) {
                Some(core_kind) => Lookup::Core(core_kind),
                None => match inbox.lookup(&kind) {
                    Some(lookup) => lookup,
                    None => continue,
                },
            };
            let body = inbox.get(&location).await?;
            let applied_at_ms = self.now_ms();
            let settled = match serde_json::from_slice::<serde_json::Value>(&body) {
                Err(err) => {
                    InboxRecordHandle::<()>::new(id.clone(), applied_at_ms).settle(malformed(err))
                }
                Ok(body) => {
                    let route = match lookup {
                        Lookup::Core(core_kind) => Route::Core(core_kind),
                        Lookup::Whole(handler) => Route::Handler(handler),
                        Lookup::ByField(field) => {
                            let value = body.get(&field).and_then(|v| v.as_str());
                            match value.and_then(|value| inbox.scoped(&kind, value)) {
                                Some(handler) => Route::Handler(handler),
                                None => {
                                    debug!(
                                        kind,
                                        id, field, "inbox entry of a scope without a handler; left"
                                    );
                                    continue;
                                }
                            }
                        }
                    };
                    match route {
                        Route::Core(core_kind) => {
                            self.apply_core_kind(inbox, core_kind, &id, applied_at_ms, body)
                                .await
                        }
                        Route::Handler(handler) => handler.apply(&id, applied_at_ms, body).await,
                    }
                }
            };
            let recorded = match settled {
                Ok(settled) => self.record_entry(&id, &kind, settled).await,
                Err(err) => Err(err),
            };
            // A record committed before the error stands.
            let rejected = match recorded {
                Ok(rejected) => rejected,
                Err(err) => match self.view.inbox_record_rejected(&id).await? {
                    Some(rejected) => rejected,
                    None => return Err(err),
                },
            };
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

    /// Verify or commit the record of the entry `id` of `kind` as `settled`
    /// reports it, and return whether the entry was rejected.
    async fn record_entry(&self, id: &str, kind: &str, settled: Settled) -> Result<bool> {
        match settled {
            Settled::Committed => {
                if self.db.get(inbox_record_key(id)).await?.is_none() {
                    return Err(Error::InboxRecordMissing { id: id.to_string() });
                }
                Ok(false)
            }
            Settled::Separately { record, rejection } => {
                let effects = SettlementEffects {
                    inbox_record: Some(record),
                    ..SettlementEffects::default()
                };
                self.commit_effects_where(effects, &[]).await?;
                if let Some(reason) = &rejection {
                    warn!(kind, id, reason, "inbox entry rejected");
                }
                Ok(rejection.is_some())
            }
        }
    }
}

/// The application of one entry: through the core, or through a handler.
enum Route {
    Core(CoreKind),
    Handler(Arc<dyn DynInboxHandler>),
}

#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::json;
use slatedb::object_store::{ObjectStoreExt, PutPayload, path::Path};

use super::*;
use crate::error::Error;
use crate::inbox::entry::{CancelEntry, EnqueueEntry, SignalEntry};
use crate::inbox::sender::InboxSender;
use crate::queue::{CancelOutcome, Queue};
use crate::reader::QueueReader;
use crate::test_util::*;

const INITIAL: u64 = 1_700_000_000_000;
const LEASE: Duration = Duration::from_secs(30);

fn inbox_opts(clock: &MockClock) -> OpenOptions {
    OpenOptions {
        clock: Arc::new(clock.clone()),
        inbox: Some(InboxOptions::default().queue("work").queue("waits")),
        ..no_backoff_opts()
    }
}

async fn open(store: &Arc<dyn ObjectStore>, clock: &MockClock) -> Queue {
    Queue::open_with_options(store.clone(), "test", inbox_opts(clock))
        .await
        .unwrap()
}

fn sender(store: &Arc<dyn ObjectStore>) -> InboxSender {
    InboxSender::new(store.clone(), default_inbox_path("test"))
}

/// Write `body` at the entry path of `kind` and `id`.
async fn put_raw(store: &Arc<dyn ObjectStore>, kind: &str, id: &str, body: &[u8]) {
    let path = Path::from(entry_path("test-inbox", kind, id));
    store
        .put(&path, PutPayload::from(body.to_vec()))
        .await
        .unwrap();
}

/// The options of a job that waits at `key` for an hour.
fn waiting(key: &str) -> EnqueueOptions {
    EnqueueOptions::default()
        .run_at(std::time::UNIX_EPOCH + Duration::from_millis(INITIAL + 3_600_000))
        .signal_key(key.to_string())
}

#[tokio::test]
async fn an_entry_is_applied_once_with_a_record_and_its_object_deleted() {
    let store = make_store();
    let clock = MockClock::new(INITIAL);
    let q = open(&store, &clock).await;
    let id = sender(&store)
        .send(&EnqueueEntry::new("work", b"hello"))
        .await
        .unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/enqueue").await, 1);

    q.apply_inbox_now().await.unwrap();
    let job = q.claim("work", LEASE).await.unwrap().unwrap();
    assert_eq!(job.payload, b"hello");
    assert_eq!(object_count(&store, "test-inbox/v1/enqueue").await, 0);
    let record = q.view().inbox_record(&id).await.unwrap().unwrap();
    assert_eq!(record.applied_at_ms, INITIAL);
    assert_eq!(
        record.outcome,
        InboxOutcome::Applied(json!({ "job_id": job.id, "new": true }))
    );

    // The record is read from another process, and an entry sent again with the
    // id is deleted without an effect.
    let reader = QueueReader::open(store.clone(), "test").await.unwrap();
    assert_eq!(reader.view().inbox_record(&id).await.unwrap(), Some(record));
    reader.close().await.unwrap();
    sender(&store)
        .send_with_id(&id, &EnqueueEntry::new("work", b"again"))
        .await
        .unwrap();
    q.apply_inbox_now().await.unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/enqueue").await, 0);
    assert!(q.claim("work", LEASE).await.unwrap().is_none());
    q.close().await.unwrap();
}

#[tokio::test]
async fn entries_apply_in_id_order_across_kinds() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    let sender = sender(&store);
    sender
        .send_with_id("b", &EnqueueEntry::new("work", b"second"))
        .await
        .unwrap();
    sender
        .send_with_id("a", &EnqueueEntry::new("work", b"first"))
        .await
        .unwrap();
    q.apply_inbox_now().await.unwrap();
    let jobs = q.claim_batch("work", 2, LEASE).await.unwrap();
    let payloads: Vec<&[u8]> = jobs.iter().map(|j| j.payload.as_slice()).collect();
    assert_eq!(payloads, [b"first".as_slice(), b"second".as_slice()]);
    q.close().await.unwrap();
}

#[tokio::test]
async fn a_malformed_or_disallowed_entry_is_rejected_with_a_record() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    put_raw(&store, "enqueue", "bad-json", b"not json").await;
    put_raw(&store, "enqueue", "bad-shape", br#"{"queue": 1}"#).await;
    sender(&store)
        .send_with_id("other-queue", &EnqueueEntry::new("other", b"x"))
        .await
        .unwrap();
    sender(&store)
        .send_with_id(
            "bad-id",
            &EnqueueEntry {
                job_id: Some("a:b".to_string()),
                ..EnqueueEntry::new("work", b"x")
            },
        )
        .await
        .unwrap();

    q.apply_inbox_now().await.unwrap();
    for id in ["bad-json", "bad-shape", "other-queue", "bad-id"] {
        let record = q.view().inbox_record(id).await.unwrap().unwrap();
        assert!(
            matches!(record.outcome, InboxOutcome::Rejected(_)),
            "{id}: {:?}",
            record.outcome
        );
    }
    assert_eq!(object_count(&store, "test-inbox/v1/enqueue").await, 0);
    assert_eq!(q.view().stats("work").await.unwrap().pending, 0);
    assert_eq!(q.view().stats("other").await.unwrap().pending, 0);
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_object_outside_the_layout_is_left_in_place() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    for path in [
        "test-inbox/v2/enqueue/future",
        "test-inbox/v1/enqueue/nested/id",
        "test-inbox/v1/Enqueue/id",
        "test-inbox/v1/enqueue/bad.id",
        "test-inbox/v1/stray",
    ] {
        store
            .put(&Path::from(path), PutPayload::from_static(b"{}"))
            .await
            .unwrap();
    }
    q.apply_inbox_now().await.unwrap();
    assert_eq!(object_count(&store, "test-inbox/v2/enqueue").await, 1);
    assert_eq!(
        object_count(&store, "test-inbox/v1/enqueue/nested").await,
        1
    );
    assert_eq!(object_count(&store, "test-inbox/v1/Enqueue").await, 1);
    assert_eq!(object_count(&store, "test-inbox/v1/enqueue").await, 1);
    assert_eq!(object_count(&store, "test-inbox/v1").await, 1);
    q.close().await.unwrap();
}

/// How the probe handler reports an entry.
#[derive(Clone, Copy)]
enum Report {
    /// Commit the record with a KV write of the body.
    Commit,
    /// Report the outcome without a record.
    Separately,
    /// Report `Committed` without a record.
    Forget,
    /// Commit the record, commit it again and fail with the second error.
    Twice,
}

/// A handler that reports an entry as `report` says, and fails the first call
/// after `fail_once` is set.
struct Probe {
    queue: Arc<Queue>,
    report: Report,
    fail_once: AtomicBool,
}

impl Probe {
    fn new(queue: &Arc<Queue>, report: Report) -> Self {
        Self {
            queue: queue.clone(),
            report,
            fail_once: AtomicBool::new(false),
        }
    }
}

impl InboxHandler for Probe {
    async fn apply(
        &self,
        entry: &InboxEntry,
        record: &InboxRecordHandle,
    ) -> std::result::Result<Applied, InboxHandlerError> {
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err("transient".into());
        }
        let outcome = json!({ "seen": entry.id });
        match self.report {
            Report::Commit => {
                let effects = record
                    .effects(outcome)
                    .kv_put(format!("seen/{}", entry.id), entry.body.to_string());
                self.queue.commit_effects(effects).await?;
                Ok(Applied::Committed)
            }
            Report::Separately => Ok(Applied::Separately(InboxOutcome::Applied(outcome))),
            Report::Forget => Ok(Applied::Committed),
            Report::Twice => {
                self.queue
                    .commit_effects(record.effects(outcome.clone()))
                    .await?;
                let again = self
                    .queue
                    .commit_effects(record.effects(json!({ "seen": "again" })))
                    .await
                    .unwrap_err();
                Err(again.into())
            }
        }
    }
}

/// Close `queue`, of which the test has the only reference.
async fn close(queue: Arc<Queue>) {
    Arc::into_inner(queue)
        .expect("the registrations are dropped before the close")
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn an_entry_of_an_unregistered_kind_waits_for_its_handler() {
    let store = make_store();
    let q = Arc::new(open(&store, &MockClock::new(INITIAL)).await);
    put_raw(&store, "custom", "c1", br#"{"n": 1}"#).await;
    q.apply_inbox_now().await.unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/custom").await, 1);
    assert_eq!(q.view().inbox_record("c1").await.unwrap(), None);

    let registration = q
        .register_inbox_handler("custom", Probe::new(&q, Report::Commit))
        .unwrap();
    q.apply_inbox_now().await.unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/custom").await, 0);
    assert_eq!(
        q.view().inbox_record("c1").await.unwrap().unwrap().outcome,
        InboxOutcome::Applied(json!({ "seen": "c1" }))
    );
    assert_eq!(
        q.view().kv_get(b"seen/c1").await.unwrap().as_deref(),
        Some(br#"{"n":1}"#.as_slice())
    );

    // A kind with a live registration, a core kind and an invalid kind are
    // refused.
    assert!(matches!(
        q.register_inbox_handler("custom", Probe::new(&q, Report::Commit)),
        Err(Error::InboxKindTaken { value: None, .. })
    ));
    assert!(matches!(
        q.register_inbox_handler("enqueue", Probe::new(&q, Report::Commit)),
        Err(Error::InboxKindTaken { .. })
    ));
    assert!(matches!(
        q.register_inbox_handler("Custom", Probe::new(&q, Report::Commit)),
        Err(Error::InvalidInboxKind { .. })
    ));

    // The registration ends with its guard: the kind is free again, and an
    // entry of it waits.
    drop(registration);
    put_raw(&store, "custom", "c2", br#"{"n": 2}"#).await;
    q.apply_inbox_now().await.unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/custom").await, 1);
    let registration = q
        .register_inbox_handler("custom", Probe::new(&q, Report::Separately))
        .unwrap();
    q.apply_inbox_now().await.unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/custom").await, 0);
    assert_eq!(
        q.view().inbox_record("c2").await.unwrap().unwrap().outcome,
        InboxOutcome::Applied(json!({ "seen": "c2" }))
    );
    drop(registration);
    close(q).await;
}

#[tokio::test]
async fn a_scoped_registration_serves_the_entries_that_name_its_value() {
    let store = make_store();
    let q = Arc::new(open(&store, &MockClock::new(INITIAL)).await);
    let to_a = q
        .register_inbox_handler_for("custom", "target", "a", Probe::new(&q, Report::Commit))
        .unwrap();
    let to_b = q
        .register_inbox_handler_for("custom", "target", "b", Probe::new(&q, Report::Separately))
        .unwrap();
    put_raw(&store, "custom", "to-a", br#"{"target": "a"}"#).await;
    put_raw(&store, "custom", "to-b", br#"{"target": "b"}"#).await;
    put_raw(&store, "custom", "to-c", br#"{"target": "c"}"#).await;
    put_raw(&store, "custom", "none", b"{}").await;

    q.apply_inbox_now().await.unwrap();
    assert!(q.view().kv_get(b"seen/to-a").await.unwrap().is_some());
    assert_eq!(
        q.view()
            .inbox_record("to-b")
            .await
            .unwrap()
            .unwrap()
            .outcome,
        InboxOutcome::Applied(json!({ "seen": "to-b" }))
    );
    assert!(q.view().kv_get(b"seen/to-b").await.unwrap().is_none());
    // The entries without a handler for their value wait.
    assert_eq!(object_count(&store, "test-inbox/v1/custom").await, 2);
    assert_eq!(q.view().inbox_record("to-c").await.unwrap(), None);

    // A value with a registration, another field and the whole kind are refused
    // while a scoped registration lives.
    assert!(matches!(
        q.register_inbox_handler_for("custom", "target", "a", Probe::new(&q, Report::Commit)),
        Err(Error::InboxKindTaken { ref value, .. }) if value.as_deref() == Some("a")
    ));
    assert!(matches!(
        q.register_inbox_handler_for("custom", "other", "z", Probe::new(&q, Report::Commit)),
        Err(Error::InboxKindTaken { ref value, .. }) if value.as_deref() == Some("z")
    ));
    assert!(matches!(
        q.register_inbox_handler("custom", Probe::new(&q, Report::Commit)),
        Err(Error::InboxKindTaken { value: None, .. })
    ));

    // Once the scoped registrations end, the whole kind is free again. Its
    // handler then applies every value.
    drop(to_a);
    drop(to_b);
    let whole = q
        .register_inbox_handler("custom", Probe::new(&q, Report::Separately))
        .unwrap();
    q.apply_inbox_now().await.unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/custom").await, 0);
    assert!(q.view().inbox_record("none").await.unwrap().is_some());
    drop(whole);
    close(q).await;
}

#[tokio::test]
async fn a_queue_without_an_inbox_refuses_a_registration() {
    let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
    assert!(matches!(
        q.register_inbox_handler("custom", Probe::new(&q, Report::Commit)),
        Err(Error::InboxNotConfigured)
    ));
    q.apply_inbox_now().await.unwrap();
    close(q).await;
}

#[tokio::test]
async fn a_failed_entry_ends_the_pass_and_the_next_pass_starts_at_it() {
    let store = make_store();
    let q = Arc::new(open(&store, &MockClock::new(INITIAL)).await);
    let probe = Probe::new(&q, Report::Commit);
    probe.fail_once.store(true, Ordering::SeqCst);
    let registration = q.register_inbox_handler("custom", probe).unwrap();
    put_raw(&store, "custom", "a", b"{}").await;
    put_raw(&store, "custom", "b", b"{}").await;

    let err = q.apply_inbox_now().await.unwrap_err();
    assert!(matches!(err, Error::InboxHandler { ref kind, .. } if kind == "custom"));
    assert!(!err.is_permanent());
    assert_eq!(object_count(&store, "test-inbox/v1/custom").await, 2);
    assert_eq!(q.view().inbox_record("a").await.unwrap(), None);

    q.apply_inbox_now().await.unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/custom").await, 0);
    for id in ["a", "b"] {
        assert!(q.view().inbox_record(id).await.unwrap().is_some(), "{id}");
    }
    drop(registration);
    close(q).await;
}

#[tokio::test]
async fn a_second_record_of_an_entry_aborts_its_commit_and_the_first_stands() {
    let store = make_store();
    let q = Arc::new(open(&store, &MockClock::new(INITIAL)).await);
    let registration = q
        .register_inbox_handler("custom", Probe::new(&q, Report::Twice))
        .unwrap();
    put_raw(&store, "custom", "a", b"{}").await;

    // The handler fails after its commit, and the pass keeps the record.
    q.apply_inbox_now().await.unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/custom").await, 0);
    assert_eq!(
        q.view().inbox_record("a").await.unwrap().unwrap().outcome,
        InboxOutcome::Applied(json!({ "seen": "a" }))
    );
    let err = q
        .commit_effects(InboxRecordHandle::new("a".to_string(), INITIAL).effects(json!({})))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InboxAlreadyApplied { ref id } if id == "a"));
    assert!(err.is_permanent());
    drop(registration);
    close(q).await;
}

#[tokio::test]
async fn the_ids_of_one_sender_increase_with_call_order() {
    let store = make_store();
    let sender = sender(&store);
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(
            sender
                .send(&CancelEntry { job_id: "j".into() })
                .await
                .unwrap(),
        );
    }
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "{ids:?}");
}

#[tokio::test]
async fn a_handler_that_forgets_the_record_stops_the_pass() {
    let store = make_store();
    let q = Arc::new(open(&store, &MockClock::new(INITIAL)).await);
    let registration = q
        .register_inbox_handler("custom", Probe::new(&q, Report::Forget))
        .unwrap();
    put_raw(&store, "custom", "a", b"{}").await;
    put_raw(&store, "custom", "b", b"{}").await;

    let err = q.apply_inbox_now().await.unwrap_err();
    assert!(matches!(err, Error::InboxRecordMissing { ref id } if id == "a"));
    assert_eq!(object_count(&store, "test-inbox/v1/custom").await, 2);
    drop(registration);
    close(q).await;
}

#[tokio::test]
async fn the_cancel_kind_removes_a_queued_job_and_records_the_other_outcomes() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    let queued = q.enqueue("work", b"a".to_vec()).await.unwrap();
    let claimed = q.enqueue("work", b"b".to_vec()).await.unwrap();
    let claim = q.claim_by_id(&claimed, LEASE).await.unwrap();
    let other = q.enqueue("other", b"c".to_vec()).await.unwrap();
    let sender = sender(&store);
    for (id, job_id) in [
        ("queued", queued.as_str()),
        ("claimed", claimed.as_str()),
        ("other", other.as_str()),
        ("missing", "01ARZ3NDEKTSV4RRFFQ69G5FAV"),
    ] {
        sender
            .send_with_id(
                id,
                &CancelEntry {
                    job_id: job_id.to_string(),
                },
            )
            .await
            .unwrap();
    }

    q.apply_inbox_now().await.unwrap();
    let outcome = |id: &str| {
        let q = &q;
        let id = id.to_string();
        async move { q.view().inbox_record(&id).await.unwrap().unwrap().outcome }
    };
    assert_eq!(
        outcome("queued").await,
        InboxOutcome::Applied(json!({ "outcome": "removed" }))
    );
    assert!(q.view().get_job(&queued).await.unwrap().is_none());
    assert_eq!(
        outcome("claimed").await,
        InboxOutcome::Applied(json!({ "outcome": "requested" }))
    );
    assert_eq!(
        outcome("missing").await,
        InboxOutcome::Applied(json!({ "outcome": "not_found" }))
    );
    assert!(matches!(outcome("other").await, InboxOutcome::Rejected(_)));
    assert_eq!(q.view().stats("other").await.unwrap().pending, 1);
    assert_eq!(q.cancel(&claimed).await.unwrap(), CancelOutcome::Requested);
    drop(claim);
    q.close().await.unwrap();
}

#[tokio::test]
async fn the_signal_kind_wakes_a_waiter_or_buffers_and_records_the_outcome() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    let waiter = q
        .enqueue_with("waits", b"w".to_vec(), waiting("order-1"))
        .await
        .unwrap();
    let sender = sender(&store);
    for (id, key) in [("to-waiter", "order-1"), ("to-nobody", "order-2")] {
        sender
            .send_with_id(
                id,
                &SignalEntry {
                    key: key.to_string(),
                    payload: b"go".to_vec(),
                },
            )
            .await
            .unwrap();
    }

    q.apply_inbox_now().await.unwrap();
    let woken = q.claim("waits", LEASE).await.unwrap().unwrap();
    assert_eq!(woken.id, waiter);
    assert_eq!(woken.wake_payload.as_deref(), Some(b"go".as_slice()));
    assert_eq!(
        q.view()
            .inbox_record("to-waiter")
            .await
            .unwrap()
            .unwrap()
            .outcome,
        InboxOutcome::Applied(json!({ "outcome": "delivered" }))
    );
    assert_eq!(
        q.view()
            .inbox_record("to-nobody")
            .await
            .unwrap()
            .unwrap()
            .outcome,
        InboxOutcome::Applied(json!({ "outcome": "buffered" }))
    );
    assert!(q.clear_signal("order-2").await.unwrap());
    q.close().await.unwrap();
}

#[tokio::test]
async fn the_first_pass_at_open_precedes_the_scheduler() {
    let store = make_store();
    let clock = MockClock::new(INITIAL);
    let q = open(&store, &clock).await;
    let waiter = q
        .enqueue_with(
            "waits",
            b"w".to_vec(),
            EnqueueOptions::default()
                .run_at(std::time::UNIX_EPOCH + Duration::from_millis(INITIAL + 10_000))
                .signal_key("order-1".to_string()),
        )
        .await
        .unwrap();
    q.close().await.unwrap();

    // The signal arrives and the timeout passes while the process is down.
    sender(&store)
        .send(&SignalEntry {
            key: "order-1".to_string(),
            payload: b"late".to_vec(),
        })
        .await
        .unwrap();
    clock.advance(Duration::from_secs(60));

    let q = open(&store, &clock).await;
    let job = q.view().get_job(&waiter).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Pending);
    assert_eq!(job.wake_payload.as_deref(), Some(b"late".as_slice()));
    assert_eq!(object_count(&store, "test-inbox/v1/signal").await, 0);
    q.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_expired_record_is_swept_and_its_id_applies_again() {
    let store = make_store();
    let clock = MockClock::new(INITIAL);
    let retention = Duration::from_secs(60);
    let reaper_interval = Duration::from_millis(10);
    let opts = OpenOptions {
        reaper_interval,
        inbox: Some(
            InboxOptions::default()
                .queue("work")
                .record_retention(retention),
        ),
        ..inbox_opts(&clock)
    };
    let q = Queue::open_with_options(store.clone(), "test", opts)
        .await
        .unwrap();
    let sender = sender(&store);
    sender
        .send_with_id("once", &EnqueueEntry::new("work", b"x"))
        .await
        .unwrap();
    q.apply_inbox_now().await.unwrap();

    clock.advance(retention - Duration::from_millis(1));
    tokio::time::sleep(reaper_interval * 2).await;
    assert!(q.view().inbox_record("once").await.unwrap().is_some());

    clock.advance(Duration::from_millis(1));
    tokio::time::sleep(reaper_interval * 2).await;
    assert_eq!(q.view().inbox_record("once").await.unwrap(), None);
    assert!(
        q.core
            .db
            .scan_prefix(tag_prefix(KeyTag::InboxExpiry), ..)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .is_none()
    );

    sender
        .send_with_id("once", &EnqueueEntry::new("work", b"y"))
        .await
        .unwrap();
    q.apply_inbox_now().await.unwrap();
    assert_eq!(q.view().stats("work").await.unwrap().pending, 2);
    q.close().await.unwrap();
}

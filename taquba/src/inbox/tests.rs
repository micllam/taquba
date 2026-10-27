use slatedb::object_store::{ObjectStoreExt, PutPayload, path::Path};

use super::*;
use crate::error::Error;
use crate::inbox::entry::{CancelEntry, EnqueueEntry, InboxEntry, SignalEntry};
use crate::inbox::record::{InboxOutcome, InboxRecordHandle};
use crate::inbox::sender::InboxSender;
use crate::queue::{CancelOutcome, EnqueueResult, Queue, SignalOutcome};
use crate::reader::QueueReader;
use crate::test_util::*;

const INITIAL: u64 = 1_700_000_000_000;
const LEASE: Duration = Duration::from_secs(30);
const WORK: &str = "work";
const WAITS: &str = "waits";

fn inbox_opts(clock: &MockClock) -> OpenOptions {
    OpenOptions {
        clock: Arc::new(clock.clone()),
        // A test runs its passes through `apply_inbox_now`.
        inbox: Some(InboxOptions::new(["work", "waits"]).touch_poll_interval(None)),
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

/// Write `body` at the entry path of `queue`, `kind` and `id`.
async fn put_raw(store: &Arc<dyn ObjectStore>, queue: &str, kind: &str, id: &str, body: &[u8]) {
    let path = Path::from(entry_path("test-inbox", queue, kind, id));
    store
        .put(&path, PutPayload::from(body.to_vec()))
        .await
        .unwrap();
}

/// The options of a job that waits for a signal for an hour.
fn waiting() -> EnqueueOptions {
    EnqueueOptions::default()
        .run_at(std::time::UNIX_EPOCH + Duration::from_millis(INITIAL + 3_600_000))
        .signal_wait(true)
}

#[tokio::test]
async fn an_entry_is_applied_once_with_a_record_and_its_object_deleted() {
    let store = make_store();
    let clock = MockClock::new(INITIAL);
    let q = open(&store, &clock).await;
    let id = sender(&store)
        .send(WORK, EnqueueEntry::new(b"hello"))
        .await
        .unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/work/enqueue").await, 1);

    q.apply_inbox_now().await.unwrap();
    let job = q.claim("work", LEASE).await.unwrap().unwrap();
    assert_eq!(job.payload, b"hello");
    assert_eq!(object_count(&store, "test-inbox/v1/work/enqueue").await, 0);
    let record = q.view().inbox_record(&id).await.unwrap().unwrap();
    assert_eq!(record.applied_at_ms, INITIAL);
    assert_eq!(
        record.outcome,
        InboxOutcome::Enqueue(EnqueueResult::New(job.id.clone()))
    );

    // The record is read from another process, and an entry sent again with the
    // id is deleted without an effect.
    let reader = QueueReader::open(store.clone(), "test").await.unwrap();
    assert_eq!(reader.view().inbox_record(&id).await.unwrap(), Some(record));
    reader.close().await.unwrap();
    sender(&store)
        .send_with_id(WORK, &id, EnqueueEntry::new(b"again"))
        .await
        .unwrap();
    q.apply_inbox_now().await.unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/work/enqueue").await, 0);
    assert!(q.claim("work", LEASE).await.unwrap().is_none());
    q.close().await.unwrap();
}

#[tokio::test]
async fn entries_apply_in_id_order_across_kinds() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    let sender = sender(&store);
    sender
        .send_with_id(WORK, "b", EnqueueEntry::new(b"second"))
        .await
        .unwrap();
    sender
        .send_with_id(WORK, "a", EnqueueEntry::new(b"first"))
        .await
        .unwrap();
    q.apply_inbox_now().await.unwrap();
    let jobs = q.claim_batch("work", 2, LEASE).await.unwrap();
    let payloads: Vec<&[u8]> = jobs.iter().map(|j| j.payload.as_slice()).collect();
    assert_eq!(payloads, [b"first".as_slice(), b"second".as_slice()]);
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_entry_that_cannot_apply_is_rejected_with_a_record() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    put_raw(&store, WORK, "enqueue", "bad-json", b"not json").await;
    put_raw(&store, WORK, "enqueue", "bad-shape", br#"{"payload": 1}"#).await;
    put_raw(&store, WORK, "custom", "unknown-kind", b"{}").await;
    sender(&store)
        .send_with_id("other", "other-queue", EnqueueEntry::new(b"x"))
        .await
        .unwrap();
    put_raw(&store, "other", "custom", "other-custom", b"{}").await;
    sender(&store)
        .send_with_id(
            WORK,
            "bad-id",
            EnqueueEntry {
                job_id: Some("a:b".to_string()),
                ..EnqueueEntry::new(b"x")
            },
        )
        .await
        .unwrap();

    q.apply_inbox_now().await.unwrap();
    for id in [
        "bad-json",
        "bad-shape",
        "unknown-kind",
        "other-queue",
        "other-custom",
        "bad-id",
    ] {
        let record = q.view().inbox_record(id).await.unwrap().unwrap();
        assert!(
            matches!(record.outcome, InboxOutcome::Rejected(_)),
            "{id}: {:?}",
            record.outcome
        );
    }
    for prefix in [
        "test-inbox/v1/work/enqueue",
        "test-inbox/v1/work/custom",
        "test-inbox/v1/other/enqueue",
        "test-inbox/v1/other/custom",
    ] {
        assert_eq!(object_count(&store, prefix).await, 0, "{prefix}");
    }
    assert_eq!(q.view().stats("work").await.unwrap().pending, 0);
    assert_eq!(q.view().stats("other").await.unwrap().pending, 0);
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_object_outside_the_layout_is_left_in_place() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    for path in [
        "test-inbox/v2/work/enqueue/future",
        "test-inbox/v1/work/enqueue/nested/id",
        "test-inbox/v1/work/enqueue/bad.id",
        "test-inbox/v1/-q/enqueue/id",
        "test-inbox/v1/work/stray",
        "test-inbox/v1/stray",
    ] {
        store
            .put(&Path::from(path), PutPayload::from_static(b"{}"))
            .await
            .unwrap();
    }
    q.apply_inbox_now().await.unwrap();
    for (prefix, count) in [
        ("test-inbox/v2/work/enqueue", 1),
        ("test-inbox/v1/work/enqueue/nested", 1),
        ("test-inbox/v1/work/enqueue", 1),
        ("test-inbox/v1/-q/enqueue", 1),
        ("test-inbox/v1/work", 1),
        ("test-inbox/v1", 1),
    ] {
        assert_eq!(object_count(&store, prefix).await, count, "{prefix}");
    }
    q.close().await.unwrap();
}

#[tokio::test]
async fn a_second_record_of_an_entry_aborts_its_commit() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    sender(&store)
        .send_with_id(WORK, "a", EnqueueEntry::new(b"x"))
        .await
        .unwrap();
    q.apply_inbox_now().await.unwrap();
    let record = InboxRecordHandle::new("a".to_string(), INITIAL);
    let err = q
        .commit_effects(
            record
                .effects(InboxOutcome::Rejected(String::new()))
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InboxAlreadyApplied { ref id } if id == "a"));
    assert!(err.is_permanent());
    q.close().await.unwrap();
}

#[tokio::test]
async fn the_ids_of_one_sender_increase_with_call_order() {
    let store = make_store();
    let sender = sender(&store);
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(
            sender
                .send(WORK, CancelEntry { job_id: "j".into() })
                .await
                .unwrap(),
        );
    }
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "{ids:?}");
}

#[tokio::test]
async fn a_permanent_error_of_a_kind_rejects_the_entry_and_the_pass_continues() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    // The index of the job `corrupt` refers to a record that does not decode.
    q.core
        .db
        .put(crate::keys::job_index_key("corrupt"), b"corrupt-record")
        .await
        .unwrap();
    q.core
        .db
        .put(b"corrupt-record", b"not a job record")
        .await
        .unwrap();
    let sender = sender(&store);
    sender
        .send_with_id(
            WORK,
            "a",
            CancelEntry {
                job_id: "corrupt".into(),
            },
        )
        .await
        .unwrap();
    sender
        .send_with_id(WORK, "b", EnqueueEntry::new(b"x"))
        .await
        .unwrap();

    q.apply_inbox_now().await.unwrap();
    let cancel = q.view().inbox_record("a").await.unwrap().unwrap();
    assert!(matches!(cancel.outcome, InboxOutcome::Rejected(_)));
    assert!(q.view().inbox_record("b").await.unwrap().is_some());
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_inbox_path_within_the_queue_path_of_its_store_fails_the_open() {
    let store = make_store();
    let clock = MockClock::new(INITIAL);
    for inside in ["test", "test/inbox"] {
        let opts = OpenOptions {
            inbox: Some(InboxOptions::new([WORK]).path(inside.to_string())),
            ..inbox_opts(&clock)
        };
        assert!(
            matches!(
                Queue::open_with_options(store.clone(), "test", opts).await,
                Err(Error::InboxPathWithinQueue { .. })
            ),
            "{inside}"
        );
    }
    // A sibling path, or the same path in a store of its own, opens.
    let sibling = OpenOptions {
        inbox: Some(InboxOptions::new([WORK]).path("test-commands".to_string())),
        ..inbox_opts(&clock)
    };
    let q = Queue::open_with_options(store.clone(), "test", sibling)
        .await
        .unwrap();
    q.close().await.unwrap();
    let separate = OpenOptions {
        inbox: Some(
            InboxOptions::new([WORK])
                .path("test".to_string())
                .object_store(make_store()),
        ),
        ..inbox_opts(&clock)
    };
    let q = Queue::open_with_options(store, "test", separate)
        .await
        .unwrap();
    q.close().await.unwrap();
}

#[tokio::test]
async fn the_inbox_accepts_the_queues_of_its_allow_list() {
    let q = open(&make_store(), &MockClock::new(INITIAL)).await;
    assert!(q.inbox_accepts(WORK));
    assert!(!q.inbox_accepts("other"));
    q.close().await.unwrap();
    let without_inbox = OpenOptions {
        inbox: None,
        ..inbox_opts(&MockClock::new(INITIAL))
    };
    let q = Queue::open_with_options(make_store(), "test", without_inbox)
        .await
        .unwrap();
    assert!(!q.inbox_accepts(WORK));
    q.close().await.unwrap();
}

#[tokio::test]
async fn a_send_and_a_listing_refuse_an_invalid_queue_name() {
    let store = make_store();
    assert!(matches!(
        sender(&store).send("a/b", EnqueueEntry::new(b"x")).await,
        Err(Error::InvalidQueueName { .. })
    ));
    assert!(matches!(
        sender(&store).pending("a/b").await,
        Err(Error::InvalidQueueName { .. })
    ));
    assert_eq!(object_count(&store, "test-inbox/v1").await, 0);
}

#[tokio::test]
async fn the_pending_entries_of_a_queue_are_listed_in_id_order_until_their_pass() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    let job_a = q.enqueue("work", b"a".to_vec()).await.unwrap();
    let job_b = q.enqueue("work", b"b".to_vec()).await.unwrap();
    let sender = sender(&store);
    for (id, job_id) in [("b", job_b.as_str()), ("a", job_a.as_str())] {
        sender
            .send_with_id(
                WORK,
                id,
                CancelEntry {
                    job_id: job_id.to_string(),
                },
            )
            .await
            .unwrap();
    }
    sender
        .send_with_id(WAITS, "w", CancelEntry { job_id: "w".into() })
        .await
        .unwrap();
    sender
        .send_with_id(WORK, "c", EnqueueEntry::new(b"x"))
        .await
        .unwrap();
    put_raw(&store, WORK, "cancel", "bad", br#"{"job": 1}"#).await;
    put_raw(&store, WORK, "cancel", "bad.id", b"{}").await;

    let pending = sender.pending(WORK).await.unwrap();
    let listed: Vec<(&str, &InboxEntry)> =
        pending.iter().map(|p| (p.id.as_str(), &p.entry)).collect();
    let cancel = |job_id: &str| {
        InboxEntry::Cancel(CancelEntry {
            job_id: job_id.to_string(),
        })
    };
    assert_eq!(
        listed,
        [
            ("a", &cancel(&job_a)),
            ("b", &cancel(&job_b)),
            ("c", &InboxEntry::Enqueue(EnqueueEntry::new(b"x"))),
        ]
    );
    let written = store
        .head(&Path::from(entry_path("test-inbox", WORK, "cancel", "a")))
        .await
        .unwrap()
        .last_modified;
    assert_eq!(pending[0].sent_at_ms as i64, written.timestamp_millis());

    // The pass deletes the applied entries, and an entry sent again with the id
    // of an applied one is listed until the next pass deletes it.
    q.apply_inbox_now().await.unwrap();
    assert!(sender.pending(WORK).await.unwrap().is_empty());
    sender
        .send_with_id(
            WORK,
            "a",
            CancelEntry {
                job_id: job_a.clone(),
            },
        )
        .await
        .unwrap();
    let again = sender.pending(WORK).await.unwrap();
    assert_eq!(again.len(), 1);
    assert!(q.view().inbox_record("a").await.unwrap().is_some());
    q.close().await.unwrap();
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
                WORK,
                id,
                CancelEntry {
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
        InboxOutcome::Cancel(CancelOutcome::Removed)
    );
    assert!(q.view().get_job(&queued).await.unwrap().is_none());
    assert_eq!(
        outcome("claimed").await,
        InboxOutcome::Cancel(CancelOutcome::Requested)
    );
    assert_eq!(
        outcome("missing").await,
        InboxOutcome::Cancel(CancelOutcome::NotFound)
    );
    // The job of another queue than the entry's is out of reach.
    assert!(matches!(outcome("other").await, InboxOutcome::Rejected(_)));
    assert_eq!(q.view().stats("other").await.unwrap().pending, 1);
    assert_eq!(q.cancel(&claimed).await.unwrap(), CancelOutcome::Requested);
    drop(claim);
    q.close().await.unwrap();
}

#[tokio::test]
async fn the_signal_kind_wakes_a_waiter_of_its_queue_or_records_that_none_waits() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    let waiter = q
        .enqueue_with("waits", b"w".to_vec(), waiting())
        .await
        .unwrap();
    let sender = sender(&store);
    for (queue, id, job_id) in [
        (WORK, "from-work", waiter.as_str()),
        (WAITS, "to-waiter", waiter.as_str()),
        (WAITS, "to-nobody", "absent"),
    ] {
        sender
            .send_with_id(
                queue,
                id,
                SignalEntry {
                    job_id: job_id.to_string(),
                    payload: id.as_bytes().to_vec(),
                },
            )
            .await
            .unwrap();
    }

    q.apply_inbox_now().await.unwrap();
    let woken = q.claim("waits", LEASE).await.unwrap().unwrap();
    assert_eq!(woken.id, waiter);
    assert_eq!(woken.signal.as_deref(), Some(b"to-waiter".as_slice()));
    let outcome = |id: &'static str| {
        let view = q.view();
        async move { view.inbox_record(id).await.unwrap().unwrap().outcome }
    };
    // The signal for another queue than the waiter's is rejected.
    assert!(matches!(
        outcome("from-work").await,
        InboxOutcome::Rejected(_)
    ));
    assert_eq!(
        outcome("to-waiter").await,
        InboxOutcome::Signal(SignalOutcome::Delivered)
    );
    assert_eq!(
        outcome("to-nobody").await,
        InboxOutcome::Signal(SignalOutcome::NotWaiting)
    );
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
                .signal_wait(true),
        )
        .await
        .unwrap();
    q.close().await.unwrap();

    // The signal arrives and the timeout passes while the process is down.
    sender(&store)
        .send(
            WAITS,
            SignalEntry {
                job_id: waiter.clone(),
                payload: b"late".to_vec(),
            },
        )
        .await
        .unwrap();
    clock.advance(Duration::from_secs(60));

    let q = open(&store, &clock).await;
    let job = q.view().get_job(&waiter).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Pending);
    assert_eq!(job.signal.as_deref(), Some(b"late".as_slice()));
    assert_eq!(object_count(&store, "test-inbox/v1/waits/signal").await, 0);
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
        inbox: Some(InboxOptions::new(["work"]).record_retention(retention)),
        ..inbox_opts(&clock)
    };
    let q = Queue::open_with_options(store.clone(), "test", opts)
        .await
        .unwrap();
    let sender = sender(&store);
    sender
        .send_with_id(WORK, "once", EnqueueEntry::new(b"x"))
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
        .send_with_id(WORK, "once", EnqueueEntry::new(b"y"))
        .await
        .unwrap();
    q.apply_inbox_now().await.unwrap();
    assert_eq!(q.view().stats("work").await.unwrap().pending, 2);
    q.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_entry_applies_after_a_touch_and_without_one_at_the_listing_interval() {
    let store = make_store();
    let clock = MockClock::new(INITIAL);
    let opts = OpenOptions {
        inbox: Some(InboxOptions::new(["work"])),
        ..inbox_opts(&clock)
    };
    let q = Queue::open_with_options(store.clone(), "test", opts)
        .await
        .unwrap();
    let applied = |id: &'static str| {
        let view = q.view();
        async move { view.inbox_record(id).await.unwrap().is_some() }
    };

    sender(&store)
        .send_with_id(WORK, "touched", EnqueueEntry::new(b"x"))
        .await
        .unwrap();
    tokio::time::sleep(DEFAULT_INBOX_TOUCH_POLL_INTERVAL * 2).await;
    assert!(applied("touched").await);

    let body = serde_json::to_vec(&EnqueueEntry::new(b"y")).unwrap();
    put_raw(&store, WORK, "enqueue", "quiet", &body).await;
    tokio::time::sleep(DEFAULT_INBOX_TOUCH_POLL_INTERVAL * 2).await;
    assert!(!applied("quiet").await);
    tokio::time::sleep(DEFAULT_INBOX_INTERVAL).await;
    assert!(applied("quiet").await);
    q.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_pass_that_fails_at_the_store_runs_again_at_the_next_poll() {
    let store = make_store();
    let faults = FaultStore::wrap();
    let inbox_store: Arc<dyn ObjectStore> = faults.clone();
    let clock = MockClock::new(INITIAL);
    let opts = OpenOptions {
        inbox: Some(InboxOptions::new(["work"]).object_store(inbox_store.clone())),
        ..inbox_opts(&clock)
    };
    let q = Queue::open_with_options(store, "test", opts).await.unwrap();

    // The pass after the touch applies the entry and fails at its delete.
    faults.fail_deletes(true);
    sender(&inbox_store)
        .send_with_id(WORK, "a", EnqueueEntry::new(b"x"))
        .await
        .unwrap();
    tokio::time::sleep(DEFAULT_INBOX_TOUCH_POLL_INTERVAL * 2).await;
    assert!(q.view().inbox_record("a").await.unwrap().is_some());
    assert_eq!(
        object_count(&inbox_store, "test-inbox/v1/work/enqueue").await,
        1
    );

    faults.fail_deletes(false);
    tokio::time::sleep(DEFAULT_INBOX_TOUCH_POLL_INTERVAL * 2).await;
    assert_eq!(
        object_count(&inbox_store, "test-inbox/v1/work/enqueue").await,
        0
    );
    q.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_failed_touch_leaves_the_send_successful() {
    let faults = FaultStore::wrap();
    let store: Arc<dyn ObjectStore> = faults.clone();
    // The entry's put succeeds, and both puts of the touch object fail.
    faults.fail_puts_after(1);
    sender(&store)
        .send(WORK, EnqueueEntry::new(b"x"))
        .await
        .unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/work/enqueue").await, 1);
    let touch = Path::from(touch_path("test-inbox"));
    assert!(store.head(&touch).await.is_err());
}

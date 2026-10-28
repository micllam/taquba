use slatedb::object_store::{ObjectStoreExt, PutPayload, path::Path};
use ulid::Ulid;

use super::*;
use crate::error::{Error, FormError};
use crate::inbox::entry::{
    CancelEntry, EnqueueEntry, EntryKind, InboxEntry, MAX_INBOX_ENTRY_SIZE, SignalEntry,
};
use crate::inbox::listing::entry_path;
use crate::inbox::record::{InboxOutcome, Rejection};
use crate::inbox::sender::{EntryRead, InboxSender};
use crate::queue::{CancelOutcome, ClaimOutcome, EnqueueResult, Queue, SignalOutcome};
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
    let kind = EntryKind::of_segment(kind);
    let path = Path::from(entry_path("test-inbox", queue, &kind, Ulid::new(), id));
    store
        .put(&path, PutPayload::from(body.to_vec()))
        .await
        .unwrap();
}

/// A body of one field `key` with the integer `value`.
fn map_body(key: &str, value: u32) -> Vec<u8> {
    crate::codec::encode(&std::collections::HashMap::from([(key, value)]))
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
    let sender = sender(&store);
    let id = sender
        .send(WORK, EnqueueEntry::new(b"hello"))
        .await
        .unwrap();
    // A resend before the pass writes an object of its own.
    sender
        .send_with_id(WORK, &id, EnqueueEntry::new(b"resent"))
        .await
        .unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/work/enqueue").await, 2);

    q.apply_inbox_now().await.unwrap();
    let job = q.claim("work", LEASE).await.unwrap().unwrap();
    assert_eq!(job.payload, b"hello");
    assert!(q.claim("work", LEASE).await.unwrap().is_none());
    assert_eq!(object_count(&store, "test-inbox/v1/work/enqueue").await, 0);
    let record = q.view().inbox_record(WORK, &id).await.unwrap().unwrap();
    assert_eq!(record.applied_at_ms, INITIAL);
    assert_eq!(
        record.outcome,
        InboxOutcome::Enqueue(EnqueueResult::New(job.id.clone()))
    );

    // The record is read from another process. An entry sent again with the id
    // is deleted without an effect, and the id applies on another queue.
    let reader = QueueReader::open(store.clone(), "test").await.unwrap();
    assert_eq!(
        reader.view().inbox_record(WORK, &id).await.unwrap(),
        Some(record)
    );
    reader.close().await.unwrap();
    for queue in [WORK, WAITS] {
        sender
            .send_with_id(queue, &id, EnqueueEntry::new(b"again"))
            .await
            .unwrap();
    }
    q.apply_inbox_now().await.unwrap();
    assert_eq!(object_count(&store, "test-inbox/v1/work/enqueue").await, 0);
    assert!(q.claim("work", LEASE).await.unwrap().is_none());
    let other = q.claim("waits", LEASE).await.unwrap().unwrap();
    assert_eq!(
        q.view()
            .inbox_record(WAITS, &id)
            .await
            .unwrap()
            .unwrap()
            .outcome,
        InboxOutcome::Enqueue(EnqueueResult::New(other.id.clone()))
    );
    q.close().await.unwrap();
}

#[tokio::test]
async fn the_entries_of_a_process_apply_in_send_order_across_senders_kinds_and_ids() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    // In id order, the generated id applies first and the cancel before the
    // enqueue of its job. Each send has a sender of its own.
    sender(&store)
        .send_with_id(
            WORK,
            "z",
            EnqueueEntry {
                options: EnqueueOptions::default().id_override("j".to_string()),
                ..EnqueueEntry::new(b"cancelled")
            },
        )
        .await
        .unwrap();
    sender(&store)
        .send(WORK, EnqueueEntry::new(b"kept"))
        .await
        .unwrap();
    sender(&store)
        .send_with_id(WORK, "a", CancelEntry { job_id: "j".into() })
        .await
        .unwrap();
    q.apply_inbox_now().await.unwrap();
    assert_eq!(
        q.view()
            .inbox_record(WORK, "a")
            .await
            .unwrap()
            .unwrap()
            .outcome,
        InboxOutcome::Cancel(CancelOutcome::Removed)
    );
    let jobs = q.claim_batch("work", 2, LEASE).await.unwrap();
    let payloads: Vec<&[u8]> = jobs.iter().map(|j| j.payload.as_slice()).collect();
    assert_eq!(payloads, [b"kept".as_slice()]);
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_enqueue_with_a_held_dedup_key_records_the_job_and_deletes_its_payload() {
    let store = make_store();
    let opts = OpenOptions {
        payload_offload_threshold: Some(64),
        ..inbox_opts(&MockClock::new(INITIAL))
    };
    let q = Queue::open_with_options(store.clone(), "test", opts)
        .await
        .unwrap();
    let options = || EnqueueOptions::default().dedup_key("once".to_string());
    let existing = q
        .enqueue_with("work", vec![1; 256], options())
        .await
        .unwrap();
    sender(&store)
        .send_with_id(
            WORK,
            "dup",
            EnqueueEntry {
                options: options(),
                ..EnqueueEntry::new(vec![2; 256])
            },
        )
        .await
        .unwrap();

    q.apply_inbox_now().await.unwrap();
    assert_eq!(
        q.view()
            .inbox_record(WORK, "dup")
            .await
            .unwrap()
            .unwrap()
            .outcome,
        InboxOutcome::Enqueue(EnqueueResult::AlreadyEnqueued(existing))
    );
    assert_eq!(object_count(&store, "test-payloads").await, 1);
    assert_eq!(q.view().stats("work").await.unwrap().pending, 1);
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_entry_that_cannot_apply_is_rejected_with_a_record() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    put_raw(&store, WORK, "enqueue", "bad-body", &[0xc1]).await;
    put_raw(
        &store,
        WORK,
        "enqueue",
        "bad-shape",
        &map_body("payload", 1),
    )
    .await;
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
                options: EnqueueOptions::default().id_override("a:b".to_string()),
                ..EnqueueEntry::new(b"x")
            },
        )
        .await
        .unwrap();
    let body = InboxEntry::from(EnqueueEntry::new(b"x")).encode();
    for name in ["no-order", "not-a-ulid.bad-order"] {
        let path = Path::from(format!("test-inbox/v1/work/enqueue/{name}"));
        store.put(&path, body.clone().into()).await.unwrap();
    }

    q.apply_inbox_now().await.unwrap();
    let outcome = |queue: &'static str, id: &'static str| {
        let view = q.view();
        async move { view.inbox_record(queue, id).await.unwrap().unwrap().outcome }
    };
    let invalid_id = Rejection::InvalidId {
        reason: FormError::InvalidCharacter { character: ':' },
    };
    for (queue, id, rejection) in [
        (WORK, "no-order", Rejection::Unordered),
        (WORK, "bad-order", Rejection::Unordered),
        (WORK, "unknown-kind", Rejection::UnknownKind),
        ("other", "other-queue", Rejection::QueueNotAccepted),
        ("other", "other-custom", Rejection::QueueNotAccepted),
        (WORK, "bad-id", invalid_id),
    ] {
        assert_eq!(
            outcome(queue, id).await,
            InboxOutcome::Rejected(rejection),
            "{id}"
        );
    }
    // The detail of a body is the text of the decoder.
    for id in ["bad-body", "bad-shape"] {
        assert!(matches!(
            outcome(WORK, id).await,
            InboxOutcome::Rejected(Rejection::Malformed { .. })
        ));
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
async fn an_entry_applies_before_an_object_name_of_its_id_without_a_ulid() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    let body = InboxEntry::from(EnqueueEntry::new(b"unordered")).encode();
    store
        .put(&Path::from("test-inbox/v1/work/enqueue/x"), body.into())
        .await
        .unwrap();
    sender(&store)
        .send_with_id(WORK, "x", EnqueueEntry::new(b"ordered"))
        .await
        .unwrap();

    q.apply_inbox_now().await.unwrap();
    let job = q.claim("work", LEASE).await.unwrap().unwrap();
    assert_eq!(job.payload, b"ordered");
    assert_eq!(
        q.view()
            .inbox_record(WORK, "x")
            .await
            .unwrap()
            .unwrap()
            .outcome,
        InboxOutcome::Enqueue(EnqueueResult::New(job.id.clone()))
    );
    assert_eq!(object_count(&store, "test-inbox/v1/work/enqueue").await, 0);
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_object_outside_the_layout_is_left_in_place() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    for path in [
        "test-inbox/v2/work/enqueue/future",
        "test-inbox/v1/work/enqueue/nested/id",
        "test-inbox/v1/work/enqueue/01ARZ3NDEKTSV4RRFFQ69G5FAV.bad.id",
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

#[tokio::test(start_paused = true)]
async fn the_objects_of_a_batch_stay_until_its_commits_are_durable() {
    let faults = FaultStore::wrap();
    let queue_store: Arc<dyn ObjectStore> = faults.clone();
    let inbox_store = make_store();
    let clock = MockClock::new(INITIAL);
    let opts = || OpenOptions {
        inbox: Some(
            InboxOptions::new(["work"])
                .object_store(inbox_store.clone())
                .touch_poll_interval(None),
        ),
        ..inbox_opts(&clock)
    };
    let q = Queue::open_with_options(queue_store.clone(), "test", opts())
        .await
        .unwrap();
    let sender = sender(&inbox_store);
    for id in ["a", "b"] {
        sender
            .send_with_id(WORK, id, EnqueueEntry::new(b"x"))
            .await
            .unwrap();
    }

    // The commits of the pass do not await durability, and the flush of the
    // batch stalls on the unavailable store. The queue is dropped without a
    // close, which simulates a crash before the flush.
    faults.fail_puts(true);
    let stalled = tokio::time::timeout(Duration::from_secs(30), q.apply_inbox_now()).await;
    assert!(stalled.is_err());
    assert_eq!(
        object_count(&inbox_store, "test-inbox/v1/work/enqueue").await,
        2
    );
    drop(q);

    faults.fail_puts(false);
    let q = Queue::open_with_options(queue_store, "test", opts())
        .await
        .unwrap();
    assert_eq!(
        object_count(&inbox_store, "test-inbox/v1/work/enqueue").await,
        0
    );
    assert_eq!(q.view().stats("work").await.unwrap().pending, 2);
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_error_at_an_entry_ends_the_pass_after_the_entries_before_it() {
    let faults = FaultStore::wrap();
    let inbox_store: Arc<dyn ObjectStore> = faults.clone();
    let opts = OpenOptions {
        inbox: Some(
            InboxOptions::new(["work"])
                .object_store(inbox_store.clone())
                .touch_poll_interval(None),
        ),
        ..inbox_opts(&MockClock::new(INITIAL))
    };
    let q = Queue::open_with_options(make_store(), "test", opts)
        .await
        .unwrap();
    let sender = sender(&inbox_store);
    for id in ["a", "b", "c"] {
        sender
            .send_with_id(WORK, id, EnqueueEntry::new(id.as_bytes()))
            .await
            .unwrap();
    }

    faults.fail_reads_ending(Some(".b"));
    assert!(matches!(
        q.apply_inbox_now().await,
        Err(Error::InboxStore(_))
    ));
    assert!(q.view().inbox_record(WORK, "a").await.unwrap().is_some());
    assert!(q.view().inbox_record(WORK, "b").await.unwrap().is_none());
    assert_eq!(
        object_count(&inbox_store, "test-inbox/v1/work/enqueue").await,
        2
    );

    faults.fail_reads_ending(None);
    q.apply_inbox_now().await.unwrap();
    assert_eq!(q.view().stats("work").await.unwrap().pending, 3);
    assert_eq!(
        object_count(&inbox_store, "test-inbox/v1/work/enqueue").await,
        0
    );
    q.close().await.unwrap();
}

#[tokio::test]
async fn passes_run_one_at_a_time() {
    let faults = FaultStore::wrap();
    let inbox_store: Arc<dyn ObjectStore> = faults.clone();
    let opts = OpenOptions {
        inbox: Some(InboxOptions::new(["work"]).object_store(inbox_store.clone())),
        ..inbox_opts(&MockClock::new(INITIAL))
    };
    let q = Queue::open_with_options(make_store(), "test", opts)
        .await
        .unwrap();
    sender(&inbox_store)
        .send_with_id(WORK, "a", EnqueueEntry::new(b"x"))
        .await
        .unwrap();

    // The listing of the first pass is waiting for the release, and the second
    // pass starts while it is waiting.
    let opened = faults.lists();
    faults.hold_next_list();
    let (lists_at_first_end, second, ()) = tokio::join!(
        async {
            q.apply_inbox_now().await.unwrap();
            faults.lists()
        },
        q.apply_inbox_now(),
        async { faults.release_list() },
    );
    second.unwrap();
    assert_eq!(lists_at_first_end, opened + 1);
    assert_eq!(faults.lists(), opened + 2);
    q.close().await.unwrap();
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
    let cancel = q.view().inbox_record(WORK, "a").await.unwrap().unwrap();
    assert!(matches!(
        cancel.outcome,
        InboxOutcome::Rejected(Rejection::Failed { .. })
    ));
    assert!(q.view().inbox_record(WORK, "b").await.unwrap().is_some());
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
async fn the_inbox_reports_its_allow_list_and_record_retention() {
    let q = open(&make_store(), &MockClock::new(INITIAL)).await;
    assert!(q.inbox_accepts(WORK));
    assert!(!q.inbox_accepts("other"));
    assert_eq!(
        q.inbox_record_retention(),
        Some(DEFAULT_INBOX_RECORD_RETENTION)
    );
    q.close().await.unwrap();
    let without_inbox = OpenOptions {
        inbox: None,
        ..inbox_opts(&MockClock::new(INITIAL))
    };
    let q = Queue::open_with_options(make_store(), "test", without_inbox)
        .await
        .unwrap();
    assert!(!q.inbox_accepts(WORK));
    assert_eq!(q.inbox_record_retention(), None);
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
async fn the_pending_entries_of_a_queue_are_listed_in_pass_order_without_a_read() {
    let faults = FaultStore::wrap();
    let store: Arc<dyn ObjectStore> = faults.clone();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    let sender = sender(&store);
    let enqueue = EnqueueEntry::new(b"x");
    sender
        .send_with_id(WORK, "b-entry", enqueue.clone())
        .await
        .unwrap();
    sender
        .send_with_id(WORK, "a-entry", CancelEntry { job_id: "j".into() })
        .await
        .unwrap();
    sender
        .send_with_id(WAITS, "w-entry", enqueue.clone())
        .await
        .unwrap();
    store
        .put(
            &Path::from("test-inbox/v1/work/enqueue/c-entry"),
            PutPayload::from(b"x".to_vec()),
        )
        .await
        .unwrap();

    faults.fail_reads_ending(Some("-entry"));
    let pending = sender.pending(WORK).await.unwrap();
    faults.fail_reads_ending(None);
    let listed: Vec<_> = pending
        .iter()
        .map(|p| (p.queue().as_str(), p.id(), p.kind(), p.size()))
        .collect();
    let enqueue_size = InboxEntry::from(enqueue.clone()).encode().len() as u64;
    let cancel_size = InboxEntry::from(CancelEntry { job_id: "j".into() })
        .encode()
        .len() as u64;
    assert_eq!(
        listed,
        [
            (WORK, "b-entry", &EntryKind::Enqueue, enqueue_size),
            (WORK, "a-entry", &EntryKind::Cancel, cancel_size),
            (WORK, "c-entry", &EntryKind::Enqueue, 1),
        ]
    );
    let written = store
        .list_with_delimiter(Some(&Path::from("test-inbox/v1/work/enqueue")))
        .await
        .unwrap()
        .objects
        .into_iter()
        .find(|object| object.location.as_ref().ends_with(".b-entry"))
        .unwrap()
        .last_modified;
    assert_eq!(pending[0].sent_at_ms() as i64, written.timestamp_millis());

    // The pass deletes the settled entries, and an entry sent again with the id
    // of an applied one is listed until the next pass deletes it.
    q.apply_inbox_now().await.unwrap();
    assert!(sender.pending(WORK).await.unwrap().is_empty());
    sender.send_with_id(WORK, "b-entry", enqueue).await.unwrap();
    assert_eq!(sender.pending(WORK).await.unwrap().len(), 1);
    assert!(
        q.view()
            .inbox_record(WORK, "b-entry")
            .await
            .unwrap()
            .is_some()
    );
    q.close().await.unwrap();
}

#[tokio::test]
async fn a_read_of_a_pending_entry_decodes_it_or_states_its_rejection() {
    let store = make_store();
    let sender = sender(&store);
    sender
        .send_with_id(WORK, "entry", EnqueueEntry::new(b"x"))
        .await
        .unwrap();
    sender
        .send_with_id(WORK, "deleted", EnqueueEntry::new(b"x"))
        .await
        .unwrap();
    put_raw(&store, WORK, "cancel", "malformed", &map_body("job", 1)).await;
    put_raw(&store, WORK, "unknown", "unknown", b"x").await;
    store
        .put(
            &Path::from("test-inbox/v1/work/enqueue/unordered"),
            PutPayload::from(b"x".to_vec()),
        )
        .await
        .unwrap();
    let pending = sender.pending(WORK).await.unwrap();
    let entry = |id: &str| pending.iter().find(|entry| entry.id() == id).unwrap();
    store.delete(entry("deleted").location()).await.unwrap();

    assert_eq!(
        sender.read(entry("entry")).await.unwrap(),
        EntryRead::Entry(InboxEntry::Enqueue(EnqueueEntry::new(b"x")))
    );
    assert_eq!(
        sender.read(entry("deleted")).await.unwrap(),
        EntryRead::Missing
    );
    assert!(matches!(
        sender.read(entry("malformed")).await.unwrap(),
        EntryRead::Rejected(Rejection::Malformed { .. })
    ));
    assert_eq!(
        sender.read(entry("unknown")).await.unwrap(),
        EntryRead::Rejected(Rejection::UnknownKind)
    );
    assert_eq!(
        sender.read(entry("unordered")).await.unwrap(),
        EntryRead::Rejected(Rejection::Unordered)
    );
}

#[tokio::test]
async fn the_cancel_kind_removes_a_queued_job_and_records_the_other_outcomes() {
    let store = make_store();
    let q = open(&store, &MockClock::new(INITIAL)).await;
    let queued = q.enqueue("work", b"a".to_vec()).await.unwrap();
    let claimed = q.enqueue("work", b"b".to_vec()).await.unwrap();
    let ClaimOutcome::Claimed(claim) = q.claim_by_id(&claimed, LEASE).await.unwrap() else {
        panic!("the job is pending");
    };
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
        async move {
            q.view()
                .inbox_record(WORK, &id)
                .await
                .unwrap()
                .unwrap()
                .outcome
        }
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
    assert!(claim.cancel_token().is_cancelled());
    assert_eq!(
        outcome("missing").await,
        InboxOutcome::Cancel(CancelOutcome::NotFound)
    );
    // The job of another queue than the entry's is out of reach.
    assert_eq!(
        outcome("other").await,
        InboxOutcome::Rejected(Rejection::OtherQueue {
            job_queue: "other".to_string()
        })
    );
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
    let outcome = |queue: &'static str, id: &'static str| {
        let view = q.view();
        async move { view.inbox_record(queue, id).await.unwrap().unwrap().outcome }
    };
    // The signal for another queue than the waiter's is rejected.
    assert_eq!(
        outcome(WORK, "from-work").await,
        InboxOutcome::Rejected(Rejection::OtherQueue {
            job_queue: WAITS.to_string()
        })
    );
    assert_eq!(
        outcome(WAITS, "to-waiter").await,
        InboxOutcome::Signal(SignalOutcome::Delivered)
    );
    assert_eq!(
        outcome(WAITS, "to-nobody").await,
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
    assert!(q.view().inbox_record(WORK, "once").await.unwrap().is_some());

    clock.advance(Duration::from_millis(1));
    tokio::time::sleep(reaper_interval * 2).await;
    assert_eq!(q.view().inbox_record(WORK, "once").await.unwrap(), None);
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
        async move { view.inbox_record(WORK, id).await.unwrap().is_some() }
    };

    sender(&store)
        .send_with_id(WORK, "touched", EnqueueEntry::new(b"x"))
        .await
        .unwrap();
    tokio::time::sleep(DEFAULT_INBOX_TOUCH_POLL_INTERVAL * 2).await;
    assert!(applied("touched").await);

    let body = InboxEntry::from(EnqueueEntry::new(b"y")).encode();
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

    // The pass after the touch fails at the read of the entry.
    faults.fail_reads_ending(Some(".a"));
    sender(&inbox_store)
        .send_with_id(WORK, "a", EnqueueEntry::new(b"x"))
        .await
        .unwrap();
    tokio::time::sleep(DEFAULT_INBOX_TOUCH_POLL_INTERVAL * 2).await;
    assert!(q.view().inbox_record(WORK, "a").await.unwrap().is_none());

    faults.fail_reads_ending(None);
    tokio::time::sleep(DEFAULT_INBOX_TOUCH_POLL_INTERVAL * 2).await;
    assert!(q.view().inbox_record(WORK, "a").await.unwrap().is_some());
    assert_eq!(
        object_count(&inbox_store, "test-inbox/v1/work/enqueue").await,
        0
    );
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_entry_above_the_size_bound_is_rejected_without_a_read() {
    let faults = FaultStore::wrap();
    let inbox_store: Arc<dyn ObjectStore> = faults.clone();
    let opts = OpenOptions {
        inbox: Some(
            InboxOptions::new(["work"])
                .object_store(inbox_store.clone())
                .touch_poll_interval(None),
        ),
        ..inbox_opts(&MockClock::new(INITIAL))
    };
    let q = Queue::open_with_options(make_store(), "test", opts)
        .await
        .unwrap();
    let size = MAX_INBOX_ENTRY_SIZE + 1;
    put_raw(&inbox_store, WORK, "enqueue", "big", &vec![0; size]).await;

    // A read of the entry fails the pass.
    faults.fail_reads_ending(Some(".big"));
    q.apply_inbox_now().await.unwrap();
    assert_eq!(
        q.view()
            .inbox_record(WORK, "big")
            .await
            .unwrap()
            .unwrap()
            .outcome,
        InboxOutcome::Rejected(Rejection::TooLarge { size: size as u64 })
    );
    assert_eq!(
        object_count(&inbox_store, "test-inbox/v1/work/enqueue").await,
        0
    );
    q.close().await.unwrap();
}

#[tokio::test]
async fn a_read_rejects_an_object_above_the_size_bound_before_its_body() {
    let store = make_store();
    let location = Path::from("test-inbox/v1/work/enqueue/big");
    let size = MAX_INBOX_ENTRY_SIZE + 1;
    store
        .put(&location, PutPayload::from(vec![0; size]))
        .await
        .unwrap();

    let read = read_object(&*store, &location).await;
    assert!(matches!(
        read,
        Read::Rejected(Rejection::TooLarge { size: read_size }) if read_size == size as u64
    ));
}

#[tokio::test]
async fn an_unreadable_entry_is_rejected_after_the_bound_and_the_pass_continues() {
    let faults = FaultStore::wrap();
    let inbox_store: Arc<dyn ObjectStore> = faults.clone();
    let clock = MockClock::new(INITIAL);
    let opts = OpenOptions {
        inbox: Some(
            InboxOptions::new(["work"])
                .object_store(inbox_store.clone())
                .touch_poll_interval(None),
        ),
        ..inbox_opts(&clock)
    };
    let q = Queue::open_with_options(make_store(), "test", opts)
        .await
        .unwrap();
    let sender = sender(&inbox_store);
    for id in ["a", "b"] {
        sender
            .send_with_id(WORK, id, EnqueueEntry::new(id.as_bytes()))
            .await
            .unwrap();
    }
    let failure = inbox_failure_key(&QueueName::new(WORK).unwrap(), "a");

    // The first failed read ends the pass and commits its time.
    faults.fail_reads_ending(Some(".a"));
    assert!(q.apply_inbox_now().await.is_err());
    assert!(q.core.db.get(&failure).await.unwrap().is_some());
    clock.advance(INBOX_UNREADABLE_AFTER - Duration::from_millis(1));
    assert!(q.apply_inbox_now().await.is_err());
    assert!(q.view().inbox_record(WORK, "b").await.unwrap().is_none());

    clock.advance(Duration::from_millis(1));
    q.apply_inbox_now().await.unwrap();
    assert!(matches!(
        q.view()
            .inbox_record(WORK, "a")
            .await
            .unwrap()
            .unwrap()
            .outcome,
        InboxOutcome::Rejected(Rejection::Unreadable { .. })
    ));
    assert!(q.view().inbox_record(WORK, "b").await.unwrap().is_some());
    assert!(q.core.db.get(&failure).await.unwrap().is_none());
    assert_eq!(
        object_count(&inbox_store, "test-inbox/v1/work/enqueue").await,
        0
    );
    q.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_delete_leaves_the_object_to_a_later_pass() {
    let faults = FaultStore::wrap();
    let inbox_store: Arc<dyn ObjectStore> = faults.clone();
    let opts = OpenOptions {
        inbox: Some(
            InboxOptions::new(["work"])
                .object_store(inbox_store.clone())
                .touch_poll_interval(None),
        ),
        ..inbox_opts(&MockClock::new(INITIAL))
    };
    let q = Queue::open_with_options(make_store(), "test", opts)
        .await
        .unwrap();
    sender(&inbox_store)
        .send_with_id(WORK, "a", EnqueueEntry::new(b"x"))
        .await
        .unwrap();

    faults.fail_deletes(true);
    q.apply_inbox_now().await.unwrap();
    assert!(q.view().inbox_record(WORK, "a").await.unwrap().is_some());
    assert_eq!(
        object_count(&inbox_store, "test-inbox/v1/work/enqueue").await,
        1
    );

    faults.fail_deletes(false);
    q.apply_inbox_now().await.unwrap();
    assert_eq!(
        object_count(&inbox_store, "test-inbox/v1/work/enqueue").await,
        0
    );
    assert_eq!(q.view().stats("work").await.unwrap().pending, 1);
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_entry_whose_object_is_missing_at_its_read_is_skipped() {
    let faults = FaultStore::wrap();
    let inbox_store: Arc<dyn ObjectStore> = faults.clone();
    let opts = OpenOptions {
        inbox: Some(
            InboxOptions::new(["work"])
                .object_store(inbox_store.clone())
                .touch_poll_interval(None),
        ),
        ..inbox_opts(&MockClock::new(INITIAL))
    };
    let q = Queue::open_with_options(make_store(), "test", opts)
        .await
        .unwrap();
    let sender = sender(&inbox_store);
    for id in ["a", "b"] {
        sender
            .send_with_id(WORK, id, EnqueueEntry::new(id.as_bytes()))
            .await
            .unwrap();
    }

    // The listing of the pass contains both objects, and the object of `a` is
    // deleted while the listing is held.
    faults.hold_next_list();
    let (passed, ()) = tokio::join!(q.apply_inbox_now(), async {
        let listed = inbox_store
            .list_with_delimiter(Some(&Path::from("test-inbox/v1/work/enqueue")))
            .await
            .unwrap();
        let a = listed
            .objects
            .iter()
            .find(|object| object.location.as_ref().ends_with(".a"))
            .unwrap();
        inbox_store.delete(&a.location).await.unwrap();
        faults.release_list();
    });
    passed.unwrap();
    assert!(q.view().inbox_record(WORK, "a").await.unwrap().is_none());
    assert!(q.view().inbox_record(WORK, "b").await.unwrap().is_some());
    let job = q.claim("work", LEASE).await.unwrap().unwrap();
    assert_eq!(job.payload, b"b");
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

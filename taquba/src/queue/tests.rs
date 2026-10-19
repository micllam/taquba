use super::*;
use crate::EnqueueRequest;
use crate::expiry::{Expired, ExpiryIndex};
use crate::keys::MAX_QUEUE_NAME_LEN;
use crate::kv::MAX_KV_VALUE_SIZE;
use crate::options::{PRIORITY_HIGH, PRIORITY_LOW, PRIORITY_NORMAL};
use crate::test_util::*;

#[tokio::test]
async fn clock_accessor_returns_the_configured_clock() {
    let clock = MockClock::new(1_700_000_000_000);
    let opts = OpenOptions {
        clock: Arc::new(clock.clone()),
        ..OpenOptions::default()
    };
    let q = Queue::open_with_options(make_store(), "test", opts)
        .await
        .unwrap();

    assert_eq!(q.clock().now_ms(), 1_700_000_000_000);
    clock.advance(Duration::from_secs(60));
    assert_eq!(q.clock().now_ms(), 1_700_000_060_000);

    q.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_fence_ends_the_background_tasks() {
    let store = make_store();
    let opts = OpenOptions {
        liveness_heartbeat: Some(Duration::from_secs(1)),
        ..OpenOptions::default()
    };
    let first = Queue::open_with_options(store.clone(), "test", opts)
        .await
        .unwrap();

    let second = Queue::open(store, "test").await.unwrap();
    assert!(first.enqueue("work", b"late".to_vec()).await.is_err());
    first.store_closed().await;
    // Paused time advances only once every task is idle, so a task that ends at
    // the close has ended when the sleep ends.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(first.reaper_task.is_finished());
    assert!(first.scheduler_task.is_finished());
    assert!(first.heartbeat.as_ref().unwrap().is_finished());
    assert!(first.close_watcher.is_finished());
    second.close().await.unwrap();
}

#[tokio::test]
async fn a_claim_on_a_fenced_empty_queue_fails() {
    let store = make_store();
    let first = Queue::open(store.clone(), "test").await.unwrap();
    // The empty claim records the queue as known empty.
    assert!(
        first
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );

    let second = Queue::open(store, "test").await.unwrap();
    assert!(first.enqueue("other", b"late".to_vec()).await.is_err());
    first.store_closed().await;
    assert!(first.claim("work", Duration::from_secs(30)).await.is_err());
    second.close().await.unwrap();
}

#[tokio::test]
async fn test_enqueue_and_claim() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    let id = q.enqueue("email", b"hello".to_vec()).await.unwrap();
    let job = q
        .claim("email", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(job.id, id);
    Ulid::from_string(&id).expect("a generated id is a ULID");
    assert_eq!(job.queue, "email");
    assert_eq!(job.payload, b"hello");
    assert_eq!(job.status, JobStatus::Claimed);
    assert_eq!(job.attempts, 1);
    assert!(job.claimed_at.is_some());
    assert!(q.lease_expiry("email", &job.id).is_some());

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_enqueue_with_id_override_uses_supplied_id() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    let returned = q
        .enqueue_with(
            "email",
            b"hello".to_vec(),
            EnqueueOptions {
                id_override: Some("user-42-welcome".to_string()),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(returned, "user-42-welcome");

    let job = q
        .claim("email", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.id, "user-42-welcome");

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_enqueue_with_invalid_id_override_rejected() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    let bad_ids: &[(&str, &str)] = &[
        ("", "empty"),
        ("has:colon", "delimiter"),
        ("has space", "space"),
        ("has/slash", "slash"),
    ];
    for (bad, label) in bad_ids {
        let err = q
            .enqueue_with(
                "email",
                b"x".to_vec(),
                EnqueueOptions {
                    id_override: Some((*bad).to_string()),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidId { .. }),
            "expected InvalidId for {label} (id={bad:?}), got {err:?}"
        );
    }

    let too_long = "a".repeat(MAX_ID_OVERRIDE_LEN + 1);
    let err = q
        .enqueue_with(
            "email",
            b"x".to_vec(),
            EnqueueOptions {
                id_override: Some(too_long),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidId { .. }));

    // No job is written for any of the rejected ids.
    assert!(
        q.claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_enqueue_with_duplicate_id_override_rejected() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    let id = q
        .enqueue_with(
            "email",
            b"first".to_vec(),
            EnqueueOptions {
                id_override: Some("duplicate-id".to_string()),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(id, "duplicate-id");

    let err = q
        .enqueue_with(
            "email",
            b"second".to_vec(),
            EnqueueOptions {
                id_override: Some("duplicate-id".to_string()),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DuplicateJobId { id } if id == "duplicate-id"));

    let job = q
        .claim("email", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.id, "duplicate-id");
    assert_eq!(job.payload, b"first");
    assert!(
        q.claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );

    q.close().await.unwrap();
}

#[tokio::test]
async fn enqueue_with_effects_with_a_duplicate_id_override_applies_no_effects() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    q.enqueue_with(
        "email",
        b"first".to_vec(),
        EnqueueOptions {
            id_override: Some("duplicate-kv-id".to_string()),
            ..EnqueueOptions::default()
        },
    )
    .await
    .unwrap();

    let err = q
        .enqueue_with_effects(
            "email",
            b"second".to_vec(),
            EnqueueOptions {
                id_override: Some("duplicate-kv-id".to_string()),
                ..EnqueueOptions::default()
            },
            SettlementEffects::default().kv_put(b"meta/duplicate", b"written"),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DuplicateJobId { id } if id == "duplicate-kv-id"));
    assert!(q.view().kv_get(b"meta/duplicate").await.unwrap().is_none());

    let job = q
        .claim("email", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.id, "duplicate-kv-id");
    assert_eq!(job.payload, b"first");

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_nack_requeues_job() {
    let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
        .await
        .unwrap();

    q.enqueue_with(
        "email",
        b"hello".to_vec(),
        EnqueueOptions {
            max_attempts: Some(3),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let job = q
        .claim("email", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.attempts, 1);

    q.nack(&job, "transient error").await.unwrap();

    let retried = q
        .claim("email", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retried.attempts, 2);
    assert_eq!(retried.last_error.as_deref(), Some("transient error"));
    assert_eq!(retried.status, JobStatus::Claimed);

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_fifo_ordering() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    let id_a = q.enqueue("work", b"first".to_vec()).await.unwrap();
    let id_b = q.enqueue("work", b"second".to_vec()).await.unwrap();
    let id_c = q.enqueue("work", b"third".to_vec()).await.unwrap();

    let j1 = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let j2 = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let j3 = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(j1.id, id_a);
    assert_eq!(j2.id, id_b);
    assert_eq!(j3.id, id_c);

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_queue_isolation() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    let id_email = q.enqueue("email", b"email job".to_vec()).await.unwrap();
    let id_resize = q.enqueue("resize", b"resize job".to_vec()).await.unwrap();

    let email_job = q
        .claim("email", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let resize_job = q
        .claim("resize", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(email_job.id, id_email);
    assert_eq!(resize_job.id, id_resize);
    assert!(
        q.claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        q.claim("resize", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_stats_track_job_lifecycle() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    q.enqueue("email", b"a".to_vec()).await.unwrap();
    q.enqueue("email", b"b".to_vec()).await.unwrap();

    let s = q.view().stats("email").await.unwrap();
    assert_eq!(s.pending, 2);
    assert_eq!(s.claimed, 0);

    let job = q
        .claim("email", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let s = q.view().stats("email").await.unwrap();
    assert_eq!(s.pending, 1);
    assert_eq!(s.claimed, 1);

    q.ack(&job).await.unwrap();
    let s = q.view().stats("email").await.unwrap();
    assert_eq!(s.pending, 1);
    assert_eq!(s.claimed, 0);
    assert_eq!(s.done, 1);

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_stats_nack_dead_letter() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    q.enqueue_with(
        "email",
        b"x".to_vec(),
        EnqueueOptions {
            max_attempts: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let job = q
        .claim("email", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    q.nack(&job, "fail").await.unwrap();

    let s = q.view().stats("email").await.unwrap();
    assert_eq!(s.pending, 0);
    assert_eq!(s.claimed, 0);
    assert_eq!(s.dead, 1);

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_dead_jobs_and_requeue() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    let id = q
        .enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                max_attempts: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let job = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    q.nack(&job, "fatal").await.unwrap();

    let dead = q.view().dead_jobs("work", None, 100).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].id, id);
    assert_eq!(dead[0].status, JobStatus::Dead);
    assert!(dead[0].failed_at.is_some());

    // A requeued dead job is claimable again.
    q.requeue_dead_job(&dead[0].id).await.unwrap();

    let revived = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(revived.id, id);
    assert_eq!(revived.attempts, 1); // fresh attempt after reset
    assert!(revived.last_error.is_none());
    assert!(
        revived.failed_at.is_none(),
        "requeue must clear failed_at so a re-fail starts a fresh retention window"
    );

    q.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_revival_is_durable_before_the_call_returns() {
    // A flush interval longer than the test, so a write is durable only when
    // its commit awaits the flush.
    let opts = || OpenOptions {
        flush_interval: Some(Duration::from_secs(3600)),
        ..OpenOptions::default()
    };
    let store = make_store();
    let q = Queue::open_with_options(store.clone(), "test", opts())
        .await
        .unwrap();
    let id = q
        .enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                max_attempts: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let claim = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    q.nack(&claim, "fatal").await.unwrap();
    let dead = q
        .view()
        .dead_jobs("work", None, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    q.requeue_dead_job(&dead.id).await.unwrap();

    // A crash inside the flush window: closing without a flush discards every
    // write that is not durable.
    q.core
        .db
        .close_with_options(slatedb::config::CloseOptions::default().with_flush_type(None))
        .await
        .unwrap();
    drop(q);

    let q = Queue::open_with_options(store, "test", opts())
        .await
        .unwrap();
    let job = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Pending);
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_over_length_queue_name_is_rejected_at_enqueue_and_claim() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    let over = "q".repeat(MAX_QUEUE_NAME_LEN + 1);
    assert!(matches!(
        q.enqueue(&over, b"payload".to_vec()).await,
        Err(Error::InvalidQueueName { .. })
    ));
    assert!(matches!(
        q.claim(&over, Duration::from_secs(30)).await,
        Err(Error::InvalidQueueName { .. })
    ));
    q.close().await.unwrap();
}

#[tokio::test]
async fn open_rejects_a_queue_config_for_an_unencodable_name() {
    let mut opts = OpenOptions::default();
    opts.queue_configs
        .insert("q".repeat(MAX_QUEUE_NAME_LEN + 1), QueueConfig::default());
    assert!(matches!(
        Queue::open_with_options(make_store(), "test", opts).await,
        Err(Error::InvalidQueueName { .. })
    ));
}

#[tokio::test]
async fn test_per_queue_config() {
    let initial = 1_700_000_000_000u64;
    let mut opts = OpenOptions {
        clock: Arc::new(MockClock::new(initial)),
        ..OpenOptions::default()
    };
    opts.queue_configs.insert(
        "fast".to_string(),
        QueueConfig {
            max_attempts: 1,
            lease_duration: Duration::from_secs(5),
            ..QueueConfig::default()
        },
    );
    let q = Queue::open_with_options(make_store(), "test", opts)
        .await
        .unwrap();

    // "fast" queue inherits max_attempts=1
    q.enqueue("fast", b"x".to_vec()).await.unwrap();
    let job = q.claim_next("fast").await.unwrap().unwrap();
    assert_eq!(job.max_attempts, 1);
    // Lease is 5s
    assert_eq!(q.lease_expiry("fast", &job.id), Some(initial + 5_000));
    assert_eq!(job.claimed_at, Some(initial));

    q.ack(&job).await.unwrap();
    q.close().await.unwrap();
}

#[tokio::test]
async fn test_priority_ordering() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    // Enqueue in reverse priority order, so insertion order and priority order
    // differ.
    let id_low = q
        .enqueue_with(
            "jobs",
            b"low".to_vec(),
            EnqueueOptions {
                priority: Some(PRIORITY_LOW),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let id_normal = q
        .enqueue_with(
            "jobs",
            b"normal".to_vec(),
            EnqueueOptions {
                priority: Some(PRIORITY_NORMAL),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let id_high = q
        .enqueue_with(
            "jobs",
            b"high".to_vec(),
            EnqueueOptions {
                priority: Some(PRIORITY_HIGH),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let j1 = q
        .claim("jobs", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let j2 = q
        .claim("jobs", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let j3 = q
        .claim("jobs", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(j1.id, id_high);
    assert_eq!(j2.id, id_normal);
    assert_eq!(j3.id, id_low);

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_ids_increase_within_one_millisecond() {
    // A clock that never advances puts every id in one millisecond, which is
    // the case a non-monotonic id source orders arbitrarily.
    let clock = MockClock::new(1_700_000_000_000);
    let q = Queue::open_with_options(
        make_store(),
        "test",
        OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        },
    )
    .await
    .unwrap();

    let ids: Vec<String> = (0..10).map(|_| q.next_job_id()).collect();

    // The first ten characters of a ULID encode its millisecond timestamp.
    assert!(
        ids.iter().all(|id| id[..10] == ids[0][..10]),
        "every id must carry the frozen clock's millisecond"
    );
    assert!(
        ids.windows(2).all(|w| w[0] < w[1]),
        "ids must increase with generation order"
    );

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_fifo_holds_across_a_claim_batch() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    let mut enqueued = Vec::new();
    for i in 0..20 {
        enqueued.push(
            q.enqueue("jobs", format!("job-{i}").into_bytes())
                .await
                .unwrap(),
        );
    }

    let claimed = q
        .claim_batch("jobs", 20, Duration::from_secs(30))
        .await
        .unwrap();
    let claimed_ids: Vec<String> = claimed.into_iter().map(|c| c.into_job().id).collect();
    assert_eq!(claimed_ids, enqueued);

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_priority_preserved_after_nack() {
    let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
        .await
        .unwrap();

    // A high-priority job that is nacked still comes back before a normal job.
    let id_high = q
        .enqueue_with(
            "jobs",
            b"high".to_vec(),
            EnqueueOptions {
                priority: Some(PRIORITY_HIGH),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let _id_normal = q
        .enqueue_with(
            "jobs",
            b"normal".to_vec(),
            EnqueueOptions {
                priority: Some(PRIORITY_NORMAL),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let job = q
        .claim("jobs", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.id, id_high);

    q.nack(&job, "retry me").await.unwrap();

    // The high-priority job is claimed again before the normal one.
    let reclaimed = q
        .claim("jobs", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed.id, id_high);
    assert_eq!(reclaimed.priority, PRIORITY_HIGH);

    q.close().await.unwrap();
}

#[tokio::test]
async fn a_separate_wal_store_holds_the_wal_and_replays_at_open() {
    let store = make_store();
    let wal_store = make_store();
    let opts = || OpenOptions::default().wal_object_store(wal_store.clone());
    let q = Queue::open_with_options(store.clone(), "test", opts())
        .await
        .unwrap();
    let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
    // The durable enqueue flushed the WAL, so its objects exist and only the
    // configured WAL store contains them.
    assert!(object_count(&wal_store, "test/wal").await > 0);
    assert_eq!(object_count(&store, "test/wal").await, 0);
    drop(q);

    // The enqueue was not flushed to the primary store, so recovery at open
    // replays it from the WAL store.
    let q = Queue::open_with_options(store, "test", opts())
        .await
        .unwrap();
    let job = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Pending);
    q.close().await.unwrap();
}

#[tokio::test]
async fn a_queue_without_the_in_process_compactor_leaves_the_compactor_epoch_at_zero() {
    let store = make_store();
    let opts = || {
        OpenOptions::default()
            .in_process_compactor(false)
            .in_process_garbage_collector(false)
    };
    let q = Queue::open_with_options(store.clone(), "test", opts())
        .await
        .unwrap();
    let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
    q.close().await.unwrap();

    // An in-process compactor fences the manifest at open, which raises the
    // compactor epoch, and the writer reads the latest manifest at the next
    // open.
    let q = Queue::open_with_options(store.clone(), "test", opts())
        .await
        .unwrap();
    assert_eq!(q.core.db.manifest().compactor_epoch(), 0);
    let job = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Pending);
    q.close().await.unwrap();

    let q = Queue::open(store.clone(), "test").await.unwrap();
    q.close().await.unwrap();
    let q = Queue::open_with_options(store, "test", opts())
        .await
        .unwrap();
    assert_eq!(q.core.db.manifest().compactor_epoch(), 1);
    q.close().await.unwrap();
}

#[tokio::test]
async fn test_dead_letter_skips_attempts_check() {
    // dead_letter() moves a job claimed -> dead unconditionally,
    // without bumping attempts or honouring max_attempts.
    let q = Queue::open_with_options(
        make_store(),
        "test",
        OpenOptions {
            queue_configs: HashMap::from([(
                "work".to_string(),
                QueueConfig {
                    max_attempts: 5,
                    ..QueueConfig::default()
                },
            )]),
            ..OpenOptions::default()
        },
    )
    .await
    .unwrap();

    let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
    let claimed = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.attempts, 1);

    q.dead_letter(&claimed, "permanent failure").await.unwrap();

    let job = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Dead);
    assert_eq!(job.attempts, 1, "attempts should not be incremented");
    assert_eq!(job.last_error.as_deref(), Some("permanent failure"));
    assert!(job.failed_at.is_some());

    let stats = q.view().stats("work").await.unwrap();
    assert_eq!(stats.dead, 1);
    assert_eq!(stats.claimed, 0);
}

#[tokio::test]
async fn test_get_job_tracks_lifecycle() {
    // Opt in to keeping done jobs so get_job can resolve them after ack.
    let opts = OpenOptions {
        default_queue_config: QueueConfig {
            keep_done_jobs: Some(Duration::from_secs(60)),
            ..QueueConfig::default()
        },
        ..OpenOptions::default()
    };
    let q = Queue::open_with_options(make_store(), "test", opts)
        .await
        .unwrap();

    let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

    let job = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Pending);

    let claimed = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let job = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Claimed);

    q.ack(&claimed).await.unwrap();
    let job = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Done);

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_ack_deletes_job_by_default() {
    // Default config: ack drops the job entirely. The done counter still
    // increments, but the ID is no longer findable via get_job.
    let q = Queue::open(make_store(), "test").await.unwrap();

    let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
    let job = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    q.ack(&job).await.unwrap();

    assert!(
        q.view().get_job(&id).await.unwrap().is_none(),
        "ack must drop the index by default"
    );
    let s = q.view().stats("work").await.unwrap();
    assert_eq!(s.done, 1, "done counter still tracks throughput");
    assert_eq!(s.pending, 0);
    assert_eq!(s.claimed, 0);

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_get_job_returns_none_for_unknown_id() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    assert!(q.view().get_job("nonexistent").await.unwrap().is_none());
    q.close().await.unwrap();
}

#[tokio::test]
async fn cancelling_a_pending_job_removes_it_and_releases_its_dedup_key() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    let opts = || EnqueueOptions {
        dedup_key: Some("user-42".to_string()),
        ..Default::default()
    };

    let id = q
        .enqueue_with("work", b"payload".to_vec(), opts())
        .await
        .unwrap();

    assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);

    // No longer claimable.
    assert!(
        q.claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );

    // No longer findable by ID.
    assert!(q.view().get_job(&id).await.unwrap().is_none());

    // Stats reflect the removal.
    assert_eq!(q.view().stats("work").await.unwrap().pending, 0);

    // The dedup key is free for a new job.
    let again = q
        .enqueue_with("work", b"payload".to_vec(), opts())
        .await
        .unwrap();
    assert_ne!(again, id);

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_cancel_scheduled_job() {
    let initial = 1_700_000_000_000u64;
    let opts = OpenOptions {
        clock: Arc::new(MockClock::new(initial)),
        ..OpenOptions::default()
    };
    let q = Queue::open_with_options(make_store(), "test", opts)
        .await
        .unwrap();

    let id = q
        .enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                run_at: Some(std::time::UNIX_EPOCH + Duration::from_millis(initial + 3_600_000)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(q.view().stats("work").await.unwrap().scheduled, 1);
    assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
    assert_eq!(q.view().stats("work").await.unwrap().scheduled, 0);
    assert!(q.view().get_job(&id).await.unwrap().is_none());

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_cancel_claimed_job_fires_token() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    q.enqueue("work", b"payload".to_vec()).await.unwrap();
    let job = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    let token = job.cancel_token().clone();
    assert!(!token.is_cancelled());

    // Cooperative cancel: token fires, persisted flag is set.
    assert_eq!(q.cancel(&job.id).await.unwrap(), CancelOutcome::Requested);
    assert!(token.is_cancelled());

    // The worker can still ack normally, because cancellation is cooperative.
    q.ack(&job).await.unwrap();
    q.close().await.unwrap();
}

#[tokio::test]
async fn test_cancel_terminal_job_is_not_found() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
    let job = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    q.ack(&job).await.unwrap();
    // Once Done (or fully deleted on default ack), cancel is a no-op.
    assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::NotFound);

    q.close().await.unwrap();
}

#[tokio::test]
async fn a_cancel_requested_during_the_delivery_survives_a_nack() {
    let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
        .await
        .unwrap();

    let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
    let claim = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);

    q.nack(&claim, "transient").await.unwrap();

    let requeued = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(requeued.status, JobStatus::Pending);
    assert!(
        requeued.cancel_requested,
        "the nack must not overwrite the persisted cancel request",
    );

    let reclaim = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert!(reclaim.cancel_requested);
    assert!(
        reclaim.cancel_token().is_cancelled(),
        "the re-claim must surface a pre-cancelled token",
    );

    q.ack(&reclaim).await.unwrap();
    q.close().await.unwrap();
}

#[tokio::test]
async fn a_cancel_requested_during_the_delivery_survives_onto_a_kept_done_record() {
    let opts = OpenOptions {
        default_queue_config: QueueConfig {
            keep_done_jobs: Some(Duration::from_secs(60)),
            ..QueueConfig::default()
        },
        ..OpenOptions::default()
    };
    let q = Queue::open_with_options(make_store(), "test", opts)
        .await
        .unwrap();

    let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
    let claim = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);

    q.ack(&claim).await.unwrap();

    let done = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(done.status, JobStatus::Done);
    assert!(
        done.cancel_requested,
        "the done record is written from the stored record, not the claim's copy",
    );
    q.close().await.unwrap();
}

#[tokio::test]
async fn a_cancel_request_stays_on_a_dead_record_until_revival() {
    let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
        .await
        .unwrap();

    let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
    let claim = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);

    q.dead_letter(&claim, "permanent").await.unwrap();

    let dead = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(dead.status, JobStatus::Dead);
    assert!(dead.cancel_requested);

    q.requeue_dead_job(&id).await.unwrap();
    let revived = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert!(!revived.cancel_requested);
    assert!(!revived.cancel_token().is_cancelled());

    q.ack(&revived).await.unwrap();
    q.close().await.unwrap();
}

#[tokio::test]
async fn test_enqueue_batch_atomic() {
    let q = Queue::open(make_store(), "test").await.unwrap();

    let payloads = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
    let ids = q.enqueue_batch("work", payloads).await.unwrap();
    assert_eq!(ids.len(), 3);

    let s = q.view().stats("work").await.unwrap();
    assert_eq!(s.pending, 3);

    // All jobs are findable and ordered FIFO.
    let j1 = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let j2 = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let j3 = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(j1.id, ids[0]);
    assert_eq!(j2.id, ids[1]);
    assert_eq!(j3.id, ids[2]);

    q.close().await.unwrap();
}

#[tokio::test]
async fn test_enqueue_batch_empty_is_noop() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    let ids = q.enqueue_batch("work", vec![]).await.unwrap();
    assert!(ids.is_empty());
    assert_eq!(q.view().stats("work").await.unwrap().pending, 0);
    q.close().await.unwrap();
}

#[tokio::test]
async fn test_enqueue_unique_nack_then_reenqueue_does_not_corrupt_dedup() {
    let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
        .await
        .unwrap();

    let id1 = q
        .enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                dedup_key: Some("user-42".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Claim and nack the first job. Without a backoff it goes back to pending.
    let job = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    // After claim, dedup_key must be cleared on the record so a future claim
    // does not try to release the (now reused) index.
    assert!(job.dedup_key.is_none());
    q.nack(&job, "transient").await.unwrap();

    // A fresh enqueue_unique with the same key is accepted now (claim released
    // the index) and creates a different job.
    let id2 = q
        .enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                dedup_key: Some("user-42".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_ne!(id1, id2);

    // Drain both jobs. Both must complete, and the second job's dedup index
    // must remain intact while it sits in pending.
    let j1 = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    // While j1 is claimed (and can be the retry of id1), a third enqueue_unique
    // with the same key must STILL be blocked by id2's index entry.
    let id3 = q
        .enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                dedup_key: Some("user-42".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        id3, id2,
        "id2's dedup index must still block the third enqueue while id2 is pending"
    );
    q.ack(&j1).await.unwrap();

    let j2 = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    q.ack(&j2).await.unwrap();

    assert_eq!(q.view().stats("work").await.unwrap().pending, 0);
    q.close().await.unwrap();
}

#[tokio::test]
async fn test_backoff_delay_calculation() {
    let base = Duration::from_secs(1);
    let max = Duration::from_secs(60);

    assert_eq!(backoff_delay(1, base, max), Duration::from_secs(1));
    assert_eq!(backoff_delay(2, base, max), Duration::from_secs(2));
    assert_eq!(backoff_delay(3, base, max), Duration::from_secs(4));
    assert_eq!(backoff_delay(4, base, max), Duration::from_secs(8));
    // Caps at max.
    assert_eq!(backoff_delay(20, base, max), max);
    // Zero base: no backoff regardless of attempts.
    assert_eq!(
        backoff_delay(5, Duration::ZERO, Duration::from_secs(10)),
        Duration::ZERO
    );
}

#[tokio::test]
async fn enqueue_with_effects_applies_the_effects_with_a_new_job() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    let index = ExpiryIndex::new(b"expiry/".to_vec());
    q.kv_put(b"runs/old", b"stale").await.unwrap();
    // The bound of the index is 5_000 before the enqueue, so a pass reads the
    // entry at 1_000 only if the enqueue records it.
    q.commit_effects(SettlementEffects::default().expiry_entry(&index, 5_000, b"later"))
        .await
        .unwrap();

    let (outcome, results) = q
        .enqueue_with_effects(
            "work",
            b"payload".to_vec(),
            EnqueueOptions::default(),
            SettlementEffects::default()
                .enqueue(EnqueueRequest {
                    queue: "next".to_string(),
                    payload: b"follow-up".to_vec(),
                    options: EnqueueOptions::default(),
                })
                .kv_put(b"runs/abc", b"submitted")
                .kv_delete(b"runs/old")
                .expiry_entry(&index, 1_000, b"abc"),
        )
        .await
        .unwrap();
    let id = match outcome {
        EnqueueResult::New(id) => id,
        other => panic!("expected New, got {other:?}"),
    };
    assert!(matches!(results.as_slice(), [EnqueueResult::New(_)]));

    assert_eq!(q.view().stats("work").await.unwrap().pending, 1);
    assert_eq!(q.view().stats("next").await.unwrap().pending, 1);
    let claimed = q
        .claim("work", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, id);
    assert_eq!(claimed.payload, b"payload");
    assert_eq!(
        q.view().kv_get(b"runs/abc").await.unwrap().as_deref(),
        Some(b"submitted".as_slice())
    );
    assert!(q.view().kv_get(b"runs/old").await.unwrap().is_none());

    let mut seen = Vec::new();
    let removed = index
        .pass(&q, 5_000, Duration::ZERO, |at_ms, suffix| {
            seen.push((at_ms, suffix));
            std::future::ready(Expired::Delete(SettlementEffects::default()))
        })
        .await
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(seen, [(1_000, b"abc".to_vec()), (5_000, b"later".to_vec())]);

    q.close().await.unwrap();
}

#[tokio::test]
async fn commit_effects_applies_enqueues_and_kv_in_one_transaction() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    q.kv_put(b"runs/old", b"stale").await.unwrap();
    let request = || EnqueueRequest {
        queue: "work".to_string(),
        payload: b"follow-up".to_vec(),
        options: EnqueueOptions::default().dedup_key(Some("once".into())),
    };

    let results = q
        .commit_effects(
            SettlementEffects::default()
                .enqueues(vec![request()])
                .kv_put(b"runs/new".to_vec(), b"reconciled".to_vec())
                .kv_deletes(vec![b"runs/old".to_vec()]),
        )
        .await
        .unwrap();
    let id = match results.as_slice() {
        [EnqueueResult::New(id)] => id.clone(),
        other => panic!("expected one new enqueue, got {other:?}"),
    };
    assert_eq!(
        q.view().get_job(&id).await.unwrap().unwrap().status,
        JobStatus::Pending
    );
    assert_eq!(
        q.view().kv_get(b"runs/new").await.unwrap().as_deref(),
        Some(b"reconciled".as_slice()),
    );
    assert!(q.view().kv_get(b"runs/old").await.unwrap().is_none());

    // A dedup hit downgrades that enqueue and leaves the writes applied.
    let results = q
        .commit_effects(
            SettlementEffects::default()
                .enqueues(vec![request()])
                .kv_put(b"runs/new".to_vec(), b"again".to_vec()),
        )
        .await
        .unwrap();
    assert!(
        matches!(results.as_slice(), [EnqueueResult::AlreadyEnqueued(existing)] if *existing == id)
    );
    assert_eq!(
        q.view().kv_get(b"runs/new").await.unwrap().as_deref(),
        Some(b"again".as_slice()),
    );
    assert_eq!(q.view().stats("work").await.unwrap().pending, 1);

    q.close().await.unwrap();
}

#[tokio::test]
async fn kv_compare_commit_applies_the_effects_only_when_every_key_matches() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    q.kv_put(b"runs/1", b"active").await.unwrap();
    q.kv_put(b"runs/1/step", b"3").await.unwrap();
    let effects = || {
        SettlementEffects::default()
            .enqueue(EnqueueRequest {
                queue: "work".to_string(),
                payload: b"follow-up".to_vec(),
                options: EnqueueOptions::default(),
            })
            .kv_put(b"runs/1".to_vec(), b"done".to_vec())
            .kv_delete(b"runs/1/step".to_vec())
    };
    async fn pending(q: &Queue) -> usize {
        q.view()
            .list_jobs("work", JobStatus::Pending, None, 10)
            .await
            .unwrap()
            .jobs
            .len()
    }

    // A different value and a wrong absence expectation do not apply the
    // effects.
    assert!(
        q.kv_compare_commit(&[(b"runs/1", Some(b"stale"))], effects())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        q.kv_compare_commit(&[(b"runs/1", None)], effects())
            .await
            .unwrap()
            .is_none()
    );
    // One compare of a list that does not match has the same effect.
    assert!(
        q.kv_compare_commit(
            &[(b"runs/1", Some(b"active")), (b"runs/1/step", Some(b"2"))],
            effects()
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(
        q.view().kv_get(b"runs/1").await.unwrap().as_deref(),
        Some(b"active".as_slice())
    );
    assert_eq!(
        q.view().kv_get(b"runs/1/step").await.unwrap().as_deref(),
        Some(b"3".as_slice())
    );
    assert_eq!(pending(&q).await, 0);

    // The expected values apply the effects, the compared keys included.
    let results = q
        .kv_compare_commit(
            &[(b"runs/1", Some(b"active")), (b"runs/1/step", Some(b"3"))],
            effects(),
        )
        .await
        .unwrap()
        .expect("the value matched");
    assert!(matches!(results.as_slice(), [EnqueueResult::New(_)]));
    assert_eq!(
        q.view().kv_get(b"runs/1").await.unwrap().as_deref(),
        Some(b"done".as_slice())
    );
    assert!(q.view().kv_get(b"runs/1/step").await.unwrap().is_none());
    assert_eq!(pending(&q).await, 1);

    // Absence is a state to compare against.
    let claim = SettlementEffects::default().kv_put(b"runs/2".to_vec(), b"active".to_vec());
    let results = q
        .kv_compare_commit(&[(b"runs/2", None)], claim)
        .await
        .unwrap();
    assert!(matches!(results.as_deref(), Some([])));
    assert_eq!(
        q.view().kv_get(b"runs/2").await.unwrap().as_deref(),
        Some(b"active".as_slice())
    );

    q.close().await.unwrap();
}

#[tokio::test]
async fn enqueue_with_effects_applies_no_effects_on_a_dedup_hit() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    let index = ExpiryIndex::new(b"expiry/".to_vec());
    let opts = || EnqueueOptions {
        dedup_key: Some("run-abc".into()),
        ..Default::default()
    };

    let (first_outcome, _) = q
        .enqueue_with_effects(
            "work",
            b"first".to_vec(),
            opts(),
            SettlementEffects::default().kv_put(b"runs/abc", b"first-record"),
        )
        .await
        .unwrap();
    let first_id = match first_outcome {
        EnqueueResult::New(id) => id,
        other => panic!("expected New, got {other:?}"),
    };

    let (second_outcome, results) = q
        .enqueue_with_effects(
            "work",
            b"second".to_vec(),
            opts(),
            SettlementEffects::default()
                .enqueue(EnqueueRequest {
                    queue: "next".to_string(),
                    payload: b"follow-up".to_vec(),
                    options: EnqueueOptions::default(),
                })
                .kv_put(b"runs/abc", b"second-record")
                .expiry_entry(&index, 1_000, b"abc"),
        )
        .await
        .unwrap();
    match second_outcome {
        EnqueueResult::AlreadyEnqueued(id) => assert_eq!(id, first_id),
        other => panic!("expected AlreadyEnqueued, got {other:?}"),
    }
    assert!(results.is_empty());

    assert_eq!(q.view().stats("work").await.unwrap().pending, 1);
    assert_eq!(q.view().stats("next").await.unwrap().pending, 0);
    assert_eq!(
        q.view().kv_get(b"runs/abc").await.unwrap().as_deref(),
        Some(b"first-record".as_slice())
    );
    assert!(
        q.view()
            .kv_get(&index.entry_key(1_000, b"abc"))
            .await
            .unwrap()
            .is_none()
    );

    q.close().await.unwrap();
}

#[tokio::test]
async fn enqueue_with_effects_rejects_an_oversized_value() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    let oversized = vec![0u8; MAX_KV_VALUE_SIZE + 1];
    let err = q
        .enqueue_with_effects(
            "work",
            b"x".to_vec(),
            EnqueueOptions::default(),
            SettlementEffects::default().kv_put(b"big", oversized),
        )
        .await
        .unwrap_err();
    match err {
        Error::KvValueTooLarge { size, max } => {
            assert_eq!(size, MAX_KV_VALUE_SIZE + 1);
            assert_eq!(max, MAX_KV_VALUE_SIZE);
        }
        other => panic!("expected KvValueTooLarge, got {other:?}"),
    }
    // Nothing enqueued: validation runs before the transaction.
    assert_eq!(q.view().stats("work").await.unwrap().pending, 0);
    q.close().await.unwrap();
}

#[tokio::test]
async fn an_exhausted_nack_clears_claimed_at_on_the_dead_record() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    let id = q
        .enqueue_with(
            "work",
            b"job".to_vec(),
            EnqueueOptions {
                max_attempts: Some(1),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();
    let job = q
        .claim("work", Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    assert!(job.claimed_at.is_some());

    q.nack(&job, "fatal").await.unwrap();
    let dead = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(dead.status, JobStatus::Dead);
    assert!(dead.claimed_at.is_none());
    q.close().await.unwrap();
}

// Every claimed record must have a registry entry. A record without one is
// invisible to the reaper until the next open.

#[tokio::test]
async fn claim_by_id_claims_the_named_pending_job_and_consumes_an_attempt() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    let first = q.enqueue("q", b"first".to_vec()).await.unwrap();
    let second = q
        .enqueue_with(
            "q",
            b"second".to_vec(),
            EnqueueOptions::default().dedup_key("once".to_string()),
        )
        .await
        .unwrap();

    let ClaimOutcome::Claimed(claim) = q
        .claim_by_id(&second, Duration::from_secs(30))
        .await
        .unwrap()
    else {
        panic!("the pending job is claimed");
    };
    assert_eq!(claim.id, second);
    assert_eq!(claim.status, JobStatus::Claimed);
    assert_eq!(claim.attempts, 1);
    assert_eq!(claim.payload, b"second");
    let stats = q.view().stats("q").await.unwrap();
    assert_eq!((stats.pending, stats.claimed), (1, 1));
    // The dedup key is released by the claim.
    let again = q
        .enqueue_with(
            "q",
            b"third".to_vec(),
            EnqueueOptions::default().dedup_key("once".to_string()),
        )
        .await
        .unwrap();
    assert_ne!(again, second);
    // The scan claim skips the claimed job and the claim settles.
    let scanned = q
        .claim("q", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(scanned.id, first);
    q.ack(&claim).await.unwrap();
    assert!(q.view().get_job(&second).await.unwrap().is_none());
}

#[tokio::test]
async fn claim_by_id_claims_a_scheduled_job_and_reports_the_rest() {
    let q = Queue::open(make_store(), "test").await.unwrap();
    assert!(matches!(
        q.claim_by_id("absent", Duration::from_secs(1))
            .await
            .unwrap(),
        ClaimOutcome::NotFound
    ));
    let scheduled = q
        .enqueue_with(
            "q",
            b"later".to_vec(),
            EnqueueOptions::default()
                .run_at(std::time::SystemTime::now() + Duration::from_secs(3600)),
        )
        .await
        .unwrap();
    // A scheduled job is claimed before its `run_at`.
    let ClaimOutcome::Claimed(early) = q
        .claim_by_id(&scheduled, Duration::from_secs(30))
        .await
        .unwrap()
    else {
        panic!("the scheduled job is claimed");
    };
    assert_eq!(early.status, JobStatus::Claimed);
    let stats = q.view().stats("q").await.unwrap();
    assert_eq!((stats.scheduled, stats.claimed), (0, 1));
    q.ack(&early).await.unwrap();
    let id = q.enqueue("q", b"now".to_vec()).await.unwrap();
    let claim = q
        .claim("q", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        q.claim_by_id(&id, Duration::from_secs(1)).await.unwrap(),
        ClaimOutcome::NotClaimable
    ));
    q.ack(&claim).await.unwrap();
    assert!(matches!(
        q.claim_by_id(&id, Duration::from_secs(1)).await.unwrap(),
        ClaimOutcome::NotFound
    ));
}

#[tokio::test]
async fn a_claim_by_id_is_reaped_when_its_lease_expires() {
    let clock = MockClock::new(1_000_000);
    let q = Queue::open_with_options(
        make_store(),
        "test",
        OpenOptions::default().clock(Arc::new(clock.clone())),
    )
    .await
    .unwrap();
    let id = q.enqueue("q", b"work".to_vec()).await.unwrap();
    let ClaimOutcome::Claimed(claim) = q.claim_by_id(&id, Duration::from_secs(10)).await.unwrap()
    else {
        panic!("the pending job is claimed");
    };
    assert_eq!(q.lease_expiry("q", &id), Some(1_010_000));

    clock.advance(Duration::from_secs(11));
    q.reap_now().await.unwrap();
    let job = q.view().get_job(&id).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Pending);
    assert!(matches!(q.ack(&claim).await, Err(Error::ClaimLost)));
}

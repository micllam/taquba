use super::*;

#[tokio::test(start_paused = true)]
async fn terminal_marker_is_written_at_the_runtime_clock() {
    // The queue's MockClock is shared into the runtime by default (via
    // Queue::clock()), so a `clock.advance` between submit and terminate is
    // visible in the marker's terminal_at_ms.
    let (queue, store, clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(
        queue,
        store.clone(),
        ScriptedRunner::new(vec![StepOutcome::Succeed {
            result: b"done".to_vec(),
        }]),
    )
    .memo_retention(Duration::from_secs(60))
    .build()
    .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: b"in".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    advance(&clock, Duration::from_secs(30)).await;
    let _ = wait_for(&runtime, &handle.run_id).await;

    let markers = terminal_markers(&runtime.inner.core.queue).await;
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].0, handle.run_id);
    // MockClock only moves on explicit advance/set, so the value the effects
    // builder reads is exactly the post-advance clock.
    assert_eq!(markers[0].1, 10_000 + 30_000);

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_malformed_terminal_marker_is_deleted_without_clearing_memos() {
    let (queue, store, clock) = open_queue_at(10_000).await;
    let runtime =
        WorkflowRuntime::builder(queue.clone(), store.clone(), ScriptedRunner::new(vec![]))
            .memo_retention(Duration::from_secs(60))
            .build()
            .unwrap();

    let memos = MemoStore::new(store, "workflow-steps-memo");
    memos
        .new_memo(&rid("bystander"), 0)
        .put("k", b"expensive")
        .await
        .unwrap();
    // A marker with an empty id, which `RunId` rejects.
    let marker = ExpiryIndex::new(TERMINAL_KV_PREFIX).entry_key(0, b"");
    queue.kv_put(&marker, b"").await.unwrap();
    // A key too short for a time.
    let unparseable = [TERMINAL_KV_PREFIX, b"short"].concat();
    queue.kv_put(&unparseable, b"").await.unwrap();

    advance(&clock, Duration::from_secs(3_600)).await;
    runtime.inner.core.sweep_once().await.unwrap();
    assert_eq!(
        memos.new_memo(&rid("bystander"), 0).get("k").await.unwrap(),
        Some(b"expensive".to_vec()),
        "an unrelated run's memo entries must survive",
    );
    assert!(
        queue.view().kv_get(&marker).await.unwrap().is_none(),
        "the marker is removed and not retried on every sweep",
    );
    assert!(queue.view().kv_get(&unparseable).await.unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn the_sweep_clears_only_markers_older_than_the_cutoff() {
    // Markers sort by timestamp, so the sweep scans from the start of the range
    // and returns at the first unexpired marker.
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime =
        WorkflowRuntime::builder(queue.clone(), store.clone(), ScriptedRunner::new(vec![]))
            .memo_retention(Duration::from_secs(1))
            .build()
            .unwrap();

    let memos = MemoStore::new(store, "workflow-steps-memo");
    let sweep = runtime.inner.core.memo_sweep.as_ref().unwrap();
    for (run_id, at_ms) in [("old", 1_000u64), ("young", 9_500u64)] {
        let run_id = rid(run_id);
        memos.new_memo(&run_id, 0).put("k", b"v").await.unwrap();
        let termination = DurableTermination {
            status: TerminalStatus::Succeeded.into(),
            error: None,
            cancelled_by: None,
            error_kind: None,
            final_step: DurableStepRef {
                step_number: 0,
                job_id: "job".to_string(),
            },
            terminated_at_ms: at_ms,
            input_hash: [0; 32],
            submitted_at_ms: at_ms,
            headers: HashMap::new(),
            result: Some(DurableResult::Empty),
        };
        let record = SettlementEffects::default().kv_put(
            run_kv_key(&run_id),
            durable::encode(&DurableRun::Terminated(termination)),
        );
        queue
            .commit_effects(sweep.mark(record, &run_id, at_ms))
            .await
            .unwrap();
    }

    // Clock at 10_000 with 1s retention: cutoff 9_000.
    let cleared = runtime.inner.core.sweep_once().await.unwrap();
    assert_eq!(cleared, 1);

    let remaining = terminal_markers(&queue).await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].0, "young");
    assert_eq!(memos.new_memo(&rid("old"), 0).get("k").await.unwrap(), None);
    assert_eq!(
        memos.new_memo(&rid("young"), 0).get("k").await.unwrap(),
        Some(b"v".to_vec()),
    );
}

#[tokio::test(start_paused = true)]
async fn a_terminated_run_keeps_its_run_id_until_the_memo_sweep_removes_it() {
    let (queue, store, clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        FixedRunner::new(Ok(StepOutcome::Succeed {
            result: b"done".to_vec(),
        })),
    )
    .memo_retention(Duration::from_secs(1))
    .build()
    .unwrap();
    let spec = RunSpec {
        run_id: Some(rid("once")),
        input: b"x".to_vec(),
        ..Default::default()
    };
    let first = runtime.submit(spec.clone()).await.unwrap();
    let claim = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let effects = runtime
        .inner
        .process_step(&claim, &LeaseHandle::detached())
        .await
        .unwrap();
    queue.ack_with(&claim, effects).await.unwrap();

    let again = runtime.submit(spec.clone()).await.unwrap();
    assert!(!again.newly_submitted, "the terminal record answers");
    assert_eq!(again.job_id, first.job_id, "the job of the final step");
    let other = RunSpec {
        input: b"y".to_vec(),
        ..spec.clone()
    };
    assert!(matches!(
        runtime.submit(other).await,
        Err(Error::InputMismatch(id)) if id == "once"
    ));

    clock.advance(Duration::from_secs(1));
    assert_eq!(runtime.inner.core.sweep_once().await.unwrap(), 1);
    assert!(runtime.submit(spec).await.unwrap().newly_submitted);
}

/// A hook that returns the next of `expiries` at each expiry.
#[tokio::test(start_paused = true)]
async fn the_memo_sweep_deletes_the_run_kv_entries_with_the_run() {
    let (queue, store, clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        FixedRunner::new(Ok(StepOutcome::Succeed {
            result: b"done".to_vec(),
        })),
    )
    .memo_retention(Duration::from_secs(1))
    .build()
    .unwrap();
    let handle = runtime
        .submit(RunSpec {
            run_id: Some(rid("once")),
            ..Default::default()
        })
        .await
        .unwrap();
    // The id `once-x` begins with the swept run id, and its entry stays.
    let entries = [
        run_kv_entry_key(&rid("once"), b"a"),
        run_kv_entry_key(&rid("once"), b"b"),
        run_kv_entry_key(&rid("once-x"), b"a"),
    ];
    let mut effects = SettlementEffects::default();
    for key in &entries {
        effects = effects.kv_put(key.clone(), "v");
    }
    queue.commit_effects(effects).await.unwrap();
    let claim = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let effects = runtime
        .inner
        .process_step(&claim, &LeaseHandle::detached())
        .await
        .unwrap();
    queue.ack_with(&claim, effects).await.unwrap();
    clock.advance(Duration::from_secs(1));

    assert_eq!(runtime.inner.core.sweep_once().await.unwrap(), 1);
    assert!(runtime.status(&handle.run_id).await.unwrap().is_none());
    for (key, kept) in entries.iter().zip([false, false, true]) {
        assert_eq!(queue.view().kv_get(key).await.unwrap().is_some(), kept);
    }
}

/// Yield up to `iters` times waiting for `cond` to become true. Used in sweeper
/// tests to let the spawned sweep task make progress between
/// `tokio::time::advance` and the assertion. Returns true if the condition held
/// within `iters` yields.
async fn yield_until<F, Fut>(iters: usize, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    for _ in 0..iters {
        if cond().await {
            return true;
        }
        tokio::task::yield_now().await;
    }
    false
}

#[tokio::test(start_paused = true)]
async fn the_sweeper_clears_a_marker_only_after_retention_elapses() {
    // Retention 200ms and a 10ms poll interval. A pass 199ms after the marker
    // is written leaves it. Within a poll interval of the boundary the sweep
    // loop clears the marker and the run's memo entries.
    let (queue, store, clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(
        queue,
        store.clone(),
        ScriptedRunner::new(vec![StepOutcome::Succeed {
            result: b"done".to_vec(),
        }]),
    )
    .memo_retention(Duration::from_millis(200))
    .poll_interval(Duration::from_millis(10))
    .build()
    .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: b"in".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let _ = wait_for(&runtime, &handle.run_id).await;
    let memos = MemoStore::new(store.clone(), "workflow-steps-memo");
    memos
        .new_memo(&handle.run_id, 0)
        .put("k", b"cached")
        .await
        .unwrap();

    advance(&clock, Duration::from_millis(199)).await;
    assert_eq!(runtime.inner.core.sweep_once().await.unwrap(), 0);
    let markers = terminal_markers(&runtime.inner.core.queue).await;
    assert_eq!(
        markers.len(),
        1,
        "a marker within the window must not be swept"
    );

    advance(&clock, Duration::from_millis(1)).await;
    advance(&clock, Duration::from_millis(10)).await;
    let cleared = yield_until(50, || async {
        terminal_markers(&runtime.inner.core.queue).await.is_empty()
    })
    .await;
    assert!(cleared, "sweeper did not clear the expired marker");
    assert_eq!(
        memos.new_memo(&handle.run_id, 0).get("k").await.unwrap(),
        None,
        "sweeper did not clear the run's memo entries",
    );
    assert!(
        runtime.status(&handle.run_id).await.unwrap().is_none(),
        "sweeper did not clear the run's terminal record",
    );

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn sweeper_keeps_memos_of_runs_without_a_terminal_marker() {
    // A run gets a terminal marker only once it terminates, and a terminated
    // run never resumes. The sweep uses those markers as its keys, so it never
    // deletes an in-flight run's memo entries before a resume reads them, even
    // past the retention window. Here a memo entry exists for a run without a
    // terminal marker. Advancing well past retention must leave it in place.
    let (queue, store, clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue, store.clone(), ScriptedRunner::new(vec![]))
        .memo_retention(Duration::from_millis(100))
        .build()
        .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let memos = MemoStore::new(store.clone(), "workflow-steps-memo");
    memos
        .new_memo(&rid("in-flight-run"), 0)
        .put("k", b"cached")
        .await
        .unwrap();

    advance(&clock, Duration::from_millis(500)).await;
    // Give the sweeper several ticks to run against the advanced clock.
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }

    assert_eq!(
        memos
            .new_memo(&rid("in-flight-run"), 0)
            .get("k")
            .await
            .unwrap(),
        Some(b"cached".to_vec()),
        "sweep must not remove memos of a run with no terminal marker",
    );

    let _ = shutdown.send(());
}

use super::*;

#[tokio::test(start_paused = true)]
async fn step_output_replay_skips_runner_after_crash_before_ack() {
    let (queue, store) = open_queue().await;
    let calls = Arc::new(AtomicU32::new(0));
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store.clone(),
        FixedRunner {
            result: Ok(StepOutcome::continue_now(b"step1-payload".to_vec())),
            calls: calls.clone(),
        },
    )
    .step_output_replay()
    .build()
    .unwrap();

    runtime
        .submit(RunSpec {
            run_id: Some(rid("replay-run")),
            input: b"input".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    let job = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    // Simulate a crash after the step output was stored but before the
    // settlement committed: discard the effects of the first delivery so
    // nothing is enqueued.
    let _ = runtime
        .inner
        .process_step(&job, &LeaseHandle::detached())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Re-processing the same claimed record replays the stored step outcome
    // without invoking the runner a second time.
    let effects = runtime
        .inner
        .process_step(&job, &LeaseHandle::detached())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    queue.ack_with(&job, effects).await.unwrap();

    let next = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.payload.as_slice(), b"step1-payload");
    assert_eq!(next.headers.get(HEADER_RUN_ID).unwrap(), "replay-run");
    assert_eq!(next.headers.get(HEADER_STEP).unwrap(), "1");
    assert!(
        queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none(),
        "the replayed continue must enqueue step 1 exactly once",
    );
}

#[tokio::test(start_paused = true)]
async fn corrupt_step_output_replay_entry_falls_back_to_runner() {
    let (queue, store) = open_queue().await;
    let calls = Arc::new(AtomicU32::new(0));
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store.clone(),
        FixedRunner {
            result: Ok(StepOutcome::continue_now(b"step1-payload".to_vec())),
            calls: calls.clone(),
        },
    )
    .step_output_replay()
    .build()
    .unwrap();

    runtime
        .submit(RunSpec {
            run_id: Some(rid("corrupt-run")),
            input: b"input".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    let job = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    runtime
        .inner
        .core
        .memo_store
        .put_step_output(&rid("corrupt-run"), 0, &job.payload, b"not msgpack")
        .await
        .unwrap();

    runtime
        .inner
        .process_step(&job, &LeaseHandle::detached())
        .await
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "corrupt entry is treated as a miss",
    );

    // The recomputed outcome overwrites the corrupt entry, so a second delivery
    // replays it without invoking the runner again.
    runtime
        .inner
        .process_step(&job, &LeaseHandle::detached())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_replayed_terminal_outcome_terminates_the_run_without_the_runner() {
    let cases = [
        (
            StepOutcome::Succeed {
                result: b"final".to_vec(),
            },
            (TerminalStatus::Succeeded, None, None),
            Some(b"final".as_slice()),
        ),
        (
            StepOutcome::Fail {
                reason: "rejected".to_string(),
            },
            (TerminalStatus::Failed, Some("rejected"), None),
            None,
        ),
        (
            StepOutcome::Cancel {
                reason: "obsolete".to_string(),
            },
            (
                TerminalStatus::Cancelled,
                Some("obsolete"),
                Some(CancelOrigin::Runner),
            ),
            None,
        ),
    ];
    for (stored, termination, result) in cases {
        let (queue, store) = open_queue().await;
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            FixedRunner {
                result: Ok(stored),
                calls: calls.clone(),
            },
        )
        .step_output_replay()
        .build()
        .unwrap();

        runtime
            .submit(RunSpec {
                run_id: Some(rid("terminal-replay")),
                input: b"input".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let job = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Redelivery after a crash before ack: the stored outcome settles the
        // run without invoking the runner again.
        let effects = runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        queue.ack_with(&job, effects).await.unwrap();

        let outcome = runtime.wait(&rid("terminal-replay")).await.unwrap();
        assert_eq!(
            (
                outcome.termination.status,
                outcome.termination.error.as_deref(),
                outcome.termination.cancelled_by,
            ),
            termination
        );
        assert_eq!(outcome.result.as_deref(), result);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn a_replayed_continue_keeps_the_original_schedule() {
    let initial = 1_700_000_000_000;
    let due = initial + 60_000;
    let cases = [
        (
            StepOutcome::continue_after(b"step1".to_vec(), Duration::from_secs(60)),
            RunState::Scheduled { run_at_ms: due },
        ),
        (
            StepOutcome::continue_on_signal(b"step1".to_vec(), Duration::from_secs(60)),
            RunState::Waiting { timeout_at_ms: due },
        ),
    ];
    for (stored, state) in cases {
        let (queue, store, clock) = open_queue_at(initial).await;
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            FixedRunner {
                result: Ok(stored),
                calls: calls.clone(),
            },
        )
        .step_output_replay()
        .build()
        .unwrap();

        runtime
            .submit(RunSpec {
                run_id: Some(rid("delayed-replay")),
                input: b"input".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let job = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        // The effects of the first delivery are dropped, as in a crash before
        // ack.
        let _ = runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        advance(&clock, Duration::from_secs(20)).await;
        let effects = runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        queue.ack_with(&job, effects).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            runtime
                .status(&rid("delayed-replay"))
                .await
                .unwrap()
                .unwrap()
                .state,
            state
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_replayed_step_outcome_restores_its_staged_effects() {
    struct StagingContinueRunner {
        calls: Arc<AtomicU32>,
    }

    impl StepRunner for StagingContinueRunner {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            step.effects
                .put(b"app/replayed".to_vec(), b"v".to_vec())
                .map_err(|e| StepError::transient(e.to_string()))?;
            step.effects
                .delete(b"app/stale".to_vec())
                .map_err(|e| StepError::transient(e.to_string()))?;
            step.effects
                .enqueue(EnqueueRequest {
                    queue: "app-requests".to_string(),
                    payload: step.next_job_id.clone().into_bytes(),
                    options: EnqueueOptions::default(),
                })
                .map_err(|e| StepError::transient(e.to_string()))?;
            Ok(StepOutcome::continue_now(b"step1".to_vec()))
        }
    }

    let (queue, store) = open_queue().await;
    let calls = Arc::new(AtomicU32::new(0));
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        StagingContinueRunner {
            calls: calls.clone(),
        },
    )
    .step_output_replay()
    .build()
    .unwrap();

    queue.kv_put(b"app/stale", b"old").await.unwrap();
    runtime
        .submit(RunSpec {
            run_id: Some(rid("replay-effects")),
            input: b"input".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    let job = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    // First delivery: the returned effects are dropped, simulating a crash
    // between the replay-record write and the settlement.
    let _ = runtime
        .inner
        .process_step(&job, &LeaseHandle::detached())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        queue
            .view()
            .kv_get(b"app/replayed")
            .await
            .unwrap()
            .is_none()
    );

    // Redelivery replays the stored outcome and restores the staged effects
    // into the settlement without invoking the runner.
    let effects = runtime
        .inner
        .process_step(&job, &LeaseHandle::detached())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        effects.kv_writes.get(b"app/replayed".as_slice()),
        Some(&b"v".to_vec())
    );
    assert!(effects.kv_deletes.contains(&b"app/stale".to_vec()));
    let replayed: Vec<_> = effects
        .enqueues
        .iter()
        .filter(|request| request.queue == "app-requests")
        .collect();
    assert_eq!(replayed.len(), 1);
    let next_step = effects
        .enqueues
        .iter()
        .find(|request| request.queue == "workflow-steps")
        .unwrap();
    assert_eq!(
        Some(String::from_utf8(replayed[0].payload.clone()).unwrap()),
        next_step.options.id_override
    );
    queue.ack_with(&job, effects).await.unwrap();
    assert_eq!(
        queue
            .view()
            .kv_get(b"app/replayed")
            .await
            .unwrap()
            .as_deref(),
        Some(b"v".as_slice())
    );
    assert!(queue.view().kv_get(b"app/stale").await.unwrap().is_none());
}

use super::*;

/// The termination that `status` reports for a terminated run.
fn status_termination(status: &RunStatus) -> RunTermination {
    match &status.state {
        RunState::Terminated(termination) => termination.clone(),
        state => panic!("run is not terminated: {state:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn a_cancellation_survives_a_restart() {
    // Models a restart: the request recorded on the run record and the job's
    // persisted `cancel_requested` persist across the restart while a fresh
    // runtime starts without process state. The runner returns Succeed, so a
    // Cancelled outcome shows the request was read.
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let before = WorkflowRuntime::builder(
        queue.clone(),
        store.clone(),
        ScriptedRunner::new(vec![StepOutcome::Succeed {
            result: b"done".to_vec(),
        }]),
    )
    .build()
    .unwrap();

    let handle = before
        .submit(RunSpec {
            input: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let claim = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .expect("step 0 is claimable");
    assert_eq!(
        before
            .cancel(&handle.run_id, Some("operator".to_string()))
            .await
            .unwrap(),
        CancelRunOutcome::Requested
    );

    let after = WorkflowRuntime::builder(
        queue.clone(),
        store.clone(),
        ScriptedRunner::new(vec![StepOutcome::Succeed {
            result: b"done".to_vec(),
        }]),
    )
    .build()
    .unwrap();
    assert_eq!(
        after.status(&handle.run_id).await.unwrap().map(|s| s.state),
        Some(RunState::Cancelling {
            reason: Some("operator".to_string()),
            requested_at_ms: 10_000,
        }),
        "the fresh runtime reads the request from the run record",
    );

    let effects = after
        .inner
        .process_step(&claim, &queue.lease_handle(&claim))
        .await
        .unwrap();
    queue.ack_with(&claim, effects).await.unwrap();

    let outcome = after.wait(&handle.run_id).await.unwrap();
    assert_eq!(outcome.termination.status, TerminalStatus::Cancelled);
    assert!(outcome.result.is_none(), "the succeed payload is discarded");
}

#[tokio::test(start_paused = true)]
async fn a_cancellation_after_the_settlement_read_reaches_the_next_step() {
    // The worker reads the claim's token once the runner returns. A request
    // recorded after that read does not affect the advancing settlement. The
    // worker reads it from the run record when the next step is claimed, and
    // settles that step without running it.
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store.clone(),
        ScriptedRunner::new(vec![
            StepOutcome::Continue {
                payload: b"next".to_vec(),
                when: Trigger::Immediate,
            },
            StepOutcome::Succeed {
                result: b"done".to_vec(),
            },
        ]),
    )
    .build()
    .unwrap();

    let handle = runtime
        .submit(RunSpec {
            input: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let step0 = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .expect("step 0 is claimable");
    let effects = runtime
        .inner
        .process_step(&step0, &queue.lease_handle(&step0))
        .await
        .unwrap();
    assert_eq!(
        runtime.cancel(&handle.run_id, None).await.unwrap(),
        CancelRunOutcome::Requested
    );
    queue.ack_with(&step0, effects).await.unwrap();

    let step1 = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .expect("step 1 is claimable");
    let effects = runtime
        .inner
        .process_step(&step1, &queue.lease_handle(&step1))
        .await
        .unwrap();
    queue.ack_with(&step1, effects).await.unwrap();

    let outcome = runtime.wait(&handle.run_id).await.unwrap();
    assert_eq!(outcome.termination.status, TerminalStatus::Cancelled);
    assert_eq!(outcome.termination.final_step.number, 1);
    assert_eq!(
        terminal_status_of(&runtime, &handle.run_id).await,
        Some(outcome.termination.status)
    );
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_pending_run_commits_its_termination_with_the_removal() {
    // Pending case: a run sits in the queue, we call `cancel()` before any
    // worker claims it. `cancel` removes the step job and terminates the run
    // before returning.

    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store.clone(), UnreachableRunner)
        .memo_retention(Duration::from_secs(60))
        .build()
        .unwrap();
    // Note: deliberately do NOT spawn the worker loop, so the submitted step
    // stays Pending in the queue while we cancel it.

    let mut headers = HashMap::new();
    headers.insert("tenant".to_string(), "acme".to_string());

    let handle = runtime
        .submit(RunSpec {
            input: b"x".to_vec(),
            options: RunOptions {
                headers,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    let status = runtime
        .status(&handle.run_id)
        .await
        .unwrap()
        .expect("active");
    assert_eq!(status.state, RunState::Pending);
    assert_eq!(status.step.job_id, handle.job_id);

    assert_eq!(
        runtime
            .cancel(&handle.run_id, Some("operator".to_string()))
            .await
            .unwrap(),
        CancelRunOutcome::Removed
    );
    let status = runtime.status(&handle.run_id).await.unwrap().unwrap();
    assert_eq!(
        status.state,
        RunState::Terminated(RunTermination {
            status: TerminalStatus::Cancelled,
            error: Some("operator".to_string()),
            cancelled_by: Some(CancelOrigin::Request),
            error_kind: None,
            final_step: StepRef {
                number: 0,
                job_id: handle.job_id.clone(),
            },
            terminated_at_ms: 10_000,
        }),
        "the terminal record commits with the removal",
    );
    assert_eq!(status.step.number, 0);
    assert_eq!(status.step.job_id, handle.job_id);
    assert_eq!(
        runtime.cancel(&handle.run_id, None).await.unwrap(),
        CancelRunOutcome::NotActive,
        "a second cancel finds no run record",
    );

    // The marker and the terminal record commit with the removal.
    let markers = terminal_markers(&queue).await;
    assert_eq!(markers, vec![(handle.run_id.clone(), 10_000)]);
    assert!(
        runtime
            .view()
            .terminal_record(&handle.run_id)
            .await
            .unwrap()
            .is_some()
    );

    let outcome = runtime.wait(&handle.run_id).await.unwrap();
    assert_eq!(outcome.run_id, handle.run_id);
    assert_eq!(outcome.termination, status_termination(&status));
    assert_eq!(outcome.result, None);
    assert_eq!(outcome.headers.get("tenant").unwrap(), "acme");

    let stats = queue.view().stats("workflow-steps").await.unwrap();
    assert_eq!(stats.dead, 0, "cancel must not dead-letter");
    assert_eq!(stats.pending, 0, "cancelled job must be removed");
}

/// Run a single step that blocks on a gate, calls `cancel(run_id)` while the
/// step is in-flight, and then has the runner return the supplied error.
/// Asserts that external cancellation suppresses the error path entirely: the
/// run terminates as `Cancelled` (not `Failed`), no dead-letter is produced
/// regardless of `permanent`/`transient`, and the worker returns `Ok` (no
/// retry, no PermanentFailure propagation).
async fn assert_cancel_suppresses_runner_error(error: StepError) {
    let (queue, store) = open_queue_with(fast_options()).await;
    let (runner, gate) = GatedRunner::new(Err(error));
    let runtime = WorkflowRuntime::builder(queue.clone(), store.clone(), runner)
        .build()
        .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    gate.claimed().await;

    assert_eq!(
        runtime.cancel(&handle.run_id, None).await.unwrap(),
        CancelRunOutcome::Requested
    );

    // Release the runner. It returns Err. Without cancellation, the worker
    // dead-letters the step (permanent) or nacks it for retry (transient).
    // Cancellation must suppress both.
    gate.release();

    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Cancelled);
    assert_eq!(
        outcome.termination.error, None,
        "the request carries no reason"
    );
    assert_eq!(
        outcome.termination.cancelled_by,
        Some(CancelOrigin::Request)
    );
    assert_eq!(
        terminal_status_of(&runtime, &handle.run_id).await,
        Some(outcome.termination.status)
    );

    // Settle window: assert no retry attempt and no dead-letter.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        gate.calls.load(Ordering::SeqCst),
        1,
        "cancellation must suppress retries",
    );
    let stats = queue.view().stats("workflow-steps").await.unwrap();
    assert_eq!(stats.dead, 0, "cancellation must suppress dead-letter");

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn cancel_suppresses_a_runner_error() {
    // Without cancellation, `StepError::permanent` dead-letters the step and
    // `StepError::transient` nacks for retry. With an external cancel in
    // flight, the worker must ack and fire `Cancelled` instead, without
    // re-invoking the runner.
    assert_cancel_suppresses_runner_error(StepError::permanent("would-dead-letter")).await;
    assert_cancel_suppresses_runner_error(StepError::transient("would-retry")).await;
}

#[tokio::test(start_paused = true)]
async fn cancel_signals_step_token_for_cooperative_short_circuit() {
    // A runner that watches `step.cancel_token` short-circuits long after-claim
    // work as soon as `WorkflowRuntime::cancel` is called. Without the token,
    // step duration bounds the cancellation latency. With it, the runner
    // returns essentially immediately. The test checks this with a step that
    // otherwise sleeps for 30 seconds. If the token does not fire, the test
    // times out.
    struct CooperativeRunner {
        claimed: Arc<tokio::sync::Notify>,
    }
    impl StepRunner for CooperativeRunner {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            self.claimed.notify_one();
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    Ok(StepOutcome::Succeed { result: b"slow".to_vec() })
                }
                _ = step.cancel_token.cancelled() => {
                    Ok(StepOutcome::Cancel { reason: "cooperative".to_string() })
                }
            }
        }
    }

    let (queue, store) = open_queue().await;
    let claimed = Arc::new(tokio::sync::Notify::new());
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store.clone(),
        CooperativeRunner {
            claimed: claimed.clone(),
        },
    )
    .build()
    .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), claimed.notified())
        .await
        .expect("runner observed token");

    assert_eq!(
        runtime
            .cancel(&handle.run_id, Some("stop".to_string()))
            .await
            .unwrap(),
        CancelRunOutcome::Requested
    );

    // The run terminates well before the 30s sleep would end.
    let outcome = wait_for(&runtime, &handle.run_id).await;

    assert_eq!(outcome.termination.status, TerminalStatus::Cancelled);
    // The request's token led to the runner's Cancel, so the request's origin
    // and reason surface.
    assert_eq!(outcome.termination.error.as_deref(), Some("stop"));
    assert_eq!(
        outcome.termination.cancelled_by,
        Some(CancelOrigin::Request)
    );
    assert_eq!(
        terminal_status_of(&runtime, &handle.run_id).await,
        Some(outcome.termination.status)
    );

    let stats = queue.view().stats("workflow-steps").await.unwrap();
    assert_eq!(stats.dead, 0);

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn cancel_returns_false_for_a_terminated_or_unknown_run() {
    // Submit a run that succeeds normally, wait for its termination, then call
    // `cancel`. The terminal record replaced the run record with the success,
    // so `cancel` must report `NotActive`.
    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(
        queue,
        store.clone(),
        ScriptedRunner::new(vec![StepOutcome::Succeed {
            result: b"done".to_vec(),
        }]),
    )
    .build()
    .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);
    assert_eq!(
        terminal_status_of(&runtime, &handle.run_id).await,
        Some(outcome.termination.status)
    );

    assert_eq!(
        runtime.cancel(&handle.run_id, None).await.unwrap(),
        CancelRunOutcome::NotActive,
        "cancel on an already-terminated run must report NotActive",
    );
    assert_eq!(
        terminal_status_of(&runtime, &handle.run_id).await,
        Some(TerminalStatus::Succeeded),
        "the cancel leaves the terminated run as it was",
    );

    assert_eq!(
        runtime.cancel(&rid("never-submitted"), None).await.unwrap(),
        CancelRunOutcome::NotActive
    );

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_running_step_overrides_its_outcome_and_writes_its_marker_at_settlement() {
    // Between the request and the settlement the run reports Cancelling and
    // keeps its record without a marker (the queue discards the effects on the
    // `Requested` arm). The settlement then commits Cancelled in place of the
    // runner's outcome.
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let (runner, gate) = GatedRunner::new(Ok(StepOutcome::Succeed {
        result: b"would-have-succeeded".to_vec(),
    }));
    let runtime = WorkflowRuntime::builder(queue.clone(), store.clone(), runner)
        .memo_retention(Duration::from_secs(60))
        .build()
        .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    gate.claimed().await;
    assert_eq!(
        runtime
            .status(&handle.run_id)
            .await
            .unwrap()
            .expect("active")
            .state,
        RunState::Running
    );

    assert_eq!(
        runtime
            .cancel(&handle.run_id, Some("operator".to_string()))
            .await
            .unwrap(),
        CancelRunOutcome::Requested
    );
    assert_eq!(
        runtime
            .status(&handle.run_id)
            .await
            .unwrap()
            .expect("entry retained while termination is in flight")
            .state,
        RunState::Cancelling {
            reason: Some("operator".to_string()),
            requested_at_ms: 10_000,
        }
    );
    assert!(
        terminal_markers(&queue).await.is_empty(),
        "a run still executing its step must have no terminal marker",
    );
    assert!(
        runtime
            .view()
            .run_record(&handle.run_id)
            .await
            .unwrap()
            .is_some(),
        "the run record must survive a cancel the worker has to finish",
    );

    gate.release();
    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Cancelled);
    assert!(
        outcome.result.is_none(),
        "succeed payload must be discarded"
    );
    assert_eq!(outcome.termination.error.as_deref(), Some("operator"));
    assert_eq!(
        outcome.termination.cancelled_by,
        Some(CancelOrigin::Request)
    );
    let status = runtime.status(&handle.run_id).await.unwrap().unwrap();
    assert!(matches!(
        status.state,
        RunState::Terminated(RunTermination {
            status: TerminalStatus::Cancelled,
            ref error,
            cancelled_by: Some(CancelOrigin::Request),
            ..
        }) if error.as_deref() == Some("operator")
    ));
    assert_eq!(
        terminal_markers(&queue).await,
        vec![(handle.run_id.clone(), 10_000)],
        "the worker's settlement writes it",
    );
    assert_eq!(queue.view().stats("workflow-steps").await.unwrap().dead, 0);

    let _ = shutdown.send(());
}

async fn wait_for_drained(queue: &Queue) {
    for _ in 0..200 {
        let stats = queue.view().stats("workflow-steps").await.unwrap();
        if stats.pending == 0 && stats.claimed == 0 && stats.scheduled == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the queue never drained");
}

#[tokio::test(start_paused = true)]
async fn a_runner_cancelling_its_own_token_is_not_an_external_cancel() {
    // The runner receives a child of the claim's token, so firing it leaves the
    // parent uncancelled and the step's staged effects are applied.
    struct SelfCancellingRunner;
    impl StepRunner for SelfCancellingRunner {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            step.cancel_token.cancel();
            step.effects
                .put("app/step-0", b"done")
                .map_err(|e| StepError::permanent(e.to_string()))?;
            Ok(StepOutcome::Succeed {
                result: b"finished".to_vec(),
            })
        }
    }

    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store, SelfCancellingRunner)
        .build()
        .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);
    assert_eq!(outcome.result.as_deref(), Some(b"finished".as_slice()));
    assert_eq!(wait_for_kv(&queue, b"app/step-0").await, b"done");

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn an_external_cancel_discards_staged_effects() {
    struct StageThenAwaitCancel {
        started: tokio::sync::mpsc::UnboundedSender<()>,
    }

    impl StepRunner for StageThenAwaitCancel {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            step.effects
                .put(b"app/override".to_vec(), b"staged".to_vec())
                .map_err(|e| StepError::permanent(e.to_string()))?;
            let _ = self.started.send(());
            step.cancel_token.cancelled().await;
            Ok(StepOutcome::continue_now(Vec::new()))
        }
    }

    let (queue, store) = open_queue().await;
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        StageThenAwaitCancel {
            started: started_tx,
        },
    )
    .build()
    .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        runtime.cancel(&handle.run_id, None).await.unwrap(),
        CancelRunOutcome::Requested
    );

    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Cancelled);
    assert_eq!(outcome.termination.error, None);

    wait_for_drained(&queue).await;
    assert!(
        queue
            .view()
            .kv_get(b"app/override")
            .await
            .unwrap()
            .is_none()
    );

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_cancel_request_does_not_reach_a_re_submission_of_the_run_id() {
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store, UnreachableRunner)
        .build()
        .unwrap();
    let spec = RunSpec {
        run_id: Some(rid("again")),
        input: b"x".to_vec(),
        ..Default::default()
    };
    runtime.submit(spec.clone()).await.unwrap();
    assert_eq!(
        runtime.cancel(&rid("again"), None).await.unwrap(),
        CancelRunOutcome::Removed
    );
    let end = runtime.wait(&rid("again")).await.unwrap();
    assert_eq!(end.termination.status, TerminalStatus::Cancelled);

    // The test removes the terminal record as the memo sweep does, so the run
    // id starts a new run.
    queue.kv_delete(&run_kv_key(&rid("again"))).await.unwrap();
    assert!(runtime.submit(spec).await.unwrap().newly_submitted);
    let record = runtime
        .view()
        .run_record(&rid("again"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.cancel_request, None);
    let status = runtime.status(&rid("again")).await.unwrap().unwrap();
    assert_eq!(status.state, RunState::Pending);
}

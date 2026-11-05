use super::*;

/// A runner that extends its lease through the step and reports the resulting
/// expiry.
struct RenewingRunner {
    queue: Arc<Queue>,
    tx: tokio::sync::mpsc::UnboundedSender<u64>,
}

impl StepRunner for RenewingRunner {
    async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
        step.lease
            .ensure_at_least(Duration::from_secs(600))
            .map_err(|e| StepError::transient(e.to_string()))?;
        let expiry = self
            .queue
            .lease_expiry("workflow-steps", &step.job_id)
            .expect("a running step holds a lease");
        let _ = self.tx.send(expiry);
        Ok(StepOutcome::Succeed { result: Vec::new() })
    }
}

#[tokio::test(start_paused = true)]
async fn a_step_runner_extends_its_lease_through_the_step() {
    let base = 1_700_000_000_000;
    let (queue, store, _clock) = open_queue_at(base).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let runtime =
        WorkflowRuntime::builder(queue.clone(), store.clone(), RenewingRunner { queue, tx })
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
    let expiry = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        expiry >= base + 600_000,
        "the extension must reach the lease registry",
    );
    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn single_step_succeeds_and_writes_no_marker_without_retention() {
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
            input: b"in".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let outcome = wait_for(&runtime, &handle.run_id).await;

    assert_eq!(outcome.run_id, handle.run_id);
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);
    assert_eq!(outcome.result.as_deref(), Some(b"done".as_slice()));
    assert_eq!(outcome.termination.final_step.number, 0);
    assert_eq!(
        terminal_status_of(&runtime, &handle.run_id).await,
        Some(TerminalStatus::Succeeded),
        "the terminal record is written without retention",
    );
    assert!(terminal_markers(&runtime.inner.core.queue).await.is_empty());

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn multi_step_run_advances_through_continue_with_its_headers() {
    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(
        queue,
        store.clone(),
        ScriptedRunner::new(vec![
            StepOutcome::continue_now(b"step1".to_vec()),
            StepOutcome::continue_now(b"step2".to_vec()),
            StepOutcome::Succeed {
                result: b"final".to_vec(),
            },
        ]),
    )
    .build()
    .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: b"start".to_vec(),
            options: RunOptions {
                headers: HashMap::from([
                    ("trace_id".to_string(), "abc-123".to_string()),
                    ("tenant".to_string(), "acme".to_string()),
                ]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    let outcome = wait_for(&runtime, &handle.run_id).await;

    assert_eq!(outcome.run_id, handle.run_id);
    assert_eq!(outcome.termination.final_step.number, 2);
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);
    assert_eq!(outcome.result.as_deref(), Some(b"final".as_slice()));
    assert_eq!(outcome.headers.get("trace_id").unwrap(), "abc-123");
    assert_eq!(outcome.headers.get("tenant").unwrap(), "acme");
    assert!(!outcome.headers.contains_key(HEADER_RUN_ID));
    assert!(!outcome.headers.contains_key(HEADER_STEP));

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn continue_after_delays_next_step_until_promotion() {
    let initial = 1_700_000_000_000u64;
    let (queue, store, clock) = open_queue_at(initial).await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store.clone(),
        ScriptedRunner::new(vec![
            StepOutcome::continue_after(b"step1".to_vec(), Duration::from_secs(60)),
            StepOutcome::Succeed {
                result: b"final".to_vec(),
            },
        ]),
    )
    .build()
    .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: b"start".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    // The delayed step is held in `scheduled` and the run must not terminate
    // while the delay is pending.
    assert!(
        tokio::time::timeout(Duration::from_millis(500), runtime.wait(&handle.run_id))
            .await
            .is_err()
    );
    let stats = queue.view().stats("workflow-steps").await.unwrap();
    assert_eq!(stats.scheduled, 1);
    assert_eq!(
        runtime.status(&handle.run_id).await.unwrap().unwrap().state,
        RunState::Scheduled {
            run_at_ms: initial + 60_000
        }
    );

    advance(&clock, Duration::from_secs(61)).await;
    queue.promote_scheduled_now().await.unwrap();

    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.run_id, handle.run_id);
    assert_eq!(outcome.termination.final_step.number, 1);
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_step_reports_its_attempt_limit() {
    struct Recording(Arc<std::sync::Mutex<Option<u32>>>);
    impl StepRunner for Recording {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            *self.0.lock().unwrap() = Some(step.max_attempts);
            Ok(StepOutcome::Succeed { result: Vec::new() })
        }
    }

    let seen = Arc::new(std::sync::Mutex::new(None));
    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store, Recording(seen.clone()))
        .build()
        .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let outcome = runtime
        .submit(RunSpec {
            input: b"x".to_vec(),
            options: RunOptions {
                max_attempts_per_step: Some(7),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    let job_id = outcome.job_id;
    queue.wait_for_completion(&job_id).await.unwrap();
    assert_eq!(*seen.lock().unwrap(), Some(7));

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn restart_resumes_at_next_step() {
    // Headline durability test: after step 0 acks and step 1 is in the queue,
    // kill runtime A entirely and spawn runtime B on the same Queue handle. B
    // claims and completes step 1 without re-running step 0.
    //
    // To make this race-free we gate step 0's runner: the test keeps the gate
    // while signalling shutdown to A so A enters drain mode without ever
    // claiming step 1. Then the gate is opened, A's spawned step-0 task
    // finishes (enqueueing step 1 + acking step 0) and A exits.
    struct CompleteOnStep1;
    impl StepRunner for CompleteOnStep1 {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            assert_eq!(step.step_number, 1, "runtime B should only ever see step 1");
            assert_eq!(step.payload.as_slice(), b"step1-payload");
            Ok(StepOutcome::Succeed {
                result: b"resumed".to_vec(),
            })
        }
    }

    let (queue, store) = open_queue().await;

    let (runner, gate) = GatedRunner::new(Ok(StepOutcome::continue_now(b"step1-payload".to_vec())));
    let runtime_a = WorkflowRuntime::builder(queue.clone(), store.clone(), runner)
        .max_concurrent_steps(1)
        .build()
        .unwrap();

    let (shutdown_a_tx, shutdown_a_rx) = oneshot::channel::<()>();
    let worker_a = {
        let runtime_a = runtime_a.clone();
        tokio::spawn(async move {
            let _ = runtime_a
                .run(async move {
                    let _ = shutdown_a_rx.await;
                })
                .await;
        })
    };

    let handle = runtime_a
        .submit(RunSpec {
            input: b"input".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    gate.claimed().await;
    let s = runtime_a
        .status(&handle.run_id)
        .await
        .unwrap()
        .expect("status");
    assert_eq!(s.state, RunState::Running);
    assert_eq!(s.step.number, 0);

    // A's worker is in the at-capacity select-loop. Signal shutdown first, then
    // open the gate so step 0 finishes processing inside drain mode (A will not
    // claim step 1).
    let _ = shutdown_a_tx.send(());
    gate.release();

    worker_a.await.expect("runtime A drained cleanly");

    // Bring up runtime B on the same Queue handle. It continues with step 1
    // from where A stopped.
    let runtime_b = WorkflowRuntime::builder(queue, store.clone(), CompleteOnStep1)
        .build()
        .unwrap();
    let shutdown_b = spawn_runtime(runtime_b.clone());

    let outcome = wait_for(&runtime_b, &handle.run_id).await;

    assert_eq!(outcome.run_id, handle.run_id);
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);
    assert_eq!(outcome.result.as_deref(), Some(b"resumed".as_slice()));
    assert_eq!(outcome.termination.final_step.number, 1);

    let _ = shutdown_b.send(());
}

/// Submits a run whose runner always returns [`StepError::transient`], capped
/// at `max_attempts`. Asserts the runner is invoked exactly `max_attempts`
/// times (per-step max-attempts propagation) and that the run terminates as
/// failed with its marker on the exhausting attempt.
async fn assert_transient_retries_until_max(max_attempts: u32) {
    let (queue, store) = open_queue_with(fast_options()).await;
    let calls = Arc::new(AtomicU32::new(0));
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store.clone(),
        FixedRunner {
            result: Err(StepError::transient("flaky")),
            calls: calls.clone(),
        },
    )
    .memo_retention(Duration::from_secs(60))
    .build()
    .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: b"x".to_vec(),
            options: RunOptions {
                max_attempts_per_step: Some(max_attempts),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();

    let outcome = wait_for(&runtime, &handle.run_id).await;

    assert_eq!(outcome.termination.status, TerminalStatus::Failed);
    assert_eq!(outcome.termination.error.as_deref(), Some("flaky"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        max_attempts,
        "runner called once per attempt up to max_attempts"
    );

    assert_eq!(queue.view().stats("workflow-steps").await.unwrap().dead, 1);
    assert!(
        runtime
            .view()
            .run_record(&handle.run_id)
            .await
            .unwrap()
            .is_none(),
        "the terminal record replaces the run record with the exhausted nack",
    );
    assert_eq!(
        terminal_markers(&queue)
            .await
            .iter()
            .filter(|(run_id, _)| *run_id == handle.run_id)
            .count(),
        1,
        "the terminal marker rides the exhausted nack",
    );

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn transient_retries_until_max_attempts() {
    assert_transient_retries_until_max(1).await;
    assert_transient_retries_until_max(3).await;
}

#[tokio::test(start_paused = true)]
async fn step_memo_survives_across_attempts_of_the_same_step() {
    // First attempt writes the memo entry and returns a transient error so the
    // runtime retries the step. The second attempt reads the same key back and
    // succeeds. This exercises the central use case: at-least-once retries of
    // one step short-circuit work the prior attempt already did.
    struct MemoRetryRunner;
    impl StepRunner for MemoRetryRunner {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            if step.attempts == 1 {
                step.memo
                    .put("cached", b"first-attempt-value")
                    .await
                    .map_err(|e| StepError::transient(e.to_string()))?;
                return Err(StepError::transient("force a retry"));
            }
            let got = step
                .memo
                .get("cached")
                .await
                .map_err(|e| StepError::transient(e.to_string()))?;
            assert_eq!(got, Some(b"first-attempt-value".to_vec()));
            Ok(StepOutcome::Succeed {
                result: got.unwrap_or_default(),
            })
        }
    }

    let (queue, store) = open_queue_with(fast_options()).await;
    let runtime = WorkflowRuntime::builder(queue, store, MemoRetryRunner)
        .build()
        .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime
        .submit(RunSpec {
            input: b"start".to_vec(),
            options: RunOptions {
                max_attempts_per_step: Some(3),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);
    assert_eq!(
        outcome.result.as_deref(),
        Some(b"first-attempt-value".as_slice())
    );

    let _ = shutdown.send(());
}

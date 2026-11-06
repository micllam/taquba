use super::*;

#[tokio::test(start_paused = true)]
async fn a_permanent_step_error_dead_letters_with_its_marker_and_no_staged_effects() {
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        EffectStagingRunner::new(vec![Err(StepError::permanent("nope"))]),
    )
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
    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.run_id, handle.run_id);
    assert_eq!(outcome.termination.status, TerminalStatus::Failed);
    assert_eq!(outcome.termination.error.as_deref(), Some("nope"));
    let status = runtime.status(&handle.run_id).await.unwrap().unwrap();
    assert!(
        matches!(
            status.state,
            RunState::Terminated(RunTermination {
                status: TerminalStatus::Failed,
                error: Some(ref error),
                error_kind: Some(StepErrorKind::Permanent),
                ..
            }) if error == "nope"
        ),
        "the terminal record commits with the dead-letter and carries the error kind",
    );

    // The dead-letter transaction commits the termination, so the dead job and
    // the marker are visible, and the staged effect was discarded with the
    // failure.
    assert_eq!(queue.view().stats("workflow-steps").await.unwrap().dead, 1);
    assert_eq!(
        terminal_markers(&queue).await,
        vec![(handle.run_id.clone(), 10_000)],
    );
    assert!(queue.view().kv_get(b"app/step-0").await.unwrap().is_none());

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_retrying_step_error_commits_no_terminal_marker() {
    // A transient failure with attempts left nacks for retry, and `nack_with`
    // discards the effects on that branch.
    struct FlakyRunner {
        attempts: Arc<std::sync::atomic::AtomicUsize>,
        clock: MockClock,
    }
    impl StepRunner for FlakyRunner {
        async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
            if self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                return Err(StepError::transient("flaky"));
            }
            // Separate the two settlements in clock time before the succeeding
            // one. Otherwise a marker wrongly written for the retry shares this
            // one's key and goes unobserved.
            self.clock.advance(Duration::from_secs(1));
            Ok(StepOutcome::Succeed {
                result: b"done".to_vec(),
            })
        }
    }

    let (queue, store, clock) = open_queue_at_with(10_000, fast_options()).await;
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store.clone(),
        FlakyRunner {
            attempts: attempts.clone(),
            clock: clock.clone(),
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
                max_attempts_per_step: Some(3),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);

    // Exactly one marker, recorded at a time after the retry's clock advance:
    // the success wrote a marker and the retry did not.
    let markers = terminal_markers(&queue).await;
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].0, handle.run_id);
    assert_eq!(markers[0].1, 11_000);

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_wait_follows_the_run_across_its_steps() {
    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(
        queue,
        store,
        ScriptedRunner::new(vec![
            StepOutcome::continue_now(b"next".to_vec()),
            StepOutcome::Succeed {
                result: b"done".to_vec(),
            },
        ]),
    )
    .build()
    .unwrap();
    assert!(matches!(
        runtime.wait(&rid("absent")).await,
        Err(Error::RunNotFound(id)) if id == "absent"
    ));
    let shutdown = spawn_runtime(runtime.clone());

    runtime
        .submit(RunSpec {
            run_id: Some(rid("two")),
            input: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(5), runtime.wait(&rid("two")))
        .await
        .expect("the wait resolved")
        .unwrap();
    assert_eq!(
        (
            outcome.termination.status,
            outcome.termination.final_step.number,
            outcome.result.as_deref()
        ),
        (TerminalStatus::Succeeded, 1, Some(b"done".as_slice()))
    );

    // A run that has already terminated is reported without a wait.
    let again = runtime.wait(&rid("two")).await.unwrap();
    assert_eq!(again, outcome);
    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_result_written_by_a_settlement_that_did_not_commit_is_not_reported() {
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        FixedRunner::new(Ok(StepOutcome::Succeed {
            result: b"done".to_vec(),
        })),
    )
    .build()
    .unwrap();
    runtime
        .submit(RunSpec {
            run_id: Some(rid("r")),
            input: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let claim = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    // The worker writes the result object before its settlement. The settlement
    // does not commit, and the queue dead-letters the step.
    runtime
        .inner
        .process_step(&claim, &LeaseHandle::detached())
        .await
        .unwrap();
    let memos = &runtime.inner.core.memo_store;
    let written = memos.get_result(&rid("r"), &claim.id, claim.attempts).await;
    assert_eq!(written.unwrap().as_deref(), Some(b"done".as_slice()));
    queue.dead_letter(&claim, "hung").await.unwrap();
    assert_eq!(runtime.inner.core.reconcile_dead_steps().await.unwrap(), 1);

    let run = runtime.wait(&rid("r")).await.unwrap();
    assert_eq!(run.termination.status, TerminalStatus::Failed);
    assert_eq!(
        run.result, None,
        "the object belongs to the settlement that did not commit"
    );
}

/// Submits the run `r` of a runner that returns `result`, settles its step and
/// returns the runtime with the job id and the attempt of the claim.
async fn settled_success(result: &[u8]) -> (WorkflowRuntime<FixedRunner>, String, u32) {
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        FixedRunner::new(Ok(StepOutcome::Succeed {
            result: result.to_vec(),
        })),
    )
    .build()
    .unwrap();
    runtime
        .submit(RunSpec {
            run_id: Some(rid("r")),
            ..Default::default()
        })
        .await
        .unwrap();
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
    (runtime, claim.id.clone(), claim.attempts)
}

#[tokio::test(start_paused = true)]
async fn an_empty_result_is_reported_without_an_object() {
    let (runtime, job_id, attempt) = settled_success(b"").await;
    let memos = &runtime.inner.core.memo_store;
    let object = memos.get_result(&rid("r"), &job_id, attempt).await;
    assert_eq!(object.unwrap(), None);
    let run = runtime.wait(&rid("r")).await.unwrap();
    assert_eq!(run.result.as_deref(), Some(b"".as_slice()));
}

#[tokio::test(start_paused = true)]
async fn a_result_object_removed_before_its_terminal_record_reads_as_swept() {
    let (runtime, _, _) = settled_success(b"done").await;
    // The memo sweep removes the objects of a run before the transaction that
    // deletes its terminal record.
    let memos = &runtime.inner.core.memo_store;
    assert_eq!(memos.clear_memos_for_run(&rid("r")).await.unwrap(), 1);
    assert!(runtime.status(&rid("r")).await.unwrap().is_some());
    assert_eq!(
        runtime.view().terminated_run(&rid("r")).await.unwrap(),
        None
    );
    assert!(matches!(
        runtime.wait(&rid("r")).await,
        Err(Error::RunNotFound(id)) if id == "r"
    ));
}

#[tokio::test(start_paused = true)]
async fn a_succeeded_terminal_record_without_a_result_reference_is_inconsistent() {
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store, UnreachableRunner)
        .build()
        .unwrap();
    // A terminal record of a run that succeeded before results were referenced
    // from the record.
    let termination = DurableTermination {
        status: TerminalStatus::Succeeded.into(),
        error: None,
        cancelled_by: None,
        error_kind: None,
        final_step: DurableStepRef {
            step_number: 0,
            job_id: "job".to_string(),
        },
        terminated_at_ms: 10_000,
        input_hash: [0; 32],
        submitted_at_ms: 10_000,
        headers: HashMap::new(),
        result: None,
    };
    let record = durable::encode(&DurableRun::Terminated(termination));
    queue
        .commit_effects(SettlementEffects::default().kv_put(run_kv_key(&rid("old")), record))
        .await
        .unwrap();
    assert!(matches!(
        runtime.wait(&rid("old")).await,
        Err(Error::InconsistentRunState(id)) if id == "old"
    ));
}

#[tokio::test(start_paused = true)]
async fn a_member_record_is_rewritten_only_by_the_terminating_settlement() {
    struct RecordReadingRunner {
        pending_seen: Arc<StdMutex<Vec<bool>>>,
    }

    impl StepRunner for RecordReadingRunner {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            let record = step
                .kv
                .get(&group_member_kv_key(&rid("g"), "m"))
                .await?
                .expect("the member record is written with the submission");
            let member: DurableMember = durable::decode(&record).unwrap();
            self.pending_seen
                .lock()
                .unwrap()
                .push(member.terminated.is_none());
            Err(StepError::transient("still failing"))
        }
    }

    let (queue, store) = open_queue_with(fast_options()).await;
    let pending_seen = Arc::new(StdMutex::new(Vec::new()));
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        RecordReadingRunner {
            pending_seen: pending_seen.clone(),
        },
    )
    .build()
    .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let group = runtime.group(rid("g"));
    group
        .submit(
            vec![GroupMember {
                key: "m".to_string(),
                input: Vec::new(),
                max_attempts: None,
            }],
            &RunOptions {
                max_attempts_per_step: Some(2),
                ..RunOptions::default()
            },
        )
        .await
        .unwrap();
    let member = crate::group::member_run_id(&rid("g"), "m", 0);
    let outcome = wait_for(&runtime, &member).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Failed);
    for _ in 0..200 {
        if queue.view().stats("workflow-steps").await.unwrap().dead == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(queue.view().stats("workflow-steps").await.unwrap().dead, 1);

    // The retried first attempt left the record pending, and the exhausted
    // second attempt's termination commits with the dead-letter.
    assert_eq!(*pending_seen.lock().unwrap(), vec![true, true]);
    let members = group.members().await.unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].key, "m");
    assert_eq!(members[0].status(), Some(TerminalStatus::Failed));
    assert_eq!(
        members[0]
            .record
            .terminated
            .as_ref()
            .unwrap()
            .error
            .as_deref(),
        Some("still failing")
    );

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn only_the_committed_outcome_is_reported() {
    struct GatedSecondAttempt {
        calls: Arc<AtomicU32>,
        running: tokio::sync::mpsc::UnboundedSender<()>,
    }

    impl StepRunner for GatedSecondAttempt {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(StepOutcome::Succeed {
                    result: b"done".to_vec(),
                });
            }
            let _ = self.running.send(());
            step.cancel_token.cancelled().await;
            Ok(StepOutcome::Succeed {
                result: b"done".to_vec(),
            })
        }
    }

    let (queue, store) = open_queue().await;
    let (running_tx, mut running_rx) = tokio::sync::mpsc::unbounded_channel();
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        GatedSecondAttempt {
            calls: Arc::new(AtomicU32::new(0)),
            running: running_tx,
        },
    )
    .build()
    .unwrap();

    runtime
        .submit(RunSpec {
            run_id: Some(rid("phantom")),
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    let job = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    // First settlement attempt: the runner succeeds and its result is written,
    // but the effects are dropped, as when the settlement loses the claim.
    let _ = runtime
        .inner
        .process_step(&job, &queue.lease_handle(&job))
        .await
        .unwrap();

    // The redelivered attempt observes an external cancel and commits
    // Cancelled.
    let worker = {
        let inner = runtime.inner.clone();
        let queue = queue.clone();
        tokio::spawn(async move {
            let effects = inner
                .process_step(&job, &queue.lease_handle(&job))
                .await
                .unwrap();
            queue.ack_with(&job, effects).await.unwrap();
        })
    };
    running_rx.recv().await.unwrap();
    assert_eq!(
        runtime.cancel(&rid("phantom"), None).await.unwrap(),
        CancelRunOutcome::Requested
    );
    worker.await.unwrap();
    let outcome = runtime.wait(&rid("phantom")).await.unwrap();
    assert_eq!(outcome.termination.status, TerminalStatus::Cancelled);
    assert_eq!(outcome.result, None, "the success never committed");
}

#[tokio::test(start_paused = true)]
async fn staged_hook_effects_commit_on_every_termination_path() {
    /// Stages a record of each termination: its status and time.
    struct StagingHook;

    impl TerminalHook for StagingHook {
        fn stage_effects(&self, outcome: &TerminatedRun, effects: &TerminalEffects) {
            let termination = &outcome.termination;
            effects
                .put(
                    format!("app/terminated/{}", outcome.run_id),
                    format!("{}@{}", termination.status, termination.terminated_at_ms),
                )
                .unwrap();
        }
    }

    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        ScriptedRunner::new(vec![StepOutcome::Succeed { result: Vec::new() }]),
    )
    .terminal_hook(StagingHook)
    .build()
    .unwrap();
    let spec = |id: &str| RunSpec {
        run_id: Some(rid(id)),
        input: Vec::new(),
        ..Default::default()
    };

    // The worker's settlement.
    runtime.submit(spec("settled")).await.unwrap();
    let job = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let effects = runtime
        .inner
        .process_step(&job, &LeaseHandle::detached())
        .await
        .unwrap();
    queue.ack_with(&job, effects).await.unwrap();

    // An external cancellation of a pending step.
    runtime.submit(spec("cancelled")).await.unwrap();
    assert_eq!(
        runtime.cancel(&rid("cancelled"), None).await.unwrap(),
        CancelRunOutcome::Removed
    );

    // The reconciliation of a step dead-lettered outside the worker.
    runtime.submit(spec("dead")).await.unwrap();
    let job = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    queue.dead_letter(&job, "hung").await.unwrap();
    assert_eq!(runtime.inner.core.reconcile_dead_steps().await.unwrap(), 1);

    // The staged record contains the time of the terminal record.
    for (id, status) in [
        ("settled", TerminalStatus::Succeeded),
        ("cancelled", TerminalStatus::Cancelled),
        ("dead", TerminalStatus::Failed),
    ] {
        let termination = runtime.wait(&rid(id)).await.unwrap().termination;
        assert_eq!(termination.status, status, "run {id}");
        let key = format!("app/terminated/{id}");
        let value = queue.view().kv_get(key.as_bytes()).await.unwrap();
        let staged = format!("{status}@{}", termination.terminated_at_ms);
        assert_eq!(value.as_deref(), Some(staged.as_bytes()), "run {id}");
    }
}

#[cfg(feature = "webhooks")]
#[tokio::test(start_paused = true)]
async fn a_webhook_hook_that_targets_the_runtime_queue_fails_the_build() {
    use crate::terminal::WebhookTerminalHook;

    let (queue, store) = open_queue().await;
    let built = WorkflowRuntime::builder(queue, store, UnreachableRunner)
        .terminal_hook(WebhookTerminalHook::new("workflow-steps"))
        .build();
    assert!(matches!(built, Err(Error::ReservedQueue(q)) if q == "workflow-steps"));
}

#[cfg(feature = "webhooks")]
#[tokio::test(start_paused = true)]
async fn the_webhook_hook_stages_its_delivery_in_the_terminating_settlement() {
    use crate::terminal::WebhookTerminalHook;

    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        ScriptedRunner::new(vec![StepOutcome::Succeed {
            result: b"payload".to_vec(),
        }]),
    )
    .terminal_hook(WebhookTerminalHook::new("callbacks"))
    .build()
    .unwrap();
    runtime
        .submit(RunSpec {
            run_id: Some(rid("with-callback")),
            input: Vec::new(),
            options: RunOptions {
                headers: HashMap::from([(
                    "callback_url".to_string(),
                    "https://example.com/done".to_string(),
                )]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    let job = queue
        .claim("workflow-steps", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let effects = runtime
        .inner
        .process_step(&job, &LeaseHandle::detached())
        .await
        .unwrap();
    queue.ack_with(&job, effects).await.unwrap();

    let webhook = queue
        .claim("callbacks", Duration::from_secs(30))
        .await
        .unwrap()
        .expect("the delivery committed with the termination");
    assert_eq!(webhook.payload.as_slice(), b"payload");
    assert_eq!(
        webhook.headers.get("webhook.url").unwrap(),
        "https://example.com/done"
    );
    assert_eq!(
        webhook.headers.get("http.Workflow-Run-Status").unwrap(),
        "succeeded"
    );
    assert!(
        !webhook
            .headers
            .contains_key("http.Workflow-Run-Cancelled-By")
    );

    // A cancelled run delivers its reason and the origin of the cancel.
    runtime
        .submit(RunSpec {
            run_id: Some(rid("cancelled-callback")),
            input: Vec::new(),
            options: RunOptions {
                headers: HashMap::from([(
                    "callback_url".to_string(),
                    "https://example.com/done".to_string(),
                )]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    runtime
        .cancel(&rid("cancelled-callback"), Some("operator".to_string()))
        .await
        .unwrap();
    let webhook = queue
        .claim("callbacks", Duration::from_secs(30))
        .await
        .unwrap()
        .expect("the delivery committed with the removal");
    assert_eq!(webhook.payload.as_slice(), b"operator");
    assert_eq!(
        webhook
            .headers
            .get("http.Workflow-Run-Cancelled-By")
            .unwrap(),
        "request"
    );
}

use super::*;

#[tokio::test(start_paused = true)]
async fn step_effects_commit_with_the_acking_settlement() {
    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        EffectStagingRunner::new(vec![
            Ok(StepOutcome::continue_now(b"next".to_vec())),
            Ok(StepOutcome::Succeed { result: Vec::new() }),
        ]),
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
    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);
    assert_eq!(wait_for_kv(&queue, b"app/step-0").await, b"done");
    assert_eq!(wait_for_kv(&queue, b"app/step-1").await, b"done");
    assert_eq!(queue.view().stats("app-requests").await.unwrap().pending, 2);

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_step_effect_is_readable_by_the_next_step() {
    struct ReadingRunner {
        read_under_staging: Arc<StdMutex<Option<Option<Vec<u8>>>>>,
    }
    impl StepRunner for ReadingRunner {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            let read = step
                .kv
                .get(b"app/marker")
                .await
                .map_err(|e| StepError::permanent(e.to_string()))?
                .map(|b| b.to_vec());
            if step.step_number == 0 {
                step.effects
                    .put("app/marker", b"v".to_vec())
                    .map_err(|e| StepError::permanent(e.to_string()))?;
                let staged_read = step
                    .kv
                    .get(b"app/marker")
                    .await
                    .map_err(|e| StepError::permanent(e.to_string()))?
                    .map(|b| b.to_vec());
                *self.read_under_staging.lock().unwrap() = Some(staged_read);
                return Ok(StepOutcome::continue_now(Vec::new()));
            }
            Ok(StepOutcome::Succeed {
                result: read.unwrap_or_default(),
            })
        }
    }

    let (queue, store) = open_queue().await;
    let read_under_staging = Arc::new(StdMutex::new(None));
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        ReadingRunner {
            read_under_staging: read_under_staging.clone(),
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
    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);
    assert_eq!(outcome.result.as_deref(), Some(b"v".as_slice()));
    assert_eq!(*read_under_staging.lock().unwrap(), Some(None));

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_run_kv_entry_commits_with_its_step_and_reads_within_its_run() {
    struct RunKvRunner;
    impl StepRunner for RunKvRunner {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            let permanent = |e: Error| StepError::permanent(e.to_string());
            if step.step_number == 0 {
                for key in ["b", "a", "c"] {
                    step.effects.put_run_kv(key, key).map_err(permanent)?;
                }
                return Ok(StepOutcome::continue_now(Vec::new()));
            }
            step.effects.delete_run_kv("c").map_err(permanent)?;
            let value = step.kv.get_run_kv(b"a").await.map_err(permanent)?;
            Ok(StepOutcome::Succeed {
                result: value.unwrap_or_default().to_vec(),
            })
        }
    }

    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(queue, store, RunKvRunner)
        .build()
        .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let handle = runtime.submit(RunSpec::default()).await.unwrap();
    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.termination.status, TerminalStatus::Succeeded);
    assert_eq!(outcome.result.as_deref(), Some(b"a".as_slice()));
    let view = runtime.view();
    let entries: Vec<_> = view
        .run_kv_entries(&handle.run_id, KvOrder::Descending, 1)
        .map_ok(|(key, value)| (key, value.to_vec()))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        entries,
        [
            (b"b".to_vec(), b"b".to_vec()),
            (b"a".to_vec(), b"a".to_vec())
        ]
    );
    assert_eq!(
        view.run_kv(&handle.run_id, b"a").await.unwrap().as_deref(),
        Some(b"a".as_slice())
    );
    assert_eq!(view.run_kv(&rid("other"), b"a").await.unwrap(), None);

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_run_memo_written_in_one_step_is_readable_in_the_next() {
    struct JournalRunner;
    impl StepRunner for JournalRunner {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            if step.step_number == 0 {
                step.run_memo.put("journal", b"entry").await?;
                return Ok(StepOutcome::continue_now(Vec::new()));
            }
            let value = step.run_memo.get("journal").await?;
            Ok(StepOutcome::Succeed {
                result: value.unwrap_or_default(),
            })
        }
    }

    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(queue, store, JournalRunner)
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
    assert_eq!(outcome.result.as_deref(), Some(b"entry".as_slice()));

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_fail_verdict_acks_with_its_effects_and_no_dead_letter() {
    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        EffectStagingRunner::new(vec![Ok(StepOutcome::Fail {
            reason: "denied".to_string(),
        })]),
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
    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.run_id, handle.run_id);
    assert_eq!(outcome.termination.status, TerminalStatus::Failed);
    assert_eq!(outcome.termination.error.as_deref(), Some("denied"));
    assert_eq!(
        terminal_status_of(&runtime, &handle.run_id).await,
        Some(outcome.termination.status)
    );
    assert_eq!(wait_for_kv(&queue, b"app/step-0").await, b"done");
    assert_eq!(
        queue.view().stats("workflow-steps").await.unwrap().dead,
        0,
        "a Fail verdict must not dead-letter"
    );

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_cancel_verdict_acks_with_its_effects_and_no_dead_letter() {
    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        EffectStagingRunner::new(vec![Ok(StepOutcome::Cancel {
            reason: "obsolete".to_string(),
        })]),
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
    let outcome = wait_for(&runtime, &handle.run_id).await;
    assert_eq!(outcome.run_id, handle.run_id);
    assert_eq!(outcome.termination.status, TerminalStatus::Cancelled);
    assert_eq!(outcome.termination.error.as_deref(), Some("obsolete"));
    assert_eq!(outcome.termination.cancelled_by, Some(CancelOrigin::Runner));
    assert_eq!(
        terminal_status_of(&runtime, &handle.run_id).await,
        Some(outcome.termination.status)
    );
    assert_eq!(wait_for_kv(&queue, b"app/step-0").await, b"done");
    assert_eq!(
        queue.view().stats("workflow-steps").await.unwrap().dead,
        0,
        "a Cancel verdict must not dead-letter"
    );

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_late_write_through_an_escaped_handle_is_refused() {
    struct EscapingRunner {
        escaped: Arc<StdMutex<Option<EffectsHandle>>>,
    }

    impl StepRunner for EscapingRunner {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            *self.escaped.lock().unwrap() = Some(step.effects.clone());
            Ok(StepOutcome::Succeed { result: Vec::new() })
        }
    }

    let (queue, store) = open_queue().await;
    let escaped = Arc::new(StdMutex::new(None));
    let runtime = WorkflowRuntime::builder(
        queue,
        store,
        EscapingRunner {
            escaped: escaped.clone(),
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
    wait_for(&runtime, &handle.run_id).await;

    let handle = escaped.lock().unwrap().take().unwrap();
    assert!(matches!(
        handle.put("app/late", "v"),
        Err(Error::EffectsSealed)
    ));

    let _ = shutdown.send(());
}

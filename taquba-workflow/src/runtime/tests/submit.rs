use super::*;

#[tokio::test(start_paused = true)]
async fn a_duplicate_submit_is_idempotent_drops_its_effects_and_rejects_a_changed_input() {
    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store, ScriptedRunner::new(vec![]))
        .build()
        .unwrap();
    // No worker loop runs, so the step stays queued and the run is active for
    // every later submit.
    let spec = |input: &[u8], key: &[u8]| RunSpec {
        run_id: Some(rid("fixed-id")),
        input: input.to_vec(),
        effects: SettlementEffects::default().kv_put(key, b"1"),
        ..Default::default()
    };

    let first = runtime.submit(spec(b"x", b"app/first")).await.unwrap();
    assert!(first.newly_submitted);
    assert!(runtime.status(&rid("fixed-id")).await.unwrap().is_some());
    assert_eq!(
        queue.view().kv_get(b"app/first").await.unwrap().as_deref(),
        Some(b"1".as_slice())
    );

    let duplicate = runtime.submit(spec(b"x", b"app/second")).await.unwrap();
    assert_eq!(duplicate.run_id, "fixed-id");
    assert!(!duplicate.newly_submitted);
    assert_eq!(duplicate.job_id, first.job_id);
    assert!(queue.view().kv_get(b"app/second").await.unwrap().is_none());

    let err = runtime.submit(spec(b"y", b"app/third")).await.unwrap_err();
    assert!(matches!(&err, Error::InputMismatch(id) if id == "fixed-id"));
    assert!(err.is_permanent());
    assert!(queue.view().kv_get(b"app/third").await.unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn a_duplicate_known_only_from_the_durable_record_reports_the_current_job() {
    let (queue, store) = open_queue().await;
    // Step 0 continues into a step scheduled an hour out, so the run rests at
    // step 1 with a job the second runtime never saw.
    let first = WorkflowRuntime::builder(
        queue.clone(),
        store.clone(),
        ScriptedRunner::new(vec![StepOutcome::continue_after(
            b"next".to_vec(),
            Duration::from_secs(3600),
        )]),
    )
    .build()
    .unwrap();
    let shutdown = spawn_runtime(first.clone());
    let submitted = first
        .submit(RunSpec {
            run_id: Some(rid("durable")),
            input: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    for _ in 0..200 {
        if queue
            .view()
            .stats("workflow-steps")
            .await
            .unwrap()
            .scheduled
            == 1
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = shutdown.send(());

    let second = WorkflowRuntime::builder(queue.clone(), store, ScriptedRunner::new(vec![]))
        .build()
        .unwrap();
    let duplicate = second
        .submit(RunSpec {
            run_id: Some(rid("durable")),
            input: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!duplicate.newly_submitted);
    assert_ne!(
        duplicate.job_id, submitted.job_id,
        "the pointer moved to step 1"
    );
    let step_1 = queue
        .view()
        .get_job(&duplicate.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(step_1.status, taquba::JobStatus::Scheduled);
    assert_eq!(
        step_1.headers.get(HEADER_STEP).map(String::as_str),
        Some("1")
    );
}

#[tokio::test(start_paused = true)]
async fn a_run_submitted_with_run_at_stays_scheduled_until_then() {
    struct Echo;
    impl StepRunner for Echo {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            Ok(StepOutcome::Succeed {
                result: step.payload.clone(),
            })
        }
    }

    let t0 = 1_700_000_000_000;
    let (queue, store, clock) = open_queue_at(t0).await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store, Echo)
        .build()
        .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    runtime
        .submit(RunSpec {
            input: b"x".to_vec(),
            options: RunOptions {
                run_at: Some(UNIX_EPOCH + Duration::from_millis(t0 + 60_000)),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    let scheduled = queue
        .view()
        .list_jobs("workflow-steps", taquba::JobStatus::Scheduled, None, 10)
        .await
        .unwrap()
        .jobs;
    assert_eq!(scheduled.len(), 1);
    let job_id = scheduled[0].id.clone();

    let waiter = tokio::spawn({
        let queue = queue.clone();
        async move { queue.wait_for_completion(&job_id).await }
    });
    advance(&clock, Duration::from_secs(120)).await;
    assert!(matches!(
        waiter.await.unwrap().unwrap(),
        taquba::WaitOutcome::Done(_),
    ));

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn concurrent_submits_of_one_run_admit_one_and_reject_a_changed_input() {
    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(queue, store.clone(), PauseRunner)
        .build()
        .unwrap();
    let spec = |input: &[u8]| RunSpec {
        run_id: Some(rid("raced")),
        input: input.to_vec(),
        ..Default::default()
    };

    let (first, same, changed) = tokio::join!(
        runtime.submit(spec(b"x")),
        runtime.submit(spec(b"x")),
        runtime.submit(spec(b"y")),
    );
    let first = first.unwrap();
    let same = same.unwrap();
    assert!(first.newly_submitted);
    assert!(!same.newly_submitted);
    assert_eq!(same.job_id, first.job_id);
    assert!(matches!(changed, Err(Error::InputMismatch(id)) if id == "raced"));
}

#[tokio::test(start_paused = true)]
async fn submit_rejects_the_reserved_names_in_its_headers_and_effects() {
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue, store, ScriptedRunner::new(vec![]))
        .build()
        .unwrap();

    let err = runtime
        .submit(RunSpec {
            input: b"x".to_vec(),
            options: RunOptions {
                headers: HashMap::from([("workflow.run_id".to_string(), "evil".to_string())]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::ReservedHeader(k) if k == "workflow.run_id"),
        "got: {err:?}"
    );

    let err = runtime
        .submit(RunSpec {
            input: Vec::new(),
            effects: SettlementEffects::default().enqueue(EnqueueRequest {
                queue: "side".to_string(),
                payload: Vec::new(),
                options: EnqueueOptions::default().headers(HashMap::from([(
                    "workflow.step".to_string(),
                    "0".to_string(),
                )])),
            }),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(&err, Error::ReservedHeader(k) if k == "workflow.step"));

    let err = runtime
        .submit(RunSpec {
            input: Vec::new(),
            effects: SettlementEffects::default().enqueue(EnqueueRequest {
                queue: "workflow-steps".to_string(),
                payload: Vec::new(),
                options: EnqueueOptions::default(),
            }),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(&err, Error::ReservedQueue(q) if q == "workflow-steps"));

    let err = runtime
        .submit(RunSpec {
            input: Vec::new(),
            effects: SettlementEffects::default().kv_put(b"workflow/x", b"v"),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ReservedKvKey(_)));

    let err = runtime
        .submit(RunSpec {
            input: Vec::new(),
            effects: SettlementEffects::default().kv_delete(b"workflow/x"),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ReservedKvKey(_)));

    let err = runtime
        .submit(RunSpec {
            input: Vec::new(),
            effects: SettlementEffects::default().count_up("workflow/groups/g"),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(&err, Error::ReservedCountName(name) if name == "workflow/groups/g"));
}

#[tokio::test(start_paused = true)]
async fn submit_applies_the_deletes_and_expiry_entries_of_the_spec() {
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store, ScriptedRunner::new(vec![]))
        .build()
        .unwrap();
    let index = ExpiryIndex::new(b"app/expiry/".to_vec());
    queue.kv_put(b"app/stale", b"1").await.unwrap();
    // An entry at 9_000 sets the bound of the index, so a pass reads the entry
    // at 8_000 only if the submit records it.
    queue
        .commit_effects(SettlementEffects::default().expiry_entry(&index, 9_000, b"later"))
        .await
        .unwrap();

    runtime
        .submit(RunSpec {
            input: b"x".to_vec(),
            effects: SettlementEffects::default()
                .kv_delete(b"app/stale")
                .expiry_entry(&index, 8_000, b"run"),
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(queue.view().kv_get(b"app/stale").await.unwrap().is_none());
    let mut seen = Vec::new();
    index
        .pass(&queue, 10_000, Duration::ZERO, |at_ms, suffix| {
            seen.push((at_ms, suffix));
            std::future::ready(Expired::Delete(SettlementEffects::default()))
        })
        .await
        .unwrap();
    assert_eq!(seen, [(8_000, b"run".to_vec()), (9_000, b"later".to_vec())]);
}

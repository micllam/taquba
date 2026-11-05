use super::*;

#[tokio::test(start_paused = true)]
async fn a_view_reads_the_same_state_through_the_runtime_and_a_reader() {
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store.clone(), UnreachableRunner)
        .memo_prefix("memo")
        .build()
        .unwrap();
    // The worker loop is not spawned, so the step stays pending.
    let handle = runtime.submit(RunSpec::default()).await.unwrap();

    let reader = QueueReader::open(store.clone(), "test").await.unwrap();
    let view = WorkflowView::new(reader.view().clone(), MemoStore::new(store.clone(), "memo"));
    let through_reader = view.status(&handle.run_id).await.unwrap().expect("active");
    let through_runtime = runtime.status(&handle.run_id).await.unwrap().unwrap();
    assert_eq!(through_reader.run_id, handle.run_id);
    assert_eq!(through_reader.state, RunState::Pending);
    assert_eq!(through_reader.state, through_runtime.state);
    assert_eq!(through_reader.step.number, through_runtime.step.number);
    assert!(view.terminated_run(&handle.run_id).await.unwrap().is_none());
    assert!(view.status(&rid("unknown")).await.unwrap().is_none());

    assert_eq!(
        runtime.cancel(&handle.run_id, None).await.unwrap(),
        CancelRunOutcome::Removed
    );
    let reader = QueueReader::open(store.clone(), "test").await.unwrap();
    let view = WorkflowView::new(reader.view().clone(), MemoStore::new(store, "memo"));
    let status = view
        .status(&handle.run_id)
        .await
        .unwrap()
        .expect("terminal record");
    let termination = RunTermination {
        status: TerminalStatus::Cancelled,
        error: None,
        cancelled_by: Some(CancelOrigin::Request),
        error_kind: None,
        final_step: StepRef {
            number: 0,
            job_id: handle.job_id.clone(),
        },
        terminated_at_ms: 10_000,
    };
    assert_eq!(status.state, RunState::Terminated(termination.clone()));
    let run = view
        .terminated_run(&handle.run_id)
        .await
        .unwrap()
        .expect("terminal record");
    assert_eq!(run.termination, termination);
    assert_eq!(run.result, None);
}

#[tokio::test(start_paused = true)]
async fn the_listing_pages_the_runs_in_run_id_order_and_a_run_reads_as_it_lists() {
    let (queue, store, clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store.clone(), UnreachableRunner)
        .memo_prefix("memo")
        .build()
        .unwrap();
    // The worker loop is not spawned, so the steps stay pending.
    for run_id in ["c", "a", "b"] {
        runtime
            .submit(RunSpec {
                run_id: Some(rid(run_id)),
                options: RunOptions {
                    headers: HashMap::from([("tenant".to_string(), run_id.to_string())]),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .unwrap();
        clock.advance(Duration::from_secs(1));
    }
    assert_eq!(
        runtime.cancel(&rid("b"), None).await.unwrap(),
        CancelRunOutcome::Removed
    );

    let reader = QueueReader::open(store.clone(), "test").await.unwrap();
    let view = WorkflowView::new(reader.view().clone(), MemoStore::new(store, "memo"));
    let columns = |run: crate::ListedRun| {
        (
            run.status.run_id.to_string(),
            matches!(run.status.state, RunState::Terminated(_)),
            run.submitted_at_ms,
            run.headers["tenant"].clone(),
        )
    };
    let list = |filter, order| {
        view.runs(filter, order, 1)
            .map_ok(columns)
            .try_collect::<Vec<_>>()
    };
    let row = |id: &str, terminated, submitted_at_ms| {
        (id.to_string(), terminated, submitted_at_ms, id.to_string())
    };
    let (a, b, c) = (
        row("a", false, 11_000),
        row("b", true, 12_000),
        row("c", false, 10_000),
    );
    assert_eq!(
        list(RunFilter::All, KvOrder::Ascending).await.unwrap(),
        [a.clone(), b.clone(), c.clone()]
    );
    assert_eq!(
        list(RunFilter::All, KvOrder::Descending).await.unwrap(),
        [c.clone(), b.clone(), a.clone()]
    );
    for row in [&a, &b, &c] {
        let run = view.run(&rid(&row.0)).await.unwrap().map(columns);
        assert_eq!(run.as_ref(), Some(row));
    }
    assert_eq!(
        list(RunFilter::Active, KvOrder::Ascending).await.unwrap(),
        [a, c]
    );
    assert_eq!(
        list(RunFilter::Terminated, KvOrder::Ascending)
            .await
            .unwrap(),
        [b]
    );
}

#[tokio::test(start_paused = true)]
async fn a_run_that_terminates_during_the_listing_lists_once() {
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue, store, UnreachableRunner)
        .build()
        .unwrap();
    for run_id in ["a", "b"] {
        runtime
            .submit(RunSpec {
                run_id: Some(rid(run_id)),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let view = runtime.view();
    let mut runs = std::pin::pin!(view.runs(RunFilter::All, KvOrder::Ascending, 1));
    let first = runs.try_next().await.unwrap().unwrap();
    assert_eq!(first.status.run_id, "a");
    runtime.cancel(&rid("b"), None).await.unwrap();
    let rest: Vec<_> = runs.try_collect().await.unwrap();
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].status.run_id, "b");
    assert!(matches!(rest[0].status.state, RunState::Terminated(_)));
}

#[tokio::test(start_paused = true)]
async fn generated_run_ids_list_in_submission_order_at_the_time_of_the_queue_clock() {
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue, store, UnreachableRunner)
        .build()
        .unwrap();
    // The clock stays in one millisecond, where ULIDs with fresh random bits
    // sort at random.
    let mut submitted = Vec::new();
    for _ in 0..10 {
        let outcome = runtime.submit(RunSpec::default()).await.unwrap();
        submitted.push(outcome.run_id.to_string());
    }

    let listed: Vec<String> = runtime
        .view()
        .runs(RunFilter::All, KvOrder::Ascending, 10)
        .map_ok(|run| run.status.run_id.to_string())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(listed, submitted);
    for run_id in &listed {
        let generated = ulid::Ulid::from_string(run_id).unwrap();
        assert_eq!(generated.timestamp_ms(), 10_000);
    }
}

#[tokio::test(start_paused = true)]
async fn the_status_of_a_run_does_not_read_its_offloaded_step_payload() {
    let payloads: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (queue, store, _clock) = open_queue_at_with(
        10_000,
        OpenOptions::default()
            .payload_offload_threshold(64)
            .payload_store(payloads.clone()),
    )
    .await;
    let runtime = WorkflowRuntime::builder(queue, store, UnreachableRunner)
        .memo_prefix("memo")
        .build()
        .unwrap();
    let handle = runtime
        .submit(RunSpec {
            input: vec![7u8; 512],
            ..Default::default()
        })
        .await
        .unwrap();

    // A read that fetches the step's payload object fails once the object is
    // deleted.
    let objects: Vec<_> = payloads.list(None).try_collect().await.unwrap();
    assert!(!objects.is_empty(), "the step payload is offloaded");
    for object in objects {
        payloads.delete(&object.location).await.unwrap();
    }
    let status = runtime.status(&handle.run_id).await.unwrap().unwrap();
    assert_eq!(status.state, RunState::Pending);
}

#[tokio::test(start_paused = true)]
async fn a_pointer_over_a_missing_job_is_an_inconsistent_run_state() {
    let (queue, store) = open_queue().await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store, UnreachableRunner)
        .build()
        .unwrap();
    let submitted = runtime
        .submit(RunSpec {
            run_id: Some(rid("torn")),
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    // The step job is removed without the runtime, so the pointer outlives it.
    queue.cancel(&submitted.job_id).await.unwrap();
    assert!(matches!(
        runtime.status(&rid("torn")).await,
        Err(Error::InconsistentRunState(id)) if id == "torn"
    ));
    assert!(matches!(
        runtime.wait(&rid("torn")).await,
        Err(Error::InconsistentRunState(id)) if id == "torn"
    ));
    assert!(matches!(
        runtime.cancel(&rid("torn"), None).await,
        Err(Error::InconsistentRunState(id)) if id == "torn"
    ));
}

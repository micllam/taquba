use super::*;

#[tokio::test(start_paused = true)]
async fn a_run_dead_lettered_by_the_reaper_is_terminated_by_reconciliation() {
    let (queue, store, clock) = open_queue_at_with(
        1_700_000_000_000,
        fast_options().default_queue_config(
            QueueConfig::default()
                .retry_backoff_base(Duration::ZERO)
                .lease_duration(Duration::from_secs(1)),
        ),
    )
    .await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store, PauseRunner)
        .poll_interval(Duration::from_millis(10))
        .build()
        .unwrap();
    let shutdown = spawn_runtime(runtime.clone());

    let submitted = runtime
        .submit(RunSpec {
            run_id: Some(rid("hung")),
            input: Vec::new(),
            options: RunOptions {
                max_attempts_per_step: Some(1),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    for _ in 0..200 {
        if queue.view().stats("workflow-steps").await.unwrap().claimed == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        runtime.status(&rid("hung")).await.unwrap().map(|s| s.state),
        Some(RunState::Running)
    );

    // The lease expires past the attempt limit: the reaper dead-letters the
    // step outside the worker, without a worker in the loop.
    advance(&clock, Duration::from_secs(2)).await;
    let outcome = wait_for(&runtime, &rid("hung")).await;
    assert_eq!(outcome.run_id, "hung");
    assert_eq!(outcome.termination.status, TerminalStatus::Failed);
    assert_eq!(outcome.termination.final_step.number, 0);
    assert_eq!(
        queue
            .view()
            .get_job(&submitted.job_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Dead
    );
    assert!(
        runtime
            .view()
            .run_record(&rid("hung"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        queue
            .view()
            .kv_get(&step_kv_key(&rid("hung")))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        terminal_status_of(&runtime, &rid("hung")).await,
        Some(TerminalStatus::Failed)
    );
    let _ = shutdown.send(());

    // The test removes the terminal record as the memo sweep does. The run id
    // is then submitted again while the dead job is retained: the pass
    // identifies a dead-letter outside the worker by the current step's job, so
    // the new run is left alone.
    queue.kv_delete(&run_kv_key(&rid("hung"))).await.unwrap();
    let again = runtime
        .submit(RunSpec {
            run_id: Some(rid("hung")),
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(again.newly_submitted);
    assert_eq!(runtime.inner.core.reconcile_dead_steps().await.unwrap(), 0);
    assert!(
        runtime.status(&rid("hung")).await.unwrap().is_some(),
        "the re-submitted run is active"
    );
}

#[tokio::test(start_paused = true)]
async fn a_step_dead_lettered_outside_the_worker_is_waited_for_until_reconciliation() {
    let (queue, store, _clock) = open_queue_at(10_000).await;
    let runtime = WorkflowRuntime::builder(queue.clone(), store, UnreachableRunner)
        .poll_interval(Duration::from_millis(10))
        .build()
        .unwrap();
    runtime
        .submit(RunSpec {
            run_id: Some(rid("hung")),
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    let claim = queue
        .claim("workflow-steps", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    queue.dead_letter(&claim, "hung").await.unwrap();

    let waiting = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.wait(&rid("hung")).await }
    });
    assert_eq!(
        runtime.cancel(&rid("hung"), None).await.unwrap(),
        CancelRunOutcome::NotActive,
        "the cancel does not apply to a dead run"
    );
    assert_eq!(
        runtime.status(&rid("hung")).await.unwrap().unwrap().state,
        RunState::DeadLettered,
        "the dead step takes precedence over the cancellation request"
    );
    assert!(
        runtime
            .wait_timeout(&rid("hung"), Duration::from_secs(1))
            .await
            .unwrap()
            .is_none(),
        "nothing terminates the run without a worker"
    );
    assert_eq!(runtime.inner.core.reconcile_dead_steps().await.unwrap(), 1);
    let end = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the wait resolved")
        .unwrap()
        .unwrap();
    assert_eq!(
        end.termination,
        RunTermination {
            status: TerminalStatus::Failed,
            error: Some("hung".into()),
            cancelled_by: None,
            error_kind: None,
            final_step: StepRef {
                number: 0,
                job_id: claim.id.clone(),
            },
            terminated_at_ms: 10_000,
        },
        "the termination is read from the terminal record",
    );
    assert_eq!(end.result, None);
    assert_eq!(
        runtime.wait(&rid("hung")).await.unwrap(),
        end,
        "the record is retained without memo retention",
    );
}

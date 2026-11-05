use super::*;

type ObservedSignals = Arc<StdMutex<Vec<Option<Vec<u8>>>>>;

/// Two-step runner: step 0 continues with a signal wait, step 1 records
/// [`Step::signal`] and succeeds. With `fail_first`, the first attempt of step
/// 0 fails transiently. Every attempt of step 0 records its
/// [`Delivery::next_job_id`] in `wait_ids`.
struct SignalProbe {
    timeout: Duration,
    fail_first: bool,
    observed: ObservedSignals,
    wait_ids: Arc<StdMutex<Vec<String>>>,
}

impl StepRunner for SignalProbe {
    async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
        if step.step_number == 0 {
            self.wait_ids.lock().unwrap().push(step.next_job_id.clone());
            if self.fail_first && step.attempts == 1 {
                return Err(StepError::transient("first attempt"));
            }
            Ok(StepOutcome::continue_on_signal(Vec::new(), self.timeout))
        } else {
            self.observed.lock().unwrap().push(step.signal.clone());
            Ok(StepOutcome::Succeed { result: Vec::new() })
        }
    }
}

async fn wait_for_scheduled(queue: &Queue, count: i64) {
    for _ in 0..200 {
        if queue
            .view()
            .stats("workflow-steps")
            .await
            .unwrap()
            .scheduled
            == count
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("scheduled count never reached {count}");
}

/// The job id of the step that `run_id` waits at.
async fn waiting_job_id<R: StepRunner>(runtime: &WorkflowRuntime<R>, run_id: &RunId) -> String {
    let status = runtime.status(run_id).await.unwrap().unwrap();
    assert!(
        matches!(status.state, RunState::Waiting { .. }),
        "run is not waiting: {:?}",
        status.state
    );
    status.step.job_id
}

struct ProbeRuntime {
    runtime: WorkflowRuntime<SignalProbe>,
    observed: ObservedSignals,
    wait_ids: Arc<StdMutex<Vec<String>>>,
}

fn signal_probe_runtime(
    queue: Arc<Queue>,
    store: Arc<dyn taquba::object_store::ObjectStore>,
    timeout: Duration,
    fail_first: bool,
) -> ProbeRuntime {
    let observed = Arc::new(StdMutex::new(Vec::new()));
    let wait_ids = Arc::new(StdMutex::new(Vec::new()));
    let runtime = WorkflowRuntime::builder(
        queue,
        store,
        SignalProbe {
            timeout,
            fail_first,
            observed: observed.clone(),
            wait_ids: wait_ids.clone(),
        },
    )
    .build()
    .unwrap();
    ProbeRuntime {
        runtime,
        observed,
        wait_ids,
    }
}

#[tokio::test(start_paused = true)]
async fn signal_wakes_waiting_run_early_with_payload() {
    let (queue, store, _clock) = open_queue_at(1_700_000_000_000).await;
    let probe = signal_probe_runtime(queue.clone(), store, Duration::from_secs(3600), false);
    let shutdown = spawn_runtime(probe.runtime.clone());

    let handle = probe
        .runtime
        .submit(RunSpec {
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    wait_for_scheduled(&queue, 1).await;
    let job_id = waiting_job_id(&probe.runtime, &handle.run_id).await;
    assert_eq!(
        probe
            .runtime
            .status(&handle.run_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        RunState::Waiting {
            timeout_at_ms: 1_700_000_000_000 + 3_600_000,
        }
    );

    let outcome = queue.signal(&job_id, b"paid".to_vec()).await.unwrap();
    assert_eq!(outcome, SignalOutcome::Delivered);

    // The run completes without the timeout elapsing on the mock clock.
    let terminal = wait_for(&probe.runtime, &handle.run_id).await;
    assert_eq!(terminal.termination.status, TerminalStatus::Succeeded);
    assert_eq!(
        probe.observed.lock().unwrap().as_slice(),
        &[Some(b"paid".to_vec())]
    );
    assert_eq!(
        queue.signal(&job_id, b"again".to_vec()).await.unwrap(),
        SignalOutcome::NotWaiting
    );

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn next_job_id_is_the_same_on_every_attempt_and_is_the_id_of_the_wait() {
    let (queue, store, _clock) = open_queue_at_with(1_700_000_000_000, fast_options()).await;
    let probe = signal_probe_runtime(queue.clone(), store, Duration::from_secs(3600), true);
    let shutdown = spawn_runtime(probe.runtime.clone());

    let handle = probe
        .runtime
        .submit(RunSpec {
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    wait_for_scheduled(&queue, 1).await;
    let job_id = waiting_job_id(&probe.runtime, &handle.run_id).await;
    let wait_ids = probe.wait_ids.lock().unwrap().clone();
    assert_eq!(wait_ids, vec![job_id.clone(), job_id]);

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_run_waiting_on_a_signal_survives_a_close_and_reopen() {
    let store: Arc<dyn taquba::object_store::ObjectStore> = Arc::new(InMemory::new());
    let open = |store: Arc<dyn taquba::object_store::ObjectStore>| async move {
        Arc::new(
            Queue::open_with_options(
                store,
                "test",
                OpenOptions::default().clock(Arc::new(MockClock::new(1_700_000_000_000))),
            )
            .await
            .unwrap(),
        )
    };

    let queue = open(store.clone()).await;
    let probe = signal_probe_runtime(
        queue.clone(),
        store.clone(),
        Duration::from_secs(3600),
        false,
    );
    let runtime = probe.runtime;
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let worker = tokio::spawn({
        let runtime = runtime.clone();
        async move {
            runtime
                .run(async move {
                    let _ = stop_rx.await;
                })
                .await
        }
    });
    let handle = runtime
        .submit(RunSpec {
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    wait_for_scheduled(&queue, 1).await;
    let _ = stop_tx.send(());
    worker.await.unwrap().unwrap();
    drop(runtime);
    Arc::into_inner(queue)
        .expect("no other queue references at close")
        .close()
        .await
        .unwrap();

    let queue = open(store.clone()).await;
    assert_eq!(
        queue
            .view()
            .stats("workflow-steps")
            .await
            .unwrap()
            .scheduled,
        1
    );

    let probe = signal_probe_runtime(queue.clone(), store, Duration::from_secs(3600), false);
    let shutdown = spawn_runtime(probe.runtime.clone());
    let job_id = waiting_job_id(&probe.runtime, &handle.run_id).await;

    let delivery = queue.signal(&job_id, b"approved".to_vec()).await.unwrap();
    assert_eq!(delivery, SignalOutcome::Delivered);

    let terminal = wait_for(&probe.runtime, &handle.run_id).await;
    assert_eq!(terminal.termination.status, TerminalStatus::Succeeded);
    assert_eq!(
        probe.observed.lock().unwrap().as_slice(),
        &[Some(b"approved".to_vec())]
    );
    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn signal_timeout_delivers_none_and_rejects_a_late_signal() {
    let (queue, store, clock) = open_queue_at(1_700_000_000_000).await;
    let probe = signal_probe_runtime(queue.clone(), store, Duration::from_secs(60), false);
    let shutdown = spawn_runtime(probe.runtime.clone());

    let handle = probe
        .runtime
        .submit(RunSpec {
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    wait_for_scheduled(&queue, 1).await;
    let job_id = waiting_job_id(&probe.runtime, &handle.run_id).await;

    advance(&clock, Duration::from_secs(61)).await;
    queue.promote_scheduled_now().await.unwrap();

    let terminal = wait_for(&probe.runtime, &handle.run_id).await;
    assert_eq!(terminal.termination.status, TerminalStatus::Succeeded);
    assert_eq!(probe.observed.lock().unwrap().as_slice(), &[None]);
    assert_eq!(
        queue.signal(&job_id, b"late".to_vec()).await.unwrap(),
        SignalOutcome::NotWaiting
    );

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_wake_of_a_waiting_step_delivers_none() {
    let (queue, store, _clock) = open_queue_at(1_700_000_000_000).await;
    let probe = signal_probe_runtime(queue.clone(), store, Duration::from_secs(3600), false);
    let shutdown = spawn_runtime(probe.runtime.clone());

    let handle = probe
        .runtime
        .submit(RunSpec {
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    wait_for_scheduled(&queue, 1).await;
    let job_id = waiting_job_id(&probe.runtime, &handle.run_id).await;

    assert_eq!(
        queue.wake_scheduled(&job_id).await.unwrap(),
        WakeOutcome::Woken
    );

    let terminal = wait_for(&probe.runtime, &handle.run_id).await;
    assert_eq!(terminal.termination.status, TerminalStatus::Succeeded);
    assert_eq!(probe.observed.lock().unwrap().as_slice(), &[None]);

    let _ = shutdown.send(());
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_wait_rejects_a_signal() {
    let (queue, store, _clock) = open_queue_at(1_700_000_000_000).await;
    let probe = signal_probe_runtime(queue.clone(), store, Duration::from_secs(3600), false);
    let shutdown = spawn_runtime(probe.runtime.clone());

    let handle = probe
        .runtime
        .submit(RunSpec {
            input: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    wait_for_scheduled(&queue, 1).await;
    let job_id = waiting_job_id(&probe.runtime, &handle.run_id).await;

    assert_eq!(
        probe.runtime.cancel(&handle.run_id, None).await.unwrap(),
        CancelRunOutcome::Removed
    );
    let terminal = wait_for(&probe.runtime, &handle.run_id).await;
    assert_eq!(terminal.termination.status, TerminalStatus::Cancelled);
    assert_eq!(
        queue.signal(&job_id, b"orphan".to_vec()).await.unwrap(),
        SignalOutcome::NotWaiting
    );

    let _ = shutdown.send(());
}

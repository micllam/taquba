//! Owned work: the runs submitted with [`RunOptions::owner`]. Every step job of
//! an owned run has the header [`HEADER_OWNER`], and the submission writes an
//! index entry of the run under the owner's submission id and the queue of the
//! run's runtime. The owner's queues are one key, which each submission to a
//! new queue rewrites under a compare.
//!
//! An owned submission is fenced on the claim of the owner's step, so a step
//! whose claim has ended cannot submit. When the step returns, the runtime
//! seals its owner after the submissions in flight have committed. The
//! termination of the owner follows the seal or the end of the claim, so it
//! reads every queue with owned runs and enqueues a cancel job with
//! [`HEADER_OWNED`] on each. The runtime of the queue cancels the owned runs of
//! its index entries, and the termination of each owned run deletes its entry.
//!
//! [`RunOptions::owner`]: crate::RunOptions::owner

use std::fmt;
use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};
use taquba::{
    ClaimFence, EnqueueOptions, EnqueueRequest, JobRecord, KvCompare, KvOrder, Queue,
    SettlementEffects, WorkerError,
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tracing::warn;

use crate::durable::{self, Submission};
use crate::error::{Error, Result, worker_error};
use crate::keys::{
    OWNED_KV_PREFIX, RunId, owned_run_kv_key, owned_runs_range, owner_queues_kv_key,
};
use crate::runtime::RuntimeCore;
use crate::terminal::CancelOrigin;

/// Header key identifying the owner of a run and its submission id, set on
/// every step job of an owned run.
pub(crate) const HEADER_OWNER: &str = "workflow.owner";
/// Header key marking the job that cancels the owned runs of an owner's
/// submission in the runtime of the job's queue.
pub(crate) const HEADER_OWNED: &str = "workflow.owned";

/// The reason of the cancel of an owned run whose owner terminated.
pub(crate) const OWNER_TERMINATED: &str = "the owner run terminated";

/// The cancels of owned runs that a cancel job keeps in flight.
const CANCEL_CONCURRENCY: usize = 100;

/// The index entries of owned runs read per page.
const OWNED_PAGE_SIZE: usize = 256;

/// The id of one submission of a run id: the job id of its step 0, a ULID, as
/// its 16 bytes. A run id identifies a new run after the records of the earlier
/// one are removed, and the submission id tells the two apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct SubmissionId([u8; 16]);

impl SubmissionId {
    /// The submission id of the run whose step 0 is the job `job_id`, an id
    /// from [`Queue::next_job_id`].
    pub(crate) fn of_job(job_id: &str) -> Self {
        let id = ulid::Ulid::from_string(job_id).expect("`Queue::next_job_id` returns a ULID");
        Self(id.to_bytes())
    }

    /// The submission id of [`crate::Delivery::detached`].
    pub(crate) fn detached() -> Self {
        Self([0; 16])
    }

    fn parse(text: &str) -> Option<Self> {
        ulid::Ulid::from_string(text)
            .ok()
            .map(|id| Self(id.to_bytes()))
    }
}

impl fmt::Display for SubmissionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ulid::Ulid::from_bytes(self.0))
    }
}

/// An owner run as the index, the headers and a group's manifest store it: its
/// run id and submission id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OwnerRef {
    pub(crate) run_id: RunId,
    pub(crate) submission_id: SubmissionId,
}

impl OwnerRef {
    /// The value of [`HEADER_OWNER`] and [`HEADER_OWNED`]: `{run_id}:{ulid}`. A
    /// run id does not contain `:`.
    pub(crate) fn header_value(&self) -> String {
        format!("{}:{}", self.run_id, self.submission_id)
    }

    /// The owner of a header value from [`Self::header_value`].
    pub(crate) fn from_header(value: &str) -> Result<Self> {
        let invalid = || Error::InvalidStepHeader {
            header: HEADER_OWNER,
            value: value.to_string(),
        };
        let (run_id, submission_id) = value.split_once(':').ok_or_else(invalid)?;
        Ok(Self {
            run_id: RunId::new(run_id)?,
            submission_id: SubmissionId::parse(submission_id).ok_or_else(invalid)?,
        })
    }

    /// The dedup key of the cancel job of this owner on a queue.
    fn cancel_dedup_key(&self) -> String {
        format!("owned:{}", self.header_value())
    }
}

/// The run of a step as the owner of the runs submitted with it in
/// [`RunOptions::owner`](crate::RunOptions::owner), from
/// [`Delivery::owner`](crate::Delivery::owner). The owner's termination cancels
/// its owned runs. An owner is valid while its step runs: a submission with it
/// fails with [`Error::OwnerStepEnded`] once the step has returned or its claim
/// has ended, and with [`Error::OwnerOfAnotherQueue`] on a runtime over another
/// [`Queue`].
#[derive(Clone)]
pub struct Owner {
    pub(crate) run: OwnerRef,
    /// The queue of the owner's runtime, absent for a detached delivery.
    pub(crate) queue: Option<Arc<Queue>>,
    /// The step that submits with the owner, absent for a detached delivery and
    /// for the stored owner of a group.
    pub(crate) step: Option<OwnerStep>,
}

impl Owner {
    /// The run id of the owner.
    pub fn run_id(&self) -> &RunId {
        &self.run.run_id
    }
}

impl fmt::Debug for Owner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Owner")
            .field("run_id", &self.run.run_id)
            .field("submission_id", &self.run.submission_id)
            .finish_non_exhaustive()
    }
}

/// The step of an [`Owner`]: the fence of its claim and its seal, which the
/// runtime closes when the step returns.
#[derive(Clone)]
pub(crate) struct OwnerStep {
    fence: ClaimFence,
    sealed: Arc<RwLock<bool>>,
}

impl OwnerStep {
    pub(crate) fn new(fence: ClaimFence) -> Self {
        Self {
            fence,
            sealed: Arc::new(RwLock::new(false)),
        }
    }

    /// Seal the step once the submissions in flight have committed.
    pub(crate) async fn seal(&self) {
        *self.sealed.write().await = true;
    }
}

impl Submission {
    /// The run `run_id` of this submission as an owner.
    pub(crate) fn owner(&self, run_id: &RunId) -> OwnerRef {
        OwnerRef {
            run_id: run_id.clone(),
            submission_id: self.submission_id,
        }
    }
}

/// The queues with owned runs of an owner's submission.
pub(crate) struct OwnedQueues {
    pub(crate) owner: OwnerRef,
    queues: Vec<String>,
}

impl RuntimeCore {
    /// The queues with owned runs of `owner`, and the stored value of their
    /// key.
    async fn read_owned_queues(&self, owner: &OwnerRef) -> Result<(Vec<String>, KvCompare)> {
        let key = owner_queues_kv_key(owner);
        Ok(match self.queue.view().kv_get(&key).await? {
            Some(stored) => (durable::decode(&stored)?, KvCompare::value(key, stored)),
            None => (Vec::new(), KvCompare::absent(key)),
        })
    }

    /// The queues with owned runs of `owner`.
    pub(crate) async fn owned_queues(&self, owner: OwnerRef) -> Result<OwnedQueues> {
        let (queues, _) = self.read_owned_queues(&owner).await?;
        Ok(OwnedQueues { owner, queues })
    }

    /// Add the effects of the termination of an owner to `effects`: a cancel
    /// job on each queue of `owned`.
    pub(crate) fn owner_termination_effects(
        &self,
        effects: SettlementEffects,
        owned: &OwnedQueues,
    ) -> SettlementEffects {
        let header = owned.owner.header_value();
        owned.queues.iter().fold(effects, |effects, queue| {
            effects.enqueue(EnqueueRequest {
                queue: queue.clone(),
                payload: Vec::new(),
                options: EnqueueOptions::default()
                    .headers([(HEADER_OWNED.to_string(), header.clone())].into())
                    .dedup_key(Some(owned.owner.cancel_dedup_key())),
            })
        })
    }

    /// Open a submission with `owner`: a guard that the seal of the owner's
    /// step waits for, kept until the submission has committed. Fails with
    /// [`Error::OwnerOfAnotherQueue`] for an owner of another queue and with
    /// [`Error::OwnerStepEnded`] for an owner without a running step.
    pub(crate) async fn open_owned_submission(
        &self,
        owner: &Owner,
    ) -> Result<OwnedRwLockReadGuard<bool>> {
        if !owner
            .queue
            .as_ref()
            .is_some_and(|queue| Arc::ptr_eq(queue, &self.queue))
        {
            return Err(Error::OwnerOfAnotherQueue(owner.run.run_id.clone()));
        }
        let ended = || Error::OwnerStepEnded(owner.run.run_id.clone());
        let step = owner.step.as_ref().ok_or_else(ended)?;
        let sealed = step.sealed.clone().read_owned().await;
        if *sealed {
            return Err(ended());
        }
        Ok(sealed)
    }

    /// Add the effects of the submission of the run `run_id` owned by `owner`
    /// to `effects`: the fence of the owner's step, the index entry of the run
    /// and, for the first owned run on this runtime's queue, the rewritten key
    /// of the owner's queues under a compare. A submission must open with
    /// [`Self::open_owned_submission`] first.
    pub(crate) async fn owned_submission_effects(
        &self,
        effects: SettlementEffects,
        owner: &Owner,
        run_id: &RunId,
    ) -> Result<SettlementEffects> {
        let step = owner
            .step
            .as_ref()
            .ok_or_else(|| Error::OwnerStepEnded(owner.run.run_id.clone()))?;
        let mut effects = effects.claim_fence(step.fence.clone()).kv_put(
            owned_run_kv_key(&owner.run, &self.queue_name, run_id),
            Vec::new(),
        );
        let (mut queues, compare) = self.read_owned_queues(&owner.run).await?;
        if !queues.contains(&self.queue_name) {
            let key = compare.key.clone();
            queues.push(self.queue_name.clone());
            effects = effects
                .kv_compare(compare)
                .kv_put(key, durable::encode(&queues));
        }
        Ok(effects)
    }

    /// Process a cancel job with [`HEADER_OWNED`]: cancel each run of the index
    /// entries of the owner on this runtime's queue. A failed cancel leaves the
    /// others to run, and the job returns the first error after the scan, so a
    /// retry cancels the rest.
    pub(crate) async fn cancel_owned(
        &self,
        job: &JobRecord,
        header: &str,
    ) -> std::result::Result<SettlementEffects, WorkerError> {
        let owner = OwnerRef::from_header(header).map_err(worker_error)?;
        let run_ids: Vec<RunId> = self
            .queue
            .view()
            .kv_entries(
                OWNED_KV_PREFIX,
                owned_runs_range(&owner, &self.queue_name),
                KvOrder::Ascending,
                OWNED_PAGE_SIZE,
            )
            .map_ok(|(key, _)| key)
            .try_collect::<Vec<_>>()
            .await
            .map_err(|err| worker_error(Error::from(err)))?
            .iter()
            .filter_map(|key| {
                let run_id = key.rsplit(|b| *b == b'/').next()?;
                RunId::new(String::from_utf8_lossy(run_id).into_owned()).ok()
            })
            .collect();
        let mut first_error = None;
        let mut cancels = stream::iter(run_ids)
            .map(|run_id| async move {
                let result = self
                    .cancel_run_with(
                        &run_id,
                        Some(OWNER_TERMINATED.to_string()),
                        CancelOrigin::Owner,
                    )
                    .await;
                (run_id, result)
            })
            .buffer_unordered(CANCEL_CONCURRENCY);
        while let Some((run_id, result)) = cancels.next().await {
            if let Err(err) = result {
                warn!(job_id = %job.id, run_id = %run_id, "cancel of an owned run failed: {err}");
                first_error.get_or_insert(err);
            }
        }
        match first_error {
            Some(err) => Err(worker_error(err)),
            None => Ok(SettlementEffects::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::{Duration, UNIX_EPOCH};

    use taquba::object_store::ObjectStore;

    use super::*;
    use crate::group::{GroupMember, MemberResult};
    use crate::runner::{Delivery, Step, StepError, StepOutcome, StepRunner};
    use crate::runtime::{RunOptions, RunSpec, WorkflowRuntime};
    use crate::terminal::TerminalStatus;
    use crate::test_util::{open_queue_at, rid};
    use crate::view::RunState;

    /// Succeeds at its first step.
    struct Succeeds;

    impl StepRunner for Succeeds {
        async fn run_step(&self, _: &Step) -> std::result::Result<StepOutcome, StepError> {
            Ok(StepOutcome::Succeed { result: Vec::new() })
        }
    }

    /// The owned work of a step of [`Owning`].
    #[derive(Clone, Copy)]
    enum Work {
        /// The run `owned`.
        Run,
        /// The group `g` with the members `a` and `b`.
        Group,
    }

    /// How step 0 of [`Owning`] ends.
    #[derive(Clone, Copy)]
    enum End {
        Succeed,
        /// Waits for the group `g`.
        WaitForGroup,
        /// Continues after an hour.
        Wait,
    }

    /// Submits `work` on `members` with the owner of step 0, keeps the owner
    /// and ends step 0 with `end`. Step 1 succeeds.
    struct Owning {
        members: WorkflowRuntime<Succeeds>,
        work: Work,
        end: End,
        kept: Arc<Mutex<Option<Owner>>>,
    }

    impl StepRunner for Owning {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            if step.step_number > 0 {
                return Ok(StepOutcome::Succeed { result: Vec::new() });
            }
            *self.kept.lock().unwrap() = Some(step.owner.clone());
            let options = owned(&step.owner);
            match self.work {
                Work::Run => {
                    self.members
                        .submit(RunSpec {
                            run_id: Some(rid("owned")),
                            options,
                            ..RunSpec::default()
                        })
                        .await?;
                }
                Work::Group => {
                    self.members
                        .group(rid("g"))
                        .submit(vec![member("a"), member("b")], &options)
                        .await?;
                }
            }
            let hour = Duration::from_secs(3_600);
            Ok(match self.end {
                End::Succeed => StepOutcome::Succeed { result: Vec::new() },
                End::WaitForGroup => StepOutcome::continue_on_group(Vec::new(), rid("g"), hour),
                End::Wait => StepOutcome::continue_after(Vec::new(), hour),
            })
        }
    }

    fn members_runtime(
        queue: &Arc<Queue>,
        store: &Arc<dyn ObjectStore>,
    ) -> WorkflowRuntime<Succeeds> {
        WorkflowRuntime::builder(queue.clone(), store.clone(), Succeeds)
            .queue_name("members")
            .poll_interval(Duration::from_millis(10))
            .build()
            .unwrap()
    }

    /// The runtime of the owners on `queue`, whose runs submit `work` on
    /// `members`, and the owner that the step kept.
    fn owners_runtime(
        queue: &Arc<Queue>,
        store: &Arc<dyn ObjectStore>,
        members: &WorkflowRuntime<Succeeds>,
        work: Work,
        end: End,
    ) -> (WorkflowRuntime<Owning>, Arc<Mutex<Option<Owner>>>) {
        let kept = Arc::new(Mutex::new(None));
        let runner = Owning {
            members: members.clone(),
            work,
            end,
            kept: kept.clone(),
        };
        let owners = WorkflowRuntime::builder(queue.clone(), store.clone(), runner)
            .queue_name("owners")
            .poll_interval(Duration::from_millis(10))
            .memo_retention(Duration::from_secs(60))
            .build()
            .unwrap();
        (owners, kept)
    }

    /// Submit an owner and run its step 0, which ends with a wait.
    async fn run_owner_step(owners: &WorkflowRuntime<Owning>) -> RunId {
        let owner_id = owners.submit(RunSpec::default()).await.unwrap().run_id;
        let worker = owners.spawn(std::future::pending::<()>());
        tokio::time::timeout(Duration::from_secs(10), async {
            while owners.status(&owner_id).await.unwrap().unwrap().step.number == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("step 0 settled in time");
        worker.shutdown().await.unwrap();
        owner_id
    }

    /// The options of a run owned by `owner`, whose step 0 waits for an hour,
    /// so that only a cancel ends it.
    fn owned(owner: &Owner) -> RunOptions {
        RunOptions {
            owner: Some(owner.clone()),
            run_at: Some(UNIX_EPOCH + Duration::from_secs(3_600)),
            ..RunOptions::default()
        }
    }

    fn member(key: &str) -> GroupMember {
        GroupMember {
            key: key.to_string(),
            input: Vec::new(),
            max_attempts: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancel_of_an_owner_cancels_its_owned_group_on_another_queue() {
        let (queue, store, _clock) = open_queue_at(10_000).await;
        let members = members_runtime(&queue, &store);
        let (owners, kept) =
            owners_runtime(&queue, &store, &members, Work::Group, End::WaitForGroup);
        let owner_id = run_owner_step(&owners).await;

        owners.cancel(&owner_id, None).await.unwrap();
        let group = members.group(rid("g"));
        let worker = members.spawn(std::future::pending::<()>());
        let results: Vec<MemberResult> = tokio::time::timeout(Duration::from_secs(10), async {
            group.results().await.unwrap().try_collect().await
        })
        .await
        .expect("the members terminated in time")
        .unwrap();
        worker.shutdown().await.unwrap();

        assert_eq!(results.len(), 2);
        for result in &results {
            let termination = &result.outcome.termination;
            assert_eq!(termination.status, TerminalStatus::Cancelled);
            assert_eq!(termination.cancelled_by, Some(CancelOrigin::Owner));
        }
        let owner = kept.lock().unwrap().clone().unwrap();
        let entries: Vec<_> = queue
            .view()
            .kv_entries(
                OWNED_KV_PREFIX,
                owned_runs_range(&owner.run, "members"),
                KvOrder::Ascending,
                OWNED_PAGE_SIZE,
            )
            .try_collect()
            .await
            .unwrap();
        assert!(entries.is_empty(), "each member deleted its index entry");
    }

    #[tokio::test(start_paused = true)]
    async fn an_owner_that_succeeds_cancels_its_owned_run_and_its_sweep_removes_its_queues() {
        let (queue, store, clock) = open_queue_at(10_000).await;
        let members = members_runtime(&queue, &store);
        let (owners, kept) = owners_runtime(&queue, &store, &members, Work::Run, End::Succeed);
        let owner_id = owners.submit(RunSpec::default()).await.unwrap().run_id;

        let worker = owners.spawn(std::future::pending::<()>());
        let ended = tokio::time::timeout(Duration::from_secs(10), owners.wait(&owner_id))
            .await
            .expect("the owner terminated in time")
            .unwrap();
        worker.shutdown().await.unwrap();
        assert_eq!(ended.termination.status, TerminalStatus::Succeeded);

        let worker = members.spawn(std::future::pending::<()>());
        let cancelled = tokio::time::timeout(Duration::from_secs(10), members.wait(&rid("owned")))
            .await
            .expect("the owned run terminated in time")
            .unwrap();
        worker.shutdown().await.unwrap();
        assert_eq!(
            cancelled.termination.cancelled_by,
            Some(CancelOrigin::Owner)
        );

        clock.advance(Duration::from_secs(60));
        assert_eq!(owners.inner.core.sweep_once().await.unwrap(), 1);
        let owner = kept.lock().unwrap().clone().unwrap();
        let queues_key = owner_queues_kv_key(&owner.run);
        assert!(queue.view().kv_get(&queues_key).await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_submission_requires_the_running_step_of_an_owner_of_its_queue() {
        let (queue, store, _clock) = open_queue_at(10_000).await;
        let (other_queue, other_store, _clock) = open_queue_at(10_000).await;
        let members = members_runtime(&queue, &store);
        let foreign = members_runtime(&other_queue, &other_store);
        let (owners, kept) = owners_runtime(&queue, &store, &members, Work::Group, End::Wait);
        let owner_id = run_owner_step(&owners).await;
        let owner = kept.lock().unwrap().clone().unwrap();
        let spec = |owner: &Owner| RunSpec {
            options: owned(owner),
            ..RunSpec::default()
        };

        // The step has returned, so its owner is sealed.
        let err = members.submit(spec(&owner)).await.unwrap_err();
        assert!(matches!(err, Error::OwnerStepEnded(id) if id == owner_id));
        // A stale attempt whose seal is open fails at the fence of its claim.
        let stale = Owner {
            step: owner
                .step
                .as_ref()
                .map(|step| OwnerStep::new(step.fence.clone())),
            ..owner.clone()
        };
        let err = members.submit(spec(&stale)).await.unwrap_err();
        assert!(matches!(err, Error::OwnerStepEnded(_)));
        // The stored owner of the group does not have a step.
        let group = members.group(rid("g"));
        group.cancel(None).await.unwrap();
        let err = group.resume(&RunOptions::default()).await.unwrap_err();
        assert!(matches!(err, Error::OwnerStepEnded(_)));

        let err = foreign.submit(spec(&owner)).await.unwrap_err();
        assert!(matches!(err, Error::OwnerOfAnotherQueue(_)));
        let err = members
            .submit(spec(&Delivery::detached().owner))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::OwnerOfAnotherQueue(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_seal_waits_for_the_submissions_in_flight() {
        let (queue, store, _clock) = open_queue_at(10_000).await;
        let members = members_runtime(&queue, &store);
        queue.enqueue("owners", Vec::new()).await.unwrap();
        let claim = queue
            .claim("owners", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let owner = Owner {
            run: OwnerRef {
                run_id: rid("owner"),
                submission_id: SubmissionId::detached(),
            },
            queue: Some(queue.clone()),
            step: queue.lease_handle(&claim).fence().map(OwnerStep::new),
        };
        let core = &members.inner.core;

        let in_flight = core.open_owned_submission(&owner).await.unwrap();
        let step = owner.step.clone().unwrap();
        let sealing = tokio::spawn(async move { step.seal().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!sealing.is_finished(), "the seal waits for the submission");

        drop(in_flight);
        sealing.await.unwrap();
        assert!(matches!(
            core.open_owned_submission(&owner).await,
            Err(Error::OwnerStepEnded(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancel_job_of_an_earlier_submission_leaves_the_runs_of_a_later_one() {
        let (queue, store, _clock) = open_queue_at(10_000).await;
        let members = members_runtime(&queue, &store);
        let (owners, _) = owners_runtime(&queue, &store, &members, Work::Run, End::Wait);
        let owner_id = run_owner_step(&owners).await;

        // The cancel job of an earlier submission of the owner's run id.
        let earlier = OwnedQueues {
            owner: OwnerRef {
                run_id: owner_id,
                submission_id: SubmissionId::of_job(&queue.next_job_id()),
            },
            queues: vec!["members".to_string()],
        };
        let effects = owners
            .inner
            .core
            .owner_termination_effects(SettlementEffects::default(), &earlier);
        queue.commit_effects(effects).await.unwrap();

        let worker = members.spawn(std::future::pending::<()>());
        tokio::time::timeout(Duration::from_secs(10), async {
            while queue.view().stats("members").await.unwrap().pending > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the cancel job completed in time");
        worker.shutdown().await.unwrap();
        let status = members.status(&rid("owned")).await.unwrap().unwrap();
        assert!(matches!(status.state, RunState::Scheduled { .. }));
    }
}

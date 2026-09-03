//! Groups of typed jobs: [`JobGroup`] submits many jobs as one durable
//! set and joins their typed results.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use futures_util::TryStreamExt;

use crate::group::{ManifestMember, MemberSpec, MemberState, RunGroup};
use crate::jobs::handle::{JobError, decode_outcome};
use crate::jobs::job::Job;
use crate::jobs::runner::{Dispatch, Inner, SubmitOptions, job_payload};
use crate::outcome::StoredOutcome;
use crate::terminal::{NoopTerminalHook, TerminalStatus};
use crate::{Error, Result, StepErrorKind};

/// A group of jobs of one type, identified by a group id. Obtained from
/// [`JobRunner::group`](crate::jobs::JobRunner::group) or
/// [`JobRunner::new_group`](crate::jobs::JobRunner::new_group).
///
/// The group's members are identified by key: a job's
/// [`idempotency_key`](Job::idempotency_key), or its position as
/// `item-{i}`. A member's job id is derived from the group id and its
/// key, so the same job submitted to two groups runs twice, and a
/// second [`submit`](Self::submit) of the group runs again every member
/// that did not succeed. The group's membership is a durable manifest,
/// so [`join`](Self::join), [`status`](Self::status) and
/// [`forget`](Self::forget) answer after a restart and from any runner
/// over the same queue.
pub struct JobGroup<J: Job> {
    inner: Arc<Inner>,
    id: String,
    _marker: PhantomData<fn() -> J>,
}

/// The durable state of a job group, read from its manifest and member
/// records by [`JobGroup::status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupStatus {
    /// The group id.
    pub group_id: String,
    /// Number of members in the group's manifest.
    pub total: usize,
    /// Members submitted and not yet terminated.
    pub pending: usize,
    /// Members whose last recorded outcome is a success.
    pub succeeded: usize,
    /// Members whose last recorded outcome is a failure.
    pub failed: usize,
    /// Members whose last recorded outcome is a cancellation.
    pub cancelled: usize,
}

/// The result of one member of a job group, returned by
/// [`JobGroup::join`].
#[derive(Debug)]
pub struct GroupResult<J: Job> {
    /// The member's key.
    pub key: String,
    /// The job's typed output, or its failure.
    pub result: std::result::Result<J::Output, JobError>,
}

impl<J: Job> JobGroup<J> {
    pub(crate) fn new(inner: Arc<Inner>, id: String) -> Self {
        Self {
            inner,
            id,
            _marker: PhantomData,
        }
    }

    /// The group id.
    pub fn id(&self) -> &str {
        &self.id
    }

    fn group(&self) -> RunGroup<'_, Dispatch, NoopTerminalHook> {
        self.inner
            .typed
            .group(self.id.clone())
            .expect("a job group id is a valid group id")
    }

    /// Submit `jobs` as the group's members with the queue's default
    /// options; see [`submit_with`](Self::submit_with).
    pub async fn submit(&self, jobs: impl IntoIterator<Item = J>) -> Result<()> {
        self.submit_with(jobs, SubmitOptions::default()).await
    }

    /// Submit `jobs` as the group's members. The first submission
    /// writes the group's manifest; a later one with a different member
    /// set is rejected with [`Error::GroupMismatch`], and one with the
    /// same set submits every member whose last recorded outcome is not
    /// a success, so a group is re-submitted after a crash or to run its
    /// failed members again. Two jobs with one key are rejected with
    /// [`Error::DuplicateMemberKey`].
    ///
    /// `opts` applies to every member; [`Job::max_attempts`] is not
    /// consulted, so set [`SubmitOptions::max_attempts`] for a limit
    /// other than the queue's.
    pub async fn submit_with(
        &self,
        jobs: impl IntoIterator<Item = J>,
        opts: SubmitOptions,
    ) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        let mut members = Vec::new();
        for (i, job) in jobs.into_iter().enumerate() {
            let key = job.idempotency_key().unwrap_or_else(|| format!("item-{i}"));
            if !seen.insert(key.clone()) {
                return Err(Error::DuplicateMemberKey(key));
            }
            members.push(ManifestMember {
                key,
                input: job_payload(&job)?,
            });
        }
        let spec = MemberSpec {
            headers: opts.headers,
            priority: opts.priority,
            max_attempts_per_step: opts.max_attempts,
            run_at: opts.run_at,
        };
        self.group().submit(members, &spec).await
    }

    /// Wait until every member has terminated and return their results
    /// in submission order. A member that terminated without an outcome
    /// record (cancelled, or dead-lettered outside its handler) is
    /// reported as a transient [`JobError`]. Returns
    /// [`Error::GroupNotFound`] for a group never submitted.
    pub async fn join(&self) -> Result<Vec<GroupResult<J>>> {
        let group = self.group();
        let manifest = group.manifest().await?;
        let mut results: HashMap<String, std::result::Result<J::Output, JobError>> = HashMap::new();
        let mut terminations = std::pin::pin!(group.terminations().await?);
        while let Some(member) = terminations.try_next().await? {
            let result = self.member_result(&member).await?;
            results.insert(member.key, result);
        }
        Ok(manifest
            .members
            .into_iter()
            .filter_map(|member| {
                results.remove(&member.key).map(|result| GroupResult {
                    key: member.key,
                    result,
                })
            })
            .collect())
    }

    /// The result of a terminated member: its outcome record decoded, or
    /// a transient [`JobError`] from its member record.
    async fn member_result(
        &self,
        member: &MemberState,
    ) -> Result<std::result::Result<J::Output, JobError>> {
        if let Some(record) = self.inner.typed.outcome(&member.record.run_id).await? {
            let recorded = matches!(
                (&record.outcome, member.status()),
                (
                    StoredOutcome::Success { .. },
                    Some(TerminalStatus::Succeeded)
                ) | (StoredOutcome::Failure { .. }, Some(TerminalStatus::Failed))
            );
            if recorded {
                return decode_outcome::<J>(record.outcome);
            }
        }
        let message = match member.status() {
            Some(TerminalStatus::Cancelled) => "job cancelled".to_string(),
            _ => member
                .record
                .terminated
                .as_ref()
                .and_then(|termination| termination.error.clone())
                .unwrap_or_else(|| "job terminated without recording an outcome".to_string()),
        };
        Ok(Err(JobError {
            kind: StepErrorKind::Transient,
            message,
        }))
    }

    /// The group's durable state. Returns [`Error::GroupNotFound`] for a
    /// group never submitted.
    pub async fn status(&self) -> Result<GroupStatus> {
        let group = self.group();
        let manifest = group.manifest().await?;
        let mut status = GroupStatus {
            group_id: self.id.clone(),
            total: manifest.members.len(),
            pending: 0,
            succeeded: 0,
            failed: 0,
            cancelled: 0,
        };
        for member in group.members().await? {
            match member.status() {
                None => status.pending += 1,
                Some(TerminalStatus::Succeeded) => status.succeeded += 1,
                Some(TerminalStatus::Failed) => status.failed += 1,
                Some(TerminalStatus::Cancelled) => status.cancelled += 1,
            }
        }
        Ok(status)
    }

    /// Remove the group's state: its manifest, member records and the
    /// memo entries and outcome records of its members. A later
    /// [`submit`](Self::submit) under the same id starts from nothing.
    pub async fn forget(&self) -> Result<()> {
        self.group().forget().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use serde::{Deserialize, Serialize};

    use crate::jobs::{Job, JobContext, JobRunner};
    use crate::test_util::open_queue;
    use crate::{Error, StepErrorKind};

    #[derive(Debug, thiserror::Error)]
    #[error("{0}")]
    struct TestError(String);

    #[derive(Serialize, Deserialize)]
    struct Square {
        n: u32,
    }

    impl Job for Square {
        const NAME: &'static str = "test.square";
        type Output = u32;
        type Error = TestError;

        async fn run(&self, ctx: JobContext<'_>) -> std::result::Result<u32, TestError> {
            ctx.state::<Arc<AtomicU32>>().fetch_add(1, Ordering::SeqCst);
            if self.n == 13 {
                return Err(TestError("unlucky".to_string()));
            }
            Ok(self.n * self.n)
        }

        fn classify(&self, _error: &TestError) -> StepErrorKind {
            StepErrorKind::Permanent
        }

        fn idempotency_key(&self) -> Option<String> {
            Some(format!("square:{}", self.n))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_group_joins_its_results_in_submission_order_and_reruns_failures() {
        let (queue, store) = open_queue().await;
        let runs = Arc::new(AtomicU32::new(0));
        let mut runner = JobRunner::builder(queue, store)
            .register::<Square>()
            .state(runs.clone())
            .build();
        let worker = runner.spawn(std::future::pending::<()>());

        let jobs = || vec![Square { n: 3 }, Square { n: 13 }, Square { n: 2 }];
        let group = runner.group::<Square>("squares").unwrap();
        group.submit(jobs()).await.unwrap();
        let results = tokio::time::timeout(std::time::Duration::from_secs(10), group.join())
            .await
            .expect("join finished in time")
            .unwrap();
        let keys: Vec<&str> = results.iter().map(|r| r.key.as_str()).collect();
        assert_eq!(keys, ["square:3", "square:13", "square:2"]);
        assert_eq!(results[0].result.as_ref().unwrap(), &9);
        assert_eq!(results[2].result.as_ref().unwrap(), &4);
        let failure = results[1].result.as_ref().unwrap_err();
        assert_eq!(
            (failure.kind, failure.message.as_str()),
            (StepErrorKind::Permanent, "unlucky")
        );
        let status = group.status().await.unwrap();
        assert_eq!(
            (
                status.total,
                status.pending,
                status.succeeded,
                status.failed
            ),
            (3, 0, 2, 1)
        );

        // A second submission runs the failed member again only.
        group.submit(jobs()).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), group.join())
            .await
            .expect("join finished in time")
            .unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 4);

        let err = group.submit(vec![Square { n: 3 }]).await.unwrap_err();
        assert!(matches!(err, Error::GroupMismatch(id) if id == "squares"));
        assert!(matches!(
            runner.group::<Square>("a/b").map(|g| g.id().to_string()),
            Err(Error::InvalidGroupId(_))
        ));

        group.forget().await.unwrap();
        assert!(matches!(group.status().await, Err(Error::GroupNotFound(_))));
        worker.shutdown().await.unwrap();
    }
}

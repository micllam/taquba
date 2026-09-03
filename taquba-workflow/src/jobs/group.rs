//! Groups of typed jobs: [`JobGroup`] submits many jobs as one durable
//! set and joins their typed results.

use std::collections::HashMap;
use std::marker::PhantomData;

use futures_util::{Stream, StreamExt, TryStreamExt};

use crate::group::{GroupMember, GroupStatus, RunGroup};
use crate::jobs::handle::{JobError, decode_end};
use crate::jobs::job::Job;
use crate::jobs::runner::{Dispatch, job_payload};
use crate::terminal::NoopTerminalHook;
use crate::{Result, RunOptions};

/// A [`RunGroup`] of jobs of one type, identified by a group id.
/// Obtained from [`JobRunner::group`](crate::jobs::JobRunner::group) or
/// [`JobRunner::new_group`](crate::jobs::JobRunner::new_group). Members
/// are keyed by the job's [`idempotency_key`](Job::idempotency_key) or
/// the positional `item-{i}`; see the [module
/// documentation](crate::jobs#job-groups).
pub struct JobGroup<J: Job> {
    group: RunGroup<Dispatch, NoopTerminalHook>,
    _marker: PhantomData<fn() -> J>,
}

/// The result of one member of a job group, yielded by
/// [`JobGroup::results`] and returned by [`JobGroup::join`].
#[derive(Debug)]
pub struct GroupResult<J: Job> {
    /// The member's key.
    pub key: String,
    /// The job's typed output, or its failure.
    pub result: std::result::Result<J::Output, JobError>,
}

impl<J: Job> JobGroup<J> {
    pub(crate) fn new(group: RunGroup<Dispatch, NoopTerminalHook>) -> Self {
        Self {
            group,
            _marker: PhantomData,
        }
    }

    /// The group id.
    pub fn id(&self) -> &str {
        self.group.id()
    }

    /// Submit `jobs` as the group's members with the queue's default
    /// options; see [`submit_with`](Self::submit_with).
    pub async fn submit(&self, jobs: impl IntoIterator<Item = J>) -> Result<()> {
        self.submit_with(jobs, RunOptions::default()).await
    }

    /// Submit `jobs` as the group's members, as [`RunGroup::submit`]
    /// submits members: the first submission writes the group's
    /// manifest, a later one with a different member set is rejected
    /// with [`Error::GroupMismatch`](crate::Error::GroupMismatch), one with the same set submits
    /// every member whose last recorded termination is not a success
    /// and two jobs with one key are rejected with
    /// [`Error::DuplicateMemberKey`](crate::Error::DuplicateMemberKey).
    ///
    /// `options` applies to every member; [`Job::max_attempts`] is not
    /// consulted, so set [`RunOptions::max_attempts_per_step`] for a
    /// limit other than the queue's.
    pub async fn submit_with(
        &self,
        jobs: impl IntoIterator<Item = J>,
        options: RunOptions,
    ) -> Result<()> {
        let mut members = Vec::new();
        for (i, job) in jobs.into_iter().enumerate() {
            members.push(GroupMember {
                key: job.idempotency_key().unwrap_or_else(|| format!("item-{i}")),
                input: job_payload(&job)?,
            });
        }
        self.group.submit(members, &options).await
    }

    /// Submit the members of the group's manifest that did not succeed,
    /// with the queue's default options; see
    /// [`resume_with`](Self::resume_with).
    pub async fn resume(&self) -> Result<()> {
        self.resume_with(RunOptions::default()).await
    }

    /// Submit the members of the group's manifest whose last recorded
    /// termination is not a success, without the jobs; see
    /// [`RunGroup::resume`]. `options` applies as in
    /// [`submit_with`](Self::submit_with).
    pub async fn resume_with(&self, options: RunOptions) -> Result<()> {
        self.group.resume(&options).await
    }

    /// The members' results as each one terminates, in completion
    /// order; a member already terminated is yielded at once. A member
    /// that terminated without a run result record (cancelled, or
    /// dead-lettered outside its handler) is reported as a transient
    /// [`JobError`]. Returns [`Error::GroupNotFound`](crate::Error::GroupNotFound) for a group never
    /// submitted.
    pub async fn results(&self) -> Result<impl Stream<Item = Result<GroupResult<J>>> + use<J>> {
        let results = self.group.results().await?;
        Ok(results.map(|member| {
            let member = member?;
            Ok(GroupResult {
                key: member.key,
                result: decode_end::<J>(Some(member.termination), member.outcome)?,
            })
        }))
    }

    /// Wait until every member has terminated and return their results
    /// in submission order; see [`results`](Self::results).
    pub async fn join(&self) -> Result<Vec<GroupResult<J>>> {
        let manifest = self.group.manifest().await?;
        let mut results: HashMap<String, std::result::Result<J::Output, JobError>> = HashMap::new();
        let mut stream = std::pin::pin!(self.results().await?);
        while let Some(result) = stream.try_next().await? {
            results.insert(result.key, result.result);
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

    /// The group's durable state; see [`RunGroup::status`].
    pub async fn status(&self) -> Result<GroupStatus> {
        self.group.status().await
    }

    /// Request cancellation of every active member; see
    /// [`RunGroup::cancel`].
    pub async fn cancel(&self) -> Result<usize> {
        self.group.cancel().await
    }

    /// Remove the group's state; see [`RunGroup::forget`].
    pub async fn forget(&self) -> Result<()> {
        self.group.forget().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use futures_util::TryStreamExt;
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

        // A resume from the manifest runs the failed member once more,
        // and the results stream yields the succeeded ones at once.
        group.resume().await.unwrap();
        let mut streamed = 0;
        let mut results = std::pin::pin!(group.results().await.unwrap());
        while let Some(result) =
            tokio::time::timeout(std::time::Duration::from_secs(10), results.try_next())
                .await
                .expect("results finished in time")
                .unwrap()
        {
            streamed += 1;
            assert_eq!(result.result.is_ok(), result.key != "square:13");
        }
        assert_eq!(streamed, 3);
        assert_eq!(runs.load(Ordering::SeqCst), 5);

        group.forget().await.unwrap();
        assert!(matches!(group.status().await, Err(Error::GroupNotFound(_))));
        assert!(matches!(group.resume().await, Err(Error::GroupNotFound(_))));
        worker.shutdown().await.unwrap();
    }
}

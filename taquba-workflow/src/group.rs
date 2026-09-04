//! Run groups: a durable set of runs of one runtime, identified by a
//! group id, whose membership is a manifest in the object store and
//! whose per-member state is a record in the queue's KV namespace.
//! [`RunGroup`] submits the members, yields their results, cancels them
//! and removes the group's state; [`jobs::JobGroup`](crate::jobs::JobGroup)
//! is its typed presentation.

use std::collections::HashMap;
use std::sync::Arc;

use futures_util::stream::{self, FuturesUnordered, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use taquba::object_store::{ObjectStore, path::Path};
use taquba::{Queue, SettlementEffects};
use tracing::warn;

use crate::blob::ObjectPrefix;
use crate::durable::{self, DurableMember, DurableTermination};
use crate::error::{Error, Result};
use crate::keys::{
    HEADER_GROUP, HEADER_GROUP_KEY, group_member_kv_key, group_members_kv_prefix,
    group_terminal_kv_key, hex_sha256, outcome_kv_key,
};
use crate::memo::MemoStore;
use crate::runner::StepRunner;
use crate::runtime::{RunOptions, RunSpec, RunTermination, RuntimeCore, WorkflowRuntime};
use crate::sweep::Clearable;
use crate::terminal::{RunOutcome, TerminalHook, TerminalStatus};

/// Member submissions and cancellations in flight at once. Each blocks
/// on a durable commit, and concurrent commits share WAL flushes.
const SUBMIT_CONCURRENCY: usize = 32;
/// Member records read per page, and deleted per transaction by
/// [`GroupStore::forget`].
const MEMBER_PAGE_SIZE: usize = 1000;

/// The run id of the member `key` of group `group_id`: the hex SHA-256
/// digest of `{group_id}/{key}`, so a key may hold characters a run id
/// rejects and groups never share run state.
pub(crate) fn member_run_id(group_id: &str, key: &str) -> String {
    hex_sha256(&[group_id.as_bytes(), b"/", key.as_bytes()])
}

/// The group membership of a run, set on every step job of the run in
/// the [`HEADER_GROUP`] and [`HEADER_GROUP_KEY`] headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Membership {
    pub(crate) group_id: String,
    pub(crate) key: String,
}

impl Membership {
    /// The membership named by `headers`, when both headers are present.
    pub(crate) fn from_headers(headers: &HashMap<String, String>) -> Option<Self> {
        Some(Self {
            group_id: headers.get(HEADER_GROUP)?.clone(),
            key: headers.get(HEADER_GROUP_KEY)?.clone(),
        })
    }

    /// The reserved headers that hold this membership on a step job.
    pub(crate) fn reserved_headers(&self) -> Vec<(&'static str, String)> {
        vec![
            (HEADER_GROUP, self.group_id.clone()),
            (HEADER_GROUP_KEY, self.key.clone()),
        ]
    }

    /// The KV key of this member's record.
    pub(crate) fn kv_key(&self) -> Vec<u8> {
        group_member_kv_key(&self.group_id, &self.key)
    }

    /// The run id of this member.
    pub(crate) fn run_id(&self) -> String {
        member_run_id(&self.group_id, &self.key)
    }
}

/// One member of a [`RunGroup`]: its key, unique within the group, and
/// the input of its run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMember {
    /// The member's key.
    pub key: String,
    /// The input of the member's step 0.
    pub input: Vec<u8>,
}

/// The members of one group, in submission order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub(crate) group_id: String,
    pub(crate) members: Vec<GroupMember>,
}

/// The durable state of a group, read from its manifest and member
/// records by [`RunGroup::status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupStatus {
    /// The group id.
    pub group_id: String,
    /// Number of members in the group's manifest.
    pub total: usize,
    /// Members submitted and not yet terminated.
    pub pending: usize,
    /// Members whose last recorded termination is a success.
    pub succeeded: usize,
    /// Members whose last recorded termination is a failure.
    pub failed: usize,
    /// Members whose last recorded termination is a cancellation.
    pub cancelled: usize,
}

/// A terminated member of a group, yielded by [`RunGroup::results`].
#[derive(Debug, Clone)]
pub struct MemberResult {
    /// The member's key.
    pub key: String,
    /// The run id of the member's run.
    pub run_id: String,
    /// The member's last recorded termination.
    pub termination: RunTermination,
    /// The member's committed outcome, read as [`WorkflowRuntime::outcome`]
    /// reads it; `None` for a member terminated without a worker.
    pub outcome: Option<RunOutcome>,
}

/// A member record as read back, with the key it is stored under.
pub(crate) struct MemberState {
    pub(crate) key: String,
    pub(crate) record: DurableMember,
}

impl MemberState {
    /// The member's terminal status, `None` while it is active.
    pub(crate) fn status(&self) -> Option<TerminalStatus> {
        self.record
            .terminated
            .as_ref()
            .map(|termination| termination.status.into())
    }
}

/// The durable state of groups: manifests under
/// `<memo_prefix>/groups/<group_id>/manifest` in the object store and
/// member records under `workflow/groups/<group_id>/` in the queue's KV
/// namespace.
#[derive(Clone)]
pub(crate) struct GroupStore {
    objects: ObjectPrefix,
    memo_store: MemoStore,
    queue: Arc<Queue>,
}

impl GroupStore {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        memo_store: MemoStore,
        queue: Arc<Queue>,
    ) -> Self {
        Self {
            objects: ObjectPrefix::new(store, prefix),
            memo_store,
            queue,
        }
    }

    fn manifest_path(&self, group_id: &str) -> Path {
        self.objects.path(&format!("groups/{group_id}/manifest"))
    }

    pub(crate) async fn read_manifest(&self, group_id: &str) -> Result<Option<Manifest>> {
        match self.objects.get(&self.manifest_path(group_id)).await? {
            Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn write_manifest(&self, manifest: &Manifest) -> Result<()> {
        self.objects
            .put(
                &self.manifest_path(&manifest.group_id),
                &durable::encode(manifest),
            )
            .await
    }

    /// The member record of `key` in `group_id`, when one exists.
    pub(crate) async fn member(&self, group_id: &str, key: &str) -> Result<Option<DurableMember>> {
        match self
            .queue
            .kv_get(&group_member_kv_key(group_id, key))
            .await?
        {
            Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Every member record of `group_id`, in key order. A record that
    /// fails to decode is skipped.
    pub(crate) async fn members(&self, group_id: &str) -> Result<Vec<MemberState>> {
        let prefix = group_members_kv_prefix(group_id);
        let mut members = Vec::new();
        let mut entries = std::pin::pin!(self.queue.kv_entries(&prefix, MEMBER_PAGE_SIZE));
        while let Some((kv_key, value)) = entries.try_next().await? {
            let key = String::from_utf8_lossy(&kv_key[prefix.len()..]).into_owned();
            match rmp_serde::from_slice(&value) {
                Ok(record) => members.push(MemberState { key, record }),
                Err(err) => {
                    warn!(group_id, key, error = %err, "group member record failed to decode");
                }
            }
        }
        Ok(members)
    }

    /// Remove the state of `group_id`: the memo entries and the terminal
    /// record of every member in its manifest, its member records and
    /// the manifest. A group without a manifest has its member records
    /// removed and nothing else.
    pub(crate) async fn forget(&self, group_id: &str) -> Result<()> {
        let mut keys = Vec::new();
        if let Some(manifest) = self.read_manifest(group_id).await? {
            for member in &manifest.members {
                let run_id = member_run_id(group_id, &member.key);
                self.memo_store.clear_memos_for_run(&run_id).await?;
                keys.push(outcome_kv_key(&run_id));
                if keys.len() == MEMBER_PAGE_SIZE {
                    self.delete_keys(std::mem::take(&mut keys)).await?;
                }
            }
        }
        let prefix = group_members_kv_prefix(group_id);
        let mut entries = std::pin::pin!(self.queue.kv_entries(&prefix, MEMBER_PAGE_SIZE));
        while let Some((key, _)) = entries.try_next().await? {
            keys.push(key);
            if keys.len() == MEMBER_PAGE_SIZE {
                self.delete_keys(std::mem::take(&mut keys)).await?;
            }
        }
        if !keys.is_empty() {
            self.delete_keys(keys).await?;
        }
        self.objects.delete(&self.manifest_path(group_id)).await?;
        Ok(())
    }

    /// Delete the KV entries under `keys` in one transaction.
    async fn delete_keys(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        self.queue
            .commit_effects(SettlementEffects::default().kv_deletes(keys))
            .await?;
        Ok(())
    }
}

impl Clearable for GroupStore {
    type Error = Error;

    async fn clear(&self, group_id: &str) -> Result<Vec<Vec<u8>>> {
        self.forget(group_id).await.map(|()| Vec::new())
    }
}

/// A group of runs of one runtime, identified by a group id. Obtained
/// from [`WorkflowRuntime::group`] or [`WorkflowRuntime::new_group`];
/// cheap to clone.
///
/// The group's members are identified by key. A member's run id is
/// derived from the group id and its key, so the same input submitted
/// to two groups runs twice, and a second [`submit`](Self::submit) of
/// the group runs again every member that did not succeed. The group's
/// membership is a durable manifest, so [`results`](Self::results),
/// [`status`](Self::status), [`cancel`](Self::cancel) and
/// [`forget`](Self::forget) answer after a restart and from any runtime
/// over the same queue.
pub struct RunGroup<R, H> {
    runtime: WorkflowRuntime<R, H>,
    id: String,
}

impl<R, H> Clone for RunGroup<R, H> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            id: self.id.clone(),
        }
    }
}

impl<R: StepRunner, H: TerminalHook> RunGroup<R, H> {
    pub(crate) fn new(runtime: WorkflowRuntime<R, H>, id: String) -> Self {
        Self { runtime, id }
    }

    /// The group id.
    pub fn id(&self) -> &str {
        &self.id
    }

    fn core(&self) -> &RuntimeCore {
        &self.runtime.inner.core
    }

    fn store(&self) -> &GroupStore {
        &self.core().group_store
    }

    /// The group's manifest; [`Error::GroupNotFound`] without one.
    pub(crate) async fn manifest(&self) -> Result<Manifest> {
        self.store()
            .read_manifest(&self.id)
            .await?
            .ok_or_else(|| Error::GroupNotFound(self.id.clone()))
    }

    /// Every member record of the group, in key order.
    pub(crate) async fn members(&self) -> Result<Vec<MemberState>> {
        self.store().members(&self.id).await
    }

    /// Submit `members` as the group's members. The first submission
    /// writes the group's manifest; a later one with a different member
    /// set is rejected with [`Error::GroupMismatch`], and one with the
    /// same set submits every member whose last recorded termination is
    /// not a success, so a group is re-submitted after a crash or to run
    /// its failed members again. Two members with one key are rejected
    /// with [`Error::DuplicateMemberKey`]. `options` applies to every
    /// member. A submission that fails returns after the members
    /// submitted so far.
    pub async fn submit(&self, members: Vec<GroupMember>, options: &RunOptions) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for member in &members {
            if !seen.insert(member.key.as_str()) {
                return Err(Error::DuplicateMemberKey(member.key.clone()));
            }
        }
        let manifest = Manifest {
            group_id: self.id.clone(),
            members,
        };
        match self.store().read_manifest(&self.id).await? {
            Some(existing) if existing.members != manifest.members => {
                return Err(Error::GroupMismatch(self.id.clone()));
            }
            Some(_) => {}
            None => self.store().write_manifest(&manifest).await?,
        }
        self.submit_members(manifest.members, options).await
    }

    /// Submit the members of the group's manifest whose last recorded
    /// termination is not a success: a member still active continues,
    /// and the rest run. Returns [`Error::GroupNotFound`] for a group
    /// never submitted. `options` applies as in [`submit`](Self::submit).
    pub async fn resume(&self, options: &RunOptions) -> Result<()> {
        let manifest = self.manifest().await?;
        self.submit_members(manifest.members, options).await
    }

    async fn submit_members(&self, members: Vec<GroupMember>, options: &RunOptions) -> Result<()> {
        let succeeded: std::collections::HashSet<String> = self
            .members()
            .await?
            .into_iter()
            .filter(|member| member.status() == Some(TerminalStatus::Succeeded))
            .map(|member| member.key)
            .collect();
        let mut submissions = stream::iter(
            members
                .into_iter()
                .filter(|member| !succeeded.contains(&member.key)),
        )
        .map(|member| async move {
            let membership = Membership {
                group_id: self.id.clone(),
                key: member.key,
            };
            self.runtime
                .submit_member(
                    &membership,
                    RunSpec {
                        run_id: Some(membership.run_id()),
                        input: member.input,
                        options: options.clone(),
                        kv_writes: HashMap::new(),
                    },
                )
                .await
                .map(|_| ())
        })
        .buffer_unordered(SUBMIT_CONCURRENCY);
        while submissions.try_next().await?.is_some() {}
        Ok(())
    }

    /// The members of the manifest as each one terminates, in
    /// completion order; a member already terminated is yielded at
    /// once. Every member must have been submitted. Once every member
    /// has been yielded, the group's terminal marker is written, from
    /// which the group retention sweep counts the window; a failed
    /// marker write is logged.
    async fn terminations(&self) -> Result<impl Stream<Item = Result<MemberState>> + use<R, H>> {
        let manifest = self.manifest().await?;
        let waits: FuturesUnordered<_> = manifest
            .members
            .into_iter()
            .map(|member| {
                let group = self.clone();
                async move {
                    let record = group.wait_member(&member.key).await?;
                    Ok(MemberState {
                        key: member.key,
                        record,
                    })
                }
            })
            .collect();
        let group = self.clone();
        let marker = stream::once(async move {
            if let Err(err) = group.mark_terminated().await {
                warn!(group_id = %group.id, "group terminal marker write failed: {err}");
            }
        })
        .filter_map(|()| async { None::<Result<MemberState>> });
        Ok(waits.chain(marker))
    }

    /// The members' results as each one terminates, in completion
    /// order; a member already terminated is yielded at once. Returns
    /// [`Error::GroupNotFound`] for a group never submitted.
    pub async fn results(&self) -> Result<impl Stream<Item = Result<MemberResult>> + use<R, H>> {
        let terminations = self.terminations().await?;
        let group = self.clone();
        Ok(terminations.then(move |member| {
            let group = group.clone();
            async move {
                let member = member?;
                let termination: RunTermination = member
                    .record
                    .terminated
                    .ok_or_else(|| Error::InconsistentRunState(member.record.run_id.clone()))?
                    .into();
                let outcome = group
                    .core()
                    .run_result_of(&member.record.run_id, &termination)
                    .await?;
                Ok(MemberResult {
                    key: member.key,
                    run_id: member.record.run_id,
                    termination,
                    outcome: outcome.map(|result| result.outcome),
                })
            }
        }))
    }

    /// The group's durable state. Returns [`Error::GroupNotFound`] for a
    /// group never submitted.
    pub async fn status(&self) -> Result<GroupStatus> {
        let manifest = self.manifest().await?;
        let mut status = GroupStatus {
            group_id: self.id.clone(),
            total: manifest.members.len(),
            pending: 0,
            succeeded: 0,
            failed: 0,
            cancelled: 0,
        };
        for member in self.members().await? {
            match member.status() {
                None => status.pending += 1,
                Some(TerminalStatus::Succeeded) => status.succeeded += 1,
                Some(TerminalStatus::Failed) => status.failed += 1,
                Some(TerminalStatus::Cancelled) => status.cancelled += 1,
            }
        }
        Ok(status)
    }

    /// Request cancellation of every active member, as
    /// [`WorkflowRuntime::cancel`] does for one run. Returns the number
    /// of members whose request was recorded.
    pub async fn cancel(&self) -> Result<usize> {
        let mut cancellations = stream::iter(
            self.members()
                .await?
                .into_iter()
                .filter(|member| member.status().is_none()),
        )
        .map(|member| async move { self.runtime.cancel(&member.record.run_id).await })
        .buffer_unordered(SUBMIT_CONCURRENCY);
        let mut cancelled = 0;
        while let Some(recorded) = cancellations.try_next().await? {
            cancelled += usize::from(recorded);
        }
        Ok(cancelled)
    }

    /// Wait until the member `key` terminates and return its record.
    /// Returns [`Error::MemberNotSubmitted`] for a member of the
    /// manifest without a record.
    async fn wait_member(&self, key: &str) -> Result<DurableMember> {
        let member = |member: Option<DurableMember>| {
            member.ok_or_else(|| Error::MemberNotSubmitted {
                group_id: self.id.clone(),
                key: key.to_string(),
            })
        };
        let record = member(self.store().member(&self.id, key).await?)?;
        if record.terminated.is_some() {
            return Ok(record);
        }
        let run_id = member_run_id(&self.id, key);
        self.core().wait_run(&run_id).await?;
        // The pointer and the member record change in one transaction,
        // so the record of a run without a pointer is terminated.
        let record = member(self.store().member(&self.id, key).await?)?;
        if record.terminated.is_none() {
            return Err(Error::InconsistentRunState(run_id));
        }
        Ok(record)
    }

    /// Write the group's terminal marker; no marker is written without
    /// [`WorkflowRuntimeBuilder::group_retention`](crate::WorkflowRuntimeBuilder::group_retention).
    async fn mark_terminated(&self) -> Result<()> {
        let core = self.core();
        if core.group_retention.is_some() {
            let key = group_terminal_kv_key(&self.id, core.clock.now_ms());
            core.queue.kv_put(&key, b"").await?;
        }
        Ok(())
    }

    /// Remove the group's state: its manifest, member records and the
    /// memo entries, run result records and terminal records of its
    /// members. A later [`submit`](Self::submit) under the same id
    /// starts from nothing.
    pub async fn forget(&self) -> Result<()> {
        self.store().forget(&self.id).await
    }
}

/// The member record written with a member's submission.
pub(crate) fn pending_member(run_id: &str) -> DurableMember {
    DurableMember {
        run_id: run_id.to_string(),
        terminated: None,
    }
}

/// The member record written by the settlement that terminates a member.
pub(crate) fn terminated_member(run_id: &str, termination: DurableTermination) -> DurableMember {
    DurableMember {
        run_id: run_id.to_string(),
        terminated: Some(termination),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::runner::{Step, StepError, StepOutcome};
    use crate::runtime::RunSpec;
    use crate::terminal::NoopTerminalHook;
    use crate::test_util::{open_queue, open_queue_at};

    /// Continues once, then succeeds with the step number.
    struct TwoSteps;

    impl StepRunner for TwoSteps {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            if step.step_number == 0 {
                Ok(StepOutcome::continue_now(step.payload.clone()))
            } else {
                Ok(StepOutcome::Succeed {
                    result: step.step_number.to_string().into_bytes(),
                })
            }
        }
    }

    fn member(key: &str) -> GroupMember {
        GroupMember {
            key: key.to_string(),
            input: key.as_bytes().to_vec(),
        }
    }

    /// Fails permanently.
    struct Rejecting;

    impl StepRunner for Rejecting {
        async fn run_step(&self, _: &Step) -> std::result::Result<StepOutcome, StepError> {
            Err(StepError::permanent("rejected"))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_result_record_of_an_earlier_termination_is_not_reported_for_a_re_run_member() {
        let (queue, store, clock) = open_queue_at(10_000).await;
        let runtime =
            WorkflowRuntime::builder(queue.clone(), store, Rejecting, NoopTerminalHook).build();
        let group = runtime.group("g").unwrap();
        group
            .submit(vec![member("a")], &RunOptions::default())
            .await
            .unwrap();
        let worker = runtime.spawn(std::future::pending::<()>());
        let first: Vec<MemberResult> = tokio::time::timeout(Duration::from_secs(10), async {
            group.results().await.unwrap().try_collect().await
        })
        .await
        .expect("results finished in time")
        .unwrap();
        assert_eq!(first[0].termination.error.as_deref(), Some("rejected"));
        assert_eq!(
            first[0].termination.error_kind,
            Some(crate::StepErrorKind::Permanent)
        );
        assert!(
            first[0].outcome.is_some(),
            "the worker recorded the failure"
        );
        worker.shutdown().await.unwrap();

        // The member runs again and the queue dead-letters it outside
        // the worker, so reconciliation terminates it and no new record
        // is written.
        clock.advance(Duration::from_secs(1));
        group
            .submit(vec![member("a")], &RunOptions::default())
            .await
            .unwrap();
        let claim = queue
            .claim("workflow-steps", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        queue.dead_letter(&claim, "hung").await.unwrap();
        assert_eq!(runtime.inner.reconcile_dead_steps().await.unwrap(), 1);

        let second: Vec<MemberResult> = group.results().await.unwrap().try_collect().await.unwrap();
        assert_eq!(second[0].termination.error.as_deref(), Some("hung"));
        assert_eq!(second[0].termination.error_kind, None);
        assert_eq!(second[0].termination.terminated_at_ms, 11_000);
        assert!(
            second[0].outcome.is_none(),
            "the first termination's record does not belong to the second",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn membership_holds_across_steps_and_terminations_wait_for_the_last_one() {
        let (queue, store) = open_queue().await;
        let runtime = WorkflowRuntime::builder(queue.clone(), store, TwoSteps, NoopTerminalHook)
            .poll_interval(Duration::from_millis(10))
            .build();
        let group = runtime.group("g").unwrap();
        group
            .submit(vec![member("a"), member("b")], &RunOptions::default())
            .await
            .unwrap();
        let pending = group.members().await.unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|m| m.status().is_none()));
        assert_eq!(pending[0].record.run_id, member_run_id("g", "a"));

        assert!(matches!(
            group.submit(vec![member("a"), member("a")], &RunOptions::default()).await,
            Err(Error::DuplicateMemberKey(key)) if key == "a"
        ));

        let worker = runtime.spawn(std::future::pending::<()>());
        let terminated: Vec<MemberResult> = tokio::time::timeout(Duration::from_secs(10), async {
            group.results().await.unwrap().try_collect().await
        })
        .await
        .expect("results finished in time")
        .unwrap();
        assert_eq!(terminated.len(), 2);
        for m in &terminated {
            assert_eq!(m.termination.status, TerminalStatus::Succeeded);
            let outcome = m.outcome.as_ref().expect("the worker recorded the outcome");
            assert_eq!(outcome.run_id, m.run_id);
            assert_eq!(
                outcome.final_step, 1,
                "the member terminated at its second step"
            );
            assert_eq!(outcome.result.as_deref(), Some(b"1".as_slice()));
        }
        let status = group.status().await.unwrap();
        assert_eq!((status.total, status.succeeded, status.pending), (2, 2, 0));

        // A member already terminated is yielded again at once.
        let again: Vec<MemberResult> = group.results().await.unwrap().try_collect().await.unwrap();
        assert_eq!(again.len(), 2);
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_group_cancellation_records_the_member_cancelled() {
        let (queue, store, _clock) = open_queue_at(10_000).await;
        let runtime =
            WorkflowRuntime::builder(queue.clone(), store, TwoSteps, NoopTerminalHook).build();
        let group = runtime.group("g").unwrap();
        group
            .submit(vec![member("a")], &RunOptions::default())
            .await
            .unwrap();
        let run_id = member_run_id("g", "a");
        assert_eq!(group.cancel().await.unwrap(), 1);
        assert_eq!(group.cancel().await.unwrap(), 0, "no member is active");

        let results: Vec<MemberResult> =
            group.results().await.unwrap().try_collect().await.unwrap();
        assert_eq!(results[0].run_id, run_id);
        assert_eq!(
            results[0].termination,
            RunTermination {
                status: TerminalStatus::Cancelled,
                error: None,
                error_kind: None,
                final_step: 0,
                terminated_at_ms: 10_000,
            }
        );
        assert!(
            results[0].outcome.is_none(),
            "a pending step is cancelled without a worker, so no result is recorded",
        );

        // The cancelled member is submitted again; a member is grouped by
        // its key, so a plain submission of the same run id is not.
        group
            .submit(vec![member("a")], &RunOptions::default())
            .await
            .unwrap();
        assert!(group.members().await.unwrap()[0].status().is_none());
        let plain = runtime
            .submit(RunSpec {
                run_id: Some(run_id.clone()),
                input: b"a".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!plain.newly_submitted);

        group.forget().await.unwrap();
        assert!(group.members().await.unwrap().is_empty());
        assert!(matches!(
            group.manifest().await,
            Err(Error::GroupNotFound(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn the_group_sweep_removes_the_members_records_with_the_group() {
        let (queue, store, clock) = open_queue_at(10_000).await;
        let runtime =
            WorkflowRuntime::builder(queue.clone(), store.clone(), TwoSteps, NoopTerminalHook)
                .group_retention(Duration::from_secs(1))
                .build();
        let group = runtime.group("g").unwrap();
        group
            .submit(vec![member("a")], &RunOptions::default())
            .await
            .unwrap();
        let run_id = member_run_id("g", "a");
        assert_eq!(group.cancel().await.unwrap(), 1);
        let memos = crate::memo::MemoStore::new(store, "workflow-memo");
        memos.new_run_memo(&run_id).put("k", b"v").await.unwrap();
        let results: Vec<MemberResult> =
            group.results().await.unwrap().try_collect().await.unwrap();
        assert_eq!(results.len(), 1);
        let marker = group_terminal_kv_key("g", 10_000);
        assert!(
            queue.kv_get(&marker).await.unwrap().is_some(),
            "the marker is written when the last termination is observed"
        );
        let terminal_record = outcome_kv_key(&run_id);
        assert!(queue.kv_get(&terminal_record).await.unwrap().is_some());

        clock.advance(Duration::from_secs(1));
        assert_eq!(
            runtime.inner.core.sweep_once().await.unwrap(),
            0,
            "the marker is not yet expired"
        );
        clock.advance(Duration::from_millis(1));
        assert_eq!(runtime.inner.core.sweep_once().await.unwrap(), 1);
        assert!(queue.kv_get(&marker).await.unwrap().is_none());
        assert!(group.members().await.unwrap().is_empty());
        assert!(matches!(
            group.manifest().await,
            Err(Error::GroupNotFound(_))
        ));
        assert!(
            queue.kv_get(&terminal_record).await.unwrap().is_none(),
            "the member's terminal record is removed with the group"
        );
        assert!(
            memos
                .new_run_memo(&run_id)
                .get("k")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            runtime.status(&run_id).await.unwrap().is_none(),
            "nothing of the member remains"
        );
    }
}

//! Run groups: a durable set of runs of one runtime, identified by a group id,
//! whose membership is a manifest in the object store and whose per-member
//! state is a record in the queue's KV namespace. [`RunGroup`] submits the
//! members, yields their results, cancels them and removes the group's state.
//! [`jobs::JobGroup`](crate::jobs::JobGroup) is the typed presentation of a run
//! group.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::stream::{self, FuturesUnordered, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use taquba::object_store::path::Path;
use taquba::{KvOrder, Queue, QueueView, SettlementEffects};
use tracing::warn;

use crate::blob::ObjectPrefix;
use crate::durable::{self, DurableMember, DurableTermination};
use crate::error::{Error, Result};
use crate::keys::{
    HEADER_GROUP, HEADER_GROUP_KEY, RunId, group_member_kv_key, group_members_kv_prefix,
    outcome_kv_key,
};
use crate::memo::MemoStore;
use crate::runtime::{RunOptions, RunSpec, RunTermination, RuntimeCore};
use crate::sweep::{Clearable, Cleared};
use crate::terminal::{RunOutcome, TerminalStatus};

/// Member submissions and cancellations in flight at once. Each blocks on a
/// durable commit, and concurrent commits share WAL flushes.
const SUBMIT_CONCURRENCY: usize = 32;
/// Member records read per page, and members removed per transaction by
/// [`GroupStore::remove`].
const MEMBER_PAGE_SIZE: usize = 1000;

/// The run id of run `rerun` of the member `key` of group `group_id`. The id of
/// run 0 is the hex SHA-256 digest of `{group_id}/{key}`, and a later run
/// appends `-r{rerun}` to it. A key with characters that a run id rejects still
/// maps to a valid run id, and groups never share run state.
pub(crate) fn member_run_id(group_id: &RunId, key: &str, rerun: u32) -> RunId {
    let first = RunId::digest(&[group_id.as_bytes(), b"/", key.as_bytes()]);
    if rerun == 0 {
        return first;
    }
    RunId::new(format!("{}-r{rerun}", first.as_str()))
        .expect("a digest with a rerun suffix is a valid run id")
}

/// The rerun number of the member run `run_id` that [`member_run_id`] derived.
fn rerun_of(run_id: &RunId) -> u32 {
    run_id
        .rsplit_once("-r")
        .and_then(|(_, rerun)| rerun.parse().ok())
        .unwrap_or(0)
}

/// The run id under which a submission runs the member `key` with the member
/// record `record`: run 0 without a record, the run of an active member and the
/// next run after a termination that is not a success. It is `None` after a
/// success.
fn submission_run_id(group_id: &RunId, key: &str, record: Option<&DurableMember>) -> Option<RunId> {
    let Some(record) = record else {
        return Some(member_run_id(group_id, key, 0));
    };
    match &record.terminated {
        None => Some(record.run_id.clone()),
        Some(termination)
            if TerminalStatus::from(termination.status) == TerminalStatus::Succeeded =>
        {
            None
        }
        Some(_) => Some(member_run_id(group_id, key, rerun_of(&record.run_id) + 1)),
    }
}

/// The group membership of a run, set on every step job of the run in the
/// [`HEADER_GROUP`] and [`HEADER_GROUP_KEY`] headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Membership {
    pub(crate) group_id: RunId,
    pub(crate) key: String,
}

impl Membership {
    /// The membership named by `headers`, when both headers are present.
    pub(crate) fn from_headers(headers: &HashMap<String, String>) -> Result<Option<Self>> {
        let (Some(group_id), Some(key)) =
            (headers.get(HEADER_GROUP), headers.get(HEADER_GROUP_KEY))
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            group_id: RunId::new(group_id.as_str())?,
            key: key.clone(),
        }))
    }

    /// The reserved headers that hold this membership on a step job.
    pub(crate) fn reserved_headers(&self) -> Vec<(&'static str, String)> {
        vec![
            (HEADER_GROUP, self.group_id.to_string()),
            (HEADER_GROUP_KEY, self.key.clone()),
        ]
    }

    /// The KV key of this member's record.
    pub(crate) fn kv_key(&self) -> Vec<u8> {
        group_member_kv_key(&self.group_id, &self.key)
    }
}

/// One member of a [`RunGroup`]: the input of its run and its key, which is
/// unique within the group.
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
    pub(crate) group_id: RunId,
    pub(crate) members: Vec<GroupMember>,
}

/// The durable state of a group, read from its manifest and member records by
/// [`WorkflowView::group_status`](crate::WorkflowView::group_status).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupStatus {
    /// The group id.
    pub group_id: RunId,
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
    /// The run id of the member's last run.
    pub run_id: RunId,
    /// The member's last recorded termination.
    pub termination: RunTermination,
    /// The member's committed outcome, as [`RunEnd::outcome`](crate::RunEnd)
    /// reports it, or `None` for a member terminated without a worker.
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

/// The path of the manifest of `group_id`.
fn manifest_path(objects: &ObjectPrefix, group_id: &RunId) -> Path {
    objects.path(&format!("groups/{group_id}/manifest"))
}

/// The manifest of `group_id` in the object store of `memos`, when one exists.
pub(crate) async fn read_manifest(memos: &MemoStore, group_id: &RunId) -> Result<Option<Manifest>> {
    let objects = memos.objects();
    match objects.get(&manifest_path(objects, group_id)).await? {
        Some(bytes) => durable::decode(&bytes).map(Some),
        None => Ok(None),
    }
}

/// Every member record of `group_id`, in key order. A record that fails to
/// decode is skipped.
pub(crate) async fn read_members(queue: &QueueView, group_id: &RunId) -> Result<Vec<MemberState>> {
    let prefix = group_members_kv_prefix(group_id);
    let mut members = Vec::new();
    let mut entries =
        std::pin::pin!(queue.kv_entries(&prefix, .., KvOrder::Ascending, MEMBER_PAGE_SIZE));
    while let Some((kv_key, value)) = entries.try_next().await? {
        let key = String::from_utf8_lossy(&kv_key[prefix.len()..]).into_owned();
        if let Some(record) = durable::decode_or_absent(
            &value,
            "group member record",
            &format_args!("{group_id}/{key}"),
        ) {
            members.push(MemberState { key, record });
        }
    }
    Ok(members)
}

/// The durable state of groups: manifests under
/// `<memo_prefix>/groups/<group_id>/manifest` in the object store of the memo
/// store and member records under `workflow/groups/<group_id>/` in the queue's
/// KV namespace.
#[derive(Clone)]
pub(crate) struct GroupStore {
    memo_store: MemoStore,
    queue: Arc<Queue>,
}

impl GroupStore {
    pub(crate) fn new(memo_store: MemoStore, queue: Arc<Queue>) -> Self {
        Self { memo_store, queue }
    }

    pub(crate) async fn read_manifest(&self, group_id: &RunId) -> Result<Option<Manifest>> {
        read_manifest(&self.memo_store, group_id).await
    }

    async fn write_manifest(&self, manifest: &Manifest) -> Result<()> {
        let objects = self.memo_store.objects();
        objects
            .put(
                &manifest_path(objects, &manifest.group_id),
                &durable::encode(manifest),
            )
            .await
    }

    /// The member record of `key` in `group_id`, when one exists.
    pub(crate) async fn member(
        &self,
        group_id: &RunId,
        key: &str,
    ) -> Result<Option<DurableMember>> {
        durable::kv_record(self.queue.view(), &group_member_kv_key(group_id, key)).await
    }

    /// Every member record of `group_id`, in key order.
    pub(crate) async fn members(&self, group_id: &RunId) -> Result<Vec<MemberState>> {
        read_members(self.queue.view(), group_id).await
    }

    /// Removes the state of `group_id`. Fails with [`Error::GroupActive`] when
    /// a member is active, or when a member is submitted before the removal
    /// commits.
    pub(crate) async fn forget(&self, group_id: &RunId) -> Result<()> {
        let removal = self.removal(group_id).await?;
        if removal.any_member(|member| member.terminated.is_none()) || !self.remove(removal).await?
        {
            return Err(Error::GroupActive(group_id.clone()));
        }
        Ok(())
    }

    /// The state of `group_id` as one read of its manifest and its member
    /// records: every member of the manifest, and every member record without a
    /// member in the manifest.
    async fn removal(&self, group_id: &RunId) -> Result<Removal> {
        let mut members = BTreeMap::new();
        if let Some(manifest) = self.read_manifest(group_id).await? {
            for member in manifest.members {
                members.insert(
                    group_member_kv_key(group_id, &member.key),
                    RemovedMember {
                        key: Some(member.key),
                        stored: None,
                        record: None,
                    },
                );
            }
        }
        let prefix = group_members_kv_prefix(group_id);
        let mut entries = std::pin::pin!(self.queue.view().kv_entries(
            &prefix,
            ..,
            KvOrder::Ascending,
            MEMBER_PAGE_SIZE
        ));
        while let Some((key, value)) = entries.try_next().await? {
            let record = durable::decode_or_absent(
                &value,
                "group member record",
                &format_args!(
                    "{group_id}/{}",
                    String::from_utf8_lossy(&key[prefix.len()..])
                ),
            );
            let member = members.entry(key).or_insert(RemovedMember {
                key: None,
                stored: None,
                record: None,
            });
            member.stored = Some(value);
            member.record = record;
        }
        Ok(Removal {
            group_id: group_id.clone(),
            members,
        })
    }

    /// Remove the state that `removal` read: the member records and the
    /// terminal record of every run of every member of the manifest, then the
    /// memo entries of those runs and the manifest. A group without a manifest
    /// has its member records removed and nothing else.
    ///
    /// A transaction removes the KV state of [`MEMBER_PAGE_SIZE`] members and
    /// compares the record of each with the state that `removal` read. Returns
    /// `false` without a further removal when a compare does not match.
    async fn remove(&self, removal: Removal) -> Result<bool> {
        let members: Vec<_> = removal.members.iter().collect();
        for page in members.chunks(MEMBER_PAGE_SIZE) {
            let compares: Vec<_> = page
                .iter()
                .map(|(key, member)| (key.as_slice(), member.stored.as_deref()))
                .collect();
            let outcomes = page
                .iter()
                .flat_map(|(_, member)| member.run_ids(&removal.group_id))
                .map(|run_id| outcome_kv_key(&run_id));
            let records = page
                .iter()
                .filter(|(_, member)| member.stored.is_some())
                .map(|(key, _)| (*key).clone());
            let effects =
                SettlementEffects::default().kv_deletes(records.chain(outcomes).collect());
            if self
                .queue
                .kv_compare_commit(&compares, effects)
                .await?
                .is_none()
            {
                return Ok(false);
            }
        }
        for member in removal.members.values() {
            for run_id in member.run_ids(&removal.group_id) {
                self.memo_store.clear_memos_for_run(&run_id).await?;
            }
        }
        let objects = self.memo_store.objects();
        objects
            .delete(&manifest_path(objects, &removal.group_id))
            .await?;
        Ok(true)
    }
}

/// One member of a [`Removal`].
struct RemovedMember {
    /// The member's key, for a member of the manifest.
    key: Option<String>,
    /// The stored bytes of the member record, `None` without a record.
    stored: Option<Bytes>,
    /// The decoded member record, `None` without a record or for a record that
    /// fails to decode.
    record: Option<DurableMember>,
}

impl RemovedMember {
    /// The run ids of a member of the manifest of `group_id`, from run 0 to the
    /// run that its record names.
    fn run_ids(&self, group_id: &RunId) -> Vec<RunId> {
        let Some(key) = &self.key else {
            return Vec::new();
        };
        let last = self
            .record
            .as_ref()
            .map_or(0, |record| rerun_of(&record.run_id));
        (0..=last)
            .map(|rerun| member_run_id(group_id, key, rerun))
            .collect()
    }
}

/// The state of a group that [`GroupStore::remove`] removes, with the KV key of
/// each member record.
struct Removal {
    group_id: RunId,
    members: BTreeMap<Vec<u8>, RemovedMember>,
}

impl Removal {
    /// Whether `test` is true for the record of a member.
    fn any_member(&self, test: impl Fn(&DurableMember) -> bool) -> bool {
        self.members
            .values()
            .filter_map(|member| member.record.as_ref())
            .any(test)
    }
}

impl Clearable for GroupStore {
    type Error = Error;

    /// A member that is active, or that terminated after the marker, belongs to
    /// a later submission of the group, whose marker [`RunGroup::results`]
    /// writes.
    async fn clear(&self, group_id: &RunId, marked_at_ms: u64) -> Result<Cleared> {
        let removal = self.removal(group_id).await?;
        let superseded = removal.any_member(|member| {
            member
                .terminated
                .as_ref()
                .is_none_or(|termination| termination.terminated_at_ms > marked_at_ms)
        });
        if superseded || !self.remove(removal).await? {
            return Ok(Cleared::Superseded);
        }
        Ok(Cleared::Removed(Vec::new()))
    }
}

/// A group of runs of one runtime, identified by a group id. Obtained from
/// [`WorkflowRuntime::group`](crate::WorkflowRuntime::group) or
/// [`WorkflowRuntime::new_group`](crate::WorkflowRuntime::new_group). A clone
/// copies an `Arc` and the group id.
///
/// The group's members are identified by key. A member's run id is derived from
/// the group id, its key and its count of earlier runs, so the same input
/// submitted to two groups runs twice. A second [`submit`](Self::submit) of the
/// group runs every member that did not succeed again under a new run id, which
/// does not share the state of the earlier run. The group's membership is a
/// durable manifest, so [`results`](Self::results), [`status`](Self::status),
/// [`cancel`](Self::cancel) and [`forget`](Self::forget) work after a restart
/// and from any runtime over the same queue.
#[derive(Clone)]
pub struct RunGroup {
    runtime: Arc<RuntimeCore>,
    id: RunId,
}

impl RunGroup {
    pub(crate) fn new(runtime: Arc<RuntimeCore>, id: RunId) -> Self {
        Self { runtime, id }
    }

    /// The group id.
    pub fn id(&self) -> &RunId {
        &self.id
    }

    fn core(&self) -> &RuntimeCore {
        &self.runtime
    }

    fn store(&self) -> &GroupStore {
        &self.core().group_store
    }

    /// The group's manifest, or [`Error::GroupNotFound`] when the manifest is
    /// absent.
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

    /// Submit `members` as the group's members. The first submission writes the
    /// group's manifest. A later submission with a different member set is
    /// rejected with [`Error::GroupMismatch`], and a later submission with the
    /// same set submits every member whose last recorded termination is not a
    /// success, so a group is re-submitted after a crash or to run its failed
    /// members again. Two members with one key are rejected with
    /// [`Error::DuplicateMemberKey`]. `options` applies to every member. A
    /// submission that fails returns after the members submitted so far.
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
    /// termination is not a success: a member still active continues, and the
    /// rest run. Returns [`Error::GroupNotFound`] for a group never submitted.
    /// `options` applies as in [`submit`](Self::submit).
    pub async fn resume(&self, options: &RunOptions) -> Result<()> {
        let manifest = self.manifest().await?;
        self.submit_members(manifest.members, options).await
    }

    async fn submit_members(&self, members: Vec<GroupMember>, options: &RunOptions) -> Result<()> {
        let records: HashMap<String, DurableMember> = self
            .members()
            .await?
            .into_iter()
            .map(|member| (member.key, member.record))
            .collect();
        let mut submissions = stream::iter(members.into_iter().filter_map(|member| {
            let run_id = submission_run_id(&self.id, &member.key, records.get(&member.key))?;
            Some((member, run_id))
        }))
        .map(|(member, run_id)| async move {
            let membership = Membership {
                group_id: self.id.clone(),
                key: member.key,
            };
            self.runtime
                .submit_member(
                    &membership,
                    RunSpec {
                        run_id: Some(run_id),
                        input: member.input,
                        options: options.clone(),
                        effects: SettlementEffects::default(),
                    },
                )
                .await
                .map(|_| ())
        })
        .buffer_unordered(SUBMIT_CONCURRENCY);
        while submissions.try_next().await?.is_some() {}
        Ok(())
    }

    /// The members of the manifest as each one terminates, in completion order.
    /// A member already terminated is yielded at once. Every member must be
    /// submitted before the call. After the last member is yielded, the group's
    /// terminal marker is written, and the group retention sweep counts the
    /// window from it. A failed marker write is logged.
    async fn terminations(&self) -> Result<impl Stream<Item = Result<MemberState>> + use<>> {
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

    /// The members' results as each one terminates, in completion order. A
    /// member already terminated is yielded at once. Returns
    /// [`Error::GroupNotFound`] for a group never submitted.
    pub async fn results(&self) -> Result<impl Stream<Item = Result<MemberResult>> + use<>> {
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
                    .view
                    .run_result_of(&member.record.run_id, &termination)
                    .await?;
                Ok(MemberResult {
                    key: member.key,
                    run_id: member.record.run_id,
                    termination,
                    outcome,
                })
            }
        }))
    }

    /// The group's durable state, as
    /// [`WorkflowView::group_status`](crate::WorkflowView::group_status) reads
    /// it. Returns [`Error::GroupNotFound`] for a group never submitted.
    pub async fn status(&self) -> Result<GroupStatus> {
        self.core()
            .view
            .group_status(&self.id)
            .await?
            .ok_or_else(|| Error::GroupNotFound(self.id.clone()))
    }

    /// Request cancellation of every active member, as
    /// [`WorkflowRuntime::cancel`](crate::WorkflowRuntime::cancel) does for one
    /// run. Returns the number of members whose request was recorded.
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

    /// Wait until the member `key` terminates and return its record. Returns
    /// [`Error::MemberNotSubmitted`] for a member of the manifest without a
    /// record.
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
        let run_id = record.run_id;
        self.core().wait_run(&run_id).await?;
        // The pointer and the member record change in one transaction, so the
        // record of a run without a pointer is terminated.
        let record = member(self.store().member(&self.id, key).await?)?;
        if record.terminated.is_none() {
            return Err(Error::InconsistentRunState(run_id));
        }
        Ok(record)
    }

    /// Write the group's terminal marker. No marker is written without
    /// [`group_retention`](crate::WorkflowRuntimeBuilder::group_retention) on
    /// the builder.
    async fn mark_terminated(&self) -> Result<()> {
        let core = self.core();
        if let Some(sweep) = &core.group_sweep {
            let effects = sweep.mark(SettlementEffects::default(), &self.id, core.clock.now_ms());
            core.queue.commit_effects(effects).await?;
        }
        Ok(())
    }

    /// Remove the group's state: its manifest, member records and the memo
    /// entries, run result records and terminal records of its members. A later
    /// [`submit`](Self::submit) with the same id starts from nothing. Returns
    /// [`Error::GroupActive`] and keeps the group's state for a group with an
    /// active member. It returns the same error for a group whose member is
    /// submitted before the removal commits. A transaction removes the records
    /// of 1,000 members.
    pub async fn forget(&self) -> Result<()> {
        self.store().forget(&self.id).await
    }
}

/// The member record written with a member's submission.
pub(crate) fn pending_member(run_id: &RunId) -> DurableMember {
    DurableMember {
        run_id: run_id.clone(),
        terminated: None,
    }
}

/// The member record written by the settlement that terminates a member.
pub(crate) fn terminated_member(run_id: &RunId, termination: DurableTermination) -> DurableMember {
    DurableMember {
        run_id: run_id.clone(),
        terminated: Some(termination),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::runner::{Step, StepError, StepOutcome, StepRunner};
    use crate::runtime::{RunSpec, StepRef, WorkflowRuntime};
    use crate::terminal::NoopTerminalHook;
    use crate::test_util::{open_queue, open_queue_at, rid};

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

    /// Fails each run, and counts the runs.
    struct Failing(Arc<AtomicU32>);

    impl StepRunner for Failing {
        async fn run_step(&self, _: &Step) -> std::result::Result<StepOutcome, StepError> {
            let run = self.0.fetch_add(1, Ordering::SeqCst);
            Ok(StepOutcome::Fail {
                reason: format!("run {run}"),
            })
        }
    }

    #[test]
    fn rerun_of_reads_the_rerun_number_of_a_member_run_id() {
        for rerun in [0, 1, 12] {
            assert_eq!(rerun_of(&member_run_id(&rid("g"), "a/b", rerun)), rerun);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_member_run_again_does_not_replay_the_step_outputs_of_its_earlier_run() {
        let (queue, store) = open_queue().await;
        let runs = Arc::new(AtomicU32::new(0));
        let runtime =
            WorkflowRuntime::builder(queue, store, Failing(runs.clone()), NoopTerminalHook)
                .step_output_replay()
                .build()
                .unwrap();
        let group = runtime.group(rid("g"));
        let worker = runtime.spawn(std::future::pending::<()>());
        let mut results = Vec::new();
        for _ in 0..2 {
            group
                .submit(vec![member("a")], &RunOptions::default())
                .await
                .unwrap();
            let mut member: Vec<MemberResult> =
                tokio::time::timeout(Duration::from_secs(10), async {
                    group.results().await.unwrap().try_collect().await
                })
                .await
                .expect("results finished in time")
                .unwrap();
            results.push(member.remove(0));
        }
        worker.shutdown().await.unwrap();

        assert_eq!(runs.load(Ordering::SeqCst), 2);
        assert_eq!(results[0].run_id, member_run_id(&rid("g"), "a", 0));
        assert_eq!(results[1].run_id, member_run_id(&rid("g"), "a", 1));
        assert_eq!(results[1].termination.error.as_deref(), Some("run 1"));
        assert_eq!(
            results[1].outcome.as_ref().and_then(|o| o.error.as_deref()),
            Some("run 1"),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn membership_holds_across_steps_and_terminations_wait_for_the_last_one() {
        let (queue, store) = open_queue().await;
        let runtime = WorkflowRuntime::builder(queue.clone(), store, TwoSteps, NoopTerminalHook)
            .poll_interval(Duration::from_millis(10))
            .build()
            .unwrap();
        let group = runtime.group(rid("g"));
        group
            .submit(vec![member("a"), member("b")], &RunOptions::default())
            .await
            .unwrap();
        let pending = group.members().await.unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|m| m.status().is_none()));
        assert_eq!(pending[0].record.run_id, member_run_id(&rid("g"), "a", 0));

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
                outcome.final_step.number, 1,
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
    async fn the_run_options_apply_to_every_member() {
        let (queue, store) = open_queue().await;
        let runtime = WorkflowRuntime::builder(queue.clone(), store, TwoSteps, NoopTerminalHook)
            .build()
            .unwrap();
        let group = runtime.group(rid("g"));
        let options = RunOptions {
            priority: Some(3),
            max_attempts_per_step: Some(5),
            headers: HashMap::from([("tenant".to_string(), "acme".to_string())]),
            ..Default::default()
        };
        group
            .submit(vec![member("a"), member("b")], &options)
            .await
            .unwrap();

        for _ in 0..2 {
            let job = queue
                .claim("workflow-steps", Duration::from_secs(30))
                .await
                .unwrap()
                .expect("a member's step job");
            assert_eq!(job.priority, 3);
            assert_eq!(job.max_attempts, 5);
            assert_eq!(job.headers.get("tenant").map(String::as_str), Some("acme"));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_group_cancellation_records_the_member_cancelled() {
        let (queue, store, _clock) = open_queue_at(10_000).await;
        let runtime = WorkflowRuntime::builder(queue.clone(), store, TwoSteps, NoopTerminalHook)
            .build()
            .unwrap();
        let group = runtime.group(rid("g"));
        group
            .submit(vec![member("a")], &RunOptions::default())
            .await
            .unwrap();
        let run_id = member_run_id(&rid("g"), "a", 0);
        let job_id = runtime.status(&run_id).await.unwrap().unwrap().step.job_id;
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
                final_step: StepRef { number: 0, job_id },
                terminated_at_ms: 10_000,
            }
        );
        assert!(
            results[0].outcome.is_none(),
            "a pending step is cancelled without a worker, so no result is recorded",
        );

        // The cancelled member runs again under the next run id, and a plain
        // submission of that run id finds the member's run active.
        group
            .submit(vec![member("a")], &RunOptions::default())
            .await
            .unwrap();
        let members = group.members().await.unwrap();
        assert!(members[0].status().is_none());
        let rerun_id = member_run_id(&rid("g"), "a", 1);
        assert_eq!(members[0].record.run_id, rerun_id);
        let plain = runtime
            .submit(RunSpec {
                run_id: Some(rerun_id),
                input: b"a".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!plain.newly_submitted);
    }

    #[tokio::test(start_paused = true)]
    async fn forget_refuses_a_group_with_an_active_member() {
        let (queue, store) = open_queue().await;
        let runtime = WorkflowRuntime::builder(queue, store, TwoSteps, NoopTerminalHook)
            .build()
            .unwrap();
        let group = runtime.group(rid("g"));
        group
            .submit(vec![member("a")], &RunOptions::default())
            .await
            .unwrap();

        assert!(matches!(
            group.forget().await,
            Err(Error::GroupActive(id)) if id == "g"
        ));
        assert_eq!(group.status().await.unwrap().pending, 1);

        assert_eq!(group.cancel().await.unwrap(), 1);
        group.forget().await.unwrap();
        assert!(group.members().await.unwrap().is_empty());
        assert!(matches!(
            group.manifest().await,
            Err(Error::GroupNotFound(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_reader_view_reads_the_group_status_of_the_runtime() {
        let (queue, store) = open_queue().await;
        let runtime = WorkflowRuntime::builder(queue, store.clone(), TwoSteps, NoopTerminalHook)
            .memo_prefix("memo")
            .build()
            .unwrap();
        let group = runtime.group(rid("g"));
        // The worker loop is not spawned, so both members stay pending.
        group
            .submit(vec![member("a"), member("b")], &RunOptions::default())
            .await
            .unwrap();

        let reader = taquba::QueueReader::open(store.clone(), "test")
            .await
            .unwrap();
        let view = crate::WorkflowView::new(reader.view().clone(), MemoStore::new(store, "memo"));
        let status = view
            .group_status(&rid("g"))
            .await
            .unwrap()
            .expect("submitted");
        assert_eq!((status.total, status.pending), (2, 2));
        assert_eq!(status, group.status().await.unwrap());
        assert!(view.group_status(&rid("unknown")).await.unwrap().is_none());
        assert!(matches!(
            runtime.group(rid("unknown")).status().await,
            Err(Error::GroupNotFound(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_removal_does_not_apply_after_a_later_submission() {
        let (queue, store) = open_queue().await;
        let runtime = WorkflowRuntime::builder(queue, store, TwoSteps, NoopTerminalHook)
            .build()
            .unwrap();
        let group = runtime.group(rid("g"));
        let options = RunOptions::default();
        group.submit(vec![member("a")], &options).await.unwrap();
        assert_eq!(group.cancel().await.unwrap(), 1);

        // The submission commits between the read and the removal.
        let removal = group.store().removal(&rid("g")).await.unwrap();
        group.submit(vec![member("a")], &options).await.unwrap();
        assert!(!group.store().remove(removal).await.unwrap());
        assert_eq!(group.status().await.unwrap().pending, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn the_group_sweep_retains_a_group_submitted_again_after_its_marker() {
        let (queue, store, clock) = open_queue_at(10_000).await;
        let runtime = WorkflowRuntime::builder(queue.clone(), store, TwoSteps, NoopTerminalHook)
            .group_retention(Duration::from_secs(1))
            .build()
            .unwrap();
        let group = runtime.group(rid("g"));
        let sweep = runtime.inner.core.group_sweep.as_ref().unwrap();
        let options = RunOptions::default();
        let submit = || group.submit(vec![member("a")], &options);
        let observe = || async {
            let results: Vec<MemberResult> =
                group.results().await.unwrap().try_collect().await.unwrap();
            assert_eq!(results.len(), 1);
        };

        submit().await.unwrap();
        assert_eq!(group.cancel().await.unwrap(), 1);
        observe().await;

        // The member is active when the first marker expires.
        clock.advance(Duration::from_millis(500));
        submit().await.unwrap();
        clock.advance(Duration::from_millis(500));
        assert_eq!(runtime.inner.core.sweep_once().await.unwrap(), 1);
        let first = sweep.marker_key(&rid("g"), 10_000);
        assert!(queue.view().kv_get(&first).await.unwrap().is_none());
        assert_eq!(group.status().await.unwrap().pending, 1);

        // The member terminated after the second marker and its termination is
        // not observed when that marker expires.
        assert_eq!(group.cancel().await.unwrap(), 1);
        observe().await;
        clock.advance(Duration::from_millis(500));
        submit().await.unwrap();
        assert_eq!(group.cancel().await.unwrap(), 1);
        clock.advance(Duration::from_millis(500));
        assert_eq!(runtime.inner.core.sweep_once().await.unwrap(), 1);
        let second = sweep.marker_key(&rid("g"), 11_000);
        assert!(queue.view().kv_get(&second).await.unwrap().is_none());
        assert_eq!(group.status().await.unwrap().cancelled, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn the_group_sweep_removes_the_records_of_every_run_of_a_member() {
        let (queue, store, clock) = open_queue_at(10_000).await;
        let runtime =
            WorkflowRuntime::builder(queue.clone(), store.clone(), TwoSteps, NoopTerminalHook)
                .group_retention(Duration::from_secs(1))
                .build()
                .unwrap();
        let group = runtime.group(rid("g"));
        let memos = crate::memo::MemoStore::new(store, "workflow-steps-memo");
        // The second submission runs the cancelled member again under the next
        // run id.
        let run_ids = [
            member_run_id(&rid("g"), "a", 0),
            member_run_id(&rid("g"), "a", 1),
        ];
        for run_id in &run_ids {
            group
                .submit(vec![member("a")], &RunOptions::default())
                .await
                .unwrap();
            assert_eq!(group.cancel().await.unwrap(), 1);
            memos.new_run_memo(run_id).put("k", b"v").await.unwrap();
            let results: Vec<MemberResult> =
                group.results().await.unwrap().try_collect().await.unwrap();
            assert_eq!(results[0].run_id, *run_id);
            assert!(
                queue
                    .view()
                    .kv_get(&outcome_kv_key(run_id))
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        let sweep = runtime.inner.core.group_sweep.as_ref().unwrap();
        let marker = sweep.marker_key(&rid("g"), 10_000);
        assert!(
            queue.view().kv_get(&marker).await.unwrap().is_some(),
            "the marker is written when the last termination is observed"
        );

        clock.advance(Duration::from_millis(999));
        assert_eq!(
            runtime.inner.core.sweep_once().await.unwrap(),
            0,
            "the marker is not yet expired"
        );
        clock.advance(Duration::from_millis(1));
        assert_eq!(runtime.inner.core.sweep_once().await.unwrap(), 1);
        assert!(queue.view().kv_get(&marker).await.unwrap().is_none());
        assert!(group.members().await.unwrap().is_empty());
        assert!(matches!(
            group.manifest().await,
            Err(Error::GroupNotFound(_))
        ));
        for run_id in &run_ids {
            assert!(
                queue
                    .view()
                    .kv_get(&outcome_kv_key(run_id))
                    .await
                    .unwrap()
                    .is_none(),
                "the terminal record of every run is removed with the group"
            );
            assert!(memos.new_run_memo(run_id).get("k").await.unwrap().is_none());
            assert!(
                runtime.status(run_id).await.unwrap().is_none(),
                "nothing of the member remains"
            );
        }
    }
}

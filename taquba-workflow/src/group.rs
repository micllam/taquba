//! Run groups: a durable set of runs of one runtime, identified by a
//! group id, whose membership is a manifest in the object store and
//! whose per-member state is a record in the queue's KV namespace.
//! [`RunGroup`] submits the members, waits for their terminations and
//! removes the group's state; [`jobs::JobGroup`](crate::jobs::JobGroup)
//! presents it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use futures_util::stream::{self, FuturesUnordered, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use taquba::object_store::{ObjectStore, path::Path};
use taquba::{Queue, SettlementEffects, WaitOutcome};
use tracing::warn;

use crate::blob::ObjectPrefix;
use crate::durable::{self, DurableMember, DurableTermination};
use crate::error::{Error, Result};
use crate::keys::{
    HEADER_GROUP, HEADER_GROUP_KEY, group_member_kv_key, group_members_kv_prefix,
    group_terminal_kv_key, hex_sha256,
};
use crate::memo::MemoStore;
use crate::runner::StepRunner;
use crate::runtime::{RunSpec, RuntimeCore, WorkflowRuntime};
use crate::sweep::Clearable;
use crate::terminal::{TerminalHook, TerminalStatus};

/// Member submissions in flight at once. Each blocks on a durable
/// enqueue commit, and concurrent commits share WAL flushes.
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
}

/// One member of a group's manifest: its key and the input of its run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ManifestMember {
    pub(crate) key: String,
    pub(crate) input: Vec<u8>,
}

/// The members of one group, in submission order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub(crate) group_id: String,
    pub(crate) members: Vec<ManifestMember>,
}

/// The submission settings applied to every member of a group.
#[derive(Debug, Clone, Default)]
pub(crate) struct MemberSpec {
    pub(crate) headers: HashMap<String, String>,
    pub(crate) priority: Option<u32>,
    pub(crate) max_attempts_per_step: Option<u32>,
    pub(crate) run_at: Option<SystemTime>,
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

    /// Remove the state of `group_id`: the memo entries of every member
    /// in its manifest, its member records and the manifest. A group
    /// without a manifest has its member records removed and nothing
    /// else.
    pub(crate) async fn forget(&self, group_id: &str) -> Result<()> {
        if let Some(manifest) = self.read_manifest(group_id).await? {
            for member in &manifest.members {
                self.memo_store
                    .clear_memos_for_run(&member_run_id(group_id, &member.key))
                    .await?;
            }
        }
        let prefix = group_members_kv_prefix(group_id);
        loop {
            let page = self.queue.kv_scan(&prefix, None, MEMBER_PAGE_SIZE).await?;
            if page.entries.is_empty() {
                break;
            }
            let keys = page.entries.into_iter().map(|(key, _)| key).collect();
            self.queue
                .commit_effects(SettlementEffects::default().kv_deletes(keys))
                .await?;
        }
        self.objects.delete(&self.manifest_path(group_id)).await?;
        Ok(())
    }
}

impl Clearable for GroupStore {
    type Error = Error;

    async fn clear(&self, group_id: &str) -> Result<Vec<Vec<u8>>> {
        self.forget(group_id).await.map(|()| Vec::new())
    }
}

/// A group of runs of one runtime. Obtained from
/// [`WorkflowRuntime::group`] or [`WorkflowRuntime::new_group`].
pub(crate) struct RunGroup<'a, R, H> {
    runtime: &'a WorkflowRuntime<R, H>,
    id: String,
}

impl<R, H> Clone for RunGroup<'_, R, H> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime,
            id: self.id.clone(),
        }
    }
}

impl<'a, R: StepRunner, H: TerminalHook> RunGroup<'a, R, H> {
    pub(crate) fn new(runtime: &'a WorkflowRuntime<R, H>, id: String) -> Self {
        Self { runtime, id }
    }

    pub(crate) fn id(&self) -> &str {
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

    /// Write the manifest, or check `members` against the existing one,
    /// and submit every member whose record is absent or terminated
    /// other than `Succeeded`. A submission that fails returns after the
    /// members submitted so far.
    pub(crate) async fn submit(
        &self,
        members: Vec<ManifestMember>,
        spec: &MemberSpec,
    ) -> Result<()> {
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
        self.submit_members(manifest.members, spec).await
    }

    /// [`Self::submit`] from the stored manifest.
    pub(crate) async fn resume(&self, spec: &MemberSpec) -> Result<()> {
        let manifest = self.manifest().await?;
        self.submit_members(manifest.members, spec).await
    }

    async fn submit_members(&self, members: Vec<ManifestMember>, spec: &MemberSpec) -> Result<()> {
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
                        headers: spec.headers.clone(),
                        priority: spec.priority,
                        max_attempts_per_step: spec.max_attempts_per_step,
                        run_at: spec.run_at,
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
    pub(crate) async fn terminations(
        &self,
    ) -> Result<impl Stream<Item = Result<MemberState>> + use<'a, R, H>> {
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

    /// Wait until the member `key` terminates and return its record.
    async fn wait_member(&self, key: &str) -> Result<DurableMember> {
        let core = self.core();
        let run_id = member_run_id(&self.id, key);
        let mut absent_job: Option<String> = None;
        let mut missing_pointer = false;
        loop {
            let member = self
                .store()
                .member(&self.id, key)
                .await?
                .ok_or_else(|| Error::InconsistentRunState(run_id.clone()))?;
            if member.terminated.is_some() {
                return Ok(member);
            }
            let Some(current) = core.current_step_if_active(&run_id).await? else {
                // The pointer and the member record change in one
                // transaction: a second read without either is a store
                // the runtime did not write.
                if missing_pointer {
                    return Err(Error::InconsistentRunState(run_id));
                }
                missing_pointer = true;
                continue;
            };
            if let WaitOutcome::NotFound = core.queue.wait_for_completion(&current.job_id).await? {
                if absent_job.as_deref() == Some(current.job_id.as_str()) {
                    return Err(Error::InconsistentRunState(run_id));
                }
                absent_job = Some(current.job_id);
            }
        }
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
    /// memo entries and outcome records of its members.
    pub(crate) async fn forget(&self) -> Result<()> {
        self.store().forget(&self.id).await
    }
}

impl Membership {
    /// The run id of this member.
    pub(crate) fn run_id(&self) -> String {
        member_run_id(&self.group_id, &self.key)
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

    fn member(key: &str) -> ManifestMember {
        ManifestMember {
            key: key.to_string(),
            input: key.as_bytes().to_vec(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn membership_holds_across_steps_and_terminations_wait_for_the_last_one() {
        let (queue, store) = open_queue().await;
        let runtime = WorkflowRuntime::builder(queue.clone(), store, TwoSteps, NoopTerminalHook)
            .poll_interval(Duration::from_millis(10))
            .build();
        let group = runtime.group("g").unwrap();
        group
            .submit(vec![member("a"), member("b")], &MemberSpec::default())
            .await
            .unwrap();
        let pending = group.members().await.unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|m| m.status().is_none()));
        assert_eq!(pending[0].record.run_id, member_run_id("g", "a"));

        let worker = runtime.spawn(std::future::pending::<()>());
        let terminated: Vec<MemberState> = tokio::time::timeout(Duration::from_secs(10), async {
            group.terminations().await.unwrap().try_collect().await
        })
        .await
        .expect("terminations finished in time")
        .unwrap();
        assert_eq!(terminated.len(), 2);
        for m in &terminated {
            assert_eq!(m.status(), Some(TerminalStatus::Succeeded));
            let termination = m.record.terminated.as_ref().unwrap();
            assert_eq!(
                termination.final_step, 1,
                "the member terminated at its second step"
            );
        }

        // A member already terminated is yielded again at once.
        let again: Vec<MemberState> = group
            .terminations()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(again.len(), 2);
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn an_external_cancellation_records_the_member_cancelled() {
        let (queue, store, _clock) = open_queue_at(10_000).await;
        let runtime =
            WorkflowRuntime::builder(queue.clone(), store, TwoSteps, NoopTerminalHook).build();
        let group = runtime.group("g").unwrap();
        group
            .submit(vec![member("a")], &MemberSpec::default())
            .await
            .unwrap();
        let run_id = member_run_id("g", "a");
        assert!(runtime.cancel(&run_id).await.unwrap());

        let members = group.members().await.unwrap();
        assert_eq!(members[0].status(), Some(TerminalStatus::Cancelled));
        assert_eq!(
            members[0]
                .record
                .terminated
                .as_ref()
                .unwrap()
                .terminated_at_ms,
            10_000
        );

        // The cancelled member is submitted again; a member is grouped by
        // its key, so a plain submission of the same run id is not.
        group
            .submit(vec![member("a")], &MemberSpec::default())
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
}

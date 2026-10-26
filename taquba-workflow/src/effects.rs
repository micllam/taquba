use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use taquba::{EnqueueEntry, EnqueueRequest, SettlementEffects};

use crate::error::{Error, Result};
use crate::keys::{RunId, check_enqueue, check_kv_key, run_kv_entry_key};

/// Application KV effects and enqueues staged during a step, applied in the
/// same transaction as the settlement that commits the step's outcome.
///
/// Obtained through [`Delivery::effects`](crate::Delivery::effects). Writes,
/// deletes and enqueues staged here are applied atomically with the
/// acknowledgement of the [`StepOutcome`](crate::StepOutcome) the runner
/// returns (`Continue`, `Succeed`, `Fail` and `Cancel`): either the settlement
/// and every staged effect commit together or none of them do. Delivery is
/// at-least-once, so a retried step stages its effects again and every staged
/// value must be correct when applied more than once. No effects are applied
/// when the runner returns a [`StepError`](crate::StepError) or when an
/// external [`WorkflowRuntime::cancel`](crate::WorkflowRuntime::cancel)
/// overrides the runner's outcome.
///
/// Each operation is validated when it is staged: keys of the caller KV
/// namespace must not start with the reserved `workflow/` prefix
/// ([`RESERVED_KV_PREFIX`](crate::RESERVED_KV_PREFIX)), values are capped at
/// [`taquba::MAX_KV_VALUE_SIZE`] each (an effects set does not have an
/// aggregate cap) and a key cannot be staged for both a write and a delete
/// within one step. The handle is sealed once `run_step` returns. Staging
/// through a clone retained past that point returns [`Error::EffectsSealed`].
///
/// A clone of the handle increments a reference count, and clones share one
/// accumulator. Use [`EffectsHandle::detached`] when constructing a
/// [`Step`](crate::Step) in tests.
#[derive(Debug, Clone)]
pub struct EffectsHandle {
    inner: Arc<Mutex<EffectsState>>,
    /// The run of the delivery, whose run KV space the handle stages into.
    run_id: RunId,
}

#[derive(Debug, Default)]
struct EffectsState {
    staged: StagedEffects,
    sealed: bool,
    /// The queue of the runtime, `None` for a detached handle.
    runtime_queue: Option<String>,
}

impl EffectsState {
    /// The state of a handle for a delivery of the runtime with the queue
    /// `runtime_queue`.
    fn for_delivery(runtime_queue: &str) -> Self {
        Self {
            runtime_queue: Some(runtime_queue.to_string()),
            ..Self::default()
        }
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.check_key(&key)?;
        self.stage_write(key, value)
    }

    /// Stage a write of the run KV entry at the full key `key`.
    fn put_run_kv(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.check_sealed()?;
        self.stage_write(key, value)
    }

    /// Stage a delete of the run KV entry at the full key `key`.
    fn delete_run_kv(&mut self, key: Vec<u8>) -> Result<()> {
        self.check_sealed()?;
        self.stage_delete(key)
    }

    fn stage_write(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if value.len() > taquba::MAX_KV_VALUE_SIZE {
            return Err(Error::Queue(taquba::Error::KvValueTooLarge {
                size: value.len(),
                max: taquba::MAX_KV_VALUE_SIZE,
            }));
        }
        if self.staged.deletes.contains(&key) {
            return Err(Error::ConflictingKvEffect(display_key(&key)));
        }
        self.staged.writes.insert(key, value);
        Ok(())
    }

    /// Seal the state and move out everything staged.
    fn seal_and_take(&mut self) -> StagedEffects {
        self.sealed = true;
        std::mem::take(&mut self.staged)
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.check_key(&key)?;
        self.stage_delete(key)
    }

    fn stage_delete(&mut self, key: Vec<u8>) -> Result<()> {
        if self.staged.writes.contains_key(&key) {
            return Err(Error::ConflictingKvEffect(display_key(&key)));
        }
        self.staged.deletes.insert(key);
        Ok(())
    }

    fn enqueue(&mut self, request: EnqueueRequest) -> Result<()> {
        if self.sealed {
            return Err(Error::EffectsSealed);
        }
        check_enqueue(&request, self.runtime_queue.as_deref())?;
        self.staged
            .enqueues
            .push(EnqueueEntry::from_request(request));
        Ok(())
    }

    fn check_key(&self, key: &[u8]) -> Result<()> {
        self.check_sealed()?;
        check_kv_key(key)
    }

    fn check_sealed(&self) -> Result<()> {
        if self.sealed {
            return Err(Error::EffectsSealed);
        }
        Ok(())
    }
}

/// Writes, deletes and enqueues accumulated by an [`EffectsHandle`] or a
/// [`TerminalEffects`]. Stored in the step-output replay record so a replayed
/// delivery applies the same effects.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct StagedEffects {
    pub(crate) writes: HashMap<Vec<u8>, Vec<u8>>,
    pub(crate) deletes: HashSet<Vec<u8>>,
    /// The queue and the entry of each enqueue.
    pub(crate) enqueues: Vec<(String, EnqueueEntry)>,
}

impl StagedEffects {
    /// Add the staged effects to `effects`.
    pub(crate) fn add_to(self, effects: &mut SettlementEffects) {
        effects.kv_writes.extend(self.writes);
        effects.kv_deletes.extend(self.deletes);
        effects.enqueues.extend(
            self.enqueues
                .into_iter()
                .map(|(queue, entry)| entry.into_request(queue)),
        );
    }
}

impl EffectsHandle {
    /// Build a handle that is not bound to a delivery, for constructing a
    /// [`Step`](crate::Step) in tests. A detached handle accepts and validates
    /// effects like a delivery-bound one, is never sealed and its staged
    /// effects are never applied. Its run is `detached`, the run of
    /// [`Delivery::detached`](crate::Delivery::detached).
    pub fn detached() -> Self {
        Self {
            inner: Arc::new(Mutex::new(EffectsState::default())),
            run_id: RunId::new("detached").expect("a literal run id"),
        }
    }

    /// A handle for a delivery of the run `run_id` of the runtime with the
    /// queue `runtime_queue`.
    pub(crate) fn for_delivery(runtime_queue: &str, run_id: &RunId) -> Self {
        Self {
            inner: Arc::new(Mutex::new(EffectsState::for_delivery(runtime_queue))),
            run_id: run_id.clone(),
        }
    }

    /// Stage a write of `value` under `key` in the run KV space of the
    /// delivery's run. An entry of the space belongs to its run: the memo sweep
    /// and the group sweep delete it with the run, and
    /// [`KvReadHandle::get_run_kv`](crate::KvReadHandle::get_run_kv) and
    /// [`WorkflowView::run_kv`](crate::WorkflowView::run_kv) read it.
    ///
    /// # Errors
    ///
    /// [`Error::Queue`] with [`taquba::Error::KvValueTooLarge`] when `value`
    /// exceeds [`taquba::MAX_KV_VALUE_SIZE`], [`Error::ConflictingKvEffect`]
    /// when `key` is already staged for a delete and [`Error::EffectsSealed`]
    /// when the step returned.
    pub fn put_run_kv(&self, key: impl AsRef<[u8]>, value: impl Into<Vec<u8>>) -> Result<()> {
        let key = run_kv_entry_key(&self.run_id, key.as_ref());
        self.inner.lock().unwrap().put_run_kv(key, value.into())
    }

    /// Stage a delete of `key` from the run KV space of the delivery's run.
    ///
    /// # Errors
    ///
    /// [`Error::ConflictingKvEffect`] when `key` is already staged for a write
    /// and [`Error::EffectsSealed`] when the step returned.
    pub fn delete_run_kv(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let key = run_kv_entry_key(&self.run_id, key.as_ref());
        self.inner.lock().unwrap().delete_run_kv(key)
    }

    /// Stage a write of `value` under `key` in the caller KV namespace.
    ///
    /// # Errors
    ///
    /// [`Error::ReservedKvKey`] when `key` starts with the reserved `workflow/`
    /// prefix, [`Error::Queue`] with [`taquba::Error::KvValueTooLarge`] when
    /// `value` exceeds [`taquba::MAX_KV_VALUE_SIZE`],
    /// [`Error::ConflictingKvEffect`] when `key` is already staged for a delete
    /// and [`Error::EffectsSealed`] when the step returned.
    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        self.inner.lock().unwrap().put(key.into(), value.into())
    }

    /// Stage a delete of `key` from the caller KV namespace.
    ///
    /// # Errors
    ///
    /// [`Error::ReservedKvKey`] when `key` starts with the reserved `workflow/`
    /// prefix, [`Error::ConflictingKvEffect`] when `key` is already staged for
    /// a write and [`Error::EffectsSealed`] when the step returned.
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.inner.lock().unwrap().delete(key.into())
    }

    /// Stage an enqueue, committed in the settlement of the step. A step that
    /// continues with [`StepOutcome::continue_on_signal`](crate::StepOutcome::continue_on_signal)
    /// stages here the request that leads to the signal, with
    /// [`Delivery::next_job_id`](crate::Delivery::next_job_id) in it, so the
    /// request leaves after the wait commits.
    ///
    /// # Errors
    ///
    /// [`Error::EffectsSealed`] when the step returned,
    /// [`Error::ReservedHeader`] when a header of `request` starts with the
    /// reserved `workflow.*` prefix and [`Error::ReservedQueue`] when `request`
    /// targets the queue of the runtime.
    pub fn enqueue(&self, request: EnqueueRequest) -> Result<()> {
        self.inner.lock().unwrap().enqueue(request)
    }

    /// Seal the handle and move out everything staged. An effect staged after
    /// the seal cannot join the settlement, so later staging attempts return
    /// [`Error::EffectsSealed`].
    pub(crate) fn seal_and_take(&self) -> StagedEffects {
        self.inner.lock().unwrap().seal_and_take()
    }
}

/// Effects staged by a [`TerminalHook`](crate::TerminalHook). Effects staged in
/// [`TerminalHook::stage_effects`](crate::TerminalHook::stage_effects) are
/// applied in the settlement that terminates the run, and effects staged in
/// [`TerminalHook::on_termination`](crate::TerminalHook::on_termination) in the
/// same transaction as the notification job's acknowledgement.
///
/// A hook stages the KV writes and deletes and the enqueues of
/// [`EffectsHandle`], validated by the same rules, so work that a run's
/// termination starts is created atomically with the termination or with the
/// notification's acknowledgement. The effects of `on_termination` are applied
/// only when it returns `Ok`, and a retried `on_termination` stages its effects
/// again.
///
/// The handle is sealed once the hook returns. Staging through a clone retained
/// past that point returns [`Error::EffectsSealed`]. Use
/// [`TerminalEffects::detached`] when invoking a hook directly in tests.
#[derive(Debug, Clone)]
pub struct TerminalEffects {
    inner: Arc<Mutex<EffectsState>>,
}

impl TerminalEffects {
    /// Build a handle that is not bound to a delivery, for invoking a
    /// [`TerminalHook`](crate::TerminalHook) directly in tests. A detached
    /// handle accepts and validates effects like a delivery-bound one, is never
    /// sealed and its staged effects are never applied. It does not belong to a
    /// runtime, so it accepts an enqueue to every queue.
    pub fn detached() -> Self {
        Self {
            inner: Arc::new(Mutex::new(EffectsState::default())),
        }
    }

    /// A handle for a delivery of the runtime with the queue `runtime_queue`.
    pub(crate) fn for_delivery(runtime_queue: &str) -> Self {
        Self {
            inner: Arc::new(Mutex::new(EffectsState::for_delivery(runtime_queue))),
        }
    }

    /// Stage a follow-up enqueue, committed with the notification's
    /// acknowledgement.
    ///
    /// # Errors
    ///
    /// As [`EffectsHandle::enqueue`].
    pub fn enqueue(&self, request: EnqueueRequest) -> Result<()> {
        self.inner.lock().unwrap().enqueue(request)
    }

    /// Stage a write of `value` under `key` in the caller KV namespace.
    ///
    /// # Errors
    ///
    /// As [`EffectsHandle::put`].
    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        self.inner.lock().unwrap().put(key.into(), value.into())
    }

    /// Stage a delete of `key` from the caller KV namespace.
    ///
    /// # Errors
    ///
    /// As [`EffectsHandle::delete`].
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.inner.lock().unwrap().delete(key.into())
    }

    /// Seal the handle and move out everything staged, as the effects of a
    /// settlement.
    pub(crate) fn seal_into_settlement(&self) -> SettlementEffects {
        let mut effects = SettlementEffects::default();
        self.inner
            .lock()
            .unwrap()
            .seal_and_take()
            .add_to(&mut effects);
        effects
    }
}

fn display_key(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use taquba::EnqueueOptions;

    #[test]
    fn staging_validates_keys_values_and_conflicts() {
        let handle = EffectsHandle::detached();
        assert!(matches!(
            handle.put("workflow/x", "v"),
            Err(Error::ReservedKvKey(_))
        ));
        assert!(matches!(
            handle.delete("workflow/x"),
            Err(Error::ReservedKvKey(_))
        ));
        let oversized = vec![0u8; taquba::MAX_KV_VALUE_SIZE + 1];
        assert!(matches!(
            handle.put("k", oversized),
            Err(Error::Queue(taquba::Error::KvValueTooLarge { .. }))
        ));
        handle.put("a", "v").unwrap();
        assert!(matches!(
            handle.delete("a"),
            Err(Error::ConflictingKvEffect(_))
        ));
        handle.delete("b").unwrap();
        assert!(matches!(
            handle.put("b", "v"),
            Err(Error::ConflictingKvEffect(_))
        ));
        // A run KV key is not in the caller namespace, so the reserved prefix
        // is a key like any other.
        handle.put_run_kv("workflow/x", "v").unwrap();
        assert!(matches!(
            handle.delete_run_kv("workflow/x"),
            Err(Error::ConflictingKvEffect(_))
        ));
        let request = |queue: &str, header: &str| EnqueueRequest {
            queue: queue.to_string(),
            payload: Vec::new(),
            options: EnqueueOptions::default()
                .headers(HashMap::from([(header.to_string(), String::new())])),
        };
        let bound = EffectsHandle::for_delivery("workflow-steps", &RunId::new("r").unwrap());
        assert!(matches!(
            bound.enqueue(request("workflow-steps", "h")),
            Err(Error::ReservedQueue(_))
        ));
        assert!(matches!(
            bound.enqueue(request("other", "workflow.x")),
            Err(Error::ReservedHeader(_))
        ));
        bound.enqueue(request("other", "h")).unwrap();
        handle.enqueue(request("workflow-steps", "h")).unwrap();
        let detached = RunId::new("detached").unwrap();
        let staged = handle.seal_and_take();
        assert!(
            staged
                .writes
                .contains_key(&run_kv_entry_key(&detached, b"workflow/x"))
        );
    }

    #[test]
    fn a_clone_stages_into_the_shared_accumulator_until_the_seal() {
        let run_id = RunId::new("r").unwrap();
        let handle = EffectsHandle::for_delivery("workflow-steps", &run_id);
        let clone = handle.clone();
        clone.put("a", "v").unwrap();
        clone.delete("b").unwrap();
        clone.put_run_kv("a", "w").unwrap();
        let staged = handle.seal_and_take();
        assert_eq!(staged.writes.get(b"a".as_slice()), Some(&b"v".to_vec()));
        assert!(staged.deletes.contains(b"b".as_slice()));
        assert_eq!(
            staged.writes.get(&run_kv_entry_key(&run_id, b"a")),
            Some(&b"w".to_vec())
        );
        assert!(matches!(clone.put("c", "v"), Err(Error::EffectsSealed)));
        assert!(matches!(clone.delete("c"), Err(Error::EffectsSealed)));
        let request = EnqueueRequest {
            queue: "other".to_string(),
            payload: Vec::new(),
            options: EnqueueOptions::default(),
        };
        assert!(matches!(clone.enqueue(request), Err(Error::EffectsSealed)));
    }

    #[test]
    fn the_terminal_handle_applies_the_staging_and_seal_rules() {
        let handle = TerminalEffects::for_delivery("workflow-steps");
        assert!(matches!(
            handle.put("workflow/x", "v"),
            Err(Error::ReservedKvKey(_))
        ));
        assert!(matches!(
            handle.delete("workflow/x"),
            Err(Error::ReservedKvKey(_))
        ));
        handle.put("a", "v").unwrap();
        assert!(matches!(
            handle.delete("a"),
            Err(Error::ConflictingKvEffect(_))
        ));
        handle.delete("b").unwrap();
        assert!(matches!(
            handle.enqueue(taquba::EnqueueRequest {
                queue: "side".to_string(),
                payload: Vec::new(),
                options: taquba::EnqueueOptions::default().headers(HashMap::from([(
                    "workflow.terminal".to_string(),
                    "1".to_string()
                )])),
            }),
            Err(Error::ReservedHeader(name)) if name == "workflow.terminal"
        ));
        let to_runtime = || taquba::EnqueueRequest {
            queue: "workflow-steps".to_string(),
            payload: Vec::new(),
            options: Default::default(),
        };
        assert!(matches!(
            handle.enqueue(to_runtime()),
            Err(Error::ReservedQueue(queue)) if queue == "workflow-steps"
        ));
        TerminalEffects::detached().enqueue(to_runtime()).unwrap();
        handle
            .enqueue(taquba::EnqueueRequest {
                queue: "side".to_string(),
                payload: Vec::new(),
                options: Default::default(),
            })
            .unwrap();
        let staged = handle.seal_into_settlement();
        assert_eq!(staged.kv_writes.len(), 1);
        assert_eq!(staged.kv_deletes.len(), 1);
        assert_eq!(staged.enqueues.len(), 1);
        assert!(matches!(handle.put("c", "v"), Err(Error::EffectsSealed)));
        assert!(matches!(handle.delete("c"), Err(Error::EffectsSealed)));
        assert!(matches!(
            handle.enqueue(taquba::EnqueueRequest {
                queue: "side".to_string(),
                payload: Vec::new(),
                options: Default::default(),
            }),
            Err(Error::EffectsSealed)
        ));
    }
}

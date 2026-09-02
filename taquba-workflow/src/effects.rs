use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use taquba::EnqueueRequest;

use crate::error::{Error, Result};
use crate::keys::RESERVED_KV_PREFIX;

/// Application KV effects staged during a step, applied in the same
/// transaction as the settlement that commits the step's outcome.
///
/// Obtained through [`Step::effects`](crate::Step::effects). Writes and
/// deletes staged here are applied atomically with the acknowledgement
/// of the [`StepOutcome`](crate::StepOutcome) the runner returns
/// (`Continue`, `Succeed`, `Fail` and `Cancel`): either the settlement
/// and every staged effect commit together or none of them do. Delivery
/// is at-least-once, so a retried step stages its effects again and
/// every staged value must be correct when applied more than once. No
/// effects are applied when the runner returns a
/// [`StepError`](crate::StepError) or when an external
/// [`WorkflowRuntime::cancel`](crate::WorkflowRuntime::cancel)
/// overrides the runner's outcome.
///
/// Each operation is validated when it is staged: keys must not start
/// with the reserved `workflow/` prefix
/// ([`RESERVED_KV_PREFIX`](crate::RESERVED_KV_PREFIX)), values are
/// capped at [`taquba::MAX_KV_VALUE_SIZE`] each (an effects set has no
/// aggregate cap) and a key cannot be staged for both a write and a
/// delete within one step. The handle is sealed once `run_step`
/// returns; staging through a clone retained past that point returns
/// [`Error::EffectsSealed`].
///
/// The handle is cheap to clone and clones share one accumulator. Use
/// [`EffectsHandle::detached`] when constructing a
/// [`Step`](crate::Step) in tests.
#[derive(Debug, Clone)]
pub struct EffectsHandle {
    inner: Arc<Mutex<EffectsState>>,
}

#[derive(Debug, Default)]
struct EffectsState {
    staged: StagedEffects,
    sealed: bool,
}

impl EffectsState {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.check_key(&key)?;
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

    /// Stage a write under a key of the reserved namespace, for the
    /// crate's own records; validated like [`put`](Self::put) otherwise.
    fn put_reserved(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if self.sealed {
            return Err(Error::EffectsSealed);
        }
        if value.len() > taquba::MAX_KV_VALUE_SIZE {
            return Err(Error::Queue(taquba::Error::KvValueTooLarge {
                size: value.len(),
                max: taquba::MAX_KV_VALUE_SIZE,
            }));
        }
        self.staged.writes.insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.check_key(&key)?;
        if self.staged.writes.contains_key(&key) {
            return Err(Error::ConflictingKvEffect(display_key(&key)));
        }
        self.staged.deletes.insert(key);
        Ok(())
    }

    fn check_key(&self, key: &[u8]) -> Result<()> {
        if self.sealed {
            return Err(Error::EffectsSealed);
        }
        if key.starts_with(RESERVED_KV_PREFIX.as_bytes()) {
            return Err(Error::ReservedKvKey(display_key(key)));
        }
        Ok(())
    }
}

/// Writes and deletes accumulated by an [`EffectsHandle`]. Stored in
/// the step-output replay record so a replayed delivery applies the
/// same effects.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct StagedEffects {
    pub(crate) writes: HashMap<Vec<u8>, Vec<u8>>,
    pub(crate) deletes: HashSet<Vec<u8>>,
}

impl EffectsHandle {
    /// Build a handle bound to no delivery, for constructing a
    /// [`Step`](crate::Step) in tests. A detached handle accepts and
    /// validates effects like a delivery-bound one, is never sealed and
    /// its staged effects are never applied.
    pub fn detached() -> Self {
        Self::for_delivery()
    }

    pub(crate) fn for_delivery() -> Self {
        Self {
            inner: Arc::new(Mutex::new(EffectsState::default())),
        }
    }

    /// Stage a write of `value` under `key` in the caller KV namespace.
    ///
    /// # Errors
    ///
    /// [`Error::ReservedKvKey`] when `key` starts with the reserved
    /// `workflow/` prefix, [`Error::Queue`] with
    /// [`taquba::Error::KvValueTooLarge`] when `value` exceeds
    /// [`taquba::MAX_KV_VALUE_SIZE`], [`Error::ConflictingKvEffect`]
    /// when `key` is already staged for a delete and
    /// [`Error::EffectsSealed`] when the step has returned.
    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        self.inner.lock().unwrap().put(key.into(), value.into())
    }

    /// Stage a delete of `key` from the caller KV namespace.
    ///
    /// # Errors
    ///
    /// [`Error::ReservedKvKey`] when `key` starts with the reserved
    /// `workflow/` prefix, [`Error::ConflictingKvEffect`] when `key` is
    /// already staged for a write and [`Error::EffectsSealed`] when the
    /// step has returned.
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.inner.lock().unwrap().delete(key.into())
    }

    /// Seal the handle and move out everything staged. An effect staged
    /// after the seal could not join the settlement, so later staging
    /// attempts return [`Error::EffectsSealed`].
    pub(crate) fn seal_and_take(&self) -> StagedEffects {
        let mut state = self.inner.lock().unwrap();
        state.sealed = true;
        std::mem::take(&mut state.staged)
    }
}

/// Effects staged by a [`TerminalHook`](crate::TerminalHook) during a
/// notification delivery, applied in the same transaction as the
/// notification job's acknowledgement.
///
/// Passed to
/// [`TerminalHook::on_termination`](crate::TerminalHook::on_termination).
/// Beyond the KV writes and deletes of [`EffectsHandle`] (validated by
/// the same rules), a hook stages follow-up enqueues, so work driven by
/// a run's termination is created atomically with the notification
/// being acknowledged. Effects are applied only when the hook returns
/// `Ok`; a retried hook stages its effects again.
///
/// The handle is sealed once the hook returns; staging through a clone
/// retained past that point returns [`Error::EffectsSealed`]. Use
/// [`TerminalEffects::detached`] when invoking a hook directly in
/// tests.
#[derive(Debug, Clone)]
pub struct TerminalEffects {
    inner: Arc<Mutex<TerminalState>>,
}

#[derive(Debug, Default)]
struct TerminalState {
    kv: EffectsState,
    enqueues: Vec<EnqueueRequest>,
}

impl TerminalEffects {
    /// Build a handle bound to no delivery, for invoking a
    /// [`TerminalHook`](crate::TerminalHook) directly in tests. A
    /// detached handle accepts and validates effects like a
    /// delivery-bound one, is never sealed and its staged effects are
    /// never applied.
    pub fn detached() -> Self {
        Self::for_delivery()
    }

    pub(crate) fn for_delivery() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TerminalState::default())),
        }
    }

    /// Stage a follow-up enqueue, committed with the notification's
    /// acknowledgement.
    ///
    /// # Errors
    ///
    /// [`Error::EffectsSealed`] when the hook has returned.
    pub fn enqueue(&self, request: EnqueueRequest) -> Result<()> {
        let mut state = self.inner.lock().unwrap();
        if state.kv.sealed {
            return Err(Error::EffectsSealed);
        }
        state.enqueues.push(request);
        Ok(())
    }

    /// Stage a write of `value` under `key` in the caller KV namespace.
    ///
    /// # Errors
    ///
    /// As [`EffectsHandle::put`].
    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        self.inner.lock().unwrap().kv.put(key.into(), value.into())
    }

    /// Stage a delete of `key` from the caller KV namespace.
    ///
    /// # Errors
    ///
    /// As [`EffectsHandle::delete`].
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.inner.lock().unwrap().kv.delete(key.into())
    }

    /// Stage a write under a key of the reserved `workflow/` namespace,
    /// for the crate's own records.
    pub(crate) fn put_reserved(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.inner.lock().unwrap().kv.put_reserved(key, value)
    }

    /// Seal the handle and move out everything staged.
    pub(crate) fn seal_and_take(&self) -> (StagedEffects, Vec<EnqueueRequest>) {
        let mut state = self.inner.lock().unwrap();
        state.kv.sealed = true;
        (
            std::mem::take(&mut state.kv.staged),
            std::mem::take(&mut state.enqueues),
        )
    }
}

fn display_key(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[test]
    fn a_sealed_handle_rejects_staging() {
        let handle = EffectsHandle::for_delivery();
        let clone = handle.clone();
        handle.put("a", "v").unwrap();
        let staged = handle.seal_and_take();
        assert_eq!(staged.writes.len(), 1);
        assert!(matches!(clone.put("b", "v"), Err(Error::EffectsSealed)));
        assert!(matches!(clone.delete("b"), Err(Error::EffectsSealed)));
    }

    #[test]
    fn the_terminal_handle_applies_the_staging_and_seal_rules() {
        let handle = TerminalEffects::for_delivery();
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
        handle
            .enqueue(taquba::EnqueueRequest {
                queue: "side".to_string(),
                payload: Vec::new(),
                options: Default::default(),
            })
            .unwrap();
        let (staged, enqueues) = handle.seal_and_take();
        assert_eq!(staged.writes.len(), 1);
        assert_eq!(staged.deletes.len(), 1);
        assert_eq!(enqueues.len(), 1);
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

    #[test]
    fn clones_share_one_accumulator() {
        let handle = EffectsHandle::for_delivery();
        let clone = handle.clone();
        clone.put("a", "v").unwrap();
        clone.delete("b").unwrap();
        let staged = handle.seal_and_take();
        assert_eq!(staged.writes.get(b"a".as_slice()), Some(&b"v".to_vec()));
        assert!(staged.deletes.contains(b"b".as_slice()));
    }
}

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use crate::Delivery;

/// Type-erased application state shared with every job handler.
#[derive(Default)]
pub(crate) struct State {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl State {
    pub(crate) fn insert<T: Any + Send + Sync>(&mut self, value: T) {
        self.map.insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub(crate) fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
    }
}

/// The per-call context handed to [`Job::run`](crate::jobs::Job::run):
/// the application state registered on the
/// [`JobRunner`](crate::jobs::JobRunner) and the [`Delivery`] the job
/// runs under, which it dereferences to (identity, attempt count, lease,
/// cancellation token, memo, staged KV effects and committed KV reads).
/// It holds no domain-specific clients (HTTP, LLM, etc.); those belong
/// to the application's registered state or to layers built on top.
pub struct JobContext<'a> {
    state: &'a State,
    delivery: &'a Delivery,
}

impl<'a> JobContext<'a> {
    pub(crate) fn new(state: &'a State, delivery: &'a Delivery) -> Self {
        Self { state, delivery }
    }

    /// Borrow a piece of application state by type.
    ///
    /// State is registered on the runner via
    /// [`JobRunnerBuilder::state`](crate::jobs::JobRunnerBuilder::state).
    ///
    /// # Panics
    ///
    /// Panics if no value of type `T` was registered. Use
    /// [`try_state`](Self::try_state) for a non-panicking lookup.
    pub fn state<T: Any + Send + Sync>(&self) -> &T {
        self.try_state().unwrap_or_else(|| {
            panic!(
                "no application state of type `{}` registered on the JobRunner",
                std::any::type_name::<T>()
            )
        })
    }

    /// Borrow a piece of application state by type, returning `None` if no
    /// value of type `T` was registered.
    pub fn try_state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get::<T>()
    }
}

impl Deref for JobContext<'_> {
    type Target = Delivery;

    fn deref(&self) -> &Delivery {
        self.delivery
    }
}

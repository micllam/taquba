//! Bulk multi-step processing for Taquba.
//!
//! This crate re-exports [`taquba_workflow::bulk`], where the
//! implementation lives. Depend on `taquba-workflow` and use its `bulk`
//! module directly; this crate receives no further development.

#![warn(missing_docs)]

pub use taquba_workflow::bulk::*;
pub use taquba_workflow::{EffectsHandle, StepError, StepErrorKind};

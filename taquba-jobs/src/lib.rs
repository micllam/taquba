//! Typed single-function jobs for Taquba.
//!
//! This crate re-exports [`taquba_workflow::jobs`], where the
//! implementation lives. Depend on `taquba-workflow` and use its `jobs`
//! module directly; this crate receives no further development.

#![warn(missing_docs)]

pub use taquba_workflow::jobs::*;

/// Re-export of the underlying [`taquba`] crate.
pub use taquba;

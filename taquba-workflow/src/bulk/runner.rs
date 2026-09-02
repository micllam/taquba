//! Adapter that drives a [`Pipeline`] as a single [`crate`] step.

use crate::{Step, StepError, StepOutcome, StepRunner};
use serde::{Deserialize, Serialize};

use crate::bulk::cost::CostReport;
use crate::bulk::pipeline::{BulkCtx, Pipeline};
use crate::outcome::run_recorded;

/// The per-item result the runner writes as the workflow step's `Succeed`
/// payload. Carries both the user [`Output`](Pipeline::Output) and the cost
/// accumulated while producing it, so the bulk terminal hook can stream the
/// output and roll the cost into the batch total in one decode.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ItemEnvelope<O> {
    pub output: O,
    pub cost: CostReport,
}

/// Bridges a [`Pipeline`] to [`crate::StepRunner`]. Each item is one
/// workflow run whose step 0 decodes the input, runs the pipeline once,
/// writes the item's outcome record to the run memo and `Succeed`s with an
/// [`ItemEnvelope`]. The pipeline's own multi-step logic lives inside
/// [`Pipeline::run`] as memoized calls; the runner never emits
/// [`StepOutcome::Continue`].
pub(crate) struct PipelineRunner<P> {
    pipeline: P,
}

impl<P> PipelineRunner<P> {
    pub(crate) fn new(pipeline: P) -> Self {
        Self { pipeline }
    }
}

impl<P: Pipeline> StepRunner for PipelineRunner<P> {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        run_recorded(step, async {
            // A bad payload does not decode on retry either: fail permanently.
            let input: P::Input = rmp_serde::from_slice(&step.payload)
                .map_err(|e| StepError::permanent(format!("failed to decode bulk input: {e}")))?;
            let ctx = BulkCtx::new(input, step);
            let output = self.pipeline.run(&ctx).await.map_err(Into::into)?;
            let envelope = ItemEnvelope {
                output,
                cost: ctx.cost(),
            };
            rmp_serde::to_vec_named(&envelope)
                .map_err(|e| StepError::permanent(format!("failed to encode bulk output: {e}")))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoStore;
    use std::collections::HashMap;
    use std::sync::Arc;
    use taquba::object_store::memory::InMemory;
    use tokio_util::sync::CancellationToken;

    #[derive(Serialize, Deserialize)]
    struct Doubler;

    impl Pipeline for Doubler {
        type Input = u32;
        type Output = u32;
        type Error = StepError;

        async fn run(&self, ctx: &BulkCtx<u32>) -> Result<u32, StepError> {
            ctx.record_cost("calls", 1.0);
            Ok(ctx.input * 2)
        }
    }

    struct AlwaysFails;

    impl Pipeline for AlwaysFails {
        type Input = u32;
        type Output = u32;
        type Error = StepError;

        async fn run(&self, _ctx: &BulkCtx<u32>) -> Result<u32, StepError> {
            Err(StepError::permanent("nope"))
        }
    }

    fn step_with_input(payload: Vec<u8>) -> Step {
        let memo_store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        Step {
            run_id: "run-1".into(),
            step_number: 0,
            payload,
            headers: HashMap::new(),
            job_id: "job-1".into(),
            attempts: 1,
            max_attempts: 3,
            cancel_token: CancellationToken::new(),
            lease: taquba::LeaseHandle::detached(),
            memo: memo_store.new_memo("run-1", 0),
            run_memo: memo_store.new_run_memo("run-1"),
            effects: crate::EffectsHandle::detached(),
            kv: crate::KvReadHandle::detached(),
            signal: None,
        }
    }

    #[tokio::test]
    async fn runs_pipeline_and_encodes_envelope() {
        let runner = PipelineRunner::new(Doubler);
        let step = step_with_input(rmp_serde::to_vec_named(&21u32).unwrap());

        let outcome = runner.run_step(&step).await.unwrap();
        let StepOutcome::Succeed { result } = outcome else {
            panic!("expected Succeed, got {outcome:?}");
        };
        let envelope: ItemEnvelope<u32> = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(envelope.output, 42);
        assert_eq!(envelope.cost.get("calls"), 1.0);
    }

    #[tokio::test]
    async fn undecodable_input_is_permanent() {
        let runner = PipelineRunner::new(Doubler);
        // A string where a u32 is expected: msgpack decode fails.
        let step = step_with_input(rmp_serde::to_vec_named(&"not a number").unwrap());
        let err = runner.run_step(&step).await.unwrap_err();
        assert_eq!(err.kind, crate::StepErrorKind::Permanent);
    }

    #[tokio::test]
    async fn pipeline_error_propagates_as_step_error() {
        let runner = PipelineRunner::new(AlwaysFails);
        let step = step_with_input(rmp_serde::to_vec_named(&1u32).unwrap());
        let err = runner.run_step(&step).await.unwrap_err();
        assert_eq!(err.message, "nope");
        assert_eq!(err.kind, crate::StepErrorKind::Permanent);
    }
}

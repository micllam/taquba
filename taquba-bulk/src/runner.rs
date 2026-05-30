//! Adapter that drives a [`Pipeline`] as a single [`taquba_workflow`] step.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use taquba_workflow::{Step, StepError, StepOutcome, StepRunner};

use crate::cost::CostReport;
use crate::pipeline::{BulkCtx, Pipeline};

/// The per-item result the runner writes as the workflow step's `Succeed`
/// payload. Carries both the user [`Output`](Pipeline::Output) and the cost
/// accumulated while producing it, so the bulk terminal hook can stream the
/// output and roll the cost into the batch total in one decode.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ItemEnvelope<O> {
    pub output: O,
    pub cost: CostReport,
}

/// Bridges a [`Pipeline`] to [`taquba_workflow::StepRunner`]. Each item is one
/// workflow run whose step 0 decodes the input, runs the pipeline once, and
/// `Succeed`s with an [`ItemEnvelope`]. The pipeline's own multi-step logic
/// lives inside [`Pipeline::run`] via [`BulkCtx::memoized`]; the runner never
/// emits [`StepOutcome::Continue`].
pub(crate) struct PipelineRunner<P> {
    pipeline: Arc<P>,
}

impl<P> PipelineRunner<P> {
    pub(crate) fn new(pipeline: Arc<P>) -> Self {
        Self { pipeline }
    }
}

impl<P: Pipeline> StepRunner for PipelineRunner<P> {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        // A bad payload won't decode on retry either, so fail permanently.
        let input: P::Input = rmp_serde::from_slice(&step.payload)
            .map_err(|e| StepError::permanent(format!("failed to decode bulk input: {e}")))?;

        let ctx = BulkCtx::new(
            input,
            step.run_id.clone(),
            step.headers.clone(),
            step.memo.clone(),
            step.cancel_token.clone(),
        );

        let output = self.pipeline.run(&ctx).await.map_err(Into::into)?;

        let envelope = ItemEnvelope {
            output,
            cost: ctx.cost(),
        };
        let result = rmp_serde::to_vec_named(&envelope)
            .map_err(|e| StepError::permanent(format!("failed to encode bulk output: {e}")))?;
        Ok(StepOutcome::Succeed { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use taquba::object_store::memory::InMemory;
    use taquba_workflow::MemoStore;
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
        let memo = MemoStore::new(Arc::new(InMemory::new()), "memo").new_memo("run-1", 0);
        Step {
            run_id: "run-1".into(),
            step_number: 0,
            payload,
            headers: HashMap::new(),
            job_id: "job-1".into(),
            attempts: 1,
            cancel_token: CancellationToken::new(),
            memo,
        }
    }

    #[tokio::test]
    async fn runs_pipeline_and_encodes_envelope() {
        let runner = PipelineRunner::new(Arc::new(Doubler));
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
        let runner = PipelineRunner::new(Arc::new(Doubler));
        // A string where a u32 is expected: msgpack decode fails.
        let step = step_with_input(rmp_serde::to_vec_named(&"not a number").unwrap());
        let err = runner.run_step(&step).await.unwrap_err();
        assert_eq!(err.kind, taquba_workflow::StepErrorKind::Permanent);
    }

    #[tokio::test]
    async fn pipeline_error_propagates_as_step_error() {
        let runner = PipelineRunner::new(Arc::new(AlwaysFails));
        let step = step_with_input(rmp_serde::to_vec_named(&1u32).unwrap());
        let err = runner.run_step(&step).await.unwrap_err();
        assert_eq!(err.message, "nope");
        assert_eq!(err.kind, taquba_workflow::StepErrorKind::Permanent);
    }
}

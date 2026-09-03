//! Adapter that drives a [`Pipeline`] as a single [`crate`] step.

use crate::{Step, StepError, StepOutcome, StepRunner};
use serde::{Deserialize, Serialize};

use crate::bulk::cost::CostReport;
use crate::bulk::pipeline::{BulkCtx, Pipeline};
use crate::bulk::progress::{ItemMarker, MarkerStatus};
use crate::keys::bulk_item_kv_key;
use crate::outcome::run_typed_step;

/// The payload of an item's run: the batch and key that identify the item
/// and its serialized input.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ItemPayload {
    pub(crate) batch_id: String,
    pub(crate) key: String,
    pub(crate) input: Vec<u8>,
}

/// The per-item result the runner writes as the workflow step's `Succeed`
/// payload and into the item's outcome record. Carries both the user
/// [`Output`](Pipeline::Output) and the cost accumulated while producing
/// it, so the batch run can stream the output and roll the cost into the
/// batch total in one decode.
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
///
/// The item's marker is staged on the step's effects: with the outcome
/// for a success, and in the failure set for an error, so it commits with
/// the acknowledgement or with the dead-lettering settlement and never
/// with a retried attempt.
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
        let ItemPayload {
            batch_id,
            key,
            input,
        } = rmp_serde::from_slice(&step.payload).map_err(|err| {
            StepError::permanent(format!(
                "item {} has a malformed payload: {err}",
                step.run_id
            ))
        })?;
        let outcome = run_typed_step(step, "bulk item", &input, |input: P::Input| async {
            let ctx = BulkCtx::new(&batch_id, &key, input, step);
            let output = self.pipeline.run(&ctx).await.map_err(Into::into)?;
            Ok(ItemEnvelope {
                output,
                cost: ctx.cost(),
            })
        })
        .await;
        let marker_key = bulk_item_kv_key(&batch_id, &key);
        match &outcome {
            Ok(StepOutcome::Succeed { result }) => {
                let cost = rmp_serde::from_slice::<ItemEnvelope<P::Output>>(result)
                    .map(|envelope| envelope.cost)
                    .unwrap_or_default();
                let marker = ItemMarker::new(MarkerStatus::Succeeded, None, cost);
                step.effects.put_reserved(marker_key, marker.encode()?)?;
            }
            Err(err) => {
                let marker = ItemMarker::new(
                    MarkerStatus::Failed,
                    Some(err.message.clone()),
                    CostReport::new(),
                );
                step.effects
                    .put_reserved_on_failure(marker_key, marker.encode()?)?;
            }
            Ok(_) => {}
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn step_with_input(input: Vec<u8>) -> Step {
        let payload = rmp_serde::to_vec_named(&ItemPayload {
            batch_id: "b".into(),
            key: "item-1".into(),
            input,
        })
        .unwrap();
        Step::detached(payload)
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
